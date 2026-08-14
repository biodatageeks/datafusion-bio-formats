use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::companion::sanitize_location;

use crate::catalog::BgenVariant;
use crate::header::{BgenCompression, BgenHeader, BgenLayout};
use crate::table_provider::{BgenOutputMode, BgenReadOptions};

#[derive(Debug)]
pub(crate) struct DecodedGenotypes {
    pub(crate) phased: bool,
    pub(crate) bits: u8,
    pub(crate) ploidy: Vec<u8>,
    pub(crate) values: DecodedValues,
    pub(crate) decompressed_bytes: usize,
}

#[derive(Debug)]
pub(crate) enum DecodedValues {
    Probabilities(Vec<Option<Vec<f32>>>),
    Dosages(Vec<Option<f32>>),
}

impl DecodedGenotypes {
    pub(crate) fn estimated_arrow_bytes(&self) -> usize {
        let value_bytes = match &self.values {
            DecodedValues::Probabilities(samples) => samples
                .iter()
                .filter_map(Option::as_ref)
                .map(|values| values.len().saturating_mul(size_of::<f32>()))
                .fold(0_usize, usize::saturating_add),
            DecodedValues::Dosages(samples) => samples.len().saturating_mul(size_of::<f32>()),
        };
        value_bytes
            .saturating_add(self.ploidy.len())
            .saturating_add(self.ploidy.len().saturating_mul(size_of::<i32>()))
    }
}

/// Buffers reused across the variants decoded by one partition.
///
/// Decoding allocates a decompressor and probability buffers whose sizes depend
/// only on the file dimensions, so a partition keeps one set and reuses it for
/// every variant instead of allocating per variant or per sample.
pub(crate) struct DecodeScratch {
    zlib: libdeflater::Decompressor,
    block: Vec<u8>,
    stored: Vec<u64>,
    complete: Vec<u64>,
    offsets: Vec<u64>,
}

impl DecodeScratch {
    pub(crate) fn new() -> Self {
        Self {
            zlib: libdeflater::Decompressor::new(),
            block: Vec::new(),
            stored: Vec::new(),
            complete: Vec::new(),
            offsets: Vec::new(),
        }
    }
}

impl std::fmt::Debug for DecodeScratch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DecodeScratch")
            .field("block_capacity", &self.block.capacity())
            .finish_non_exhaustive()
    }
}

pub(crate) fn decode_variant(
    path: &str,
    variant: &BgenVariant,
    header: &BgenHeader,
    payload: &[u8],
    selected_samples: &[usize],
    options: &BgenReadOptions,
    scratch: &mut DecodeScratch,
) -> Result<DecodedGenotypes> {
    match header.layout {
        BgenLayout::Layout1 => decode_layout1(
            path,
            variant,
            header,
            payload,
            selected_samples,
            options,
            scratch,
        ),
        BgenLayout::Layout2 => decode_layout2(
            path,
            variant,
            header,
            payload,
            selected_samples,
            options,
            scratch,
        ),
    }
}

fn decode_layout1(
    path: &str,
    variant: &BgenVariant,
    header: &BgenHeader,
    payload: &[u8],
    selected_samples: &[usize],
    options: &BgenReadOptions,
    scratch: &mut DecodeScratch,
) -> Result<DecodedGenotypes> {
    let DecodeScratch {
        zlib,
        block: block_buffer,
        ..
    } = scratch;
    let expected = (header.sample_count as usize)
        .checked_mul(6)
        .ok_or_else(|| execution_error(path, variant, "Layout 1 size arithmetic overflowed"))?;
    if expected > options.max_decompressed_block_bytes {
        return Err(execution_error(
            path,
            variant,
            "Layout 1 probability block exceeds max_decompressed_block_bytes",
        ));
    }
    let data: &[u8] = match header.compression {
        BgenCompression::None => {
            if payload.len() != expected {
                return Err(execution_error(
                    path,
                    variant,
                    &format!(
                        "Layout 1 payload length {}, expected {expected}",
                        payload.len()
                    ),
                ));
            }
            payload
        }
        BgenCompression::Zlib => {
            let compressed_length = read_u32(payload, 0)? as usize;
            let compressed = payload.get(4..).ok_or_else(|| {
                execution_error(path, variant, "Layout 1 compressed payload is truncated")
            })?;
            if compressed.len() != compressed_length {
                return Err(execution_error(
                    path,
                    variant,
                    &format!(
                        "Layout 1 compressed length {}, declared {compressed_length}",
                        compressed.len()
                    ),
                ));
            }
            decompress_zlib(block_buffer, zlib, path, variant, compressed, expected)?;
            block_buffer.as_slice()
        }
        BgenCompression::Zstd => {
            return Err(execution_error(
                path,
                variant,
                "Layout 1 cannot use zstd compression",
            ));
        }
    };

    let mut probabilities = Vec::with_capacity(selected_samples.len());
    let mut dosages = Vec::with_capacity(selected_samples.len());
    for &sample in selected_samples {
        let start = sample.checked_mul(6).ok_or_else(|| {
            execution_error(path, variant, "sample probability offset overflowed")
        })?;
        let values = [
            read_u16(data, start)? as u64,
            read_u16(data, start + 2)? as u64,
            read_u16(data, start + 4)? as u64,
        ];
        let sum: u64 = values.iter().sum();
        if sum == 0 {
            probabilities.push(None);
            dosages.push(None);
            continue;
        }
        if sum > 32_769 {
            return Err(execution_error(
                path,
                variant,
                &format!("Layout 1 sample {sample} probability total {sum} exceeds 32769"),
            ));
        }
        match options.output_mode {
            BgenOutputMode::Probability => probabilities.push(Some(
                values
                    .iter()
                    .map(|value| *value as f32 / 32_768.0)
                    .collect(),
            )),
            BgenOutputMode::Dosage => {
                dosages.push(Some((values[1] + 2 * values[2]) as f32 / 32_768.0));
            }
        }
    }

    Ok(DecodedGenotypes {
        phased: false,
        bits: 16,
        ploidy: vec![2; selected_samples.len()],
        values: match options.output_mode {
            BgenOutputMode::Probability => DecodedValues::Probabilities(probabilities),
            BgenOutputMode::Dosage => DecodedValues::Dosages(dosages),
        },
        decompressed_bytes: data.len(),
    })
}

fn decode_layout2(
    path: &str,
    variant: &BgenVariant,
    header: &BgenHeader,
    payload: &[u8],
    selected_samples: &[usize],
    options: &BgenReadOptions,
    scratch: &mut DecodeScratch,
) -> Result<DecodedGenotypes> {
    let DecodeScratch {
        zlib,
        block: block_buffer,
        stored,
        complete,
        offsets: sample_bit_offsets,
    } = scratch;
    let block_length = read_u32(payload, 0)? as usize;
    if payload.len() != block_length.saturating_add(4) {
        return Err(execution_error(
            path,
            variant,
            &format!(
                "Layout 2 payload length {}, declared {} plus length field",
                payload.len(),
                block_length
            ),
        ));
    }
    let block: &[u8] = match header.compression {
        BgenCompression::None => payload
            .get(4..)
            .ok_or_else(|| execution_error(path, variant, "Layout 2 payload is truncated"))?,
        BgenCompression::Zlib | BgenCompression::Zstd => {
            if block_length < 4 {
                return Err(execution_error(
                    path,
                    variant,
                    "compressed Layout 2 block length is smaller than 4",
                ));
            }
            let expected = read_u32(payload, 4)? as usize;
            if expected > options.max_decompressed_block_bytes {
                return Err(execution_error(
                    path,
                    variant,
                    &format!(
                        "declared decompressed length {expected} exceeds max_decompressed_block_bytes {}",
                        options.max_decompressed_block_bytes
                    ),
                ));
            }
            let compressed = payload.get(8..).ok_or_else(|| {
                execution_error(path, variant, "compressed Layout 2 payload is truncated")
            })?;
            if compressed.len() != block_length - 4 {
                return Err(execution_error(
                    path,
                    variant,
                    "compressed Layout 2 length does not match payload boundary",
                ));
            }
            match header.compression {
                BgenCompression::Zlib => {
                    decompress_zlib(block_buffer, zlib, path, variant, compressed, expected)?
                }
                BgenCompression::Zstd => {
                    decompress_zstd(block_buffer, path, variant, compressed, expected)?
                }
                BgenCompression::None => unreachable!(),
            }
            block_buffer.as_slice()
        }
    };
    if block.len() > options.max_decompressed_block_bytes {
        return Err(execution_error(
            path,
            variant,
            "Layout 2 probability block exceeds max_decompressed_block_bytes",
        ));
    }
    decode_layout2_block(
        path,
        variant,
        header,
        block,
        selected_samples,
        options,
        stored,
        complete,
        sample_bit_offsets,
    )
}

#[allow(clippy::too_many_arguments)]
fn decode_layout2_block(
    path: &str,
    variant: &BgenVariant,
    header: &BgenHeader,
    block: &[u8],
    selected_samples: &[usize],
    options: &BgenReadOptions,
    stored: &mut Vec<u64>,
    complete: &mut Vec<u64>,
    sample_bit_offsets: &mut Vec<u64>,
) -> Result<DecodedGenotypes> {
    let sample_count = read_u32(block, 0)?;
    let allele_count = read_u16(block, 4)? as usize;
    if sample_count != header.sample_count {
        return Err(execution_error(
            path,
            variant,
            &format!(
                "probability sample count {sample_count} differs from header count {}",
                header.sample_count
            ),
        ));
    }
    if allele_count != variant.alleles.len() {
        return Err(execution_error(
            path,
            variant,
            &format!(
                "probability allele count {allele_count} differs from metadata count {}",
                variant.alleles.len()
            ),
        ));
    }
    if allele_count == 0 {
        return Err(execution_error(
            path,
            variant,
            "probability allele count is zero",
        ));
    }
    let min_ploidy = *block
        .get(6)
        .ok_or_else(|| execution_error(path, variant, "missing minimum ploidy"))?;
    let max_ploidy = *block
        .get(7)
        .ok_or_else(|| execution_error(path, variant, "missing maximum ploidy"))?;
    if min_ploidy > 63 || max_ploidy > 63 || min_ploidy > max_ploidy {
        return Err(execution_error(
            path,
            variant,
            &format!("invalid ploidy range {min_ploidy}..={max_ploidy}"),
        ));
    }
    let ploidy_start = 8_usize;
    let ploidy_end = ploidy_start
        .checked_add(sample_count as usize)
        .ok_or_else(|| execution_error(path, variant, "ploidy array length overflowed"))?;
    let ploidy_bytes = block
        .get(ploidy_start..ploidy_end)
        .ok_or_else(|| execution_error(path, variant, "ploidy array is truncated"))?;
    let phased = match block.get(ploidy_end).copied() {
        Some(0) => false,
        Some(1) => true,
        Some(value) => {
            return Err(execution_error(
                path,
                variant,
                &format!("invalid phased flag {value}"),
            ));
        }
        None => return Err(execution_error(path, variant, "missing phased flag")),
    };
    let bits = *block
        .get(ploidy_end + 1)
        .ok_or_else(|| execution_error(path, variant, "missing probability bit precision"))?;
    if !(1..=32).contains(&bits) {
        return Err(execution_error(
            path,
            variant,
            &format!("invalid probability bit precision {bits}"),
        ));
    }
    if options.output_mode == BgenOutputMode::Dosage && allele_count != 2 {
        return Err(execution_error(
            path,
            variant,
            "BGEN dosage mode does not support multiallelic variants",
        ));
    }

    let probability_bytes = block
        .get(ploidy_end + 2..)
        .ok_or_else(|| execution_error(path, variant, "probability bytes are truncated"))?;

    // State counts depend only on ploidy, so resolve the declared ploidy range
    // once instead of recomputing binomial coefficients for every sample.
    let mut state_counts = [(0_u64, 0_u64); 64];
    for ploidy in min_ploidy..=max_ploidy {
        state_counts[ploidy as usize] = (
            stored_probability_count(ploidy, allele_count, phased)?,
            complete_probability_count(ploidy, allele_count, phased)?,
        );
    }

    // One declared ploidy means every sample occupies the same number of bits,
    // so the offset table collapses to a constant stride and the per-sample
    // state lookups and checked accumulation are hoisted out of the scan.
    let uniform_stride_bits = if min_ploidy == max_ploidy {
        let (stored_states, complete_states) = state_counts[min_ploidy as usize];
        if complete_states > options.max_states_per_sample as u64 {
            return Err(execution_error(
                path,
                variant,
                &format!(
                    "sample 0 state count {complete_states} exceeds max_states_per_sample {}",
                    options.max_states_per_sample
                ),
            ));
        }
        Some(stored_states.checked_mul(bits as u64).ok_or_else(|| {
            execution_error(path, variant, "sample probability bit count overflowed")
        })?)
    } else {
        None
    };

    sample_bit_offsets.clear();
    if uniform_stride_bits.is_some() {
        for (sample, &ploidy_missing) in ploidy_bytes.iter().enumerate() {
            if ploidy_missing & 0x40 != 0 {
                return Err(execution_error(
                    path,
                    variant,
                    &format!("sample {sample} has a non-zero reserved ploidy bit"),
                ));
            }
            if ploidy_missing & 0x3f != min_ploidy {
                return Err(execution_error(
                    path,
                    variant,
                    &format!(
                        "sample {sample} ploidy {} is outside declared range",
                        ploidy_missing & 0x3f
                    ),
                ));
            }
        }
    } else {
        sample_bit_offsets.reserve(sample_count as usize + 1);
        sample_bit_offsets.push(0_u64);
        for (sample, &ploidy_missing) in ploidy_bytes.iter().enumerate() {
            if ploidy_missing & 0x40 != 0 {
                return Err(execution_error(
                    path,
                    variant,
                    &format!("sample {sample} has a non-zero reserved ploidy bit"),
                ));
            }
            let ploidy = ploidy_missing & 0x3f;
            if ploidy < min_ploidy || ploidy > max_ploidy {
                return Err(execution_error(
                    path,
                    variant,
                    &format!("sample {sample} ploidy {ploidy} is outside declared range"),
                ));
            }
            let (stored_states, complete_states) = state_counts[ploidy as usize];
            if complete_states > options.max_states_per_sample as u64 {
                return Err(execution_error(
                    path,
                    variant,
                    &format!(
                        "sample {sample} state count {complete_states} exceeds max_states_per_sample {}",
                        options.max_states_per_sample
                    ),
                ));
            }
            let bits_for_sample = stored_states.checked_mul(bits as u64).ok_or_else(|| {
                execution_error(path, variant, "sample probability bit count overflowed")
            })?;
            sample_bit_offsets.push(
                sample_bit_offsets
                    .last()
                    .copied()
                    .unwrap_or(0)
                    .checked_add(bits_for_sample)
                    .ok_or_else(|| {
                        execution_error(path, variant, "probability bit offset overflowed")
                    })?,
            );
        }
    }
    let total_bits = match uniform_stride_bits {
        Some(stride) => (sample_count as u64)
            .checked_mul(stride)
            .ok_or_else(|| execution_error(path, variant, "probability bit offset overflowed"))?,
        None => *sample_bit_offsets.last().unwrap_or(&0),
    };
    let required_bytes = total_bits
        .checked_add(7)
        .and_then(|value| value.checked_div(8))
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| execution_error(path, variant, "probability byte count overflowed"))?;
    if probability_bytes.len() != required_bytes {
        return Err(execution_error(
            path,
            variant,
            &format!(
                "probability data has {} bytes, dimensions require {required_bytes}",
                probability_bytes.len()
            ),
        ));
    }
    if total_bits % 8 != 0
        && let Some(&last) = probability_bytes.last()
    {
        let used = (total_bits % 8) as u8;
        let mask = !((1_u8 << used) - 1);
        if last & mask != 0 {
            return Err(execution_error(
                path,
                variant,
                "probability data has non-zero trailing padding bits",
            ));
        }
    }

    let denominator = (1_u64 << bits) - 1;
    let mut ploidies = Vec::with_capacity(selected_samples.len());
    let mut probabilities = Vec::with_capacity(selected_samples.len());
    let mut dosages = Vec::with_capacity(selected_samples.len());

    // Whole-cohort dosage scans of biallelic 8-bit blocks with one declared
    // ploidy are the dominant workload, and there every sample occupies the same
    // whole bytes. Reading those bytes directly keeps the inner loop free of the
    // general bit reader, the probability buffers, and the per-sample state
    // lookups; the general path below still handles every other encoding.
    if options.output_mode == BgenOutputMode::Dosage
        && allele_count == 2
        && bits == 8
        && min_ploidy == max_ploidy
        && min_ploidy > 0
    {
        let stride = min_ploidy as usize;
        for &sample in selected_samples {
            let ploidy_missing = ploidy_bytes[sample];
            ploidies.push(ploidy_missing & 0x3f);
            let start = sample * stride;
            let values = probability_bytes
                .get(start..start + stride)
                .ok_or_else(|| {
                    execution_error(path, variant, "probability bitstream is truncated")
                })?;
            if ploidy_missing & 0x80 != 0 {
                if values.iter().any(|&value| value != 0) {
                    return Err(execution_error(
                        path,
                        variant,
                        &format!("missing sample {sample} has non-zero stored probabilities"),
                    ));
                }
                dosages.push(None);
                continue;
            }
            let numerator =
                byte_dosage_numerator(values, denominator, min_ploidy, phased).ok_or_else(|| {
                    let sum: u64 = values.iter().map(|&value| value as u64).sum();
                    execution_error(
                        path,
                        variant,
                        &format!(
                            "sample {sample} stored probability sum {sum} exceeds denominator {denominator}"
                        ),
                    )
                })?;
            dosages.push(Some(numerator as f32 / denominator as f32));
        }

        return Ok(DecodedGenotypes {
            phased,
            bits,
            ploidy: ploidies,
            values: DecodedValues::Dosages(dosages),
            decompressed_bytes: block.len(),
        });
    }

    for &sample in selected_samples {
        let ploidy_missing = ploidy_bytes[sample];
        let ploidy = ploidy_missing & 0x3f;
        let missing = ploidy_missing & 0x80 != 0;
        ploidies.push(ploidy);
        let offset = match uniform_stride_bits {
            Some(stride) => sample as u64 * stride,
            None => sample_bit_offsets[sample],
        };
        let stored_count = state_counts[ploidy as usize].0;
        read_probability_integers(
            stored,
            probability_bytes,
            offset,
            stored_count,
            bits,
            path,
            variant,
        )?;
        if missing {
            if stored.iter().any(|&value| value != 0) {
                return Err(execution_error(
                    path,
                    variant,
                    &format!("missing sample {sample} has non-zero stored probabilities"),
                ));
            }
            probabilities.push(None);
            dosages.push(None);
            continue;
        }
        let context = ProbabilityContext {
            path,
            variant,
            sample,
        };
        match options.output_mode {
            BgenOutputMode::Probability => {
                reconstruct_probabilities(
                    complete,
                    stored,
                    denominator,
                    ploidy,
                    allele_count,
                    phased,
                    context,
                )?;
                probabilities.push(Some(
                    complete
                        .iter()
                        .map(|value| *value as f32 / denominator as f32)
                        .collect(),
                ));
            }
            BgenOutputMode::Dosage => {
                // Dosage rejects multiallelic variants above, so the omitted
                // state is always implied by the stored ones and the expected
                // allele-one count can be summed without materializing the
                // complete probability vector.
                let numerator =
                    biallelic_dosage_numerator(stored, denominator, ploidy, phased, context)?;
                dosages.push(Some(numerator as f32 / denominator as f32));
            }
        }
    }

    Ok(DecodedGenotypes {
        phased,
        bits,
        ploidy: ploidies,
        values: match options.output_mode {
            BgenOutputMode::Probability => DecodedValues::Probabilities(probabilities),
            BgenOutputMode::Dosage => DecodedValues::Dosages(dosages),
        },
        decompressed_bytes: block.len(),
    })
}

fn stored_probability_count(ploidy: u8, allele_count: usize, phased: bool) -> Result<u64> {
    complete_probability_count(ploidy, allele_count, phased)?
        .checked_sub(if phased { ploidy as u64 } else { 1 })
        .ok_or_else(|| {
            DataFusionError::Execution("BGEN stored state count underflowed".to_string())
        })
}

fn complete_probability_count(ploidy: u8, allele_count: usize, phased: bool) -> Result<u64> {
    if phased {
        (ploidy as u64)
            .checked_mul(allele_count as u64)
            .ok_or_else(|| DataFusionError::Execution("phased state count overflowed".to_string()))
    } else {
        choose(
            (ploidy as u64)
                .checked_add(allele_count.saturating_sub(1) as u64)
                .ok_or_else(|| {
                    DataFusionError::Execution("unphased state count overflowed".to_string())
                })?,
            allele_count.saturating_sub(1) as u64,
        )
    }
}

fn choose(mut n: u64, mut k: u64) -> Result<u64> {
    if k > n {
        return Ok(0);
    }
    k = k.min(n - k);
    let mut result = 1_u64;
    for divisor in 1..=k {
        result = result.checked_mul(n).ok_or_else(|| {
            DataFusionError::Execution("BGEN genotype state count overflowed".to_string())
        })? / divisor;
        n -= 1;
    }
    Ok(result)
}

/// Reads `count` little-endian `bits`-wide integers starting at `start_bit` into
/// `values`, which is cleared first and reused across samples.
fn read_probability_integers(
    values: &mut Vec<u64>,
    bytes: &[u8],
    start_bit: u64,
    count: u64,
    bits: u8,
    path: &str,
    variant: &BgenVariant,
) -> Result<()> {
    let capacity = usize::try_from(count)
        .map_err(|_| execution_error(path, variant, "probability count does not fit usize"))?;
    values.clear();
    values.reserve(capacity);

    // Byte-aligned 8-bit probabilities are by far the most common encoding, so
    // read them directly instead of running the general bit-extraction loop.
    if bits == 8 && start_bit.is_multiple_of(8) {
        let start = usize::try_from(start_bit / 8).map_err(|_| {
            execution_error(path, variant, "probability byte offset does not fit usize")
        })?;
        let end = start
            .checked_add(capacity)
            .ok_or_else(|| execution_error(path, variant, "probability byte range overflowed"))?;
        let source = bytes
            .get(start..end)
            .ok_or_else(|| execution_error(path, variant, "probability bitstream is truncated"))?;
        values.extend(source.iter().map(|byte| u64::from(*byte)));
        return Ok(());
    }

    for index in 0..count {
        let offset = start_bit
            .checked_add(index.checked_mul(bits as u64).ok_or_else(|| {
                execution_error(path, variant, "probability bit offset overflowed")
            })?)
            .ok_or_else(|| execution_error(path, variant, "probability bit offset overflowed"))?;
        let byte_index = usize::try_from(offset / 8).map_err(|_| {
            execution_error(path, variant, "probability byte offset does not fit usize")
        })?;
        let shift = (offset % 8) as usize;
        let byte_count = (shift + bits as usize).div_ceil(8);
        let end = byte_index
            .checked_add(byte_count)
            .ok_or_else(|| execution_error(path, variant, "probability byte range overflowed"))?;
        let source = bytes
            .get(byte_index..end)
            .ok_or_else(|| execution_error(path, variant, "probability bitstream is truncated"))?;
        let mut word = 0_u64;
        for (byte_offset, byte) in source.iter().enumerate() {
            word |= u64::from(*byte) << (byte_offset * 8);
        }
        let mask = (1_u64 << bits) - 1;
        values.push((word >> shift) & mask);
    }
    Ok(())
}

struct ProbabilityContext<'a> {
    path: &'a str,
    variant: &'a BgenVariant,
    sample: usize,
}

/// [`biallelic_dosage_numerator`] for whole-byte stored values.
///
/// Returns `None` when an unphased stored sum exceeds `denominator`, which is
/// the same input the general path rejects. A byte can never exceed an 8-bit
/// denominator, so the per-haplotype phased bound cannot be violated here.
#[inline]
fn byte_dosage_numerator(values: &[u8], denominator: u64, ploidy: u8, phased: bool) -> Option<u64> {
    if phased {
        let sum: u64 = values.iter().map(|&value| value as u64).sum();
        return Some(ploidy as u64 * denominator - sum);
    }
    let mut sum = 0_u64;
    let mut weighted = 0_u64;
    for (allele_one_count, &value) in values.iter().enumerate() {
        sum += value as u64;
        weighted += allele_one_count as u64 * value as u64;
    }
    if sum > denominator {
        return None;
    }
    Some(weighted + ploidy as u64 * (denominator - sum))
}

/// Returns the expected count of encoded allele one for one biallelic sample,
/// scaled by `denominator`.
///
/// This is the dosage-mode equivalent of calling [`reconstruct_probabilities`]
/// and then weighting the complete vector, but for a biallelic variant the
/// omitted state is a function of the stored ones, so the weighted sum collapses
/// to a single pass over `stored`. The stored-sum validation matches
/// [`reconstruct_probabilities`] so both modes reject the same malformed input.
fn biallelic_dosage_numerator(
    stored: &[u64],
    denominator: u64,
    ploidy: u8,
    phased: bool,
    context: ProbabilityContext<'_>,
) -> Result<u64> {
    let ploidy_total = (ploidy as u64).saturating_mul(denominator);
    if phased {
        // `stored` holds P(allele 0) per haplotype and each omitted value is
        // `denominator - stored[h]`, so the dosage is their sum.
        let mut total = 0_u64;
        for (haplotype, &value) in stored.iter().enumerate() {
            if value > denominator {
                return Err(execution_error(
                    context.path,
                    context.variant,
                    &format!(
                        "sample {} haplotype {haplotype} stored probability sum {value} exceeds denominator {denominator}",
                        context.sample
                    ),
                ));
            }
            total += denominator - value;
        }
        return Ok(total);
    }

    // `stored[i]` is P(i copies of allele one) and the omitted final state is
    // P(ploidy copies), so the weighted sum is
    // `sum(i * stored[i]) + ploidy * (denominator - sum(stored))`.
    let mut sum = 0_u64;
    let mut weighted = 0_u64;
    for (allele_one_count, &value) in stored.iter().enumerate() {
        sum = sum.checked_add(value).ok_or_else(|| {
            execution_error(context.path, context.variant, "probability sum overflowed")
        })?;
        weighted += allele_one_count as u64 * value;
    }
    if sum > denominator {
        return Err(execution_error(
            context.path,
            context.variant,
            &format!(
                "sample {} stored probability sum {sum} exceeds denominator {denominator}",
                context.sample
            ),
        ));
    }
    Ok(weighted + ploidy_total - (ploidy as u64) * sum)
}

/// Restores the omitted final probability of each stored vector into `complete`,
/// which is cleared first and reused across samples.
fn reconstruct_probabilities(
    complete: &mut Vec<u64>,
    stored: &[u64],
    denominator: u64,
    ploidy: u8,
    allele_count: usize,
    phased: bool,
    context: ProbabilityContext<'_>,
) -> Result<()> {
    complete.clear();
    if phased {
        let stored_per_haplotype = allele_count - 1;
        if stored_per_haplotype == 0 {
            complete.resize(ploidy as usize, denominator);
            return Ok(());
        }
        complete.reserve(ploidy as usize * allele_count);
        for (haplotype, values) in stored.chunks_exact(stored_per_haplotype).enumerate() {
            let sum = values.iter().try_fold(0_u64, |sum, value| {
                sum.checked_add(*value).ok_or_else(|| {
                    execution_error(
                        context.path,
                        context.variant,
                        "phased probability sum overflowed",
                    )
                })
            })?;
            if sum > denominator {
                return Err(execution_error(
                    context.path,
                    context.variant,
                    &format!(
                        "sample {} haplotype {haplotype} stored probability sum {sum} exceeds denominator {denominator}",
                        context.sample
                    ),
                ));
            }
            complete.extend_from_slice(values);
            complete.push(denominator - sum);
        }
        Ok(())
    } else {
        let sum = stored.iter().try_fold(0_u64, |sum, value| {
            sum.checked_add(*value).ok_or_else(|| {
                execution_error(context.path, context.variant, "probability sum overflowed")
            })
        })?;
        if sum > denominator {
            return Err(execution_error(
                context.path,
                context.variant,
                &format!(
                    "sample {} stored probability sum {sum} exceeds denominator {denominator}",
                    context.sample
                ),
            ));
        }
        let expected = choose(
            ploidy as u64 + allele_count.saturating_sub(1) as u64,
            allele_count.saturating_sub(1) as u64,
        )?;
        complete.reserve(expected as usize);
        complete.extend_from_slice(stored);
        complete.push(denominator - sum);
        Ok(())
    }
}

/// Decompresses a zlib probability block into `output`.
///
/// BGEN stores the decompressed length in the block header, so the exact output
/// size is known before decompressing and a single bounded call replaces
/// incremental streaming reads. The caller has already checked `expected`
/// against `max_decompressed_block_bytes`.
fn decompress_zlib(
    output: &mut Vec<u8>,
    decompressor: &mut libdeflater::Decompressor,
    path: &str,
    variant: &BgenVariant,
    compressed: &[u8],
    expected: usize,
) -> Result<()> {
    output.clear();
    output.resize(expected, 0);
    let written = decompressor
        .zlib_decompress(compressed, output)
        .map_err(|error| {
            execution_error(
                path,
                variant,
                &format!("decompress probability block: {error}"),
            )
        })?;
    check_decompressed_length(output, written, expected, path, variant)
}

/// Decompresses a zstd probability block into `output`.
fn decompress_zstd(
    output: &mut Vec<u8>,
    path: &str,
    variant: &BgenVariant,
    compressed: &[u8],
    expected: usize,
) -> Result<()> {
    output.clear();
    output.resize(expected, 0);
    let written = zstd::bulk::decompress_to_buffer(compressed, output).map_err(|error| {
        execution_error(
            path,
            variant,
            &format!("decompress probability block: {error}"),
        )
    })?;
    check_decompressed_length(output, written, expected, path, variant)
}

fn check_decompressed_length(
    output: &mut Vec<u8>,
    written: usize,
    expected: usize,
    path: &str,
    variant: &BgenVariant,
) -> Result<()> {
    if written != expected {
        output.clear();
        return Err(execution_error(
            path,
            variant,
            &format!("decompressed probability length {written}, expected {expected}"),
        ));
    }
    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let end = offset
        .checked_add(2)
        .ok_or_else(|| DataFusionError::Execution("u16 offset overflowed".to_string()))?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| DataFusionError::Execution("truncated little-endian u16".to_string()))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| DataFusionError::Execution("u32 offset overflowed".to_string()))?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| DataFusionError::Execution("truncated little-endian u32".to_string()))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn execution_error(path: &str, variant: &BgenVariant, message: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "BGEN {} variant {} at byte {}: {message}",
        sanitize_location(path),
        variant.index,
        variant.record_offset
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_state_counts_cover_variable_ploidy() {
        assert_eq!(choose(4, 2).unwrap(), 6);
        assert_eq!(stored_probability_count(2, 2, false).unwrap(), 2);
        assert_eq!(stored_probability_count(3, 3, false).unwrap(), 9);
        assert_eq!(stored_probability_count(2, 3, true).unwrap(), 4);
    }

    fn test_variant() -> BgenVariant {
        BgenVariant {
            index: 0,
            id: None,
            rsid: None,
            chrom: String::new(),
            start: 0,
            end: 0,
            position: 1,
            alleles: vec![],
            record_offset: 0,
            record_size: 0,
            payload_offset: 0,
            payload_size: 0,
        }
    }

    #[test]
    fn reads_little_endian_unaligned_bits() {
        let mut values = Vec::new();
        read_probability_integers(
            &mut values,
            &[0b1101_0011, 0b0000_0010],
            2,
            3,
            3,
            "fixture",
            &test_variant(),
        )
        .unwrap();
        assert_eq!(values, vec![4, 6, 2]);
    }

    #[test]
    fn aligned_byte_reads_match_the_general_bit_reader() {
        let variant = test_variant();
        let bytes: Vec<u8> = (0..=255_u8).collect();
        for start_byte in 0..8_u64 {
            for count in 0..16_u64 {
                let mut fast = Vec::new();
                read_probability_integers(
                    &mut fast,
                    &bytes,
                    start_byte * 8,
                    count,
                    8,
                    "fixture",
                    &variant,
                )
                .unwrap();
                let expected: Vec<u64> = (0..count)
                    .map(|index| u64::from(bytes[(start_byte + index) as usize]))
                    .collect();
                assert_eq!(fast, expected, "start_byte {start_byte} count {count}");
            }
        }
    }

    #[test]
    fn aligned_byte_reads_reject_a_truncated_bitstream() {
        let mut values = Vec::new();
        let error =
            read_probability_integers(&mut values, &[1, 2, 3], 0, 4, 8, "fixture", &test_variant())
                .unwrap_err();
        assert!(
            error.to_string().contains("truncated"),
            "unexpected error: {error}"
        );
    }

    /// The dosage fast path must return exactly what reconstructing the
    /// complete probability vector and weighting it would produce.
    #[test]
    fn biallelic_dosage_matches_the_reconstructed_vector() {
        let variant = test_variant();
        let mut complete = Vec::new();
        for bits in [8_u8, 16] {
            let denominator = (1_u64 << bits) - 1;
            for ploidy in 1..=4_u8 {
                for phased in [false, true] {
                    let stored_count = stored_probability_count(ploidy, 2, phased).unwrap();
                    // Phased vectors bound each haplotype value by the
                    // denominator; unphased vectors bound their total, so scale
                    // by the stored count to stay inside both.
                    let scale = if phased { 1 } else { stored_count + 1 };
                    for step in 0..=8_u64 {
                        let stored: Vec<u64> = (0..stored_count)
                            .map(|index| {
                                let share = denominator / scale;
                                match step {
                                    0 => 0,
                                    1 if index == 0 => denominator / scale,
                                    1 => 0,
                                    _ => share * (step + index) / (step + index + 2),
                                }
                            })
                            .collect();
                        let total: u64 = stored.iter().sum();
                        assert!(
                            if phased {
                                stored.iter().all(|&value| value <= denominator)
                            } else {
                                total <= denominator
                            },
                            "generated an invalid stored vector {stored:?}"
                        );
                        let context = ProbabilityContext {
                            path: "fixture",
                            variant: &variant,
                            sample: 0,
                        };
                        reconstruct_probabilities(
                            &mut complete,
                            &stored,
                            denominator,
                            ploidy,
                            2,
                            phased,
                            context,
                        )
                        .unwrap();
                        let expected: u64 = if phased {
                            complete.chunks_exact(2).map(|pair| pair[1]).sum()
                        } else {
                            complete
                                .iter()
                                .enumerate()
                                .map(|(count, value)| count as u64 * value)
                                .sum()
                        };
                        let actual = biallelic_dosage_numerator(
                            &stored,
                            denominator,
                            ploidy,
                            phased,
                            ProbabilityContext {
                                path: "fixture",
                                variant: &variant,
                                sample: 0,
                            },
                        )
                        .unwrap();
                        assert_eq!(
                            actual, expected,
                            "bits {bits} ploidy {ploidy} phased {phased} stored {stored:?}"
                        );
                    }
                }
            }
        }
    }

    /// The whole-byte fast path must agree with the general helper, which the
    /// test above ties back to the reconstructed probability vector.
    #[test]
    fn byte_dosage_matches_the_general_helper() {
        let variant = test_variant();
        let denominator = 255_u64;
        for ploidy in 1..=3_u8 {
            for phased in [false, true] {
                for first in (0..=255_u64).step_by(17) {
                    for second in (0..=255_u64).step_by(23) {
                        let bytes: Vec<u8> = (0..ploidy as usize)
                            .map(|index| {
                                if index % 2 == 0 {
                                    first as u8
                                } else {
                                    second as u8
                                }
                            })
                            .collect();
                        let stored: Vec<u64> =
                            bytes.iter().map(|&value| u64::from(value)).collect();
                        let expected = biallelic_dosage_numerator(
                            &stored,
                            denominator,
                            ploidy,
                            phased,
                            ProbabilityContext {
                                path: "fixture",
                                variant: &variant,
                                sample: 0,
                            },
                        );
                        let actual = byte_dosage_numerator(&bytes, denominator, ploidy, phased);
                        match (expected, actual) {
                            (Ok(expected), Some(actual)) => assert_eq!(
                                expected, actual,
                                "ploidy {ploidy} phased {phased} bytes {bytes:?}"
                            ),
                            (Err(_), None) => {}
                            (expected, actual) => panic!(
                                "fast path disagreed for ploidy {ploidy} phased {phased} \
                                 bytes {bytes:?}: general {expected:?}, fast {actual:?}"
                            ),
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn biallelic_dosage_rejects_a_stored_sum_above_the_denominator() {
        let variant = test_variant();
        for (stored, phased) in [(vec![200_u64, 200], false), (vec![300_u64, 10], true)] {
            let error = biallelic_dosage_numerator(
                &stored,
                255,
                2,
                phased,
                ProbabilityContext {
                    path: "fixture",
                    variant: &variant,
                    sample: 3,
                },
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("exceeds denominator"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn scratch_buffers_are_cleared_between_reads() {
        let mut values = vec![7, 7, 7, 7, 7];
        read_probability_integers(&mut values, &[9, 8], 0, 2, 8, "fixture", &test_variant())
            .unwrap();
        assert_eq!(values, vec![9, 8]);
    }

    #[test]
    fn reads_every_supported_bit_precision() {
        let variant = test_variant();
        let mut values = Vec::new();
        for bits in 1..=32 {
            let mask = (1_u64 << bits) - 1;
            let expected = [mask, mask / 3, mask / 7];
            let start_bit = 5_u64;
            let total_bits = start_bit as usize + expected.len() * bits as usize;
            let mut bytes = vec![0_u8; total_bits.div_ceil(8)];
            for (index, value) in expected.iter().copied().enumerate() {
                for bit in 0..bits as usize {
                    let offset = start_bit as usize + index * bits as usize + bit;
                    bytes[offset / 8] |= (((value >> bit) & 1) as u8) << (offset % 8);
                }
            }
            read_probability_integers(
                &mut values,
                &bytes,
                start_bit,
                expected.len() as u64,
                bits,
                "fixture",
                &variant,
            )
            .unwrap();
            assert_eq!(values, expected, "bit precision {bits}");
        }
    }
}
