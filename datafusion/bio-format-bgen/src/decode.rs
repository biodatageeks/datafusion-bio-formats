use std::borrow::Cow;
use std::io::Read;

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

pub(crate) fn decode_variant(
    path: &str,
    variant: &BgenVariant,
    header: &BgenHeader,
    payload: &[u8],
    selected_samples: &[usize],
    options: &BgenReadOptions,
) -> Result<DecodedGenotypes> {
    match header.layout {
        BgenLayout::Layout1 => {
            decode_layout1(path, variant, header, payload, selected_samples, options)
        }
        BgenLayout::Layout2 => {
            decode_layout2(path, variant, header, payload, selected_samples, options)
        }
    }
}

fn decode_layout1(
    path: &str,
    variant: &BgenVariant,
    header: &BgenHeader,
    payload: &[u8],
    selected_samples: &[usize],
    options: &BgenReadOptions,
) -> Result<DecodedGenotypes> {
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
    let data = match header.compression {
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
            Cow::Borrowed(payload)
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
            Cow::Owned(decompress_zlib(
                path, variant, compressed, expected, options,
            )?)
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
            read_u16(data.as_ref(), start)? as u64,
            read_u16(data.as_ref(), start + 2)? as u64,
            read_u16(data.as_ref(), start + 4)? as u64,
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
) -> Result<DecodedGenotypes> {
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
    let block = match header.compression {
        BgenCompression::None => Cow::Borrowed(
            payload
                .get(4..)
                .ok_or_else(|| execution_error(path, variant, "Layout 2 payload is truncated"))?,
        ),
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
            Cow::Owned(match header.compression {
                BgenCompression::Zlib => {
                    decompress_zlib(path, variant, compressed, expected, options)?
                }
                BgenCompression::Zstd => {
                    decompress_zstd(path, variant, compressed, expected, options)?
                }
                BgenCompression::None => unreachable!(),
            })
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
        block.as_ref(),
        selected_samples,
        options,
    )
}

fn decode_layout2_block(
    path: &str,
    variant: &BgenVariant,
    header: &BgenHeader,
    block: &[u8],
    selected_samples: &[usize],
    options: &BgenReadOptions,
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
    let mut sample_bit_offsets = Vec::with_capacity(sample_count as usize + 1);
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
        let stored_states = stored_probability_count(ploidy, allele_count, phased)?;
        let complete_states = complete_probability_count(ploidy, allele_count, phased)?;
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
    let total_bits = *sample_bit_offsets.last().unwrap_or(&0);
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
    for &sample in selected_samples {
        let ploidy_missing = ploidy_bytes[sample];
        let ploidy = ploidy_missing & 0x3f;
        let missing = ploidy_missing & 0x80 != 0;
        ploidies.push(ploidy);
        let offset = sample_bit_offsets[sample];
        let stored_count = stored_probability_count(ploidy, allele_count, phased)?;
        let stored = read_probability_integers(
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
        let complete = reconstruct_probabilities(
            &stored,
            denominator,
            ploidy,
            allele_count,
            phased,
            ProbabilityContext {
                path,
                variant,
                sample,
            },
        )?;
        match options.output_mode {
            BgenOutputMode::Probability => probabilities.push(Some(
                complete
                    .iter()
                    .map(|value| *value as f32 / denominator as f32)
                    .collect(),
            )),
            BgenOutputMode::Dosage => {
                let dosage = if phased {
                    complete
                        .chunks_exact(2)
                        .map(|haplotype| haplotype[1])
                        .sum::<u64>()
                } else {
                    complete
                        .iter()
                        .enumerate()
                        .map(|(allele_one_count, probability)| {
                            allele_one_count as u64 * probability
                        })
                        .sum()
                };
                dosages.push(Some(dosage as f32 / denominator as f32));
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

fn read_probability_integers(
    bytes: &[u8],
    start_bit: u64,
    count: u64,
    bits: u8,
    path: &str,
    variant: &BgenVariant,
) -> Result<Vec<u64>> {
    let capacity = usize::try_from(count)
        .map_err(|_| execution_error(path, variant, "probability count does not fit usize"))?;
    let mut values = Vec::with_capacity(capacity);
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
    Ok(values)
}

struct ProbabilityContext<'a> {
    path: &'a str,
    variant: &'a BgenVariant,
    sample: usize,
}

fn reconstruct_probabilities(
    stored: &[u64],
    denominator: u64,
    ploidy: u8,
    allele_count: usize,
    phased: bool,
    context: ProbabilityContext<'_>,
) -> Result<Vec<u64>> {
    if phased {
        let stored_per_haplotype = allele_count - 1;
        if stored_per_haplotype == 0 {
            return Ok(vec![denominator; ploidy as usize]);
        }
        let mut complete = Vec::with_capacity(ploidy as usize * allele_count);
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
        Ok(complete)
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
        let mut complete = Vec::with_capacity(expected as usize);
        complete.extend_from_slice(stored);
        complete.push(denominator - sum);
        Ok(complete)
    }
}

fn decompress_zlib(
    path: &str,
    variant: &BgenVariant,
    compressed: &[u8],
    expected: usize,
    options: &BgenReadOptions,
) -> Result<Vec<u8>> {
    let decoder = flate2::read::ZlibDecoder::new(compressed);
    read_decompressed(path, variant, decoder, expected, options)
}

fn decompress_zstd(
    path: &str,
    variant: &BgenVariant,
    compressed: &[u8],
    expected: usize,
    options: &BgenReadOptions,
) -> Result<Vec<u8>> {
    let decoder = zstd::stream::read::Decoder::new(compressed)
        .map_err(|error| execution_error(path, variant, &format!("open zstd decoder: {error}")))?;
    read_decompressed(path, variant, decoder, expected, options)
}

fn read_decompressed(
    path: &str,
    variant: &BgenVariant,
    decoder: impl Read,
    expected: usize,
    options: &BgenReadOptions,
) -> Result<Vec<u8>> {
    if expected > options.max_decompressed_block_bytes {
        return Err(execution_error(
            path,
            variant,
            "decompressed block exceeds configured limit",
        ));
    }
    let mut output = Vec::with_capacity(expected);
    decoder
        .take(options.max_decompressed_block_bytes as u64 + 1)
        .read_to_end(&mut output)
        .map_err(|error| {
            execution_error(
                path,
                variant,
                &format!("decompress probability block: {error}"),
            )
        })?;
    if output.len() != expected {
        return Err(execution_error(
            path,
            variant,
            &format!(
                "decompressed probability length {}, expected {expected}",
                output.len()
            ),
        ));
    }
    Ok(output)
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

    #[test]
    fn reads_little_endian_unaligned_bits() {
        let values = read_probability_integers(
            &[0b1101_0011, 0b0000_0010],
            2,
            3,
            3,
            "fixture",
            &BgenVariant {
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
            },
        )
        .unwrap();
        assert_eq!(values, vec![4, 6, 2]);
    }

    #[test]
    fn reads_every_supported_bit_precision() {
        let variant = BgenVariant {
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
        };
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
            assert_eq!(
                read_probability_integers(
                    &bytes,
                    start_bit,
                    expected.len() as u64,
                    bits,
                    "fixture",
                    &variant,
                )
                .unwrap(),
                expected,
                "bit precision {bits}"
            );
        }
    }
}
