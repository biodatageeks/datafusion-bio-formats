use datafusion::common::{DataFusionError, Result};

use crate::fileset::{PgenMode, read_varint};

const DOSAGE_SCALE: f32 = 16_384.0;

#[derive(Clone, Debug)]
pub(crate) struct DecodedRecord {
    pub(crate) gt: Vec<Option<[u16; 2]>>,
    pub(crate) phased: Vec<Option<bool>>,
    pub(crate) ds: Vec<Option<f32>>,
    pub(crate) ds_stored: Vec<Option<f32>>,
    pub(crate) hds: Vec<Option<[f32; 2]>>,
}

/// Logical genotype children that a scan will retain in its output rows.
///
/// Physical tracks are still parsed and validated when their child is absent,
/// but the decoder does not allocate or retain unprojected logical vectors.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct GenotypeProjection {
    gt: bool,
    phased: bool,
    ds: bool,
    ds_stored: bool,
    hds: bool,
}

impl GenotypeProjection {
    pub(crate) fn gt_only() -> Self {
        Self {
            gt: true,
            ..Self::default()
        }
    }

    pub(crate) fn from_fields(fields: &[String]) -> Self {
        let mut projection = Self::default();
        for field in fields {
            match field.as_str() {
                "GT" => projection.gt = true,
                "PHASED" => projection.phased = true,
                "DS" => projection.ds = true,
                "DS_STORED" => projection.ds_stored = true,
                "HDS" => projection.hds = true,
                _ => {}
            }
        }
        projection
    }

    #[cfg(test)]
    fn all() -> Self {
        Self {
            gt: true,
            phased: true,
            ds: true,
            ds_stored: true,
            hds: true,
        }
    }
}

struct DosageTrack<'a> {
    encoding: u8,
    sample_count: usize,
    sample_ids: Vec<usize>,
    value_bytes: &'a [u8],
    values: Option<Vec<Option<f32>>>,
}

impl DosageTrack<'_> {
    fn entry_count(&self) -> usize {
        if self.encoding == 2 {
            self.sample_count
        } else {
            self.sample_ids.len()
        }
    }

    fn sample_at(&self, entry: usize) -> usize {
        if self.encoding == 2 {
            entry
        } else {
            self.sample_ids[entry]
        }
    }

    fn stored_at(&self, entry: usize) -> Option<f32> {
        let offset = entry * 2;
        let value = u16::from_le_bytes([self.value_bytes[offset], self.value_bytes[offset + 1]]);
        (value != u16::MAX).then(|| f32::from(value) / DOSAGE_SCALE)
    }

    fn effective_at(&self, entry: usize, calls: &[Option<[u16; 2]>]) -> Option<f32> {
        self.stored_at(entry)
            .or_else(|| calls[self.sample_at(entry)].map(|call| alt1_hardcall_dosage(&call)))
    }
}

/// Reusable state for the projection-specialized biallelic `GT` decoder.
///
/// The source-to-output map is built once per physical partition. Record
/// decoding then reuses both vectors instead of allocating full-cohort phase,
/// dosage, and phased-dosage intermediates for every variant.
pub(crate) struct GtDecodeWorkspace {
    categories: Vec<u8>,
    selected_codes: Vec<u8>,
    source_to_output: Vec<usize>,
    identity_selection: bool,
    retained_main: Vec<u8>,
    retained_main_valid: bool,
}

impl GtDecodeWorkspace {
    pub(crate) fn new(sample_count: usize, selected_samples: &[usize]) -> Result<Self> {
        let mut source_to_output = vec![usize::MAX; sample_count];
        for (output, &source) in selected_samples.iter().enumerate() {
            let slot = source_to_output.get_mut(source).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "selected sample index {source} is out of bounds for {sample_count} samples"
                ))
            })?;
            if *slot != usize::MAX {
                return Err(DataFusionError::Execution(format!(
                    "selected sample index {source} appears more than once"
                )));
            }
            *slot = output;
        }
        Ok(Self {
            categories: Vec::with_capacity(sample_count),
            selected_codes: Vec::with_capacity(selected_samples.len()),
            source_to_output,
            identity_selection: selected_samples.len() == sample_count
                && selected_samples
                    .iter()
                    .enumerate()
                    .all(|(index, &sample)| index == sample),
            retained_main: Vec::with_capacity(sample_count),
            retained_main_valid: false,
        })
    }

    pub(crate) fn selected_codes(&self) -> &[u8] {
        if self.identity_selection {
            &self.categories
        } else {
            &self.selected_codes
        }
    }

    pub(crate) fn has_identity_selection(&self) -> bool {
        self.identity_selection
    }

    pub(crate) fn swap_main_track(&mut self, track: &mut Vec<u8>) {
        if self.retained_main_valid {
            std::mem::swap(&mut self.retained_main, track);
            self.retained_main_valid = false;
        } else {
            std::mem::swap(&mut self.categories, track);
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_dense_biallelic_gt<F>(
    bytes: &[u8],
    mode: PgenMode,
    record_type: u8,
    variant_index: usize,
    sample_count: usize,
    mut emit: F,
) -> Result<()>
where
    F: FnMut(&[u16], u8, usize),
{
    if !supports_biallelic_gt_fast_path(record_type, 2)
        || mode != PgenMode::Plink1 && record_type & 7 != 0
    {
        return Err(DataFusionError::Execution(format!(
            "PGEN variant {variant_index} is not eligible for direct dense GT decoding"
        )));
    }

    let mut cursor = Cursor::new(bytes, variant_index);
    let packed = cursor.take(sample_count.div_ceil(4), "dense hardcalls")?;
    validate_packed_padding(packed, sample_count, 2, &cursor)?;
    if mode == PgenMode::Plink1 {
        for sample in 0..sample_count {
            let code = match packed_two_bit(packed, sample) {
                0 => 2,
                1 => 3,
                2 => 1,
                3 => 0,
                _ => unreachable!(),
            };
            emit_gt(code, &mut emit);
        }
    } else if record_type == 0xff || record_type & 0x10 == 0 {
        emit_dense_quads(packed, sample_count, |_| 0, &mut emit);
    } else {
        let heterozygous_count = packed
            .iter()
            .map(|&byte| usize::from(HETEROZYGOUS_PER_BYTE[usize::from(byte)]))
            .sum::<usize>();
        if heterozygous_count == 0 {
            return Err(cursor
                .error("hardcall-phase track is present without heterozygous calls".to_string()));
        }
        let first = *cursor
            .bytes
            .get(cursor.position)
            .ok_or_else(|| cursor.error("truncated hardcall-phase track".to_string()))?;
        let explicit_present = first & 1 != 0;
        if explicit_present {
            let present = cursor.take(
                (heterozygous_count + 1).div_ceil(8),
                "phase-present bitarray",
            )?;
            validate_phase_padding(present, heterozygous_count + 1, &cursor)?;
            let phased_count = (0..heterozygous_count)
                .filter(|&index| bit(present, index + 1))
                .count();
            let info = cursor.take(phased_count.div_ceil(8), "phase-info bitarray")?;
            validate_packed_padding(info, phased_count, 1, &cursor)?;
            let mut heterozygous_index = 0;
            let mut info_index = 0;
            for sample in 0..sample_count {
                let mut code = packed_two_bit(packed, sample);
                if code == 1 {
                    if bit(present, heterozygous_index + 1) {
                        if bit(info, info_index) {
                            code = 4;
                        }
                        info_index += 1;
                    }
                    heterozygous_index += 1;
                }
                emit_gt(code, &mut emit);
            }
        } else {
            let info = cursor.take(
                (heterozygous_count + 1).div_ceil(8),
                "implicit phase-info bitarray",
            )?;
            if info.first().is_some_and(|byte| byte & 1 != 0) {
                return Err(
                    cursor.error("implicit phase track has phase-present marker set".to_string())
                );
            }
            validate_phase_padding(info, heterozygous_count + 1, &cursor)?;
            let mut phase_offset = 1;
            emit_dense_quads(
                packed,
                sample_count,
                |byte| {
                    let count = usize::from(HETEROZYGOUS_PER_BYTE[usize::from(byte)]);
                    let pattern = read_packed_bits(info, phase_offset, count);
                    phase_offset += count;
                    pattern
                },
                &mut emit,
            );
        }
    }

    if !cursor.is_finished() {
        return Err(cursor.error(format!(
            "{} trailing bytes remain after direct dense GT decoding",
            cursor.remaining()
        )));
    }
    Ok(())
}

fn emit_dense_quads<F, P>(packed: &[u8], sample_count: usize, mut phase_pattern: P, emit: &mut F)
where
    F: FnMut(&[u16], u8, usize),
    P: FnMut(u8) -> u8,
{
    let full_bytes = sample_count / 4;
    for &byte in &packed[..full_bytes] {
        let decoded = &DENSE_QUADS[usize::from(byte)][usize::from(phase_pattern(byte))];
        emit(&decoded.alleles, decoded.validity, 4);
    }
    if !sample_count.is_multiple_of(4) {
        let byte = packed[full_bytes];
        let decoded = &DENSE_QUADS[usize::from(byte)][usize::from(phase_pattern(byte))];
        for sample in 0..sample_count % 4 {
            emit(
                &decoded.alleles[sample * 2..sample * 2 + 2],
                (decoded.validity >> sample) & 1,
                1,
            );
        }
    }
}

#[inline]
fn read_packed_bits(bytes: &[u8], offset: usize, count: usize) -> u8 {
    if count == 0 {
        return 0;
    }
    let byte = offset / 8;
    let shift = offset % 8;
    let mut value = bytes[byte] >> shift;
    if shift + count > 8 {
        value |= bytes[byte + 1] << (8 - shift);
    }
    value & ((1 << count) - 1)
}

#[derive(Clone, Copy)]
struct DecodedQuad {
    alleles: [u16; 8],
    validity: u8,
}

static DENSE_QUADS: [[DecodedQuad; 16]; 256] = build_dense_quads();

const fn build_dense_quads() -> [[DecodedQuad; 16]; 256] {
    const EMPTY: DecodedQuad = DecodedQuad {
        alleles: [0; 8],
        validity: 0,
    };
    let mut output = [[EMPTY; 16]; 256];
    let mut byte = 0;
    while byte < 256 {
        let mut pattern = 0;
        while pattern < 16 {
            let mut heterozygous_index = 0;
            let mut sample = 0;
            while sample < 4 {
                let code = (byte >> (sample * 2)) & 3;
                let allele_offset = sample * 2;
                if code == 0 {
                    output[byte][pattern].validity |= 1 << sample;
                } else if code == 1 {
                    output[byte][pattern].validity |= 1 << sample;
                    if pattern & (1 << heterozygous_index) == 0 {
                        output[byte][pattern].alleles[allele_offset + 1] = 1;
                    } else {
                        output[byte][pattern].alleles[allele_offset] = 1;
                    }
                    heterozygous_index += 1;
                } else if code == 2 {
                    output[byte][pattern].validity |= 1 << sample;
                    output[byte][pattern].alleles[allele_offset] = 1;
                    output[byte][pattern].alleles[allele_offset + 1] = 1;
                }
                sample += 1;
            }
            pattern += 1;
        }
        byte += 1;
    }
    output
}

#[inline]
fn emit_gt<F: FnMut(&[u16], u8, usize)>(code: u8, emit: &mut F) {
    const ALLELES: [[u16; 2]; 5] = [[0, 0], [0, 1], [1, 1], [0, 0], [1, 0]];
    let alleles = ALLELES[usize::from(code)];
    emit(&alleles, u8::from(code != 3), 1);
}

const HETEROZYGOUS_PER_BYTE: [u8; 256] = build_heterozygous_per_byte();

const fn build_heterozygous_per_byte() -> [u8; 256] {
    let mut output = [0_u8; 256];
    let mut byte = 0;
    while byte < 256 {
        let mut count = 0;
        let mut shift = 0;
        while shift < 8 {
            if (byte >> shift) & 3 == 1 {
                count += 1;
            }
            shift += 2;
        }
        output[byte] = count;
        byte += 1;
    }
    output
}

#[inline]
fn packed_two_bit(bytes: &[u8], sample: usize) -> u8 {
    (bytes[sample / 4] >> ((sample % 4) * 2)) & 3
}

pub(crate) fn supports_biallelic_gt_fast_path(record_type: u8, allele_count: usize) -> bool {
    allele_count == 2
        && (record_type == 0xff
            || record_type & 0x08 == 0 && (record_type >> 5) & 3 == 0 && record_type & 0x80 == 0)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_biallelic_gt_into(
    workspace: &mut GtDecodeWorkspace,
    bytes: &[u8],
    mode: PgenMode,
    record_type: u8,
    variant_index: usize,
    sample_count: usize,
    selected_samples: &[usize],
    ld_base: Option<&[u8]>,
    retain_main: bool,
) -> Result<()> {
    if !supports_biallelic_gt_fast_path(record_type, 2) {
        return Err(DataFusionError::Execution(format!(
            "PGEN variant {variant_index} is not eligible for the biallelic GT fast path"
        )));
    }
    let mut cursor = Cursor::new(bytes, variant_index);
    decode_main_into(
        &mut cursor,
        mode,
        record_type,
        sample_count,
        ld_base,
        &mut workspace.categories,
    )?;

    workspace.retained_main_valid = false;
    if !workspace.identity_selection {
        workspace.selected_codes.clear();
        for &sample in selected_samples {
            let category = *workspace.categories.get(sample).ok_or_else(|| {
                cursor.error(format!(
                    "selected sample index {sample} is out of bounds for {sample_count} samples"
                ))
            })?;
            workspace.selected_codes.push(category);
        }
    }

    if record_type != 0xff && record_type & 0x10 != 0 {
        if retain_main && workspace.identity_selection {
            workspace.retained_main.clear();
            workspace
                .retained_main
                .extend_from_slice(&workspace.categories);
            workspace.retained_main_valid = true;
        }
        decode_phase_for_selected_gt(
            &mut cursor,
            &mut workspace.categories,
            &workspace.source_to_output,
            &mut workspace.selected_codes,
            workspace.identity_selection,
        )?;
    }

    if !cursor.is_finished() {
        return Err(cursor.error(format!(
            "{} trailing bytes remain after decoding the declared GT tracks",
            cursor.remaining()
        )));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_record(
    bytes: &[u8],
    mode: PgenMode,
    record_type: u8,
    variant_index: usize,
    sample_count: usize,
    allele_count: usize,
    selected_samples: &[usize],
    ld_base: Option<&[u8]>,
) -> Result<DecodedRecord> {
    decode_record_and_main(
        bytes,
        mode,
        record_type,
        variant_index,
        sample_count,
        allele_count,
        GenotypeProjection::all(),
        selected_samples,
        ld_base,
    )
    .map(|(decoded, _)| decoded)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_record_and_main(
    bytes: &[u8],
    mode: PgenMode,
    record_type: u8,
    variant_index: usize,
    sample_count: usize,
    allele_count: usize,
    projection: GenotypeProjection,
    selected_samples: &[usize],
    ld_base: Option<&[u8]>,
) -> Result<(DecodedRecord, Vec<u8>)> {
    let mut cursor = Cursor::new(bytes, variant_index);
    for &sample in selected_samples {
        if sample >= sample_count {
            return Err(cursor.error(format!(
                "selected sample index {sample} is out of bounds for {sample_count} samples"
            )));
        }
    }
    let categories = decode_main(&mut cursor, mode, record_type, sample_count, ld_base)?;
    let mut calls = categories
        .iter()
        .map(|&category| category_call(category))
        .collect::<Vec<_>>();

    if record_type != 0xff && record_type & 0x08 != 0 {
        apply_multiallelic_patches(
            &mut cursor,
            &categories,
            &mut calls,
            sample_count,
            allele_count,
        )?;
    }

    let mut phased = (projection.phased || projection.hds).then(|| vec![None; sample_count]);
    if record_type != 0xff && record_type & 0x10 != 0 {
        decode_phase(
            &mut cursor,
            &mut calls,
            phased.as_deref_mut(),
            projection.gt || projection.hds,
        )?;
    } else if let Some(phased) = &mut phased {
        for (call, output) in calls.iter().zip(phased) {
            if call.is_some() {
                *output = Some(false);
            }
        }
    }

    let dosage_encoding = if record_type == 0xff {
        0
    } else {
        (record_type >> 5) & 3
    };
    if allele_count > 2 && dosage_encoding != 0 {
        return Err(cursor.error(
            "unsupported multiallelic PGEN dosage track; hardcalls remain supported".to_string(),
        ));
    }
    let has_hds_track = record_type != 0xff && record_type & 0x80 != 0;
    let materialize_stored = projection.ds || projection.ds_stored || projection.hds;
    let mut dosage_track = decode_dosage(
        &mut cursor,
        dosage_encoding,
        sample_count,
        &calls,
        materialize_stored,
    )?;
    let mut dosages = if projection.ds || projection.hds {
        Some(if projection.ds_stored {
            dosage_track
                .values
                .clone()
                .unwrap_or_else(|| vec![None; sample_count])
        } else {
            dosage_track
                .values
                .take()
                .unwrap_or_else(|| vec![None; sample_count])
        })
    } else {
        None
    };
    if let Some(dosages) = &mut dosages {
        for (dosage, call) in dosages.iter_mut().zip(&calls) {
            if dosage.is_none()
                && let Some(call) = call
            {
                *dosage = Some(alt1_hardcall_dosage(call));
            }
        }
    }
    let hds = if has_hds_track {
        decode_phased_dosage(
            &mut cursor,
            &dosage_track,
            dosages.as_deref(),
            &calls,
            phased.as_deref(),
            projection.hds,
        )?
    } else if projection.hds {
        Some(implicit_haplotype_dosages(
            dosages.as_deref().ok_or_else(|| {
                cursor.error("HDS projection has no effective dosage values".to_string())
            })?,
            &calls,
            phased.as_deref().ok_or_else(|| {
                cursor.error("HDS projection has no hardcall phase values".to_string())
            })?,
        ))
    } else {
        None
    };

    if !cursor.is_finished() {
        return Err(cursor.error(format!(
            "{} trailing bytes remain after decoding the declared record tracks",
            cursor.remaining()
        )));
    }
    if categories.len() != sample_count {
        return Err(cursor.error("decoded hardcall count is inconsistent".to_string()));
    }

    let selected_gt = if projection.gt {
        selected_samples
            .iter()
            .map(|&sample| calls[sample])
            .collect()
    } else {
        Vec::new()
    };
    let selected_phased = projection
        .phased
        .then(|| {
            let phased = phased
                .as_deref()
                .ok_or_else(|| cursor.error("PHASED projection has no phase values".to_string()))?;
            Ok::<_, DataFusionError>(
                selected_samples
                    .iter()
                    .map(|&sample| phased[sample])
                    .collect(),
            )
        })
        .transpose()?
        .unwrap_or_default();
    let selected_ds = projection
        .ds
        .then(|| {
            let dosages = dosages.as_deref().ok_or_else(|| {
                cursor.error("DS projection has no effective dosage values".to_string())
            })?;
            Ok::<_, DataFusionError>(
                selected_samples
                    .iter()
                    .map(|&sample| dosages[sample])
                    .collect(),
            )
        })
        .transpose()?
        .unwrap_or_default();
    let selected_ds_stored = if projection.ds_stored {
        selected_samples
            .iter()
            .map(|&sample| {
                dosage_track
                    .values
                    .as_deref()
                    .and_then(|dosages| dosages[sample])
            })
            .collect()
    } else {
        Vec::new()
    };
    let selected_hds = projection
        .hds
        .then(|| {
            let hds = hds.as_deref().ok_or_else(|| {
                cursor.error("HDS projection has no phased dosage values".to_string())
            })?;
            Ok::<_, DataFusionError>(selected_samples.iter().map(|&sample| hds[sample]).collect())
        })
        .transpose()?
        .unwrap_or_default();
    Ok((
        DecodedRecord {
            gt: selected_gt,
            phased: selected_phased,
            ds: selected_ds,
            ds_stored: selected_ds_stored,
            hds: selected_hds,
        },
        categories,
    ))
}

pub(crate) fn decode_main_track(
    bytes: &[u8],
    mode: PgenMode,
    record_type: u8,
    variant_index: usize,
    sample_count: usize,
    ld_base: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let mut cursor = Cursor::new(bytes, variant_index);
    decode_main(&mut cursor, mode, record_type, sample_count, ld_base)
}

fn decode_main(
    cursor: &mut Cursor<'_>,
    mode: PgenMode,
    record_type: u8,
    sample_count: usize,
    ld_base: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(sample_count);
    decode_main_into(
        cursor,
        mode,
        record_type,
        sample_count,
        ld_base,
        &mut output,
    )?;
    Ok(output)
}

fn decode_main_into(
    cursor: &mut Cursor<'_>,
    mode: PgenMode,
    record_type: u8,
    sample_count: usize,
    ld_base: Option<&[u8]>,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    if mode == PgenMode::Plink1 {
        let bytes = cursor.take(sample_count.div_ceil(4), "PLINK 1 hardcalls")?;
        validate_packed_padding(bytes, sample_count, 2, cursor)?;
        unpack_two_bit(bytes, sample_count, output);
        for category in output {
            *category = match *category {
                0 => 2,
                1 => 3,
                2 => 1,
                3 => 0,
                _ => unreachable!(),
            };
        }
        return Ok(());
    }

    match record_type & 7 {
        0 => {
            let bytes = cursor.take(sample_count.div_ceil(4), "dense hardcalls")?;
            validate_packed_padding(bytes, sample_count, 2, cursor)?;
            unpack_two_bit(bytes, sample_count, output);
            Ok(())
        }
        1 => decode_onebit_into(cursor, sample_count, output),
        2 | 3 => {
            let base = ld_base.ok_or_else(|| {
                cursor.error("LD-compressed record was decoded without its base".to_string())
            })?;
            if base.len() != sample_count {
                return Err(cursor.error(format!(
                    "LD base has {} calls; expected {sample_count}",
                    base.len()
                )));
            }
            output.extend_from_slice(base);
            for (sample, value) in decode_difflist(cursor, sample_count, true)? {
                output[sample] = value.ok_or_else(|| {
                    cursor.error("LD difflist omitted a genotype value".to_string())
                })?;
            }
            if record_type & 1 != 0 {
                for category in output {
                    *category = match *category {
                        0 => 2,
                        2 => 0,
                        value => value,
                    };
                }
            }
            Ok(())
        }
        common @ (4 | 6 | 7) => {
            let common = common & 3;
            output.resize(sample_count, common);
            for (sample, value) in decode_difflist(cursor, sample_count, true)? {
                output[sample] = value.ok_or_else(|| {
                    cursor.error("hardcall difflist omitted a genotype value".to_string())
                })?;
            }
            Ok(())
        }
        5 => Err(cursor.error("reserved PGEN main-track representation 5".to_string())),
        _ => unreachable!(),
    }
}

fn decode_onebit_into(
    cursor: &mut Cursor<'_>,
    sample_count: usize,
    output: &mut Vec<u8>,
) -> Result<()> {
    let code = cursor.byte("one-bit common-category code")?;
    let lower = code / 4;
    let delta = code & 3;
    if delta == 0 || lower.checked_add(delta).is_none_or(|value| value > 3) {
        return Err(cursor.error(format!("invalid one-bit common-category code 0x{code:02x}")));
    }
    let bits = cursor.take(sample_count.div_ceil(8), "one-bit hardcall array")?;
    validate_packed_padding(bits, sample_count, 1, cursor)?;
    output.clear();
    output.reserve(sample_count);
    for sample in 0..sample_count {
        output.push(lower + (bit(bits, sample) as u8) * delta);
    }
    for (sample, value) in decode_difflist(cursor, sample_count, true)? {
        output[sample] = value.ok_or_else(|| {
            cursor.error("one-bit exception omitted a genotype value".to_string())
        })?;
    }
    Ok(())
}

#[inline]
fn unpack_two_bit(bytes: &[u8], sample_count: usize, output: &mut Vec<u8>) {
    output.clear();
    output.reserve(sample_count);
    for &byte in bytes {
        output.extend_from_slice(&[byte & 3, (byte >> 2) & 3, (byte >> 4) & 3, (byte >> 6) & 3]);
    }
    output.truncate(sample_count);
}

fn decode_difflist(
    cursor: &mut Cursor<'_>,
    sample_count: usize,
    has_values: bool,
) -> Result<Vec<(usize, Option<u8>)>> {
    let length = usize::try_from(cursor.varint("difflist length")?)
        .map_err(|_| cursor.error("difflist length does not fit usize".to_string()))?;
    if length > sample_count {
        return Err(cursor.error(format!(
            "difflist length {length} exceeds sample count {sample_count}"
        )));
    }
    if length == 0 {
        return Ok(Vec::new());
    }
    let group_count = length.div_ceil(64);
    let id_width = bytes_to_represent(sample_count);
    let group_starts_raw =
        cursor.take(group_count * id_width, "difflist group-start sample IDs")?;
    let group_starts = (0..group_count)
        .map(|group| {
            read_small_le(&group_starts_raw[group * id_width..(group + 1) * id_width]) as usize
        })
        .collect::<Vec<_>>();
    let group_sizes = cursor
        .take(group_count.saturating_sub(1), "difflist group byte sizes")?
        .to_vec();
    let packed_values = if has_values {
        Some(cursor.take(length.div_ceil(4), "difflist genotype values")?)
    } else {
        None
    };
    if let Some(values) = packed_values {
        validate_packed_padding(values, length, 2, cursor)?;
    }

    let mut result = Vec::with_capacity(length);
    let mut previous_global = None;
    for group in 0..group_count {
        let group_len = (length - group * 64).min(64);
        let mut sample = group_starts[group];
        if sample >= sample_count || previous_global.is_some_and(|previous| sample <= previous) {
            return Err(cursor.error(format!(
                "difflist group {group} starts with invalid sample index {sample}"
            )));
        }
        let delta_start = cursor.position;
        for element in 0..group_len {
            if element > 0 {
                let delta =
                    usize::try_from(cursor.varint("difflist sample delta")?).map_err(|_| {
                        cursor.error("difflist sample delta does not fit usize".to_string())
                    })?;
                if delta == 0 {
                    return Err(cursor.error("difflist sample delta is zero".to_string()));
                }
                sample = sample
                    .checked_add(delta)
                    .ok_or_else(|| cursor.error("difflist sample index overflowed".to_string()))?;
                if sample >= sample_count {
                    return Err(cursor.error(format!(
                        "difflist sample index {sample} exceeds sample count {sample_count}"
                    )));
                }
            }
            let value =
                packed_values.map(|values| packed_value(values, group * 64 + element, 2) as u8);
            result.push((sample, value));
            previous_global = Some(sample);
        }
        if group + 1 < group_count {
            let observed = cursor.position - delta_start;
            let declared = usize::from(group_sizes[group]) + 63;
            if observed != declared {
                return Err(cursor.error(format!(
                    "difflist group {group} delta bytes {observed} differ from declared {declared}"
                )));
            }
        }
    }
    Ok(result)
}

fn apply_multiallelic_patches(
    cursor: &mut Cursor<'_>,
    categories: &[u8],
    calls: &mut [Option<[u16; 2]>],
    sample_count: usize,
    allele_count: usize,
) -> Result<()> {
    if !(3..=65_536).contains(&allele_count) {
        return Err(cursor.error(format!(
            "PGEN raw-allele output supports 3..=65536 alleles, observed {allele_count}"
        )));
    }
    let control = cursor.byte("multiallelic patch control")?;
    let category_one = category_samples(categories, 1);
    let category_two = category_samples(categories, 2);
    let patch_one = decode_patch_samples(cursor, control & 0x0f, &category_one, sample_count)?;
    let alt_count = allele_count - 1;
    let width_one = patch_one_width(alt_count);
    let values_one = if width_one == 0 {
        vec![0_u32; patch_one.len()]
    } else {
        decode_packed_values(
            cursor,
            patch_one.len(),
            width_one,
            "category-1 patch alleles",
        )?
    };
    for (sample, value) in patch_one.into_iter().zip(values_one) {
        let allele = usize::try_from(value)
            .ok()
            .and_then(|value| value.checked_add(2))
            .ok_or_else(|| cursor.error("category-1 patch allele overflowed".to_string()))?;
        if allele >= allele_count {
            return Err(cursor.error(format!(
                "category-1 patch references allele {allele} outside {allele_count} alleles for sample {sample}"
            )));
        }
        calls[sample] = Some([0, allele as u16]);
    }

    let patch_two = decode_patch_samples(cursor, control >> 4, &category_two, sample_count)?;
    if alt_count == 2 {
        let values = decode_packed_values(cursor, patch_two.len(), 1, "category-2 patch alleles")?;
        for (sample, value) in patch_two.into_iter().zip(values) {
            calls[sample] = if value == 0 {
                Some([1, 2])
            } else {
                Some([2, 2])
            };
        }
    } else {
        let width = patch_two_width(alt_count);
        let values = decode_packed_values(
            cursor,
            patch_two.len().checked_mul(2).ok_or_else(|| {
                cursor.error("category-2 patch value count overflowed".to_string())
            })?,
            width,
            "category-2 patch alleles",
        )?;
        for (patch_index, sample) in patch_two.into_iter().enumerate() {
            let first = values[patch_index * 2] as usize + 1;
            let second = values[patch_index * 2 + 1] as usize + 1;
            if first > second || second >= allele_count {
                return Err(cursor.error(format!(
                    "category-2 patch has invalid alleles [{first}, {second}] for sample {sample}"
                )));
            }
            calls[sample] = Some([first as u16, second as u16]);
        }
    }
    Ok(())
}

fn category_samples(categories: &[u8], category: u8) -> Vec<usize> {
    categories
        .iter()
        .enumerate()
        .filter_map(|(sample, &value)| (value == category).then_some(sample))
        .collect()
}

fn decode_patch_samples(
    cursor: &mut Cursor<'_>,
    format: u8,
    category_samples: &[usize],
    sample_count: usize,
) -> Result<Vec<usize>> {
    match format {
        0 => {
            let bits = cursor.take(
                category_samples.len().div_ceil(8),
                "multiallelic patch bitarray",
            )?;
            validate_packed_padding(bits, category_samples.len(), 1, cursor)?;
            Ok(category_samples
                .iter()
                .enumerate()
                .filter_map(|(index, &sample)| bit(bits, index).then_some(sample))
                .collect())
        }
        1 => {
            let decoded = decode_difflist(cursor, sample_count, false)?;
            let membership = category_samples
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>();
            let mut samples = Vec::with_capacity(decoded.len());
            for (sample, _) in decoded {
                if !membership.contains(&sample) {
                    return Err(cursor.error(format!(
                        "multiallelic patch addresses sample {sample} outside its genotype category"
                    )));
                }
                samples.push(sample);
            }
            Ok(samples)
        }
        15 => Ok(Vec::new()),
        _ => Err(cursor.error(format!("reserved multiallelic patch-set format {format}"))),
    }
}

fn patch_one_width(alt_count: usize) -> usize {
    match alt_count {
        2 => 0,
        3 => 1,
        4..=5 => 2,
        6..=17 => 4,
        18..=257 => 8,
        _ => 16,
    }
}

fn patch_two_width(alt_count: usize) -> usize {
    match alt_count {
        3..=4 => 2,
        5..=16 => 4,
        17..=256 => 8,
        _ => 16,
    }
}

fn decode_packed_values(
    cursor: &mut Cursor<'_>,
    count: usize,
    width: usize,
    context: &str,
) -> Result<Vec<u32>> {
    let bit_count = count
        .checked_mul(width)
        .ok_or_else(|| cursor.error(format!("{context} bit count overflowed")))?;
    let bytes = cursor.take(bit_count.div_ceil(8), context)?;
    validate_packed_padding(bytes, count, width, cursor)?;
    Ok((0..count)
        .map(|index| packed_value(bytes, index, width))
        .collect())
}

fn decode_phase(
    cursor: &mut Cursor<'_>,
    calls: &mut [Option<[u16; 2]>],
    mut phased: Option<&mut [Option<bool>]>,
    orient_calls: bool,
) -> Result<()> {
    let heterozygous = calls
        .iter()
        .enumerate()
        .filter_map(|(sample, call)| {
            call.filter(|alleles| alleles[0] != alleles[1])
                .map(|_| sample)
        })
        .collect::<Vec<_>>();
    if heterozygous.is_empty() {
        return Err(
            cursor.error("hardcall-phase track is present without heterozygous calls".to_string())
        );
    }
    let phase_bit_count = heterozygous
        .len()
        .checked_add(1)
        .ok_or_else(|| cursor.error("hardcall-phase bit count overflowed".to_string()))?;
    let first = *cursor
        .bytes
        .get(cursor.position)
        .ok_or_else(|| cursor.error("truncated hardcall-phase track".to_string()))?;
    let explicit_present = first & 1 != 0;
    let present = if explicit_present {
        let bytes = cursor.take(phase_bit_count.div_ceil(8), "phase-present bitarray")?;
        validate_phase_padding(bytes, phase_bit_count, cursor)?;
        (0..heterozygous.len())
            .map(|index| bit(bytes, index + 1))
            .collect::<Vec<_>>()
    } else {
        vec![true; heterozygous.len()]
    };
    let phased_count = present.iter().filter(|&&value| value).count();
    let info = if explicit_present {
        let bytes = cursor.take(phased_count.div_ceil(8), "phase-info bitarray")?;
        validate_packed_padding(bytes, phased_count, 1, cursor)?;
        (0..phased_count)
            .map(|index| bit(bytes, index))
            .collect::<Vec<_>>()
    } else {
        let bytes = cursor.take(phase_bit_count.div_ceil(8), "implicit phase-info bitarray")?;
        if bytes.first().is_some_and(|byte| byte & 1 != 0) {
            return Err(
                cursor.error("implicit phase track has phase-present marker set".to_string())
            );
        }
        validate_phase_padding(bytes, phase_bit_count, cursor)?;
        (0..heterozygous.len())
            .map(|index| bit(bytes, index + 1))
            .collect::<Vec<_>>()
    };
    let mut info_index = 0;
    for (heterozygous_index, &sample) in heterozygous.iter().enumerate() {
        if present[heterozygous_index] {
            if info[info_index] && orient_calls {
                let call = calls[sample].as_mut().ok_or_else(|| {
                    cursor.error(format!(
                        "phase information addresses a missing hardcall for sample {sample}"
                    ))
                })?;
                call.swap(0, 1);
            }
            if let Some(output) = phased.as_deref_mut() {
                output[sample] = Some(true);
            }
            info_index += 1;
        } else if let Some(output) = phased.as_deref_mut() {
            output[sample] = Some(false);
        }
    }
    if let Some(phased) = phased {
        for (sample, call) in calls.iter().enumerate() {
            if call.is_some() && phased[sample].is_none() {
                phased[sample] = Some(false);
            }
        }
    }
    Ok(())
}

fn decode_phase_for_selected_gt(
    cursor: &mut Cursor<'_>,
    categories: &mut [u8],
    source_to_output: &[usize],
    selected_codes: &mut [u8],
    identity_selection: bool,
) -> Result<()> {
    // With a subset selection, phase orientation belongs only in selected_codes:
    // categories must remain the raw main track so it can become a later LD base.
    // For an identity selection, retain_main snapshots categories before this call.
    let heterozygous_count = categories.iter().filter(|&&category| category == 1).count();
    if heterozygous_count == 0 {
        return Err(
            cursor.error("hardcall-phase track is present without heterozygous calls".to_string())
        );
    }

    let first = *cursor
        .bytes
        .get(cursor.position)
        .ok_or_else(|| cursor.error("truncated hardcall-phase track".to_string()))?;
    let explicit_present = first & 1 != 0;
    if explicit_present {
        let present = cursor.take(
            (heterozygous_count + 1).div_ceil(8),
            "phase-present bitarray",
        )?;
        validate_phase_padding(present, heterozygous_count + 1, cursor)?;
        let phased_count = (0..heterozygous_count)
            .filter(|&index| bit(present, index + 1))
            .count();
        let info = cursor.take(phased_count.div_ceil(8), "phase-info bitarray")?;
        validate_packed_padding(info, phased_count, 1, cursor)?;

        let mut heterozygous_index = 0;
        let mut info_index = 0;
        for source in 0..categories.len() {
            let category = categories[source];
            if category != 1 {
                continue;
            }
            if bit(present, heterozygous_index + 1) {
                if bit(info, info_index) {
                    orient_selected_call(
                        source,
                        categories,
                        source_to_output,
                        selected_codes,
                        identity_selection,
                        cursor,
                    )?;
                }
                info_index += 1;
            }
            heterozygous_index += 1;
        }
    } else {
        let info = cursor.take(
            (heterozygous_count + 1).div_ceil(8),
            "implicit phase-info bitarray",
        )?;
        if info.first().is_some_and(|byte| byte & 1 != 0) {
            return Err(
                cursor.error("implicit phase track has phase-present marker set".to_string())
            );
        }
        validate_phase_padding(info, heterozygous_count + 1, cursor)?;
        let mut heterozygous_index = 0;
        for source in 0..categories.len() {
            let category = categories[source];
            if category != 1 {
                continue;
            }
            if bit(info, heterozygous_index + 1) {
                orient_selected_call(
                    source,
                    categories,
                    source_to_output,
                    selected_codes,
                    identity_selection,
                    cursor,
                )?;
            }
            heterozygous_index += 1;
        }
    }
    Ok(())
}

fn orient_selected_call(
    source: usize,
    categories: &mut [u8],
    source_to_output: &[usize],
    selected_codes: &mut [u8],
    identity_selection: bool,
    cursor: &Cursor<'_>,
) -> Result<()> {
    if identity_selection {
        categories[source] = 4;
        return Ok(());
    }
    let output = source_to_output[source];
    if output == usize::MAX {
        return Ok(());
    }
    let code = selected_codes.get_mut(output).ok_or_else(|| {
        cursor.error(format!(
            "phase information addresses an invalid selected call for sample {source}"
        ))
    })?;
    *code = 4;
    Ok(())
}

fn decode_dosage<'a>(
    cursor: &mut Cursor<'a>,
    encoding: u8,
    sample_count: usize,
    calls: &[Option<[u16; 2]>],
    materialize: bool,
) -> Result<DosageTrack<'a>> {
    let sample_ids = match encoding {
        0 => Vec::new(),
        1 => decode_difflist(cursor, sample_count, false)?
            .into_iter()
            .map(|(sample, _)| sample)
            .collect(),
        2 => Vec::new(),
        3 => {
            let bits = cursor.take(sample_count.div_ceil(8), "dosage-presence bitarray")?;
            validate_packed_padding(bits, sample_count, 1, cursor)?;
            (0..sample_count)
                .filter(|&sample| bit(bits, sample))
                .collect()
        }
        _ => unreachable!(),
    };
    if encoding == 0 {
        return Ok(DosageTrack {
            encoding,
            sample_count,
            sample_ids,
            value_bytes: &[],
            values: None,
        });
    }
    let entry_count = if encoding == 2 {
        sample_count
    } else {
        sample_ids.len()
    };
    let value_bytes = cursor.take(
        entry_count
            .checked_mul(2)
            .ok_or_else(|| cursor.error("dosage byte count overflowed".to_string()))?,
        "dosage values",
    )?;
    let mut output = materialize.then(|| vec![None; sample_count]);
    for index in 0..entry_count {
        let sample = if encoding == 2 {
            index
        } else {
            sample_ids[index]
        };
        let value = u16::from_le_bytes([value_bytes[index * 2], value_bytes[index * 2 + 1]]);
        if encoding == 2 && value == u16::MAX {
            if calls[sample].is_some() {
                return Err(cursor.error(format!(
                    "fixed-width dosage is missing while the hardcall is present for sample {sample}"
                )));
            }
            continue;
        }
        if value > 32_768 {
            return Err(cursor.error(format!(
                "dosage integer {value} exceeds 32768 for sample {sample}"
            )));
        }
        let dosage = f32::from(value) / DOSAGE_SCALE;
        if let Some(call) = calls[sample] {
            let hardcall = alt1_hardcall_dosage(&call);
            if (hardcall - dosage).abs() > 0.5001 {
                return Err(cursor.error(format!(
                    "dosage {dosage} is inconsistent with hardcall ALT count {hardcall} for sample {sample}"
                )));
            }
        }
        if let Some(output) = &mut output {
            output[sample] = Some(dosage);
        }
    }
    Ok(DosageTrack {
        encoding,
        sample_count,
        sample_ids,
        value_bytes,
        values: output,
    })
}

fn decode_phased_dosage(
    cursor: &mut Cursor<'_>,
    dosage: &DosageTrack<'_>,
    dosages: Option<&[Option<f32>]>,
    calls: &[Option<[u16; 2]>],
    phased: Option<&[Option<bool>]>,
    materialize: bool,
) -> Result<Option<Vec<Option<[f32; 2]>>>> {
    if dosage.encoding == 0 {
        return Err(
            cursor.error("phased-dosage track is present without a dosage track".to_string())
        );
    }
    let entry_count = dosage.entry_count();
    let presence = if dosage.encoding == 2 {
        None
    } else {
        let bits = cursor.take(entry_count.div_ceil(8), "phased-dosage-presence bitarray")?;
        validate_packed_padding(bits, entry_count, 1, cursor)?;
        Some(bits)
    };
    let explicit_count = presence.map_or(entry_count, |bits| {
        (0..entry_count).filter(|&index| bit(bits, index)).count()
    });
    let values = cursor.take(
        explicit_count
            .checked_mul(2)
            .ok_or_else(|| cursor.error("phased dosage byte count overflowed".to_string()))?,
        "phased-dosage values",
    )?;
    let mut output = if materialize {
        Some(implicit_haplotype_dosages(
            dosages.ok_or_else(|| {
                cursor.error("phased dosage output has no total dosage values".to_string())
            })?,
            calls,
            phased.ok_or_else(|| {
                cursor.error("phased dosage output has no hardcall phase values".to_string())
            })?,
        ))
    } else {
        None
    };
    let mut value_index = 0;
    for entry in 0..entry_count {
        if presence.is_some_and(|bits| !bit(bits, entry)) {
            continue;
        }
        let sample = dosage.sample_at(entry);
        let value = i16::from_le_bytes([values[value_index * 2], values[value_index * 2 + 1]]);
        value_index += 1;
        if dosage.encoding == 2 && value == i16::MIN {
            continue;
        }
        if !(-16_384..=16_384).contains(&value) {
            return Err(cursor.error(format!(
                "phased-dosage difference {value} is outside [-16384, 16384] for sample {sample}"
            )));
        }
        let Some(total) = dosages
            .and_then(|values| values[sample])
            .or_else(|| dosage.effective_at(entry, calls))
        else {
            return Err(cursor.error(format!(
                "phased dosage exists without total dosage for sample {sample}"
            )));
        };
        let difference = f32::from(value) / DOSAGE_SCALE;
        let left = (total + difference) * 0.5;
        let right = (total - difference) * 0.5;
        if !(-0.0001..=1.0001).contains(&left) || !(-0.0001..=1.0001).contains(&right) {
            return Err(cursor.error(format!(
                "phased dosage [{left}, {right}] is outside haplotype bounds for sample {sample}"
            )));
        }
        if let Some(output) = &mut output {
            output[sample] = Some([left.clamp(0.0, 1.0), right.clamp(0.0, 1.0)]);
        }
    }
    debug_assert_eq!(value_index, explicit_count);
    Ok(output)
}

fn implicit_haplotype_dosages(
    dosages: &[Option<f32>],
    calls: &[Option<[u16; 2]>],
    phased: &[Option<bool>],
) -> Vec<Option<[f32; 2]>> {
    dosages
        .iter()
        .zip(calls)
        .zip(phased)
        .map(|((dosage, call), phased)| {
            let (Some(total), Some(call), Some(true)) = (dosage, call, phased) else {
                return None;
            };
            if call == &[0, 1] {
                Some(if *total <= 1.0 {
                    [0.0, *total]
                } else {
                    [*total - 1.0, 1.0]
                })
            } else if call == &[1, 0] {
                Some(if *total <= 1.0 {
                    [*total, 0.0]
                } else {
                    [1.0, *total - 1.0]
                })
            } else {
                None
            }
        })
        .collect()
}

fn alt1_hardcall_dosage(call: &[u16; 2]) -> f32 {
    call.iter().filter(|&&allele| allele == 1).count() as f32
}

fn category_call(category: u8) -> Option<[u16; 2]> {
    match category {
        0 => Some([0, 0]),
        1 => Some([0, 1]),
        2 => Some([1, 1]),
        3 => None,
        _ => None,
    }
}

fn validate_phase_padding(bytes: &[u8], bit_count: usize, cursor: &Cursor<'_>) -> Result<()> {
    validate_packed_padding(bytes, bit_count, 1, cursor)
}

fn validate_packed_padding(
    bytes: &[u8],
    value_count: usize,
    width: usize,
    cursor: &Cursor<'_>,
) -> Result<()> {
    let bits = value_count
        .checked_mul(width)
        .ok_or_else(|| cursor.error("packed-array bit count overflowed".to_string()))?;
    let remainder = bits % 8;
    if remainder > 0 && bytes.last().is_some_and(|byte| byte >> remainder != 0) {
        return Err(cursor.error("packed array has nonzero padding bits".to_string()));
    }
    Ok(())
}

fn packed_value(bytes: &[u8], index: usize, width: usize) -> u32 {
    let bit_offset = index * width;
    let byte_offset = bit_offset / 8;
    let shift = bit_offset % 8;
    let mut word = 0_u64;
    for (index, byte) in bytes
        .get(byte_offset..)
        .unwrap_or_default()
        .iter()
        .take(5)
        .enumerate()
    {
        word |= u64::from(*byte) << (index * 8);
    }
    ((word >> shift) & ((1_u64 << width) - 1)) as u32
}

fn bit(bytes: &[u8], index: usize) -> bool {
    bytes[index / 8] & (1 << (index % 8)) != 0
}

fn bytes_to_represent(value_count: usize) -> usize {
    if value_count <= 1 << 8 {
        1
    } else if value_count <= 1 << 16 {
        2
    } else if value_count <= 1 << 24 {
        3
    } else {
        4
    }
}

fn read_small_le(bytes: &[u8]) -> u32 {
    bytes
        .iter()
        .enumerate()
        .fold(0_u32, |value, (shift, byte)| {
            value | (u32::from(*byte) << (shift * 8))
        })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
    variant_index: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8], variant_index: usize) -> Self {
        Self {
            bytes,
            position: 0,
            variant_index,
        }
    }

    fn take(&mut self, length: usize, context: &str) -> Result<&'a [u8]> {
        let start = self.position;
        let end = start
            .checked_add(length)
            .ok_or_else(|| self.error(format!("{context} length overflowed")))?;
        let result = self.bytes.get(start..end).ok_or_else(|| {
            self.error(format!(
                "truncated {context} at record byte {start}; need {length} bytes"
            ))
        })?;
        self.position = end;
        Ok(result)
    }

    fn byte(&mut self, context: &str) -> Result<u8> {
        Ok(self.take(1, context)?[0])
    }

    fn varint(&mut self, context: &str) -> Result<u64> {
        read_varint(self.bytes, &mut self.position)
            .map_err(|error| self.error(format!("{context}: {error}")))
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn is_finished(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn error(&self, detail: String) -> DataFusionError {
        DataFusionError::Execution(format!(
            "PGEN variant {} at record byte {}: {detail}",
            self.variant_index, self.position
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_varint(mut value: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            bytes.push(byte);
            if value == 0 {
                return bytes;
            }
        }
    }

    #[test]
    fn decodes_dense_hardcalls() {
        let record = decode_record(
            &[0b11_10_01_00],
            PgenMode::Variable,
            0,
            0,
            4,
            2,
            &[3, 1, 0],
            None,
        )
        .unwrap();
        assert_eq!(record.gt, vec![None, Some([0, 1]), Some([0, 0])]);
    }

    #[test]
    fn retains_only_projected_genotype_children() {
        let fields = vec!["PHASED".to_string()];
        let (record, _) = decode_record_and_main(
            &[0b11_10_01_00],
            PgenMode::Variable,
            0,
            0,
            4,
            2,
            GenotypeProjection::from_fields(&fields),
            &[0, 1, 2, 3],
            None,
        )
        .unwrap();
        assert!(record.gt.is_empty());
        assert_eq!(
            record.phased,
            vec![Some(false), Some(false), Some(false), None]
        );
        assert!(record.ds.is_empty());
        assert!(record.ds_stored.is_empty());
        assert!(record.hds.is_empty());

        let mut physical_hds = vec![0b11_10_01_00];
        for dosage in [0_u16, 16_384, 32_768, u16::MAX] {
            physical_hds.extend_from_slice(&dosage.to_le_bytes());
        }
        for difference in [0_i16, 0, 0, i16::MIN] {
            physical_hds.extend_from_slice(&difference.to_le_bytes());
        }
        let (record, _) = decode_record_and_main(
            &physical_hds,
            PgenMode::Variable,
            0xc0,
            0,
            4,
            2,
            GenotypeProjection::from_fields(&fields),
            &[0, 1, 2, 3],
            None,
        )
        .unwrap();
        assert_eq!(
            record.phased,
            vec![Some(false), Some(false), Some(false), None]
        );
        assert!(record.ds.is_empty());
        assert!(record.ds_stored.is_empty());
        assert!(record.hds.is_empty());
    }

    #[test]
    fn decodes_onebit_and_exception_difflist() {
        let mut bytes = vec![2, 0b0000_1010];
        bytes.extend(encode_varint(1));
        bytes.push(2);
        bytes.push(1);
        let record =
            decode_record(&bytes, PgenMode::Variable, 1, 0, 4, 2, &[0, 1, 2, 3], None).unwrap();
        assert_eq!(
            record.gt,
            vec![Some([0, 0]), Some([1, 1]), Some([0, 1]), Some([1, 1])]
        );
    }

    #[test]
    fn rejects_zero_difflist_delta() {
        let mut cursor = Cursor::new(&[2, 0, 0, 0], 7);
        let error = decode_difflist(&mut cursor, 8, false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("delta is zero"), "{error}");
    }

    #[test]
    fn decodes_difflist_group_boundary() {
        let mut bytes = encode_varint(65);
        bytes.extend([0, 64]);
        bytes.push(0);
        bytes.extend(pack_two_bit_values(&[2; 65]));
        bytes.extend(vec![1; 63]);
        let mut cursor = Cursor::new(&bytes, 0);
        let decoded = decode_difflist(&mut cursor, 130, true).unwrap();
        assert_eq!(decoded.len(), 65);
        assert_eq!(decoded.first(), Some(&(0, Some(2))));
        assert_eq!(decoded.last(), Some(&(64, Some(2))));
        assert!(cursor.is_finished());
    }

    #[test]
    fn rejects_overlapping_difflist_groups() {
        let mut bytes = encode_varint(65);
        bytes.extend([0, 63]);
        bytes.push(0);
        bytes.extend(pack_two_bit_values(&[0; 65]));
        bytes.extend(vec![1; 63]);
        let mut cursor = Cursor::new(&bytes, 3);
        let error = decode_difflist(&mut cursor, 130, true)
            .unwrap_err()
            .to_string();
        assert!(error.contains("invalid sample index 63"), "{error}");
    }

    fn pack_two_bit_values(values: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0; values.len().div_ceil(4)];
        for (index, value) in values.iter().copied().enumerate() {
            bytes[index / 4] |= value << ((index % 4) * 2);
        }
        bytes
    }

    fn decode_direct_dense(
        bytes: &[u8],
        mode: PgenMode,
        record_type: u8,
        sample_count: usize,
    ) -> Vec<Option<[u16; 2]>> {
        let mut output = Vec::new();
        decode_dense_biallelic_gt(
            bytes,
            mode,
            record_type,
            0,
            sample_count,
            |alleles, validity, samples| {
                for sample in 0..samples {
                    output.push(
                        (validity & (1 << sample) != 0)
                            .then(|| [alleles[sample * 2], alleles[sample * 2 + 1]]),
                    );
                }
            },
        )
        .unwrap();
        output
    }

    #[test]
    fn direct_dense_decodes_implicit_phase_in_quad_chunks() {
        assert_eq!(
            decode_direct_dense(&[0b11_00_01_01, 0b0000_0100], PgenMode::Variable, 0x10, 4,),
            vec![Some([0, 1]), Some([1, 0]), Some([0, 0]), None]
        );
    }

    #[test]
    fn direct_dense_handles_partial_final_byte() {
        assert_eq!(
            decode_direct_dense(&[0b00_11_01_00, 0b0000_0010], PgenMode::Variable, 0x10, 3,),
            vec![Some([0, 0]), Some([1, 0]), None]
        );
    }

    #[test]
    fn direct_dense_preserves_unphased_and_missing_calls() {
        assert_eq!(
            decode_direct_dense(&[0b11_10_01_00], PgenMode::Variable, 0, 4,),
            vec![Some([0, 0]), Some([0, 1]), Some([1, 1]), None]
        );
    }
}
