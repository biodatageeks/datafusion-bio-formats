use datafusion::common::{DataFusionError, Result};

use crate::fileset::{PgenMode, read_varint};

const DOSAGE_SCALE: f32 = 16_384.0;

#[derive(Clone, Debug)]
pub(crate) struct DecodedRecord {
    pub(crate) gt: Vec<Option<[u16; 2]>>,
    pub(crate) phased: Vec<Option<bool>>,
    pub(crate) ds: Vec<Option<f32>>,
    pub(crate) hds: Vec<Option<[f32; 2]>>,
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
    selected_samples: &[usize],
    ld_base: Option<&[u8]>,
) -> Result<(DecodedRecord, Vec<u8>)> {
    let mut cursor = Cursor::new(bytes, variant_index);
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
    } else if allele_count > 2 {
        return Err(cursor.error(format!(
            "multiallelic PVAR row has {allele_count} alleles but its PGEN record has no hardcall patch track"
        )));
    }

    let mut phased = vec![None; sample_count];
    if record_type != 0xff && record_type & 0x10 != 0 {
        decode_phase(&mut cursor, &mut calls, &mut phased)?;
    } else {
        for (call, output) in calls.iter().zip(&mut phased) {
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
    let (mut dosages, dosage_sample_ids) =
        decode_dosage(&mut cursor, dosage_encoding, sample_count)?;
    if dosage_encoding != 0 {
        for (dosage, call) in dosages.iter_mut().zip(&calls) {
            if dosage.is_none()
                && let Some(call) = call
            {
                *dosage = Some((u16::from(call[0] > 0) + u16::from(call[1] > 0)) as f32);
            }
        }
    }
    validate_dosage_hardcall_consistency(&calls, &dosages, &cursor)?;
    let hds = if record_type != 0xff && record_type & 0x80 != 0 {
        decode_phased_dosage(
            &mut cursor,
            dosage_encoding,
            &dosage_sample_ids,
            &dosages,
            &calls,
            &phased,
        )?
    } else {
        implicit_haplotype_dosages(&dosages, &calls, &phased)
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

    let mut selected_gt = Vec::with_capacity(selected_samples.len());
    let mut selected_phased = Vec::with_capacity(selected_samples.len());
    let mut selected_ds = Vec::with_capacity(selected_samples.len());
    let mut selected_hds = Vec::with_capacity(selected_samples.len());
    for &sample in selected_samples {
        if sample >= sample_count {
            return Err(cursor.error(format!(
                "selected sample index {sample} is out of bounds for {sample_count} samples"
            )));
        }
        selected_gt.push(calls[sample]);
        selected_phased.push(phased[sample]);
        selected_ds.push(dosages[sample]);
        selected_hds.push(hds[sample]);
    }
    dosages.clear();
    Ok((
        DecodedRecord {
            gt: selected_gt,
            phased: selected_phased,
            ds: selected_ds,
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
    if mode == PgenMode::Plink1 {
        let bytes = cursor.take(sample_count.div_ceil(4), "PLINK 1 hardcalls")?;
        validate_packed_padding(bytes, sample_count, 2, cursor)?;
        return Ok((0..sample_count)
            .map(|sample| match packed_value(bytes, sample, 2) as u8 {
                0 => 2,
                1 => 3,
                2 => 1,
                3 => 0,
                _ => unreachable!(),
            })
            .collect());
    }

    match record_type & 7 {
        0 => {
            let bytes = cursor.take(sample_count.div_ceil(4), "dense hardcalls")?;
            validate_packed_padding(bytes, sample_count, 2, cursor)?;
            Ok((0..sample_count)
                .map(|sample| packed_value(bytes, sample, 2) as u8)
                .collect())
        }
        1 => decode_onebit(cursor, sample_count),
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
            let mut result = base.to_vec();
            for (sample, value) in decode_difflist(cursor, sample_count, true)? {
                result[sample] = value.ok_or_else(|| {
                    cursor.error("LD difflist omitted a genotype value".to_string())
                })?;
            }
            if record_type & 1 != 0 {
                for category in &mut result {
                    *category = match *category {
                        0 => 2,
                        2 => 0,
                        value => value,
                    };
                }
            }
            Ok(result)
        }
        common @ (4 | 6 | 7) => {
            let common = common & 3;
            let mut result = vec![common; sample_count];
            for (sample, value) in decode_difflist(cursor, sample_count, true)? {
                result[sample] = value.ok_or_else(|| {
                    cursor.error("hardcall difflist omitted a genotype value".to_string())
                })?;
            }
            Ok(result)
        }
        5 => Err(cursor.error("reserved PGEN main-track representation 5".to_string())),
        _ => unreachable!(),
    }
}

fn decode_onebit(cursor: &mut Cursor<'_>, sample_count: usize) -> Result<Vec<u8>> {
    let code = cursor.byte("one-bit common-category code")?;
    let lower = code / 4;
    let delta = code & 3;
    if delta == 0 || lower.checked_add(delta).is_none_or(|value| value > 3) {
        return Err(cursor.error(format!("invalid one-bit common-category code 0x{code:02x}")));
    }
    let bits = cursor.take(sample_count.div_ceil(8), "one-bit hardcall array")?;
    validate_packed_padding(bits, sample_count, 1, cursor)?;
    let mut result = (0..sample_count)
        .map(|sample| lower + (bit(bits, sample) as u8) * delta)
        .collect::<Vec<_>>();
    for (sample, value) in decode_difflist(cursor, sample_count, true)? {
        result[sample] = value.ok_or_else(|| {
            cursor.error("one-bit exception omitted a genotype value".to_string())
        })?;
    }
    Ok(result)
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
    phased: &mut [Option<bool>],
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
    let first = *cursor
        .bytes
        .get(cursor.position)
        .ok_or_else(|| cursor.error("truncated hardcall-phase track".to_string()))?;
    let explicit_present = first & 1 != 0;
    let present = if explicit_present {
        let bytes = cursor.take(
            (heterozygous.len() + 1).div_ceil(8),
            "phase-present bitarray",
        )?;
        validate_phase_padding(bytes, heterozygous.len() + 1, cursor)?;
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
        let bytes = cursor.take(
            (heterozygous.len() + 1).div_ceil(8),
            "implicit phase-info bitarray",
        )?;
        if bytes.first().is_some_and(|byte| byte & 1 != 0) {
            return Err(
                cursor.error("implicit phase track has phase-present marker set".to_string())
            );
        }
        validate_phase_padding(bytes, heterozygous.len() + 1, cursor)?;
        (0..heterozygous.len())
            .map(|index| bit(bytes, index + 1))
            .collect::<Vec<_>>()
    };
    let mut info_index = 0;
    for (heterozygous_index, &sample) in heterozygous.iter().enumerate() {
        if present[heterozygous_index] {
            if info[info_index] {
                calls[sample].as_mut().unwrap().swap(0, 1);
            }
            phased[sample] = Some(true);
            info_index += 1;
        } else {
            phased[sample] = Some(false);
        }
    }
    for (sample, call) in calls.iter().enumerate() {
        if call.is_some() && phased[sample].is_none() {
            phased[sample] = Some(false);
        }
    }
    Ok(())
}

fn decode_dosage(
    cursor: &mut Cursor<'_>,
    encoding: u8,
    sample_count: usize,
) -> Result<(Vec<Option<f32>>, Vec<usize>)> {
    let sample_ids = match encoding {
        0 => Vec::new(),
        1 => decode_difflist(cursor, sample_count, false)?
            .into_iter()
            .map(|(sample, _)| sample)
            .collect(),
        2 => (0..sample_count).collect(),
        3 => {
            let bits = cursor.take(sample_count.div_ceil(8), "dosage-presence bitarray")?;
            validate_packed_padding(bits, sample_count, 1, cursor)?;
            (0..sample_count)
                .filter(|&sample| bit(bits, sample))
                .collect()
        }
        _ => unreachable!(),
    };
    let value_bytes = cursor.take(
        sample_ids
            .len()
            .checked_mul(2)
            .ok_or_else(|| cursor.error("dosage byte count overflowed".to_string()))?,
        "dosage values",
    )?;
    let mut output = vec![None; sample_count];
    for (index, &sample) in sample_ids.iter().enumerate() {
        let value = u16::from_le_bytes([value_bytes[index * 2], value_bytes[index * 2 + 1]]);
        if encoding == 2 && value == u16::MAX {
            continue;
        }
        if value > 32_768 {
            return Err(cursor.error(format!(
                "dosage integer {value} exceeds 32768 for sample {sample}"
            )));
        }
        output[sample] = Some(f32::from(value) / DOSAGE_SCALE);
    }
    Ok((output, sample_ids))
}

fn validate_dosage_hardcall_consistency(
    calls: &[Option<[u16; 2]>],
    dosages: &[Option<f32>],
    cursor: &Cursor<'_>,
) -> Result<()> {
    for (sample, (call, dosage)) in calls.iter().zip(dosages).enumerate() {
        let (Some(call), Some(dosage)) = (call, dosage) else {
            continue;
        };
        let hardcall = (u16::from(call[0] > 0) + u16::from(call[1] > 0)) as f32;
        if (hardcall - dosage).abs() > 0.5001 {
            return Err(cursor.error(format!(
                "dosage {dosage} is inconsistent with hardcall ALT count {hardcall} for sample {sample}"
            )));
        }
    }
    Ok(())
}

fn decode_phased_dosage(
    cursor: &mut Cursor<'_>,
    dosage_encoding: u8,
    dosage_sample_ids: &[usize],
    dosages: &[Option<f32>],
    calls: &[Option<[u16; 2]>],
    phased: &[Option<bool>],
) -> Result<Vec<Option<[f32; 2]>>> {
    if dosage_encoding == 0 {
        return Err(
            cursor.error("phased-dosage track is present without a dosage track".to_string())
        );
    }
    let explicit_samples = if dosage_encoding == 2 {
        dosage_sample_ids.to_vec()
    } else {
        let bits = cursor.take(
            dosage_sample_ids.len().div_ceil(8),
            "phased-dosage-presence bitarray",
        )?;
        validate_packed_padding(bits, dosage_sample_ids.len(), 1, cursor)?;
        dosage_sample_ids
            .iter()
            .enumerate()
            .filter_map(|(index, &sample)| bit(bits, index).then_some(sample))
            .collect()
    };
    let values = cursor.take(
        explicit_samples
            .len()
            .checked_mul(2)
            .ok_or_else(|| cursor.error("phased dosage byte count overflowed".to_string()))?,
        "phased-dosage values",
    )?;
    let mut output = implicit_haplotype_dosages(dosages, calls, phased);
    for (index, &sample) in explicit_samples.iter().enumerate() {
        let value = i16::from_le_bytes([values[index * 2], values[index * 2 + 1]]);
        if dosage_encoding == 2 && value == i16::MIN {
            continue;
        }
        if !(-16_384..=16_384).contains(&value) {
            return Err(cursor.error(format!(
                "phased-dosage difference {value} is outside [-16384, 16384] for sample {sample}"
            )));
        }
        let Some(total) = dosages[sample] else {
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
        output[sample] = Some([left.clamp(0.0, 1.0), right.clamp(0.0, 1.0)]);
    }
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
}
