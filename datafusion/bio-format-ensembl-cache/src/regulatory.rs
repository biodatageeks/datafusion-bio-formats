use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, canonical_json_string as canonical_storable_json_string, decode_nstore,
};
use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::util::ProvenanceWriter;
use crate::util::{
    BatchBuilder, ColumnMap, canonical_json_string, json_f64, json_i32, json_i64, json_str,
    normalize_genomic_end, normalize_genomic_start, parse_i64, read_maybe_gzip_bytes, stable_hash,
};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegulatoryTarget {
    RegulatoryFeature,
    MotifFeature,
}

// ---------------------------------------------------------------------------
// RegulatoryColumnIndices – pre-computed builder indices from ColumnMap
// ---------------------------------------------------------------------------

pub(crate) struct RegulatoryColumnIndices {
    chrom: Option<usize>,
    start: Option<usize>,
    end: Option<usize>,
    strand: Option<usize>,
    stable_id: Option<usize>,
    db_id: Option<usize>,
    // RegulatoryFeature-specific
    feature_type: Option<usize>,
    epigenome_count: Option<usize>,
    regulatory_build_id: Option<usize>,
    // MotifFeature-specific
    score: Option<usize>,
    binding_matrix: Option<usize>,
    overlapping_regulatory_feature: Option<usize>,
    // Shared
    cell_types: Option<usize>,
    raw_object_json: Option<usize>,
    object_hash: Option<usize>,
}

impl RegulatoryColumnIndices {
    pub fn new(col_map: &ColumnMap) -> Self {
        Self {
            chrom: col_map.get("chrom"),
            start: col_map.get("start"),
            end: col_map.get("end"),
            strand: col_map.get("strand"),
            stable_id: col_map.get("stable_id"),
            db_id: col_map.get("db_id"),
            feature_type: col_map.get("feature_type"),
            epigenome_count: col_map.get("epigenome_count"),
            regulatory_build_id: col_map.get("regulatory_build_id"),
            score: col_map.get("score"),
            binding_matrix: col_map.get("binding_matrix"),
            overlapping_regulatory_feature: col_map.get("overlapping_regulatory_feature"),
            cell_types: col_map.get("cell_types"),
            raw_object_json: col_map.get("raw_object_json"),
            object_hash: col_map.get("object_hash"),
        }
    }
}

struct RegulatoryRowCore {
    chrom: String,
    start: i64,
    end: i64,
    strand: i8,
    target: RegulatoryTarget,
}

// ---------------------------------------------------------------------------
// Direct builder parser for text lines (Phase 1+2)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_regulatory_line_into(
    line: &str,
    source_file_str: &str,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    target: RegulatoryTarget,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &RegulatoryColumnIndices,
    provenance: &ProvenanceWriter,
) -> Result<bool> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(false);
    }

    // Iterator-based prefix split – avoids Vec allocation
    let mut split_iter = trimmed.splitn(4, '\t');
    let part0 = split_iter.next().ok_or_else(|| {
        exec_err(format!(
            "Malformed regulatory row in {}: {}",
            source_file_str, trimmed
        ))
    })?;
    let part1 = split_iter.next().ok_or_else(|| {
        exec_err(format!(
            "Malformed regulatory row in {}: {}",
            source_file_str, trimmed
        ))
    })?;
    let part2 = split_iter.next().ok_or_else(|| {
        exec_err(format!(
            "Malformed regulatory row in {}: {}",
            source_file_str, trimmed
        ))
    })?;
    let part3 = split_iter.next().ok_or_else(|| {
        exec_err(format!(
            "Malformed regulatory row in {}: {}",
            source_file_str, trimmed
        ))
    })?;

    // Early predicate check from prefix columns
    let prefix_chrom = {
        let c = part0.trim();
        if c.is_empty() || c == "." {
            None
        } else {
            Some(c)
        }
    };
    let prefix_start = parse_i64(Some(part1));
    let prefix_end = parse_i64(Some(part2));

    if let (Some(chrom_ref), Some(raw_start), Some(raw_end)) =
        (prefix_chrom, prefix_start, prefix_end)
    {
        let start = normalize_genomic_start(raw_start, coordinate_system_zero_based);
        let end = normalize_genomic_end(raw_end, coordinate_system_zero_based);
        if !predicate.matches(chrom_ref, start, end) {
            return Ok(false);
        }
    }

    let serializer = cache_info.serializer_type.as_deref().ok_or_else(|| {
        exec_err(format!(
            "Unknown serializer for regulatory entity. serialiser_type missing in info.txt under {}",
            cache_info.cache_root.display()
        ))
    })?;

    let payload = decode_payload(serializer, part3)?;
    let object = payload.as_object().ok_or_else(|| {
        exec_err(format!(
            "Regulatory payload must be a JSON object in {}",
            source_file_str
        ))
    })?;

    let kind = infer_kind(object);
    if (kind == RegulatoryTarget::RegulatoryFeature
        && target != RegulatoryTarget::RegulatoryFeature)
        || (kind == RegulatoryTarget::MotifFeature && target != RegulatoryTarget::MotifFeature)
    {
        return Ok(false);
    }

    let chrom = if let Some(c) = prefix_chrom {
        c.to_string()
    } else {
        json_str(object.get("chr").or_else(|| object.get("chrom"))).ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required chrom in {}",
                source_file_str
            ))
        })?
    };

    let source_start = prefix_start
        .or_else(|| json_i64(object.get("start")))
        .ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required start in {}",
                source_file_str
            ))
        })?;

    let source_end = prefix_end
        .or_else(|| json_i64(object.get("end")))
        .ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required end in {}",
                source_file_str
            ))
        })?;

    let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
    let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

    if !predicate.matches(&chrom, start, end) {
        return Ok(false);
    }

    let strand = json_i64(object.get("strand"))
        .and_then(|v| i8::try_from(v).ok())
        .ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required strand in {}",
                source_file_str
            ))
        })?;

    // Write required columns (direct index access, no HashMap lookups)
    if let Some(idx) = col_idx.chrom {
        batch.set_utf8(idx, &chrom);
    }
    if let Some(idx) = col_idx.start {
        batch.set_i64(idx, start);
    }
    if let Some(idx) = col_idx.end {
        batch.set_i64(idx, end);
    }
    if let Some(idx) = col_idx.strand {
        batch.set_i8(idx, strand);
    }
    if let Some(idx) = col_idx.stable_id {
        batch.set_opt_utf8_owned(idx, json_str(object.get("stable_id")).as_ref());
    }
    if let Some(idx) = col_idx.db_id {
        batch.set_opt_i64(idx, json_i64(object.get("db_id")));
    }

    match target {
        RegulatoryTarget::RegulatoryFeature => {
            if let Some(idx) = col_idx.feature_type {
                batch.set_opt_utf8_owned(idx, json_str(object.get("feature_type")).as_ref());
            }
            if let Some(idx) = col_idx.epigenome_count {
                batch.set_opt_i32(idx, json_i32(object.get("epigenome_count")));
            }
            if let Some(idx) = col_idx.regulatory_build_id {
                batch.set_opt_i64(idx, json_i64(object.get("regulatory_build_id")));
            }
            if let Some(idx) = col_idx.cell_types {
                batch.set_opt_utf8_owned(idx, json_str(object.get("cell_types")).as_ref());
            }
        }
        RegulatoryTarget::MotifFeature => {
            if let Some(idx) = col_idx.score {
                batch.set_opt_f64(idx, json_f64(object.get("score")));
            }
            if let Some(idx) = col_idx.binding_matrix {
                batch.set_opt_utf8_owned(idx, json_str(object.get("binding_matrix")).as_ref());
            }
            if let Some(idx) = col_idx.cell_types {
                batch.set_opt_utf8_owned(idx, json_str(object.get("cell_types")).as_ref());
            }
            if let Some(idx) = col_idx.overlapping_regulatory_feature {
                batch.set_opt_utf8_owned(
                    idx,
                    json_str(object.get("overlapping_regulatory_feature")).as_ref(),
                );
            }
        }
    }

    // Only compute canonical JSON + hash if projected
    let need_json = col_idx.raw_object_json.is_some();
    let need_hash = col_idx.object_hash.is_some();
    if need_json || need_hash {
        let canonical_json = canonical_json_string(&payload)?;
        if let Some(idx) = col_idx.object_hash {
            let hash = stable_hash(&canonical_json);
            batch.set_utf8(idx, &hash);
        }
        if let Some(idx) = col_idx.raw_object_json {
            batch.set_utf8(idx, &canonical_json);
        }
    }

    provenance.write(batch, source_file_str);

    batch.finish_row();
    Ok(true)
}

// ---------------------------------------------------------------------------
// Storable binary parser (direct-to-batch streaming)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_regulatory_storable_file_into<F>(
    source_file: &Path,
    source_file_str: &str,
    predicate: &SimplePredicate,
    target: RegulatoryTarget,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &RegulatoryColumnIndices,
    provenance: &ProvenanceWriter,
    mut on_row_added: F,
) -> Result<()>
where
    F: FnMut(&mut BatchBuilder) -> Result<bool>,
{
    let bytes = read_maybe_gzip_bytes(source_file)?;
    if !bytes.starts_with(b"pst0") {
        return Err(exec_err(format!(
            "File {} is not a storable payload",
            source_file.display()
        )));
    }

    let root = decode_nstore(&bytes)?;
    drop(bytes);
    let root_obj = root.as_hash().ok_or_else(|| {
        exec_err(format!(
            "Decoded storable root must be object for {}",
            source_file.display()
        ))
    })?;

    for (region_chr, region_payload) in root_obj {
        let Some(region_obj) = region_payload.as_hash() else {
            continue;
        };

        for (container_name, features_payload) in region_obj {
            let container_kind = infer_kind_from_name(container_name);
            let Some(items) = features_payload.as_array() else {
                continue;
            };

            for item in items {
                let obj = item.as_hash().ok_or_else(|| {
                    exec_err(format!(
                        "Regulatory storable object payload must be a hash in {}",
                        source_file.display()
                    ))
                })?;

                let kind = container_kind.unwrap_or_else(|| infer_kind_storable(obj));
                if kind != target {
                    continue;
                }

                let chrom = sv_str(obj.get("chr").or_else(|| obj.get("chrom")))
                    .or_else(|| {
                        obj.get("slice")
                            .and_then(SValue::as_hash)
                            .and_then(|slice| sv_str(slice.get("seq_region_name")))
                    })
                    .unwrap_or_else(|| region_chr.clone());

                let source_start = sv_i64(obj.get("start")).ok_or_else(|| {
                    exec_err(format!(
                        "Regulatory storable object missing start in {}",
                        source_file.display()
                    ))
                })?;
                let source_end = sv_i64(obj.get("end")).ok_or_else(|| {
                    exec_err(format!(
                        "Regulatory storable object missing end in {}",
                        source_file.display()
                    ))
                })?;
                let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
                let end = normalize_genomic_end(source_end, coordinate_system_zero_based);
                if !predicate.matches(&chrom, start, end) {
                    continue;
                }

                let strand = sv_i64(obj.get("strand"))
                    .and_then(|v| i8::try_from(v).ok())
                    .ok_or_else(|| {
                        exec_err(format!(
                            "Regulatory storable object missing strand in {}",
                            source_file.display()
                        ))
                    })?;

                let core = RegulatoryRowCore {
                    chrom,
                    start,
                    end,
                    strand,
                    target,
                };

                append_regulatory_storable_row_into(
                    item,
                    obj,
                    core,
                    source_file_str,
                    batch,
                    col_idx,
                    provenance,
                )?;

                if !on_row_added(batch)? {
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

fn infer_kind(object: &serde_json::Map<String, serde_json::Value>) -> RegulatoryTarget {
    if let Some(kind) = json_str(object.get("kind").or_else(|| object.get("entity"))) {
        let lower = kind.to_ascii_lowercase();
        if lower.contains("motif") {
            return RegulatoryTarget::MotifFeature;
        }
        return RegulatoryTarget::RegulatoryFeature;
    }

    if object.contains_key("score") || object.contains_key("binding_matrix") {
        RegulatoryTarget::MotifFeature
    } else {
        RegulatoryTarget::RegulatoryFeature
    }
}

fn infer_kind_storable(object: &std::collections::BTreeMap<String, SValue>) -> RegulatoryTarget {
    if let Some(kind) = sv_str(
        object
            .get("kind")
            .or_else(|| object.get("entity"))
            .or_else(|| object.get("_vep_feature_type")),
    ) {
        let lower = kind.to_ascii_lowercase();
        if lower.contains("motif") {
            return RegulatoryTarget::MotifFeature;
        }
        return RegulatoryTarget::RegulatoryFeature;
    }

    if object.contains_key("score") || object.contains_key("binding_matrix") {
        RegulatoryTarget::MotifFeature
    } else {
        RegulatoryTarget::RegulatoryFeature
    }
}

fn infer_kind_from_name(name: &str) -> Option<RegulatoryTarget> {
    let lower = name.to_ascii_lowercase();
    if lower.contains("motif") {
        Some(RegulatoryTarget::MotifFeature)
    } else if lower.contains("regulatory") {
        Some(RegulatoryTarget::RegulatoryFeature)
    } else {
        None
    }
}

fn append_regulatory_storable_row_into(
    payload: &SValue,
    object: &std::collections::BTreeMap<String, SValue>,
    core: RegulatoryRowCore,
    source_file_str: &str,
    batch: &mut BatchBuilder,
    col_idx: &RegulatoryColumnIndices,
    provenance: &ProvenanceWriter,
) -> Result<()> {
    let RegulatoryRowCore {
        chrom,
        start,
        end,
        strand,
        target,
    } = core;

    // Required columns
    if let Some(idx) = col_idx.chrom {
        batch.set_utf8(idx, &chrom);
    }
    if let Some(idx) = col_idx.start {
        batch.set_i64(idx, start);
    }
    if let Some(idx) = col_idx.end {
        batch.set_i64(idx, end);
    }
    if let Some(idx) = col_idx.strand {
        batch.set_i8(idx, strand);
    }
    if let Some(idx) = col_idx.stable_id {
        let value = sv_str(object.get("stable_id"));
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.db_id {
        batch.set_opt_i64(
            idx,
            sv_i64(object.get("db_id").or_else(|| object.get("dbID"))),
        );
    }

    match target {
        RegulatoryTarget::RegulatoryFeature => {
            if let Some(idx) = col_idx.feature_type {
                let value = sv_str(
                    object
                        .get("feature_type")
                        .or_else(|| object.get("_vep_feature_type")),
                );
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.epigenome_count {
                batch.set_opt_i32(
                    idx,
                    sv_i64(object.get("epigenome_count")).and_then(|v| i32::try_from(v).ok()),
                );
            }
            if let Some(idx) = col_idx.regulatory_build_id {
                batch.set_opt_i64(idx, sv_i64(object.get("regulatory_build_id")));
            }
            if let Some(idx) = col_idx.cell_types {
                let value = sv_str(object.get("cell_types"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
        }
        RegulatoryTarget::MotifFeature => {
            if let Some(idx) = col_idx.score {
                batch.set_opt_f64(idx, sv_f64(object.get("score")));
            }
            if let Some(idx) = col_idx.binding_matrix {
                let value = sv_str(object.get("binding_matrix"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.cell_types {
                let value = sv_str(object.get("cell_types"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.overlapping_regulatory_feature {
                let value = sv_str(
                    object
                        .get("overlapping_regulatory_feature")
                        .or_else(|| object.get("regulatory_feature_stable_id")),
                );
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
        }
    }

    // Only compute canonical JSON + hash if projected
    let need_json = col_idx.raw_object_json.is_some();
    let need_hash = col_idx.object_hash.is_some();
    if need_json || need_hash {
        let canonical_json = canonical_storable_json_string(payload);
        if let Some(idx) = col_idx.object_hash {
            let hash = stable_hash(&canonical_json);
            batch.set_utf8(idx, &hash);
        }
        if let Some(idx) = col_idx.raw_object_json {
            batch.set_utf8(idx, &canonical_json);
        }
    }

    provenance.write(batch, source_file_str);
    batch.finish_row();
    Ok(())
}

fn sv_str(value: Option<&SValue>) -> Option<String> {
    value.and_then(SValue::as_string).and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() || trimmed == "." {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn sv_i64(value: Option<&SValue>) -> Option<i64> {
    value.and_then(SValue::as_i64)
}

fn sv_f64(value: Option<&SValue>) -> Option<f64> {
    value
        .and_then(SValue::as_string)
        .and_then(|v| v.parse::<f64>().ok())
}
