use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, canonical_json_string as canonical_storable_json_string, decode_nstore,
};
use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::row::{CellValue, Row};
use crate::util::{
    canonical_json_string, json_f64, json_i32, json_i64, json_str, normalize_genomic_end,
    normalize_genomic_start, parse_i64, read_maybe_gzip_bytes, stable_hash,
};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegulatoryTarget {
    RegulatoryFeature,
    MotifFeature,
}

pub(crate) fn parse_regulatory_line(
    line: &str,
    source_file: &Path,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    target: RegulatoryTarget,
    coordinate_system_zero_based: bool,
) -> Result<Option<Row>> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(None);
    }

    let parts: Vec<&str> = trimmed.splitn(4, '\t').collect();
    if parts.len() < 4 {
        return Err(exec_err(format!(
            "Malformed regulatory row in {}: {}",
            source_file.display(),
            trimmed
        )));
    }

    let serializer = cache_info.serializer_type.as_deref().ok_or_else(|| {
        exec_err(format!(
            "Unknown serializer for regulatory entity. serialiser_type missing in info.txt under {}",
            cache_info.cache_root.display()
        ))
    })?;

    let payload = decode_payload(serializer, parts[3])?;
    let object = payload.as_object().ok_or_else(|| {
        exec_err(format!(
            "Regulatory payload must be a JSON object in {}",
            source_file.display()
        ))
    })?;

    let kind = infer_kind(object);
    if (kind == RegulatoryTarget::RegulatoryFeature
        && target != RegulatoryTarget::RegulatoryFeature)
        || (kind == RegulatoryTarget::MotifFeature && target != RegulatoryTarget::MotifFeature)
    {
        return Ok(None);
    }

    let chr = if !parts[0].trim().is_empty() && parts[0].trim() != "." {
        parts[0].trim().to_string()
    } else {
        json_str(object.get("chr").or_else(|| object.get("chrom"))).ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required chr in {}",
                source_file.display()
            ))
        })?
    };

    let source_start = parse_i64(Some(parts[1]))
        .or_else(|| json_i64(object.get("start")))
        .ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required start in {}",
                source_file.display()
            ))
        })?;

    let source_end = parse_i64(Some(parts[2]))
        .or_else(|| json_i64(object.get("end")))
        .ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required end in {}",
                source_file.display()
            ))
        })?;

    let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
    let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

    if !predicate.matches(&chr, start, end) {
        return Ok(None);
    }

    let strand = json_i64(object.get("strand"))
        .and_then(|v| i8::try_from(v).ok())
        .ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required strand in {}",
                source_file.display()
            ))
        })?;

    let canonical_json = canonical_json_string(&payload)?;
    let object_hash = stable_hash(&canonical_json);

    let mut row: Row = HashMap::new();
    row.insert("chr".to_string(), CellValue::Utf8(chr));
    row.insert("start".to_string(), CellValue::Int64(start));
    row.insert("end".to_string(), CellValue::Int64(end));
    row.insert("strand".to_string(), CellValue::Int8(strand));

    if let Some(stable_id) = json_str(object.get("stable_id")) {
        row.insert("stable_id".to_string(), CellValue::Utf8(stable_id));
    }

    insert_opt_i64(&mut row, "db_id", json_i64(object.get("db_id")));

    match target {
        RegulatoryTarget::RegulatoryFeature => {
            insert_opt_utf8(
                &mut row,
                "feature_type",
                json_str(object.get("feature_type")),
            );
            insert_opt_i32(
                &mut row,
                "epigenome_count",
                json_i32(object.get("epigenome_count")),
            );
            insert_opt_i64(
                &mut row,
                "regulatory_build_id",
                json_i64(object.get("regulatory_build_id")),
            );
            insert_opt_utf8(&mut row, "cell_types", json_str(object.get("cell_types")));
        }
        RegulatoryTarget::MotifFeature => {
            insert_opt_f64(&mut row, "score", json_f64(object.get("score")));
            insert_opt_utf8(
                &mut row,
                "binding_matrix",
                json_str(object.get("binding_matrix")),
            );
            insert_opt_utf8(&mut row, "cell_types", json_str(object.get("cell_types")));
            insert_opt_utf8(
                &mut row,
                "overlapping_regulatory_feature",
                json_str(object.get("overlapping_regulatory_feature")),
            );
        }
    }

    row.insert(
        "raw_object_json".to_string(),
        CellValue::Utf8(canonical_json),
    );
    row.insert("object_hash".to_string(), CellValue::Utf8(object_hash));

    append_provenance(&mut row, cache_info, source_file);

    Ok(Some(row))
}

pub(crate) fn parse_regulatory_storable_file(
    source_file: &Path,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    target: RegulatoryTarget,
    coordinate_system_zero_based: bool,
) -> Result<Vec<Row>> {
    let bytes = read_maybe_gzip_bytes(source_file)?;
    if !bytes.starts_with(b"pst0") {
        return Err(exec_err(format!(
            "File {} is not a storable payload",
            source_file.display()
        )));
    }

    let root = decode_nstore(&bytes)?;
    let root_obj = root.as_hash().ok_or_else(|| {
        exec_err(format!(
            "Decoded storable root must be object for {}",
            source_file.display()
        ))
    })?;

    let mut rows = Vec::new();

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

                let chr = sv_str(obj.get("chr").or_else(|| obj.get("chrom")))
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
                if !predicate.matches(&chr, start, end) {
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

                rows.push(build_regulatory_row_storable(
                    item,
                    obj,
                    chr,
                    start,
                    end,
                    strand,
                    target,
                    cache_info,
                    source_file,
                )?);
            }
        }
    }

    Ok(rows)
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

fn build_regulatory_row_storable(
    payload: &SValue,
    object: &std::collections::BTreeMap<String, SValue>,
    chr: String,
    start: i64,
    end: i64,
    strand: i8,
    target: RegulatoryTarget,
    cache_info: &CacheInfo,
    source_file: &Path,
) -> Result<Row> {
    let canonical_json = canonical_storable_json_string(payload);
    let object_hash = stable_hash(&canonical_json);

    let mut row: Row = HashMap::new();
    row.insert("chr".to_string(), CellValue::Utf8(chr));
    row.insert("start".to_string(), CellValue::Int64(start));
    row.insert("end".to_string(), CellValue::Int64(end));
    row.insert("strand".to_string(), CellValue::Int8(strand));

    insert_opt_utf8(&mut row, "stable_id", sv_str(object.get("stable_id")));
    insert_opt_i64(
        &mut row,
        "db_id",
        sv_i64(object.get("db_id").or_else(|| object.get("dbID"))),
    );

    match target {
        RegulatoryTarget::RegulatoryFeature => {
            insert_opt_utf8(
                &mut row,
                "feature_type",
                sv_str(
                    object
                        .get("feature_type")
                        .or_else(|| object.get("_vep_feature_type")),
                ),
            );
            insert_opt_i32(
                &mut row,
                "epigenome_count",
                sv_i64(object.get("epigenome_count")).and_then(|v| i32::try_from(v).ok()),
            );
            insert_opt_i64(
                &mut row,
                "regulatory_build_id",
                sv_i64(object.get("regulatory_build_id")),
            );
            insert_opt_utf8(&mut row, "cell_types", sv_str(object.get("cell_types")));
        }
        RegulatoryTarget::MotifFeature => {
            insert_opt_f64(&mut row, "score", sv_f64(object.get("score")));
            insert_opt_utf8(
                &mut row,
                "binding_matrix",
                sv_str(object.get("binding_matrix")),
            );
            insert_opt_utf8(&mut row, "cell_types", sv_str(object.get("cell_types")));
            insert_opt_utf8(
                &mut row,
                "overlapping_regulatory_feature",
                sv_str(
                    object
                        .get("overlapping_regulatory_feature")
                        .or_else(|| object.get("regulatory_feature_stable_id")),
                ),
            );
        }
    }

    row.insert(
        "raw_object_json".to_string(),
        CellValue::Utf8(canonical_json),
    );
    row.insert("object_hash".to_string(), CellValue::Utf8(object_hash));

    append_provenance(&mut row, cache_info, source_file);
    Ok(row)
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

fn insert_opt_utf8(row: &mut Row, key: &str, value: Option<String>) {
    if let Some(value) = value {
        row.insert(key.to_string(), CellValue::Utf8(value));
    }
}

fn insert_opt_i64(row: &mut Row, key: &str, value: Option<i64>) {
    if let Some(value) = value {
        row.insert(key.to_string(), CellValue::Int64(value));
    }
}

fn insert_opt_i32(row: &mut Row, key: &str, value: Option<i32>) {
    if let Some(value) = value {
        row.insert(key.to_string(), CellValue::Int32(value));
    }
}

fn insert_opt_f64(row: &mut Row, key: &str, value: Option<f64>) {
    if let Some(value) = value {
        row.insert(key.to_string(), CellValue::Float64(value));
    }
}

fn append_provenance(row: &mut Row, cache_info: &CacheInfo, source_file: &Path) {
    row.insert(
        "species".to_string(),
        CellValue::Utf8(cache_info.species.clone()),
    );
    row.insert(
        "assembly".to_string(),
        CellValue::Utf8(cache_info.assembly.clone()),
    );
    row.insert(
        "cache_version".to_string(),
        CellValue::Utf8(cache_info.cache_version.clone()),
    );
    for source in &cache_info.source_descriptors {
        row.insert(
            source.source_column.clone(),
            CellValue::Utf8(source.value.clone()),
        );
    }
    if let Some(serializer) = &cache_info.serializer_type {
        row.insert(
            "serializer_type".to_string(),
            CellValue::Utf8(serializer.clone()),
        );
    }
    row.insert(
        "source_cache_path".to_string(),
        CellValue::Utf8(cache_info.source_cache_path.clone()),
    );
    row.insert(
        "source_file".to_string(),
        CellValue::Utf8(source_file.to_string_lossy().to_string()),
    );
}
