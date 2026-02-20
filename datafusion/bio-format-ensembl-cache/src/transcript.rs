use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, canonical_json_string as canonical_storable_json_string, decode_nstore,
};
use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::row::{CellValue, Row};
use crate::util::{
    canonical_json_string, json_bool, json_i32, json_i64, json_str, parse_i64,
    read_maybe_gzip_bytes, stable_hash,
};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

pub(crate) fn parse_transcript_line(
    line: &str,
    source_file: &Path,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
) -> Result<Option<Row>> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(None);
    }

    let parts: Vec<&str> = trimmed.splitn(4, '\t').collect();
    if parts.len() < 4 {
        return Err(exec_err(format!(
            "Malformed transcript row in {}: {}",
            source_file.display(),
            trimmed
        )));
    }

    let serializer = cache_info.serializer_type.as_deref().ok_or_else(|| {
        exec_err(format!(
            "Unknown serializer for transcript entity. serialiser_type missing in info.txt under {}",
            cache_info.cache_root.display()
        ))
    })?;

    let payload = decode_payload(serializer, parts[3])?;
    let object = payload.as_object().ok_or_else(|| {
        exec_err(format!(
            "Transcript payload must be a JSON object in {}",
            source_file.display()
        ))
    })?;

    let chr = if !parts[0].trim().is_empty() && parts[0].trim() != "." {
        parts[0].trim().to_string()
    } else {
        json_str(object.get("chr").or_else(|| object.get("chrom"))).ok_or_else(|| {
            exec_err(format!(
                "Transcript row missing required chr in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?
    };

    let start = parse_i64(Some(parts[1]))
        .or_else(|| json_i64(object.get("start")))
        .ok_or_else(|| {
            exec_err(format!(
                "Transcript row missing required start in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?;

    let end = parse_i64(Some(parts[2]))
        .or_else(|| json_i64(object.get("end")))
        .ok_or_else(|| {
            exec_err(format!(
                "Transcript row missing required end in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?;

    if !predicate.matches(&chr, start, end) {
        return Ok(None);
    }

    let strand = json_i64(object.get("strand"))
        .and_then(|v| i8::try_from(v).ok())
        .ok_or_else(|| {
            exec_err(format!(
                "Transcript row missing required strand in {}",
                source_file.display()
            ))
        })?;

    let stable_id = json_str(object.get("stable_id")).ok_or_else(|| {
        exec_err(format!(
            "Transcript row missing required stable_id in {}",
            source_file.display()
        ))
    })?;

    Ok(Some(build_transcript_row(
        &payload,
        object,
        chr,
        start,
        end,
        strand,
        stable_id,
        cache_info,
        source_file,
    )?))
}

pub(crate) fn parse_transcript_storable_file(
    source_file: &Path,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
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
    for (region_chr, region_value) in root_obj {
        let Some(items) = region_value.as_array() else {
            continue;
        };

        for item in items {
            let obj = item.as_hash().ok_or_else(|| {
                exec_err(format!(
                    "Transcript storable object payload must be a hash in {}",
                    source_file.display()
                ))
            })?;

            let chr = sv_str(obj.get("chr").or_else(|| obj.get("chrom")))
                .or_else(|| {
                    obj.get("slice")
                        .and_then(SValue::as_hash)
                        .and_then(|slice| sv_str(slice.get("seq_region_name")))
                })
                .unwrap_or_else(|| region_chr.clone());

            let start = sv_i64(obj.get("start")).ok_or_else(|| {
                exec_err(format!(
                    "Transcript storable object missing start in {}",
                    source_file.display()
                ))
            })?;
            let end = sv_i64(obj.get("end")).ok_or_else(|| {
                exec_err(format!(
                    "Transcript storable object missing end in {}",
                    source_file.display()
                ))
            })?;
            let strand = sv_i64(obj.get("strand"))
                .and_then(|v| i8::try_from(v).ok())
                .ok_or_else(|| {
                    exec_err(format!(
                        "Transcript storable object missing strand in {}",
                        source_file.display()
                    ))
                })?;
            let stable_id = sv_str(obj.get("stable_id")).ok_or_else(|| {
                exec_err(format!(
                    "Transcript storable object missing stable_id in {}",
                    source_file.display()
                ))
            })?;

            if !predicate.matches(&chr, start, end) {
                continue;
            }

            rows.push(build_transcript_row_storable(
                item,
                obj,
                chr,
                start,
                end,
                strand,
                stable_id,
                cache_info,
                source_file,
            )?);
        }
    }

    Ok(rows)
}

fn build_transcript_row(
    payload: &Value,
    object: &serde_json::Map<String, Value>,
    chr: String,
    start: i64,
    end: i64,
    strand: i8,
    stable_id: String,
    cache_info: &CacheInfo,
    source_file: &Path,
) -> Result<Row> {
    let canonical_json = canonical_json_string(payload)?;
    let object_hash = stable_hash(&canonical_json);

    let mut row: Row = HashMap::new();
    row.insert("chr".to_string(), CellValue::Utf8(chr));
    row.insert("start".to_string(), CellValue::Int64(start));
    row.insert("end".to_string(), CellValue::Int64(end));
    row.insert("strand".to_string(), CellValue::Int8(strand));
    row.insert("stable_id".to_string(), CellValue::Utf8(stable_id));

    insert_opt_i32(&mut row, "version", json_i32(object.get("version")));
    insert_opt_utf8(&mut row, "biotype", json_str(object.get("biotype")));
    insert_opt_utf8(
        &mut row,
        "source",
        json_str(object.get("source").or_else(|| object.get("_source_cache"))),
    );
    insert_opt_bool(
        &mut row,
        "is_canonical",
        json_bool(object.get("is_canonical")),
    );
    insert_opt_utf8(
        &mut row,
        "gene_stable_id",
        json_str(
            object
                .get("gene_stable_id")
                .or_else(|| object.get("_gene_stable_id")),
        ),
    );
    insert_opt_utf8(
        &mut row,
        "gene_symbol",
        json_str(
            object
                .get("gene_symbol")
                .or_else(|| object.get("_gene_symbol")),
        ),
    );
    insert_opt_utf8(
        &mut row,
        "gene_symbol_source",
        json_str(
            object
                .get("gene_symbol_source")
                .or_else(|| object.get("_gene_symbol_source")),
        ),
    );
    insert_opt_utf8(
        &mut row,
        "gene_hgnc_id",
        json_str(object.get("gene_hgnc_id")),
    );
    if let Some(refseq_id) =
        json_str(object.get("refseq_id").or_else(|| object.get("_refseq"))).filter(|v| v != "-")
    {
        row.insert("refseq_id".to_string(), CellValue::Utf8(refseq_id));
    }
    insert_opt_i64(
        &mut row,
        "coding_region_start",
        json_i64(object.get("coding_region_start")),
    );
    insert_opt_i64(
        &mut row,
        "coding_region_end",
        json_i64(object.get("coding_region_end")),
    );
    insert_opt_i64(
        &mut row,
        "cdna_coding_start",
        json_i64(object.get("cdna_coding_start")),
    );
    insert_opt_i64(
        &mut row,
        "cdna_coding_end",
        json_i64(object.get("cdna_coding_end")),
    );

    if let Some(translation_obj) = object
        .get("translation")
        .and_then(unwrap_blessed_object_optional)
    {
        insert_opt_utf8(
            &mut row,
            "translation_stable_id",
            json_str(translation_obj.get("stable_id")),
        );
        insert_opt_i64(
            &mut row,
            "translation_start",
            json_i64(translation_obj.get("start")),
        );
        insert_opt_i64(
            &mut row,
            "translation_end",
            json_i64(translation_obj.get("end")),
        );
    }

    let exon_count = object
        .get("exon_count")
        .and_then(|v| json_i32(Some(v)))
        .or_else(|| {
            object
                .get("_trans_exon_array")
                .and_then(Value::as_array)
                .and_then(|arr| i32::try_from(arr.len()).ok())
        });
    insert_opt_i32(&mut row, "exon_count", exon_count);

    row.insert(
        "raw_object_json".to_string(),
        CellValue::Utf8(canonical_json),
    );
    row.insert("object_hash".to_string(), CellValue::Utf8(object_hash));

    append_provenance(&mut row, cache_info, source_file);
    Ok(row)
}

fn unwrap_blessed_object(value: &Value) -> Result<&serde_json::Map<String, Value>> {
    let mut current = value;
    for _ in 0..4 {
        if let Some(obj) = current.as_object() {
            if let Some(inner) = obj.get("__value") {
                current = inner;
                continue;
            }
            return Ok(obj);
        }
        break;
    }

    Err(exec_err("Expected storable object payload"))
}

fn unwrap_blessed_object_optional(value: &Value) -> Option<&serde_json::Map<String, Value>> {
    unwrap_blessed_object(value).ok()
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

fn insert_opt_bool(row: &mut Row, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        row.insert(key.to_string(), CellValue::Boolean(value));
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

fn build_transcript_row_storable(
    payload: &SValue,
    object: &std::collections::BTreeMap<String, SValue>,
    chr: String,
    start: i64,
    end: i64,
    strand: i8,
    stable_id: String,
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
    row.insert("stable_id".to_string(), CellValue::Utf8(stable_id));

    insert_opt_i32(
        &mut row,
        "version",
        sv_i64(object.get("version")).and_then(|v| i32::try_from(v).ok()),
    );
    insert_opt_utf8(&mut row, "biotype", sv_str(object.get("biotype")));
    insert_opt_utf8(
        &mut row,
        "source",
        sv_str(object.get("source").or_else(|| object.get("_source_cache"))),
    );
    insert_opt_bool(
        &mut row,
        "is_canonical",
        sv_bool(object.get("is_canonical")),
    );
    insert_opt_utf8(
        &mut row,
        "gene_stable_id",
        sv_str(
            object
                .get("gene_stable_id")
                .or_else(|| object.get("_gene_stable_id")),
        ),
    );
    insert_opt_utf8(
        &mut row,
        "gene_symbol",
        sv_str(
            object
                .get("gene_symbol")
                .or_else(|| object.get("_gene_symbol")),
        ),
    );
    insert_opt_utf8(
        &mut row,
        "gene_symbol_source",
        sv_str(
            object
                .get("gene_symbol_source")
                .or_else(|| object.get("_gene_symbol_source")),
        ),
    );
    insert_opt_utf8(&mut row, "gene_hgnc_id", sv_str(object.get("gene_hgnc_id")));
    if let Some(refseq_id) =
        sv_str(object.get("refseq_id").or_else(|| object.get("_refseq"))).filter(|v| v != "-")
    {
        row.insert("refseq_id".to_string(), CellValue::Utf8(refseq_id));
    }
    insert_opt_i64(
        &mut row,
        "coding_region_start",
        sv_i64(object.get("coding_region_start")),
    );
    insert_opt_i64(
        &mut row,
        "coding_region_end",
        sv_i64(object.get("coding_region_end")),
    );
    insert_opt_i64(
        &mut row,
        "cdna_coding_start",
        sv_i64(object.get("cdna_coding_start")),
    );
    insert_opt_i64(
        &mut row,
        "cdna_coding_end",
        sv_i64(object.get("cdna_coding_end")),
    );

    if let Some(translation_obj) = object.get("translation").and_then(SValue::as_hash) {
        insert_opt_utf8(
            &mut row,
            "translation_stable_id",
            sv_str(translation_obj.get("stable_id")),
        );
        insert_opt_i64(
            &mut row,
            "translation_start",
            sv_i64(translation_obj.get("start")),
        );
        insert_opt_i64(
            &mut row,
            "translation_end",
            sv_i64(translation_obj.get("end")),
        );
    }

    let exon_count = sv_i64(object.get("exon_count"))
        .and_then(|v| i32::try_from(v).ok())
        .or_else(|| {
            object
                .get("_trans_exon_array")
                .and_then(SValue::as_array)
                .and_then(|arr| i32::try_from(arr.len()).ok())
        });
    insert_opt_i32(&mut row, "exon_count", exon_count);

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

fn sv_bool(value: Option<&SValue>) -> Option<bool> {
    value.and_then(SValue::as_bool)
}
