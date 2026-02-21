use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, canonical_json_string as canonical_storable_json_string, decode_nstore,
};
use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::row::{CellValue, Row};
use crate::util::{
    BatchBuilder, ColumnMap, canonical_json_string, json_bool, json_i32, json_i64, json_str,
    normalize_genomic_end, normalize_genomic_start, parse_i64, read_maybe_gzip_bytes, stable_hash,
};
use crate::variation::append_provenance;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

struct TranscriptRowCore {
    chrom: String,
    start: i64,
    end: i64,
    strand: i8,
    stable_id: String,
}

struct TranscriptRowContext<'a> {
    coordinate_system_zero_based: bool,
    cache_info: &'a CacheInfo,
    source_file: &'a Path,
}

// ---------------------------------------------------------------------------
// Direct builder parser for text lines (Phase 1+2+6)
// ---------------------------------------------------------------------------

pub(crate) fn parse_transcript_line_into(
    line: &str,
    source_file: &Path,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_map: &ColumnMap,
) -> Result<bool> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(false);
    }

    let parts: Vec<&str> = trimmed.splitn(4, '\t').collect();
    if parts.len() < 4 {
        return Err(exec_err(format!(
            "Malformed transcript row in {}: {}",
            source_file.display(),
            trimmed
        )));
    }

    // Phase 6: Earlier predicate evaluation BEFORE expensive decode_payload.
    // Extract chrom/start/end from the TSV prefix columns first.
    let prefix_chrom = {
        let c = parts[0].trim();
        if c.is_empty() || c == "." {
            None
        } else {
            Some(c)
        }
    };
    let prefix_start = parse_i64(Some(parts[1]));
    let prefix_end = parse_i64(Some(parts[2]));

    // If we have enough info from the prefix, check predicate early
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

    let chrom = if let Some(c) = prefix_chrom {
        c.to_string()
    } else {
        json_str(object.get("chr").or_else(|| object.get("chrom"))).ok_or_else(|| {
            exec_err(format!(
                "Transcript row missing required chrom in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?
    };

    let source_start = prefix_start
        .or_else(|| json_i64(object.get("start")))
        .ok_or_else(|| {
            exec_err(format!(
                "Transcript row missing required start in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?;

    let source_end = prefix_end
        .or_else(|| json_i64(object.get("end")))
        .ok_or_else(|| {
            exec_err(format!(
                "Transcript row missing required end in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?;

    let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
    let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

    // Full predicate check (covers fallback case where prefix was incomplete)
    if !predicate.matches(&chrom, start, end) {
        return Ok(false);
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

    // Write required columns
    if let Some(idx) = col_map.get("chrom") {
        batch.set_utf8(idx, &chrom);
    }
    if let Some(idx) = col_map.get("start") {
        batch.set_i64(idx, start);
    }
    if let Some(idx) = col_map.get("end") {
        batch.set_i64(idx, end);
    }
    if let Some(idx) = col_map.get("strand") {
        batch.set_i8(idx, strand);
    }
    if let Some(idx) = col_map.get("stable_id") {
        batch.set_utf8(idx, &stable_id);
    }

    // Optional columns
    if let Some(idx) = col_map.get("version") {
        batch.set_opt_i32(idx, json_i32(object.get("version")));
    }
    if let Some(idx) = col_map.get("biotype") {
        batch.set_opt_utf8_owned(idx, json_str(object.get("biotype")).as_ref());
    }
    if let Some(idx) = col_map.get("source") {
        batch.set_opt_utf8_owned(
            idx,
            json_str(object.get("source").or_else(|| object.get("_source_cache"))).as_ref(),
        );
    }
    if let Some(idx) = col_map.get("is_canonical") {
        batch.set_opt_bool(idx, json_bool(object.get("is_canonical")));
    }
    if let Some(idx) = col_map.get("gene_stable_id") {
        batch.set_opt_utf8_owned(
            idx,
            json_str(
                object
                    .get("gene_stable_id")
                    .or_else(|| object.get("_gene_stable_id")),
            )
            .as_ref(),
        );
    }
    if let Some(idx) = col_map.get("gene_symbol") {
        batch.set_opt_utf8_owned(
            idx,
            json_str(
                object
                    .get("gene_symbol")
                    .or_else(|| object.get("_gene_symbol")),
            )
            .as_ref(),
        );
    }
    if let Some(idx) = col_map.get("gene_symbol_source") {
        batch.set_opt_utf8_owned(
            idx,
            json_str(
                object
                    .get("gene_symbol_source")
                    .or_else(|| object.get("_gene_symbol_source")),
            )
            .as_ref(),
        );
    }
    if let Some(idx) = col_map.get("gene_hgnc_id") {
        batch.set_opt_utf8_owned(idx, json_str(object.get("gene_hgnc_id")).as_ref());
    }
    if let Some(idx) = col_map.get("refseq_id") {
        let refseq_id = json_str(object.get("refseq_id").or_else(|| object.get("_refseq")))
            .filter(|v| v != "-");
        batch.set_opt_utf8_owned(idx, refseq_id.as_ref());
    }
    if let Some(idx) = col_map.get("coding_region_start") {
        batch.set_opt_i64(
            idx,
            json_i64(object.get("coding_region_start"))
                .map(|v| normalize_genomic_start(v, coordinate_system_zero_based)),
        );
    }
    if let Some(idx) = col_map.get("coding_region_end") {
        batch.set_opt_i64(
            idx,
            json_i64(object.get("coding_region_end"))
                .map(|v| normalize_genomic_end(v, coordinate_system_zero_based)),
        );
    }
    if let Some(idx) = col_map.get("cdna_coding_start") {
        batch.set_opt_i64(idx, json_i64(object.get("cdna_coding_start")));
    }
    if let Some(idx) = col_map.get("cdna_coding_end") {
        batch.set_opt_i64(idx, json_i64(object.get("cdna_coding_end")));
    }

    // Translation sub-object
    let translation_projected = col_map.has_any(&[
        "translation_stable_id",
        "translation_start",
        "translation_end",
    ]);
    if translation_projected {
        if let Some(translation_obj) = object
            .get("translation")
            .and_then(unwrap_blessed_object_optional)
        {
            if let Some(idx) = col_map.get("translation_stable_id") {
                batch.set_opt_utf8_owned(idx, json_str(translation_obj.get("stable_id")).as_ref());
            }
            if let Some(idx) = col_map.get("translation_start") {
                batch.set_opt_i64(idx, json_i64(translation_obj.get("start")));
            }
            if let Some(idx) = col_map.get("translation_end") {
                batch.set_opt_i64(idx, json_i64(translation_obj.get("end")));
            }
        }
        // If translation is null, finish_row() will append nulls for these columns
    }

    if let Some(idx) = col_map.get("exon_count") {
        let exon_count = object
            .get("exon_count")
            .and_then(|v| json_i32(Some(v)))
            .or_else(|| {
                object
                    .get("_trans_exon_array")
                    .and_then(Value::as_array)
                    .and_then(|arr| i32::try_from(arr.len()).ok())
            });
        batch.set_opt_i32(idx, exon_count);
    }

    // Phase 2: Only compute canonical JSON + hash if projected
    let need_json = col_map.get("raw_object_json").is_some();
    let need_hash = col_map.get("object_hash").is_some();
    if need_json || need_hash {
        let canonical_json = canonical_json_string(&payload)?;
        if let Some(idx) = col_map.get("object_hash") {
            let hash = stable_hash(&canonical_json);
            batch.set_utf8(idx, &hash);
        }
        if let Some(idx) = col_map.get("raw_object_json") {
            batch.set_utf8(idx, &canonical_json);
        }
    }

    append_provenance(batch, col_map, cache_info, source_file);

    batch.finish_row();
    Ok(true)
}

// ---------------------------------------------------------------------------
// Storable binary parser (kept with Vec<Row> for now)
// ---------------------------------------------------------------------------

pub(crate) fn parse_transcript_storable_file(
    source_file: &Path,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
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

            let chrom = sv_str(obj.get("chr").or_else(|| obj.get("chrom")))
                .or_else(|| {
                    obj.get("slice")
                        .and_then(SValue::as_hash)
                        .and_then(|slice| sv_str(slice.get("seq_region_name")))
                })
                .unwrap_or_else(|| region_chr.clone());

            let source_start = sv_i64(obj.get("start")).ok_or_else(|| {
                exec_err(format!(
                    "Transcript storable object missing start in {}",
                    source_file.display()
                ))
            })?;
            let source_end = sv_i64(obj.get("end")).ok_or_else(|| {
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

            let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
            let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

            if !predicate.matches(&chrom, start, end) {
                continue;
            }

            let core = TranscriptRowCore {
                chrom,
                start,
                end,
                strand,
                stable_id,
            };
            let context = TranscriptRowContext {
                coordinate_system_zero_based,
                cache_info,
                source_file,
            };

            rows.push(build_transcript_row_storable(item, obj, core, &context)?);
        }
    }

    Ok(rows)
}

fn build_transcript_row_storable(
    payload: &SValue,
    object: &std::collections::BTreeMap<String, SValue>,
    core: TranscriptRowCore,
    context: &TranscriptRowContext<'_>,
) -> Result<Row> {
    let TranscriptRowCore {
        chrom,
        start,
        end,
        strand,
        stable_id,
    } = core;
    let canonical_json = canonical_storable_json_string(payload);
    let object_hash = stable_hash(&canonical_json);

    let mut row: Row = HashMap::new();
    row.insert("chrom".to_string(), CellValue::Utf8(chrom));
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
        sv_i64(object.get("coding_region_start"))
            .map(|value| normalize_genomic_start(value, context.coordinate_system_zero_based)),
    );
    insert_opt_i64(
        &mut row,
        "coding_region_end",
        sv_i64(object.get("coding_region_end"))
            .map(|value| normalize_genomic_end(value, context.coordinate_system_zero_based)),
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

    append_provenance_row(&mut row, context.cache_info, context.source_file);
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

fn append_provenance_row(row: &mut Row, cache_info: &CacheInfo, source_file: &Path) {
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
