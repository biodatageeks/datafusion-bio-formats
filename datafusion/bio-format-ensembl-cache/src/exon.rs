use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, canonical_json_string as canonical_storable_json_string,
    collect_nstore_alias_counts_and_top_keys_from_reader,
    stream_nstore_top_hash_array_items_keyed_with_alias_counts_from_reader,
};
use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::util::ProvenanceWriter;
use crate::util::{
    BatchBuilder, ColumnMap, canonical_json_string, json_bool, json_i32, json_i64, json_str,
    normalize_genomic_end, normalize_genomic_start, open_binary_reader, parse_i64, stable_hash,
};
use serde_json::Value;
use std::path::Path;

// ---------------------------------------------------------------------------
// ExonColumnIndices – pre-computed builder indices from ColumnMap
// ---------------------------------------------------------------------------

pub(crate) struct ExonColumnIndices {
    chrom: Option<usize>,
    start: Option<usize>,
    end: Option<usize>,
    strand: Option<usize>,
    stable_id: Option<usize>,
    version: Option<usize>,
    phase: Option<usize>,
    end_phase: Option<usize>,
    is_current: Option<usize>,
    is_constitutive: Option<usize>,
    transcript_stable_id: Option<usize>,
    gene_stable_id: Option<usize>,
    exon_rank: Option<usize>,
    raw_object_json: Option<usize>,
    object_hash: Option<usize>,
}

impl ExonColumnIndices {
    pub fn new(col_map: &ColumnMap) -> Self {
        Self {
            chrom: col_map.get("chrom"),
            start: col_map.get("start"),
            end: col_map.get("end"),
            strand: col_map.get("strand"),
            stable_id: col_map.get("stable_id"),
            version: col_map.get("version"),
            phase: col_map.get("phase"),
            end_phase: col_map.get("end_phase"),
            is_current: col_map.get("is_current"),
            is_constitutive: col_map.get("is_constitutive"),
            transcript_stable_id: col_map.get("transcript_id"),
            gene_stable_id: col_map.get("gene_stable_id"),
            exon_rank: col_map.get("exon_number"),
            raw_object_json: col_map.get("raw_object_json"),
            object_hash: col_map.get("object_hash"),
        }
    }
}

// ---------------------------------------------------------------------------
// Text-line parser – one transcript line produces N exon rows
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_exon_line_into<F>(
    line: &str,
    source_file_str: &str,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &ExonColumnIndices,
    provenance: &ProvenanceWriter,
    mut on_row_added: F,
) -> Result<bool>
where
    F: FnMut(&mut BatchBuilder) -> Result<bool>,
{
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(true);
    }

    let mut split_iter = trimmed.splitn(4, '\t');
    let part0 = split_iter
        .next()
        .ok_or_else(|| exec_err(format!("Malformed exon row in {source_file_str}")))?;
    let part1 = split_iter
        .next()
        .ok_or_else(|| exec_err(format!("Malformed exon row in {source_file_str}")))?;
    let part2 = split_iter
        .next()
        .ok_or_else(|| exec_err(format!("Malformed exon row in {source_file_str}")))?;
    let part3 = split_iter
        .next()
        .ok_or_else(|| exec_err(format!("Malformed exon row in {source_file_str}")))?;

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
            return Ok(true);
        }
    }

    let serializer = cache_info.serializer_type.as_deref().ok_or_else(|| {
        exec_err(format!(
            "Unknown serializer for exon entity. serialiser_type missing in info.txt under {}",
            cache_info.cache_root.display()
        ))
    })?;

    let payload = decode_payload(serializer, part3)?;
    let object = payload.as_object().ok_or_else(|| {
        exec_err(format!(
            "Exon payload must be a JSON object in {source_file_str}"
        ))
    })?;

    let chrom = if let Some(c) = prefix_chrom {
        c.to_string()
    } else {
        json_str(object.get("chr").or_else(|| object.get("chrom"))).ok_or_else(|| {
            exec_err(format!(
                "Exon row missing required chrom in {source_file_str}"
            ))
        })?
    };

    let source_start = prefix_start
        .or_else(|| json_i64(object.get("start")))
        .ok_or_else(|| {
            exec_err(format!(
                "Exon row missing required start in {source_file_str}"
            ))
        })?;
    let source_end = prefix_end
        .or_else(|| json_i64(object.get("end")))
        .ok_or_else(|| {
            exec_err(format!(
                "Exon row missing required end in {source_file_str}"
            ))
        })?;
    let tx_start = normalize_genomic_start(source_start, coordinate_system_zero_based);
    let tx_end = normalize_genomic_end(source_end, coordinate_system_zero_based);

    if !predicate.matches(&chrom, tx_start, tx_end) {
        return Ok(true);
    }

    let strand = json_i64(object.get("strand"))
        .and_then(|v| i8::try_from(v).ok())
        .ok_or_else(|| {
            exec_err(format!(
                "Exon row missing required strand in {source_file_str}"
            ))
        })?;
    let transcript_stable_id = json_str(object.get("stable_id")).ok_or_else(|| {
        exec_err(format!(
            "Exon row missing required stable_id in {source_file_str}"
        ))
    })?;
    let gene_stable_id = json_str(
        object
            .get("gene_stable_id")
            .or_else(|| object.get("_gene_stable_id")),
    );

    let exon_array = match object.get("_trans_exon_array").and_then(Value::as_array) {
        Some(arr) => arr,
        None => return Ok(true), // no exons in this transcript
    };

    for (rank, exon_val) in exon_array.iter().enumerate() {
        let Some(exon_obj) = unwrap_blessed_object_optional(exon_val) else {
            continue;
        };

        let exon_start = json_i64(exon_obj.get("start"))
            .map(|v| normalize_genomic_start(v, coordinate_system_zero_based));
        let exon_end = json_i64(exon_obj.get("end"))
            .map(|v| normalize_genomic_end(v, coordinate_system_zero_based));

        let (es, ee) = match (exon_start, exon_end) {
            (Some(s), Some(e)) => (s, e),
            _ => continue,
        };

        if let Some(idx) = col_idx.chrom {
            batch.set_utf8(idx, &chrom);
        }
        if let Some(idx) = col_idx.start {
            batch.set_i64(idx, es);
        }
        if let Some(idx) = col_idx.end {
            batch.set_i64(idx, ee);
        }
        if let Some(idx) = col_idx.strand {
            batch.set_i8(idx, strand);
        }
        if let Some(idx) = col_idx.stable_id {
            batch.set_opt_utf8_owned(idx, json_str(exon_obj.get("stable_id")).as_ref());
        }
        if let Some(idx) = col_idx.version {
            batch.set_opt_i32(idx, json_i32(exon_obj.get("version")));
        }
        if let Some(idx) = col_idx.phase {
            batch.set_opt_i8(
                idx,
                json_i64(exon_obj.get("phase")).and_then(|v| i8::try_from(v).ok()),
            );
        }
        if let Some(idx) = col_idx.end_phase {
            batch.set_opt_i8(
                idx,
                json_i64(exon_obj.get("end_phase")).and_then(|v| i8::try_from(v).ok()),
            );
        }
        if let Some(idx) = col_idx.is_current {
            batch.set_opt_bool(idx, json_bool(exon_obj.get("is_current")));
        }
        if let Some(idx) = col_idx.is_constitutive {
            batch.set_opt_bool(idx, json_bool(exon_obj.get("is_constitutive")));
        }
        if let Some(idx) = col_idx.transcript_stable_id {
            batch.set_utf8(idx, &transcript_stable_id);
        }
        if let Some(idx) = col_idx.gene_stable_id {
            batch.set_opt_utf8_owned(idx, gene_stable_id.as_ref());
        }
        if let Some(idx) = col_idx.exon_rank {
            batch.set_opt_i32(idx, i32::try_from(rank).ok());
        }

        let need_json = col_idx.raw_object_json.is_some();
        let need_hash = col_idx.object_hash.is_some();
        if need_json || need_hash {
            let canonical_json = canonical_json_string(exon_val)?;
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

        if !on_row_added(batch)? {
            return Ok(false);
        }
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// Storable binary parser (direct-to-batch streaming)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_exon_storable_file_into<F>(
    source_file: &Path,
    source_file_str: &str,
    predicate: &SimplePredicate,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &ExonColumnIndices,
    provenance: &ProvenanceWriter,
    mut on_row_added: F,
) -> Result<()>
where
    F: FnMut(&mut BatchBuilder) -> Result<bool>,
{
    let mut process_item = |item: &SValue, region_key: &str| -> Result<bool> {
        let obj = item.as_hash().ok_or_else(|| {
            exec_err(format!(
                "Exon storable transcript must be a hash in {}",
                source_file.display()
            ))
        })?;

        let chrom = sv_str(obj.get("chr").or_else(|| obj.get("chrom")))
            .or_else(|| {
                obj.get("slice")
                    .and_then(SValue::as_hash)
                    .and_then(|slice| sv_str(slice.get("seq_region_name")))
            })
            .unwrap_or_else(|| region_key.to_string());

        let source_start = sv_i64(obj.get("start")).ok_or_else(|| {
            exec_err(format!(
                "Exon storable transcript missing start in {}",
                source_file.display()
            ))
        })?;
        let source_end = sv_i64(obj.get("end")).ok_or_else(|| {
            exec_err(format!(
                "Exon storable transcript missing end in {}",
                source_file.display()
            ))
        })?;

        let tx_start = normalize_genomic_start(source_start, coordinate_system_zero_based);
        let tx_end = normalize_genomic_end(source_end, coordinate_system_zero_based);

        if !predicate.matches(&chrom, tx_start, tx_end) {
            return Ok(true);
        }

        let strand = sv_i64(obj.get("strand"))
            .and_then(|v| i8::try_from(v).ok())
            .ok_or_else(|| {
                exec_err(format!(
                    "Exon storable transcript missing strand in {}",
                    source_file.display()
                ))
            })?;
        let transcript_stable_id = sv_str(obj.get("stable_id")).ok_or_else(|| {
            exec_err(format!(
                "Exon storable transcript missing stable_id in {}",
                source_file.display()
            ))
        })?;
        let gene_stable_id = sv_str(
            obj.get("gene_stable_id")
                .or_else(|| obj.get("_gene_stable_id")),
        );

        let exon_array = match obj.get("_trans_exon_array").and_then(SValue::as_array) {
            Some(arr) => arr,
            None => return Ok(true),
        };

        for (rank, exon_val) in exon_array.iter().enumerate() {
            let Some(exon_obj) = exon_val.as_hash() else {
                continue;
            };

            let exon_start = sv_i64(exon_obj.get("start"))
                .map(|v| normalize_genomic_start(v, coordinate_system_zero_based));
            let exon_end = sv_i64(exon_obj.get("end"))
                .map(|v| normalize_genomic_end(v, coordinate_system_zero_based));

            let (es, ee) = match (exon_start, exon_end) {
                (Some(s), Some(e)) => (s, e),
                _ => continue,
            };

            if let Some(idx) = col_idx.chrom {
                batch.set_utf8(idx, &chrom);
            }
            if let Some(idx) = col_idx.start {
                batch.set_i64(idx, es);
            }
            if let Some(idx) = col_idx.end {
                batch.set_i64(idx, ee);
            }
            if let Some(idx) = col_idx.strand {
                batch.set_i8(idx, strand);
            }
            if let Some(idx) = col_idx.stable_id {
                let value = sv_str(exon_obj.get("stable_id"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.version {
                batch.set_opt_i32(
                    idx,
                    sv_i64(exon_obj.get("version")).and_then(|v| i32::try_from(v).ok()),
                );
            }
            if let Some(idx) = col_idx.phase {
                batch.set_opt_i8(
                    idx,
                    sv_i64(exon_obj.get("phase")).and_then(|v| i8::try_from(v).ok()),
                );
            }
            if let Some(idx) = col_idx.end_phase {
                batch.set_opt_i8(
                    idx,
                    sv_i64(exon_obj.get("end_phase")).and_then(|v| i8::try_from(v).ok()),
                );
            }
            if let Some(idx) = col_idx.is_current {
                batch.set_opt_bool(idx, sv_bool(exon_obj.get("is_current")));
            }
            if let Some(idx) = col_idx.is_constitutive {
                batch.set_opt_bool(idx, sv_bool(exon_obj.get("is_constitutive")));
            }
            if let Some(idx) = col_idx.transcript_stable_id {
                batch.set_utf8(idx, &transcript_stable_id);
            }
            if let Some(idx) = col_idx.gene_stable_id {
                batch.set_opt_utf8_owned(idx, gene_stable_id.as_ref());
            }
            if let Some(idx) = col_idx.exon_rank {
                batch.set_opt_i32(idx, i32::try_from(rank).ok());
            }

            let need_json = col_idx.raw_object_json.is_some();
            let need_hash = col_idx.object_hash.is_some();
            if need_json || need_hash {
                let canonical_json = canonical_storable_json_string(exon_val);
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

            if !on_row_added(batch)? {
                return Ok(false);
            }
        }

        Ok(true)
    };

    let (alias_counts, entry_keys) =
        collect_nstore_alias_counts_and_top_keys_from_reader(open_binary_reader(source_file)?)
            .map_err(|e| {
                exec_err(format!(
                    "Failed collecting storable alias references from {}: {}",
                    source_file.display(),
                    e
                ))
            })?;

    let reader = open_binary_reader(source_file)?;
    stream_nstore_top_hash_array_items_keyed_with_alias_counts_from_reader(
        reader,
        alias_counts,
        entry_keys,
        |region_key, item| process_item(&item, region_key),
    )
    .map_err(|e| {
        exec_err(format!(
            "Failed streaming storable exon payload from {}: {}",
            source_file.display(),
            e
        ))
    })?;

    Ok(())
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
