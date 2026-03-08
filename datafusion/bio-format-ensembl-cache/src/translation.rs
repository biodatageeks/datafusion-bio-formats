use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, canonical_json_string as canonical_storable_json_string,
    collect_nstore_alias_counts_and_top_keys_from_reader,
    stream_nstore_top_hash_array_items_keyed_with_alias_counts_from_reader,
};
use crate::errors::{Result, exec_err};
use crate::exon::is_excluded_biotype;
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::util::ProvenanceWriter;
use crate::util::{
    BatchBuilder, ColumnMap, canonical_json_string, json_i32, json_i64, json_str,
    normalize_genomic_end, normalize_genomic_start, open_binary_reader, parse_i64, stable_hash,
};
use std::collections::HashSet;
use std::path::Path;

// ---------------------------------------------------------------------------
// TranslationColumnIndices – pre-computed builder indices from ColumnMap
// ---------------------------------------------------------------------------

pub(crate) struct TranslationColumnIndices {
    chrom: Option<usize>,
    start: Option<usize>,
    end: Option<usize>,
    stable_id: Option<usize>,
    version: Option<usize>,
    translation_start: Option<usize>,
    translation_end: Option<usize>,
    protein_length: Option<usize>,
    transcript_stable_id: Option<usize>,
    gene_stable_id: Option<usize>,
    cdna_coding_start: Option<usize>,
    cdna_coding_end: Option<usize>,
    cds_len: Option<usize>,
    peptide_seq: Option<usize>,
    cdna_seq: Option<usize>,
    sequences_projected: bool,
    raw_object_json: Option<usize>,
    object_hash: Option<usize>,
}

impl TranslationColumnIndices {
    pub fn new(col_map: &ColumnMap) -> Self {
        let cdna_coding_start = col_map.get("cdna_coding_start");
        let cdna_coding_end = col_map.get("cdna_coding_end");
        let peptide_seq = col_map.get("translation_seq");
        let cdna_seq = col_map.get("cds_sequence");
        let sequences_projected = peptide_seq.is_some() || cdna_seq.is_some();
        Self {
            chrom: col_map.get("chrom"),
            start: col_map.get("start"),
            end: col_map.get("end"),
            stable_id: col_map.get("stable_id"),
            version: col_map.get("version"),
            translation_start: col_map.get("translation_start"),
            translation_end: col_map.get("translation_end"),
            protein_length: col_map.get("protein_len"),
            transcript_stable_id: col_map.get("transcript_id"),
            gene_stable_id: col_map.get("gene_stable_id"),
            cdna_coding_start,
            cdna_coding_end,
            cds_len: col_map.get("cds_len"),
            peptide_seq,
            cdna_seq,
            sequences_projected,
            raw_object_json: col_map.get("raw_object_json"),
            object_hash: col_map.get("object_hash"),
        }
    }
}

// ---------------------------------------------------------------------------
// Text-line parser – returns bool (at most 1 translation per transcript)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_translation_line_into(
    line: &str,
    source_file_str: &str,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &TranslationColumnIndices,
    provenance: &ProvenanceWriter,
    seen: &mut HashSet<String>,
) -> Result<bool> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(false);
    }

    let mut split_iter = trimmed.splitn(4, '\t');
    let part0 = split_iter
        .next()
        .ok_or_else(|| exec_err(format!("Malformed translation row in {source_file_str}")))?;
    let part1 = split_iter
        .next()
        .ok_or_else(|| exec_err(format!("Malformed translation row in {source_file_str}")))?;
    let part2 = split_iter
        .next()
        .ok_or_else(|| exec_err(format!("Malformed translation row in {source_file_str}")))?;
    let part3 = split_iter
        .next()
        .ok_or_else(|| exec_err(format!("Malformed translation row in {source_file_str}")))?;

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
            "Unknown serializer for translation entity. serialiser_type missing in info.txt under {}",
            cache_info.cache_root.display()
        ))
    })?;

    let payload = decode_payload(serializer, part3)?;
    let object = payload.as_object().ok_or_else(|| {
        exec_err(format!(
            "Translation payload must be a JSON object in {source_file_str}"
        ))
    })?;

    // Check if transcript has a translation sub-object
    let translation_obj = match object
        .get("translation")
        .and_then(unwrap_blessed_object_optional)
    {
        Some(t) => t,
        None => return Ok(false), // non-coding transcript
    };

    let chrom = if let Some(c) = prefix_chrom {
        c.to_string()
    } else {
        json_str(object.get("chr").or_else(|| object.get("chrom"))).ok_or_else(|| {
            exec_err(format!(
                "Translation row missing required chrom in {source_file_str}"
            ))
        })?
    };

    let source_start = prefix_start
        .or_else(|| json_i64(object.get("start")))
        .ok_or_else(|| {
            exec_err(format!(
                "Translation row missing required start in {source_file_str}"
            ))
        })?;
    let source_end = prefix_end
        .or_else(|| json_i64(object.get("end")))
        .ok_or_else(|| {
            exec_err(format!(
                "Translation row missing required end in {source_file_str}"
            ))
        })?;

    let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
    let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

    if !predicate.matches(&chrom, start, end) {
        return Ok(false);
    }

    let transcript_stable_id = json_str(object.get("stable_id")).ok_or_else(|| {
        exec_err(format!(
            "Translation row missing transcript stable_id in {source_file_str}"
        ))
    })?;
    let gene_stable_id = json_str(
        object
            .get("gene_stable_id")
            .or_else(|| object.get("_gene_stable_id")),
    );

    // Skip Gnomon transcripts and LOC-prefixed gene pseudo-records.
    let source_val = json_str(object.get("source").or_else(|| object.get("_source_cache")));
    if source_val.as_deref() == Some("Gnomon") {
        return Ok(false);
    }
    if transcript_stable_id.starts_with("LOC") {
        return Ok(false);
    }
    if let Some(bt) = json_str(object.get("biotype")) {
        if is_excluded_biotype(&bt) {
            return Ok(false);
        }
    }

    // Deduplicate: same transcript can appear in multiple region bins.
    if !seen.insert(transcript_stable_id.clone()) {
        return Ok(false);
    }

    if let Some(idx) = col_idx.chrom {
        batch.set_utf8(idx, &chrom);
    }
    if let Some(idx) = col_idx.start {
        batch.set_i64(idx, start);
    }
    if let Some(idx) = col_idx.end {
        batch.set_i64(idx, end);
    }
    if let Some(idx) = col_idx.stable_id {
        batch.set_opt_utf8_owned(idx, json_str(translation_obj.get("stable_id")).as_ref());
    }
    if let Some(idx) = col_idx.version {
        batch.set_opt_i32(idx, json_i32(translation_obj.get("version")));
    }
    if let Some(idx) = col_idx.translation_start {
        batch.set_opt_i64(idx, json_i64(translation_obj.get("start")));
    }
    if let Some(idx) = col_idx.translation_end {
        batch.set_opt_i64(idx, json_i64(translation_obj.get("end")));
    }
    if let Some(idx) = col_idx.protein_length {
        batch.set_opt_i64(idx, json_i64(translation_obj.get("length")));
    }
    if let Some(idx) = col_idx.transcript_stable_id {
        batch.set_utf8(idx, &transcript_stable_id);
    }
    if let Some(idx) = col_idx.gene_stable_id {
        batch.set_opt_utf8_owned(idx, gene_stable_id.as_ref());
    }
    let cdna_coding_start_val = json_i64(object.get("cdna_coding_start"));
    let cdna_coding_end_val = json_i64(object.get("cdna_coding_end"));
    if let Some(idx) = col_idx.cdna_coding_start {
        batch.set_opt_i64(idx, cdna_coding_start_val);
    }
    if let Some(idx) = col_idx.cdna_coding_end {
        batch.set_opt_i64(idx, cdna_coding_end_val);
    }
    if let Some(idx) = col_idx.cds_len {
        let cds_len = match (cdna_coding_start_val, cdna_coding_end_val) {
            (Some(s), Some(e)) => Some(e - s + 1),
            _ => None,
        };
        batch.set_opt_i64(idx, cds_len);
    }

    // Sequences from _variation_effect_feature_cache — only parse when projected
    if col_idx.sequences_projected {
        let vef_cache = object
            .get("_variation_effect_feature_cache")
            .and_then(unwrap_blessed_object_optional);
        if let Some(idx) = col_idx.peptide_seq {
            batch.set_opt_utf8_owned(
                idx,
                vef_cache.and_then(|c| json_str(c.get("peptide"))).as_ref(),
            );
        }
        if let Some(idx) = col_idx.cdna_seq {
            batch.set_opt_utf8_owned(
                idx,
                vef_cache
                    .and_then(|c| json_str(c.get("translateable_seq")))
                    .as_ref(),
            );
        }
    }

    let need_json = col_idx.raw_object_json.is_some();
    let need_hash = col_idx.object_hash.is_some();
    if need_json || need_hash {
        let translation_val = object.get("translation").unwrap();
        let canonical_json = canonical_json_string(translation_val)?;
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
pub(crate) fn parse_translation_storable_file_into<F>(
    source_file: &Path,
    source_file_str: &str,
    predicate: &SimplePredicate,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &TranslationColumnIndices,
    provenance: &ProvenanceWriter,
    seen: &mut HashSet<String>,
    mut on_row_added: F,
) -> Result<()>
where
    F: FnMut(&mut BatchBuilder) -> Result<bool>,
{
    let mut process_item = |item: &SValue, region_key: &str| -> Result<bool> {
        let obj = item.as_hash().ok_or_else(|| {
            exec_err(format!(
                "Translation storable transcript must be a hash in {}",
                source_file.display()
            ))
        })?;

        // Check for translation sub-object
        let translation_obj = match obj.get("translation").and_then(SValue::as_hash) {
            Some(t) => t,
            None => return Ok(true), // non-coding transcript, skip
        };

        let chrom = sv_str(obj.get("chr").or_else(|| obj.get("chrom")))
            .or_else(|| {
                obj.get("slice")
                    .and_then(SValue::as_hash)
                    .and_then(|slice| sv_str(slice.get("seq_region_name")))
            })
            .unwrap_or_else(|| region_key.to_string());

        let source_start = sv_i64(obj.get("start")).ok_or_else(|| {
            exec_err(format!(
                "Translation storable transcript missing start in {}",
                source_file.display()
            ))
        })?;
        let source_end = sv_i64(obj.get("end")).ok_or_else(|| {
            exec_err(format!(
                "Translation storable transcript missing end in {}",
                source_file.display()
            ))
        })?;

        let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
        let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

        if !predicate.matches(&chrom, start, end) {
            return Ok(true);
        }

        let transcript_stable_id = sv_str(obj.get("stable_id")).ok_or_else(|| {
            exec_err(format!(
                "Translation storable transcript missing stable_id in {}",
                source_file.display()
            ))
        })?;
        let gene_stable_id = sv_str(
            obj.get("gene_stable_id")
                .or_else(|| obj.get("_gene_stable_id")),
        );

        // Skip Gnomon transcripts and LOC-prefixed gene pseudo-records.
        let source_val = sv_str(obj.get("source").or_else(|| obj.get("_source_cache")));
        if source_val.as_deref() == Some("Gnomon") {
            return Ok(true);
        }
        if transcript_stable_id.starts_with("LOC") {
            return Ok(true);
        }
        if let Some(bt) = sv_str(obj.get("biotype")) {
            if is_excluded_biotype(&bt) {
                return Ok(true);
            }
        }

        // Deduplicate: same transcript can appear in multiple region bins.
        if !seen.insert(transcript_stable_id.clone()) {
            return Ok(true);
        }

        if let Some(idx) = col_idx.chrom {
            batch.set_utf8(idx, &chrom);
        }
        if let Some(idx) = col_idx.start {
            batch.set_i64(idx, start);
        }
        if let Some(idx) = col_idx.end {
            batch.set_i64(idx, end);
        }
        if let Some(idx) = col_idx.stable_id {
            let value = sv_str(translation_obj.get("stable_id"));
            batch.set_opt_utf8_owned(idx, value.as_ref());
        }
        if let Some(idx) = col_idx.version {
            batch.set_opt_i32(
                idx,
                sv_i64(translation_obj.get("version")).and_then(|v| i32::try_from(v).ok()),
            );
        }
        if let Some(idx) = col_idx.translation_start {
            batch.set_opt_i64(idx, sv_i64(translation_obj.get("start")));
        }
        if let Some(idx) = col_idx.translation_end {
            batch.set_opt_i64(idx, sv_i64(translation_obj.get("end")));
        }
        if let Some(idx) = col_idx.protein_length {
            batch.set_opt_i64(idx, sv_i64(translation_obj.get("length")));
        }
        if let Some(idx) = col_idx.transcript_stable_id {
            batch.set_utf8(idx, &transcript_stable_id);
        }
        if let Some(idx) = col_idx.gene_stable_id {
            batch.set_opt_utf8_owned(idx, gene_stable_id.as_ref());
        }
        let cdna_coding_start_val = sv_i64(obj.get("cdna_coding_start"));
        let cdna_coding_end_val = sv_i64(obj.get("cdna_coding_end"));
        if let Some(idx) = col_idx.cdna_coding_start {
            batch.set_opt_i64(idx, cdna_coding_start_val);
        }
        if let Some(idx) = col_idx.cdna_coding_end {
            batch.set_opt_i64(idx, cdna_coding_end_val);
        }
        if let Some(idx) = col_idx.cds_len {
            let cds_len = match (cdna_coding_start_val, cdna_coding_end_val) {
                (Some(s), Some(e)) => Some(e - s + 1),
                _ => None,
            };
            batch.set_opt_i64(idx, cds_len);
        }

        // Sequences from _variation_effect_feature_cache — only parse when projected
        if col_idx.sequences_projected {
            if let Some(vef_cache) = obj
                .get("_variation_effect_feature_cache")
                .and_then(SValue::as_hash)
            {
                if let Some(idx) = col_idx.peptide_seq {
                    let value = sv_str(vef_cache.get("peptide"));
                    batch.set_opt_utf8_owned(idx, value.as_ref());
                }
                if let Some(idx) = col_idx.cdna_seq {
                    let value = sv_str(vef_cache.get("translateable_seq"));
                    batch.set_opt_utf8_owned(idx, value.as_ref());
                }
            }
        }

        let need_json = col_idx.raw_object_json.is_some();
        let need_hash = col_idx.object_hash.is_some();
        if need_json || need_hash {
            let translation_sval = obj.get("translation").unwrap();
            let canonical_json = canonical_storable_json_string(translation_sval);
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

        on_row_added(batch)
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
            "Failed streaming storable translation payload from {}: {}",
            source_file.display(),
            e
        ))
    })?;

    Ok(())
}

fn unwrap_blessed_object(
    value: &serde_json::Value,
) -> Result<&serde_json::Map<String, serde_json::Value>> {
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

fn unwrap_blessed_object_optional(
    value: &serde_json::Value,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
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
