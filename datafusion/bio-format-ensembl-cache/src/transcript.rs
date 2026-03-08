use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, canonical_json_string as canonical_storable_json_string,
    collect_nstore_alias_counts_and_top_keys_from_reader,
    stream_nstore_top_hash_array_items_keyed_with_alias_counts_from_reader,
};
use crate::errors::{Result, exec_err};
use crate::exon::{is_excluded_biotype, is_exon_stable_id};
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
// TranscriptColumnIndices – pre-computed builder indices from ColumnMap
// ---------------------------------------------------------------------------

pub(crate) struct TranscriptColumnIndices {
    chrom: Option<usize>,
    start: Option<usize>,
    end: Option<usize>,
    strand: Option<usize>,
    stable_id: Option<usize>,
    version: Option<usize>,
    biotype: Option<usize>,
    source: Option<usize>,
    is_canonical: Option<usize>,
    gene_stable_id: Option<usize>,
    gene_symbol: Option<usize>,
    gene_symbol_source: Option<usize>,
    gene_hgnc_id: Option<usize>,
    refseq_id: Option<usize>,
    coding_region_start: Option<usize>,
    coding_region_end: Option<usize>,
    cdna_coding_start: Option<usize>,
    cdna_coding_end: Option<usize>,
    translation_stable_id: Option<usize>,
    translation_start: Option<usize>,
    translation_end: Option<usize>,
    translation_projected: bool,
    exon_count: Option<usize>,
    // VEP-related columns
    exons: Option<usize>,
    exons_projected: bool,
    cdna_seq: Option<usize>,
    peptide_seq: Option<usize>,
    sequences_projected: bool,
    codon_table: Option<usize>,
    tsl: Option<usize>,
    mane_select: Option<usize>,
    mane_plus_clinical: Option<usize>,
    raw_object_json: Option<usize>,
    object_hash: Option<usize>,
}

impl TranscriptColumnIndices {
    pub fn new(col_map: &ColumnMap) -> Self {
        let translation_stable_id = col_map.get("translation_stable_id");
        let translation_start = col_map.get("translation_start");
        let translation_end = col_map.get("translation_end");
        let translation_projected = translation_stable_id.is_some()
            || translation_start.is_some()
            || translation_end.is_some();

        let exons = col_map.get("exons");
        let exons_projected = exons.is_some();

        let cdna_seq = col_map.get("cdna_seq");
        let peptide_seq = col_map.get("peptide_seq");
        let sequences_projected = cdna_seq.is_some() || peptide_seq.is_some();

        Self {
            chrom: col_map.get("chrom"),
            start: col_map.get("start"),
            end: col_map.get("end"),
            strand: col_map.get("strand"),
            stable_id: col_map.get("stable_id"),
            version: col_map.get("version"),
            biotype: col_map.get("biotype"),
            source: col_map.get("source"),
            is_canonical: col_map.get("is_canonical"),
            gene_stable_id: col_map.get("gene_stable_id"),
            gene_symbol: col_map.get("gene_symbol"),
            gene_symbol_source: col_map.get("gene_symbol_source"),
            gene_hgnc_id: col_map.get("gene_hgnc_id"),
            refseq_id: col_map.get("refseq_id"),
            coding_region_start: col_map.get("cds_start"),
            coding_region_end: col_map.get("cds_end"),
            cdna_coding_start: col_map.get("cdna_coding_start"),
            cdna_coding_end: col_map.get("cdna_coding_end"),
            translation_stable_id,
            translation_start,
            translation_end,
            translation_projected,
            exon_count: col_map.get("exon_count"),
            exons,
            exons_projected,
            cdna_seq,
            peptide_seq,
            sequences_projected,
            codon_table: col_map.get("codon_table"),
            tsl: col_map.get("tsl"),
            mane_select: col_map.get("mane_select"),
            mane_plus_clinical: col_map.get("mane_plus_clinical"),
            raw_object_json: col_map.get("raw_object_json"),
            object_hash: col_map.get("object_hash"),
        }
    }
}

struct TranscriptRowCore {
    chrom: String,
    start: i64,
    end: i64,
    /// Raw 1-based source start (before coordinate normalization).
    source_start: i64,
    /// Raw 1-based source end (before coordinate normalization).
    source_end: i64,
    strand: i8,
    stable_id: String,
}

// ---------------------------------------------------------------------------
// cDNA → genomic coordinate mapping
// ---------------------------------------------------------------------------

/// Maps a 1-based cDNA position to a 1-based genomic coordinate using the
/// exon array and strand.  Returns `None` if the position falls outside the
/// exon coverage (shouldn't happen for well-formed caches).
///
/// Exons in `_trans_exon_array` are stored in transcript order (5'→3').
/// For forward strand (+1) this means ascending genomic order; for reverse
/// strand (−1) this means descending genomic order.
///
/// Each exon spans `|end − start| + 1` bases in cDNA space (VEP uses 1-based
/// inclusive coordinates).
fn cdna_to_genomic(exon_coords: &[(i64, i64)], strand: i8, cdna_pos: i64) -> Option<i64> {
    let mut accumulated: i64 = 0;
    for &(exon_start, exon_end) in exon_coords {
        let exon_len = (exon_end - exon_start).abs() + 1;
        if cdna_pos <= accumulated + exon_len {
            let offset = cdna_pos - accumulated - 1; // 0-based offset within exon
            return if strand >= 0 {
                Some(exon_start + offset)
            } else {
                Some(exon_end - offset)
            };
        }
        accumulated += exon_len;
    }
    None
}

/// Returns the storable exon array, trying `_trans_exon_array` first and
/// falling back to `_variation_effect_feature_cache.sorted_exons`.
///
/// The primary array is only used when it contains at least one parseable
/// exon hash (with a `stable_id`).  Otherwise we fall back to `sorted_exons`
/// which always contains fully materialised exon objects.
fn storable_exon_array<'a>(
    object: &'a std::collections::BTreeMap<String, SValue>,
) -> Option<&'a [SValue]> {
    let primary = object.get("_trans_exon_array").and_then(SValue::as_array);
    let has_parseable = primary
        .map(|arr| {
            arr.iter().any(|v| {
                v.as_hash()
                    .and_then(|h| sv_str(h.get("stable_id")))
                    .is_some()
            })
        })
        .unwrap_or(false);

    if has_parseable {
        primary
    } else {
        object
            .get("_variation_effect_feature_cache")
            .and_then(SValue::as_hash)
            .and_then(|vef| vef.get("sorted_exons"))
            .and_then(SValue::as_array)
            .or(primary)
    }
}

/// Returns the JSON exon array, trying `_trans_exon_array` first and
/// falling back to `_variation_effect_feature_cache.sorted_exons`.
///
/// The primary array is only used when it contains at least one parseable
/// blessed object.  Otherwise we fall back to `sorted_exons`.
fn json_exon_array<'a>(object: &'a serde_json::Map<String, Value>) -> Option<&'a Vec<Value>> {
    let primary = object.get("_trans_exon_array").and_then(Value::as_array);
    let has_parseable = primary
        .map(|arr| {
            arr.iter()
                .any(|v| unwrap_blessed_object_optional(v).is_some())
        })
        .unwrap_or(false);

    if has_parseable {
        primary
    } else {
        object
            .get("_variation_effect_feature_cache")
            .and_then(unwrap_blessed_object_optional)
            .and_then(|vef| vef.get("sorted_exons"))
            .and_then(Value::as_array)
            .or(primary)
    }
}

/// Extracts exon (start, end) pairs from a storable transcript object.
fn extract_exon_coords_storable(
    object: &std::collections::BTreeMap<String, SValue>,
) -> Option<Vec<(i64, i64)>> {
    let arr = storable_exon_array(object)?;
    let coords: Vec<(i64, i64)> = arr
        .iter()
        .filter_map(|exon_val| {
            let exon_obj = exon_val.as_hash()?;
            // Skip non-exon objects (slices, transcripts, genes).
            let id = sv_str(exon_obj.get("stable_id"))?;
            if !is_exon_stable_id(&id) {
                return None;
            }
            let start = sv_i64(exon_obj.get("start"))?;
            let end = sv_i64(exon_obj.get("end"))?;
            Some((start, end))
        })
        .collect();
    if coords.is_empty() {
        None
    } else {
        Some(coords)
    }
}

/// Extracts exon (start, end) pairs from a JSON transcript object.
fn extract_exon_coords_json(object: &serde_json::Map<String, Value>) -> Option<Vec<(i64, i64)>> {
    let arr = json_exon_array(object)?;
    let coords: Vec<(i64, i64)> = arr
        .iter()
        .filter_map(|exon_val| {
            let exon_obj = unwrap_blessed_object_optional(exon_val)?;
            // Skip non-exon objects (slices, transcripts, genes).
            let id = json_str(exon_obj.get("stable_id"))?;
            if !is_exon_stable_id(&id) {
                return None;
            }
            let start = json_i64(exon_obj.get("start"))?;
            let end = json_i64(exon_obj.get("end"))?;
            Some((start, end))
        })
        .collect();
    if coords.is_empty() {
        None
    } else {
        Some(coords)
    }
}

/// Computes `(coding_region_start, coding_region_end)` in 1-based genomic
/// coordinates from `cdna_coding_start`, `cdna_coding_end`, the exon array,
/// and the strand.  Returns `(None, None)` if any required input is missing
/// or if the result fails a sanity check (e.g. when storable alias eviction
/// causes exon coordinates to be incomplete).
///
/// `tx_start`/`tx_end` are the transcript's genomic span (1-based) used as a
/// sanity bound: the coding region must fall within the transcript.
fn derive_coding_region(
    exon_coords: &[(i64, i64)],
    strand: i8,
    cdna_coding_start: Option<i64>,
    cdna_coding_end: Option<i64>,
    tx_start: i64,
    tx_end: i64,
) -> (Option<i64>, Option<i64>) {
    let (Some(cds_start), Some(cds_end)) = (cdna_coding_start, cdna_coding_end) else {
        return (None, None);
    };
    let genomic_a = cdna_to_genomic(exon_coords, strand, cds_start);
    let genomic_b = cdna_to_genomic(exon_coords, strand, cds_end);
    match (genomic_a, genomic_b) {
        (Some(a), Some(b)) => {
            let cr_start = a.min(b);
            let cr_end = a.max(b);
            // Sanity: coding region must fall within the transcript span.
            // Storable alias eviction can cause incomplete exon arrays which
            // produce bogus coordinates — discard those.
            if cr_start >= tx_start && cr_end <= tx_end {
                (Some(cr_start), Some(cr_end))
            } else {
                (None, None)
            }
        }
        _ => (None, None),
    }
}

// ---------------------------------------------------------------------------
// Direct builder parser for text lines (Phase 1+2+6)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_transcript_line_into(
    line: &str,
    source_file_str: &str,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &TranscriptColumnIndices,
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
            "Malformed transcript row in {}: {}",
            source_file_str, trimmed
        ))
    })?;
    let part1 = split_iter.next().ok_or_else(|| {
        exec_err(format!(
            "Malformed transcript row in {}: {}",
            source_file_str, trimmed
        ))
    })?;
    let part2 = split_iter.next().ok_or_else(|| {
        exec_err(format!(
            "Malformed transcript row in {}: {}",
            source_file_str, trimmed
        ))
    })?;
    let part3 = split_iter.next().ok_or_else(|| {
        exec_err(format!(
            "Malformed transcript row in {}: {}",
            source_file_str, trimmed
        ))
    })?;

    // Phase 6: Earlier predicate evaluation BEFORE expensive decode_payload.
    // Extract chrom/start/end from the TSV prefix columns first.
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

    let payload = decode_payload(serializer, part3)?;
    let object = payload.as_object().ok_or_else(|| {
        exec_err(format!(
            "Transcript payload must be a JSON object in {}",
            source_file_str
        ))
    })?;

    let chrom = if let Some(c) = prefix_chrom {
        c.to_string()
    } else {
        json_str(object.get("chr").or_else(|| object.get("chrom"))).ok_or_else(|| {
            exec_err(format!(
                "Transcript row missing required chrom in {}: {}",
                source_file_str, trimmed
            ))
        })?
    };

    let source_start = prefix_start
        .or_else(|| json_i64(object.get("start")))
        .ok_or_else(|| {
            exec_err(format!(
                "Transcript row missing required start in {}: {}",
                source_file_str, trimmed
            ))
        })?;

    let source_end = prefix_end
        .or_else(|| json_i64(object.get("end")))
        .ok_or_else(|| {
            exec_err(format!(
                "Transcript row missing required end in {}: {}",
                source_file_str, trimmed
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
                source_file_str
            ))
        })?;

    let stable_id = json_str(object.get("stable_id")).ok_or_else(|| {
        exec_err(format!(
            "Transcript row missing required stable_id in {}",
            source_file_str
        ))
    })?;

    // Skip Gnomon (NCBI automated prediction) transcripts — VEP excludes
    // these even in --merged mode.
    let source_val = json_str(object.get("source").or_else(|| object.get("_source_cache")));
    if source_val.as_deref() == Some("Gnomon") {
        return Ok(false);
    }

    // Skip LOC-prefixed gene pseudo-records — these are gene-level entries,
    // not real transcripts. VEP skips them during annotation.
    if stable_id.starts_with("LOC") {
        return Ok(false);
    }

    // Skip generic pseudogene / aligned_transcript biotypes — VEP does not
    // use these for consequence annotation.
    if let Some(bt) = json_str(object.get("biotype")) {
        if is_excluded_biotype(&bt) {
            return Ok(false);
        }
    }

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
        batch.set_utf8(idx, &stable_id);
    }

    // Optional columns
    if let Some(idx) = col_idx.version {
        batch.set_opt_i32(idx, json_i32(object.get("version")));
    }
    if let Some(idx) = col_idx.biotype {
        batch.set_opt_utf8_owned(idx, json_str(object.get("biotype")).as_ref());
    }
    if let Some(idx) = col_idx.source {
        batch.set_opt_utf8_owned(
            idx,
            json_str(object.get("source").or_else(|| object.get("_source_cache"))).as_ref(),
        );
    }
    if let Some(idx) = col_idx.is_canonical {
        batch.set_opt_bool(idx, json_bool(object.get("is_canonical")));
    }
    if let Some(idx) = col_idx.gene_stable_id {
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
    if let Some(idx) = col_idx.gene_symbol {
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
    if let Some(idx) = col_idx.gene_symbol_source {
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
    if let Some(idx) = col_idx.gene_hgnc_id {
        batch.set_opt_utf8_owned(idx, json_str(object.get("gene_hgnc_id")).as_ref());
    }
    if let Some(idx) = col_idx.refseq_id {
        let refseq_id = json_str(object.get("refseq_id").or_else(|| object.get("_refseq")))
            .filter(|v| v != "-");
        batch.set_opt_utf8_owned(idx, refseq_id.as_ref());
    }
    // coding_region_start/end: read from object first; if null, derive from
    // cdna_coding_start/end + exon array (same derivation as storable path).
    let cdna_coding_start_val = json_i64(object.get("cdna_coding_start"));
    let cdna_coding_end_val = json_i64(object.get("cdna_coding_end"));

    let mut coding_region_start_val = json_i64(object.get("coding_region_start"));
    let mut coding_region_end_val = json_i64(object.get("coding_region_end"));

    if (col_idx.coding_region_start.is_some() || col_idx.coding_region_end.is_some())
        && coding_region_start_val.is_none()
        && coding_region_end_val.is_none()
    {
        if let Some(exon_coords) = extract_exon_coords_json(object) {
            let (derived_start, derived_end) = derive_coding_region(
                &exon_coords,
                strand,
                cdna_coding_start_val,
                cdna_coding_end_val,
                source_start,
                source_end,
            );
            coding_region_start_val = derived_start;
            coding_region_end_val = derived_end;
        }
    }

    if let Some(idx) = col_idx.coding_region_start {
        batch.set_opt_i64(
            idx,
            coding_region_start_val
                .map(|v| normalize_genomic_start(v, coordinate_system_zero_based)),
        );
    }
    if let Some(idx) = col_idx.coding_region_end {
        batch.set_opt_i64(
            idx,
            coding_region_end_val.map(|v| normalize_genomic_end(v, coordinate_system_zero_based)),
        );
    }
    if let Some(idx) = col_idx.cdna_coding_start {
        batch.set_opt_i64(idx, cdna_coding_start_val);
    }
    if let Some(idx) = col_idx.cdna_coding_end {
        batch.set_opt_i64(idx, cdna_coding_end_val);
    }

    // Translation sub-object
    if col_idx.translation_projected {
        if let Some(translation_obj) = object
            .get("translation")
            .and_then(unwrap_blessed_object_optional)
        {
            if let Some(idx) = col_idx.translation_stable_id {
                batch.set_opt_utf8_owned(idx, json_str(translation_obj.get("stable_id")).as_ref());
            }
            if let Some(idx) = col_idx.translation_start {
                batch.set_opt_i64(idx, json_i64(translation_obj.get("start")));
            }
            if let Some(idx) = col_idx.translation_end {
                batch.set_opt_i64(idx, json_i64(translation_obj.get("end")));
            }
        }
        // If translation is null, finish_row() will append nulls for these columns
    }

    if let Some(idx) = col_idx.exon_count {
        let exon_count = object
            .get("exon_count")
            .and_then(|v| json_i32(Some(v)))
            .or_else(|| json_exon_array(object).and_then(|arr| i32::try_from(arr.len()).ok()));
        batch.set_opt_i32(idx, exon_count);
    }

    // Exon structured array — only parse when projected
    if col_idx.exons_projected {
        if let Some(idx) = col_idx.exons {
            let exon_tuples = json_exon_array(object).map(|arr| {
                arr.iter()
                    .filter_map(|exon_val| {
                        let exon_obj = unwrap_blessed_object_optional(exon_val)?;
                        // Skip non-exon objects (slices, transcripts, genes).
                        let id = json_str(exon_obj.get("stable_id"))?;
                        if !is_exon_stable_id(&id) {
                            return None;
                        }
                        let start = json_i64(exon_obj.get("start"))
                            .map(|v| normalize_genomic_start(v, coordinate_system_zero_based))?;
                        let end = json_i64(exon_obj.get("end"))
                            .map(|v| normalize_genomic_end(v, coordinate_system_zero_based))?;
                        let phase = json_i64(exon_obj.get("phase"))
                            .and_then(|v| i8::try_from(v).ok())
                            .unwrap_or(-1);
                        Some((start, end, phase))
                    })
                    .collect::<Vec<(i64, i64, i8)>>()
            });
            batch.set_exon_list(idx, exon_tuples.as_deref());
        }
    }

    // Sequences from _variation_effect_feature_cache — only parse when projected
    if col_idx.sequences_projected {
        let vef_cache = object
            .get("_variation_effect_feature_cache")
            .and_then(unwrap_blessed_object_optional);
        if let Some(idx) = col_idx.cdna_seq {
            batch.set_opt_utf8_owned(
                idx,
                vef_cache
                    .and_then(|c| json_str(c.get("translateable_seq")))
                    .as_ref(),
            );
        }
        if let Some(idx) = col_idx.peptide_seq {
            batch.set_opt_utf8_owned(
                idx,
                vef_cache.and_then(|c| json_str(c.get("peptide"))).as_ref(),
            );
        }
    }

    // Simple scalar VEP fields
    if let Some(idx) = col_idx.codon_table {
        batch.set_opt_i32(idx, json_i32(object.get("codon_table")));
    }
    if let Some(idx) = col_idx.tsl {
        batch.set_opt_i32(idx, json_i32(object.get("tsl")));
    }
    if let Some(idx) = col_idx.mane_select {
        batch.set_opt_utf8_owned(idx, json_str(object.get("mane_select")).as_ref());
    }
    if let Some(idx) = col_idx.mane_plus_clinical {
        batch.set_opt_utf8_owned(idx, json_str(object.get("mane_plus_clinical")).as_ref());
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
pub(crate) fn parse_transcript_storable_file_into<F>(
    source_file: &Path,
    source_file_str: &str,
    predicate: &SimplePredicate,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &TranscriptColumnIndices,
    provenance: &ProvenanceWriter,
    mut on_row_added: F,
) -> Result<()>
where
    F: FnMut(&mut BatchBuilder) -> Result<bool>,
{
    let mut process_item = |item: &SValue, region_key: &str| -> Result<bool> {
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
            .unwrap_or_else(|| region_key.to_string());

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
            return Ok(true);
        }

        // Skip Gnomon (NCBI automated prediction) transcripts — VEP excludes
        // these even in --merged mode.
        let source_val = sv_str(obj.get("source").or_else(|| obj.get("_source_cache")));
        if source_val.as_deref() == Some("Gnomon") {
            return Ok(true);
        }

        // Skip LOC-prefixed gene pseudo-records.
        if stable_id.starts_with("LOC") {
            return Ok(true);
        }

        // Skip generic pseudogene / aligned_transcript biotypes.
        if let Some(bt) = sv_str(obj.get("biotype")) {
            if is_excluded_biotype(&bt) {
                return Ok(true);
            }
        }

        let core = TranscriptRowCore {
            chrom,
            start,
            end,
            source_start,
            source_end,
            strand,
            stable_id,
        };
        append_transcript_storable_row_into(
            item,
            obj,
            core,
            coordinate_system_zero_based,
            source_file_str,
            batch,
            col_idx,
            provenance,
        )?;

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
            "Failed streaming storable transcript payload from {}: {}",
            source_file.display(),
            e
        ))
    })?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_transcript_storable_row_into(
    payload: &SValue,
    object: &std::collections::BTreeMap<String, SValue>,
    core: TranscriptRowCore,
    coordinate_system_zero_based: bool,
    source_file_str: &str,
    batch: &mut BatchBuilder,
    col_idx: &TranscriptColumnIndices,
    provenance: &ProvenanceWriter,
) -> Result<()> {
    let TranscriptRowCore {
        chrom,
        start,
        end,
        source_start,
        source_end,
        strand,
        stable_id,
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
        batch.set_utf8(idx, &stable_id);
    }

    // Optional scalar columns
    if let Some(idx) = col_idx.version {
        batch.set_opt_i32(
            idx,
            sv_i64(object.get("version")).and_then(|v| i32::try_from(v).ok()),
        );
    }
    if let Some(idx) = col_idx.biotype {
        let value = sv_str(object.get("biotype"));
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.source {
        let value = sv_str(object.get("source").or_else(|| object.get("_source_cache")));
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.is_canonical {
        batch.set_opt_bool(idx, sv_bool(object.get("is_canonical")));
    }
    if let Some(idx) = col_idx.gene_stable_id {
        let value = sv_str(
            object
                .get("gene_stable_id")
                .or_else(|| object.get("_gene_stable_id")),
        );
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.gene_symbol {
        let value = sv_str(
            object
                .get("gene_symbol")
                .or_else(|| object.get("_gene_symbol")),
        );
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.gene_symbol_source {
        let value = sv_str(
            object
                .get("gene_symbol_source")
                .or_else(|| object.get("_gene_symbol_source")),
        );
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.gene_hgnc_id {
        let value = sv_str(object.get("gene_hgnc_id"));
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.refseq_id {
        let value =
            sv_str(object.get("refseq_id").or_else(|| object.get("_refseq"))).filter(|v| v != "-");
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    // coding_region_start/end: read from object first; if undef (common in
    // storable binary), derive from cdna_coding_start/end + exon array.
    let cdna_coding_start_val = sv_i64(object.get("cdna_coding_start"));
    let cdna_coding_end_val = sv_i64(object.get("cdna_coding_end"));

    let mut coding_region_start_val = sv_i64(object.get("coding_region_start"));
    let mut coding_region_end_val = sv_i64(object.get("coding_region_end"));

    if (col_idx.coding_region_start.is_some() || col_idx.coding_region_end.is_some())
        && coding_region_start_val.is_none()
        && coding_region_end_val.is_none()
    {
        if let Some(exon_coords) = extract_exon_coords_storable(object) {
            let (derived_start, derived_end) = derive_coding_region(
                &exon_coords,
                strand,
                cdna_coding_start_val,
                cdna_coding_end_val,
                source_start,
                source_end,
            );
            coding_region_start_val = derived_start;
            coding_region_end_val = derived_end;
        }
    }

    if let Some(idx) = col_idx.coding_region_start {
        batch.set_opt_i64(
            idx,
            coding_region_start_val
                .map(|value| normalize_genomic_start(value, coordinate_system_zero_based)),
        );
    }
    if let Some(idx) = col_idx.coding_region_end {
        batch.set_opt_i64(
            idx,
            coding_region_end_val
                .map(|value| normalize_genomic_end(value, coordinate_system_zero_based)),
        );
    }
    if let Some(idx) = col_idx.cdna_coding_start {
        batch.set_opt_i64(idx, cdna_coding_start_val);
    }
    if let Some(idx) = col_idx.cdna_coding_end {
        batch.set_opt_i64(idx, cdna_coding_end_val);
    }

    // Translation sub-object
    if col_idx.translation_projected {
        if let Some(translation_obj) = object.get("translation").and_then(SValue::as_hash) {
            if let Some(idx) = col_idx.translation_stable_id {
                let value = sv_str(translation_obj.get("stable_id"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.translation_start {
                batch.set_opt_i64(idx, sv_i64(translation_obj.get("start")));
            }
            if let Some(idx) = col_idx.translation_end {
                batch.set_opt_i64(idx, sv_i64(translation_obj.get("end")));
            }
        }
    }

    if let Some(idx) = col_idx.exon_count {
        let exon_count = sv_i64(object.get("exon_count"))
            .and_then(|v| i32::try_from(v).ok())
            .or_else(|| storable_exon_array(object).and_then(|arr| i32::try_from(arr.len()).ok()));
        batch.set_opt_i32(idx, exon_count);
    }

    // Exon structured array — only parse when projected
    if col_idx.exons_projected {
        if let Some(idx) = col_idx.exons {
            let exon_tuples = storable_exon_array(object).map(|exon_arr| {
                exon_arr
                    .iter()
                    .filter_map(|exon_val| {
                        let exon_obj = exon_val.as_hash()?;
                        // Skip non-exon objects (slices, transcripts, genes).
                        let sid = sv_str(exon_obj.get("stable_id"))?;
                        if !is_exon_stable_id(&sid) {
                            return None;
                        }
                        let start = sv_i64(exon_obj.get("start"))
                            .map(|v| normalize_genomic_start(v, coordinate_system_zero_based))?;
                        let end = sv_i64(exon_obj.get("end"))
                            .map(|v| normalize_genomic_end(v, coordinate_system_zero_based))?;
                        let phase = sv_i64(exon_obj.get("phase"))
                            .and_then(|v| i8::try_from(v).ok())
                            .unwrap_or(-1);
                        Some((start, end, phase))
                    })
                    .collect::<Vec<(i64, i64, i8)>>()
            });
            batch.set_exon_list(idx, exon_tuples.as_deref());
        }
    }

    // Sequences from _variation_effect_feature_cache — only parse when projected
    if col_idx.sequences_projected {
        if let Some(vef_cache) = object
            .get("_variation_effect_feature_cache")
            .and_then(SValue::as_hash)
        {
            if let Some(idx) = col_idx.cdna_seq {
                let value = sv_str(vef_cache.get("translateable_seq"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.peptide_seq {
                let value = sv_str(vef_cache.get("peptide"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
        }
    }

    // Simple scalar VEP fields
    if let Some(idx) = col_idx.codon_table {
        batch.set_opt_i32(
            idx,
            sv_i64(object.get("codon_table")).and_then(|v| i32::try_from(v).ok()),
        );
    }
    if let Some(idx) = col_idx.tsl {
        batch.set_opt_i32(
            idx,
            sv_i64(object.get("tsl")).and_then(|v| i32::try_from(v).ok()),
        );
    }
    if let Some(idx) = col_idx.mane_select {
        let value = sv_str(object.get("mane_select"));
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.mane_plus_clinical {
        let value = sv_str(object.get("mane_plus_clinical"));
        batch.set_opt_utf8_owned(idx, value.as_ref());
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
