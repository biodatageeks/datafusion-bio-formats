use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, canonical_json_string as canonical_storable_json_string,
    collect_nstore_alias_counts_and_top_keys_from_reader,
    stream_nstore_top_hash_array_items_keyed_with_alias_counts_from_reader, sv_i64, sv_str,
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
use std::collections::HashMap;
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
    peptide_seq_canonical: Option<usize>,
    cdna_seq_canonical: Option<usize>,
    sequences_projected: bool,
    protein_features: Option<usize>,
    protein_features_projected: bool,
    sift_predictions: Option<usize>,
    polyphen_predictions: Option<usize>,
    predictions_projected: bool,
    raw_object_json: Option<usize>,
    object_hash: Option<usize>,
}

impl TranslationColumnIndices {
    pub fn new(col_map: &ColumnMap) -> Self {
        let cdna_coding_start = col_map.get("cdna_coding_start");
        let cdna_coding_end = col_map.get("cdna_coding_end");
        let peptide_seq = col_map.get("translation_seq");
        let cdna_seq = col_map.get("cds_sequence");
        let peptide_seq_canonical = col_map.get("translation_seq_canonical");
        let cdna_seq_canonical = col_map.get("cds_sequence_canonical");
        let sequences_projected = peptide_seq.is_some()
            || cdna_seq.is_some()
            || peptide_seq_canonical.is_some()
            || cdna_seq_canonical.is_some();
        let protein_features = col_map.get("protein_features");
        let protein_features_projected = protein_features.is_some();
        let sift_predictions = col_map.get("sift_predictions");
        let polyphen_predictions = col_map.get("polyphen_predictions");
        let predictions_projected = sift_predictions.is_some() || polyphen_predictions.is_some();
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
            peptide_seq_canonical,
            cdna_seq_canonical,
            sequences_projected,
            protein_features,
            protein_features_projected,
            sift_predictions,
            polyphen_predictions,
            predictions_projected,
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

    // Skip LOC-prefixed gene pseudo-records.
    if transcript_stable_id.starts_with("LOC") {
        return Ok(false);
    }
    if let Some(bt) = json_str(object.get("biotype"))
        && is_excluded_biotype(&bt)
    {
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

    // Sequences, protein features, and predictions from _variation_effect_feature_cache.
    if col_idx.sequences_projected
        || col_idx.protein_features_projected
        || col_idx.predictions_projected
    {
        let vef_cache = object
            .get("_variation_effect_feature_cache")
            .and_then(unwrap_blessed_object_optional);
        let edited_peptide = vef_cache.and_then(|c| json_str(c.get("peptide")));
        let edited_cds = vef_cache.and_then(|c| json_str(c.get("translateable_seq")));
        if let Some(idx) = col_idx.peptide_seq {
            batch.set_opt_utf8_owned(idx, edited_peptide.as_ref());
        }
        if let Some(idx) = col_idx.cdna_seq {
            batch.set_opt_utf8_owned(idx, edited_cds.as_ref());
        }
        // Derive canonical (pre-BAM-edit) sequences by reversing the list of
        // `_rna_edit` attribute insertions on the edited CDS. For non-BAM-
        // edited transcripts this is a no-op (canonical ≡ edited). See the
        // RnaEdit docs above for coordinate semantics.
        //
        // The upstream extractor used to look for `translation.primary_seq`
        // or a top-level `translateable_seq` — neither is populated in raw
        // VEP merged caches (the primary_seq is a lazy Perl method, and
        // translateable_seq only appears inside `_variation_effect_feature_cache`).
        // Reversing the explicitly-stored edits is the only path that
        // actually recovers the canonical peptide for BAM-edited RefSeq.
        let edits_json = parse_rna_edits_json(object.get("attributes"));
        let (canonical_cds, canonical_peptide) = derive_canonical_sequences(
            edited_cds.as_deref(),
            edited_peptide.as_deref(),
            &edits_json,
            cdna_coding_start_val,
            cdna_coding_end_val,
        );
        if let Some(idx) = col_idx.peptide_seq_canonical {
            batch.set_opt_utf8_owned(idx, canonical_peptide.as_ref());
        }
        if let Some(idx) = col_idx.cdna_seq_canonical {
            batch.set_opt_utf8_owned(idx, canonical_cds.as_ref());
        }
        if let Some(idx) = col_idx.protein_features {
            let features = vef_cache.and_then(extract_protein_features_json);
            batch.set_protein_feature_list(idx, features.as_deref());
        }
        // SIFT/PolyPhen predictions — populated from pre-decoded data.
        // These are NULL when reading from raw VEP cache (binary matrices
        // require external pre-processing); they become populated when
        // reading from parquet caches that have been pre-computed.
        if col_idx.predictions_projected {
            let pfp = vef_cache.and_then(|c| {
                c.get("protein_function_predictions")
                    .and_then(unwrap_blessed_object_optional)
            });
            if let Some(idx) = col_idx.sift_predictions {
                let preds = pfp.and_then(|p| extract_predictions_json(p, "sift"));
                batch.set_prediction_list(idx, preds.as_deref());
            }
            if let Some(idx) = col_idx.polyphen_predictions {
                let preds = pfp.and_then(|p| extract_predictions_json(p, "polyphen_humvar"));
                batch.set_prediction_list(idx, preds.as_deref());
            }
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
    alias_prelude: Option<(HashMap<usize, usize>, Vec<String>)>,
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

        // Skip LOC-prefixed gene pseudo-records.
        if transcript_stable_id.starts_with("LOC") {
            return Ok(true);
        }
        if let Some(bt) = sv_str(obj.get("biotype"))
            && is_excluded_biotype(&bt)
        {
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

        // Sequences, protein features, and predictions from _variation_effect_feature_cache.
        if col_idx.sequences_projected
            || col_idx.protein_features_projected
            || col_idx.predictions_projected
        {
            let vef_cache = obj
                .get("_variation_effect_feature_cache")
                .and_then(SValue::as_hash);
            let edited_peptide = vef_cache.and_then(|c| sv_str(c.get("peptide")));
            let edited_cds = vef_cache.and_then(|c| sv_str(c.get("translateable_seq")));

            if let Some(idx) = col_idx.peptide_seq {
                batch.set_opt_utf8_owned(idx, edited_peptide.as_ref());
            }
            if let Some(idx) = col_idx.cdna_seq {
                batch.set_opt_utf8_owned(idx, edited_cds.as_ref());
            }
            // Reverse `_rna_edit` insertions on the edited CDS to recover
            // canonical. Non-edited transcripts: no-op (canonical ≡ edited).
            // See RnaEdit / derive_canonical_sequences for details.
            let edits = parse_rna_edits_storable(obj.get("attributes"));
            let (canonical_cds, canonical_peptide) = derive_canonical_sequences(
                edited_cds.as_deref(),
                edited_peptide.as_deref(),
                &edits,
                cdna_coding_start_val,
                cdna_coding_end_val,
            );
            if let Some(idx) = col_idx.peptide_seq_canonical {
                batch.set_opt_utf8_owned(idx, canonical_peptide.as_ref());
            }
            if let Some(idx) = col_idx.cdna_seq_canonical {
                batch.set_opt_utf8_owned(idx, canonical_cds.as_ref());
            }
            if let Some(vef_cache) = vef_cache {
                if let Some(idx) = col_idx.protein_features {
                    let features = extract_protein_features_storable(vef_cache);
                    batch.set_protein_feature_list(idx, features.as_deref());
                }
                // SIFT/PolyPhen predictions — populated from pre-decoded data.
                if col_idx.predictions_projected {
                    let pfp = vef_cache
                        .get("protein_function_predictions")
                        .and_then(SValue::as_hash);
                    if let Some(idx) = col_idx.sift_predictions {
                        let preds = pfp.and_then(|p| extract_predictions_storable(p, "sift"));
                        batch.set_prediction_list(idx, preds.as_deref());
                    }
                    if let Some(idx) = col_idx.polyphen_predictions {
                        let preds =
                            pfp.and_then(|p| extract_predictions_storable(p, "polyphen_humvar"));
                        batch.set_prediction_list(idx, preds.as_deref());
                    }
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

    let (alias_counts, entry_keys) = match alias_prelude {
        Some(prelude) => prelude,
        None => {
            collect_nstore_alias_counts_and_top_keys_from_reader(open_binary_reader(source_file)?)
                .map_err(|e| {
                exec_err(format!(
                    "Failed collecting storable alias references from {}: {}",
                    source_file.display(),
                    e
                ))
            })?
        }
    };

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

/// A single protein feature: (analysis, hseqname, start, end).
type ProteinFeature = (Option<String>, Option<String>, Option<i64>, Option<i64>);

/// Extract protein features from JSON `_variation_effect_feature_cache` object.
/// Path: `protein_features` array of ProteinFeature objects.
fn extract_protein_features_json(
    vef_cache: &serde_json::Map<String, serde_json::Value>,
) -> Option<Vec<ProteinFeature>> {
    let arr = vef_cache.get("protein_features")?.as_array()?;
    let features: Vec<ProteinFeature> = arr
        .iter()
        .filter_map(|item| {
            let obj = unwrap_blessed_object_optional(item).or_else(|| item.as_object())?;
            // Extract analysis display label from nested Analysis object.
            // Format 1: analysis.__value._display_label (blessed object)
            // Format 2: analysis._display_label (plain object)
            // Fallback: analysis.__value.logic_name or _analysis string.
            let analysis = obj
                .get("analysis")
                .and_then(|a| {
                    // Try unwrapping blessed object first, fall back to plain object
                    let analysis_obj =
                        unwrap_blessed_object_optional(a).or_else(|| a.as_object())?;
                    json_str(analysis_obj.get("_display_label"))
                        .or_else(|| json_str(analysis_obj.get("logic_name")))
                })
                .or_else(|| json_str(obj.get("_analysis")));
            let hseqname = json_str(obj.get("hseqname"));
            let start = json_i64(obj.get("start"));
            let end = json_i64(obj.get("end"));
            Some((analysis, hseqname, start, end))
        })
        .collect();
    if features.is_empty() {
        None
    } else {
        Some(features)
    }
}

/// Extract protein features from Storable `_variation_effect_feature_cache` hash.
fn extract_protein_features_storable(
    vef_cache: &std::collections::HashMap<String, SValue>,
) -> Option<Vec<ProteinFeature>> {
    let arr = vef_cache.get("protein_features")?.as_array()?;
    let features: Vec<ProteinFeature> = arr
        .iter()
        .filter_map(|item| {
            let obj = item.as_hash()?;
            let analysis = obj
                .get("analysis")
                .and_then(|a| {
                    // Storable blessed objects are auto-unwrapped, so just get the hash
                    let analysis_hash = a.as_hash()?;
                    sv_str(analysis_hash.get("_display_label"))
                        .or_else(|| sv_str(analysis_hash.get("logic_name")))
                })
                .or_else(|| sv_str(obj.get("_analysis")));
            let hseqname = sv_str(obj.get("hseqname"));
            let start = sv_i64(obj.get("start"));
            let end = sv_i64(obj.get("end"));
            Some((analysis, hseqname, start, end))
        })
        .collect();
    if features.is_empty() {
        None
    } else {
        Some(features)
    }
}

/// A single SIFT/PolyPhen prediction entry: (position, amino_acid, prediction, score).
type PredictionEntry = (i32, String, String, f32);

// ---------------------------------------------------------------------------
// VEP ProteinFunctionPredictionMatrix binary format decoder
// ---------------------------------------------------------------------------

/// VEP binary matrix header bytes.
const MATRIX_HEADER: &[u8] = b"VEP";

/// 20 standard amino acids in VEP's canonical order.
const ALL_AAS: [char; 20] = [
    'A', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W',
    'Y',
];
const NUM_AAS: usize = 20;
const BYTES_PER_PRED: usize = 2;

/// Number of bits used for the qualitative prediction code (ceil(log2(4)) = 2).
const NUM_PRED_BITS: u16 = 2;

/// Special marker for "no prediction" (reference amino acid at this position).
const NO_PREDICTION: u16 = 0xFFFF;

/// SIFT prediction codes (value → string).
const SIFT_PREDICTIONS: [&str; 4] = [
    "tolerated",
    "deleterious",
    "tolerated - low confidence",
    "deleterious - low confidence",
];

/// PolyPhen prediction codes (value → string).
const POLYPHEN_PREDICTIONS: [&str; 4] = [
    "probably damaging",
    "possibly damaging",
    "benign",
    "unknown",
];

/// Decode a VEP ProteinFunctionPredictionMatrix from raw bytes (already decompressed).
/// Returns prediction entries for all non-null positions.
fn decode_prediction_matrix(matrix: &[u8], analysis: &str) -> Option<Vec<PredictionEntry>> {
    // Validate header
    if matrix.len() < MATRIX_HEADER.len() || &matrix[..MATRIX_HEADER.len()] != MATRIX_HEADER {
        return None;
    }

    let data = &matrix[MATRIX_HEADER.len()..];
    let total_predictions = data.len() / BYTES_PER_PRED;
    if total_predictions == 0 || !data.len().is_multiple_of(BYTES_PER_PRED) {
        return None;
    }
    let protein_length = total_predictions / NUM_AAS;
    if protein_length == 0 {
        return None;
    }

    let pred_labels = match analysis {
        "sift" => &SIFT_PREDICTIONS,
        "polyphen_humvar" | "polyphen" => &POLYPHEN_PREDICTIONS,
        _ => return None,
    };

    let mut entries = Vec::new();
    for pos in 0..protein_length {
        for (aa_idx, &aa) in ALL_AAS.iter().enumerate() {
            let offset = (pos * NUM_AAS + aa_idx) * BYTES_PER_PRED;
            if offset + 1 >= data.len() {
                break;
            }
            let val = u16::from_le_bytes([data[offset], data[offset + 1]]);
            if val == NO_PREDICTION {
                continue;
            }
            let pred_code = (val >> (16 - NUM_PRED_BITS)) as usize;
            let score_raw = val & 0x3FF; // bottom 10 bits
            let score = score_raw as f32 / 1000.0;

            if pred_code < pred_labels.len() {
                entries.push((
                    (pos + 1) as i32, // 1-based position
                    aa.to_string(),
                    pred_labels[pred_code].to_string(),
                    score,
                ));
            }
        }
    }

    if entries.is_empty() {
        None
    } else {
        Some(entries)
    }
}

/// Decompress gzip data and decode the prediction matrix.
fn decode_compressed_matrix(compressed: &[u8], analysis: &str) -> Option<Vec<PredictionEntry>> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(compressed);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).ok()?;
    decode_prediction_matrix(&decompressed, analysis)
}

/// Extract SIFT/PolyPhen predictions from JSON `protein_function_predictions`.
/// The `key` is "sift" or "polyphen_humvar".
///
/// Supports pre-decoded format (array of {position, amino_acid, prediction, score}).
/// Binary matrix data cannot be represented in JSON, so raw VEP cache files
/// decoded through the JSON text path will return None.
fn extract_predictions_json(
    pfp: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<Vec<PredictionEntry>> {
    let predictor = pfp.get(key)?;
    // Pre-decoded format: array of {position, amino_acid, prediction, score}
    let arr = predictor.as_array().or_else(|| {
        unwrap_blessed_object_optional(predictor)
            .and_then(|obj| obj.get("predictions"))
            .and_then(|v| v.as_array())
    })?;
    let entries: Vec<PredictionEntry> = arr
        .iter()
        .filter_map(|item| {
            let obj = item
                .as_object()
                .or_else(|| unwrap_blessed_object_optional(item))?;
            let position = json_i64(obj.get("position")).and_then(|v| i32::try_from(v).ok())?;
            let amino_acid = json_str(obj.get("amino_acid"))?;
            let prediction = json_str(obj.get("prediction"))?;
            let score = obj.get("score").and_then(|v| {
                v.as_f64()
                    .or_else(|| json_str(Some(v))?.parse::<f64>().ok())
            })? as f32;
            Some((position, amino_acid, prediction, score))
        })
        .collect();
    if entries.is_empty() {
        None
    } else {
        Some(entries)
    }
}

/// Extract SIFT/PolyPhen predictions from Storable `protein_function_predictions`.
///
/// Handles both:
/// 1. Raw VEP cache: blessed ProteinFunctionPredictionMatrix with gzip-compressed
///    `matrix` binary blob — decoded natively via `decode_compressed_matrix()`.
/// 2. Pre-decoded format: hash with `predictions` array of structured entries.
fn extract_predictions_storable(
    pfp: &std::collections::HashMap<String, SValue>,
    key: &str,
) -> Option<Vec<PredictionEntry>> {
    let predictor = pfp.get(key)?;
    let obj = predictor.as_hash()?;

    // Check for raw binary matrix (from VEP cache Storable files).
    if let Some(matrix_data) = obj.get("matrix").and_then(SValue::as_bytes) {
        let is_compressed = obj
            .get("matrix_compressed")
            .and_then(SValue::as_i64)
            .unwrap_or(0)
            != 0;
        let analysis = obj
            .get("analysis")
            .and_then(SValue::as_string)
            .unwrap_or_else(|| key.to_string());
        return if is_compressed {
            decode_compressed_matrix(matrix_data, &analysis)
        } else {
            decode_prediction_matrix(matrix_data, &analysis)
        };
    }

    // Fallback: pre-decoded format with "predictions" array.
    let arr = obj.get("predictions").and_then(SValue::as_array)?;
    let entries: Vec<PredictionEntry> = arr
        .iter()
        .filter_map(|item| {
            let entry = item.as_hash()?;
            let position = sv_i64(entry.get("position")).and_then(|v| i32::try_from(v).ok())?;
            let amino_acid = sv_str(entry.get("amino_acid"))?;
            let prediction = sv_str(entry.get("prediction"))?;
            let score = sv_i64(entry.get("score"))
                .map(|v| v as f32)
                .or_else(|| sv_str(entry.get("score")).and_then(|s| s.parse::<f32>().ok()))?;
            Some((position, amino_acid, prediction, score))
        })
        .collect();
    if entries.is_empty() {
        None
    } else {
        Some(entries)
    }
}

// ---------------------------------------------------------------------------
// RNA-edit (BAM-edit) reversal for canonical sequence recovery.
//
// Raw VEP merged caches for BAM-edited RefSeq transcripts carry the edited
// peptide / CDS in `_variation_effect_feature_cache.{peptide,translateable_seq}`.
// They do NOT separately serialize the pre-edit (canonical) versions —
// `translation.primary_seq` is a lazy Perl method, not a stored field. What
// IS stored is the list of edits as `_rna_edit` attribute entries on the
// transcript, e.g. `"256 255 GCAGCA"` meaning "insert GCAGCA between cdna
// positions 255 and 256". Reversing those insertions on the BAM-edited CDS
// reconstructs the canonical CDS, which is then translated to the canonical
// peptide.
//
// Edit semantics follow Ensembl `Bio::EnsEMBL::SeqEdit`:
//   start, end = 1-based cdna coordinates. If `end < start` (conventionally
//   end = start - 1) the edit is a pure insertion between `end` and `start`.
//   `alt` is the inserted bases. The edit is applied with
//   `substr($seq, $start - 1, $end - $start + 1) = $alt`, so a 0-len slice at
//   0-indexed position `start - 1` receives `alt`.
//
// Ensembl applies edits in ascending `start` order against the current
// sequence, so each edit's coordinates reference the already-edited sequence
// produced by preceding edits. To undo we traverse in DESCENDING start order
// and remove `alt.len()` bytes at position `start - 1` (0-indexed) — later
// undoes cannot disturb earlier positions because they act above them.
// ---------------------------------------------------------------------------

/// A single RNA edit parsed from an Ensembl `_rna_edit` attribute value.
///
/// Coordinates are 1-based cdna (transcript-relative). `end < start` denotes
/// a pure insertion (the BAM-edit case).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RnaEdit {
    pub start: i64,
    pub end: i64,
    pub alt: String,
}

impl RnaEdit {
    /// Parse an attribute value like `"256 255 GCAGCA"` into a structured edit.
    ///
    /// Returns `None` on malformed values (missing fields, non-integer
    /// coordinates, etc.) rather than erroring — the caller skips unparseable
    /// edits without aborting translation ingestion.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split_whitespace();
        let start = parts.next()?.parse::<i64>().ok()?;
        let end = parts.next()?.parse::<i64>().ok()?;
        let alt = parts.next().unwrap_or("").to_string();
        // Remaining tokens (if any) ignored — VEP's format has exactly 3.
        Some(Self { start, end, alt })
    }

    /// True if this edit is a pure insertion (no bases replaced).
    pub(crate) fn is_pure_insertion(&self) -> bool {
        self.end + 1 == self.start && !self.alt.is_empty()
    }
}

/// Extract `_rna_edit` attributes from a Storable transcript attributes array.
///
/// Each element of `attributes` is expected to be a blessed
/// `Bio::EnsEMBL::Attribute` hash with `code` and `value` keys. Entries whose
/// `code` is not `"_rna_edit"` are skipped. Unparseable values are skipped.
fn parse_rna_edits_storable(attributes: Option<&SValue>) -> Vec<RnaEdit> {
    let Some(array) = attributes.and_then(SValue::as_array) else {
        return Vec::new();
    };
    array
        .iter()
        .filter_map(|attr| {
            let obj = attr.as_hash()?;
            let code = sv_str(obj.get("code"))?;
            if code != "_rna_edit" {
                return None;
            }
            let value = sv_str(obj.get("value"))?;
            RnaEdit::parse(&value)
        })
        .collect()
}

/// JSON-path equivalent of [`parse_rna_edits_storable`]. The JSON caches wrap
/// attributes as blessed objects (payload under `__value`) so we unwrap first.
fn parse_rna_edits_json(attributes: Option<&serde_json::Value>) -> Vec<RnaEdit> {
    let Some(arr) = attributes.and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|attr| {
            let obj = unwrap_blessed_object_optional(attr).or_else(|| attr.as_object())?;
            let code = json_str(obj.get("code"))?;
            if code != "_rna_edit" {
                return None;
            }
            let value = json_str(obj.get("value"))?;
            RnaEdit::parse(&value)
        })
        .collect()
}

/// Reverse the given `_rna_edit` insertions against the edited cdna / CDS
/// string to recover the pre-edit sequence.
///
/// Only pure insertions are undone. Deletions or substitutions leave the
/// sequence shorter than it would be in pre-edit form *and* require the
/// original bases (which the attribute value does not carry), so this helper
/// bails out (returns `None`) if any non-insertion edit appears. That signals
/// to the caller "the cache lacks enough information to recover canonical" —
/// the canonical column is then left null rather than populated with a
/// half-undone mix.
///
/// `coord_offset` is subtracted from each edit's 1-based cdna start to
/// translate into the coordinate space of `edited`:
/// - `0` when `edited` is the spliced (cdna) sequence
/// - `cdna_coding_start - 1` when `edited` is the translateable (CDS)
///   sequence — this filters out edits that fall outside the CDS as a
///   side effect.
fn undo_rna_edit_insertions(
    edited: &str,
    edits: &[RnaEdit],
    coord_offset: i64,
    keep_range: Option<(i64, i64)>,
) -> Option<String> {
    let mut relevant: Vec<&RnaEdit> = edits
        .iter()
        .filter(|e| match keep_range {
            Some((lo, hi)) => e.start >= lo && e.start <= hi,
            None => true,
        })
        .collect();
    relevant.sort_by(|a, b| b.start.cmp(&a.start)); // descending
    let mut seq: Vec<u8> = edited.as_bytes().to_vec();
    for edit in relevant {
        if !edit.is_pure_insertion() {
            return None;
        }
        let offset_start = edit.start - coord_offset;
        if offset_start < 1 {
            // Insertion is before the window `edited` represents — for a CDS
            // window this means the edit is in the 5' UTR, outside the CDS.
            // Skip it silently.
            continue;
        }
        let start_idx = (offset_start - 1) as usize;
        let end_idx = start_idx + edit.alt.len();
        if end_idx > seq.len() {
            return None;
        }
        // Sanity check: the bytes we're about to remove must equal the edit's
        // alt payload. If they don't, our coordinate / ordering assumption is
        // wrong and we should bail instead of silently corrupting the result.
        if &seq[start_idx..end_idx] != edit.alt.as_bytes() {
            return None;
        }
        seq.drain(start_idx..end_idx);
    }
    String::from_utf8(seq).ok()
}

/// Translate a CDS byte slice to a peptide string using NCBI translation
/// table 1 (the standard code), trimming at the first stop codon.
///
/// Returns `None` if the CDS length is not a multiple of 3 or contains
/// non-ACGT characters that can't be resolved unambiguously. Ambiguous IUPAC
/// codes are not supported — BAM-edited RefSeq and Ensembl canonical CDSes
/// in the cache are unambiguous, so this is fine in practice.
fn translate_cds_table1(cds: &str) -> Option<String> {
    let bytes = cds.as_bytes();
    if !bytes.len().is_multiple_of(3) {
        return None;
    }
    let mut peptide = String::with_capacity(bytes.len() / 3);
    for chunk in bytes.chunks_exact(3) {
        let codon = [
            chunk[0].to_ascii_uppercase(),
            chunk[1].to_ascii_uppercase(),
            chunk[2].to_ascii_uppercase(),
        ];
        let aa = codon_table1(codon)?;
        if aa == '*' {
            break;
        }
        peptide.push(aa);
    }
    Some(peptide)
}

#[inline]
fn codon_table1(codon: [u8; 3]) -> Option<char> {
    // NCBI translation table 1 (standard). Stop codons = '*'.
    match codon {
        [b'T', b'T', b'T'] | [b'T', b'T', b'C'] => Some('F'),
        [b'T', b'T', b'A'] | [b'T', b'T', b'G'] => Some('L'),
        [b'C', b'T', _] => Some('L'),
        [b'A', b'T', b'T'] | [b'A', b'T', b'C'] | [b'A', b'T', b'A'] => Some('I'),
        [b'A', b'T', b'G'] => Some('M'),
        [b'G', b'T', _] => Some('V'),
        [b'T', b'C', _] => Some('S'),
        [b'C', b'C', _] => Some('P'),
        [b'A', b'C', _] => Some('T'),
        [b'G', b'C', _] => Some('A'),
        [b'T', b'A', b'T'] | [b'T', b'A', b'C'] => Some('Y'),
        [b'T', b'A', b'A'] | [b'T', b'A', b'G'] => Some('*'),
        [b'C', b'A', b'T'] | [b'C', b'A', b'C'] => Some('H'),
        [b'C', b'A', b'A'] | [b'C', b'A', b'G'] => Some('Q'),
        [b'A', b'A', b'T'] | [b'A', b'A', b'C'] => Some('N'),
        [b'A', b'A', b'A'] | [b'A', b'A', b'G'] => Some('K'),
        [b'G', b'A', b'T'] | [b'G', b'A', b'C'] => Some('D'),
        [b'G', b'A', b'A'] | [b'G', b'A', b'G'] => Some('E'),
        [b'T', b'G', b'T'] | [b'T', b'G', b'C'] => Some('C'),
        [b'T', b'G', b'A'] => Some('*'),
        [b'T', b'G', b'G'] => Some('W'),
        [b'C', b'G', _] => Some('R'),
        [b'A', b'G', b'T'] | [b'A', b'G', b'C'] => Some('S'),
        [b'A', b'G', b'A'] | [b'A', b'G', b'G'] => Some('R'),
        [b'G', b'G', _] => Some('G'),
        _ => None,
    }
}

/// Compute canonical CDS + peptide from the BAM-edited sequences and the
/// list of `_rna_edit` attributes.
///
/// Returns `(canonical_cds, canonical_peptide)`. Either may be `None` if the
/// input is missing or an edit cannot be reversed cleanly. The typical
/// non-BAM-edited transcript has `edits.is_empty()` and both canonical values
/// are returned unchanged (equal to the BAM-edited copies).
fn derive_canonical_sequences(
    edited_cds: Option<&str>,
    edited_peptide: Option<&str>,
    edits: &[RnaEdit],
    cdna_coding_start: Option<i64>,
    cdna_coding_end: Option<i64>,
) -> (Option<String>, Option<String>) {
    if edits.is_empty() {
        // Non-BAM-edited transcript — canonical ≡ edited.
        return (
            edited_cds.map(|s| s.to_string()),
            edited_peptide.map(|s| s.to_string()),
        );
    }
    let (Some(cds), Some(coding_start), Some(coding_end)) =
        (edited_cds, cdna_coding_start, cdna_coding_end)
    else {
        // Can't locate CDS within the cdna sequence — bail.
        return (None, None);
    };
    let canonical_cds = undo_rna_edit_insertions(
        cds,
        edits,
        coding_start - 1,
        Some((coding_start, coding_end)),
    );
    let canonical_peptide = canonical_cds
        .as_deref()
        .and_then(translate_cds_table1)
        .or_else(|| edited_peptide.map(|s| s.to_string()));
    (canonical_cds, canonical_peptide)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic VEP prediction matrix for 2 positions × 20 amino acids.
    fn build_test_matrix(analysis: &str) -> Vec<u8> {
        let mut matrix = Vec::new();
        matrix.extend_from_slice(MATRIX_HEADER); // "VEP"

        // Position 1: set prediction for amino acid 'A' (index 0) and 'C' (index 1)
        // All others are NO_PREDICTION (0xFFFF)
        for aa_idx in 0..NUM_AAS {
            let val: u16 = match aa_idx {
                0 => {
                    // A: prediction code 0, score 0.5 → 500
                    (0u16 << (16 - NUM_PRED_BITS)) | 500
                }
                1 => {
                    // C: prediction code 1, score 0.02 → 20
                    (1u16 << (16 - NUM_PRED_BITS)) | 20
                }
                _ => NO_PREDICTION,
            };
            matrix.extend_from_slice(&val.to_le_bytes());
        }

        // Position 2: all NO_PREDICTION except 'D' (index 2)
        for aa_idx in 0..NUM_AAS {
            let val: u16 = if aa_idx == 2 {
                // D: prediction code 2, score 0.85 → 850
                (2u16 << (16 - NUM_PRED_BITS)) | 850
            } else {
                NO_PREDICTION
            };
            matrix.extend_from_slice(&val.to_le_bytes());
        }

        // Verify it's the right analysis to pick correct labels
        assert!(analysis == "sift" || analysis == "polyphen_humvar");
        matrix
    }

    #[test]
    fn decode_sift_matrix() {
        let matrix = build_test_matrix("sift");
        let entries = decode_prediction_matrix(&matrix, "sift").unwrap();

        assert_eq!(entries.len(), 3);

        // Position 1, A: tolerated, 0.5
        assert_eq!(entries[0].0, 1); // position
        assert_eq!(entries[0].1, "A"); // amino acid
        assert_eq!(entries[0].2, "tolerated"); // prediction
        assert!((entries[0].3 - 0.5).abs() < 0.001); // score

        // Position 1, C: deleterious, 0.02
        assert_eq!(entries[1].0, 1);
        assert_eq!(entries[1].1, "C");
        assert_eq!(entries[1].2, "deleterious");
        assert!((entries[1].3 - 0.02).abs() < 0.001);

        // Position 2, D: tolerated - low confidence, 0.85
        assert_eq!(entries[2].0, 2);
        assert_eq!(entries[2].1, "D");
        assert_eq!(entries[2].2, "tolerated - low confidence");
        assert!((entries[2].3 - 0.85).abs() < 0.001);
    }

    #[test]
    fn decode_polyphen_matrix() {
        let matrix = build_test_matrix("polyphen_humvar");
        let entries = decode_prediction_matrix(&matrix, "polyphen_humvar").unwrap();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].2, "probably damaging");
        assert_eq!(entries[1].2, "possibly damaging");
        assert_eq!(entries[2].2, "benign");
    }

    #[test]
    fn decode_matrix_bad_header() {
        let matrix = b"BAD".to_vec();
        assert!(decode_prediction_matrix(&matrix, "sift").is_none());
    }

    #[test]
    fn decode_matrix_empty() {
        assert!(decode_prediction_matrix(b"VEP", "sift").is_none());
    }

    #[test]
    fn decode_compressed_matrix_roundtrip() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let matrix = build_test_matrix("sift");

        // Gzip compress the matrix
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&matrix).unwrap();
        let compressed = encoder.finish().unwrap();

        let entries = decode_compressed_matrix(&compressed, "sift").unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].2, "tolerated");
        assert_eq!(entries[1].2, "deleterious");
    }

    #[test]
    fn decode_unknown_analysis_returns_none() {
        let matrix = build_test_matrix("sift");
        assert!(decode_prediction_matrix(&matrix, "unknown_tool").is_none());
    }

    #[test]
    fn protein_features_json_display_label_blessed() {
        let vef_cache = serde_json::json!({
            "protein_features": [
                {
                    "__class": "Bio::EnsEMBL::ProteinFeature",
                    "__value": {
                        "analysis": {
                            "__class": "Bio::EnsEMBL::Analysis",
                            "__value": { "_display_label": "Pfam" }
                        },
                        "hseqname": "PF02083",
                        "start": "128",
                        "end": "139"
                    }
                }
            ]
        });
        let map = vef_cache.as_object().unwrap();
        let features = extract_protein_features_json(map).unwrap();
        assert_eq!(features.len(), 1);
        assert_eq!(features[0].0.as_deref(), Some("Pfam"));
        assert_eq!(features[0].1.as_deref(), Some("PF02083"));
        assert_eq!(features[0].2, Some(128));
        assert_eq!(features[0].3, Some(139));
    }

    #[test]
    fn protein_features_json_display_label_plain() {
        let vef_cache = serde_json::json!({
            "protein_features": [
                {
                    "analysis": { "_display_label": "SMART" },
                    "hseqname": "SM00220",
                    "start": "10",
                    "end": "50"
                }
            ]
        });
        let map = vef_cache.as_object().unwrap();
        let features = extract_protein_features_json(map).unwrap();
        assert_eq!(features[0].0.as_deref(), Some("SMART"));
    }

    #[test]
    fn protein_features_json_logic_name_fallback() {
        let vef_cache = serde_json::json!({
            "protein_features": [
                {
                    "analysis": { "logic_name": "gene3d" },
                    "hseqname": "1.10.150.50",
                    "start": "1",
                    "end": "100"
                }
            ]
        });
        let map = vef_cache.as_object().unwrap();
        let features = extract_protein_features_json(map).unwrap();
        assert_eq!(features[0].0.as_deref(), Some("gene3d"));
    }

    #[test]
    fn protein_features_storable_display_label() {
        let mut analysis = std::collections::HashMap::new();
        analysis.insert(
            "_display_label".to_string(),
            SValue::String(std::sync::Arc::from("Pfam")),
        );

        let mut feature = std::collections::HashMap::new();
        feature.insert(
            "analysis".to_string(),
            SValue::Hash(std::sync::Arc::new(analysis)),
        );
        feature.insert(
            "hseqname".to_string(),
            SValue::String(std::sync::Arc::from("PF02083")),
        );
        feature.insert("start".to_string(), SValue::Int(128));
        feature.insert("end".to_string(), SValue::Int(139));

        let mut vef_cache = std::collections::HashMap::new();
        vef_cache.insert(
            "protein_features".to_string(),
            SValue::Array(std::sync::Arc::new(vec![SValue::Hash(
                std::sync::Arc::new(feature),
            )])),
        );

        let features = extract_protein_features_storable(&vef_cache).unwrap();
        assert_eq!(features.len(), 1);
        assert_eq!(features[0].0.as_deref(), Some("Pfam"));
    }

    #[test]
    fn protein_features_json_analysis_string_fallback() {
        let vef_cache = serde_json::json!({
            "protein_features": [
                {
                    "_analysis": "CDD",
                    "hseqname": "cd00001",
                    "start": "5",
                    "end": "55"
                }
            ]
        });
        let map = vef_cache.as_object().unwrap();
        let features = extract_protein_features_json(map).unwrap();
        assert_eq!(features[0].0.as_deref(), Some("CDD"));
    }

    #[test]
    fn protein_features_json_display_label_over_logic_name() {
        // When both _display_label and logic_name exist, _display_label wins
        let vef_cache = serde_json::json!({
            "protein_features": [
                {
                    "analysis": {
                        "_display_label": "Gene3D",
                        "logic_name": "gene3d"
                    },
                    "hseqname": "1.10.150.50",
                    "start": "1",
                    "end": "100"
                }
            ]
        });
        let map = vef_cache.as_object().unwrap();
        let features = extract_protein_features_json(map).unwrap();
        assert_eq!(features[0].0.as_deref(), Some("Gene3D"));
    }

    #[test]
    fn protein_features_json_empty_array_returns_none() {
        let vef_cache = serde_json::json!({
            "protein_features": []
        });
        let map = vef_cache.as_object().unwrap();
        assert!(extract_protein_features_json(map).is_none());
    }

    #[test]
    fn protein_features_storable_analysis_string_fallback() {
        let mut feature = std::collections::HashMap::new();
        feature.insert(
            "_analysis".to_string(),
            SValue::String(std::sync::Arc::from("CDD")),
        );
        feature.insert(
            "hseqname".to_string(),
            SValue::String(std::sync::Arc::from("cd00001")),
        );
        feature.insert("start".to_string(), SValue::Int(5));
        feature.insert("end".to_string(), SValue::Int(55));

        let mut vef_cache = std::collections::HashMap::new();
        vef_cache.insert(
            "protein_features".to_string(),
            SValue::Array(std::sync::Arc::new(vec![SValue::Hash(
                std::sync::Arc::new(feature),
            )])),
        );

        let features = extract_protein_features_storable(&vef_cache).unwrap();
        assert_eq!(features[0].0.as_deref(), Some("CDD"));
    }

    #[test]
    fn protein_features_storable_display_label_over_logic_name() {
        let mut analysis = std::collections::HashMap::new();
        analysis.insert(
            "_display_label".to_string(),
            SValue::String(std::sync::Arc::from("Gene3D")),
        );
        analysis.insert(
            "logic_name".to_string(),
            SValue::String(std::sync::Arc::from("gene3d")),
        );

        let mut feature = std::collections::HashMap::new();
        feature.insert(
            "analysis".to_string(),
            SValue::Hash(std::sync::Arc::new(analysis)),
        );
        feature.insert(
            "hseqname".to_string(),
            SValue::String(std::sync::Arc::from("1.10.150.50")),
        );
        feature.insert("start".to_string(), SValue::Int(1));
        feature.insert("end".to_string(), SValue::Int(100));

        let mut vef_cache = std::collections::HashMap::new();
        vef_cache.insert(
            "protein_features".to_string(),
            SValue::Array(std::sync::Arc::new(vec![SValue::Hash(
                std::sync::Arc::new(feature),
            )])),
        );

        let features = extract_protein_features_storable(&vef_cache).unwrap();
        assert_eq!(features[0].0.as_deref(), Some("Gene3D"));
    }

    #[test]
    fn protein_features_storable_empty_array_returns_none() {
        let mut vef_cache = std::collections::HashMap::new();
        vef_cache.insert(
            "protein_features".to_string(),
            SValue::Array(std::sync::Arc::new(vec![])),
        );
        assert!(extract_protein_features_storable(&vef_cache).is_none());
    }

    // -----------------------------------------------------------------------
    // RnaEdit parsing + reversal + codon-table translation
    //
    // These cover the canonical-sequence recovery path: parse `_rna_edit`
    // attribute values, drop insertions back out of the BAM-edited CDS, and
    // translate the recovered CDS with the standard codon table.
    // -----------------------------------------------------------------------

    #[test]
    fn rna_edit_parse_insertion_form() {
        // The BAM-edited NM_002111.8 HTT polyQ edit has exactly this shape.
        let edit = RnaEdit::parse("256 255 GCAGCA").expect("parse");
        assert_eq!(edit.start, 256);
        assert_eq!(edit.end, 255);
        assert_eq!(edit.alt, "GCAGCA");
        assert!(edit.is_pure_insertion());
    }

    #[test]
    fn rna_edit_parse_rejects_malformed() {
        assert!(RnaEdit::parse("").is_none());
        assert!(RnaEdit::parse("not a number").is_none());
        assert!(RnaEdit::parse("256").is_none());
        // Extra whitespace-separated tokens are tolerated (VEP stores exactly
        // 3 but the parser shouldn't choke if the format loosens later).
        let e = RnaEdit::parse("10 9 ACG trailing").expect("parse tolerant");
        assert_eq!(e.start, 10);
        assert_eq!(e.alt, "ACG");
    }

    #[test]
    fn rna_edit_is_pure_insertion_requires_end_plus_one_equals_start() {
        assert!(RnaEdit::parse("256 255 A").unwrap().is_pure_insertion());
        // end == start → substitution, not a pure insertion.
        assert!(!RnaEdit::parse("256 256 A").unwrap().is_pure_insertion());
        // Empty alt (deletion) → not a pure insertion.
        assert!(!RnaEdit::parse("256 255 ").unwrap().is_pure_insertion());
    }

    #[test]
    fn parse_rna_edits_storable_filters_by_code_and_extracts_value() {
        let mut e1 = std::collections::HashMap::new();
        e1.insert(
            "code".to_string(),
            SValue::String(std::sync::Arc::from("_rna_edit")),
        );
        e1.insert(
            "value".to_string(),
            SValue::String(std::sync::Arc::from("256 255 GCAGCA")),
        );
        let mut e2 = std::collections::HashMap::new();
        e2.insert(
            "code".to_string(),
            SValue::String(std::sync::Arc::from("MANE_Select")),
        );
        e2.insert(
            "value".to_string(),
            SValue::String(std::sync::Arc::from("NM_001388492.1")),
        );
        let attrs = SValue::Array(std::sync::Arc::new(vec![
            SValue::Hash(std::sync::Arc::new(e1)),
            SValue::Hash(std::sync::Arc::new(e2)),
        ]));
        let edits = parse_rna_edits_storable(Some(&attrs));
        assert_eq!(edits.len(), 1, "MANE_Select attribute must be ignored");
        assert_eq!(edits[0].alt, "GCAGCA");
    }

    #[test]
    fn parse_rna_edits_json_handles_blessed_wrapper() {
        let attrs = serde_json::json!([
            {
                "__class": "Bio::EnsEMBL::Attribute",
                "__value": { "code": "_rna_edit", "value": "256 255 GCAGCA" }
            },
            {
                "__class": "Bio::EnsEMBL::Attribute",
                "__value": { "code": "TSL", "value": "tsl1" }
            }
        ]);
        let edits = parse_rna_edits_json(Some(&attrs));
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].start, 256);
    }

    #[test]
    fn undo_rna_edit_removes_insertion_and_recovers_original_cds() {
        // Synthetic: insert GCAGCA at cdna 111 (= mid-CDS with cdna offset 145).
        // Edit value: start=111, end=110, alt=GCAGCA.
        let edited_cds = "AAACCAGCAGGGG"; // "AAA"+"CC"+"AGCAG"+"GGG"+"G" — contrived.
        // We want to simulate: pre-edit was "AAACCGGGG" (9 nt). Edit inserts
        // "AGCAG" starting at cdna 6 (which in CDS space after offset=0 means
        // position 6). Hmm let me re-plan this test.
        let _ = edited_cds;
        // Pre-edit CDS: "AAACCAGGG" (9 nt, codons AAA, CCA, GGG → K P G).
        // Insert "CAG" at cdna 7 (between cdna 6 and 7) → "AAACCACAGGG"?
        // Actually let's simulate the real HTT edit in miniature.
        // Pre-edit CDS: "ATGCAGCAGCCCCCC" (5 codons M Q Q P P, 15 nt)
        // Edit: insert "CAGCAG" (6 nt, 2 Q codons) between cdna 6 and 7
        //   value = "7 6 CAGCAG"
        // Post-edit CDS: "ATGCAG" + "CAGCAG" + "CAGCCCCCC"
        //   = "ATGCAGCAGCAGCAGCCCCCC" (21 nt, 7 codons M Q Q Q Q P P)
        let pre_edit = "ATGCAGCAGCCCCCC";
        let post_edit = "ATGCAGCAGCAGCAGCCCCCC";
        let edit = RnaEdit::parse("7 6 CAGCAG").unwrap();
        let recovered = undo_rna_edit_insertions(post_edit, &[edit], 0, None)
            .expect("insertion must reverse cleanly");
        assert_eq!(recovered, pre_edit);
    }

    #[test]
    fn undo_rna_edit_multiple_insertions_applied_descending() {
        // Simulate Ensembl applying edits in ascending-start order, each edit
        // referencing the CURRENT (post-previous-edit) sequence.
        //
        //   pre              = "0123456789"                (10 chars)
        //   edit 1 (s=5,e=4,alt=XX)  inserts before cdna 5 of pre
        //     → "0123XX456789"                             (12 chars, = post_1)
        //   edit 2 (s=10,e=9,alt=YY) inserts before cdna 10 of post_1
        //     (cdna 10 of post_1 is 0-indexed 9 = char '7')
        //     → "0123XX456YY789"                           (14 chars, = post_both)
        //
        // Undoing in descending start order — edit 2 then edit 1 — must take
        // post_both back to pre exactly. Lower-position edit's bytes are not
        // disturbed by the higher-position undo, so the second pass finds
        // its alt at the expected coordinates.
        let pre = "0123456789";
        let post_both = "0123XX456YY789";
        let edits = vec![
            RnaEdit {
                start: 5,
                end: 4,
                alt: "XX".to_string(),
            },
            RnaEdit {
                start: 10,
                end: 9,
                alt: "YY".to_string(),
            },
        ];
        let recovered =
            undo_rna_edit_insertions(post_both, &edits, 0, None).expect("two-edit reverse");
        assert_eq!(recovered, pre);
    }

    #[test]
    fn undo_rna_edit_bails_on_non_insertion() {
        // A substitution-shape edit (start == end, single alt char) can't be
        // reversed without the original base → helper must return None.
        let edits = vec![RnaEdit {
            start: 5,
            end: 5,
            alt: "X".to_string(),
        }];
        assert!(undo_rna_edit_insertions("AAAAXAAAA", &edits, 0, None).is_none());
    }

    #[test]
    fn undo_rna_edit_bails_on_coordinate_mismatch() {
        // alt="XX" but the bytes at that position are "AA" → our coordinate /
        // ordering model is wrong for this input; bail instead of corrupting.
        let edits = vec![RnaEdit {
            start: 3,
            end: 2,
            alt: "XX".to_string(),
        }];
        assert!(undo_rna_edit_insertions("AAAAAA", &edits, 0, None).is_none());
    }

    #[test]
    fn undo_rna_edit_keep_range_skips_utr_edits() {
        // Edit outside the CDS window — skipped silently so the CDS reversal
        // still succeeds when a 3'UTR edit is part of the attribute list.
        let cds = "ATGAAAGGGCCC"; // 12 nt, 4 codons M K G P
        let edits = vec![RnaEdit {
            start: 200, // well past the CDS end
            end: 199,
            alt: "TTT".to_string(),
        }];
        let recovered = undo_rna_edit_insertions(cds, &edits, 0, Some((1, 12))).expect("no-op");
        assert_eq!(recovered, cds);
    }

    #[test]
    fn translate_cds_table1_stops_at_first_stop() {
        // ATG TTT TTA TAA AAA → M F L * (stop) — peptide = "MFL".
        assert_eq!(
            translate_cds_table1("ATGTTTTTATAAAAA").as_deref(),
            Some("MFL")
        );
    }

    #[test]
    fn translate_cds_table1_rejects_non_triplet_length() {
        assert!(translate_cds_table1("ATGAA").is_none());
    }

    #[test]
    fn translate_cds_table1_handles_lowercase_and_ambiguity() {
        // Lowercase is folded; non-ACGT resolves to None.
        assert_eq!(translate_cds_table1("atgggg").as_deref(), Some("MG"));
        assert!(translate_cds_table1("ATGNNN").is_none());
    }

    #[test]
    fn derive_canonical_sequences_is_identity_when_no_edits() {
        let (cds, pep) =
            derive_canonical_sequences(Some("ATGAAA"), Some("MK"), &[], Some(1), Some(6));
        assert_eq!(cds.as_deref(), Some("ATGAAA"));
        assert_eq!(pep.as_deref(), Some("MK"));
    }

    #[test]
    fn derive_canonical_sequences_reverses_cds_and_retranslates() {
        // Miniature HTT-style: pre-edit CDS encodes M Q P (9 nt).
        // BAM edit inserts "CAG" (1 Q) between cdna 6 and 7 → edited encodes
        // M Q Q P (12 nt). Canonical recovery drops the Q back to M Q P.
        let edited_cds = "ATGCAGCAGCCC"; // 12 nt = MQQP
        let edited_peptide = "MQQP";
        let edits = vec![RnaEdit::parse("7 6 CAG").unwrap()];
        let (cds, pep) = derive_canonical_sequences(
            Some(edited_cds),
            Some(edited_peptide),
            &edits,
            Some(1),
            Some(12),
        );
        assert_eq!(cds.as_deref(), Some("ATGCAGCCC"));
        assert_eq!(pep.as_deref(), Some("MQP"));
    }

    #[test]
    fn derive_canonical_sequences_returns_none_cds_when_edit_irreversible() {
        // Substitution-shape edit → canonical CDS can't be recovered → None.
        let edits = vec![RnaEdit {
            start: 4,
            end: 4,
            alt: "X".to_string(),
        }];
        let (cds, pep) =
            derive_canonical_sequences(Some("ATGXAA"), Some("MK"), &edits, Some(1), Some(6));
        assert!(
            cds.is_none(),
            "canonical CDS must be None when unreversible"
        );
        // Peptide still falls back to the BAM-edited peptide rather than being
        // left null — downstream HGVSp consumers can still render *something*.
        assert_eq!(pep.as_deref(), Some("MK"));
    }

    // -----------------------------------------------------------------------
    // HTT polyQ regression against a real VEP merged cache sample.
    //
    // Ignored by default because it reaches into an absolute path outside the
    // repo. Run locally with:
    //   cargo test --lib translation::tests::htt_canonical_from_real_merged_cache \
    //     -- --ignored --nocapture
    // -----------------------------------------------------------------------
    #[test]
    #[ignore = "requires /Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged cache"]
    fn htt_canonical_from_real_merged_cache() {
        let path = std::path::Path::new(
            "/Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged/115_GRCh38/4/3000001-4000000.gz",
        );
        if !path.exists() {
            println!("SKIP: cache not found at {}", path.display());
            return;
        }
        let reader = open_binary_reader(path).expect("open reader");
        let (alias_counts, entry_keys) =
            collect_nstore_alias_counts_and_top_keys_from_reader(reader).expect("aliases");
        let reader = open_binary_reader(path).expect("open reader 2");

        let mut refseq_canonical: Option<String> = None;
        let mut refseq_edited: Option<String> = None;
        let mut ensembl_canonical: Option<String> = None;
        let mut ensembl_edited: Option<String> = None;

        stream_nstore_top_hash_array_items_keyed_with_alias_counts_from_reader(
            reader,
            alias_counts,
            entry_keys,
            |_region, item| {
                let Some(obj) = item.as_hash() else {
                    return Ok(true);
                };
                let stable_id = sv_str(obj.get("stable_id")).unwrap_or_default();
                if !matches!(stable_id.as_str(), "NM_002111.8" | "ENST00000355072") {
                    return Ok(true);
                }
                let vef_cache = obj
                    .get("_variation_effect_feature_cache")
                    .and_then(SValue::as_hash);
                let edited_cds = vef_cache.and_then(|c| sv_str(c.get("translateable_seq")));
                let edited_peptide = vef_cache.and_then(|c| sv_str(c.get("peptide")));
                let edits = parse_rna_edits_storable(obj.get("attributes"));
                let cdna_coding_start = sv_i64(obj.get("cdna_coding_start"));
                let cdna_coding_end = sv_i64(obj.get("cdna_coding_end"));
                let (cds_canon, pep_canon) = derive_canonical_sequences(
                    edited_cds.as_deref(),
                    edited_peptide.as_deref(),
                    &edits,
                    cdna_coding_start,
                    cdna_coding_end,
                );
                match stable_id.as_str() {
                    "NM_002111.8" => {
                        refseq_edited = edited_peptide;
                        refseq_canonical = pep_canon;
                        println!(
                            "NM_002111.8 edited_cds_len={:?} canonical_cds_len={:?}",
                            edited_cds.as_ref().map(|s| s.len()),
                            cds_canon.as_ref().map(|s| s.len()),
                        );
                    }
                    "ENST00000355072" => {
                        ensembl_edited = edited_peptide;
                        ensembl_canonical = pep_canon;
                    }
                    _ => {}
                }
                Ok(true)
            },
        )
        .expect("stream");

        // Ensembl has no BAM edits → canonical must equal edited exactly.
        let ensembl_edited = ensembl_edited.expect("Ensembl ENST00000355072 not found");
        let ensembl_canonical = ensembl_canonical.expect("Ensembl canonical missing");
        assert_eq!(ensembl_edited, ensembl_canonical);
        assert_eq!(
            ensembl_edited.len(),
            3142,
            "Ensembl HTT peptide should be 3142 AA (21-Q canonical)"
        );

        // RefSeq NM_002111.8 has an _rna_edit that inserts 2 Qs. Edited = 3144,
        // canonical = 3142 once we reverse the edit. And the two sequences must
        // differ by exactly two Q insertions at the polyQ tract.
        let refseq_edited = refseq_edited.expect("NM_002111.8 not found");
        let refseq_canonical = refseq_canonical.expect("NM_002111.8 canonical missing");
        assert_eq!(
            refseq_edited.len(),
            3144,
            "BAM-edited NP_002102.4 = 3144 AA"
        );
        assert_eq!(
            refseq_canonical.len(),
            3142,
            "canonical NP_002102.4 (BAM edit reversed) = 3142 AA"
        );
        // Canonical NP_002102.4 should match Ensembl ENSP00000347184's peptide
        // (both come from the same GRCh38-derived CDS).
        assert_eq!(
            refseq_canonical, ensembl_canonical,
            "RefSeq canonical peptide must equal Ensembl's peptide at the same locus"
        );

        // Concrete polyQ check: canonical must have 21 Qs at positions 18-38
        // and P at position 39 (this is what makes VEP's HGVSp say Pro39...).
        let q_run: String = refseq_canonical[17..]
            .chars()
            .take_while(|c| *c == 'Q')
            .collect();
        assert_eq!(
            q_run.len(),
            21,
            "canonical HTT polyQ tract should be 21 residues long"
        );
        assert_eq!(
            refseq_canonical.as_bytes()[38],
            b'P',
            "canonical HTT position 39 (0-indexed 38) should be Pro"
        );
    }
}
