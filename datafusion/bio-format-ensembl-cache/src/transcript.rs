use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, TopHashArrayEvent, canonical_json_string as canonical_storable_json_string,
    stream_nstore_top_hash_array_items_from_reader,
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
            coding_region_start: col_map.get("coding_region_start"),
            coding_region_end: col_map.get("coding_region_end"),
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
    strand: i8,
    stable_id: String,
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
    if let Some(idx) = col_idx.coding_region_start {
        batch.set_opt_i64(
            idx,
            json_i64(object.get("coding_region_start"))
                .map(|v| normalize_genomic_start(v, coordinate_system_zero_based)),
        );
    }
    if let Some(idx) = col_idx.coding_region_end {
        batch.set_opt_i64(
            idx,
            json_i64(object.get("coding_region_end"))
                .map(|v| normalize_genomic_end(v, coordinate_system_zero_based)),
        );
    }
    if let Some(idx) = col_idx.cdna_coding_start {
        batch.set_opt_i64(idx, json_i64(object.get("cdna_coding_start")));
    }
    if let Some(idx) = col_idx.cdna_coding_end {
        batch.set_opt_i64(idx, json_i64(object.get("cdna_coding_end")));
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
            .or_else(|| {
                object
                    .get("_trans_exon_array")
                    .and_then(Value::as_array)
                    .and_then(|arr| i32::try_from(arr.len()).ok())
            });
        batch.set_opt_i32(idx, exon_count);
    }

    // Exon structured array — only parse when projected
    if col_idx.exons_projected {
        if let Some(idx) = col_idx.exons {
            let exon_tuples = object
                .get("_trans_exon_array")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(|exon_val| {
                            let exon_obj = unwrap_blessed_object_optional(exon_val)?;
                            let start = json_i64(exon_obj.get("start")).map(|v| {
                                normalize_genomic_start(v, coordinate_system_zero_based)
                            })?;
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
    let mut pending_without_chrom: Vec<SValue> = Vec::new();
    let mut process_item = |item: &SValue, fallback_region_chrom: Option<&str>| -> Result<bool> {
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
            .or_else(|| fallback_region_chrom.map(|value| value.to_string()))
            .ok_or_else(|| {
                exec_err(format!(
                    "Transcript storable object missing chrom in {}",
                    source_file.display()
                ))
            })?;

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

        let core = TranscriptRowCore {
            chrom,
            start,
            end,
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

    let reader = open_binary_reader(source_file)?;
    stream_nstore_top_hash_array_items_from_reader(reader, |event| match event {
        TopHashArrayEvent::Item(item) => {
            let chrom_present = item
                .as_hash()
                .and_then(|obj| {
                    sv_str(obj.get("chr").or_else(|| obj.get("chrom"))).or_else(|| {
                        obj.get("slice")
                            .and_then(SValue::as_hash)
                            .and_then(|slice| sv_str(slice.get("seq_region_name")))
                    })
                })
                .is_some();

            if chrom_present {
                process_item(&item, None)
            } else {
                pending_without_chrom.push(item);
                Ok(true)
            }
        }
        TopHashArrayEvent::EntryKey(region_chr) => {
            for item in pending_without_chrom.drain(..) {
                if !process_item(&item, Some(region_chr.as_str()))? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    })
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
    if let Some(idx) = col_idx.coding_region_start {
        batch.set_opt_i64(
            idx,
            sv_i64(object.get("coding_region_start"))
                .map(|value| normalize_genomic_start(value, coordinate_system_zero_based)),
        );
    }
    if let Some(idx) = col_idx.coding_region_end {
        batch.set_opt_i64(
            idx,
            sv_i64(object.get("coding_region_end"))
                .map(|value| normalize_genomic_end(value, coordinate_system_zero_based)),
        );
    }
    if let Some(idx) = col_idx.cdna_coding_start {
        batch.set_opt_i64(idx, sv_i64(object.get("cdna_coding_start")));
    }
    if let Some(idx) = col_idx.cdna_coding_end {
        batch.set_opt_i64(idx, sv_i64(object.get("cdna_coding_end")));
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
            .or_else(|| {
                object
                    .get("_trans_exon_array")
                    .and_then(SValue::as_array)
                    .and_then(|arr| i32::try_from(arr.len()).ok())
            });
        batch.set_opt_i32(idx, exon_count);
    }

    // Exon structured array — only parse when projected
    if col_idx.exons_projected {
        if let Some(idx) = col_idx.exons {
            let exon_tuples = object
                .get("_trans_exon_array")
                .and_then(SValue::as_array)
                .map(|exon_arr| {
                    exon_arr
                        .iter()
                        .filter_map(|exon_val| {
                            let exon_obj = exon_val.as_hash()?;
                            let start = sv_i64(exon_obj.get("start")).map(|v| {
                                normalize_genomic_start(v, coordinate_system_zero_based)
                            })?;
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
