use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, canonical_json_string as canonical_storable_json_string, decode_nstore,
};
use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::row::{CellValue, Row};
use crate::util::ProvenanceWriter;
use crate::util::{
    BatchBuilder, ColumnMap, canonical_json_string, json_bool, json_i32, json_i64, json_str,
    normalize_genomic_end, normalize_genomic_start, parse_i64, read_maybe_gzip_bytes, stable_hash,
};
use serde_json::Value;
use std::collections::HashMap;
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
    pub fn exons_projected(&self) -> bool {
        self.exons_projected
    }

    pub fn sequences_projected(&self) -> bool {
        self.sequences_projected
    }

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

struct TranscriptRowContext<'a> {
    coordinate_system_zero_based: bool,
    cache_info: &'a CacheInfo,
    source_file: &'a Path,
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
// Storable binary parser (kept with Vec<Row> for now)
// ---------------------------------------------------------------------------

pub(crate) fn parse_transcript_storable_file(
    source_file: &Path,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    coordinate_system_zero_based: bool,
    exons_projected: bool,
    sequences_projected: bool,
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

            rows.push(build_transcript_row_storable(
                item,
                obj,
                core,
                &context,
                exons_projected,
                sequences_projected,
            )?);
        }
    }

    Ok(rows)
}

fn build_transcript_row_storable(
    payload: &SValue,
    object: &std::collections::BTreeMap<String, SValue>,
    core: TranscriptRowCore,
    context: &TranscriptRowContext<'_>,
    exons_projected: bool,
    sequences_projected: bool,
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

    // Exon structured array (skip when not projected)
    if exons_projected {
        if let Some(exon_arr) = object.get("_trans_exon_array").and_then(SValue::as_array) {
            let exon_tuples: Vec<(i64, i64, i8)> = exon_arr
                .iter()
                .filter_map(|exon_val| {
                    let exon_obj = exon_val.as_hash()?;
                    let start = sv_i64(exon_obj.get("start")).map(|v| {
                        normalize_genomic_start(v, context.coordinate_system_zero_based)
                    })?;
                    let end = sv_i64(exon_obj.get("end"))
                        .map(|v| normalize_genomic_end(v, context.coordinate_system_zero_based))?;
                    let phase = sv_i64(exon_obj.get("phase"))
                        .and_then(|v| i8::try_from(v).ok())
                        .unwrap_or(-1);
                    Some((start, end, phase))
                })
                .collect();
            row.insert("exons".to_string(), CellValue::ExonList(exon_tuples));
        }
    }

    // Sequences from _variation_effect_feature_cache (skip when not projected)
    if sequences_projected {
        if let Some(vef_cache) = object
            .get("_variation_effect_feature_cache")
            .and_then(SValue::as_hash)
        {
            insert_opt_utf8(
                &mut row,
                "cdna_seq",
                sv_str(vef_cache.get("translateable_seq")),
            );
            insert_opt_utf8(&mut row, "peptide_seq", sv_str(vef_cache.get("peptide")));
        }
    }

    // Simple scalar VEP fields
    insert_opt_i32(
        &mut row,
        "codon_table",
        sv_i64(object.get("codon_table")).and_then(|v| i32::try_from(v).ok()),
    );
    insert_opt_i32(
        &mut row,
        "tsl",
        sv_i64(object.get("tsl")).and_then(|v| i32::try_from(v).ok()),
    );
    insert_opt_utf8(&mut row, "mane_select", sv_str(object.get("mane_select")));
    insert_opt_utf8(
        &mut row,
        "mane_plus_clinical",
        sv_str(object.get("mane_plus_clinical")),
    );

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
