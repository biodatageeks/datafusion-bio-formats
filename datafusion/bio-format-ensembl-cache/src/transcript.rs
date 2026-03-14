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
    gene_phenotype: Option<usize>,
    ccds: Option<usize>,
    swissprot: Option<usize>,
    trembl: Option<usize>,
    uniparc: Option<usize>,
    uniprot_isoform: Option<usize>,
    cds_start_nf: Option<usize>,
    cds_end_nf: Option<usize>,
    mature_mirna_regions: Option<usize>,
    transcript_attributes_projected: bool,
    // Promoted VEP fields (issue #125)
    translateable_seq: Option<usize>,
    cdna_mapper_segments: Option<usize>,
    cdna_mapper_projected: bool,
    bam_edit_status: Option<usize>,
    has_non_polya_rna_edit: Option<usize>,
    spliced_seq: Option<usize>,
    flags_str: Option<usize>,
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
        let spliced_seq = col_map.get("spliced_seq");
        let sequences_projected =
            cdna_seq.is_some() || peptide_seq.is_some() || spliced_seq.is_some();

        let cds_start_nf = col_map.get("cds_start_nf");
        let cds_end_nf = col_map.get("cds_end_nf");
        let mature_mirna_regions = col_map.get("mature_mirna_regions");
        let has_non_polya_rna_edit = col_map.get("has_non_polya_rna_edit");
        let flags_str = col_map.get("flags_str");
        let transcript_attributes_projected = cds_start_nf.is_some()
            || cds_end_nf.is_some()
            || mature_mirna_regions.is_some()
            || has_non_polya_rna_edit.is_some()
            || flags_str.is_some();

        let cdna_mapper_segments = col_map.get("cdna_mapper_segments");
        let cdna_mapper_projected = cdna_mapper_segments.is_some();

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
            gene_phenotype: col_map.get("gene_phenotype"),
            ccds: col_map.get("ccds"),
            swissprot: col_map.get("swissprot"),
            trembl: col_map.get("trembl"),
            uniparc: col_map.get("uniparc"),
            uniprot_isoform: col_map.get("uniprot_isoform"),
            cds_start_nf,
            cds_end_nf,
            mature_mirna_regions,
            transcript_attributes_projected,
            translateable_seq: col_map.get("translateable_seq"),
            cdna_mapper_segments,
            cdna_mapper_projected,
            bam_edit_status: col_map.get("bam_edit_status"),
            has_non_polya_rna_edit,
            spliced_seq,
            flags_str,
            raw_object_json: col_map.get("raw_object_json"),
            object_hash: col_map.get("object_hash"),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TranscriptAttributes {
    cds_start_nf: bool,
    cds_end_nf: bool,
    /// CDS NF flags in attribute encounter order (for flags_str).
    cds_nf_order: Vec<&'static str>,
    tsl: Option<i32>,
    mature_mirna_regions: Vec<(i64, i64)>,
    has_non_polya_rna_edit: bool,
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

/// Derives `(coding_region_start, coding_region_end)` from the Translation
/// sub-object's `start_exon`/`end_exon` references.  This is VEP's own method
/// (`Translation::genomic_start`/`genomic_end`) and works even when the
/// transcript's exon array is incomplete due to storable alias eviction.
///
/// For + strand: `cds_start = start_exon.start + tl.start - 1`
/// For − strand: `cds_start = start_exon.end - tl.start + 1`
fn derive_coding_region_from_translation_json(
    object: &serde_json::Map<String, Value>,
    strand: i8,
    tx_start: i64,
    tx_end: i64,
) -> (Option<i64>, Option<i64>) {
    let tl = object
        .get("translation")
        .and_then(unwrap_blessed_object_optional);
    let tl = match tl {
        Some(t) => t,
        None => return (None, None),
    };
    let tl_start = json_i64(tl.get("start"));
    let tl_end = json_i64(tl.get("end"));
    let se = tl
        .get("start_exon")
        .and_then(unwrap_blessed_object_optional);
    let ee = tl.get("end_exon").and_then(unwrap_blessed_object_optional);
    compute_cds_from_translation_exons(
        tl_start,
        tl_end,
        se,
        ee,
        strand,
        tx_start,
        tx_end,
        |obj, key| json_i64(obj.get(key)),
    )
}

fn derive_coding_region_from_translation_storable(
    object: &std::collections::BTreeMap<String, SValue>,
    strand: i8,
    tx_start: i64,
    tx_end: i64,
) -> (Option<i64>, Option<i64>) {
    let tl = object.get("translation").and_then(SValue::as_hash);
    let tl = match tl {
        Some(t) => t,
        None => return (None, None),
    };
    let tl_start = sv_i64(tl.get("start"));
    let tl_end = sv_i64(tl.get("end"));
    let se = tl.get("start_exon").and_then(SValue::as_hash);
    let ee = tl.get("end_exon").and_then(SValue::as_hash);
    compute_cds_from_translation_exons(
        tl_start,
        tl_end,
        se,
        ee,
        strand,
        tx_start,
        tx_end,
        |obj, key| sv_i64(obj.get(key)),
    )
}

fn compute_cds_from_translation_exons<T>(
    tl_start: Option<i64>,
    tl_end: Option<i64>,
    start_exon: Option<&T>,
    end_exon: Option<&T>,
    strand: i8,
    tx_start: i64,
    tx_end: i64,
    get_i64: impl Fn(&T, &str) -> Option<i64>,
) -> (Option<i64>, Option<i64>) {
    let (Some(tl_s), Some(tl_e)) = (tl_start, tl_end) else {
        return (None, None);
    };
    let (Some(se), Some(ee)) = (start_exon, end_exon) else {
        return (None, None);
    };
    let (genomic_start, genomic_end) = if strand >= 0 {
        let se_start = match get_i64(se, "start") {
            Some(v) => v,
            None => return (None, None),
        };
        let ee_start = match get_i64(ee, "start") {
            Some(v) => v,
            None => return (None, None),
        };
        (se_start + tl_s - 1, ee_start + tl_e - 1)
    } else {
        let se_end = match get_i64(se, "end") {
            Some(v) => v,
            None => return (None, None),
        };
        let ee_end = match get_i64(ee, "end") {
            Some(v) => v,
            None => return (None, None),
        };
        (ee_end - tl_e + 1, se_end - tl_s + 1)
    };
    let cr_start = genomic_start.min(genomic_end);
    let cr_end = genomic_start.max(genomic_end);
    if cr_start >= tx_start && cr_end <= tx_end {
        (Some(cr_start), Some(cr_end))
    } else {
        (None, None)
    }
}

fn non_dash_string(value: Option<String>) -> Option<String> {
    value.filter(|v| v != "-")
}

fn json_first_string(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| non_dash_string(json_str(object.get(*key))))
}

fn sv_first_string(
    object: &std::collections::BTreeMap<String, SValue>,
    keys: &[&str],
) -> Option<String> {
    keys.iter()
        .find_map(|key| non_dash_string(sv_str(object.get(*key))))
}

fn json_first_bool(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<bool> {
    keys.iter().find_map(|key| json_bool(object.get(*key)))
}

fn sv_first_bool(
    object: &std::collections::BTreeMap<String, SValue>,
    keys: &[&str],
) -> Option<bool> {
    keys.iter().find_map(|key| sv_bool(object.get(*key)))
}

/// Parse TSL attribute value like "tsl1" → 1, "tsl5" → 5, or plain "1" → 1.
/// Also handles extended format "tsl2 (assigned to previous version 1)".
fn parse_tsl_value(value: &str) -> Option<i32> {
    let stripped = value.strip_prefix("tsl").unwrap_or(value);
    let digits: &str = &stripped[..stripped
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(stripped.len())];
    digits.parse::<i32>().ok().filter(|&v| (1..=5).contains(&v))
}

/// Extract MANE_Select and MANE_Plus_Clinical from JSON transcript attributes.
/// VEP stores these as attributes with codes "MANE_Select" / "MANE_Plus_Clinical".
fn extract_mane_from_json_attributes(
    object: &serde_json::Map<String, Value>,
) -> (Option<String>, Option<String>) {
    let mut mane_select = None;
    let mut mane_plus_clinical = None;
    let Some(attributes) = object.get("attributes").and_then(Value::as_array) else {
        return (mane_select, mane_plus_clinical);
    };
    for attr in attributes {
        let Some(attr_obj) = unwrap_blessed_object_optional(attr) else {
            continue;
        };
        let code = attr_obj.get("code").and_then(Value::as_str).unwrap_or("");
        match code {
            "MANE_Select" => {
                mane_select = attr_obj
                    .get("value")
                    .and_then(Value::as_str)
                    .map(|s| s.to_string());
            }
            "MANE_Plus_Clinical" => {
                mane_plus_clinical = attr_obj
                    .get("value")
                    .and_then(Value::as_str)
                    .map(|s| s.to_string());
            }
            _ => {}
        }
        if mane_select.is_some() && mane_plus_clinical.is_some() {
            break;
        }
    }
    (mane_select, mane_plus_clinical)
}

/// Extract MANE_Select and MANE_Plus_Clinical from Storable transcript attributes.
fn extract_mane_from_storable_attributes(
    object: &std::collections::BTreeMap<String, SValue>,
) -> (Option<String>, Option<String>) {
    let mut mane_select = None;
    let mut mane_plus_clinical = None;
    let Some(attributes) = object.get("attributes").and_then(SValue::as_array) else {
        return (mane_select, mane_plus_clinical);
    };
    for attr in attributes {
        let Some(attr_obj) = attr.as_hash() else {
            continue;
        };
        let code = attr_obj
            .get("code")
            .and_then(SValue::as_string)
            .unwrap_or_default();
        match code.as_str() {
            "MANE_Select" => {
                mane_select = attr_obj.get("value").and_then(SValue::as_string);
            }
            "MANE_Plus_Clinical" => {
                mane_plus_clinical = attr_obj.get("value").and_then(SValue::as_string);
            }
            _ => {}
        }
        if mane_select.is_some() && mane_plus_clinical.is_some() {
            break;
        }
    }
    (mane_select, mane_plus_clinical)
}

fn parse_cdna_range(value: &str) -> Option<(i64, i64)> {
    let mut parts = value.splitn(2, '-');
    let start = parts.next()?.trim().parse::<i64>().ok()?;
    let end = parts.next()?.trim().parse::<i64>().ok()?;
    Some((start, end))
}

fn mirna_cdna_to_genomic_range(
    tx_start: i64,
    tx_end: i64,
    strand: i8,
    cdna_start: i64,
    cdna_end: i64,
) -> (i64, i64) {
    if strand >= 0 {
        (tx_start + cdna_start - 1, tx_start + cdna_end - 1)
    } else {
        (tx_end - cdna_end + 1, tx_end - cdna_start + 1)
    }
}

/// Returns `true` if the `_rna_edit` value represents a non-poly-A RNA edit.
/// Format: "start end replacement_sequence".
fn is_non_polya_rna_edit(value: &str) -> bool {
    let parts: Vec<&str> = value.split_whitespace().collect();
    if parts.len() >= 3 {
        let seq = parts[2];
        !seq.is_empty() && !seq.bytes().all(|b| b == b'A' || b == b'a')
    } else {
        false
    }
}

fn parse_transcript_attributes_json(
    object: &serde_json::Map<String, Value>,
    tx_start: i64,
    tx_end: i64,
    strand: i8,
    parse_mirna_regions: bool,
    check_rna_edits: bool,
) -> TranscriptAttributes {
    let mut out = TranscriptAttributes::default();
    let Some(attributes) = object.get("attributes").and_then(Value::as_array) else {
        return out;
    };

    for attr in attributes {
        let Some(attr_obj) = unwrap_blessed_object_optional(attr) else {
            continue;
        };
        let code = attr_obj.get("code").and_then(Value::as_str).unwrap_or("");
        let value = attr_obj.get("value").and_then(Value::as_str).unwrap_or("");

        match code {
            "cds_start_NF" if value == "1" => {
                out.cds_start_nf = true;
                out.cds_nf_order.push("cds_start_NF");
            }
            "cds_end_NF" if value == "1" => {
                out.cds_end_nf = true;
                out.cds_nf_order.push("cds_end_NF");
            }
            "TSL" => {
                out.tsl = parse_tsl_value(value);
            }
            "miRNA" if parse_mirna_regions => {
                if let Some((cdna_start, cdna_end)) = parse_cdna_range(value) {
                    out.mature_mirna_regions.push(mirna_cdna_to_genomic_range(
                        tx_start, tx_end, strand, cdna_start, cdna_end,
                    ));
                }
            }
            "_rna_edit" if check_rna_edits && !out.has_non_polya_rna_edit => {
                if is_non_polya_rna_edit(value) {
                    out.has_non_polya_rna_edit = true;
                }
            }
            _ => {}
        }
    }

    out
}

fn parse_transcript_attributes_storable(
    object: &std::collections::BTreeMap<String, SValue>,
    tx_start: i64,
    tx_end: i64,
    strand: i8,
    parse_mirna_regions: bool,
    check_rna_edits: bool,
) -> TranscriptAttributes {
    let mut out = TranscriptAttributes::default();
    let Some(attributes) = object.get("attributes").and_then(SValue::as_array) else {
        return out;
    };

    for attr in attributes {
        let Some(attr_obj) = attr.as_hash() else {
            continue;
        };
        let code = attr_obj
            .get("code")
            .and_then(SValue::as_string)
            .unwrap_or_default();
        let value = attr_obj
            .get("value")
            .and_then(SValue::as_string)
            .unwrap_or_default();

        match code.as_str() {
            "cds_start_NF" if value == "1" => {
                out.cds_start_nf = true;
                out.cds_nf_order.push("cds_start_NF");
            }
            "cds_end_NF" if value == "1" => {
                out.cds_end_nf = true;
                out.cds_nf_order.push("cds_end_NF");
            }
            "TSL" => {
                out.tsl = parse_tsl_value(&value);
            }
            "miRNA" if parse_mirna_regions => {
                if let Some((cdna_start, cdna_end)) = parse_cdna_range(&value) {
                    out.mature_mirna_regions.push(mirna_cdna_to_genomic_range(
                        tx_start, tx_end, strand, cdna_start, cdna_end,
                    ));
                }
            }
            "_rna_edit" if check_rna_edits && !out.has_non_polya_rna_edit => {
                if is_non_polya_rna_edit(&value) {
                    out.has_non_polya_rna_edit = true;
                }
            }
            _ => {}
        }
    }

    out
}

// ---------------------------------------------------------------------------
// cDNA mapper segment extraction (issue #125)
// ---------------------------------------------------------------------------

/// A single cDNA mapper segment: (genomic_start, genomic_end, cdna_start, cdna_end, ori).
type MapperSegment = (i64, i64, i64, i64, i8);

/// Extract cDNA mapper segments from JSON transcript object.
/// Path: `_variation_effect_feature_cache.mapper.pair_genomic.{region_key}[].{from,to,ori}`
fn extract_cdna_mapper_segments_json(
    object: &serde_json::Map<String, Value>,
) -> Option<Vec<MapperSegment>> {
    let vef = object
        .get("_variation_effect_feature_cache")
        .and_then(unwrap_blessed_object_optional)?;
    let mapper = vef.get("mapper").and_then(unwrap_blessed_object_optional)?;
    let pair_genomic = mapper
        .get("pair_genomic")
        .and_then(unwrap_blessed_object_optional)?;

    let mut segments = Vec::new();
    for (_key, value) in pair_genomic {
        // Skip non-array entries like _pair_count
        let Some(pairs) = value.as_array() else {
            continue;
        };
        for pair_val in pairs {
            let pair = unwrap_blessed_object_optional(pair_val).or_else(|| pair_val.as_object());
            let Some(pair) = pair else {
                continue;
            };
            if let Some(seg) = extract_mapper_pair_json(pair) {
                segments.push(seg);
            }
        }
    }
    if segments.is_empty() {
        None
    } else {
        Some(segments)
    }
}

fn extract_mapper_pair_json(pair: &serde_json::Map<String, Value>) -> Option<MapperSegment> {
    let from = pair.get("from").and_then(unwrap_blessed_object_optional)?;
    let to = pair.get("to").and_then(unwrap_blessed_object_optional)?;
    let ori = json_i64(pair.get("ori")).and_then(|v| i8::try_from(v).ok())?;
    let g_start = json_i64(from.get("start"))?;
    let g_end = json_i64(from.get("end"))?;
    let c_start = json_i64(to.get("start"))?;
    let c_end = json_i64(to.get("end"))?;
    Some((g_start, g_end, c_start, c_end, ori))
}

/// Extract cDNA mapper segments from Storable transcript object.
fn extract_cdna_mapper_segments_storable(
    object: &std::collections::BTreeMap<String, SValue>,
) -> Option<Vec<MapperSegment>> {
    let vef = object
        .get("_variation_effect_feature_cache")
        .and_then(SValue::as_hash)?;
    let mapper = vef.get("mapper").and_then(SValue::as_hash)?;
    let pair_genomic = mapper.get("pair_genomic").and_then(SValue::as_hash)?;

    let mut segments = Vec::new();
    for (key, value) in pair_genomic.iter() {
        // Skip non-array entries like _pair_count
        if key.starts_with('_') {
            continue;
        }
        let Some(pairs) = value.as_array() else {
            continue;
        };
        for pair_val in pairs.iter() {
            let Some(pair) = pair_val.as_hash() else {
                continue;
            };
            if let Some(seg) = extract_mapper_pair_storable(pair) {
                segments.push(seg);
            }
        }
    }
    if segments.is_empty() {
        None
    } else {
        Some(segments)
    }
}

fn extract_mapper_pair_storable(
    pair: &std::collections::BTreeMap<String, SValue>,
) -> Option<MapperSegment> {
    let from = pair.get("from").and_then(SValue::as_hash)?;
    let to = pair.get("to").and_then(SValue::as_hash)?;
    let ori = sv_i64(pair.get("ori")).and_then(|v| i8::try_from(v).ok())?;
    let g_start = sv_i64(from.get("start"))?;
    let g_end = sv_i64(from.get("end"))?;
    let c_start = sv_i64(to.get("start"))?;
    let c_end = sv_i64(to.get("end"))?;
    Some((g_start, g_end, c_start, c_end, ori))
}

/// Build the FLAGS string from transcript attributes.
/// VEP convention: "cds_start_NF", "cds_end_NF", joined with "&".
/// Build the FLAGS string preserving VEP attribute encounter order.
fn build_flags_str(attrs: &TranscriptAttributes) -> Option<String> {
    if attrs.cds_nf_order.is_empty() {
        None
    } else {
        Some(attrs.cds_nf_order.join("&"))
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

    // Skip LOC-prefixed gene pseudo-records — these are gene-level entries,
    // not real transcripts. VEP skips them during annotation.
    if stable_id.starts_with("LOC") {
        return Ok(false);
    }

    let biotype = json_str(object.get("biotype"));

    // Skip generic pseudogene / aligned_transcript biotypes — VEP does not
    // use these for consequence annotation.
    if biotype.as_deref().is_some_and(is_excluded_biotype) {
        return Ok(false);
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
        batch.set_opt_utf8_owned(idx, biotype.as_ref());
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
        batch.set_opt_utf8_owned(
            idx,
            json_str(
                object
                    .get("gene_hgnc_id")
                    .or_else(|| object.get("_gene_hgnc_id")),
            )
            .as_ref(),
        );
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
        // Primary: derive from cdna positions + exon array
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
        // Fallback: derive from translation.start_exon/end_exon (VEP's own method).
        // Works even when exon array is incomplete due to storable alias eviction.
        if coding_region_start_val.is_none() {
            let (derived_start, derived_end) = derive_coding_region_from_translation_json(
                object,
                strand,
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

    // Sequences from _variation_effect_feature_cache — only parse when projected.
    if col_idx.sequences_projected || col_idx.cdna_mapper_projected {
        let vef_cache = object
            .get("_variation_effect_feature_cache")
            .and_then(unwrap_blessed_object_optional);
        if let Some(idx) = col_idx.cdna_seq {
            let value = vef_cache.and_then(|c| json_str(c.get("translateable_seq")));
            batch.set_opt_utf8_owned(idx, value.as_ref());
        }
        if let Some(idx) = col_idx.peptide_seq {
            let value = vef_cache.and_then(|c| json_str(c.get("peptide")));
            batch.set_opt_utf8_owned(idx, value.as_ref());
        }
        if let Some(idx) = col_idx.spliced_seq {
            let value = vef_cache.and_then(|c| json_str(c.get("spliced_seq")));
            batch.set_opt_utf8_owned(idx, value.as_ref());
        }
    }

    // cDNA mapper segments from _variation_effect_feature_cache.mapper
    if col_idx.cdna_mapper_projected {
        if let Some(idx) = col_idx.cdna_mapper_segments {
            let segments = extract_cdna_mapper_segments_json(object);
            batch.set_cdna_mapper_list(idx, segments.as_deref());
        }
    }

    // Top-level promoted fields (issue #125)
    if let Some(idx) = col_idx.translateable_seq {
        let value = json_str(object.get("translateable_seq"));
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.bam_edit_status {
        let value = json_str(object.get("_bam_edit_status"));
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }

    // Simple scalar VEP fields
    if let Some(idx) = col_idx.codon_table {
        batch.set_opt_i32(idx, json_i32(object.get("codon_table")));
    }
    // TSL is stored as an attribute {"code":"TSL","value":"tsl1"}, not a top-level key.
    // Fall back to top-level "tsl" for forward-compat with future cache formats.
    if let Some(idx) = col_idx.tsl {
        let tsl_from_attrs = object
            .get("attributes")
            .and_then(Value::as_array)
            .and_then(|attrs| {
                attrs.iter().find_map(|attr| {
                    let obj = unwrap_blessed_object_optional(attr)?;
                    let code = obj.get("code").and_then(Value::as_str)?;
                    if code == "TSL" {
                        parse_tsl_value(obj.get("value").and_then(Value::as_str).unwrap_or(""))
                    } else {
                        None
                    }
                })
            });
        batch.set_opt_i32(idx, tsl_from_attrs.or_else(|| json_i32(object.get("tsl"))));
    }
    // MANE_Select / MANE_Plus_Clinical are stored as attributes, not top-level keys.
    if col_idx.mane_select.is_some() || col_idx.mane_plus_clinical.is_some() {
        let (mane_select_val, mane_plus_clinical_val) = extract_mane_from_json_attributes(object);
        if let Some(idx) = col_idx.mane_select {
            batch.set_opt_utf8_owned(idx, mane_select_val.as_ref());
        }
        if let Some(idx) = col_idx.mane_plus_clinical {
            batch.set_opt_utf8_owned(idx, mane_plus_clinical_val.as_ref());
        }
    }

    if let Some(idx) = col_idx.gene_phenotype {
        batch.set_opt_bool(
            idx,
            json_first_bool(object, &["_gene_phenotype", "gene_phenotype"]),
        );
    }
    if let Some(idx) = col_idx.ccds {
        let value = json_first_string(object, &["_ccds", "ccds"]);
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.swissprot {
        let value = json_first_string(object, &["_swissprot", "swissprot"]);
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.trembl {
        let value = json_first_string(object, &["_trembl", "trembl"]);
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.uniparc {
        let value = json_first_string(object, &["_uniparc", "uniparc"]);
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.uniprot_isoform {
        let value = json_first_string(object, &["_uniprot_isoform", "uniprot_isoform"]);
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }

    if col_idx.transcript_attributes_projected {
        let parse_mirna_regions =
            biotype.as_deref() == Some("miRNA") && col_idx.mature_mirna_regions.is_some();
        let check_rna_edits = col_idx.has_non_polya_rna_edit.is_some();
        let attributes = parse_transcript_attributes_json(
            object,
            source_start,
            source_end,
            strand,
            parse_mirna_regions,
            check_rna_edits,
        );

        if let Some(idx) = col_idx.cds_start_nf {
            batch.set_opt_bool(idx, Some(attributes.cds_start_nf));
        }
        if let Some(idx) = col_idx.cds_end_nf {
            batch.set_opt_bool(idx, Some(attributes.cds_end_nf));
        }
        if let Some(idx) = col_idx.mature_mirna_regions {
            if parse_mirna_regions {
                let regions = attributes
                    .mature_mirna_regions
                    .iter()
                    .map(|&(region_start, region_end)| {
                        (
                            normalize_genomic_start(region_start, coordinate_system_zero_based),
                            normalize_genomic_end(region_end, coordinate_system_zero_based),
                        )
                    })
                    .collect::<Vec<(i64, i64)>>();
                batch.set_mirna_region_list(idx, Some(regions.as_slice()));
            } else {
                batch.set_mirna_region_list(idx, None);
            }
        }
        if let Some(idx) = col_idx.has_non_polya_rna_edit {
            batch.set_opt_bool(idx, Some(attributes.has_non_polya_rna_edit));
        }
        if let Some(idx) = col_idx.flags_str {
            let value = build_flags_str(&attributes);
            batch.set_opt_utf8_owned(idx, value.as_ref());
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

        // Skip LOC-prefixed gene pseudo-records.
        if stable_id.starts_with("LOC") {
            return Ok(true);
        }

        let biotype = sv_str(obj.get("biotype"));

        // Skip generic pseudogene / aligned_transcript biotypes.
        if biotype.as_deref().is_some_and(is_excluded_biotype) {
            return Ok(true);
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
    let biotype = sv_str(object.get("biotype"));

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
        batch.set_opt_utf8_owned(idx, biotype.as_ref());
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
        let value = sv_str(
            object
                .get("gene_hgnc_id")
                .or_else(|| object.get("_gene_hgnc_id")),
        );
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
        // Primary: derive from cdna positions + exon array
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
        // Fallback: derive from translation.start_exon/end_exon (VEP's own method).
        // Works even when exon array is incomplete due to storable alias eviction.
        if coding_region_start_val.is_none() {
            let (derived_start, derived_end) = derive_coding_region_from_translation_storable(
                object,
                strand,
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

    // Sequences from _variation_effect_feature_cache — only parse when projected.
    if col_idx.sequences_projected || col_idx.cdna_mapper_projected {
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
            if let Some(idx) = col_idx.spliced_seq {
                let value = sv_str(vef_cache.get("spliced_seq"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
        }
    }

    // cDNA mapper segments from _variation_effect_feature_cache.mapper
    if col_idx.cdna_mapper_projected {
        if let Some(idx) = col_idx.cdna_mapper_segments {
            let segments = extract_cdna_mapper_segments_storable(object);
            batch.set_cdna_mapper_list(idx, segments.as_deref());
        }
    }

    // Top-level promoted fields (issue #125)
    if let Some(idx) = col_idx.translateable_seq {
        let value = sv_str(object.get("translateable_seq"));
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.bam_edit_status {
        let value = sv_str(object.get("_bam_edit_status"));
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }

    // Simple scalar VEP fields
    if let Some(idx) = col_idx.codon_table {
        batch.set_opt_i32(
            idx,
            sv_i64(object.get("codon_table")).and_then(|v| i32::try_from(v).ok()),
        );
    }
    // TSL is stored as an attribute {"code":"TSL","value":"tsl1"}, not a top-level key.
    if let Some(idx) = col_idx.tsl {
        let tsl_from_attrs = object
            .get("attributes")
            .and_then(SValue::as_array)
            .and_then(|attrs| {
                attrs.iter().find_map(|attr| {
                    let obj = attr.as_hash()?;
                    let code = obj.get("code").and_then(SValue::as_string)?;
                    if code == "TSL" {
                        parse_tsl_value(
                            &obj.get("value")
                                .and_then(SValue::as_string)
                                .unwrap_or_default(),
                        )
                    } else {
                        None
                    }
                })
            });
        batch.set_opt_i32(
            idx,
            tsl_from_attrs
                .or_else(|| sv_i64(object.get("tsl")).and_then(|v| i32::try_from(v).ok())),
        );
    }
    // MANE_Select / MANE_Plus_Clinical are stored as attributes, not top-level keys.
    if col_idx.mane_select.is_some() || col_idx.mane_plus_clinical.is_some() {
        let (mane_select_val, mane_plus_clinical_val) =
            extract_mane_from_storable_attributes(object);
        if let Some(idx) = col_idx.mane_select {
            batch.set_opt_utf8_owned(idx, mane_select_val.as_ref());
        }
        if let Some(idx) = col_idx.mane_plus_clinical {
            batch.set_opt_utf8_owned(idx, mane_plus_clinical_val.as_ref());
        }
    }

    if let Some(idx) = col_idx.gene_phenotype {
        batch.set_opt_bool(
            idx,
            sv_first_bool(object, &["_gene_phenotype", "gene_phenotype"]),
        );
    }
    if let Some(idx) = col_idx.ccds {
        let value = sv_first_string(object, &["_ccds", "ccds"]);
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.swissprot {
        let value = sv_first_string(object, &["_swissprot", "swissprot"]);
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.trembl {
        let value = sv_first_string(object, &["_trembl", "trembl"]);
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.uniparc {
        let value = sv_first_string(object, &["_uniparc", "uniparc"]);
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.uniprot_isoform {
        let value = sv_first_string(object, &["_uniprot_isoform", "uniprot_isoform"]);
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }

    if col_idx.transcript_attributes_projected {
        let parse_mirna_regions =
            biotype.as_deref() == Some("miRNA") && col_idx.mature_mirna_regions.is_some();
        let check_rna_edits = col_idx.has_non_polya_rna_edit.is_some();
        let attributes = parse_transcript_attributes_storable(
            object,
            source_start,
            source_end,
            strand,
            parse_mirna_regions,
            check_rna_edits,
        );

        if let Some(idx) = col_idx.cds_start_nf {
            batch.set_opt_bool(idx, Some(attributes.cds_start_nf));
        }
        if let Some(idx) = col_idx.cds_end_nf {
            batch.set_opt_bool(idx, Some(attributes.cds_end_nf));
        }
        if let Some(idx) = col_idx.mature_mirna_regions {
            if parse_mirna_regions {
                let regions = attributes
                    .mature_mirna_regions
                    .iter()
                    .map(|&(region_start, region_end)| {
                        (
                            normalize_genomic_start(region_start, coordinate_system_zero_based),
                            normalize_genomic_end(region_end, coordinate_system_zero_based),
                        )
                    })
                    .collect::<Vec<(i64, i64)>>();
                batch.set_mirna_region_list(idx, Some(regions.as_slice()));
            } else {
                batch.set_mirna_region_list(idx, None);
            }
        }
        if let Some(idx) = col_idx.has_non_polya_rna_edit {
            batch.set_opt_bool(idx, Some(attributes.has_non_polya_rna_edit));
        }
        if let Some(idx) = col_idx.flags_str {
            let value = build_flags_str(&attributes);
            batch.set_opt_utf8_owned(idx, value.as_ref());
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[test]
    fn json_transcript_attributes_parse_flags_and_mirna_regions_plus_strand() {
        let payload = json!({
            "attributes": [
                { "code": "cds_start_NF", "value": "1" },
                { "code": "miRNA", "value": "42-59" }
            ]
        });
        let object = payload.as_object().unwrap();

        let parsed = parse_transcript_attributes_json(object, 100, 200, 1, true, false);

        assert!(parsed.cds_start_nf);
        assert!(!parsed.cds_end_nf);
        assert_eq!(parsed.mature_mirna_regions, vec![(141, 158)]);
    }

    #[test]
    fn json_transcript_attributes_parse_wrapped_minus_strand_regions() {
        let payload = json!({
            "attributes": [
                {
                    "__class": "Bio::EnsEMBL::Attribute",
                    "__value": { "code": "cds_end_NF", "value": "1" }
                },
                {
                    "__class": "Bio::EnsEMBL::Attribute",
                    "__value": { "code": "miRNA", "value": "42-59" }
                }
            ]
        });
        let object = payload.as_object().unwrap();

        let parsed = parse_transcript_attributes_json(object, 100, 200, -1, true, false);

        assert!(!parsed.cds_start_nf);
        assert!(parsed.cds_end_nf);
        assert_eq!(parsed.mature_mirna_regions, vec![(142, 159)]);
    }

    #[test]
    fn storable_transcript_attributes_parse_flags_and_mirna_regions() {
        let mut attr_start = BTreeMap::new();
        attr_start.insert(
            "code".to_string(),
            SValue::String(Arc::from("cds_start_NF")),
        );
        attr_start.insert("value".to_string(), SValue::String(Arc::from("1")));

        let mut attr_region = BTreeMap::new();
        attr_region.insert("code".to_string(), SValue::String(Arc::from("miRNA")));
        attr_region.insert("value".to_string(), SValue::String(Arc::from("42-59")));

        let mut object = BTreeMap::new();
        object.insert(
            "attributes".to_string(),
            SValue::Array(Arc::new(vec![
                SValue::Hash(Arc::new(attr_start)),
                SValue::Hash(Arc::new(attr_region)),
            ])),
        );

        let parsed = parse_transcript_attributes_storable(&object, 100, 200, 1, true, false);

        assert!(parsed.cds_start_nf);
        assert!(!parsed.cds_end_nf);
        assert_eq!(parsed.mature_mirna_regions, vec![(141, 158)]);
    }

    #[test]
    fn json_mane_select_from_attributes() {
        let payload = json!({
            "attributes": [
                { "code": "cds_start_NF", "value": "1" },
                { "code": "MANE_Select", "value": "NM_021090.4" }
            ]
        });
        let object = payload.as_object().unwrap();
        let (ms, mpc) = extract_mane_from_json_attributes(object);
        assert_eq!(ms.as_deref(), Some("NM_021090.4"));
        assert_eq!(mpc, None);
    }

    #[test]
    fn json_mane_both_from_wrapped_attributes() {
        let payload = json!({
            "attributes": [
                {
                    "__class": "Bio::EnsEMBL::Attribute",
                    "__value": { "code": "MANE_Select", "value": "NM_001044370.2" }
                },
                {
                    "__class": "Bio::EnsEMBL::Attribute",
                    "__value": { "code": "MANE_Plus_Clinical", "value": "NM_014346.5" }
                }
            ]
        });
        let object = payload.as_object().unwrap();
        let (ms, mpc) = extract_mane_from_json_attributes(object);
        assert_eq!(ms.as_deref(), Some("NM_001044370.2"));
        assert_eq!(mpc.as_deref(), Some("NM_014346.5"));
    }

    #[test]
    fn json_mane_absent_returns_none() {
        let payload = json!({
            "attributes": [
                { "code": "cds_start_NF", "value": "1" }
            ]
        });
        let object = payload.as_object().unwrap();
        let (ms, mpc) = extract_mane_from_json_attributes(object);
        assert_eq!(ms, None);
        assert_eq!(mpc, None);
    }

    #[test]
    fn storable_mane_select_from_attributes() {
        let mut attr_mane = BTreeMap::new();
        attr_mane.insert("code".to_string(), SValue::String(Arc::from("MANE_Select")));
        attr_mane.insert(
            "value".to_string(),
            SValue::String(Arc::from("NM_021090.4")),
        );

        let mut object = BTreeMap::new();
        object.insert(
            "attributes".to_string(),
            SValue::Array(Arc::new(vec![SValue::Hash(Arc::new(attr_mane))])),
        );

        let (ms, mpc) = extract_mane_from_storable_attributes(&object);
        assert_eq!(ms.as_deref(), Some("NM_021090.4"));
        assert_eq!(mpc, None);
    }

    #[test]
    fn storable_mane_both_from_attributes() {
        let mut attr_ms = BTreeMap::new();
        attr_ms.insert("code".to_string(), SValue::String(Arc::from("MANE_Select")));
        attr_ms.insert(
            "value".to_string(),
            SValue::String(Arc::from("NM_001044370.2")),
        );

        let mut attr_mpc = BTreeMap::new();
        attr_mpc.insert(
            "code".to_string(),
            SValue::String(Arc::from("MANE_Plus_Clinical")),
        );
        attr_mpc.insert(
            "value".to_string(),
            SValue::String(Arc::from("NM_014346.5")),
        );

        let mut object = BTreeMap::new();
        object.insert(
            "attributes".to_string(),
            SValue::Array(Arc::new(vec![
                SValue::Hash(Arc::new(attr_ms)),
                SValue::Hash(Arc::new(attr_mpc)),
            ])),
        );

        let (ms, mpc) = extract_mane_from_storable_attributes(&object);
        assert_eq!(ms.as_deref(), Some("NM_001044370.2"));
        assert_eq!(mpc.as_deref(), Some("NM_014346.5"));
    }

    #[test]
    fn parse_tsl_value_prefixed() {
        assert_eq!(parse_tsl_value("tsl1"), Some(1));
        assert_eq!(parse_tsl_value("tsl5"), Some(5));
        assert_eq!(parse_tsl_value("tsl3"), Some(3));
    }

    #[test]
    fn parse_tsl_value_plain_numeric() {
        assert_eq!(parse_tsl_value("1"), Some(1));
        assert_eq!(parse_tsl_value("5"), Some(5));
    }

    #[test]
    fn parse_tsl_value_with_parenthetical_suffix() {
        assert_eq!(
            parse_tsl_value("tsl2 (assigned to previous version 1)"),
            Some(2)
        );
        assert_eq!(
            parse_tsl_value("tsl4 (assigned to previous version 1)"),
            Some(4)
        );
        assert_eq!(
            parse_tsl_value("tsl1 (assigned to previous version 5)"),
            Some(1)
        );
    }

    #[test]
    fn parse_tsl_value_out_of_range() {
        assert_eq!(parse_tsl_value("tsl0"), None);
        assert_eq!(parse_tsl_value("tsl6"), None);
        assert_eq!(parse_tsl_value("0"), None);
        assert_eq!(parse_tsl_value(""), None);
        assert_eq!(parse_tsl_value("NA"), None);
    }

    #[test]
    fn json_transcript_attributes_parse_tsl() {
        let payload = json!({
            "attributes": [
                { "code": "cds_start_NF", "value": "1" },
                { "code": "TSL", "value": "tsl1" }
            ]
        });
        let object = payload.as_object().unwrap();
        let parsed = parse_transcript_attributes_json(object, 100, 200, 1, false, false);
        assert_eq!(parsed.tsl, Some(1));
    }

    #[test]
    fn json_transcript_attributes_parse_tsl_wrapped() {
        let payload = json!({
            "attributes": [
                {
                    "__class": "Bio::EnsEMBL::Attribute",
                    "__value": { "code": "TSL", "value": "tsl5" }
                }
            ]
        });
        let object = payload.as_object().unwrap();
        let parsed = parse_transcript_attributes_json(object, 100, 200, 1, false, false);
        assert_eq!(parsed.tsl, Some(5));
    }

    #[test]
    fn storable_transcript_attributes_parse_tsl() {
        let mut attr_tsl = BTreeMap::new();
        attr_tsl.insert("code".to_string(), SValue::String(Arc::from("TSL")));
        attr_tsl.insert("value".to_string(), SValue::String(Arc::from("tsl3")));

        let mut object = BTreeMap::new();
        object.insert(
            "attributes".to_string(),
            SValue::Array(Arc::new(vec![SValue::Hash(Arc::new(attr_tsl))])),
        );

        let parsed = parse_transcript_attributes_storable(&object, 100, 200, 1, false, false);
        assert_eq!(parsed.tsl, Some(3));
    }

    #[test]
    fn json_first_string_filters_exact_dash_only() {
        let payload = json!({
            "_trembl": "-",
            "trembl": "Q9TEST",
            "_uniprot_isoform": "P05546-1"
        });
        let object = payload.as_object().unwrap();

        assert_eq!(
            json_first_string(object, &["_trembl", "trembl"]).as_deref(),
            Some("Q9TEST")
        );
        assert_eq!(
            json_first_string(object, &["_uniprot_isoform"]).as_deref(),
            Some("P05546-1")
        );
    }
}
