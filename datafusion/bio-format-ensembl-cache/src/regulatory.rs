use crate::decode::decode_payload;
use crate::decode::storable_binary::{
    SValue, canonical_json_string as canonical_storable_json_string,
    collect_nstore_alias_counts_and_top_keys_from_reader,
    stream_nstore_top_hash_entries_with_alias_counts_from_reader, sv_i64, sv_str,
};
use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::util::ProvenanceWriter;
use crate::util::{
    BatchBuilder, ColumnMap, canonical_json_string, json_f64, json_i32, json_i64, json_str,
    normalize_genomic_end, normalize_genomic_start, open_binary_reader, parse_i64, stable_hash,
};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegulatoryTarget {
    RegulatoryFeature,
    MotifFeature,
}

// ---------------------------------------------------------------------------
// RegulatoryColumnIndices – pre-computed builder indices from ColumnMap
// ---------------------------------------------------------------------------

pub(crate) struct RegulatoryColumnIndices {
    chrom: Option<usize>,
    start: Option<usize>,
    end: Option<usize>,
    strand: Option<usize>,
    stable_id: Option<usize>,
    db_id: Option<usize>,
    // RegulatoryFeature-specific
    feature_type: Option<usize>,
    epigenome_count: Option<usize>,
    regulatory_build_id: Option<usize>,
    // MotifFeature-specific
    score: Option<usize>,
    binding_matrix: Option<usize>,
    binding_matrix_length: Option<usize>,
    binding_matrix_elements: Option<usize>,
    binding_matrix_unit: Option<usize>,
    motif_seq: Option<usize>,
    overlapping_regulatory_feature: Option<usize>,
    transcription_factors: Option<usize>,
    // Shared
    cell_types: Option<usize>,
    raw_object_json: Option<usize>,
    object_hash: Option<usize>,
}

impl RegulatoryColumnIndices {
    pub fn new(col_map: &ColumnMap) -> Self {
        Self {
            chrom: col_map.get("chrom"),
            start: col_map.get("start"),
            end: col_map.get("end"),
            strand: col_map.get("strand"),
            stable_id: col_map.get("stable_id").or(col_map.get("motif_id")),
            db_id: col_map.get("db_id"),
            feature_type: col_map.get("feature_type"),
            epigenome_count: col_map.get("epigenome_count"),
            regulatory_build_id: col_map.get("regulatory_build_id"),
            score: col_map.get("score"),
            binding_matrix: col_map.get("binding_matrix"),
            binding_matrix_length: col_map.get("binding_matrix_length"),
            binding_matrix_elements: col_map.get("binding_matrix_elements"),
            binding_matrix_unit: col_map.get("binding_matrix_unit"),
            motif_seq: col_map.get("motif_seq"),
            overlapping_regulatory_feature: col_map.get("overlapping_regulatory_feature"),
            transcription_factors: col_map.get("transcription_factors"),
            cell_types: col_map.get("cell_types"),
            raw_object_json: col_map.get("raw_object_json"),
            object_hash: col_map.get("object_hash"),
        }
    }
}

/// Unwraps Storable blessed-object wrappers (`{"__class":..,"__value":..}`)
/// until the underlying JSON map is reached.
fn json_unwrap_blessed(
    value: &serde_json::Value,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    let mut current = value;
    for _ in 0..4 {
        let obj = current.as_object()?;
        match obj.get("__value") {
            Some(inner) => current = inner,
            None => return Some(obj),
        }
    }
    None
}

/// Extracts the BindingMatrix length. Ensembl VEP uses it to place a variant
/// within a reverse-strand motif and to bounds-check MOTIF_POS.
fn json_binding_matrix_length(value: Option<&serde_json::Value>) -> Option<i32> {
    let obj = json_unwrap_blessed(value?)?;
    json_str(obj.get("length"))?.parse().ok()
}

/// Storable counterpart of [`json_binding_matrix_length`].
fn sv_binding_matrix_length(value: Option<&SValue>) -> Option<i32> {
    let map = sv_unwrap_blessed(value?)?;
    sv_str(map.get("length"))?.parse().ok()
}

/// Nucleotide order used when flattening a BindingMatrix, matching
/// `Bio::EnsEMBL::Funcgen::BindingMatrix::Constants::VALID_NUCLEOTIDES`.
const MATRIX_NUCLEOTIDES: [&str; 4] = ["A", "C", "G", "T"];

/// Flattens a BindingMatrix frequency matrix into `A,C,G,T` per position, with
/// positions separated by `;` in ascending order: `"980,99,249,216;234,..."`.
///
/// VEP needs the full matrix to compute `HIGH_INF_POS` and
/// `MOTIF_SCORE_CHANGE`; without it both fields are always empty. The flat
/// encoding keeps the column dictionary-friendly — a whole cache draws on a few
/// dozen distinct matrices — and avoids re-parsing `raw_object_json`.
///
/// Yields `None` unless every position in `1..=length` carries all four
/// nucleotides, so a partially decoded matrix is never scored against.
fn json_binding_matrix_elements(value: Option<&serde_json::Value>) -> Option<String> {
    let obj = json_unwrap_blessed(value?)?;
    let elements = json_unwrap_blessed(obj.get("elements")?)?;
    let length: usize = json_str(obj.get("length"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(elements.len());
    flatten_matrix_positions(length, |position, nucleotide| {
        let column = json_unwrap_blessed(elements.get(&position.to_string())?)?;
        json_str(column.get(nucleotide))
    })
}

/// Storable counterpart of [`json_binding_matrix_elements`].
fn sv_binding_matrix_elements(value: Option<&SValue>) -> Option<String> {
    let map = sv_unwrap_blessed(value?)?;
    let elements = sv_unwrap_blessed(map.get("elements")?)?;
    let length: usize = sv_str(map.get("length"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(elements.len());
    flatten_matrix_positions(length, |position, nucleotide| {
        let column = sv_unwrap_blessed(elements.get(&position.to_string())?)?;
        sv_str(column.get(nucleotide))
    })
}

/// Shared assembly for the two `*_binding_matrix_elements` extractors.
fn flatten_matrix_positions<F>(length: usize, mut cell: F) -> Option<String>
where
    F: FnMut(usize, &str) -> Option<String>,
{
    if length == 0 {
        return None;
    }
    let mut out = String::new();
    for position in 1..=length {
        if position > 1 {
            out.push(';');
        }
        for (index, nucleotide) in MATRIX_NUCLEOTIDES.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(&cell(position, nucleotide)?);
        }
    }
    Some(out)
}

/// The unit the matrix elements are expressed in ("Frequencies"). VEP's
/// frequency-to-weights conversion is only valid for that unit, so scoring has
/// to be able to check it rather than assume it.
fn json_binding_matrix_unit(value: Option<&serde_json::Value>) -> Option<String> {
    json_str(json_unwrap_blessed(value?)?.get("unit"))
}

/// Storable counterpart of [`json_binding_matrix_unit`].
fn sv_binding_matrix_unit(value: Option<&SValue>) -> Option<String> {
    sv_str(sv_unwrap_blessed(value?)?.get("unit"))
}

/// The motif's reference sequence, already in motif orientation (Ensembl
/// reverse-complements it for reverse-strand motifs). VEP reads it from the
/// same cached slot — `MotifFeatureVariation::_motif_feature_seq` — to score
/// the reference and variant sequences against the matrix.
fn json_motif_seq(object: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    let cache = json_unwrap_blessed(object.get("_variation_effect_feature_cache")?)?;
    json_str(cache.get("seq"))
}

/// Storable counterpart of [`json_motif_seq`].
fn sv_motif_seq(object: &HashMap<String, SValue>) -> Option<String> {
    let cache = sv_unwrap_blessed(object.get("_variation_effect_feature_cache")?)?;
    sv_str(cache.get("seq"))
}

/// Extracts the BindingMatrix stable id from a MotifFeature `binding_matrix`.
///
/// Ensembl >= 116 serialises `binding_matrix` as a nested BindingMatrix object
/// (`{"stable_id":"ENSPFM0510", ..}`), while older caches stored a scalar id.
/// `json_str` yields `None` for objects, which silently emptied the column and
/// therefore VEP's `MOTIF_NAME` field.
fn json_binding_matrix_id(value: Option<&serde_json::Value>) -> Option<String> {
    let value = value?;
    match json_unwrap_blessed(value) {
        Some(obj) => json_str(obj.get("stable_id")),
        None => json_str(Some(value)),
    }
}

/// Collects transcription factor names from a nested BindingMatrix.
fn json_binding_matrix_transcription_factors(value: Option<&serde_json::Value>) -> Option<String> {
    let obj = json_unwrap_blessed(value?)?;
    let complexes = obj
        .get("associated_transcription_factor_complexes")?
        .as_array()?;
    let mut names: Vec<String> = Vec::new();
    for complex in complexes {
        let Some(inner) = json_unwrap_blessed(complex) else {
            continue;
        };
        // Filter the placeholder on each candidate *before* falling back, so a
        // complex with `display_name: "-"` still resolves via `production_name`.
        if let Some(name) = json_str(inner.get("display_name"))
            .filter(|value| value != "-")
            .or_else(|| json_str(inner.get("production_name")).filter(|value| value != "-"))
            && !names.contains(&name)
        {
            names.push(name);
        }
    }
    if names.is_empty() {
        None
    } else {
        Some(names.join(","))
    }
}

/// Storable counterpart of [`json_unwrap_blessed`].
fn sv_unwrap_blessed(value: &SValue) -> Option<&HashMap<String, SValue>> {
    let mut current = value;
    for _ in 0..4 {
        match current {
            SValue::Blessed { value, .. } => current = value.as_ref(),
            SValue::Hash(map) => return Some(map.as_ref()),
            _ => return None,
        }
    }
    None
}

/// Storable counterpart of [`json_binding_matrix_id`].
fn sv_binding_matrix_id(value: Option<&SValue>) -> Option<String> {
    let value = value?;
    match sv_unwrap_blessed(value) {
        Some(map) => sv_str(map.get("stable_id")),
        None => sv_str(Some(value)),
    }
}

/// Storable counterpart of [`json_binding_matrix_transcription_factors`].
fn sv_binding_matrix_transcription_factors(value: Option<&SValue>) -> Option<String> {
    let map = sv_unwrap_blessed(value?)?;
    let complexes = match map.get("associated_transcription_factor_complexes")? {
        SValue::Array(items) => items,
        _ => return None,
    };
    let mut names: Vec<String> = Vec::new();
    for complex in complexes.iter() {
        let Some(inner) = sv_unwrap_blessed(complex) else {
            continue;
        };
        // Same placeholder-before-fallback ordering as the JSON path.
        if let Some(name) = sv_str(inner.get("display_name"))
            .filter(|value| value != "-")
            .or_else(|| sv_str(inner.get("production_name")).filter(|value| value != "-"))
            && !names.contains(&name)
        {
            names.push(name);
        }
    }
    if names.is_empty() {
        None
    } else {
        Some(names.join(","))
    }
}

fn json_transcription_factors(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    [
        "_transcription_factors",
        "transcription_factors",
        "_transcription_factor",
        "transcription_factor",
    ]
    .into_iter()
    .find_map(|key| json_str(object.get(key)).filter(|value| value != "-"))
    .or_else(|| json_binding_matrix_transcription_factors(object.get("binding_matrix")))
}

fn storable_transcription_factors(
    object: &std::collections::HashMap<String, SValue>,
) -> Option<String> {
    [
        "_transcription_factors",
        "transcription_factors",
        "_transcription_factor",
        "transcription_factor",
    ]
    .into_iter()
    .find_map(|key| sv_str(object.get(key)).filter(|value| value != "-"))
    .or_else(|| sv_binding_matrix_transcription_factors(object.get("binding_matrix")))
}

struct RegulatoryRowCore {
    chrom: String,
    start: i64,
    end: i64,
    strand: i8,
    target: RegulatoryTarget,
}

// ---------------------------------------------------------------------------
// Direct builder parser for text lines (Phase 1+2)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_regulatory_line_into(
    line: &str,
    source_file_str: &str,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    target: RegulatoryTarget,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &RegulatoryColumnIndices,
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
            "Malformed regulatory row in {source_file_str}: {trimmed}"
        ))
    })?;
    let part1 = split_iter.next().ok_or_else(|| {
        exec_err(format!(
            "Malformed regulatory row in {source_file_str}: {trimmed}"
        ))
    })?;
    let part2 = split_iter.next().ok_or_else(|| {
        exec_err(format!(
            "Malformed regulatory row in {source_file_str}: {trimmed}"
        ))
    })?;
    let part3 = split_iter.next().ok_or_else(|| {
        exec_err(format!(
            "Malformed regulatory row in {source_file_str}: {trimmed}"
        ))
    })?;

    // Early predicate check from prefix columns
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
            "Unknown serializer for regulatory entity. serialiser_type missing in info.txt under {}",
            cache_info.cache_root.display()
        ))
    })?;

    let payload = decode_payload(serializer, part3)?;
    let object = payload.as_object().ok_or_else(|| {
        exec_err(format!(
            "Regulatory payload must be a JSON object in {source_file_str}"
        ))
    })?;

    let kind = infer_kind(object);
    if (kind == RegulatoryTarget::RegulatoryFeature
        && target != RegulatoryTarget::RegulatoryFeature)
        || (kind == RegulatoryTarget::MotifFeature && target != RegulatoryTarget::MotifFeature)
    {
        return Ok(false);
    }

    let chrom = if let Some(c) = prefix_chrom {
        c.to_string()
    } else {
        json_str(object.get("chr").or_else(|| object.get("chrom"))).ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required chrom in {source_file_str}"
            ))
        })?
    };

    let source_start = prefix_start
        .or_else(|| json_i64(object.get("start")))
        .ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required start in {source_file_str}"
            ))
        })?;

    let source_end = prefix_end
        .or_else(|| json_i64(object.get("end")))
        .ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required end in {source_file_str}"
            ))
        })?;

    let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
    let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

    if !predicate.matches(&chrom, start, end) {
        return Ok(false);
    }

    let strand = json_i64(object.get("strand"))
        .and_then(|v| i8::try_from(v).ok())
        .ok_or_else(|| {
            exec_err(format!(
                "Regulatory row missing required strand in {source_file_str}"
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
        batch.set_opt_utf8_owned(idx, json_str(object.get("stable_id")).as_ref());
    }
    if let Some(idx) = col_idx.db_id {
        batch.set_opt_i64(idx, json_i64(object.get("db_id")));
    }

    match target {
        RegulatoryTarget::RegulatoryFeature => {
            if let Some(idx) = col_idx.feature_type {
                batch.set_opt_utf8_owned(idx, json_str(object.get("feature_type")).as_ref());
            }
            if let Some(idx) = col_idx.epigenome_count {
                batch.set_opt_i32(idx, json_i32(object.get("epigenome_count")));
            }
            if let Some(idx) = col_idx.regulatory_build_id {
                batch.set_opt_i64(idx, json_i64(object.get("regulatory_build_id")));
            }
            if let Some(idx) = col_idx.cell_types {
                batch.set_opt_utf8_owned(idx, json_str(object.get("cell_types")).as_ref());
            }
        }
        RegulatoryTarget::MotifFeature => {
            if let Some(idx) = col_idx.score {
                batch.set_opt_f64(idx, json_f64(object.get("score")));
            }
            if let Some(idx) = col_idx.binding_matrix {
                batch.set_opt_utf8_owned(
                    idx,
                    json_binding_matrix_id(object.get("binding_matrix")).as_ref(),
                );
            }
            if let Some(idx) = col_idx.binding_matrix_length {
                batch.set_opt_i32(
                    idx,
                    json_binding_matrix_length(object.get("binding_matrix")),
                );
            }
            if let Some(idx) = col_idx.binding_matrix_elements {
                batch.set_opt_utf8_owned(
                    idx,
                    json_binding_matrix_elements(object.get("binding_matrix")).as_ref(),
                );
            }
            if let Some(idx) = col_idx.binding_matrix_unit {
                batch.set_opt_utf8_owned(
                    idx,
                    json_binding_matrix_unit(object.get("binding_matrix")).as_ref(),
                );
            }
            if let Some(idx) = col_idx.motif_seq {
                batch.set_opt_utf8_owned(idx, json_motif_seq(object).as_ref());
            }
            if let Some(idx) = col_idx.transcription_factors {
                let value = json_transcription_factors(object);
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.cell_types {
                batch.set_opt_utf8_owned(idx, json_str(object.get("cell_types")).as_ref());
            }
            if let Some(idx) = col_idx.overlapping_regulatory_feature {
                batch.set_opt_utf8_owned(
                    idx,
                    json_str(object.get("overlapping_regulatory_feature")).as_ref(),
                );
            }
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
pub(crate) fn parse_regulatory_storable_file_into<F>(
    source_file: &Path,
    source_file_str: &str,
    predicate: &SimplePredicate,
    target: RegulatoryTarget,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &RegulatoryColumnIndices,
    provenance: &ProvenanceWriter,
    alias_prelude: Option<(HashMap<usize, usize>, Vec<String>)>,
    mut on_row_added: F,
) -> Result<()>
where
    F: FnMut(&mut BatchBuilder) -> Result<bool>,
{
    let (alias_counts, _entry_keys) = match alias_prelude {
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
    stream_nstore_top_hash_entries_with_alias_counts_from_reader(
        reader,
        alias_counts,
        |region_chr, region_payload| {
            let Some(region_obj) = region_payload.as_hash() else {
                return Ok(true);
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

                    let chrom = sv_str(obj.get("chr").or_else(|| obj.get("chrom")))
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
                    if !predicate.matches(&chrom, start, end) {
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

                    let core = RegulatoryRowCore {
                        chrom,
                        start,
                        end,
                        strand,
                        target,
                    };

                    append_regulatory_storable_row_into(
                        item,
                        obj,
                        core,
                        source_file_str,
                        batch,
                        col_idx,
                        provenance,
                    )?;

                    if !on_row_added(batch)? {
                        return Ok(false);
                    }
                }
            }

            Ok(true)
        },
    )
    .map_err(|e| {
        exec_err(format!(
            "Failed streaming storable regulatory payload from {}: {}",
            source_file.display(),
            e
        ))
    })?;

    Ok(())
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

fn infer_kind_storable(object: &std::collections::HashMap<String, SValue>) -> RegulatoryTarget {
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

fn append_regulatory_storable_row_into(
    payload: &SValue,
    object: &std::collections::HashMap<String, SValue>,
    core: RegulatoryRowCore,
    source_file_str: &str,
    batch: &mut BatchBuilder,
    col_idx: &RegulatoryColumnIndices,
    provenance: &ProvenanceWriter,
) -> Result<()> {
    let RegulatoryRowCore {
        chrom,
        start,
        end,
        strand,
        target,
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
        let value = sv_str(object.get("stable_id"));
        batch.set_opt_utf8_owned(idx, value.as_ref());
    }
    if let Some(idx) = col_idx.db_id {
        batch.set_opt_i64(
            idx,
            sv_i64(object.get("db_id").or_else(|| object.get("dbID"))),
        );
    }

    match target {
        RegulatoryTarget::RegulatoryFeature => {
            if let Some(idx) = col_idx.feature_type {
                let value = sv_str(
                    object
                        .get("feature_type")
                        .or_else(|| object.get("_vep_feature_type")),
                );
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.epigenome_count {
                batch.set_opt_i32(
                    idx,
                    sv_i64(object.get("epigenome_count")).and_then(|v| i32::try_from(v).ok()),
                );
            }
            if let Some(idx) = col_idx.regulatory_build_id {
                batch.set_opt_i64(idx, sv_i64(object.get("regulatory_build_id")));
            }
            if let Some(idx) = col_idx.cell_types {
                let value = sv_str(object.get("cell_types"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
        }
        RegulatoryTarget::MotifFeature => {
            if let Some(idx) = col_idx.score {
                batch.set_opt_f64(idx, sv_f64(object.get("score")));
            }
            if let Some(idx) = col_idx.binding_matrix {
                let value = sv_binding_matrix_id(object.get("binding_matrix"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.binding_matrix_length {
                batch.set_opt_i32(idx, sv_binding_matrix_length(object.get("binding_matrix")));
            }
            if let Some(idx) = col_idx.binding_matrix_elements {
                let value = sv_binding_matrix_elements(object.get("binding_matrix"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.binding_matrix_unit {
                let value = sv_binding_matrix_unit(object.get("binding_matrix"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.motif_seq {
                let value = sv_motif_seq(object);
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.transcription_factors {
                let value = storable_transcription_factors(object);
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.cell_types {
                let value = sv_str(object.get("cell_types"));
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
            if let Some(idx) = col_idx.overlapping_regulatory_feature {
                let value = sv_str(
                    object
                        .get("overlapping_regulatory_feature")
                        .or_else(|| object.get("regulatory_feature_stable_id")),
                );
                batch.set_opt_utf8_owned(idx, value.as_ref());
            }
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

fn sv_f64(value: Option<&SValue>) -> Option<f64> {
    value
        .and_then(SValue::as_string)
        .and_then(|v| v.parse::<f64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn json_transcription_factors_supports_aliases_and_dash_filtering() {
        let payload = json!({
            "_transcription_factors": "-",
            "transcription_factors": ["TFAP2A", "GATA3"]
        });
        let object = payload.as_object().unwrap();

        assert_eq!(
            json_transcription_factors(object).as_deref(),
            Some("TFAP2A,GATA3")
        );
    }

    #[test]
    fn storable_transcription_factors_supports_aliases() {
        let mut object = HashMap::new();
        object.insert(
            "transcription_factor".to_string(),
            SValue::String(Arc::from("CTCF")),
        );

        assert_eq!(
            storable_transcription_factors(&object).as_deref(),
            Some("CTCF")
        );
    }

    // ------------------------------------------------------------------
    // MotifFeature binding matrix (Ensembl >= 116 nested BindingMatrix)
    // ------------------------------------------------------------------

    /// Real release-116 shape: `binding_matrix` is a nested BindingMatrix
    /// object, not a scalar id.
    fn binding_matrix_payload() -> serde_json::Value {
        json!({
            "associated_transcription_factor_complexes": [{
                "__class": "Bio::EnsEMBL::Funcgen::TranscriptionFactorComplex",
                "__value": {
                    "components": [{
                        "__class": "Bio::EnsEMBL::Funcgen::TranscriptionFactor",
                        "__value": { "gene_stable_id": "ENSG00000073861", "name": "TBX21" }
                    }],
                    "display_name": "TBX21",
                    "production_name": "TBX21"
                }
            }],
            "length": "23",
            "name": "TBX1_AI_TACGGA40NGGC_TYTCACACCTNNNAGGTGTGARA_m2_c4_Cell2013",
            "source": "SELEX",
            "stable_id": "ENSPFM0510",
            "threshold": "4.4",
            "unit": "Frequencies"
        })
    }

    /// A two-position frequency matrix, in the shape release 116 serialises.
    fn elements_payload() -> serde_json::Value {
        json!({
            "length": "2",
            "unit": "Frequencies",
            "elements": {
                "1": { "A": 980, "C": 99, "G": 249, "T": 216 },
                "2": { "A": 234, "C": 210, "G": 463, "T": 636 }
            }
        })
    }

    #[test]
    fn json_binding_matrix_elements_flattens_positions_in_acgt_order() {
        let value = elements_payload();
        assert_eq!(
            json_binding_matrix_elements(Some(&value)).as_deref(),
            Some("980,99,249,216;234,210,463,636")
        );
    }

    /// Positions arrive as string keys, so `"10"` must not sort before `"2"`.
    #[test]
    fn json_binding_matrix_elements_orders_positions_numerically() {
        let mut elements = serde_json::Map::new();
        for position in 1..=10 {
            elements.insert(
                position.to_string(),
                json!({ "A": position, "C": 0, "G": 0, "T": 0 }),
            );
        }
        let value = json!({ "length": "10", "elements": elements });

        let flattened = json_binding_matrix_elements(Some(&value)).unwrap();
        let first_of_each: Vec<&str> = flattened
            .split(';')
            .map(|column| column.split(',').next().unwrap())
            .collect();
        assert_eq!(
            first_of_each,
            ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
        );
    }

    /// A matrix missing a position (or a nucleotide) must not be scored
    /// against, so extraction yields nothing rather than a short matrix.
    #[test]
    fn json_binding_matrix_elements_is_none_when_incomplete() {
        let truncated = json!({
            "length": "3",
            "elements": {
                "1": { "A": 1, "C": 2, "G": 3, "T": 4 },
                "2": { "A": 1, "C": 2, "G": 3, "T": 4 }
            }
        });
        assert_eq!(json_binding_matrix_elements(Some(&truncated)), None);

        let missing_nucleotide = json!({
            "length": "1",
            "elements": { "1": { "A": 1, "C": 2, "G": 3 } }
        });
        assert_eq!(
            json_binding_matrix_elements(Some(&missing_nucleotide)),
            None
        );
    }

    #[test]
    fn json_binding_matrix_elements_is_none_without_a_matrix() {
        assert_eq!(json_binding_matrix_elements(None), None);
        assert_eq!(json_binding_matrix_elements(Some(&json!({}))), None);
    }

    #[test]
    fn json_binding_matrix_unit_reads_nested_unit() {
        let value = elements_payload();
        assert_eq!(
            json_binding_matrix_unit(Some(&value)).as_deref(),
            Some("Frequencies")
        );
    }

    #[test]
    fn json_motif_seq_reads_the_variation_effect_feature_cache() {
        let payload = json!({
            "_variation_effect_feature_cache": { "seq": "ACTCTCCAGGGATT" }
        });
        let object = payload.as_object().unwrap();
        assert_eq!(json_motif_seq(object).as_deref(), Some("ACTCTCCAGGGATT"));
    }

    #[test]
    fn json_motif_seq_is_none_without_a_cached_sequence() {
        let payload = json!({ "_variation_effect_feature_cache": {} });
        assert_eq!(json_motif_seq(payload.as_object().unwrap()), None);
        assert_eq!(json_motif_seq(json!({}).as_object().unwrap()), None);
    }

    /// Storable mirror of the JSON flattening, including the blessed wrapper
    /// the decoder emits for the first occurrence of a shared matrix.
    #[test]
    fn sv_binding_matrix_elements_flattens_blessed_matrix() {
        let mut column = HashMap::new();
        for (nucleotide, frequency) in [("A", 980), ("C", 99), ("G", 249), ("T", 216)] {
            column.insert(nucleotide.to_string(), SValue::Int(frequency));
        }
        let mut elements = HashMap::new();
        elements.insert("1".to_string(), SValue::Hash(Arc::new(column)));
        let mut matrix = HashMap::new();
        matrix.insert("length".to_string(), SValue::Int(1));
        matrix.insert("elements".to_string(), SValue::Hash(Arc::new(elements)));
        let value = SValue::Blessed {
            class: Arc::from("Bio::EnsEMBL::Funcgen::BindingMatrix"),
            value: Arc::new(SValue::Hash(Arc::new(matrix))),
        };

        assert_eq!(
            sv_binding_matrix_elements(Some(&value)).as_deref(),
            Some("980,99,249,216")
        );
    }

    #[test]
    fn sv_motif_seq_reads_the_variation_effect_feature_cache() {
        let mut cache = HashMap::new();
        cache.insert(
            "seq".to_string(),
            SValue::String(Arc::from("AATCCCTGGAGAGT")),
        );
        let mut object = HashMap::new();
        object.insert(
            "_variation_effect_feature_cache".to_string(),
            SValue::Hash(Arc::new(cache)),
        );

        assert_eq!(sv_motif_seq(&object).as_deref(), Some("AATCCCTGGAGAGT"));
    }

    #[test]
    fn json_binding_matrix_id_reads_stable_id_from_nested_object() {
        let value = binding_matrix_payload();
        assert_eq!(
            json_binding_matrix_id(Some(&value)).as_deref(),
            Some("ENSPFM0510")
        );
    }

    #[test]
    fn json_binding_matrix_id_accepts_scalar_for_older_caches() {
        let value = json!("MA0001.1");
        assert_eq!(
            json_binding_matrix_id(Some(&value)).as_deref(),
            Some("MA0001.1")
        );
    }

    #[test]
    fn json_binding_matrix_id_is_none_when_absent() {
        assert_eq!(json_binding_matrix_id(None), None);
        assert_eq!(json_binding_matrix_id(Some(&json!(null))), None);
        assert_eq!(json_binding_matrix_id(Some(&json!({}))), None);
    }

    #[test]
    fn json_transcription_factors_reads_nested_binding_matrix_complexes() {
        let payload = json!({ "binding_matrix": binding_matrix_payload() });
        let object = payload.as_object().unwrap();
        assert_eq!(json_transcription_factors(object).as_deref(), Some("TBX21"));
    }

    #[test]
    fn json_transcription_factors_prefers_top_level_aliases() {
        let payload = json!({
            "transcription_factors": ["TFAP2A", "GATA3"],
            "binding_matrix": binding_matrix_payload()
        });
        let object = payload.as_object().unwrap();
        assert_eq!(
            json_transcription_factors(object).as_deref(),
            Some("TFAP2A,GATA3")
        );
    }

    #[test]
    fn sv_binding_matrix_id_reads_stable_id_from_hash() {
        let mut matrix = HashMap::new();
        matrix.insert(
            "stable_id".to_string(),
            SValue::String(Arc::from("ENSPFM0510")),
        );
        let value = SValue::Hash(Arc::new(matrix));
        assert_eq!(
            sv_binding_matrix_id(Some(&value)).as_deref(),
            Some("ENSPFM0510")
        );
    }

    #[test]
    fn sv_binding_matrix_id_unwraps_blessed_matrix() {
        let mut matrix = HashMap::new();
        matrix.insert(
            "stable_id".to_string(),
            SValue::String(Arc::from("ENSPFM0510")),
        );
        let value = SValue::Blessed {
            class: Arc::from("Bio::EnsEMBL::Funcgen::BindingMatrix"),
            value: Arc::new(SValue::Hash(Arc::new(matrix))),
        };
        assert_eq!(
            sv_binding_matrix_id(Some(&value)).as_deref(),
            Some("ENSPFM0510")
        );
    }

    #[test]
    fn sv_binding_matrix_id_accepts_scalar_for_older_caches() {
        let value = SValue::String(Arc::from("MA0001.1"));
        assert_eq!(
            sv_binding_matrix_id(Some(&value)).as_deref(),
            Some("MA0001.1")
        );
    }

    /// A complex whose `display_name` is the `-` placeholder must fall back to
    /// `production_name` rather than discarding the whole entry.
    #[test]
    fn json_transcription_factors_falls_back_to_production_name_over_placeholder() {
        let payload = json!({
            "binding_matrix": {
                "associated_transcription_factor_complexes": [{
                    "__class": "Bio::EnsEMBL::Funcgen::TranscriptionFactorComplex",
                    "__value": { "display_name": "-", "production_name": "TBX21" }
                }]
            }
        });
        let object = payload.as_object().unwrap();
        assert_eq!(json_transcription_factors(object).as_deref(), Some("TBX21"));
    }

    /// Both names being placeholders yields no transcription factors.
    #[test]
    fn json_transcription_factors_skips_complexes_with_only_placeholders() {
        let payload = json!({
            "binding_matrix": {
                "associated_transcription_factor_complexes": [{
                    "__class": "Bio::EnsEMBL::Funcgen::TranscriptionFactorComplex",
                    "__value": { "display_name": "-", "production_name": "-" }
                }]
            }
        });
        let object = payload.as_object().unwrap();
        assert_eq!(json_transcription_factors(object), None);
    }

    /// Storable mirror of the placeholder fallback.
    #[test]
    fn sv_transcription_factors_falls_back_to_production_name_over_placeholder() {
        let mut complex = HashMap::new();
        complex.insert("display_name".to_string(), SValue::String(Arc::from("-")));
        complex.insert(
            "production_name".to_string(),
            SValue::String(Arc::from("TBX21")),
        );
        let complex = SValue::Blessed {
            class: Arc::from("Bio::EnsEMBL::Funcgen::TranscriptionFactorComplex"),
            value: Arc::new(SValue::Hash(Arc::new(complex))),
        };
        let mut matrix = HashMap::new();
        matrix.insert(
            "associated_transcription_factor_complexes".to_string(),
            SValue::Array(Arc::new(vec![complex])),
        );
        let mut object = HashMap::new();
        object.insert("binding_matrix".to_string(), SValue::Hash(Arc::new(matrix)));

        assert_eq!(
            storable_transcription_factors(&object).as_deref(),
            Some("TBX21")
        );
    }

    #[test]
    fn json_binding_matrix_length_reads_nested_length() {
        assert_eq!(
            json_binding_matrix_length(Some(&binding_matrix_payload())),
            Some(23)
        );
        assert_eq!(json_binding_matrix_length(Some(&json!("MA0001.1"))), None);
        assert_eq!(json_binding_matrix_length(None), None);
    }

    #[test]
    fn sv_binding_matrix_length_reads_nested_length() {
        let mut matrix = HashMap::new();
        matrix.insert("length".to_string(), SValue::String(Arc::from("23")));
        let value = SValue::Hash(Arc::new(matrix));
        assert_eq!(sv_binding_matrix_length(Some(&value)), Some(23));
    }
}
