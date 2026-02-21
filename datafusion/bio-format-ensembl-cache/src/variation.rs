use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::util::{
    BatchBuilder, ColumnMap, normalize_genomic_end, normalize_genomic_start,
    normalize_nullable_ref, parse_f64_ref, parse_i8_ref, parse_i64_ref,
};
use std::collections::HashMap;
use std::path::Path;

// ---------------------------------------------------------------------------
// VariationContext – pre-computed per-partition field mapping
// ---------------------------------------------------------------------------

pub(crate) struct VariationContext {
    // Tab field index for required fields
    chrom_tab: Option<usize>,
    start_tab: Option<usize>,
    end_tab: Option<usize>,
    var_name_tab: Option<usize>,
    allele_tab: Option<usize>,
    // Tab field index for optional canonical columns
    failed_tab: Option<usize>,
    somatic_tab: Option<usize>,
    strand_tab: Option<usize>,
    minor_allele_tab: Option<usize>,
    minor_allele_freq_tab: Option<usize>,
    clin_sig_tab: Option<usize>,
    phenotype_or_disease_tab: Option<usize>,
    clinical_impact_tab: Option<usize>,
    pubmed_tab: Option<usize>,
    var_synonyms_tab: Option<usize>,
    // Extra columns: (tab_idx, output_column_name)
    extra_columns: Vec<(usize, String)>,
    // Pre-computed source ID mapping: source_key -> ids_column_name
    source_to_id_column: HashMap<String, String>,
}

impl VariationContext {
    pub fn new(cache_info: &CacheInfo) -> Self {
        let cols = &cache_info.variation_cols;

        let find = |candidates: &[&str]| -> Option<usize> {
            for candidate in candidates {
                if let Some(idx) = cols.iter().position(|c| c == candidate) {
                    return Some(idx);
                }
            }
            None
        };

        let chrom_tab = find(&["chr", "chrom", "seq_region_name"]);
        let start_tab = find(&["start", "pos", "position"]);
        let end_tab = find(&["end"]);
        let var_name_tab = find(&["variation_name", "id"]);
        let allele_tab = find(&["allele_string", "alleles"]);

        let find_exact = |name: &str| -> Option<usize> { cols.iter().position(|c| c == name) };

        let failed_tab = find_exact("failed");
        let somatic_tab = find_exact("somatic");
        let strand_tab = find_exact("strand");
        let minor_allele_tab = find_exact("minor_allele");
        let minor_allele_freq_tab = find_exact("minor_allele_freq");
        let clin_sig_tab = find_exact("clin_sig");
        let phenotype_or_disease_tab = find_exact("phenotype_or_disease");
        let clinical_impact_tab = find_exact("clinical_impact");
        let pubmed_tab = find_exact("pubmed");
        let var_synonyms_tab = find_exact("var_synonyms");

        // Build known set for filtering extras
        let mut known: std::collections::HashSet<&str> = [
            "chr",
            "chrom",
            "seq_region_name",
            "start",
            "pos",
            "position",
            "end",
            "variation_name",
            "id",
            "allele_string",
            "alleles",
            "failed",
            "somatic",
            "strand",
            "minor_allele",
            "minor_allele_freq",
            "clin_sig",
            "phenotype_or_disease",
            "clinical_impact",
            "pubmed",
            "var_synonyms",
        ]
        .into_iter()
        .collect();
        for source in &cache_info.source_descriptors {
            known.insert(&source.ids_column);
        }

        let extra_columns: Vec<(usize, String)> = cols
            .iter()
            .enumerate()
            .filter(|(_, name)| !known.contains(name.as_str()))
            .map(|(idx, name)| (idx, name.clone()))
            .collect();

        let source_to_id_column: HashMap<String, String> = cache_info
            .source_descriptors
            .iter()
            .map(|s| (s.source_key.clone(), s.ids_column.clone()))
            .collect();

        Self {
            chrom_tab,
            start_tab,
            end_tab,
            var_name_tab,
            allele_tab,
            failed_tab,
            somatic_tab,
            strand_tab,
            minor_allele_tab,
            minor_allele_freq_tab,
            clin_sig_tab,
            phenotype_or_disease_tab,
            clinical_impact_tab,
            pubmed_tab,
            var_synonyms_tab,
            extra_columns,
            source_to_id_column,
        }
    }
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

pub(crate) fn detect_region_size(
    cache_info: &CacheInfo,
    variation_files: &[std::path::PathBuf],
) -> i64 {
    if let Some(size) = cache_info.cache_region_size {
        if size > 0 {
            return size;
        }
    }

    for path in variation_files {
        let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
            continue;
        };
        if let Some(size) = parse_region_from_filename(name) {
            return size;
        }
    }

    1_000_000
}

pub(crate) fn parse_region_from_filename(name: &str) -> Option<i64> {
    let marker = name.find('_')?;
    let suffix = &name[marker + 1..];
    let (start_raw, rest) = suffix.split_once('-')?;
    let end_raw: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if end_raw.is_empty() {
        return None;
    }

    let start = start_raw.parse::<i64>().ok()?;
    let end = end_raw.parse::<i64>().ok()?;
    if end >= start {
        Some(end - start + 1)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Direct builder parser
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_variation_line_into(
    line: &str,
    source_file: &Path,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    cache_region_size: i64,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_map: &ColumnMap,
    ctx: &VariationContext,
) -> Result<bool> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(false);
    }

    let fields: Vec<&str> = trimmed.split('\t').collect();

    // Helper to get a tab field by pre-computed index
    let field_at =
        |tab_idx: Option<usize>| -> Option<&str> { tab_idx.and_then(|i| fields.get(i).copied()) };

    // Extract required fields using zero-copy refs
    let chrom_ref = field_at(ctx.chrom_tab)
        .and_then(normalize_nullable_ref)
        .ok_or_else(|| {
            exec_err(format!(
                "Variation row missing required chrom in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?;

    let source_start = parse_i64_ref(field_at(ctx.start_tab)).ok_or_else(|| {
        exec_err(format!(
            "Variation row missing required start in {}: {}",
            source_file.display(),
            trimmed
        ))
    })?;

    let source_end = parse_i64_ref(field_at(ctx.end_tab)).unwrap_or(source_start);

    let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
    let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

    if !predicate.matches(chrom_ref, start, end) {
        return Ok(false);
    }

    let variation_name_ref = field_at(ctx.var_name_tab)
        .and_then(normalize_nullable_ref)
        .ok_or_else(|| {
            exec_err(format!(
                "Variation row missing required variation_name in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?;

    let allele_string_ref = field_at(ctx.allele_tab)
        .and_then(normalize_nullable_ref)
        .ok_or_else(|| {
            exec_err(format!(
                "Variation row missing required allele_string in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?;

    let region_bin = ((source_start.saturating_sub(1)) / cache_region_size.max(1)).max(0);

    // Write required columns
    if let Some(idx) = col_map.get("chrom") {
        batch.set_utf8(idx, chrom_ref);
    }
    if let Some(idx) = col_map.get("start") {
        batch.set_i64(idx, start);
    }
    if let Some(idx) = col_map.get("end") {
        batch.set_i64(idx, end);
    }
    if let Some(idx) = col_map.get("variation_name") {
        batch.set_utf8(idx, variation_name_ref);
    }
    if let Some(idx) = col_map.get("allele_string") {
        batch.set_utf8(idx, allele_string_ref);
    }
    if let Some(idx) = col_map.get("region_bin") {
        batch.set_i64(idx, region_bin);
    }

    // Write optional canonical columns (only if projected)
    if let Some(idx) = col_map.get("failed") {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.failed_tab)));
    }
    if let Some(idx) = col_map.get("somatic") {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.somatic_tab)));
    }
    if let Some(idx) = col_map.get("strand") {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.strand_tab)));
    }
    if let Some(idx) = col_map.get("minor_allele") {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.minor_allele_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_map.get("minor_allele_freq") {
        batch.set_opt_f64(idx, parse_f64_ref(field_at(ctx.minor_allele_freq_tab)));
    }
    if let Some(idx) = col_map.get("clin_sig") {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.clin_sig_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_map.get("phenotype_or_disease") {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.phenotype_or_disease_tab)));
    }
    if let Some(idx) = col_map.get("clinical_impact") {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.clinical_impact_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_map.get("pubmed") {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.pubmed_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_map.get("var_synonyms") {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.var_synonyms_tab).and_then(normalize_nullable_ref),
        );
    }

    // Source IDs: only compute if any source ID column is projected
    let any_source_id_projected = cache_info
        .source_descriptors
        .iter()
        .any(|s| col_map.get(&s.ids_column).is_some());
    if any_source_id_projected {
        let var_synonyms_ref = field_at(ctx.var_synonyms_tab).and_then(normalize_nullable_ref);
        let source_ids = extract_source_ids(
            Some(variation_name_ref),
            var_synonyms_ref,
            &ctx.source_to_id_column,
        );
        for source in &cache_info.source_descriptors {
            if let Some(idx) = col_map.get(&source.ids_column) {
                match source_ids.get(&source.ids_column) {
                    Some(value) => batch.set_utf8(idx, value),
                    None => batch.set_null(idx),
                }
            }
        }
    }

    // Extra columns (dynamic, not in known set)
    for (tab_idx, col_name) in &ctx.extra_columns {
        if let Some(builder_idx) = col_map.get(col_name) {
            batch.set_opt_utf8(
                builder_idx,
                fields
                    .get(*tab_idx)
                    .copied()
                    .and_then(normalize_nullable_ref),
            );
        }
    }

    // Provenance columns
    append_provenance(batch, col_map, cache_info, source_file);

    batch.finish_row();
    Ok(true)
}

// ---------------------------------------------------------------------------
// Provenance – shared helper for appending constant provenance columns
// ---------------------------------------------------------------------------

pub(crate) fn append_provenance(
    batch: &mut BatchBuilder,
    col_map: &ColumnMap,
    cache_info: &CacheInfo,
    source_file: &Path,
) {
    if let Some(idx) = col_map.get("species") {
        batch.set_utf8(idx, &cache_info.species);
    }
    if let Some(idx) = col_map.get("assembly") {
        batch.set_utf8(idx, &cache_info.assembly);
    }
    if let Some(idx) = col_map.get("cache_version") {
        batch.set_utf8(idx, &cache_info.cache_version);
    }
    for source in &cache_info.source_descriptors {
        if let Some(idx) = col_map.get(&source.source_column) {
            batch.set_utf8(idx, &source.value);
        }
    }
    if let Some(idx) = col_map.get("serializer_type") {
        match &cache_info.serializer_type {
            Some(s) => batch.set_utf8(idx, s),
            None => batch.set_null(idx),
        }
    }
    if let Some(idx) = col_map.get("source_cache_path") {
        batch.set_utf8(idx, &cache_info.source_cache_path);
    }
    if let Some(idx) = col_map.get("source_file") {
        // Avoid String allocation when path is valid UTF-8
        let path_str = source_file.to_str().unwrap_or_default();
        if !path_str.is_empty() {
            batch.set_utf8(idx, path_str);
        } else {
            batch.set_utf8(idx, &source_file.to_string_lossy());
        }
    }
}

// ---------------------------------------------------------------------------
// Source ID extraction
// ---------------------------------------------------------------------------

fn extract_source_ids(
    variation_name: Option<&str>,
    var_synonyms: Option<&str>,
    source_to_id_column: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut buckets: HashMap<String, Vec<String>> = HashMap::new();

    if let Some(variation_name) = variation_name {
        assign_by_pattern(variation_name, source_to_id_column, &mut buckets);
    }

    if let Some(var_synonyms) = var_synonyms {
        let mut current_source_column: Option<String> = None;

        for token in var_synonyms.split(',') {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }

            if let Some((source, value)) = token.split_once("::") {
                let normalized_source = normalize_source_token(source);
                current_source_column = source_to_id_column.get(&normalized_source).cloned();
                if let Some(column) = &current_source_column {
                    push_source_id(column, value.trim(), &mut buckets);
                } else {
                    assign_by_pattern(token, source_to_id_column, &mut buckets);
                }
                continue;
            }

            if let Some((source, value)) = token.split_once(':') {
                let normalized_source = normalize_source_token(source);
                current_source_column = source_to_id_column.get(&normalized_source).cloned();
                if let Some(column) = &current_source_column {
                    push_source_id(column, value.trim(), &mut buckets);
                    continue;
                }
            }

            if let Some(column) = &current_source_column {
                push_source_id(column, token, &mut buckets);
            } else {
                assign_by_pattern(token, source_to_id_column, &mut buckets);
            }
        }
    }

    buckets
        .into_iter()
        .filter_map(|(column, values)| {
            if values.is_empty() {
                None
            } else {
                Some((column, values.join(",")))
            }
        })
        .collect()
}

fn assign_by_pattern(
    value: &str,
    source_to_id_column: &HashMap<String, String>,
    buckets: &mut HashMap<String, Vec<String>>,
) {
    let value = value.trim();
    if value.is_empty() || value == "." {
        return;
    }

    let mut maybe_column = None;
    if value.starts_with("rs") && value[2..].chars().all(|c| c.is_ascii_digit()) {
        maybe_column = source_to_id_column.get("dbsnp");
    } else if value.starts_with("COSM") || value.starts_with("COSV") {
        maybe_column = source_to_id_column.get("cosmic");
    } else if value.starts_with("CM") && value[2..].chars().any(|c| c.is_ascii_digit()) {
        maybe_column = source_to_id_column.get("hgmd_public");
    } else if value.starts_with("RCV") || value.starts_with("VCV") {
        maybe_column = source_to_id_column.get("clinvar");
    }

    if let Some(column) = maybe_column {
        push_source_id(column, value, buckets);
    }
}

fn push_source_id(column: &str, value: &str, buckets: &mut HashMap<String, Vec<String>>) {
    let value = value.trim();
    if value.is_empty() || value == "." {
        return;
    }

    let values = buckets.entry(column.to_string()).or_default();
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_string());
    }
}

fn normalize_source_token(raw: &str) -> String {
    let mut normalized = String::with_capacity(raw.len() + 4);
    let mut previous_is_underscore = false;

    for ch in raw.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '_'
        };

        if mapped == '_' {
            if !previous_is_underscore {
                normalized.push('_');
                previous_is_underscore = true;
            }
        } else {
            normalized.push(mapped);
            previous_is_underscore = false;
        }
    }

    let normalized = normalized.trim_matches('_');
    if normalized.is_empty() {
        return String::new();
    }

    let mut out = normalized.to_string();
    if out.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        out.insert_str(0, "src_");
    }

    out
}
