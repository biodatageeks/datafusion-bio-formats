use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::row::{CellValue, Row};
use crate::util::{
    normalize_genomic_end, normalize_genomic_start, normalize_nullable, parse_f64, parse_i8,
    parse_i64,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;

const OPTIONAL_CANONICAL_COLUMNS: [&str; 10] = [
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
];

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

pub(crate) fn parse_variation_line(
    line: &str,
    source_file: &Path,
    cache_info: &CacheInfo,
    predicate: &SimplePredicate,
    cache_region_size: i64,
    coordinate_system_zero_based: bool,
) -> Result<Option<Row>> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(None);
    }

    let fields: Vec<&str> = trimmed.split('\t').collect();
    let mut source_values: HashMap<String, String> = HashMap::new();
    for (idx, column_name) in cache_info.variation_cols.iter().enumerate() {
        let value = fields.get(idx).copied().unwrap_or_default();
        if let Some(normalized) = normalize_nullable(value) {
            source_values.insert(column_name.clone(), normalized);
        }
    }

    let chr =
        first_non_empty(&source_values, &["chr", "chrom", "seq_region_name"]).ok_or_else(|| {
            exec_err(format!(
                "Variation row missing required chr in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?;

    let source_start =
        parse_i64(first_non_empty(&source_values, &["start", "pos", "position"]).as_deref())
            .ok_or_else(|| {
                exec_err(format!(
                    "Variation row missing required start in {}: {}",
                    source_file.display(),
                    trimmed
                ))
            })?;

    let source_end =
        parse_i64(first_non_empty(&source_values, &["end"]).as_deref()).unwrap_or(source_start);

    let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
    let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

    if !predicate.matches(&chr, start, end) {
        return Ok(None);
    }

    let variation_name =
        first_non_empty(&source_values, &["variation_name", "id"]).ok_or_else(|| {
            exec_err(format!(
                "Variation row missing required variation_name in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?;

    let allele_string =
        first_non_empty(&source_values, &["allele_string", "alleles"]).ok_or_else(|| {
            exec_err(format!(
                "Variation row missing required allele_string in {}: {}",
                source_file.display(),
                trimmed
            ))
        })?;

    let region_bin = ((source_start.saturating_sub(1)) / cache_region_size.max(1)).max(0);
    let variation_name_for_ids = variation_name.clone();

    let mut row: Row = HashMap::new();
    row.insert("chr".to_string(), CellValue::Utf8(chr));
    row.insert("start".to_string(), CellValue::Int64(start));
    row.insert("end".to_string(), CellValue::Int64(end));
    row.insert(
        "variation_name".to_string(),
        CellValue::Utf8(variation_name),
    );
    row.insert("allele_string".to_string(), CellValue::Utf8(allele_string));
    row.insert("region_bin".to_string(), CellValue::Int64(region_bin));

    if let Some(v) = parse_i8(source_values.get("failed").map(String::as_str)) {
        row.insert("failed".to_string(), CellValue::Int8(v));
    }
    if let Some(v) = parse_i8(source_values.get("somatic").map(String::as_str)) {
        row.insert("somatic".to_string(), CellValue::Int8(v));
    }
    if let Some(v) = parse_i8(source_values.get("strand").map(String::as_str)) {
        row.insert("strand".to_string(), CellValue::Int8(v));
    }
    if let Some(v) = source_values.get("minor_allele") {
        row.insert("minor_allele".to_string(), CellValue::Utf8(v.clone()));
    }
    if let Some(v) = parse_f64(source_values.get("minor_allele_freq").map(String::as_str)) {
        row.insert("minor_allele_freq".to_string(), CellValue::Float64(v));
    }
    if let Some(v) = source_values.get("clin_sig") {
        row.insert("clin_sig".to_string(), CellValue::Utf8(v.clone()));
    }
    if let Some(v) = parse_i8(
        source_values
            .get("phenotype_or_disease")
            .map(String::as_str),
    ) {
        row.insert("phenotype_or_disease".to_string(), CellValue::Int8(v));
    }
    if let Some(v) = source_values.get("clinical_impact") {
        row.insert("clinical_impact".to_string(), CellValue::Utf8(v.clone()));
    }
    if let Some(v) = source_values.get("pubmed") {
        row.insert("pubmed".to_string(), CellValue::Utf8(v.clone()));
    }
    if let Some(v) = source_values.get("var_synonyms") {
        row.insert("var_synonyms".to_string(), CellValue::Utf8(v.clone()));
    }

    let source_ids = extract_source_ids(
        Some(variation_name_for_ids.as_str()),
        source_values.get("var_synonyms"),
        cache_info,
    );
    for (column, value) in source_ids {
        row.insert(column, CellValue::Utf8(value));
    }

    let mut known: HashSet<String> = [
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
    ]
    .into_iter()
    .chain(OPTIONAL_CANONICAL_COLUMNS)
    .map(ToString::to_string)
    .collect();
    for source in &cache_info.source_descriptors {
        known.insert(source.ids_column.clone());
    }

    for column_name in &cache_info.variation_cols {
        if known.contains(column_name.as_str()) {
            continue;
        }
        if let Some(value) = source_values.get(column_name) {
            row.insert(column_name.clone(), CellValue::Utf8(value.clone()));
        }
    }

    append_provenance(&mut row, cache_info, source_file);
    Ok(Some(row))
}

fn first_non_empty(values: &HashMap<String, String>, candidates: &[&str]) -> Option<String> {
    for key in candidates {
        if let Some(value) = values.get(*key) {
            if !value.is_empty() {
                return Some(value.clone());
            }
        }
    }
    None
}

fn parse_region_from_filename(name: &str) -> Option<i64> {
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

fn append_provenance(row: &mut Row, cache_info: &CacheInfo, source_file: &Path) {
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

fn extract_source_ids(
    variation_name: Option<&str>,
    var_synonyms: Option<&String>,
    cache_info: &CacheInfo,
) -> HashMap<String, String> {
    let source_to_id_column: HashMap<String, String> = cache_info
        .source_descriptors
        .iter()
        .map(|source| (source.source_key.clone(), source.ids_column.clone()))
        .collect();
    let mut buckets: HashMap<String, Vec<String>> = HashMap::new();

    if let Some(variation_name) = variation_name {
        assign_by_pattern(variation_name, &source_to_id_column, &mut buckets);
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
                    assign_by_pattern(token, &source_to_id_column, &mut buckets);
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
                assign_by_pattern(token, &source_to_id_column, &mut buckets);
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
