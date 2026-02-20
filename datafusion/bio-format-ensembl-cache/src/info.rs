use crate::errors::{Result, exec_err};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub(crate) struct SourceDescriptor {
    /// Normalized source name without `source_` prefix, e.g. `dbsnp`, `hgmd_public`.
    pub source_key: String,
    /// Column name exposing source version metadata, e.g. `source_dbsnp`.
    pub source_column: String,
    /// Column name exposing per-row IDs parsed from `var_synonyms`, e.g. `dbsnp_ids`.
    pub ids_column: String,
    /// Source metadata value from `info.txt`.
    pub value: String,
}

impl SourceDescriptor {
    fn from_info_source(key: &str, value: &str) -> Option<Self> {
        if !key.starts_with("source_") {
            return None;
        }

        let normalized = normalize_source_name(key.trim_start_matches("source_"));
        let value = value.trim();
        if normalized.is_empty() || value.is_empty() {
            return None;
        }

        Some(Self {
            source_key: normalized.clone(),
            source_column: format!("source_{}", normalized),
            ids_column: format!("{}_ids", normalized),
            value: value.to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CacheInfo {
    pub cache_root: PathBuf,
    pub source_cache_path: String,
    pub species: String,
    pub assembly: String,
    pub cache_version: String,
    pub serializer_type: Option<String>,
    pub var_type: Option<String>,
    pub cache_region_size: Option<i64>,
    pub variation_cols: Vec<String>,
    pub source_descriptors: Vec<SourceDescriptor>,
}

impl CacheInfo {
    pub fn from_root(cache_root: &Path) -> Result<Self> {
        let info_path = cache_root.join("info.txt");
        if !info_path.exists() {
            return Err(exec_err(format!(
                "Missing required VEP cache metadata file: {}",
                info_path.display()
            )));
        }

        let file = File::open(&info_path)
            .map_err(|e| exec_err(format!("Failed opening {}: {}", info_path.display(), e)))?;

        let mut species: Option<String> = None;
        let mut assembly: Option<String> = None;
        let mut cache_version: Option<String> = None;
        let mut serializer_type: Option<String> = None;
        let mut var_type: Option<String> = None;
        let mut cache_region_size: Option<i64> = None;
        let mut variation_cols: Vec<String> = Vec::new();
        let mut source_by_column: BTreeMap<String, SourceDescriptor> = BTreeMap::new();

        for line in BufReader::new(file).lines() {
            let line = line
                .map_err(|e| exec_err(format!("Failed reading {}: {}", info_path.display(), e)))?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let (key, value) = parse_key_value(line);
            let Some((key, value)) = key.zip(value) else {
                continue;
            };

            if let Some(source) = SourceDescriptor::from_info_source(&key, &value) {
                source_by_column.insert(source.source_column.clone(), source);
            }

            match key.as_str() {
                "species" => species = Some(value),
                "assembly" => assembly = Some(value),
                "cache_version" | "version" => cache_version = Some(value),
                "serialiser_type" | "serializer_type" | "serialiser" | "serializer" => {
                    serializer_type = Some(value.to_ascii_lowercase())
                }
                "var_type" => var_type = Some(value.to_ascii_lowercase()),
                "cache_region_size" | "region_size" => {
                    cache_region_size = value.parse::<i64>().ok();
                }
                "variation_cols" => variation_cols = parse_variation_cols(&value),
                _ => {}
            }
        }

        let source_cache_path = cache_root.to_string_lossy().to_string();
        Ok(Self {
            cache_root: cache_root.to_path_buf(),
            source_cache_path,
            species: species.unwrap_or_else(|| "unknown".to_string()),
            assembly: assembly.unwrap_or_else(|| "unknown".to_string()),
            cache_version: cache_version.unwrap_or_else(|| "unknown".to_string()),
            // Some merged/tabix cache bundles omit serializer metadata entirely.
            // VEP transcript/regulatory caches are storable in that layout.
            serializer_type: serializer_type.or_else(|| Some("storable".to_string())),
            var_type,
            cache_region_size,
            variation_cols,
            source_descriptors: source_by_column.into_values().collect(),
        })
    }
}

fn parse_key_value(line: &str) -> (Option<String>, Option<String>) {
    if let Some((key, value)) = line.split_once('=') {
        return (
            Some(key.trim().to_ascii_lowercase()),
            Some(value.trim().trim_matches('"').to_string()),
        );
    }

    let mut parts = line.split_whitespace();
    let Some(key) = parts.next() else {
        return (None, None);
    };
    let value = parts.collect::<Vec<_>>().join(" ");
    if value.is_empty() {
        return (None, None);
    }

    (
        Some(key.trim().to_ascii_lowercase()),
        Some(value.trim().trim_matches('"').to_string()),
    )
}

fn parse_variation_cols(raw: &str) -> Vec<String> {
    let cleaned = raw
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .replace('"', "")
        .replace('\'', "");

    cleaned
        .split([',', ' ', '\t'])
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn normalize_source_name(raw: &str) -> String {
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
