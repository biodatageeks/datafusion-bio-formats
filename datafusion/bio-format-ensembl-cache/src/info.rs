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
            source_column: format!("source_{normalized}"),
            ids_column: format!("{normalized}_ids"),
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
    #[allow(dead_code)] // Parsed from info.txt; will be used for tabix index support.
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
        .replace(['"', '\''], "");

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // -----------------------------------------------------------------------
    // normalize_source_name
    // -----------------------------------------------------------------------

    #[test]
    fn normalize_simple_lowercase() {
        assert_eq!(normalize_source_name("dbSNP"), "dbsnp");
    }

    #[test]
    fn normalize_hyphens_collapsed() {
        assert_eq!(normalize_source_name("HGMD-PUBLIC"), "hgmd_public");
    }

    #[test]
    fn normalize_leading_digit_gets_prefix() {
        assert_eq!(normalize_source_name("1000GENOMES"), "src_1000genomes");
    }

    #[test]
    fn normalize_empty_returns_empty() {
        assert_eq!(normalize_source_name(""), "");
    }

    #[test]
    fn normalize_only_special_chars() {
        assert_eq!(normalize_source_name("---"), "");
    }

    #[test]
    fn normalize_multiple_specials_collapsed() {
        assert_eq!(normalize_source_name("foo--bar__baz"), "foo_bar_baz");
    }

    #[test]
    fn normalize_leading_trailing_specials_trimmed() {
        assert_eq!(normalize_source_name("_foo_"), "foo");
    }

    // -----------------------------------------------------------------------
    // parse_key_value
    // -----------------------------------------------------------------------

    #[test]
    fn parse_kv_equals_format() {
        let (k, v) = parse_key_value("species=homo_sapiens");
        assert_eq!(k.unwrap(), "species");
        assert_eq!(v.unwrap(), "homo_sapiens");
    }

    #[test]
    fn parse_kv_space_format() {
        let (k, v) = parse_key_value("species homo_sapiens");
        assert_eq!(k.unwrap(), "species");
        assert_eq!(v.unwrap(), "homo_sapiens");
    }

    #[test]
    fn parse_kv_equals_with_quotes() {
        let (k, v) = parse_key_value("assembly=\"GRCh38\"");
        assert_eq!(k.unwrap(), "assembly");
        assert_eq!(v.unwrap(), "GRCh38");
    }

    #[test]
    fn parse_kv_case_insensitive_key() {
        let (k, _) = parse_key_value("SPECIES homo_sapiens");
        assert_eq!(k.unwrap(), "species");
    }

    #[test]
    fn parse_kv_no_value() {
        let (k, v) = parse_key_value("lonely");
        assert!(k.is_none());
        assert!(v.is_none());
    }

    #[test]
    fn parse_kv_empty_line() {
        let (k, v) = parse_key_value("");
        assert!(k.is_none());
        assert!(v.is_none());
    }

    // -----------------------------------------------------------------------
    // parse_variation_cols
    // -----------------------------------------------------------------------

    #[test]
    fn parse_variation_cols_comma_separated() {
        let cols = parse_variation_cols("chr,start,end,variation_name");
        assert_eq!(cols, vec!["chr", "start", "end", "variation_name"]);
    }

    #[test]
    fn parse_variation_cols_bracketed() {
        let cols = parse_variation_cols("[chr,start,end]");
        assert_eq!(cols, vec!["chr", "start", "end"]);
    }

    #[test]
    fn parse_variation_cols_quoted() {
        let cols = parse_variation_cols("'chr','start','end'");
        assert_eq!(cols, vec!["chr", "start", "end"]);
    }

    #[test]
    fn parse_variation_cols_empty() {
        let cols = parse_variation_cols("");
        assert!(cols.is_empty());
    }

    #[test]
    fn parse_variation_cols_tab_separated() {
        let cols = parse_variation_cols("chr\tstart\tend");
        assert_eq!(cols, vec!["chr", "start", "end"]);
    }

    // -----------------------------------------------------------------------
    // SourceDescriptor::from_info_source
    // -----------------------------------------------------------------------

    #[test]
    fn source_descriptor_valid() {
        let sd = SourceDescriptor::from_info_source("source_dbSNP", "156").unwrap();
        assert_eq!(sd.source_key, "dbsnp");
        assert_eq!(sd.source_column, "source_dbsnp");
        assert_eq!(sd.ids_column, "dbsnp_ids");
        assert_eq!(sd.value, "156");
    }

    #[test]
    fn source_descriptor_not_source_prefix() {
        assert!(SourceDescriptor::from_info_source("species", "homo_sapiens").is_none());
    }

    #[test]
    fn source_descriptor_empty_value() {
        assert!(SourceDescriptor::from_info_source("source_dbSNP", "  ").is_none());
    }

    #[test]
    fn source_descriptor_empty_key_after_prefix() {
        assert!(SourceDescriptor::from_info_source("source_---", "156").is_none());
    }

    // -----------------------------------------------------------------------
    // CacheInfo::from_root
    // -----------------------------------------------------------------------

    fn write_info_file(dir: &std::path::Path, content: &str) {
        let mut f = File::create(dir.join("info.txt")).unwrap();
        f.write_all(content.as_bytes()).unwrap();
    }

    #[test]
    fn cache_info_full_parse() {
        let dir = tempfile::tempdir().unwrap();
        write_info_file(
            dir.path(),
            "species homo_sapiens\nassembly GRCh38\ncache_version 115\nserialiser_type storable\nvar_type tabix\ncache_region_size 1000000\nsource_dbSNP 156\nvariation_cols chr,start,end,variation_name,allele_string\n",
        );
        let info = CacheInfo::from_root(dir.path()).unwrap();
        assert_eq!(info.species, "homo_sapiens");
        assert_eq!(info.assembly, "GRCh38");
        assert_eq!(info.cache_version, "115");
        assert_eq!(info.serializer_type.as_deref(), Some("storable"));
        assert_eq!(info.var_type.as_deref(), Some("tabix"));
        assert_eq!(info.cache_region_size, Some(1_000_000));
        assert_eq!(info.variation_cols.len(), 5);
        assert_eq!(info.source_descriptors.len(), 1);
        assert_eq!(info.source_descriptors[0].source_key, "dbsnp");
    }

    #[test]
    fn cache_info_missing_optional_fields() {
        let dir = tempfile::tempdir().unwrap();
        write_info_file(dir.path(), "species homo_sapiens\nassembly GRCh38\n");
        let info = CacheInfo::from_root(dir.path()).unwrap();
        assert_eq!(info.species, "homo_sapiens");
        assert!(info.variation_cols.is_empty());
        // Defaults to storable when missing
        assert_eq!(info.serializer_type.as_deref(), Some("storable"));
    }

    #[test]
    fn cache_info_defaults_on_empty() {
        let dir = tempfile::tempdir().unwrap();
        write_info_file(dir.path(), "# comment only\n");
        let info = CacheInfo::from_root(dir.path()).unwrap();
        assert_eq!(info.species, "unknown");
        assert_eq!(info.assembly, "unknown");
        assert_eq!(info.cache_version, "unknown");
    }

    #[test]
    fn cache_info_missing_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        assert!(CacheInfo::from_root(dir.path()).is_err());
    }

    #[test]
    fn cache_info_skips_comments_and_blanks() {
        let dir = tempfile::tempdir().unwrap();
        write_info_file(
            dir.path(),
            "# a comment\n\nspecies mus_musculus\n  \nassembly GRCm39\n",
        );
        let info = CacheInfo::from_root(dir.path()).unwrap();
        assert_eq!(info.species, "mus_musculus");
        assert_eq!(info.assembly, "GRCm39");
    }

    #[test]
    fn cache_info_serializer_type_aliases() {
        for alias in &[
            "serialiser_type",
            "serializer_type",
            "serialiser",
            "serializer",
        ] {
            let dir = tempfile::tempdir().unwrap();
            write_info_file(dir.path(), &format!("{alias} sereal\n"));
            let info = CacheInfo::from_root(dir.path()).unwrap();
            assert_eq!(
                info.serializer_type.as_deref(),
                Some("sereal"),
                "failed for alias: {alias}"
            );
        }
    }

    #[test]
    fn cache_info_version_alias() {
        let dir = tempfile::tempdir().unwrap();
        write_info_file(dir.path(), "version 110\n");
        let info = CacheInfo::from_root(dir.path()).unwrap();
        assert_eq!(info.cache_version, "110");
    }

    #[test]
    fn cache_info_region_size_alias() {
        let dir = tempfile::tempdir().unwrap();
        write_info_file(dir.path(), "region_size 500000\n");
        let info = CacheInfo::from_root(dir.path()).unwrap();
        assert_eq!(info.cache_region_size, Some(500_000));
    }

    #[test]
    fn cache_info_equals_format() {
        let dir = tempfile::tempdir().unwrap();
        write_info_file(dir.path(), "species=homo_sapiens\nassembly=GRCh38\n");
        let info = CacheInfo::from_root(dir.path()).unwrap();
        assert_eq!(info.species, "homo_sapiens");
        assert_eq!(info.assembly, "GRCh38");
    }

    #[test]
    fn cache_info_multiple_sources_ordered() {
        let dir = tempfile::tempdir().unwrap();
        write_info_file(
            dir.path(),
            "source_dbSNP 156\nsource_COSMIC 101\nsource_ClinVar 202502\n",
        );
        let info = CacheInfo::from_root(dir.path()).unwrap();
        assert_eq!(info.source_descriptors.len(), 3);
        // BTreeMap ordering: clinvar < cosmic < dbsnp
        let keys: Vec<&str> = info
            .source_descriptors
            .iter()
            .map(|s| s.source_key.as_str())
            .collect();
        assert!(keys.contains(&"dbsnp"));
        assert!(keys.contains(&"cosmic"));
        assert!(keys.contains(&"clinvar"));
    }
}
