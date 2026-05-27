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

/// Validates that the parsed cache info's assembly matches the expected value.
///
/// Equivalent to Perl `Bio::EnsEMBL::VEP::CacheDir->new` raising
/// "Cache assembly version <X> do not match" (CacheDir.t subtest #17) or
/// "Mismatch in assembly versions" (subtest #20). Vepyr uses a single
/// idiomatic error string for both control-flow paths since the underlying
/// invariant is identical: `info.assembly == expected`.
///
/// Test-driven helper: no production caller today. Surfaced as `pub(crate)`
/// for a future config-vs-cache validation pass (would live in
/// `AnnotateVcfConfig::validate_against(&CacheInfo)` once that lands).
#[allow(dead_code)]
pub(crate) fn validate_assembly(info: &CacheInfo, expected: &str) -> Result<()> {
    if info.assembly != expected {
        return Err(exec_err(format!(
            "Cache assembly mismatch: info.txt declares {} but expected {}",
            info.assembly, expected
        )));
    }
    Ok(())
}

/// Validates that the cache exposes a `gnomADe` source descriptor.
///
/// Equivalent to Perl `--af_gnomad` rejection when the cache lacks gnomADe
/// (CacheDir.t subtest #33; regex `/gnomad.+not available/`). Wording is
/// vepyr-idiomatic; the contract is "case-insensitive 'gnomad' and 'not
/// available' substrings are present in the error message".
///
/// Test-driven helper: no production caller today. Same future fold-in as
/// `validate_assembly`.
#[allow(dead_code)]
pub(crate) fn validate_af_gnomad(info: &CacheInfo) -> Result<()> {
    if !info
        .source_descriptors
        .iter()
        .any(|s| s.source_key == "gnomade")
    {
        return Err(exec_err(
            "gnomAD allele-frequency data not available in this cache \
             (no source_gnomADe entry in info.txt)",
        ));
    }
    Ok(())
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

    // =======================================================================
    // Port: CacheDir.t  (Perl `Bio::EnsEMBL::VEP::CacheDir`)
    //
    // Tracks `porting-tests/detailed_plans/CacheDir.md` (v2 paradigm; sztywno
    // 1:1; standalone tests). Each row in this submodule corresponds to a
    // numbered Perl subtest. See the audit for full justifications.
    //
    // Architectural-no-analogue rows (no Rust code; documented here):
    //   #3   Config->new ok                  vepyr has no separate
    //                                        construct-and-validate step on
    //                                        clap-derived AnnotateVcfConfig.
    //   #5   ref($cd) == 'CacheDir'          Rust uses concrete types; class
    //                                        identity is statically guaranteed.
    //   #21  $cd->root_dir == <root>         vepyr stores only the resolved
    //                                        full path (cache_root); no
    //                                        separate root_dir concept.
    //   #26  ref($as->[0]) == 'Cache::Transcript'
    //   #27  is_deeply($as->[0]->info, $cd->version_data)
    //   #31  ref($as->[1]) == 'Cache::Variation'
    //                                        Class identity + per-source info
    //                                        passthrough are Perl-internal;
    //                                        vepyr providers share one
    //                                        CacheInfo via ProviderInner.
    //   #34  3-source ordering [Transcript, RegFeat, Variation]
    //                                        vepyr has no ordered list of
    //                                        providers; per-source CSQ
    //                                        coverage owned by the per-source
    //                                        ports.
    //   #35  var_type='tabix' -> Cache::VariationTabix
    //                                        post-A1 spec collapse (2026-05-25,
    //                                        commit 99c4881): vepyr converts
    //                                        all variation caches to parquet
    //                                        at build time; var_type is
    //                                        parsed but no longer dispatches
    //                                        to a separate backend.
    // =======================================================================
    mod port_cache_dir {
        use super::*;
        use std::collections::HashMap;
        use std::fs::File;

        /// Writes a v115-shaped `info.txt` into `dir`. Used by the happy-path
        /// rows (4, 12, 22, 23, 24, B2, rowB1 negative) as a curated fixture.
        fn make_v115_info_txt(dir: &std::path::Path) {
            let content = "species\thomo_sapiens\n\
                           assembly\tGRCh38\n\
                           cache_version\t115\n\
                           serialiser_type\tstorable\n\
                           var_type\ttabix\n\
                           cache_region_size\t1000000\n\
                           variation_cols\tchr,variation_name,start,end,allele_string\n\
                           source_dbSNP\t156\n\
                           source_gnomADe\tv4.1\n";
            write_info_file(dir, content);
        }

        // -------------------------------------------------------------------
        // Row 4 / Row 12 / Row 22 — happy-path construction + cache_root getter
        // -------------------------------------------------------------------

        // SUBTEST #4 (Group A — constructor happy path):
        //   Perl: CacheDir->new({config, root_dir}) returns defined object.
        //   Rust: CacheInfo::from_root(path) returns Ok for a v115 info.txt.
        #[test]
        fn row04_constructor_happy_path() {
            let dir = tempfile::tempdir().unwrap();
            make_v115_info_txt(dir.path());
            let info = CacheInfo::from_root(dir.path()).expect("happy-path construction");
            assert_eq!(info.species, "homo_sapiens");
            assert_eq!(info.assembly, "GRCh38");
            assert_eq!(info.cache_version, "115");
        }

        // SUBTEST #12 (Group A — new({dir => full_path}) bypasses path
        // resolution): the full-path entry point is vepyr's primary mode.
        // Overlaps semantically with row 4; documented as a deliberate
        // duplicate to honour sztywno-1:1.
        #[test]
        fn row12_full_path_entry_point() {
            let dir = tempfile::tempdir().unwrap();
            make_v115_info_txt(dir.path());
            let info = CacheInfo::from_root(dir.path()).expect("full-path entry point");
            // Same shape as row04; the test exists to honour sztywno-1:1.
            assert_eq!(info.species, "homo_sapiens");
        }

        // SUBTEST #22 (Group C — $cd->dir getter returns resolved full path):
        //   Perl: $cd->dir == <cache_dir>
        //   Rust: info.cache_root equals the tempdir path passed in.
        #[test]
        fn row22_cache_root_getter() {
            let dir = tempfile::tempdir().unwrap();
            make_v115_info_txt(dir.path());
            let info = CacheInfo::from_root(dir.path()).unwrap();
            assert_eq!(info.cache_root, dir.path().to_path_buf());
        }

        // -------------------------------------------------------------------
        // Row 10 — no_fasta suppression (vacuous-true in vepyr)
        // -------------------------------------------------------------------

        // SUBTEST #10 (Group A — no_fasta => 1 suppresses FASTA auto-detection):
        //   Perl: with `no_fasta => 1`, `param('fasta')` returns falsy.
        //   Rust: vepyr's CacheInfo has no `fasta` field; there is no
        //   auto-detection to suppress. The equivalent in vepyr is: explicit
        //   FASTA path required, absence => no FASTA reference checks at all.
        //   This test asserts that even with a sibling .fa file present,
        //   CacheInfo::from_root never populates a fasta-like field
        //   (because the struct has no such field).
        #[test]
        fn row10_no_fasta_suppression_vacuous_in_vepyr() {
            let dir = tempfile::tempdir().unwrap();
            make_v115_info_txt(dir.path());
            // Write a .fa next to info.txt — Perl would auto-detect this.
            File::create(dir.path().join("test.fa")).unwrap();
            let info = CacheInfo::from_root(dir.path()).unwrap();
            // Enumerate CacheInfo fields explicitly; none of them is fasta-like.
            let _: &std::path::PathBuf = &info.cache_root;
            let _: &String = &info.source_cache_path;
            let _: &String = &info.species;
            let _: &String = &info.assembly;
            let _: &String = &info.cache_version;
            let _: &Option<String> = &info.serializer_type;
            let _: &Option<String> = &info.var_type;
            let _: &Option<i64> = &info.cache_region_size;
            let _: &Vec<String> = &info.variation_cols;
            let _: &Vec<SourceDescriptor> = &info.source_descriptors;
            // No fasta field exists; the .fa next to info.txt is invisible
            // to vepyr's parser. Suppression is vacuously true.
        }

        // -------------------------------------------------------------------
        // Row 17 / Row 20 — assembly-mismatch validator
        // -------------------------------------------------------------------

        // SUBTEST #17 (Group B5 — assembly mismatch error):
        //   Perl: throws /Cache assembly version .+bar.+ do not match/ when
        //         config passes assembly => 'bar' against a GRCh38 cache.
        //   Rust: validate_assembly returns Err with both names embedded.
        //   Wording deviation: vepyr uses "Cache assembly mismatch: ..."
        //   idiom rather than Perl regex literal; Rule 1 permits per-row
        //   wording deviation when contract semantics are preserved.
        #[test]
        fn row17_assembly_mismatch_config_vs_cache() {
            let dir = tempfile::tempdir().unwrap();
            make_v115_info_txt(dir.path());
            let info = CacheInfo::from_root(dir.path()).unwrap();
            let err = validate_assembly(&info, "bar")
                .expect_err("validator must reject mismatched assembly")
                .to_string();
            assert!(err.contains("GRCh38"), "err missing GRCh38: {err}");
            assert!(err.contains("bar"), "err missing 'bar': {err}");
            assert!(
                err.to_lowercase().contains("assembly"),
                "err missing 'assembly': {err}"
            );
        }

        // SUBTEST #20 (Group B8 — info.txt vs config silent divergence):
        //   Perl: cache dir physically exists but info.txt assembly mismatches
        //         /Mismatch in assembly versions/. Distinct control-flow path
        //         from #17 (which tests config-vs-cache mismatch).
        //   Rust: same validator, inverse scenario (info.txt declares GRCh37
        //   but caller expected GRCh38).
        #[test]
        fn row20_assembly_mismatch_info_vs_config() {
            let dir = tempfile::tempdir().unwrap();
            write_info_file(
                dir.path(),
                "species\thomo_sapiens\nassembly\tGRCh37\ncache_version\t115\n",
            );
            let info = CacheInfo::from_root(dir.path()).unwrap();
            assert_eq!(info.assembly, "GRCh37");
            let err = validate_assembly(&info, "GRCh38")
                .expect_err("validator must reject info-vs-config mismatch")
                .to_string();
            assert!(err.contains("GRCh37"), "err missing GRCh37: {err}");
            assert!(err.contains("GRCh38"), "err missing GRCh38: {err}");
        }

        // -------------------------------------------------------------------
        // Row 23 — full info-hash snapshot (v115 shape)
        // -------------------------------------------------------------------

        // SUBTEST #23 (Group C — is_deeply $cd->info, {...} 60-line snapshot):
        //   Perl: full info hash snapshot, including v84-only fields.
        //   Rust: explicit field-by-field assertion against the v115-shaped
        //   tempdir info.txt. Perl-only fields (polyphen, sift, cell_types,
        //   regulatory, build, valid_chromosomes) are INTENTIONALLY OMITTED:
        //   they are not on vepyr's CacheInfo struct, by design. See
        //   CacheDir.md "Known v115-vs-Perl-fixture differences".
        //   cell_types specifically is a blocked-future-work entry (row #29).
        #[test]
        fn row23_info_snapshot_v115_shape() {
            let dir = tempfile::tempdir().unwrap();
            make_v115_info_txt(dir.path());
            let info = CacheInfo::from_root(dir.path()).unwrap();
            assert_eq!(info.species, "homo_sapiens");
            assert_eq!(info.assembly, "GRCh38");
            assert_eq!(info.cache_version, "115");
            assert_eq!(info.serializer_type.as_deref(), Some("storable"));
            assert_eq!(info.var_type.as_deref(), Some("tabix"));
            assert_eq!(info.cache_region_size, Some(1_000_000));
            assert_eq!(
                info.variation_cols,
                vec!["chr", "variation_name", "start", "end", "allele_string"]
            );
            assert_eq!(
                info.source_descriptors.len(),
                2,
                "fixture declares source_dbSNP + source_gnomADe"
            );
        }

        // -------------------------------------------------------------------
        // Row 24 — version_data sub-hash via source_descriptors
        // -------------------------------------------------------------------

        // SUBTEST #24 (Group C — is_deeply $cd->version_data, {...}):
        //   Perl: source-version sub-hash on the cache.
        //   Rust: source_descriptors carries the same role as Perl's
        //   version_data. Field mapping: Perl's version_data includes
        //   `assembly`, which in vepyr lives on CacheInfo::assembly (NOT on
        //   source_descriptors).
        #[test]
        fn row24_version_data_via_source_descriptors() {
            let dir = tempfile::tempdir().unwrap();
            make_v115_info_txt(dir.path());
            let info = CacheInfo::from_root(dir.path()).unwrap();
            let by_key: HashMap<&str, &str> = info
                .source_descriptors
                .iter()
                .map(|s| (s.source_key.as_str(), s.value.as_str()))
                .collect();
            assert_eq!(by_key.get("dbsnp"), Some(&"156"));
            assert_eq!(by_key.get("gnomade"), Some(&"v4.1"));
            // `assembly` lives on CacheInfo, not on source_descriptors:
            assert_eq!(info.assembly, "GRCh38");
        }

        // -------------------------------------------------------------------
        // Row 32 / Row 33 — gnomADe presence + absence validator
        // -------------------------------------------------------------------

        // SUBTEST #32 (Group D — check_existing=1 + af_gnomad=1 against v84
        // cache succeeds because v84 has source_gnomADe):
        //   Perl: passes silently when the cache declares source_gnomADe.
        //   Rust: validate_af_gnomad returns Ok when source_descriptors
        //   contains "gnomade".
        #[test]
        fn row32_af_gnomad_passes_when_source_present() {
            let dir = tempfile::tempdir().unwrap();
            make_v115_info_txt(dir.path()); // includes source_gnomADe v4.1
            let info = CacheInfo::from_root(dir.path()).unwrap();
            validate_af_gnomad(&info).expect("validator must accept gnomADe-bearing cache");
        }

        // SUBTEST #33 (Group D — check_existing=1 + af_gnomad=1 against
        // ExAC-only cache throws):
        //   Perl: throws /gnomad.+not available/.
        //   Rust: validate_af_gnomad returns Err. Wording: case-insensitive
        //   match to Perl regex (both "gnomad" and "not available" substrings
        //   must be present in the error message).
        #[test]
        fn row33_af_gnomad_rejects_when_source_absent() {
            let dir = tempfile::tempdir().unwrap();
            // Curated ExAC-shaped info.txt (no source_gnomADe).
            write_info_file(
                dir.path(),
                "species\thomo_sapiens\nassembly\tGRCh38\nsource_ExAC\t0.3\n",
            );
            let info = CacheInfo::from_root(dir.path()).unwrap();
            let err = validate_af_gnomad(&info)
                .expect_err("validator must reject gnomADe-less cache")
                .to_string()
                .to_lowercase();
            assert!(
                err.contains("gnomad") && err.contains("not available"),
                "err missing required substrings: {err}"
            );
        }

        // -------------------------------------------------------------------
        // Axis-B Row B1 — post-A1 collapse: parquet serializer accepted by parser
        // -------------------------------------------------------------------

        // AXIS-B ROW B1 (vepyr-side invariant; verified via verify-sweeps PR #21):
        //   CacheInfo::from_root accepts serializer_type=parquet without
        //   rejection. The validate_serializer guard
        //   (table_provider.rs::validate_serializer) is only invoked from
        //   ProviderInner::new for transcript/regulatory/motif/exon/translation
        //   entities — NOT from CacheInfo::from_root itself. The partitioned
        //   parquet production path used by annotate_to_vcf bypasses
        //   CacheInfo::from_root and EnsemblCacheTableProvider::for_entity
        //   entirely (it reads Arrow metadata via
        //   CacheSourceType::from_partitioned_cache_source instead).
        #[allow(non_snake_case)]
        #[test]
        fn rowB1_from_root_accepts_parquet_serializer() {
            let dir = tempfile::tempdir().unwrap();
            write_info_file(
                dir.path(),
                "species\thomo_sapiens\nassembly\tGRCh38\nserializer_type\tparquet\n",
            );
            let info = CacheInfo::from_root(dir.path())
                .expect("CacheInfo::from_root must accept serializer_type=parquet");
            assert_eq!(info.serializer_type.as_deref(), Some("parquet"));
        }

        // -------------------------------------------------------------------
        // Axis-B Row B2 — post-A1 collapse: var_type=tabix parsed-but-no-op
        // -------------------------------------------------------------------

        // AXIS-B ROW B2 (vepyr-side invariant):
        //   var_type=tabix is parsed into Some("tabix") on CacheInfo, but
        //   does NOT trigger any separate tabix-backend dispatch at provider
        //   construction. Pins the post-A1 invariant that the field is
        //   parsed-but-no-op, preventing accidental re-introduction of a
        //   tabix backend path.
        #[allow(non_snake_case)]
        #[test]
        fn rowB2_var_type_tabix_parsed_but_no_dispatch() {
            let dir = tempfile::tempdir().unwrap();
            make_v115_info_txt(dir.path());
            let info = CacheInfo::from_root(dir.path()).unwrap();
            assert_eq!(info.var_type.as_deref(), Some("tabix"));
            // No backend-dispatch effect to assert positively. The contract
            // is "the parsed value never reaches a backend selector"; this
            // is structurally enforced by ProviderInner::new (table_provider.rs:120)
            // not branching on info.var_type at all.
        }

        // ===================================================================
        // Blocked-future-work rows (commented-out tests).
        //
        // Each row below is gated on a missing vepyr API. The example test
        // body documents what the test would assert once the API lands; the
        // commented-out form keeps cargo green while the gap exists. Cross-
        // reference: `porting-tests/future-work-vepyr.md`.
        // ===================================================================

        // SUBTEST #6 (Group A — auto-detected synonyms path):
        //   Perl: param('synonyms') resolves to <cache_dir>/chr_synonyms.txt
        //         when present in the cache directory.
        //   Rust: BLOCKED — no auto_detect_chr_synonyms helper in vepyr.
        //   Future-work entry: future-work-vepyr.md
        //     §"auto_detect_chr_synonyms / auto_detect_fasta on cache_root".
        //
        // #[test]
        // fn row06_auto_detect_chr_synonyms() {
        //     let dir = tempfile::tempdir().unwrap();
        //     make_v115_info_txt(dir.path());
        //     File::create(dir.path().join("chr_synonyms.txt")).unwrap();
        //     // let detected = auto_detect_chr_synonyms(dir.path()); // missing API
        //     // assert_eq!(detected, Some(dir.path().join("chr_synonyms.txt")));
        // }

        // SUBTEST #7 (Group A — auto-detected FASTA path):
        //   Perl: param('fasta') resolves to the cache dir's *.fa file.
        //   Rust: BLOCKED — no auto_detect_fasta helper in vepyr.
        //   Future-work entry: same as #6.
        //
        // #[test]
        // fn row07_auto_detect_fasta() {
        //     let dir = tempfile::tempdir().unwrap();
        //     make_v115_info_txt(dir.path());
        //     File::create(dir.path().join("test.fa")).unwrap();
        //     // let detected = auto_detect_fasta(dir.path()); // missing API
        //     // assert_eq!(detected, Some(dir.path().join("test.fa")));
        // }

        // SUBTEST #8 (Group A — config-set synonyms wins over auto-detect):
        //   Perl: param('synonyms') == 'bar' when config provides 'bar' even
        //         though chr_synonyms.txt is present.
        //   Rust: BLOCKED — gated on row 6 + a config priority rule.
        //
        // #[test]
        // fn row08_synonyms_config_wins_over_autodetect() {
        //     // ... gated on row 6 helper landing ...
        // }

        // SUBTEST #9 (Group A — config-set FASTA wins over auto-detect):
        //   Perl: param('fasta') == 'foo' even though cache_dir/*.fa exists.
        //   Rust: BLOCKED — gated on row 7 + a config priority rule.
        //
        // #[test]
        // fn row09_fasta_config_wins_over_autodetect() {
        //     // ... gated on row 7 helper landing ...
        // }

        // SUBTEST #11 (Group A — new({cache_version}) resolves species + assembly):
        //   Perl: with only cache_version on the config, CacheDir scans the
        //         root_dir for the unique species/assembly under that version.
        //   Rust: BLOCKED — vepyr has no resolve_cache_dir helper.
        //   Future-work entry: future-work-vepyr.md §"resolve_cache_dir —
        //     path resolver from (species, version, assembly)".
        //
        // #[test]
        // fn row11_new_resolves_species_assembly_from_cache_version() {
        //     // let path = resolve_cache_dir(root, None, Some(115), None)?;
        //     // assert_eq!(path, root.join("homo_sapiens/115_GRCh38"));
        // }

        // SUBTEST #13 (Group B1 — no root_dir and no dir):
        //   Perl: throws /No root_dir or dir specified/.
        //   Rust: BLOCKED — gated on resolve_cache_dir (same entry).
        //
        // #[test]
        // fn row13_no_root_dir_or_dir_throws() {
        //     // resolve_cache_dir(None, ...) → Err containing "No root_dir or dir specified"
        // }

        // SUBTEST #14 (Group B2 — species='bar', no such dir):
        //   Perl: throws /Cache directory .+ not found/.
        //   Rust: BLOCKED — gated on resolve_cache_dir.
        //
        // #[test]
        // fn row14_unknown_species_throws() {
        //     // resolve_cache_dir(root, Some("bar"), Some(115), None) → Err "not found"
        // }

        // SUBTEST #15 (Group B3 — species='homo_sapiens_refseq' deprecated suffix):
        //   Perl: throws a helpful /Should not use .+ as --species/ message.
        //   Rust: BLOCKED — gated on resolve_cache_dir (vepyr's equivalent
        //   error exists but for a different reason: cache_source_type must
        //   be explicit; see table_provider.rs:121).
        //
        // #[test]
        // fn row15_refseq_species_suffix_throws_helpful_message() {
        //     // resolve_cache_dir(root, Some("homo_sapiens_refseq"), ...) → Err
        // }

        // SUBTEST #16 (Group B4 — cache_version=20, no such cache):
        //   Perl: throws /No cache found for .+ 20/.
        //   Rust: BLOCKED — gated on resolve_cache_dir.
        //
        // #[test]
        // fn row16_unknown_cache_version_throws() {
        //     // resolve_cache_dir(root, None, Some(20), None) → Err "No cache found"
        // }

        // SUBTEST #18 (Group B6 — no assembly, multiple available under species):
        //   Perl: throws /Multiple assemblies found/.
        //   Rust: BLOCKED — ambiguity-handling is part of resolve_cache_dir.
        //
        // #[test]
        // fn row18_multiple_assemblies_throws() {
        //     // resolve_cache_dir(root, Some("foo"), Some(115), None) → Err "Multiple"
        // }

        // SUBTEST #19 (Group B7 — species='foo', assembly='tar', no match):
        //   Perl: throws /No cache found/.
        //   Rust: BLOCKED — gated on resolve_cache_dir.
        //
        // #[test]
        // fn row19_unknown_species_assembly_combo_throws() {
        //     // resolve_cache_dir(root, Some("foo"), Some(115), Some("tar")) → Err
        // }

        // SUBTEST #29 (Group D — RegFeat available_cell_types matches 37-entry list):
        //   Perl: $as->[1]->{available_cell_types} parsed from info.txt
        //         cell_types line.
        //   Rust: BLOCKED — `cell_types` is not on vepyr's CacheInfo.
        //   Future-work entry: future-work-vepyr.md §"CacheInfo::cell_types".
        //
        // #[test]
        // fn row29_cell_types_parsed_from_info_txt() {
        //     let dir = tempfile::tempdir().unwrap();
        //     write_info_file(
        //         dir.path(),
        //         "species\thomo_sapiens\nassembly\tGRCh38\ncell_types\tA549,GM12878,K562\n",
        //     );
        //     let info = CacheInfo::from_root(dir.path()).unwrap();
        //     // assert_eq!(info.cell_types, vec!["A549","GM12878","K562"]); // missing field
        // }
    }

    // -----------------------------------------------------------------------
    // v2 port of ensembl-vep/t/AnnotationSource_Cache.t
    // (see porting-tests/detailed_plans/AnnotationSource_Cache.md)
    //
    // Perl test under port: 4 substantive subtests (+1 `use_ok` boilerplate)
    // pinning the `dir` accessor contract on
    // `Bio::EnsEMBL::VEP::AnnotationSource::Cache`. Vepyr's analogue is the
    // `CacheInfo::cache_root` field plus `CacheInfo::from_root` constructor.
    //
    // Coverage parity (per detailed_plan): 2/3 = 66.7%. Row 4 (setter) is
    // blocked-future-work because vepyr treats `cache_root` as immutable.
    // -----------------------------------------------------------------------
    mod port_annotation_source_cache {
        use super::*;

        // Build a tempdir holding a minimal `info.txt` the constructor accepts.
        // Mirrors the v84-cache fixture shape that VEPTestingConfig provides.
        fn minimal_info_dir() -> tempfile::TempDir {
            let dir = tempfile::tempdir().unwrap();
            write_info_file(dir.path(), "species homo_sapiens\nassembly GRCh38\n");
            dir
        }

        // Subtest row 2 (Perl: `Cache->new({dir => $dir})` returns defined).
        // Vepyr analogue: `CacheInfo::from_root(&Path)` returns `Ok(_)` for a
        // directory containing a minimal `info.txt`. Perl's blessed-ref check
        // has no direct analogue; the closest Rust contract is "constructor
        // returned a value carrying the supplied path" — i.e. `Ok(_)`.
        #[test]
        fn row2_constructor_accepts_dir() {
            let dir = minimal_info_dir();
            let info = CacheInfo::from_root(dir.path())
                .expect("constructor accepts a path with minimal info.txt");
            // Sanity: result carries the supplied path (cross-checked in row 3).
            assert_eq!(info.cache_root, dir.path().to_path_buf());
        }

        // Subtest row 3 (Perl: `$c->dir == $dir` getter).
        // Vepyr analogue: `CacheInfo::cache_root` is a `pub PathBuf` field that
        // mirrors the constructor argument verbatim (info.rs:42, 112).
        #[test]
        fn row3_dir_getter_returns_construction_value() {
            let dir = minimal_info_dir();
            let info = CacheInfo::from_root(dir.path()).unwrap();
            assert_eq!(info.cache_root, dir.path().to_path_buf());
        }

        // Subtest row 4 (Perl: `$c->dir('/tmp')` setter) — BLOCKED-FUTURE-WORK.
        //
        // `CacheInfo` has no setter for `cache_root`; the field is immutable
        // post-construction by design (every consumer — parquet readers, schema
        // metadata, region discovery — assumes a fixed path for the lifetime
        // of the object). Adding `set_cache_root(&mut self, PathBuf)` would be
        // backwards because derived state would also need to be invalidated.
        //
        // See: porting-tests/future-work-vepyr.md ::
        //      "CacheInfo::set_cache_root (or with_cache_root builder)".
        //
        // #[test]
        // fn row4_dir_setter_mutates_and_returns() {
        //     let dir1 = minimal_info_dir();
        //     let dir2 = minimal_info_dir();
        //     let mut info = CacheInfo::from_root(dir1.path()).unwrap();
        //     info.set_cache_root(dir2.path().to_path_buf());
        //     assert_eq!(info.cache_root, dir2.path().to_path_buf());
        // }
    }
}
