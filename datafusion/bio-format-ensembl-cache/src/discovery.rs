use crate::entity::EnsemblEntityKind;
use crate::errors::{Result, exec_err};
use crate::physical_exec::parse_file_chrom_region;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn discover_variation_files(cache_root: &Path) -> Result<Vec<PathBuf>> {
    let mut all_files = Vec::new();
    walk_files(cache_root, &mut all_files)?;

    let mut all_vars: Vec<PathBuf> = all_files
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|v| v.to_str())
                .is_some_and(|name| {
                    let lower = name.to_ascii_lowercase();
                    lower.starts_with("all_vars") && !is_index_sidecar_name(&lower)
                })
        })
        .cloned()
        .collect();

    if !all_vars.is_empty() {
        all_vars.sort();
        return Ok(all_vars);
    }

    let mut region_files: Vec<PathBuf> = all_files
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|v| v.to_str())
                .is_some_and(|name| {
                    let lower = name.to_ascii_lowercase();
                    !is_index_sidecar_name(&lower)
                        && (lower.contains("_var")
                            || name.ends_with(".var")
                            || name.ends_with(".var.gz")
                            || lower.contains("var.gz")
                            || (looks_like_region_file_name(&lower)
                                && !lower.ends_with("_reg.gz")
                                && !lower.contains("transcript")))
                })
        })
        .cloned()
        .collect();

    sort_variation_files_genomic(&mut region_files);
    Ok(region_files)
}

pub(crate) fn discover_transcript_files(cache_root: &Path) -> Result<Vec<PathBuf>> {
    let explicit = discover_entity_files(cache_root, &["transcript"])?;
    if !explicit.is_empty() {
        return Ok(explicit);
    }

    // Merged/tabix cache layout stores transcript chunks as region files
    // like `1-1000000.gz` without "transcript" in the filename.
    let mut all_files = Vec::new();
    walk_files(cache_root, &mut all_files)?;

    let mut result: Vec<PathBuf> = all_files
        .into_iter()
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();

            if is_index_sidecar_name(&name) {
                return false;
            }

            if name == "info.txt" {
                return false;
            }

            if name.contains("regulatory")
                || name.contains("regfeat")
                || name.contains("_reg")
                || name.contains("_var")
                || name.contains("all_vars")
                || name.ends_with(".var")
                || name.ends_with(".var.gz")
                || name.contains("var.gz")
            {
                return false;
            }

            looks_like_region_file_name(&name)
        })
        .collect();

    result.sort();
    Ok(result)
}

pub(crate) fn discover_regulatory_files(cache_root: &Path) -> Result<Vec<PathBuf>> {
    discover_entity_files(cache_root, &["regulatory", "regfeat", "_reg.gz", "_reg"])
}

fn discover_entity_files(cache_root: &Path, name_patterns: &[&str]) -> Result<Vec<PathBuf>> {
    let mut all_files = Vec::new();
    walk_files(cache_root, &mut all_files)?;

    let mut result: Vec<PathBuf> = all_files
        .into_iter()
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            !is_index_sidecar_name(&name)
                && name_patterns.iter().any(|pattern| name.contains(pattern))
        })
        .collect();

    result.sort();
    Ok(result)
}

fn walk_files(root: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let mut stack = vec![root.to_path_buf()];

    while let Some(path) = stack.pop() {
        let meta = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.file_type().is_file() {
            out.push(path);
            continue;
        }
        if !meta.file_type().is_dir() {
            continue; // skip symlinks and other non-regular entries
        }

        let entries = fs::read_dir(&path)
            .map_err(|e| exec_err(format!("Failed listing {}: {}", path.display(), e)))?;
        for entry in entries {
            let entry = entry.map_err(|e| exec_err(format!("Failed reading dir entry: {e}")))?;
            let child = entry.path();
            let child_meta = match fs::symlink_metadata(&child) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if child_meta.file_type().is_dir() {
                stack.push(child);
            } else if child_meta.file_type().is_file() {
                out.push(child);
            }
        }
    }

    Ok(())
}

fn is_index_sidecar_name(name: &str) -> bool {
    name.ends_with(".csi") || name.ends_with(".tbi")
}

/// Sort variation files in genomic order: (chromosome_numeric, region_start).
///
/// Canonical chromosomes sort first in karyotypic order (1..22, X, Y, MT),
/// followed by non-canonical contigs in lexicographic order.  Within the same
/// chromosome, files are sorted by their region start position.
///
/// Files whose names cannot be parsed (e.g. `all_vars.gz`) are placed at the
/// end, preserving their relative order.
pub(crate) fn sort_variation_files_genomic(files: &mut [PathBuf]) {
    files.sort_by(|a, b| {
        let pa = a
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(parse_chrom_and_start);
        let pb = b
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(parse_chrom_and_start);

        match (pa, pb) {
            (Some((ca, sa)), Some((cb, sb))) => chrom_sort_key(ca)
                .cmp(&chrom_sort_key(cb))
                .then(sa.cmp(&sb)),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => a.cmp(b),
        }
    });
}

/// Extract (chrom, start) from a variation filename like `1_1-1000000_var.gz`.
///
/// **Note:** splits on the first `_`, so contig names containing underscores
/// (e.g. `HSCHR1_CTG1_UNLOCALIZED`) will mis-parse.  In practice these files
/// fall through as "unparseable" and are placed at the end of the sort order,
/// which is the correct behaviour for non-canonical contigs.
fn parse_chrom_and_start(name: &str) -> Option<(&str, i64)> {
    let marker = name.find('_')?;
    let chrom = &name[..marker];
    let suffix = &name[marker + 1..];
    let start_end = suffix.split_once('_')?.0; // "1-1000000"
    let start_str = start_end.split_once('-')?.0;
    let start = start_str.parse::<i64>().ok()?;
    Some((chrom, start))
}

/// Map chromosome name to a sort key that follows karyotypic order.
///
/// Canonical: 1-22 → 1-22, X → 23, Y → 24, MT → 25.
/// Non-canonical: 100 + lexicographic position (sorts after canonical).
pub(crate) fn chrom_sort_key(chrom: &str) -> (u32, String) {
    match chrom {
        "X" | "x" => (23, String::new()),
        "Y" | "y" => (24, String::new()),
        "MT" | "mt" | "Mt" => (25, String::new()),
        s => {
            if let Ok(n) = s.parse::<u32>() {
                (n, String::new())
            } else {
                (100, s.to_string())
            }
        }
    }
}

/// Well-known entity directory names that are NOT chromosome names.
const ENTITY_DIR_NAMES: &[&str] = &["variation", "transcript", "regulatory", "motif", "regfeat"];

/// Extracts the chromosome name from a discovered file path.
///
/// Uses entity-kind-aware heuristics based on VEP cache naming conventions:
/// - Variation region files: `{chrom}_{start}-{end}_var.gz` → chrom from filename
/// - `all_vars.gz` / merged region files: chrom from parent directory name
/// - Explicit layout: `chr{N}_transcript.storable.gz` → strip `chr` prefix
///
/// Returns `None` if the chromosome cannot be reliably determined.
pub(crate) fn extract_chrom_from_path(path: &Path, kind: EnsemblEntityKind) -> Option<String> {
    let name = path.file_name()?.to_str()?;

    // For variation files, try parsing `{chrom}_{start}-{end}_var.gz` first
    if kind == EnsemblEntityKind::Variation
        && let Some((chrom, _, _)) = parse_file_chrom_region(name)
    {
        return Some(chrom.to_string());
    }

    // Try parent directory name (works for merged/tabix layout: `1/all_vars.gz`,
    // `22/1-1000000.gz`, etc.)
    let parent_name = path.parent()?.file_name()?.to_str()?;
    if !ENTITY_DIR_NAMES.contains(&parent_name.to_ascii_lowercase().as_str()) {
        return Some(parent_name.to_string());
    }

    // Explicit layout: `chr{N}_transcript.storable.gz`, `chr{N}_regulatory.storable.gz`
    // Use original name to preserve chromosome case (e.g. `X`, `MT`)
    let stem = extract_chrom_prefix_from_explicit_name(name)?;
    Some(stem.to_string())
}

/// Extracts chrom from explicit-layout filenames like `chr1_transcript.storable.gz`.
/// Returns the chromosome portion (e.g. `"1"` from `"chr1_transcript.storable.gz"`).
/// The `chr` prefix is matched case-insensitively to preserve the original case of
/// the chromosome token (e.g. `chrX_...` → `"X"`, not `"x"`).
fn extract_chrom_prefix_from_explicit_name(name: &str) -> Option<&str> {
    // Must start with "chr" (case-insensitive)
    if name.len() < 3 || !name[..3].eq_ignore_ascii_case("chr") {
        return None;
    }
    let rest = &name[3..];
    // Find the next underscore — everything between "chr" and "_" is the chrom
    let underscore_pos = rest.find('_')?;
    let chrom = &rest[..underscore_pos];
    if chrom.is_empty() {
        return None;
    }
    Some(chrom)
}

/// Extracts distinct chromosome values from a set of discovered file paths.
///
/// Returns `None` if any file's chromosome cannot be determined, signaling
/// that a full scan is required for correctness.
pub(crate) fn extract_distinct_chroms(
    files: &[PathBuf],
    kind: EnsemblEntityKind,
) -> Option<Vec<String>> {
    let mut chroms = BTreeSet::new();
    for path in files {
        chroms.insert(extract_chrom_from_path(path, kind)?);
    }
    Some(chroms.into_iter().collect())
}

fn looks_like_region_file_name(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".gz") else {
        return false;
    };
    let Some((start, end)) = stem.split_once('-') else {
        return false;
    };
    !start.is_empty()
        && !end.is_empty()
        && start.chars().all(|c| c.is_ascii_digit())
        && end.chars().all(|c| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // -----------------------------------------------------------------------
    // is_index_sidecar_name
    // -----------------------------------------------------------------------

    #[test]
    fn sidecar_csi() {
        assert!(is_index_sidecar_name("all_vars.gz.csi"));
    }

    #[test]
    fn sidecar_tbi() {
        assert!(is_index_sidecar_name("all_vars.gz.tbi"));
    }

    #[test]
    fn not_sidecar_gz() {
        assert!(!is_index_sidecar_name("all_vars.gz"));
    }

    // -----------------------------------------------------------------------
    // looks_like_region_file_name
    // -----------------------------------------------------------------------

    #[test]
    fn region_file_typical() {
        assert!(looks_like_region_file_name("1-1000000.gz"));
    }

    #[test]
    fn region_file_large_numbers() {
        assert!(looks_like_region_file_name("15000001-16000000.gz"));
    }

    #[test]
    fn not_region_file_no_gz() {
        assert!(!looks_like_region_file_name("1-1000000"));
    }

    #[test]
    fn not_region_file_no_dash() {
        assert!(!looks_like_region_file_name("1000000.gz"));
    }

    #[test]
    fn not_region_file_alpha() {
        assert!(!looks_like_region_file_name("chr1-1000000.gz"));
    }

    #[test]
    fn not_region_file_empty_parts() {
        assert!(!looks_like_region_file_name("-1000000.gz"));
    }

    // -----------------------------------------------------------------------
    // discover_variation_files
    // -----------------------------------------------------------------------

    fn create_file(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, b"test").unwrap();
    }

    #[test]
    fn variation_prefers_all_vars() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("1/all_vars.gz"));
        create_file(&dir.path().join("1/1_1-1000000_var.gz"));

        let files = discover_variation_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].to_str().unwrap().contains("all_vars"));
    }

    #[test]
    fn variation_falls_back_to_region_files() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("1/1_1-1000000_var.gz"));
        create_file(&dir.path().join("2/2_1-1000000_var.gz"));

        let files = discover_variation_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn variation_excludes_index_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("all_vars.gz"));
        create_file(&dir.path().join("all_vars.gz.csi"));
        create_file(&dir.path().join("all_vars.gz.tbi"));

        let files = discover_variation_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert!(!files[0].to_str().unwrap().contains(".csi"));
    }

    #[test]
    fn variation_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let files = discover_variation_files(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    // -----------------------------------------------------------------------
    // discover_transcript_files
    // -----------------------------------------------------------------------

    #[test]
    fn transcript_finds_explicit_dir() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("transcript/chr1_transcript.storable.gz"));
        create_file(&dir.path().join("transcript/chr2_transcript.storable.gz"));

        let files = discover_transcript_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn transcript_merged_layout_finds_region_files() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("1/1-1000000.gz"));
        create_file(&dir.path().join("2/1-1000000.gz"));

        let files = discover_transcript_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn transcript_merged_excludes_var_files() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("1/1-1000000.gz"));
        create_file(&dir.path().join("1/1_1-1000000_var.gz"));
        create_file(&dir.path().join("info.txt"));

        let files = discover_transcript_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].to_str().unwrap().contains("1-1000000.gz"));
    }

    #[test]
    fn transcript_merged_excludes_regulatory() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("1/1-1000000.gz"));
        create_file(&dir.path().join("regulatory/chr1_regulatory.storable.gz"));

        let files = discover_transcript_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn transcript_excludes_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("transcript/chr1_transcript.storable.gz"));
        create_file(
            &dir.path()
                .join("transcript/chr1_transcript.storable.gz.csi"),
        );

        let files = discover_transcript_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
    }

    // -----------------------------------------------------------------------
    // discover_regulatory_files
    // -----------------------------------------------------------------------

    #[test]
    fn regulatory_finds_files() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("regulatory/chr1_regulatory.storable.gz"));
        create_file(&dir.path().join("regulatory/chr2_regulatory.storable.gz"));

        let files = discover_regulatory_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn regulatory_matches_regfeat() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("1/1_regfeat.gz"));

        let files = discover_regulatory_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn regulatory_matches_reg_suffix() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("1/1_1-1000000_reg.gz"));

        let files = discover_regulatory_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn regulatory_excludes_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("regulatory/chr1_regulatory.storable.gz"));
        create_file(
            &dir.path()
                .join("regulatory/chr1_regulatory.storable.gz.tbi"),
        );

        let files = discover_regulatory_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
    }

    // -----------------------------------------------------------------------
    // walk_files
    // -----------------------------------------------------------------------

    #[test]
    fn walk_files_nested() {
        let dir = tempfile::tempdir().unwrap();
        create_file(&dir.path().join("a/b/c.txt"));
        create_file(&dir.path().join("a/d.txt"));
        create_file(&dir.path().join("e.txt"));

        let mut out = Vec::new();
        walk_files(dir.path(), &mut out).unwrap();
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn walk_files_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let mut out = Vec::new();
        walk_files(dir.path(), &mut out).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn walk_files_nonexistent_path() {
        let dir = tempfile::tempdir().unwrap();
        let mut out = Vec::new();
        walk_files(&dir.path().join("does_not_exist"), &mut out).unwrap();
        assert!(out.is_empty());
    }

    // -----------------------------------------------------------------------
    // extract_chrom_from_path
    // -----------------------------------------------------------------------

    #[test]
    fn chrom_variation_region_file() {
        let path = PathBuf::from("/cache/variation/1_1-1000000_var.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::Variation),
            Some("1".to_string())
        );
    }

    #[test]
    fn chrom_variation_region_file_x() {
        let path = PathBuf::from("/cache/variation/X_1-1000000_var.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::Variation),
            Some("X".to_string())
        );
    }

    #[test]
    fn chrom_all_vars_in_chrom_dir() {
        let path = PathBuf::from("/cache/1/all_vars.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::Variation),
            Some("1".to_string())
        );
    }

    #[test]
    fn chrom_all_vars_in_entity_dir() {
        // Parent dir is "variation" — not a chromosome name
        let path = PathBuf::from("/cache/variation/all_vars.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::Variation),
            None
        );
    }

    #[test]
    fn chrom_motif_entity_dir_not_chromosome() {
        // Parent dir is "motif" — not a chromosome name
        let path = PathBuf::from("/cache/motif/all_vars.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::MotifFeature),
            None
        );
    }

    #[test]
    fn chrom_regfeat_entity_dir_not_chromosome() {
        let path = PathBuf::from("/cache/regfeat/chr1_regulatory.storable.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::RegulatoryFeature),
            Some("1".to_string())
        );
    }

    #[test]
    fn chrom_transcript_merged_layout() {
        let path = PathBuf::from("/cache/22/15000001-16000000.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::Transcript),
            Some("22".to_string())
        );
    }

    #[test]
    fn chrom_transcript_explicit_layout() {
        let path = PathBuf::from("/cache/transcript/chr1_transcript.storable.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::Transcript),
            Some("1".to_string())
        );
    }

    #[test]
    fn chrom_regulatory_explicit_layout() {
        let path = PathBuf::from("/cache/regulatory/chr2_regulatory.storable.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::RegulatoryFeature),
            Some("2".to_string())
        );
    }

    #[test]
    fn chrom_transcript_explicit_layout_preserves_case() {
        let path = PathBuf::from("/cache/transcript/chrX_transcript.storable.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::Transcript),
            Some("X".to_string())
        );
    }

    #[test]
    fn chrom_regulatory_explicit_layout_preserves_case() {
        let path = PathBuf::from("/cache/regulatory/chrMT_regulatory.storable.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::RegulatoryFeature),
            Some("MT".to_string())
        );
    }

    #[test]
    fn chrom_regulatory_merged_layout() {
        let path = PathBuf::from("/cache/22/15000001-16000000_reg.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::RegulatoryFeature),
            Some("22".to_string())
        );
    }

    #[test]
    fn chrom_exon_merged_layout() {
        let path = PathBuf::from("/cache/X/1-1000000.gz");
        assert_eq!(
            extract_chrom_from_path(&path, EnsemblEntityKind::Exon),
            Some("X".to_string())
        );
    }

    // -----------------------------------------------------------------------
    // extract_distinct_chroms
    // -----------------------------------------------------------------------

    #[test]
    fn distinct_chroms_variation_region_files() {
        let files = vec![
            PathBuf::from("/cache/variation/1_1-1000000_var.gz"),
            PathBuf::from("/cache/variation/1_1000001-2000000_var.gz"),
            PathBuf::from("/cache/variation/2_1-1000000_var.gz"),
            PathBuf::from("/cache/variation/X_1-1000000_var.gz"),
        ];
        let chroms = extract_distinct_chroms(&files, EnsemblEntityKind::Variation).unwrap();
        assert_eq!(chroms, vec!["1", "2", "X"]);
    }

    #[test]
    fn distinct_chroms_returns_none_when_undetermined() {
        let files = vec![
            PathBuf::from("/cache/variation/1_1-1000000_var.gz"),
            PathBuf::from("/cache/variation/all_vars.gz"), // parent is "variation"
        ];
        assert!(extract_distinct_chroms(&files, EnsemblEntityKind::Variation).is_none());
    }

    #[test]
    fn distinct_chroms_transcript_mixed() {
        let files = vec![
            PathBuf::from("/cache/1/1-1000000.gz"),
            PathBuf::from("/cache/1/1000001-2000000.gz"),
            PathBuf::from("/cache/22/1-1000000.gz"),
        ];
        let chroms = extract_distinct_chroms(&files, EnsemblEntityKind::Transcript).unwrap();
        assert_eq!(chroms, vec!["1", "22"]);
    }

    #[test]
    fn distinct_chroms_empty_files() {
        let chroms = extract_distinct_chroms(&[], EnsemblEntityKind::Variation).unwrap();
        assert!(chroms.is_empty());
    }

    // -----------------------------------------------------------------------
    // sort_variation_files_genomic
    // -----------------------------------------------------------------------

    #[test]
    fn genomic_sort_karyotypic_order() {
        let mut files: Vec<PathBuf> = vec![
            "2_1-1000000_var.gz",
            "10_1-1000000_var.gz",
            "1_1-1000000_var.gz",
            "X_1-1000000_var.gz",
            "Y_1-1000000_var.gz",
            "MT_1-1000000_var.gz",
            "22_1-1000000_var.gz",
        ]
        .into_iter()
        .map(PathBuf::from)
        .collect();

        sort_variation_files_genomic(&mut files);

        let names: Vec<&str> = files.iter().map(|p| p.to_str().unwrap()).collect();
        assert_eq!(
            names,
            vec![
                "1_1-1000000_var.gz",
                "2_1-1000000_var.gz",
                "10_1-1000000_var.gz",
                "22_1-1000000_var.gz",
                "X_1-1000000_var.gz",
                "Y_1-1000000_var.gz",
                "MT_1-1000000_var.gz",
            ]
        );
    }

    #[test]
    fn genomic_sort_within_chromosome() {
        let mut files: Vec<PathBuf> = vec![
            "1_2000001-3000000_var.gz",
            "1_1-1000000_var.gz",
            "1_1000001-2000000_var.gz",
        ]
        .into_iter()
        .map(PathBuf::from)
        .collect();

        sort_variation_files_genomic(&mut files);

        let names: Vec<&str> = files.iter().map(|p| p.to_str().unwrap()).collect();
        assert_eq!(
            names,
            vec![
                "1_1-1000000_var.gz",
                "1_1000001-2000000_var.gz",
                "1_2000001-3000000_var.gz",
            ]
        );
    }

    #[test]
    fn genomic_sort_unparseable_files_at_end() {
        let mut files: Vec<PathBuf> =
            vec!["2_1-1000000_var.gz", "all_vars.gz", "1_1-1000000_var.gz"]
                .into_iter()
                .map(PathBuf::from)
                .collect();

        sort_variation_files_genomic(&mut files);

        let names: Vec<&str> = files.iter().map(|p| p.to_str().unwrap()).collect();
        assert_eq!(
            names,
            vec!["1_1-1000000_var.gz", "2_1-1000000_var.gz", "all_vars.gz",]
        );
    }

    // -----------------------------------------------------------------------
    // parse_chrom_and_start
    // -----------------------------------------------------------------------

    #[test]
    fn parse_chrom_start_typical() {
        assert_eq!(parse_chrom_and_start("1_1-1000000_var.gz"), Some(("1", 1)));
    }

    #[test]
    fn parse_chrom_start_x() {
        assert_eq!(
            parse_chrom_and_start("X_5000001-6000000_var.gz"),
            Some(("X", 5000001))
        );
    }

    #[test]
    fn parse_chrom_start_unparseable() {
        assert_eq!(parse_chrom_and_start("all_vars.gz"), None);
    }

    // -----------------------------------------------------------------------
    // chrom_sort_key
    // -----------------------------------------------------------------------

    #[test]
    fn chrom_sort_key_canonical_order() {
        assert!(chrom_sort_key("1") < chrom_sort_key("2"));
        assert!(chrom_sort_key("9") < chrom_sort_key("10"));
        assert!(chrom_sort_key("22") < chrom_sort_key("X"));
        assert!(chrom_sort_key("X") < chrom_sort_key("Y"));
        assert!(chrom_sort_key("Y") < chrom_sort_key("MT"));
    }

    #[test]
    fn chrom_sort_key_non_canonical_after_mt() {
        assert!(chrom_sort_key("MT") < chrom_sort_key("GL000220.1"));
    }
}
