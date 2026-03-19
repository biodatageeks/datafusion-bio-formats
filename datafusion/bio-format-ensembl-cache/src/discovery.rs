use crate::entity::EnsemblEntityKind;
use crate::errors::{Result, exec_err};
use crate::physical_exec::parse_file_chrom_region;
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

    region_files.sort();
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

/// Well-known entity directory names that are NOT chromosome names.
const ENTITY_DIR_NAMES: &[&str] = &["variation", "transcript", "regulatory"];

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
    let lower = name.to_ascii_lowercase();

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
    let stem = extract_chrom_prefix_from_explicit_name(&lower)?;
    Some(stem.to_string())
}

/// Extracts chrom from explicit-layout filenames like `chr1_transcript.storable.gz`.
/// Returns the chromosome portion (e.g. `"1"` from `"chr1_transcript.storable.gz"`).
fn extract_chrom_prefix_from_explicit_name(lower_name: &str) -> Option<&str> {
    // Must start with "chr"
    let rest = lower_name.strip_prefix("chr")?;
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
}
