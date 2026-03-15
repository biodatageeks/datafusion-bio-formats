use crate::errors::{Result, exec_err};
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
}
