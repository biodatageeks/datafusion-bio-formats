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
                .is_some_and(|name| name.starts_with("all_vars"))
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
                    lower.contains("_var")
                        || name.ends_with(".var")
                        || name.ends_with(".var.gz")
                        || lower.contains("var.gz")
                        || (looks_like_region_file_name(&lower)
                            && !lower.ends_with("_reg.gz")
                            && !lower.contains("transcript"))
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
            name_patterns.iter().any(|pattern| name.contains(pattern))
        })
        .collect();

    result.sort();
    Ok(result)
}

fn walk_files(root: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let mut stack = vec![root.to_path_buf()];

    while let Some(path) = stack.pop() {
        if path.is_file() {
            out.push(path);
            continue;
        }

        if !path.exists() {
            continue;
        }

        let entries = fs::read_dir(&path)
            .map_err(|e| exec_err(format!("Failed listing {}: {}", path.display(), e)))?;
        for entry in entries {
            let entry = entry.map_err(|e| exec_err(format!("Failed reading dir entry: {}", e)))?;
            let child = entry.path();
            if child.is_dir() {
                stack.push(child);
            } else if child.is_file() {
                out.push(child);
            }
        }
    }

    Ok(())
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
