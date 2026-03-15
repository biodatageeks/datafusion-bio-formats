/// Validate that a variation Parquet conversion captured all records from the
/// raw VEP cache.
///
/// Usage: cargo run --release --example validate_variation_parquet -- <cache_root> <parquet_path> [--chrom CHROM]
///
/// Compares raw line counts from the VEP cache variation text files against
/// parquet row counts to surface any conversion losses.
use datafusion::arrow::array::RecordBatchReader;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion_bio_format_ensembl_cache::EnsemblCacheOptions;
use flate2::read::MultiGzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

fn count_data_lines(path: &Path) -> std::io::Result<(usize, usize)> {
    let file = File::open(path)?;
    let reader: Box<dyn BufRead> = if path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
    {
        Box::new(BufReader::with_capacity(
            64 * 1024,
            MultiGzDecoder::new(BufReader::with_capacity(64 * 1024, file)),
        ))
    } else {
        Box::new(BufReader::with_capacity(64 * 1024, file))
    };

    let mut data_lines = 0usize;
    let mut total_fields_max = 0usize;
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        data_lines += 1;
        let field_count = trimmed.split('\t').count();
        if field_count > total_fields_max {
            total_fields_max = field_count;
        }
    }
    Ok((data_lines, total_fields_max))
}

fn count_parquet_rows(parquet_path: &Path, chrom_filter: &Option<String>) -> usize {
    let file = File::open(parquet_path).expect("Failed to open parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("Invalid parquet");
    let reader = builder.build().expect("Failed to build reader");

    let chrom_idx = reader
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "chrom");

    let mut total = 0usize;
    for batch in reader {
        let batch = batch.expect("Failed reading batch");
        if let (Some(idx), Some(chrom)) = (chrom_idx, chrom_filter) {
            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("chrom column not StringArray");
            for i in 0..batch.num_rows() {
                if col.value(i) == chrom.as_str() {
                    total += 1;
                }
            }
        } else {
            total += batch.num_rows();
        }
    }
    total
}

fn main() {
    let cache_root = std::env::args()
        .nth(1)
        .expect("Usage: validate_variation_parquet <cache_root> <parquet_path> [--chrom CHROM]");
    let parquet_path = std::env::args().nth(2).expect("Missing parquet_path");

    let mut chrom_filter: Option<String> = None;
    let remaining: Vec<String> = std::env::args().skip(3).collect();
    let mut i = 0;
    while i < remaining.len() {
        if remaining[i] == "--chrom" {
            chrom_filter = remaining.get(i + 1).cloned();
            i += 2;
        } else {
            i += 1;
        }
    }

    println!("Cache root:   {cache_root}");
    println!("Parquet file: {parquet_path}");
    if let Some(ref chrom) = chrom_filter {
        println!("Chrom filter: {chrom}");
    }

    // Discover variation files
    let options = EnsemblCacheOptions::new(&cache_root);
    // Use a low-level approach: read info.txt to get variation_cols,
    // then walk the cache directory to find variation files.
    let cache_path = std::path::Path::new(&cache_root);

    // Walk cache directory for variation files
    let mut var_files: Vec<std::path::PathBuf> = Vec::new();
    walk_variation_files(cache_path, &mut var_files, &chrom_filter);
    var_files.sort();

    println!("\n=== Raw VEP Cache ===");
    println!("Variation files found: {}", var_files.len());

    let mut total_raw_lines = 0usize;
    let mut max_fields = 0usize;
    let mut per_file_counts: Vec<(String, usize, usize)> = Vec::new();

    for path in &var_files {
        match count_data_lines(path) {
            Ok((lines, fields)) => {
                total_raw_lines += lines;
                if fields > max_fields {
                    max_fields = fields;
                }
                per_file_counts.push((
                    path.file_name()
                        .and_then(|f| f.to_str())
                        .unwrap_or("?")
                        .to_string(),
                    lines,
                    fields,
                ));
            }
            Err(e) => {
                eprintln!("  WARNING: failed reading {}: {e}", path.display());
            }
        }
    }

    println!("Total data lines: {total_raw_lines}");
    println!("Max fields per line: {max_fields}");
    if max_fields > 96 {
        eprintln!(
            "  WARNING: max fields ({max_fields}) exceeds parser limit (96). \
             Fields beyond index 95 will be silently dropped!"
        );
    }

    // Read variation_cols from info.txt
    let info_path = cache_path.join("info.txt");
    if info_path.exists()
        && let Ok(content) = std::fs::read_to_string(&info_path)
    {
        for line in content.lines() {
            let lower = line.trim().to_ascii_lowercase();
            if lower.starts_with("variation_cols") {
                println!("variation_cols: {}", line.trim());
                let col_count = line
                    .split_once('=')
                    .or_else(|| line.split_once(' '))
                    .map(|(_, v)| {
                        v.replace(['[', ']', '"', '\''], "")
                            .split([',', ' ', '\t'])
                            .filter(|s| !s.trim().is_empty())
                            .count()
                    })
                    .unwrap_or(0);
                println!("Declared column count: {col_count}");
                if col_count != max_fields {
                    println!(
                        "  NOTE: declared columns ({col_count}) vs actual max fields ({max_fields}). \
                             Mismatch may indicate extra/missing tab fields."
                    );
                }
            }
        }
    }

    println!("\n=== Parquet ===");
    let parquet_rows = count_parquet_rows(Path::new(&parquet_path), &chrom_filter);
    println!("Parquet rows: {parquet_rows}");

    println!("\n=== Comparison ===");
    let diff = total_raw_lines as i64 - parquet_rows as i64;
    let pct = if total_raw_lines > 0 {
        (parquet_rows as f64 / total_raw_lines as f64) * 100.0
    } else {
        0.0
    };
    println!("Raw lines:    {total_raw_lines}");
    println!("Parquet rows: {parquet_rows}");
    println!("Difference:   {diff}");
    println!("Coverage:     {pct:.4}%");

    if diff == 0 {
        println!("\nOK: Perfect 1:1 conversion.");
    } else if diff > 0 {
        println!(
            "\nWARNING: {diff} raw lines missing from parquet. \
             Check for malformed lines or parsing errors."
        );
    } else {
        println!(
            "\nNOTE: Parquet has {} more rows than raw lines. \
             This can happen if dedup added entries or multiple files overlap.",
            -diff
        );
    }

    // Print per-file details if files are few enough
    if per_file_counts.len() <= 50 {
        println!("\n=== Per-File Details ===");
        for (name, lines, fields) in &per_file_counts {
            let flag = if *fields > 96 { " [TRUNCATED]" } else { "" };
            println!("  {name}: {lines} lines, {fields} fields{flag}");
        }
    }

    // Emit the options info just to confirm it was parseable
    drop(options);
}

fn walk_variation_files(
    root: &Path,
    out: &mut Vec<std::path::PathBuf>,
    chrom_filter: &Option<String>,
) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let path = entry.path();
        if path.is_dir() {
            walk_variation_files(&path, out, chrom_filter);
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let lower = name.to_ascii_lowercase();
        // Match all_vars or _var pattern (same as discovery.rs)
        let is_var = lower.starts_with("all_vars")
            || lower.contains("_var")
            || lower.ends_with(".var")
            || lower.ends_with(".var.gz");
        if !is_var {
            continue;
        }
        // Skip index sidecars
        if lower.ends_with(".csi") || lower.ends_with(".tbi") {
            continue;
        }
        // Apply chrom filter if set
        if let Some(chrom) = chrom_filter {
            // File named like "{chrom}_start-end_var.gz" or "all_vars_{chrom}.gz"
            let file_chrom = if lower.starts_with("all_vars") {
                // all_vars_1.gz -> "1"
                name.strip_prefix("all_vars_")
                    .and_then(|s| s.split('.').next())
            } else {
                // 1_1-1000000_var.gz -> "1"
                name.split('_').next()
            };
            if let Some(fc) = file_chrom
                && fc != chrom.as_str()
            {
                continue;
            }
        }
        out.push(path);
    }
}
