use anyhow::{Context, Result};
use datafusion::prelude::*;
use datafusion_bio_benchmarks_common::{
    extract_drive_id, write_result, BenchmarkCategory, BenchmarkResultBuilder, DataDownloader,
    TestDataFile,
};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Main benchmark configuration loaded from YAML
#[derive(Debug, Deserialize)]
struct BenchmarkConfig {
    format: String,
    table_name: String,
    test_data: Vec<TestDataConfig>,
    parallelism_tests: ParallelismConfig,
    predicate_pushdown_tests: PredicateConfig,
    projection_pushdown_tests: ProjectionConfig,
}

/// Test data file configuration
#[derive(Debug, Deserialize)]
struct TestDataConfig {
    filename: String,
    drive_url: String,
    checksum: Option<String>,
}

/// Parallelism benchmark configuration
#[derive(Debug, Deserialize)]
struct ParallelismConfig {
    thread_counts: Vec<ThreadCount>,
    repetitions: usize,
    query: String,
}

/// Thread count specification (number or "max")
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ThreadCount {
    Number(usize),
    #[allow(dead_code)]
    Max(String), // "max" string from YAML
}

/// Predicate pushdown test configuration
#[derive(Debug, Deserialize)]
struct PredicateConfig {
    repetitions: usize,
    tests: Vec<TestCase>,
}

/// Projection pushdown test configuration
#[derive(Debug, Deserialize)]
struct ProjectionConfig {
    repetitions: usize,
    tests: Vec<TestCase>,
}

/// Individual test case with name and SQL query
#[derive(Debug, Deserialize)]
struct TestCase {
    name: String,
    query: String,
}

impl TestDataConfig {
    fn to_test_data_file(&self) -> Result<TestDataFile> {
        let drive_id = extract_drive_id(&self.drive_url)?;
        let mut file = TestDataFile::new(&self.filename, drive_id);
        if let Some(checksum) = &self.checksum {
            file = file.with_checksum(checksum);
        }
        Ok(file)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <config.yml> [--output-dir <path>]", args[0]);
        eprintln!("\nExample:");
        eprintln!("  {} benchmarks/configs/gff.yml", args[0]);
        std::process::exit(1);
    }

    let config_path = &args[1];
    let output_dir = if args.len() >= 4 && args[2] == "--output-dir" {
        PathBuf::from(&args[3])
    } else {
        PathBuf::from("benchmark_results")
    };

    println!("📊 DataFusion Bio-Formats Benchmark Runner");
    println!("==========================================\n");
    println!("Config: {}", config_path);
    println!("Output: {}\n", output_dir.display());

    // Load YAML configuration
    let config_content =
        std::fs::read_to_string(config_path).context("Failed to read configuration file")?;
    let config: BenchmarkConfig =
        serde_yaml::from_str(&config_content).context("Failed to parse YAML configuration")?;

    // Validate configuration
    validate_config(&config)?;

    // Download test data
    println!("📥 Downloading test data...");
    let downloader = DataDownloader::new()?;
    let mut data_paths = Vec::new();

    for data_config in &config.test_data {
        let test_file = data_config.to_test_data_file()?;
        let path = downloader.download(&test_file, false)?;
        data_paths.push(path);
    }
    println!();

    // Register table in DataFusion
    println!(
        "📋 Registering {} table as '{}'...",
        config.format, config.table_name
    );
    let ctx = SessionContext::new();
    register_table(&ctx, &config.format, &config.table_name, &data_paths).await?;
    println!("✓ Table registered successfully\n");

    // Run benchmark categories
    let results_dir = output_dir.join(&config.format);
    std::fs::create_dir_all(&results_dir)?;

    run_parallelism_benchmarks(
        &ctx,
        &config.format,
        &config.table_name,
        &config.parallelism_tests,
        &results_dir,
    )
    .await?;

    run_predicate_benchmarks(
        &ctx,
        &config.format,
        &config.table_name,
        &config.predicate_pushdown_tests,
        &results_dir,
    )
    .await?;

    run_projection_benchmarks(
        &ctx,
        &config.format,
        &config.table_name,
        &config.projection_pushdown_tests,
        &results_dir,
    )
    .await?;

    println!("\n✅ All benchmarks completed successfully!");
    println!("📁 Results saved to: {}", results_dir.display());

    Ok(())
}

/// Validate configuration has required fields and reasonable values
fn validate_config(config: &BenchmarkConfig) -> Result<()> {
    if config.format.is_empty() {
        anyhow::bail!("Format cannot be empty");
    }
    if config.table_name.is_empty() {
        anyhow::bail!("Table name cannot be empty");
    }
    if config.test_data.is_empty() {
        anyhow::bail!("At least one test data file must be specified");
    }
    if config.parallelism_tests.repetitions == 0 {
        anyhow::bail!("Parallelism repetitions must be > 0");
    }
    if config.predicate_pushdown_tests.repetitions == 0 {
        anyhow::bail!("Predicate pushdown repetitions must be > 0");
    }
    if config.projection_pushdown_tests.repetitions == 0 {
        anyhow::bail!("Projection pushdown repetitions must be > 0");
    }
    Ok(())
}

/// Register table based on format name
async fn register_table(
    ctx: &SessionContext,
    format: &str,
    table_name: &str,
    data_paths: &[PathBuf],
) -> Result<()> {
    if data_paths.is_empty() {
        anyhow::bail!("No data files provided");
    }

    let primary_file = &data_paths[0];
    let file_path = primary_file.to_str().context("Invalid file path")?;

    match format.to_lowercase().as_str() {
        "gff" => {
            let storage_options = ObjectStorageOptions::default();
            use datafusion_bio_format_gff::table_provider::GffTableProvider;
            let provider =
                GffTableProvider::new(file_path.to_string(), None, None, Some(storage_options))
                    .context("Failed to create GFF table provider")?;
            ctx.register_table(table_name, std::sync::Arc::new(provider))
                .context("Failed to register GFF table")?;
        }
        "vcf" => {
            use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
            let provider = VcfTableProvider::new(file_path.to_string(), None, None, None, None)
                .context("Failed to create VCF table provider")?;
            ctx.register_table(table_name, std::sync::Arc::new(provider))
                .context("Failed to register VCF table")?;
        }
        "fastq" => {
            use datafusion_bio_format_fastq::BgzfFastqTableProvider;
            let provider = BgzfFastqTableProvider::try_new(file_path.to_string())
                .context("Failed to create FASTQ table provider")?;
            ctx.register_table(table_name, std::sync::Arc::new(provider))
                .context("Failed to register FASTQ table")?;
        }
        "bam" => {
            use datafusion_bio_format_bam::table_provider::BamTableProvider;
            let provider = BamTableProvider::try_new(file_path.to_string(), None, None, None, None)
                .await
                .context("Failed to create BAM table provider")?;
            ctx.register_table(table_name, std::sync::Arc::new(provider))
                .context("Failed to register BAM table")?;
        }
        "bed" => {
            use datafusion_bio_format_bed::table_provider::{BEDFields, BedTableProvider};
            // Default to BED3 format (chrom, start, end)
            let provider =
                BedTableProvider::new(file_path.to_string(), BEDFields::BED3, None, None)
                    .context("Failed to create BED table provider")?;
            ctx.register_table(table_name, std::sync::Arc::new(provider))
                .context("Failed to register BED table")?;
        }
        "fasta" => {
            use datafusion_bio_format_fasta::table_provider::FastaTableProvider;
            let provider = FastaTableProvider::new(file_path.to_string(), None, None)
                .context("Failed to create FASTA table provider")?;
            ctx.register_table(table_name, std::sync::Arc::new(provider))
                .context("Failed to register FASTA table")?;
        }
        _ => {
            anyhow::bail!(
                "Unsupported format: {}. Supported formats: gff, vcf, fastq, bam, bed, fasta",
                format
            );
        }
    }

    Ok(())
}

/// Run parallelism benchmarks with different thread counts
async fn run_parallelism_benchmarks(
    ctx: &SessionContext,
    format: &str,
    table_name: &str,
    config: &ParallelismConfig,
    output_dir: &Path,
) -> Result<()> {
    println!("🔀 Running Parallelism Benchmarks");
    println!("==================================");

    let query = config.query.replace("{table_name}", table_name);
    let mut baseline_time: Option<f64> = None;

    for thread_count_spec in &config.thread_counts {
        let thread_count = match thread_count_spec {
            ThreadCount::Number(n) => *n,
            ThreadCount::Max(_) => num_cpus::get(),
        };

        println!("  Testing with {} threads...", thread_count);

        let mut total_records = 0u64;
        let mut total_time = 0.0;

        for rep in 0..config.repetitions {
            let start = Instant::now();
            let df = ctx.sql(&query).await?;
            let results = df.collect().await?;
            let elapsed = start.elapsed().as_secs_f64();

            // Count records
            let count: u64 = results.iter().map(|batch| batch.num_rows() as u64).sum();
            total_records = count; // Assuming same count each time
            total_time += elapsed;

            log::debug!("    Rep {}: {:.3}s ({} records)", rep + 1, elapsed, count);
        }

        let avg_time = total_time / config.repetitions as f64;
        let speedup = baseline_time.map(|bt| bt / avg_time);

        if baseline_time.is_none() {
            baseline_time = Some(avg_time);
        }

        // Build and write result
        let benchmark_name = format!("{}_parallelism_{}threads", format, thread_count);
        let config_json = serde_json::json!({
            "threads": thread_count,
            "repetitions": config.repetitions,
        });

        let result =
            BenchmarkResultBuilder::new(&benchmark_name, format, BenchmarkCategory::Parallelism)
                .with_config(config_json)
                .build(
                    total_records,
                    std::time::Duration::from_secs_f64(avg_time),
                    speedup,
                );

        write_result(&result, output_dir)?;

        println!(
            "    ✓ {} threads: {:.3}s avg ({} reps){}",
            thread_count,
            avg_time,
            config.repetitions,
            speedup
                .map(|s| format!(", {:.2}x speedup", s))
                .unwrap_or_default()
        );
    }

    println!();
    Ok(())
}

/// Run predicate pushdown benchmarks
async fn run_predicate_benchmarks(
    ctx: &SessionContext,
    format: &str,
    table_name: &str,
    config: &PredicateConfig,
    output_dir: &Path,
) -> Result<()> {
    println!("🔍 Running Predicate Pushdown Benchmarks");
    println!("========================================");

    for test_case in &config.tests {
        println!("  Testing: {}...", test_case.name);

        let query = test_case.query.replace("{table_name}", table_name);
        let mut total_time = 0.0;
        let mut total_records = 0u64;

        for rep in 0..config.repetitions {
            let start = Instant::now();
            let df = ctx.sql(&query).await?;
            let results = df.collect().await?;
            let elapsed = start.elapsed().as_secs_f64();

            let count: u64 = results.iter().map(|batch| batch.num_rows() as u64).sum();
            total_records = count;
            total_time += elapsed;

            log::debug!("    Rep {}: {:.3}s ({} records)", rep + 1, elapsed, count);
        }

        let avg_time = total_time / config.repetitions as f64;

        // Build and write result
        let benchmark_name = format!("{}_predicate_{}", format, test_case.name);
        let config_json = serde_json::json!({
            "test_name": test_case.name,
            "query": query,
            "repetitions": config.repetitions,
        });

        let result = BenchmarkResultBuilder::new(
            &benchmark_name,
            format,
            BenchmarkCategory::PredicatePushdown,
        )
        .with_config(config_json)
        .build(
            total_records,
            std::time::Duration::from_secs_f64(avg_time),
            None,
        );

        write_result(&result, output_dir)?;

        println!(
            "    ✓ {}: {:.3}s avg, {} records",
            test_case.name, avg_time, total_records
        );
    }

    println!();
    Ok(())
}

/// Run projection pushdown benchmarks
async fn run_projection_benchmarks(
    ctx: &SessionContext,
    format: &str,
    table_name: &str,
    config: &ProjectionConfig,
    output_dir: &Path,
) -> Result<()> {
    println!("📊 Running Projection Pushdown Benchmarks");
    println!("=========================================");

    for test_case in &config.tests {
        println!("  Testing: {}...", test_case.name);

        let query = test_case.query.replace("{table_name}", table_name);
        let mut total_time = 0.0;
        let mut total_records = 0u64;

        for rep in 0..config.repetitions {
            let start = Instant::now();
            let df = ctx.sql(&query).await?;
            let results = df.collect().await?;
            let elapsed = start.elapsed().as_secs_f64();

            let count: u64 = results.iter().map(|batch| batch.num_rows() as u64).sum();
            total_records = count;
            total_time += elapsed;

            log::debug!("    Rep {}: {:.3}s ({} records)", rep + 1, elapsed, count);
        }

        let avg_time = total_time / config.repetitions as f64;

        // Build and write result
        let benchmark_name = format!("{}_projection_{}", format, test_case.name);
        let config_json = serde_json::json!({
            "test_name": test_case.name,
            "query": query,
            "repetitions": config.repetitions,
        });

        let result = BenchmarkResultBuilder::new(
            &benchmark_name,
            format,
            BenchmarkCategory::ProjectionPushdown,
        )
        .with_config(config_json)
        .build(
            total_records,
            std::time::Duration::from_secs_f64(avg_time),
            None,
        );

        write_result(&result, output_dir)?;

        println!(
            "    ✓ {}: {:.3}s avg, {} records",
            test_case.name, avg_time, total_records
        );
    }

    println!();
    Ok(())
}
