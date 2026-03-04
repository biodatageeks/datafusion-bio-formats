//! Benchmark: multisample VCF read → filter → VCF/Parquet write
//!
//! Equivalent bcftools pipeline:
//! ```bash
//! bcftools view --samples-file samples_rand_2000.txt input.vcf.gz \
//!   | bcftools filter --exclude 'QUAL<20 || AVG(FORMAT/GQ)<15 || AVG(FORMAT/DP)<15 || AVG(FORMAT/DP)>150' \
//!   | bcftools filter --exclude 'FORMAT/GQ<10 | FORMAT/DP<10 | FORMAT/DP>200' --set-GTs '.' \
//!   -o final_piped.vcf.gz --write-index=tbi
//! ```
//!
//! DataFusion SQL equivalent (single query):
//! ```sql
//! SELECT chrom, start, "end", id, ref, alt, qual, filter,
//!   named_struct(
//!     'GT', vcf_set_gts(genotypes.GT,
//!       list_and(list_and(list_gte(genotypes.GQ, 10), list_gte(genotypes.DP, 10)),
//!               list_lte(genotypes.DP, 200))),
//!     'GQ', genotypes.GQ,
//!     'DP', genotypes.DP
//!   ) AS genotypes
//! FROM vcf_table
//! WHERE qual >= 20
//!   AND list_avg(genotypes.GQ) >= 15
//!   AND list_avg(genotypes.DP) >= 15
//!   AND list_avg(genotypes.DP) <= 150
//! ```

use std::sync::Arc;
use std::time::Instant;

use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::*;
use datafusion_bio_format_core::metadata::{VCF_SAMPLE_NAMES_KEY, from_json_string};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use datafusion_bio_format_vcf::udfs::register_vcf_udfs;

const DEFAULT_VCF: &str = "/Users/mwiewior/research/data/polars-bio/20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr_12_22_X.recalibrated_variants.exome.vcf.gz";
const DEFAULT_SAMPLES: &str = "/Users/mwiewior/research/data/polars-bio/samples_rand_2000.txt";

/// SQL that matches the full bcftools pipeline:
/// 1. Sample subsetting (handled by VcfTableProvider::new_with_samples)
/// 2. Site-level filter: exclude QUAL<20 || AVG(GQ)<15 || AVG(DP)<15 || AVG(DP)>150
/// 3. Sample-level GT masking: set GT='.' where GQ<10 | DP<10 | DP>200
const FILTER_SQL: &str = "\
SELECT chrom, start, \"end\", id, ref, alt, qual, filter, \
  named_struct(\
    'GT', vcf_set_gts(genotypes.\"GT\", \
      list_and(list_and(list_gte(genotypes.\"GQ\", 10), list_gte(genotypes.\"DP\", 10)), \
               list_lte(genotypes.\"DP\", 200))), \
    'GQ', genotypes.\"GQ\", \
    'DP', genotypes.\"DP\"\
  ) AS genotypes \
FROM vcf_table \
WHERE qual >= 20 \
  AND list_avg(genotypes.\"GQ\") >= 15.0 \
  AND list_avg(genotypes.\"DP\") >= 15.0 \
  AND list_avg(genotypes.\"DP\") <= 150.0";

struct Args {
    file: String,
    samples_file: String,
    output_dir: String,
    batch_size: usize,
    target_partitions: usize,
    skip_read: bool,
    skip_parquet: bool,
    skip_vcf: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut file = None::<String>;
    let mut samples_file = None::<String>;
    let mut output_dir = None::<String>;
    let mut batch_size = 8192usize;
    let mut target_partitions = 4usize;
    let mut skip_read = false;
    let mut skip_parquet = false;
    let mut skip_vcf = false;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--file" => file = Some(it.next().ok_or("--file requires a value")?),
            "--samples-file" => {
                samples_file = Some(it.next().ok_or("--samples-file requires a value")?)
            }
            "--output-dir" => output_dir = Some(it.next().ok_or("--output-dir requires a value")?),
            "--batch-size" => {
                let v = it.next().ok_or("--batch-size requires a value")?;
                batch_size = v
                    .parse()
                    .map_err(|e| format!("invalid --batch-size '{v}': {e}"))?;
            }
            "--target-partitions" => {
                let v = it.next().ok_or("--target-partitions requires a value")?;
                target_partitions = v
                    .parse()
                    .map_err(|e| format!("invalid --target-partitions '{v}': {e}"))?;
            }
            "--skip-read" => skip_read = true,
            "--skip-parquet" => skip_parquet = true,
            "--skip-vcf" => skip_vcf = true,
            "--help" | "-h" => {
                return Err(
                    "usage: vcf_multisample_bench [--file <path>] [--samples-file <path>] \
                     [--output-dir <path>] [--batch-size N] [--target-partitions N] \
                     [--skip-read] [--skip-parquet] [--skip-vcf]"
                        .to_string(),
                );
            }
            other => return Err(format!("unknown argument '{other}'")),
        }
    }

    Ok(Args {
        file: file.unwrap_or_else(|| DEFAULT_VCF.to_string()),
        samples_file: samples_file.unwrap_or_else(|| DEFAULT_SAMPLES.to_string()),
        output_dir: output_dir.unwrap_or_else(|| "/tmp/vcf_bench_output".to_string()),
        batch_size,
        target_partitions,
        skip_read,
        skip_parquet,
        skip_vcf,
    })
}

fn load_sample_names(path: &str) -> Result<Vec<String>, String> {
    let contents =
        std::fs::read_to_string(path).map_err(|e| format!("failed to read '{path}': {e}"))?;
    Ok(contents
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect())
}

fn make_provider(
    file: &str,
    samples: Option<Vec<String>>,
) -> datafusion::common::Result<VcfTableProvider> {
    VcfTableProvider::new_with_samples(
        file.to_string(),
        Some(vec![]),
        Some(vec!["GT".into(), "GQ".into(), "DP".into()]),
        samples,
        Some(ObjectStorageOptions::default()),
        true,
    )
}

fn make_session(config: &SessionConfig) -> SessionContext {
    let ctx = SessionContext::new_with_config(config.clone());
    register_vcf_udfs(&ctx);
    ctx
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args =
        parse_args().map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    std::fs::create_dir_all(&args.output_dir)?;

    let samples = load_sample_names(&args.samples_file)?;
    let num_samples = samples.len();

    let config = SessionConfig::new()
        .with_batch_size(args.batch_size)
        .with_target_partitions(args.target_partitions);

    eprintln!("=== VCF Multisample Benchmark ===");
    eprintln!("file        = {}", args.file);
    eprintln!("samples     = {num_samples}");
    eprintln!("batch_size  = {}", args.batch_size);
    eprintln!("partitions  = {}", args.target_partitions);
    eprintln!("output_dir  = {}", args.output_dir);
    eprintln!();

    // ── Phase 1: Read + filter (materialize all batches) ─────────────────
    if !args.skip_read {
        let provider = make_provider(&args.file, Some(samples.clone()))?;
        let ctx = make_session(&config);
        ctx.register_table("vcf_table", Arc::new(provider))?;

        eprintln!("[read+filter] sql = {FILTER_SQL}");

        let start = Instant::now();
        let df = ctx.sql(FILTER_SQL).await?;
        let batches = df.collect().await?;
        let elapsed = start.elapsed();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        eprintln!(
            "[read+filter] rows={total_rows}  batches={}  elapsed={:.3}s  rows/sec={:.0}",
            batches.len(),
            elapsed.as_secs_f64(),
            total_rows as f64 / elapsed.as_secs_f64()
        );
        eprintln!();
    }

    // ── Phase 2: Read + filter → Parquet write ───────────────────────────
    if !args.skip_parquet {
        let provider = make_provider(&args.file, Some(samples.clone()))?;
        let ctx = make_session(&config);
        ctx.register_table("vcf_table", Arc::new(provider))?;

        let parquet_path = format!("{}/output.parquet", args.output_dir);
        eprintln!("[parquet] output = {parquet_path}");

        let start = Instant::now();
        let df = ctx.sql(FILTER_SQL).await?;
        let batches = df.collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let read_elapsed = start.elapsed();

        let write_start = Instant::now();
        let mem_table =
            datafusion::datasource::MemTable::try_new(batches[0].schema(), vec![batches])?;
        let ctx2 = make_session(&config);
        ctx2.register_table("mem_data", Arc::new(mem_table))?;
        ctx2.sql(&format!(
            "COPY (SELECT * FROM mem_data) TO '{parquet_path}' STORED AS PARQUET"
        ))
        .await?
        .collect()
        .await?;
        let write_elapsed = write_start.elapsed();
        let total_elapsed = start.elapsed();

        eprintln!(
            "[parquet] rows={total_rows}  read+filter={:.3}s  write={:.3}s  total={:.3}s",
            read_elapsed.as_secs_f64(),
            write_elapsed.as_secs_f64(),
            total_elapsed.as_secs_f64()
        );
        eprintln!();
    }

    // ── Phase 3: Read + filter → VCF write ──────────────────────────────
    if !args.skip_vcf {
        let source_provider = make_provider(&args.file, Some(samples.clone()))?;
        let source_schema = source_provider.schema();
        let source_meta = source_schema.metadata().clone();

        let sample_names_from_schema = source_schema
            .metadata()
            .get(VCF_SAMPLE_NAMES_KEY)
            .and_then(|raw| from_json_string::<Vec<String>>(raw))
            .unwrap_or_default();

        let format_fields = vec!["GT".into(), "GQ".into(), "DP".into()];
        let info_fields: Vec<String> = vec![];

        let ctx = make_session(&config);
        ctx.register_table("vcf_table", Arc::new(source_provider))?;

        let vcf_path = format!("{}/output.vcf", args.output_dir);
        eprintln!("[vcf] output = {vcf_path}");

        // Build the filtered query as a view, then INSERT into VCF dest
        ctx.sql(&format!("CREATE VIEW vcf_filtered AS {FILTER_SQL}"))
            .await?;

        let dest_provider = VcfTableProvider::new_for_write_with_source_metadata(
            vcf_path.clone(),
            source_schema.clone(),
            info_fields,
            format_fields,
            sample_names_from_schema,
            true,
            source_meta,
        );
        ctx.register_table("vcf_dest", Arc::new(dest_provider))?;

        let insert_sql = "INSERT OVERWRITE vcf_dest SELECT * FROM vcf_filtered";
        eprintln!("[vcf] sql = INSERT OVERWRITE ... SELECT <filtered> FROM vcf_table");

        let start = Instant::now();
        ctx.sql(insert_sql).await?.collect().await?;
        let elapsed = start.elapsed();

        let line_count: usize = std::fs::read_to_string(&vcf_path)?
            .lines()
            .filter(|l| !l.starts_with('#'))
            .count();

        eprintln!(
            "[vcf] rows={line_count}  total={:.3}s",
            elapsed.as_secs_f64()
        );
        eprintln!();
    }

    eprintln!("=== Done ===");
    Ok(())
}
