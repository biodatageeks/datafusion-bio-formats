/// Convert Ensembl VEP cache transcripts to Parquet with memory monitoring.
///
/// Usage: cargo run --release --example transcript_to_parquet -- <cache_root> <output_dir> [partitions]
///
/// Uses streaming Parquet writers to avoid buffering all data in memory.
/// Each partition writes directly to a separate Parquet file as batches arrive.
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::execute_stream_partitioned;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_ensembl_cache::{EnsemblCacheOptions, TranscriptTableProvider};
use futures::StreamExt;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

fn rss_mb() -> f64 {
    #[cfg(target_os = "macos")]
    {
        use std::mem::MaybeUninit;
        unsafe extern "C" {
            fn mach_task_self() -> u32;
            fn task_info(
                target_task: u32,
                flavor: u32,
                task_info_out: *mut LibcTaskBasicInfo,
                task_info_cnt: *mut u32,
            ) -> i32;
        }
        #[repr(C)]
        struct LibcTaskBasicInfo {
            suspend_count: i32,
            virtual_size: u64,
            resident_size: u64,
            user_time: [u32; 2],
            system_time: [u32; 2],
            policy: i32,
        }
        const MACH_TASK_BASIC_INFO: u32 = 20;
        const MACH_TASK_BASIC_INFO_COUNT: u32 =
            (std::mem::size_of::<LibcTaskBasicInfo>() / std::mem::size_of::<u32>()) as u32;
        unsafe {
            let mut info = MaybeUninit::<LibcTaskBasicInfo>::uninit();
            let mut count = MACH_TASK_BASIC_INFO_COUNT;
            let kr = task_info(
                mach_task_self(),
                MACH_TASK_BASIC_INFO,
                info.as_mut_ptr(),
                &mut count,
            );
            if kr == 0 {
                return info.assume_init().resident_size as f64 / (1024.0 * 1024.0);
            }
        }
    }
    0.0
}

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let cache_root = std::env::args()
        .nth(1)
        .expect("Usage: transcript_to_parquet <cache_root> <output_dir> [partitions]");
    let output_dir = std::env::args()
        .nth(2)
        .expect("Usage: transcript_to_parquet <cache_root> <output_dir> [partitions]");
    let partitions: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);

    println!("Cache root:  {cache_root}");
    println!("Output dir:  {output_dir}");
    println!("Partitions:  {partitions}");
    println!("Initial RSS: {:.1} MB", rss_mb());

    let config = SessionConfig::new().with_target_partitions(partitions);
    let ctx = SessionContext::new_with_config(config);

    let mut options = EnsemblCacheOptions::new(&cache_root);
    options.target_partitions = Some(partitions);
    let provider = TranscriptTableProvider::new(options)?;
    ctx.register_table("tx", Arc::new(provider))?;

    println!("After provider init RSS: {:.1} MB", rss_mb());

    // Create output directory (clean first)
    let output_path = std::path::Path::new(&output_dir);
    if output_path.exists() {
        std::fs::remove_dir_all(output_path)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
    }
    std::fs::create_dir_all(output_path)
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;

    println!("Writing to:  {output_dir}/");

    // Start memory monitoring
    let monitor_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let monitor_flag_clone = monitor_flag.clone();
    let monitor_handle = std::thread::spawn(move || {
        let mut peak = 0.0f64;
        while monitor_flag_clone.load(std::sync::atomic::Ordering::Relaxed) {
            let rss = rss_mb();
            if rss > peak {
                peak = rss;
                println!("  [monitor] RSS: {rss:.1} MB (new peak)");
            }
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        peak
    });

    let start = Instant::now();

    // Get physical plan and execute partitioned streams
    let df = ctx.sql("SELECT * FROM tx ORDER BY chrom, start").await?;
    let plan = df.create_physical_plan().await?;
    let schema = plan.schema();
    let task_ctx = ctx.task_ctx();
    let streams = execute_stream_partitioned(plan, task_ctx)?;
    let num_partitions = streams.len();
    println!("Actual partitions: {num_partitions}");

    // Parquet writer properties
    let writer_props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(100_000)
        .build();

    // Process each partition stream sequentially (to keep memory bounded),
    // writing each to a separate Parquet file.
    let mut total_rows: usize = 0;
    let mut total_batches: usize = 0;

    for (partition_idx, mut stream) in streams.into_iter().enumerate() {
        let file_path = format!("{output_dir}/part-{partition_idx:04}.parquet");
        let file = File::create(&file_path)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props.clone()))?;

        let mut partition_rows = 0usize;
        let mut partition_batches = 0usize;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            partition_rows += batch.num_rows();
            partition_batches += 1;
            writer.write(&batch)?;
        }

        writer.close()?;
        total_rows += partition_rows;
        total_batches += partition_batches;

        let file_size = std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0);
        println!(
            "  Partition {partition_idx}: {partition_rows} rows, {partition_batches} batches, {:.1} MB | RSS: {:.1} MB",
            file_size as f64 / 1024.0 / 1024.0,
            rss_mb()
        );
    }

    let elapsed = start.elapsed();

    // Stop monitor
    monitor_flag.store(false, std::sync::atomic::Ordering::Relaxed);
    let peak_rss = monitor_handle.join().unwrap_or(0.0);

    // Sum Parquet file sizes
    let total_bytes: u64 = std::fs::read_dir(&output_dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();

    println!();
    println!("=== Summary ===");
    println!("Rows written:   {total_rows}");
    println!(
        "Parquet files:  {num_partitions} ({:.1} MB total)",
        total_bytes as f64 / 1024.0 / 1024.0
    );
    println!("Batches:        {total_batches}");
    println!("Elapsed:        {elapsed:.2?}");
    println!("Final RSS:      {:.1} MB", rss_mb());
    println!("Peak RSS:       {peak_rss:.1} MB");
    if elapsed.as_secs_f64() > 0.0 {
        println!(
            "Throughput:     {:.0} rows/sec",
            total_rows as f64 / elapsed.as_secs_f64()
        );
    }

    Ok(())
}
