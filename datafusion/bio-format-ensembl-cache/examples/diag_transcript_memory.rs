/// Diagnostic: measure RSS memory growth during transcript table scanning.
///
/// Usage: cargo run --release --example diag_transcript_memory -- /path/to/vep/cache [partitions]
///
/// Streams all rows from the transcript table and reports RSS at regular
/// intervals to identify memory growth patterns.
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_ensembl_cache::{EnsemblCacheOptions, TranscriptTableProvider};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Instant;

fn rss_mb() -> f64 {
    // macOS: use mach API for accurate self-RSS
    #[cfg(target_os = "macos")]
    {
        use std::mem::MaybeUninit;
        unsafe extern "C" {
            fn mach_task_self() -> u32;
            fn task_info(
                target_task: u32,
                flavor: u32,
                task_info_out: *mut libc_task_basic_info,
                task_info_cnt: *mut u32,
            ) -> i32;
        }
        #[repr(C)]
        struct libc_task_basic_info {
            suspend_count: i32,
            virtual_size: u64,
            resident_size: u64,
            user_time: [u32; 2],
            system_time: [u32; 2],
            policy: i32,
        }
        const MACH_TASK_BASIC_INFO: u32 = 20;
        const MACH_TASK_BASIC_INFO_COUNT: u32 =
            (std::mem::size_of::<libc_task_basic_info>() / std::mem::size_of::<u32>()) as u32;
        unsafe {
            let mut info = MaybeUninit::<libc_task_basic_info>::uninit();
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
    // Fallback: ps
    let pid = std::process::id();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map(|kb| kb / 1024.0)
        .unwrap_or(0.0)
}

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let cache_root = std::env::args().nth(1).unwrap_or_else(|| {
        "/Users/mwiewior/research/data/vep/homo_sapiens_merged/115_GRCh38".into()
    });
    let partitions: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    println!("Cache: {cache_root}");
    println!("Partitions: {partitions}");
    println!("Initial RSS: {:.1} MB", rss_mb());

    let config = SessionConfig::new().with_target_partitions(partitions);
    let ctx = SessionContext::new_with_config(config);

    let query = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "SELECT chrom, start, \"end\", stable_id FROM tx".to_string());
    let max_storable: Option<usize> = std::env::args().nth(4).and_then(|s| s.parse().ok());

    let mut options = EnsemblCacheOptions::new(&cache_root);
    options.target_partitions = Some(partitions);
    if let Some(cap) = max_storable {
        options.max_storable_partitions = Some(cap);
    }
    let provider = TranscriptTableProvider::new(options)?;
    ctx.register_table("tx", Arc::new(provider))?;

    println!("After provider init RSS: {:.1} MB", rss_mb());
    println!("Max storable partitions: {:?}", max_storable.unwrap_or(4));
    println!("Query: {}", &query);

    let start = Instant::now();
    let df = ctx.sql(&query).await?;
    let mut stream = df.execute_stream().await?;

    let mut total_rows: usize = 0;
    let mut total_batches: usize = 0;
    let mut peak_rss: f64 = 0.0;
    let report_interval = 50_000usize;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        total_rows += batch.num_rows();
        total_batches += 1;

        if total_rows % report_interval < batch.num_rows() || total_rows < report_interval {
            let rss = rss_mb();
            peak_rss = peak_rss.max(rss);
            let elapsed = start.elapsed();
            println!(
                "  rows={:<8} batches={:<5} RSS={:.1} MB  peak={:.1} MB  elapsed={:.2?}",
                total_rows, total_batches, rss, peak_rss, elapsed
            );
        }
    }
    let elapsed = start.elapsed();
    let rss = rss_mb();
    peak_rss = peak_rss.max(rss);

    println!();
    println!("=== Summary ===");
    println!("Total rows:    {total_rows}");
    println!("Total batches: {total_batches}");
    println!("Elapsed:       {elapsed:.2?}");
    println!("Final RSS:     {:.1} MB", rss);
    println!("Peak RSS:      {:.1} MB", peak_rss);
    if elapsed.as_secs_f64() > 0.0 {
        let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
        println!("Throughput:    {rows_per_sec:.0} rows/sec");
    }

    Ok(())
}
