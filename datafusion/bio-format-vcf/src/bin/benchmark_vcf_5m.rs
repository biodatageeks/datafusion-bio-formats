use datafusion::prelude::*;
use datafusion_bio_format_vcf::bgzf_parallel_reader::BgzfVcfTableProvider;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let vcf_file = "/tmp/homo_sapiens-chr1.vcf.bgz";
    let gzi_file = format!("{}.gzi", vcf_file);

    // Check if files exist
    if !std::path::Path::new(vcf_file).exists() {
        eprintln!("❌ VCF file not found: {}", vcf_file);
        return Ok(());
    }

    if !std::path::Path::new(&gzi_file).exists() {
        eprintln!("❌ GZI index not found: {}", gzi_file);
        return Ok(());
    }

    println!("🧬 VCF Parallel BGZF Benchmark (Limited to 5M records)");
    println!("📁 File: {}", vcf_file);

    let metadata = std::fs::metadata(vcf_file)?;
    let file_size_mb = metadata.len() as f64 / (1024.0 * 1024.0);
    println!("💾 File size: {:.2} MB", file_size_mb);
    println!("🎯 Processing limit: 5,000,000 records");
    println!();

    let thread_counts = vec![1, 2, 4];
    let mut results = Vec::new();

    for &thread_count in &thread_counts {
        println!("🔄 Running benchmark with {} thread(s)...", thread_count);

        if let Err(e) = async {
            // Create session with specified thread count
            let config = SessionConfig::new()
                .with_target_partitions(thread_count)
                .with_batch_size(8192); // Larger batch size for better performance
            let ctx = SessionContext::new_with_config(config);

            // Create parallel BGZF VCF table provider
            let table_provider = BgzfVcfTableProvider::try_new(vcf_file)?;
            ctx.register_table("vcf_data", Arc::new(table_provider))?;

            // Benchmark 1: COUNT with 5M limit (using subquery)
            let start = Instant::now();
            let df = ctx
                .sql(
                    "SELECT COUNT(*) as total_records \
                     FROM (SELECT * FROM vcf_data LIMIT 5000000)",
                )
                .await?;
            let result = df.collect().await?;
            let count_duration = start.elapsed();

            let record_count = if let Some(batch) = result.first() {
                if let Some(array) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                {
                    array.value(0)
                } else {
                    0
                }
            } else {
                0
            };

            // Benchmark 2: Projection query with 5M limit
            let start = Instant::now();
            let df = ctx
                .sql("SELECT chrom, start, ref, alt FROM vcf_data LIMIT 5000000")
                .await?;
            let proj_result = df.collect().await?;
            let projection_duration = start.elapsed();
            let projection_rows = proj_result
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>();

            // Benchmark 3: Aggregation query with 5M limit
            let start = Instant::now();
            let df = ctx
                .sql(
                    "SELECT chrom, COUNT(*) as variant_count \
                     FROM (SELECT * FROM vcf_data LIMIT 5000000) \
                     GROUP BY chrom \
                     ORDER BY chrom",
                )
                .await?;
            let agg_result = df.collect().await?;
            let aggregation_duration = start.elapsed();

            // Count chromosomes
            let chromosome_count = agg_result
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>();

            // Get chromosome-level counts for verification
            let mut chrom_counts = Vec::new();
            for batch in &agg_result {
                let chrom_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .unwrap();
                let count_array = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap();

                for i in 0..batch.num_rows() {
                    chrom_counts.push((chrom_array.value(i).to_string(), count_array.value(i)));
                }
            }

            results.push((
                thread_count,
                record_count,
                count_duration,
                projection_duration,
                aggregation_duration,
                projection_rows,
                chrom_counts,
            ));

            println!("   📊 Records processed: {}", record_count);
            println!("   🎯 Projection rows: {}", projection_rows);
            println!("   ⏱️  COUNT time: {:?}", count_duration);
            println!("   🔍 Projection time: {:?}", projection_duration);
            println!("   📈 Aggregation time: {:?}", aggregation_duration);
            println!("   🧬 Chromosomes found: {}", chromosome_count);

            // Show throughput
            let records_per_sec = record_count as f64 / count_duration.as_secs_f64();
            let mb_per_sec =
                (record_count as f64 / 5000000.0) * file_size_mb / count_duration.as_secs_f64();
            println!(
                "   🚀 Throughput: {:.0} records/sec, {:.1} MB/sec",
                records_per_sec, mb_per_sec
            );
            println!();

            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await
        {
            eprintln!(
                "Error during benchmark with {} thread(s): {}",
                thread_count, e
            );
            continue;
        }
    }

    // Performance analysis
    println!("📊 PERFORMANCE ANALYSIS (5M records)");
    println!("====================================");

    let baseline = &results[0]; // 1 thread

    println!("Threads | Records   | COUNT      | Projection | Aggregation | Records/sec | Speedup");
    println!("--------|-----------|------------|------------|-------------|-------------|--------");

    for (thread_count, record_count, count_time, proj_time, agg_time, _, _) in &results {
        let records_per_sec = *record_count as f64 / count_time.as_secs_f64();
        let speedup = baseline.2.as_secs_f64() / count_time.as_secs_f64();

        println!(
            "{:7} | {:9} | {:10?} | {:10?} | {:11?} | {:11.0} | {:7.2}x",
            thread_count, record_count, count_time, proj_time, agg_time, records_per_sec, speedup
        );
    }

    // Data integrity verification
    println!();
    println!("🔍 DATA INTEGRITY VERIFICATION");
    println!("==============================");

    let baseline_record_count = results[0].1;
    let baseline_projection_rows = results[0].5;
    let baseline_chrom_counts = &results[0].6;

    let mut all_consistent = true;

    for (thread_count, record_count, _, _, _, projection_rows, chrom_counts) in &results[1..] {
        let total_count_match = *record_count == baseline_record_count;
        let projection_count_match = *projection_rows == baseline_projection_rows;
        let chrom_counts_match = chrom_counts == baseline_chrom_counts;

        println!(
            "{} threads: Records ✅{}, Projection ✅{}, Chromosomes ✅{}",
            thread_count,
            if total_count_match { "✓" } else { "✗" },
            if projection_count_match { "✓" } else { "✗" },
            if chrom_counts_match { "✓" } else { "✗" }
        );

        if !total_count_match || !projection_count_match || !chrom_counts_match {
            all_consistent = false;
        }
    }

    if all_consistent {
        println!("✅ All thread configurations return identical results!");
    } else {
        println!("❌ Data inconsistency detected!");
    }

    // Calculate efficiency metrics
    if results.len() >= 2 {
        let single_thread_time = results[0].2.as_secs_f64();
        let multi_thread_time = results.last().unwrap().2.as_secs_f64();
        let max_threads = results.last().unwrap().0;

        let theoretical_speedup = max_threads as f64;
        let actual_speedup = single_thread_time / multi_thread_time;
        let efficiency = (actual_speedup / theoretical_speedup) * 100.0;

        println!();
        println!("⚡ PARALLEL EFFICIENCY ANALYSIS");
        println!("===============================");
        println!("Theoretical max speedup: {:.2}x", theoretical_speedup);
        println!("Actual speedup: {:.2}x", actual_speedup);
        println!("Parallel efficiency: {:.1}%", efficiency);

        if efficiency > 75.0 {
            println!("🎯 Excellent parallel scaling!");
        } else if efficiency > 50.0 {
            println!("👍 Good parallel scaling");
        } else if efficiency > 25.0 {
            println!("⚠️  Moderate parallel scaling");
        } else {
            println!("❌ Poor parallel scaling - likely overhead-bound");
        }
    }

    // Show chromosome distribution from first thread (for verification)
    if let Some((_, _, _, _, _, _, chrom_counts)) = results.first() {
        println!();
        println!("🧬 CHROMOSOME DISTRIBUTION (first 10)");
        println!("====================================");
        for (i, (chrom, count)) in chrom_counts.iter().take(10).enumerate() {
            println!("{}. Chromosome {}: {} variants", i + 1, chrom, count);
        }
        if chrom_counts.len() > 10 {
            println!("... and {} more chromosomes", chrom_counts.len() - 10);
        }
    }

    println!();
    println!("🎉 5M record benchmark completed successfully!");

    Ok(())
}
