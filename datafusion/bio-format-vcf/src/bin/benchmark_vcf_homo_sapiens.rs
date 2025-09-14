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
        eprintln!("💡 Create it with: bgzip -r {}", vcf_file);
        return Ok(());
    }

    println!("🧬 VCF Parallel BGZF Benchmark");
    println!("📁 File: {}", vcf_file);
    println!("📊 Index: {}", gzi_file);

    // Get file size for context
    let metadata = std::fs::metadata(vcf_file)?;
    let file_size_mb = metadata.len() as f64 / (1024.0 * 1024.0);
    println!("💾 File size: {:.2} MB", file_size_mb);
    println!();

    let thread_counts = vec![1, 2, 4];
    let mut results = Vec::new();

    for &thread_count in &thread_counts {
        println!("🔄 Running benchmark with {} thread(s)...", thread_count);

        // Create session with specified thread count
        let config = SessionConfig::new().with_target_partitions(thread_count);
        let ctx = SessionContext::new_with_config(config);

        // Create parallel BGZF VCF table provider
        let table_provider = BgzfVcfTableProvider::try_new(vcf_file)?;
        ctx.register_table("vcf_data", Arc::new(table_provider))?;

        // Benchmark 1: COUNT(*) query
        let start = Instant::now();
        let df = ctx
            .sql("SELECT COUNT(*) as total_records FROM vcf_data")
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

        // Benchmark 2: Projection query
        let start = Instant::now();
        let df = ctx
            .sql("SELECT chrom, start, end, ref, alt FROM vcf_data LIMIT 100000")
            .await?;
        let proj_result = df.collect().await?;
        let projection_duration = start.elapsed();
        let projection_rows = proj_result
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();

        // Benchmark 3: Aggregation query
        let start = Instant::now();
        let df = ctx.sql("SELECT chrom, COUNT(*) as variant_count FROM vcf_data GROUP BY chrom ORDER BY chrom").await?;
        let agg_result = df.collect().await?;
        let aggregation_duration = start.elapsed();

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

        println!("   📊 Total records: {}", record_count);
        println!("   🎯 Projection rows: {}", projection_rows);
        println!("   ⏱️  COUNT(*) time: {:?}", count_duration);
        println!("   🎯 Projection time: {:?}", projection_duration);
        println!("   📈 Aggregation time: {:?}", aggregation_duration);
        println!("   🧬 Chromosomes found: {}", chromosome_count);
        println!();
    }

    // Performance analysis
    println!("📊 PERFORMANCE ANALYSIS");
    println!("========================");

    let baseline = &results[0]; // 1 thread

    println!("Thread Count | COUNT(*)     | Projection   | Aggregation  | Records/sec  | Speedup");
    println!(
        "-------------|--------------|--------------|--------------|--------------|----------"
    );

    for (thread_count, record_count, count_time, proj_time, agg_time, _, _) in &results {
        let records_per_sec = *record_count as f64 / count_time.as_secs_f64();
        let speedup = baseline.2.as_secs_f64() / count_time.as_secs_f64();

        println!(
            "{:12} | {:12?} | {:12?} | {:12?} | {:12.0} | {:8.2}x",
            thread_count, count_time, proj_time, agg_time, records_per_sec, speedup
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
            "{} threads: Total count ✓{}, Projection ✓{}, Chrom counts ✓{}",
            thread_count,
            if total_count_match { "✅" } else { "❌" },
            if projection_count_match { "✅" } else { "❌" },
            if chrom_counts_match { "✅" } else { "❌" }
        );

        if !total_count_match || !projection_count_match || !chrom_counts_match {
            all_consistent = false;

            if !total_count_match {
                println!(
                    "  ❌ Total count mismatch: {} vs baseline {}",
                    record_count, baseline_record_count
                );
            }
            if !projection_count_match {
                println!(
                    "  ❌ Projection rows mismatch: {} vs baseline {}",
                    projection_rows, baseline_projection_rows
                );
            }
            if !chrom_counts_match {
                println!("  ❌ Chromosome counts differ from baseline");
            }
        }
    }

    if all_consistent {
        println!("✅ All thread configurations return identical results!");
    } else {
        println!("❌ Data inconsistency detected across thread configurations!");
    }

    println!();

    // Calculate efficiency metrics
    if results.len() >= 2 {
        let single_thread_time = results[0].2.as_secs_f64();
        let multi_thread_time = results.last().unwrap().2.as_secs_f64();
        let max_threads = results.last().unwrap().0;

        let theoretical_speedup = max_threads as f64;
        let actual_speedup = single_thread_time / multi_thread_time;
        let efficiency = (actual_speedup / theoretical_speedup) * 100.0;

        println!("🎯 EFFICIENCY METRICS");
        println!("====================");
        println!("Theoretical max speedup: {:.2}x", theoretical_speedup);
        println!("Actual speedup: {:.2}x", actual_speedup);
        println!("Parallel efficiency: {:.1}%", efficiency);

        if efficiency > 75.0 {
            println!("✅ Excellent parallel scaling!");
        } else if efficiency > 50.0 {
            println!("👍 Good parallel scaling");
        } else {
            println!("⚠️  Limited parallel scaling - may be I/O bound");
        }
    }

    println!();
    println!("📊 THROUGHPUT ANALYSIS");
    println!("======================");

    for (thread_count, record_count, count_time, _, _, _, _) in &results {
        let mb_per_sec = file_size_mb / count_time.as_secs_f64();
        let records_per_sec = *record_count as f64 / count_time.as_secs_f64();

        println!(
            "{} thread(s): {:.1} MB/s, {:.0} records/s",
            thread_count, mb_per_sec, records_per_sec
        );
    }

    println!();
    println!("🎉 Benchmark completed successfully!");

    Ok(())
}
