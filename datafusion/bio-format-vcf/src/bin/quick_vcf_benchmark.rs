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

    println!("🧬 Quick VCF Parallel BGZF Benchmark");
    println!("📁 File: {}", vcf_file);

    let thread_counts = vec![1, 2, 4];
    let mut results = Vec::new();

    for &thread_count in &thread_counts {
        println!("\n🔄 Testing {} thread(s)...", thread_count);

        let config = SessionConfig::new().with_target_partitions(thread_count);
        let ctx = SessionContext::new_with_config(config);

        let table_provider = BgzfVcfTableProvider::try_new(vcf_file)?;
        ctx.register_table("vcf_data", Arc::new(table_provider))?;

        // Quick count test with limit
        let start = Instant::now();
        let df = ctx
            .sql("SELECT COUNT(*) as count FROM vcf_data LIMIT 50000")
            .await?;
        let result = df.collect().await?;
        let duration = start.elapsed();

        let count = if let Some(batch) = result.first() {
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

        results.push((thread_count, count, duration));

        println!("   📊 Records: {}", count);
        println!("   ⏱️  Time: {:?}", duration);
        println!(
            "   🔥 Records/sec: {:.0}",
            count as f64 / duration.as_secs_f64()
        );
    }

    // Performance analysis
    println!("\n📊 QUICK BENCHMARK RESULTS");
    println!("===========================");

    let baseline = &results[0];

    println!("Threads | Records | Time     | Records/sec | Speedup");
    println!("--------|---------|----------|-------------|--------");

    for (thread_count, count, time) in &results {
        let records_per_sec = *count as f64 / time.as_secs_f64();
        let speedup = baseline.2.as_secs_f64() / time.as_secs_f64();

        println!(
            "{:7} | {:7} | {:8?} | {:11.0} | {:6.2}x",
            thread_count, count, time, records_per_sec, speedup
        );
    }

    // Row count verification
    println!("\n🔍 ROW COUNT VERIFICATION");
    println!("=========================");

    let baseline_count = results[0].1;
    let mut all_consistent = true;

    for (thread_count, count, _) in &results {
        let matches = *count == baseline_count;
        println!(
            "{} threads: {} records {}",
            thread_count,
            count,
            if matches { "✅" } else { "❌" }
        );
        if !matches {
            all_consistent = false;
        }
    }

    if all_consistent {
        println!("✅ All configurations return consistent row counts!");
    } else {
        println!("❌ Row count inconsistency detected!");
    }

    // Calculate parallel efficiency
    if results.len() >= 2 {
        let single = &results[0];
        let multi = results.last().unwrap();

        let actual_speedup = single.2.as_secs_f64() / multi.2.as_secs_f64();
        let max_threads = multi.0 as f64;
        let efficiency = (actual_speedup / max_threads) * 100.0;

        println!("\n⚡ PARALLEL EFFICIENCY");
        println!("=====================");
        println!("Actual speedup: {:.2}x", actual_speedup);
        println!("Parallel efficiency: {:.1}%", efficiency);

        if efficiency > 75.0 {
            println!("🎯 Excellent parallel scaling!");
        } else if efficiency > 50.0 {
            println!("👍 Good parallel scaling");
        } else {
            println!("⚠️  Limited parallel benefit");
        }
    }

    println!("\n🎉 Quick benchmark completed!");

    Ok(())
}
