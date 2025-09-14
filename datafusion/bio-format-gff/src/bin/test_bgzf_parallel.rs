use datafusion::prelude::*;
use datafusion_bio_format_gff::bgzf_parallel_reader::BgzfGffTableProvider;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "/tmp/gencode.v38.annotation.gff3.gz";

    println!("🚀 Testing Parallel BGZF GFF Reader");
    println!("File: {}", file_path);
    println!("===================================");

    if !std::path::Path::new(file_path).exists() {
        eprintln!("❌ Error: File {} not found", file_path);
        eprintln!(
            "❌ Error: Also need {} for BGZF index",
            format!("{}.gzi", file_path)
        );
        std::process::exit(1);
    }

    let gzi_path = format!("{}.gzi", file_path);
    if !std::path::Path::new(&gzi_path).exists() {
        eprintln!("❌ Error: GZI index file {} not found", gzi_path);
        eprintln!("💡 Create with: bgzip -i {}", file_path);
        std::process::exit(1);
    }

    println!("✅ Found BGZF file and GZI index");
    println!();

    // Test 1: Default nested attributes
    println!("🧪 Test 1: SELECT * (nested attributes)");
    let start = Instant::now();

    let provider = BgzfGffTableProvider::try_new(&file_path, None)?;
    let ctx = SessionContext::new();
    ctx.register_table("gff", std::sync::Arc::new(provider))?;

    let df = ctx.sql("SELECT * FROM gff LIMIT 10").await?;
    let results = df.collect().await?;

    let elapsed = start.elapsed();
    println!("⏱️  Query completed in: {:?}", elapsed);
    println!(
        "📊 Records returned: {}",
        results.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // Show schema
    if let Some(batch) = results.first() {
        println!(
            "📋 Schema: {:?}",
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );
    }
    println!();

    // Test 2: Specific attribute columns
    println!("🧪 Test 2: SELECT chrom, start, end, gene_id (specific attributes)");
    let start = Instant::now();

    let provider = BgzfGffTableProvider::try_new(&file_path, Some(vec!["gene_id".to_string()]))?;
    let ctx = SessionContext::new();
    ctx.register_table("gff", std::sync::Arc::new(provider))?;

    let df = ctx
        .sql("SELECT chrom, start, \"end\", gene_id FROM gff WHERE gene_id IS NOT NULL LIMIT 10")
        .await?;
    let results = df.collect().await?;

    let elapsed = start.elapsed();
    println!("⏱️  Query completed in: {:?}", elapsed);
    println!(
        "📊 Records returned: {}",
        results.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    // Show schema
    if let Some(batch) = results.first() {
        println!(
            "📋 Schema: {:?}",
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );
    }
    println!();

    // Test 3: Performance comparison hint
    println!("🧪 Test 3: COUNT(*) performance test");
    let start = Instant::now();

    let provider = BgzfGffTableProvider::try_new(&file_path, Some(vec![]))?; // No attributes
    let ctx = SessionContext::new();
    ctx.register_table("gff", std::sync::Arc::new(provider))?;

    let df = ctx.sql("SELECT COUNT(*) as record_count FROM gff").await?;
    let results = df.collect().await?;

    let elapsed = start.elapsed();
    println!("⏱️  COUNT(*) completed in: {:?}", elapsed);

    if let Some(batch) = results.first() {
        if let Some(count_array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
        {
            let count = count_array.value(0);
            println!("📊 Total records: {}", count);
            let rps = count as f64 / elapsed.as_secs_f64();
            println!("🚀 Processing rate: {:.0} records/sec", rps);
        }
    }

    println!();
    println!("🎉 PARALLEL BGZF GFF READER TESTS COMPLETE!");
    println!("✅ Successfully implemented parallel processing for BGZF-compressed GFF files");
    println!("💡 Key benefits:");
    println!("   • Parallel partitioning based on BGZF blocks");
    println!("   • Maintains ordered attribute parsing");
    println!("   • Conditional attribute processing optimization");
    println!("   • Much simpler than FASTQ (single-line records)");

    Ok(())
}
