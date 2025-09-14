use datafusion::prelude::*;
use datafusion_bio_format_core::object_storage::{CompressionType, ObjectStorageOptions};
use datafusion_bio_format_fastq::table_provider::FastqTableProvider;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;

// Create a larger sample FASTQ file for performance demonstration
const LARGE_SAMPLE_FASTQ: &str = r#"@read1
ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG
+
IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII
@read2
GCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTA
+
JJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJ
@read3
TTTTAAAACCCCGGGGTTTTAAAACCCCGGGGTTTTAAAACCCCGGGGTTTTAAAACCCCGGGGTTTTAAAACCCCGGGGTTTT
+
KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK
@read4
AAAATTTTGGGGCCCCAAAATTTTGGGGCCCCAAAATTTTGGGGCCCCAAAATTTTGGGGCCCCAAAATTTTGGGGCCCCAAAA
+
LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL
@read5
CCCCGGGGAAAATTTTCCCCGGGGAAAATTTTCCCCGGGGAAAATTTTCCCCGGGGAAAATTTTCCCCGGGGAAAATTTTCCCC
+
MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
"#;

async fn create_test_file(content: &str, filename: &str) -> std::io::Result<String> {
    let file_path = format!("/tmp/{}", filename);

    // Create a larger file by repeating the content
    let mut large_content = String::new();
    for i in 0..1000 {
        // Repeat 1000 times for performance testing
        let modified_content = content.replace("@read", &format!("@read_{}", i));
        large_content.push_str(&modified_content);
    }

    fs::write(&file_path, large_content).await?;
    Ok(file_path)
}

fn create_object_storage_options() -> ObjectStorageOptions {
    ObjectStorageOptions {
        allow_anonymous: true,
        enable_request_payer: false,
        max_retries: Some(1),
        timeout: Some(300),
        chunk_size: Some(16),
        concurrent_fetches: Some(8),
        compression_type: Some(CompressionType::NONE),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧬 FASTQ Projection Pushdown Performance Demo");
    println!("==============================================\n");

    // Create test file
    println!("📁 Creating test FASTQ file with 5000 records...");
    let file_path = create_test_file(LARGE_SAMPLE_FASTQ, "large_test.fastq").await?;
    let object_storage_options = create_object_storage_options();

    let table = FastqTableProvider::new(
        file_path.clone(),
        Some(1), // Single thread for consistent timing
        Some(object_storage_options),
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("fastq_data", Arc::new(table))?;

    println!("✅ Test file created and table registered\n");

    // Test 1: Query all columns (no projection pushdown benefit)
    println!("🔍 Test 1: Querying ALL columns");
    let start = Instant::now();
    let df = ctx.sql("SELECT * FROM fastq_data LIMIT 100").await?;
    let results = df.collect().await?;
    let all_columns_time = start.elapsed();

    println!("   Columns: {}", results[0].num_columns());
    println!(
        "   Rows: {}",
        results.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    println!("   Time: {:?}\n", all_columns_time);

    // Test 2: Query only name column (should benefit from projection pushdown)
    println!("🚀 Test 2: Querying ONLY 'name' column (projection pushdown)");
    let start = Instant::now();
    let df = ctx.sql("SELECT name FROM fastq_data LIMIT 100").await?;
    let results = df.collect().await?;
    let name_only_time = start.elapsed();

    println!("   Columns: {}", results[0].num_columns());
    println!(
        "   Rows: {}",
        results.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    println!("   Time: {:?}", name_only_time);

    // Calculate speedup
    let speedup = all_columns_time.as_nanos() as f64 / name_only_time.as_nanos() as f64;
    println!("   📈 Speedup: {:.2}x faster\n", speedup);

    // Test 3: Query name and sequence (partial projection)
    println!("🔍 Test 3: Querying 'name' and 'sequence' columns");
    let start = Instant::now();
    let df = ctx
        .sql("SELECT name, sequence FROM fastq_data LIMIT 100")
        .await?;
    let results = df.collect().await?;
    let two_columns_time = start.elapsed();

    println!("   Columns: {}", results[0].num_columns());
    println!(
        "   Rows: {}",
        results.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    println!("   Time: {:?}", two_columns_time);

    let speedup = all_columns_time.as_nanos() as f64 / two_columns_time.as_nanos() as f64;
    println!("   📈 Speedup vs all columns: {:.2}x faster\n", speedup);

    // Test 4: COUNT query (should skip all field parsing)
    println!("⚡ Test 4: COUNT query (maximum optimization)");
    let start = Instant::now();
    let df = ctx.sql("SELECT COUNT(name) FROM fastq_data").await?;
    let results = df.collect().await?;
    let count_time = start.elapsed();

    println!(
        "   Count result: {}",
        results[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .value(0)
    );
    println!("   Time: {:?}", count_time);

    let speedup = all_columns_time.as_nanos() as f64 / count_time.as_nanos() as f64;
    println!("   📈 Speedup vs all columns: {:.2}x faster\n", speedup);

    // Test 5: Demonstrate projection with complex query
    println!("🧮 Test 5: Complex query with projection");
    let start = Instant::now();
    let df = ctx.sql("SELECT name, LENGTH(sequence) as seq_length FROM fastq_data WHERE LENGTH(name) > 10 LIMIT 50").await?;
    let results = df.collect().await?;
    let complex_time = start.elapsed();

    if !results.is_empty() && results[0].num_rows() > 0 {
        println!("   Columns: {}", results[0].num_columns());
        println!(
            "   Rows: {}",
            results.iter().map(|b| b.num_rows()).sum::<usize>()
        );
    }
    println!("   Time: {:?}\n", complex_time);

    // Show sample data
    println!("📊 Sample data from projection query:");
    let df = ctx
        .sql("SELECT name, sequence FROM fastq_data LIMIT 3")
        .await?;
    let results = df.collect().await?;

    if !results.is_empty() && results[0].num_rows() > 0 {
        let batch = &results[0];
        let name_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let seq_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        for i in 0..batch.num_rows().min(3) {
            println!(
                "   {} -> {} (length: {})",
                name_array.value(i),
                &seq_array.value(i)[..20.min(seq_array.value(i).len())],
                seq_array.value(i).len()
            );
        }
    } else {
        println!("   No results to display");
    }

    println!("\n🎉 Demo completed!");
    println!("💡 Key benefits of projection pushdown:");
    println!("   • Skips parsing unnecessary FASTQ fields");
    println!("   • Reduces memory allocation and copying");
    println!("   • Improves query performance, especially for selective queries");
    println!("   • Works with both single-threaded and multi-threaded readers");

    // Cleanup
    let _ = fs::remove_file(&file_path).await;

    Ok(())
}
