use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, get_compression_type,
};

#[tokio::main]
async fn main() {
    let file_path =
        "gs://gcp-public-data--gnomad/release/4.1/genome_sv/gnomad.v4.1.sv.sites.vcf.gz"
            .to_string();
    let options = ObjectStorageOptions::default();

    println!("Testing compression detection for: {}", file_path);
    println!("Using options: {}", options);

    match get_compression_type(file_path.clone(), None, options).await {
        Ok(CompressionType::GZIP) => println!("✅ Success! Detected GZIP compression"),
        Ok(CompressionType::BGZF) => println!("✅ Success! Detected BGZF compression"),
        Ok(CompressionType::NONE) => println!("✅ Success! No compression detected"),
        Ok(CompressionType::AUTO) => println!("❌ Unexpected AUTO returned"),
        Err(e) => println!("❌ Error: {}", e),
    }
}
