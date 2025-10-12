use std::io::BufReader;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let vcf_file = "/tmp/homo_sapiens-chr1.vcf.bgz";
    let gzi_file = format!("{}.gzi", vcf_file);

    println!("Testing VCF file reading...");
    println!("VCF file: {}", vcf_file);
    println!("GZI file: {}", gzi_file);

    // Test 1: Basic file reading with regular VCF reader
    println!("\n1. Testing basic VCF file reading:");
    match std::fs::File::open(vcf_file) {
        Ok(file) => {
            let reader = BufReader::new(file);
            let mut vcf_reader = noodles_vcf::io::Reader::new(reader);

            match vcf_reader.read_header() {
                Ok(header) => {
                    println!("✅ Header read successfully");
                    println!("   INFO fields: {}", header.infos().len());

                    // Try reading first few records
                    let mut count = 0;
                    for result in vcf_reader.records() {
                        match result {
                            Ok(_record) => {
                                count += 1;
                                if count >= 5 {
                                    break;
                                }
                            }
                            Err(e) => {
                                println!("❌ Error reading record {}: {}", count + 1, e);
                                break;
                            }
                        }
                    }
                    println!("✅ Successfully read {} records", count);
                }
                Err(e) => println!("❌ Failed to read header: {}", e),
            }
        }
        Err(e) => println!("❌ Failed to open VCF file: {}", e),
    }

    // Test 2: GZI index reading
    println!("\n2. Testing GZI index reading:");
    match noodles_bgzf::gzi::fs::read(&gzi_file) {
        Ok(index) => {
            println!("✅ GZI index read successfully");
            println!("   Index entries: {}", index.as_ref().len());

            // Show first few entries
            for (i, (compressed, uncompressed)) in index.as_ref().iter().take(5).enumerate() {
                println!(
                    "   Entry {}: compressed={}, uncompressed={}",
                    i, compressed, uncompressed
                );
            }
        }
        Err(e) => println!("❌ Failed to read GZI index: {}", e),
    }

    // Test 3: Indexed reading
    println!("\n3. Testing indexed BGZF reading:");
    match std::fs::File::open(vcf_file) {
        Ok(file) => {
            match noodles_bgzf::gzi::fs::read(&gzi_file) {
                Ok(index) => {
                    let reader = BufReader::new(file);
                    let indexed_reader = noodles_bgzf::IndexedReader::new(reader, index);

                    let mut vcf_reader = noodles_vcf::io::Reader::new(indexed_reader);

                    match vcf_reader.read_header() {
                        Ok(_header) => {
                            println!("✅ Indexed header read successfully");

                            // Try reading first few records
                            let mut count = 0;
                            for result in vcf_reader.records() {
                                match result {
                                    Ok(_record) => {
                                        count += 1;
                                        if count >= 5 {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        println!(
                                            "❌ Error reading indexed record {}: {}",
                                            count + 1,
                                            e
                                        );
                                        break;
                                    }
                                }
                            }
                            println!("✅ Successfully read {} indexed records", count);
                        }
                        Err(e) => println!("❌ Failed to read indexed header: {}", e),
                    }
                }
                Err(e) => println!("❌ Failed to read index for indexed reading: {}", e),
            }
        }
        Err(e) => println!("❌ Failed to open file for indexed reading: {}", e),
    }

    // Test 4: Try creating our parallel table provider
    println!("\n4. Testing BgzfVcfTableProvider creation:");
    match datafusion_bio_format_vcf::bgzf_parallel_reader::BgzfVcfTableProvider::try_new(vcf_file) {
        Ok(_provider) => println!("✅ BgzfVcfTableProvider created successfully"),
        Err(e) => println!("❌ Failed to create BgzfVcfTableProvider: {}", e),
    }

    Ok(())
}
