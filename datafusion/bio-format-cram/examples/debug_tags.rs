/// Diagnostic tool to understand what tags are present in CRAM records
///
/// This example reads a CRAM file and dumps all available tags from the first few records
/// to help debug why MD and NM tags are not accessible through record.data().get()
use noodles_cram as cram;
use noodles_fasta as fasta;
use noodles_sam::alignment::Record;
use std::fs::File;
use std::io::{self, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // You need to provide your own test file paths
    let cram_file = std::env::args()
        .nth(1)
        .expect("Usage: debug_tags <cram_file> [reference_fasta]");

    let reference_path = std::env::args().nth(2);

    println!("Opening CRAM file: {}", cram_file);

    // Build reader with reference if provided
    let mut reader = if let Some(ref_path) = reference_path {
        println!("Using reference: {}", ref_path);

        // Load FASTA reference
        let fasta_file = File::open(&ref_path)?;
        let fasta_reader = BufReader::new(fasta_file);

        // Load index
        let index_path = format!("{}.fai", ref_path);
        let index_file = File::open(&index_path)?;
        let mut index_reader = fasta::fai::io::Reader::new(BufReader::new(index_file));
        let index = index_reader.read_index()?;

        // Create indexed reader and repository
        let indexed_reader = fasta::io::IndexedReader::new(fasta_reader, index);
        let adapter = fasta::repository::adapters::IndexedReader::new(indexed_reader);
        let repository = fasta::Repository::new(adapter);

        // Build CRAM reader with repository
        cram::io::reader::Builder::default()
            .set_reference_sequence_repository(repository)
            .build_from_path(&cram_file)?
    } else {
        println!("No reference provided - using embedded reference");
        let default_repo = fasta::Repository::default();
        cram::io::reader::Builder::default()
            .set_reference_sequence_repository(default_repo)
            .build_from_path(&cram_file)?
    };

    let header = reader.read_header()?;
    println!("\n=== CRAM Header ===");
    println!(
        "Reference sequences: {}",
        header.reference_sequences().len()
    );

    println!("\n=== Processing Records ===");

    let mut record_count = 0;
    let max_records = 5;

    for result in reader.records(&header) {
        let record = result?;
        record_count += 1;

        println!("\n--- Record #{} ---", record_count);

        // Print basic record info
        if let Some(name) = record.name() {
            println!("Read name: {}", name);
        }

        // Print all tags by iterating through data()
        println!("\nTags available via record.data():");
        let data = record.data();
        let mut tag_count = 0;

        // Try to iterate all tags
        for item in data.values() {
            match item {
                Ok((tag, value)) => {
                    tag_count += 1;
                    println!("  - Tag: {:?}, Value type: {:?}", tag, value);
                }
                Err(e) => {
                    println!("  - Error reading tag: {}", e);
                }
            }
        }

        println!("Total tags found: {}", tag_count);

        // Specifically try to get MD and NM
        println!("\nSpecific tag queries:");
        let md_tag = noodles_sam::alignment::record::data::field::Tag::try_from([b'M', b'D'])?;
        let nm_tag = noodles_sam::alignment::record::data::field::Tag::try_from([b'N', b'M'])?;

        match data.get(&md_tag) {
            Some(Ok(value)) => println!("  MD tag: {:?}", value),
            Some(Err(e)) => println!("  MD tag error: {}", e),
            None => println!("  MD tag: NOT FOUND"),
        }

        match data.get(&nm_tag) {
            Some(Ok(value)) => println!("  NM tag: {:?}", value),
            Some(Err(e)) => println!("  NM tag error: {}", e),
            None => println!("  NM tag: NOT FOUND"),
        }

        if record_count >= max_records {
            break;
        }
    }

    println!("\n=== Summary ===");
    println!("Processed {} records", record_count);
    println!("\nThis diagnostic shows what tags are actually accessible via record.data().");
    println!("If MD and NM are missing, they may need to be calculated from CRAM features.");

    Ok(())
}
