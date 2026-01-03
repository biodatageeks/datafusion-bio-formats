use datafusion_bio_format_vcf::storage::{VcfLocalReader, get_local_vcf_reader_with_range};
use datafusion_bio_format_vcf::table_provider::VcfByteRange;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use std::io::Write;
use tempfile::NamedTempFile;

/// Create a test VCF file with known content for testing
fn create_test_vcf_file() -> std::io::Result<NamedTempFile> {
    let mut temp_file = NamedTempFile::new()?;

    // Create a VCF file with header and 4 variant records
    // VCF format: header lines start with ## or #, variant lines don't
    let vcf_content = concat!(
        "##fileformat=VCFv4.3\n",
        "##contig=<ID=chr1,length=249250621>\n",
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n",
        "chr1\t1000\t.\tA\tT\t100\tPASS\t.\n",      // Variant 1
        "chr1\t2000\t.\tG\tC\t200\tPASS\t.\n",      // Variant 2
        "chr1\t3000\t.\tT\tA\t300\tPASS\t.\n",      // Variant 3
        "chr1\t4000\t.\tC\tG\t400\tPASS\t.\n",      // Variant 4
    );

    temp_file.write_all(vcf_content.as_bytes())?;
    temp_file.flush()?;
    Ok(temp_file)
}

#[test]
fn test_full_file_reading() -> std::io::Result<()> {
    let temp_file = create_test_vcf_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Read entire file
    let byte_range = VcfByteRange {
        start: 0,
        end: 1000,
    }; // Large end range
    let mut reader = get_local_vcf_reader_with_range(file_path, byte_range)?;

    let mut record_count = 0;
    for record_result in reader.records() {
        let _record = record_result?;
        record_count += 1;
    }

    assert_eq!(record_count, 4, "Should read all 4 VCF variant records");
    Ok(())
}

#[test]
fn test_byte_range_first_half() -> std::io::Result<()> {
    let temp_file = create_test_vcf_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Read first part of the file (after header, first 2 variants)
    // Header is ~100 bytes, each variant line is ~40 bytes
    // So bytes 100-180 should cover first 2 variants
    // Note: VCF readers read complete lines, so we may get more records
    let byte_range = VcfByteRange { start: 100, end: 180 };
    let mut reader = get_local_vcf_reader_with_range(file_path, byte_range)?;

    let mut record_count = 0;
    let mut chroms = Vec::new();

    for record_result in reader.records() {
        let record = record_result?;
        record_count += 1;
        chroms.push(record.reference_sequence_name().to_string());
    }

    assert!(record_count >= 1, "Should read at least 1 VCF variant record");
    // VCF readers read complete lines, so we may get more than 2 if lines extend past the range
    assert!(record_count <= 4, "Should not read more than all records");
    Ok(())
}

#[test]
fn test_byte_range_skip_header() -> std::io::Result<()> {
    let temp_file = create_test_vcf_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Start from middle of file - should skip header and find next variant
    let byte_range = VcfByteRange {
        start: 150,
        end: 250,
    };
    let mut reader = get_local_vcf_reader_with_range(file_path, byte_range)?;

    let mut record_count = 0;

    for record_result in reader.records() {
        match record_result {
            Ok(_record) => record_count += 1,
            Err(e) => {
                // If we hit EOF due to byte limit, that's expected
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(e);
                }
            }
        }
    }

    // Should find variant line and read complete records
    assert!(record_count >= 1, "Should read at least 1 complete record");
    assert!(record_count <= 3, "Should respect byte range limits");
    Ok(())
}

#[test]
fn test_zero_byte_range() -> std::io::Result<()> {
    let temp_file = create_test_vcf_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Zero-size range at end of file (should read no records)
    // Get file size first
    let file_size = std::fs::metadata(&file_path)?.len();
    let byte_range = VcfByteRange {
        start: file_size,
        end: file_size,
    };
    let mut reader = get_local_vcf_reader_with_range(file_path, byte_range)?;

    let mut record_count = 0;

    for record_result in reader.records() {
        let _record = record_result?;
        record_count += 1;
    }

    assert_eq!(record_count, 0, "Zero-byte range at end of file should read no records");
    Ok(())
}

#[test]
fn test_invalid_byte_range() {
    let temp_file = create_test_vcf_file().unwrap();
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Invalid range where start > end
    let byte_range = VcfByteRange {
        start: 100,
        end: 50,
    };
    let result = get_local_vcf_reader_with_range(file_path, byte_range);

    // Should handle gracefully (implementation detail - might return empty or error)
    match result {
        Ok(mut reader) => {
            let record_count = reader.records().count();
            assert_eq!(record_count, 0, "Invalid range should produce no records");
        }
        Err(_) => {
            // Also acceptable to return an error for invalid ranges
        }
    }
}

#[tokio::test]
async fn test_integration_with_vcf_local_reader() -> std::io::Result<()> {
    let temp_file = create_test_vcf_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    let byte_range = Some(VcfByteRange { start: 100, end: 180 });
    let storage_opts = ObjectStorageOptions::default();

    let mut reader = VcfLocalReader::new_with_range(
        file_path,
        1, // single thread
        byte_range,
        storage_opts,
    )
    .await?;

    let mut record_count = 0;
    let mut record_stream = reader.read_records();

    use futures::StreamExt;
    while let Some(record_result) = record_stream.next().await {
        let _record = record_result?;
        record_count += 1;
    }

    assert!(record_count >= 1, "Integration test should read at least 1 record");
    Ok(())
}

