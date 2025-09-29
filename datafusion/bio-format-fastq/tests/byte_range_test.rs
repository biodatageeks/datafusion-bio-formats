use datafusion_bio_format_fastq::storage::{get_local_fastq_reader_with_range, FastqLocalReader};
use datafusion_bio_format_fastq::table_provider::FastqByteRange;
use std::io::Write;
use tempfile::NamedTempFile;

/// Create a test FASTQ file with known content for testing
fn create_test_fastq_file() -> std::io::Result<NamedTempFile> {
    let mut temp_file = NamedTempFile::new()?;

    // Create a FASTQ file with 4 records, each record is exactly 50 bytes
    // This makes it easy to test byte range splitting
    let fastq_content = concat!(
        "@read1\n",           // 7 bytes
        "ATCGATCGATCGATCG\n", // 17 bytes
        "+\n",                // 2 bytes
        "IIIIIIIIIIIIIIII\n", // 17 bytes
        // Total: 43 bytes per record

        "@read2\n",           // 7 bytes
        "GCTAGCTAGCTAGCTA\n", // 17 bytes
        "+\n",                // 2 bytes
        "HHHHHHHHHHHHHHHH\n", // 17 bytes
        // Total: 43 bytes per record

        "@read3\n",           // 7 bytes
        "TTTTAAAACCCCGGGG\n", // 17 bytes
        "+\n",                // 2 bytes
        "JJJJJJJJJJJJJJJJ\n", // 17 bytes
        // Total: 43 bytes per record

        "@read4\n",           // 7 bytes
        "AAAATTTTCCCCGGGG\n", // 17 bytes
        "+\n",                // 2 bytes
        "KKKKKKKKKKKKKKKK\n", // 17 bytes
        // Total: 43 bytes per record
    );

    temp_file.write_all(fastq_content.as_bytes())?;
    temp_file.flush()?;
    Ok(temp_file)
}

#[test]
fn test_full_file_reading() -> std::io::Result<()> {
    let temp_file = create_test_fastq_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Read entire file
    let byte_range = FastqByteRange { start: 0, end: 1000 }; // Large end range
    let mut reader = get_local_fastq_reader_with_range(file_path, byte_range)?;

    let mut record_count = 0;
    for record_result in reader.records() {
        let _record = record_result?;
        record_count += 1;
    }

    assert_eq!(record_count, 4, "Should read all 4 FASTQ records");
    Ok(())
}

#[test]
fn test_byte_range_first_half() -> std::io::Result<()> {
    let temp_file = create_test_fastq_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Read first ~half of the file (86 bytes covers first 2 records)
    let byte_range = FastqByteRange { start: 0, end: 86 };
    let mut reader = get_local_fastq_reader_with_range(file_path, byte_range)?;

    let mut record_count = 0;
    let mut record_names = Vec::new();

    for record_result in reader.records() {
        let record = record_result?;
        record_count += 1;
        record_names.push(String::from_utf8_lossy(record.name()).to_string());
    }

    assert_eq!(record_count, 2, "Should read first 2 FASTQ records");
    assert_eq!(record_names, vec!["read1", "read2"]);
    Ok(())
}

#[test]
fn test_byte_range_second_half() -> std::io::Result<()> {
    let temp_file = create_test_fastq_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Start from middle of file - should find next record boundary
    let byte_range = FastqByteRange { start: 50, end: 200 }; // Start mid-file
    let mut reader = get_local_fastq_reader_with_range(file_path, byte_range)?;

    let mut record_count = 0;
    let mut record_names = Vec::new();

    for record_result in reader.records() {
        let record = record_result?;
        record_count += 1;
        record_names.push(String::from_utf8_lossy(record.name()).to_string());
    }

    // Should start reading from next record boundary (read2 or later)
    assert!(record_count >= 1, "Should read at least 1 record");
    assert!(record_count <= 3, "Should not read more than 3 records");

    // First record should be read2, read3, or read4 (not read1)
    assert_ne!(record_names[0], "read1", "Should skip partial read1");
    Ok(())
}

#[test]
fn test_byte_range_middle_split() -> std::io::Result<()> {
    let temp_file = create_test_fastq_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Create byte range that starts and ends in middle of records
    let byte_range = FastqByteRange { start: 20, end: 100 };
    let mut reader = get_local_fastq_reader_with_range(file_path, byte_range)?;

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

    // Should find record boundary and read complete records
    assert!(record_count >= 1, "Should read at least 1 complete record");
    assert!(record_count <= 3, "Should respect byte range limits");
    Ok(())
}

#[test]
fn test_byte_range_end_boundary() -> std::io::Result<()> {
    let temp_file = create_test_fastq_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Very small byte range - should read at most 1 record (each record is ~43 bytes)
    let byte_range = FastqByteRange { start: 0, end: 50 };
    let mut reader = get_local_fastq_reader_with_range(file_path, byte_range)?;

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

    assert!(record_count >= 1, "Should read at least 1 record within 50-byte limit");
    assert!(record_count <= 2, "Should not read more than 2 records within 50-byte limit");
    Ok(())
}

#[test]
fn test_zero_byte_range() -> std::io::Result<()> {
    let temp_file = create_test_fastq_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Zero-size range
    let byte_range = FastqByteRange { start: 10, end: 10 };
    let mut reader = get_local_fastq_reader_with_range(file_path, byte_range)?;

    let mut record_count = 0;

    for record_result in reader.records() {
        let _record = record_result?;
        record_count += 1;
    }

    assert_eq!(record_count, 0, "Zero-byte range should read no records");
    Ok(())
}

#[test]
fn test_byte_range_at_record_boundary() -> std::io::Result<()> {
    let temp_file = create_test_fastq_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Start near beginning of read3 (around byte 80-90)
    let byte_range = FastqByteRange { start: 80, end: 200 };
    let mut reader = get_local_fastq_reader_with_range(file_path, byte_range)?;

    let mut record_count = 0;
    let mut record_names = Vec::new();

    for record_result in reader.records() {
        match record_result {
            Ok(record) => {
                record_count += 1;
                record_names.push(String::from_utf8_lossy(record.name()).to_string());
            }
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

    assert!(record_count >= 1, "Should read at least 1 record");
    assert!(record_count <= 2, "Should read at most 2 records within byte range");
    // The first record should be read3 or later (not read1 or read2)
    if !record_names.is_empty() {
        assert!(record_names[0] == "read3" || record_names[0] == "read4",
                "Should read read3 or read4, got: {}", record_names[0]);
    }
    Ok(())
}

#[tokio::test]
async fn test_integration_with_fastq_local_reader() -> std::io::Result<()> {
    use datafusion_bio_format_core::object_storage::ObjectStorageOptions;

    let temp_file = create_test_fastq_file()?;
    let file_path = temp_file.path().to_string_lossy().to_string();

    let byte_range = Some(FastqByteRange { start: 0, end: 86 });
    let storage_opts = ObjectStorageOptions::default();

    let mut reader = FastqLocalReader::new_with_range(
        file_path,
        1, // single thread
        byte_range,
        storage_opts
    ).await?;

    let mut record_count = 0;
    let mut record_stream = reader.read_records().await;

    use futures_util::StreamExt;
    while let Some(record_result) = record_stream.next().await {
        let _record = record_result?;
        record_count += 1;
    }

    assert_eq!(record_count, 2, "Integration test should read 2 records");
    Ok(())
}

#[test]
fn test_invalid_byte_range() {
    let temp_file = create_test_fastq_file().unwrap();
    let file_path = temp_file.path().to_string_lossy().to_string();

    // Invalid range where start > end
    let byte_range = FastqByteRange { start: 100, end: 50 };
    let result = get_local_fastq_reader_with_range(file_path, byte_range);

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