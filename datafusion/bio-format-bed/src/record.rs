//! Shared framing and validation for synchronous and asynchronous BED readers.

use std::io;

pub(crate) fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

pub(crate) fn line_error(line_number: usize, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("BED line {line_number}: {error}"))
}

/// Normalize one physical line, returning false for non-record lines.
///
/// Always restore a newline before invoking noodles so EOF cannot bypass its
/// required-field checks. Only BED4's optional name is padded for BED3 input.
pub(crate) fn prepare_line(buf: &mut Vec<u8>, fields: usize) -> io::Result<bool> {
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    }
    let line = std::str::from_utf8(buf).map_err(|e| invalid_data(e.to_string()))?;
    if line.trim().is_empty()
        || line.starts_with('#')
        || (!line.contains('\t') && (line.starts_with("track ") || line.starts_with("browser ")))
    {
        return Ok(false);
    }
    let mut columns = line.split('\t');
    let chrom = columns.next().unwrap_or_default();
    if chrom.is_empty() || chrom.bytes().any(|b| b.is_ascii_whitespace()) {
        return Err(invalid_data(
            "chrom must be non-empty and contain no whitespace",
        ));
    }
    let start = parse_coordinate(columns.next(), "start")?;
    let raw_end = columns.next();
    let end = parse_coordinate(raw_end, "end")?;
    if end < start {
        return Err(invalid_data("end must be greater than or equal to start"));
    }
    let count = 3 + columns.count();
    // Noodles recognizes only the literal "0" as an empty end position; a
    // numerically equivalent "00" otherwise fails its non-zero Position type.
    if end == 0 && raw_end != Some("0") {
        let suffix = line.splitn(4, '\t').nth(3);
        let mut normalized = format!("{chrom}\t{start}\t0");
        if let Some(suffix) = suffix {
            normalized.push('\t');
            normalized.push_str(suffix);
        }
        *buf = normalized.into_bytes();
    }
    if fields == 4 && count == 3 {
        buf.extend_from_slice(b"\t.");
    } else if count < fields {
        return Err(invalid_data(format!(
            "expected at least {fields} fields, found {count}"
        )));
    }
    buf.push(b'\n');
    Ok(true)
}

fn parse_coordinate(value: Option<&str>, field: &str) -> io::Result<u32> {
    let value = value.ok_or_else(|| invalid_data(format!("missing required {field} field")))?;
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid_data(format!(
            "{field} must be a non-negative integer"
        )));
    }
    value
        .parse()
        .map_err(|_| invalid_data(format!("{field} exceeds UInt32 range")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_fields_are_checked_even_without_a_newline() {
        for line in ["chr1", "chr1\t0", "chr1\t0\t", "\t0\t5", "chr1 0 5"] {
            for ending in ["", "\n", "\r\n"] {
                assert!(prepare_line(&mut format!("{line}{ending}").into_bytes(), 3).is_err());
            }
        }
    }

    #[test]
    fn padding_distinguishes_missing_empty_and_dot_names() {
        for (line, expected) in [
            ("chr1\t0\t5", "chr1\t0\t5\t.\n"),
            ("chr1\t0\t5\t", "chr1\t0\t5\t\n"),
            ("chr1\t0\t5\t.", "chr1\t0\t5\t.\n"),
        ] {
            let mut buf = line.as_bytes().to_vec();
            assert!(prepare_line(&mut buf, 4).unwrap());
            assert_eq!(buf, expected.as_bytes());
        }
    }

    #[test]
    fn rejects_bad_coordinates_and_encoding() {
        for line in [
            "chr1\t-1\t5",
            "chr1\t+1\t5",
            "chr1\t1.0\t5",
            "chr1\t0\tbad",
            "chr1\t0\t4294967296",
            "chr1\t4294967296\t4294967296",
            "chr1\t5\t4",
        ] {
            assert!(
                prepare_line(&mut line.as_bytes().to_vec(), 3).is_err(),
                "{line}"
            );
        }
        assert!(prepare_line(&mut b"chr1\t0\t5\t\xff".to_vec(), 3).is_err());
        assert!(prepare_line(&mut b"chr1\t0\t0".to_vec(), 3).unwrap());
        assert!(prepare_line(&mut b"chr1\t4294967295\t4294967295".to_vec(), 3).unwrap());
    }

    #[test]
    fn directives_do_not_hide_chromosomes_named_track_or_browser() {
        for line in [
            "",
            " \t",
            "# comment",
            "track name=example",
            "browser position chr1",
        ] {
            assert!(!prepare_line(&mut line.as_bytes().to_vec(), 3).unwrap());
        }
        for line in ["track\t0\t5", "browser\t0\t5", "track1\t0\t5"] {
            assert!(prepare_line(&mut line.as_bytes().to_vec(), 3).unwrap());
        }
    }
}
