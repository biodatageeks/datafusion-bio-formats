/// Internal utilities for reading lines from async readers
mod line {
    use tokio::io::{self, AsyncBufRead, AsyncBufReadExt};

    /// Reads a single line (up to and including `\n`) from `reader` into `buf`.
    ///
    /// Strips a single trailing `\r` or `\n` or `\r\n` if present.
    ///
    /// # Arguments
    ///
    /// * `reader` - Async buffered reader
    /// * `buf` - Buffer to accumulate line data
    ///
    /// # Returns
    ///
    /// Number of bytes read (0 indicates EOF), or error
    pub async fn read_line<R>(reader: &mut R, buf: &mut Vec<u8>) -> io::Result<usize>
    where
        R: AsyncBufRead + Unpin,
    {
        const LINE_FEED: u8 = b'\n';
        const CARRIAGE_RETURN: u8 = b'\r';

        let n = reader.read_until(LINE_FEED, buf).await?;
        if n == 0 {
            // EOF
            return Ok(0);
        }

        // Remove trailing '\n'
        if buf.ends_with(&[LINE_FEED]) {
            buf.pop();
            // If now ends with '\r', strip it too.
            if buf.ends_with(&[CARRIAGE_RETURN]) {
                buf.pop();
            }
        }

        Ok(n)
    }
}

use crate::record::{line_error, prepare_line};
use async_stream::try_stream;
use futures::{Stream, stream};
use std::io::Cursor;
use tokio::io::{self, AsyncBufRead, AsyncBufReadExt};

use noodles_bed::Record;

/// An async BED reader for streaming BED records
///
/// This generic reader supports any async buffered reader and can parse
/// BED records of a specific column count (3-6).
///
/// # Type Parameters
///
/// * `R` - Async buffered reader type
/// * `N` - Number of BED columns (3-6)
pub struct Reader<R, const N: usize> {
    /// The underlying async reader
    inner: R,
    line_number: usize,
}

impl<R, const N: usize> Reader<R, N> {
    /// Creates a new async BED reader wrapping the given reader
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying async reader
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            line_number: 0,
        }
    }

    /// Returns a reference to the underlying reader.
    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    /// Returns a mutable reference to the underlying reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Unwraps and returns the underlying reader.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

macro_rules! impl_async_reader {
    ($($n:expr),*) => {
        $(
            impl<R> Reader<R, $n>
            where
                R: AsyncBufRead + Unpin,
            {


                /// Reads a single line into the provided buffer
                ///
                /// # Arguments
                ///
                /// * `buf` - Buffer to accumulate line data
                ///
                /// # Returns
                ///
                /// Number of bytes read (0 indicates EOF), or error
                pub async fn read_line(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
                    // Reuse the same logic as in GFF's read_line, minus directive handling.
                    line::read_line(&mut self.inner, buf).await
                }

                /// Returns a stream of lines from the reader
                pub fn lines(&mut self) -> impl Stream<Item = io::Result<String>> + '_ {
                    Box::pin(stream::try_unfold(
                        (self, Vec::new()),
                        |(reader, mut buf)| async move {
                            buf.clear();
                            let n = reader.read_line(&mut buf).await?;
                            Ok(if n == 0 {
                                None
                            } else {
                                let line = String::from_utf8(buf.clone())
                                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                                Some((line, (reader, buf)))
                            })

                        },
                    ))
                }

                /// Returns a stream of BED records from the reader
                pub fn records(&mut self) -> impl Stream<Item = io::Result<Record<$n>>> + '_ {
                    try_stream! {
                        let mut buf = Vec::new();
                        loop {
                            buf.clear();
                            let line_number = self.line_number + 1;
                            let bytes_read = self.inner.read_until(b'\n', &mut buf).await
                                .map_err(|e| line_error(line_number, e))?;
                            if bytes_read == 0 {
                                break;
                            }
                            self.line_number = line_number;
                            if !prepare_line(&mut buf, $n).map_err(|e| line_error(line_number, e))? {
                                continue;
                            }
                            let mut record = Record::<$n>::default();
                            let mut reader = noodles_bed::io::Reader::<$n, _>::new(Cursor::new(&buf));
                            reader.read_record(&mut record).map_err(|e| line_error(line_number, e))?;
                            yield record;
                        }
                    }
                }

            }
        )*
    };
}

impl_async_reader!(3, 4, 5, 6);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;
    use tokio_util::io::StreamReader;

    #[tokio::test]
    async fn tiny_buffers_handle_comments_crlf_utf8_and_final_eof() {
        let data = b"# header\r\n\nchr1\t0\t5\tgene-\xce\xb1\r\n# middle\nchr1\t5\t8\n# final";
        for capacity in [1, 2, 7, 64] {
            let inner = tokio::io::BufReader::with_capacity(capacity, &data[..]);
            let mut reader = Reader::<_, 4>::new(inner);
            let records = reader.records();
            futures::pin_mut!(records);
            let first = records.next().await.unwrap().unwrap();
            assert_eq!(first.name().unwrap().to_string(), "gene-α");
            assert!(records.next().await.unwrap().unwrap().name().is_none());
            assert!(records.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn io_failure_is_yielded_once_and_terminates_the_stream() {
        let chunks = futures::stream::iter([
            Ok(Bytes::from_static(b"chr1\t0\t5\n")),
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected read failure",
            )),
            Ok(Bytes::from_static(b"chr1\t5\t8\n")),
        ]);
        let mut reader = Reader::<_, 3>::new(StreamReader::new(chunks));
        let records = reader.records();
        futures::pin_mut!(records);
        assert!(records.next().await.unwrap().is_ok());
        let error = records.next().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(error.to_string().contains("BED line 2"));
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn malformed_record_is_yielded_once_with_physical_line_number() {
        let mut reader =
            Reader::<_, 3>::new(&b"# comment\nchr1\t0\t5\n\nchr1\t5\nchr1\t8\t9\n"[..]);
        let records = reader.records();
        futures::pin_mut!(records);
        assert!(records.next().await.unwrap().is_ok());
        let error = records.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("BED line 4"), "{error}");
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn lines_api_returns_utf8_errors_instead_of_panicking() {
        let mut reader = Reader::<_, 3>::new(&b"chr1\t0\t5\n\xff\n"[..]);
        let lines = reader.lines();
        futures::pin_mut!(lines);
        assert_eq!(lines.next().await.unwrap().unwrap(), "chr1\t0\t5");
        assert_eq!(
            lines.next().await.unwrap().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(lines.next().await.is_none());
    }

    #[tokio::test]
    async fn declared_low_level_widths_require_fields_even_at_eof() {
        let mut bed5 = Reader::<_, 5>::new(&b"chr1\t0\t5\tname"[..]);
        assert!(bed5.records().boxed().next().await.unwrap().is_err());
        let mut bed6 = Reader::<_, 6>::new(&b"chr1\t0\t5\tname\t0"[..]);
        assert!(bed6.records().boxed().next().await.unwrap().is_err());
        let mut bed5 = Reader::<_, 5>::new(&b"chr1\t0\t5\tname\t42"[..]);
        assert_eq!(
            bed5.records()
                .boxed()
                .next()
                .await
                .unwrap()
                .unwrap()
                .score()
                .unwrap(),
            42
        );
        let mut bed6 = Reader::<_, 6>::new(&b"chr1\t0\t5\tname\t0\t-"[..]);
        assert_eq!(
            bed6.records()
                .boxed()
                .next()
                .await
                .unwrap()
                .unwrap()
                .strand()
                .unwrap(),
            Some(noodles_bed::feature::record::Strand::Reverse)
        );
    }
}
