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

use futures::{Stream, stream};
use std::io::Cursor;
use tokio::io::{self, AsyncBufRead};

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
}

impl<R, const N: usize> Reader<R, N> {
    /// Creates a new async BED reader wrapping the given reader
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying async reader
    pub fn new(inner: R) -> Self {
        Self { inner }
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

// impl_async_reader_base!(3,6);

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
                            reader.read_line(&mut buf).await.map(|n| {
                                if n == 0 {
                                    None
                                } else {
                                    let line = String::from_utf8(buf.clone())
                                        .expect("BED lines should always be valid UTF-8");
                                    Some((line, (reader, buf)))
                                }
                            })
                        },
                    ))
                }

                /// Returns a stream of BED records from the reader
                pub fn records(&mut self) -> impl Stream<Item = io::Result<Record<$n>>> + '_ {
                        // Initial state is (self, an empty Vec<u8>)
                        Box::pin(
                            stream::try_unfold(
                                (self, Vec::new()),
                                |(reader, mut buf)| async move
                                    {
                                    buf.clear();
                                    let n = reader.read_line(&mut buf).await?;
                                    if n == 0 {
                                        return Ok(None);
                                    }
                                    let mut rec = Record::<$n>::default();
                                    let cursor = Cursor::new(buf.clone());
                                    let mut bed_reader = noodles_bed::io::Reader::<$n, _>::new(cursor);
                                    let bytes_read = bed_reader.read_record(&mut rec)?;
                                    if bytes_read == 0 {
                                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF in bed parser"));
                                    }
                                    Ok(Some((rec, (reader, buf))))
                                },
                            )
                        )
                 }
            }
        )*
    };
}

impl_async_reader!(3, 4, 5, 6);
