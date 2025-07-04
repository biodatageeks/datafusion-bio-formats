mod line {
    use tokio::io::{self, AsyncBufRead, AsyncBufReadExt};

    /// Reads a single line (up to and including `\n`) from `reader` into `buf`.
    /// Strips a single trailing `\r` or `\n` or `\r\n` if present.
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

/// An async BED reader.
pub struct Reader<R, const N: usize> {
    inner: R,
}

impl<R, const N: usize> Reader<R, N> {
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


                pub async fn read_line(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
                    // Reuse the same logic as in GFF’s read_line, minus directive handling.
                    line::read_line(&mut self.inner, buf).await
                }


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
