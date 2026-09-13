//! Unified line-oriented input for local and remote files with `gz` / `bgz` support.
//!
//! Every reader in this crate consumes a [`LineSource`]: a boxed async buffered
//! reader. Storage backend and compression are resolved once here, so the
//! parsers never need to know where the bytes came from.

use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, StorageType, get_compression_type, get_remote_stream,
    get_remote_stream_bgzf_async, get_remote_stream_gz_async, get_storage_type,
    gzip_multi_member_decoder,
};
use noodles_bgzf as bgzf;
use std::io::{self, SeekFrom};
use std::ops::Range;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use tokio_util::io::StreamReader;

/// Buffer size for line reading; alignment rows can be tens of kilobytes.
const BUF_CAPACITY: usize = 1 << 20;

/// A boxed, buffered, async byte source that the parsers read line by line.
pub type LineSource = Box<dyn AsyncBufRead + Send + Unpin>;

/// Whether `path` is on the local filesystem (as opposed to an object store).
pub fn is_local(path: &str) -> bool {
    matches!(get_storage_type(path.to_string()), StorageType::LOCAL)
}

/// Strips a `file://` scheme so a local URI can be handed to the filesystem.
///
/// `get_storage_type` classifies `file://…` as local and the shared compression
/// sniffing already strips the scheme, so without this every filesystem open
/// would look for a file whose name literally starts with `file://`. Matches
/// what the BED and Cooler providers do.
pub fn local_path(path: &str) -> &str {
    path.strip_prefix("file://").unwrap_or(path)
}

/// Resolves the effective compression of `path`, honouring an explicit hint in
/// `opts` and otherwise sniffing the extension / magic bytes.
pub async fn resolve_compression(
    path: &str,
    opts: &ObjectStorageOptions,
) -> io::Result<CompressionType> {
    get_compression_type(
        path.to_string(),
        opts.compression_type.clone(),
        opts.clone(),
    )
    .await
    .map_err(io::Error::other)
}

/// Opens `path` as a [`LineSource`].
///
/// `range` restricts reading to a byte window and is only supported for local,
/// uncompressed files — the only case where byte offsets are meaningful. Callers
/// that want to partition must check [`is_local`] and [`resolve_compression`]
/// first; passing a range for any other input is an error.
pub async fn open_lines(
    path: &str,
    opts: &ObjectStorageOptions,
    range: Option<Range<u64>>,
) -> io::Result<LineSource> {
    let compression = resolve_compression(path, opts).await?;
    let local = is_local(path);

    if range.is_some() && !(local && compression == CompressionType::NONE) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("byte-range reads are only supported for local uncompressed files: {path}"),
        ));
    }

    if local {
        let file = tokio::fs::File::open(local_path(path)).await?;
        return match compression {
            CompressionType::NONE => match range {
                Some(r) => {
                    let mut file = file;
                    file.seek(SeekFrom::Start(r.start)).await?;
                    let take = file.take(r.end.saturating_sub(r.start));
                    Ok(Box::new(BufReader::with_capacity(BUF_CAPACITY, take)))
                }
                None => Ok(Box::new(BufReader::with_capacity(BUF_CAPACITY, file))),
            },
            CompressionType::GZIP => {
                let decoder = gzip_multi_member_decoder(BufReader::new(file));
                Ok(Box::new(BufReader::with_capacity(BUF_CAPACITY, decoder)))
            }
            CompressionType::BGZF => {
                let decoder = bgzf::r#async::io::Reader::new(file);
                Ok(Box::new(BufReader::with_capacity(BUF_CAPACITY, decoder)))
            }
            other => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("unsupported compression {other:?} for {path}"),
            )),
        };
    }

    match compression {
        CompressionType::NONE => {
            let stream = get_remote_stream(path.to_string(), opts.clone(), None)
                .await
                .map_err(io::Error::other)?;
            Ok(Box::new(BufReader::with_capacity(
                BUF_CAPACITY,
                StreamReader::new(stream),
            )))
        }
        CompressionType::GZIP => {
            let decoder = get_remote_stream_gz_async(path.to_string(), opts.clone())
                .await
                .map_err(io::Error::other)?;
            Ok(Box::new(BufReader::with_capacity(BUF_CAPACITY, decoder)))
        }
        CompressionType::BGZF => {
            let decoder = get_remote_stream_bgzf_async(path.to_string(), opts.clone())
                .await
                .map_err(io::Error::other)?;
            Ok(Box::new(BufReader::with_capacity(BUF_CAPACITY, decoder)))
        }
        other => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("unsupported compression {other:?} for {path}"),
        )),
    }
}

/// Reads one line into `buf` (cleared first), stripping the trailing `\n` and
/// `\r`. Returns `false` at end of input.
pub async fn read_line(src: &mut LineSource, buf: &mut Vec<u8>) -> io::Result<bool> {
    buf.clear();
    let n = src.read_until(b'\n', buf).await?;
    if n == 0 {
        return Ok(false);
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
    }
    Ok(true)
}
