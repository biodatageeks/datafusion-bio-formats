use datafusion_bio_format_core::line_source::{
    LineSource, is_local, local_path, open_lines, resolve_compression,
};
use datafusion_bio_format_core::object_storage::{
    CompressionType, ObjectStorageOptions, RemoteObject, StorageType, get_storage_type,
    gzip_multi_member_decoder,
};
use noodles_bgzf as bgzf;
use std::{io, ops::Range, sync::Arc};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use tokio_util::io::StreamReader;

#[derive(Debug, Clone)]
enum Source {
    Local(String),
    Remote(RemoteObject),
}

impl Source {
    async fn bytes(&self, r: Range<u64>) -> io::Result<LineSource> {
        match self {
            Self::Local(path) => {
                let mut f = tokio::fs::File::open(path).await?;
                f.seek(io::SeekFrom::Start(r.start)).await?;
                Ok(Box::new(BufReader::new(f.take(r.end - r.start))))
            }
            Self::Remote(obj) => {
                let stream = obj.stream_range(r).await.map_err(io::Error::other)?;
                Ok(Box::new(BufReader::new(StreamReader::new(stream))))
            }
        }
    }
}

/// Pairs of uncompressed and compressed offsets at BGZF block starts.
#[derive(Debug)]
pub(crate) struct Layout {
    src: Source,
    physical: u64,
    logical: u64,
    blocks: Vec<(u64, u64)>,
}

impl Layout {
    async fn lines(&self, r: Range<u64>) -> io::Result<LineSource> {
        if self.blocks.is_empty() {
            return self.src.bytes(r).await;
        }
        let first = self
            .blocks
            .partition_point(|&(u, _)| u <= r.start)
            .saturating_sub(1);
        let last = self.blocks.partition_point(|&(u, _)| u < r.end);
        let (logical, physical) = self.blocks[first];
        let end = self.blocks.get(last).map_or(self.physical, |p| p.1);
        let raw = self.src.bytes(physical..end).await?;
        let mut reader = bgzf::r#async::io::Reader::new(raw);
        tokio::io::copy(
            &mut (&mut reader).take(r.start - logical),
            &mut tokio::io::sink(),
        )
        .await?;
        Ok(Box::new(BufReader::new(reader.take(r.end - r.start))))
    }
    async fn align(&self, target: u64) -> io::Result<u64> {
        // Look behind both bytes of // and CRLF. Discard the first fragment
        // unless reading at offset zero, so indentation can never disappear.
        let mut pos = target.saturating_sub(5);
        let mut reader = self.lines(pos..self.logical).await?;
        let mut line = Vec::new();
        if pos > 0 {
            pos += reader.read_until(b'\n', &mut line).await? as u64;
        }
        while pos < self.logical {
            line.clear();
            let n = reader.read_until(b'\n', &mut line).await?;
            if n == 0 {
                break;
            }
            pos += n as u64;
            if pos >= target && line.trim_ascii_end() == b"//" {
                return Ok(pos);
            }
        }
        Ok(self.logical)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Slice {
    Whole,
    Range(Arc<Layout>, Range<u64>),
    Sequential(RemoteObject, CompressionType),
}

impl Slice {
    pub(crate) async fn open(
        &self,
        path: &str,
        opts: &ObjectStorageOptions,
    ) -> io::Result<LineSource> {
        match self {
            Self::Range(layout, r) => layout.lines(r.clone()).await,
            Self::Whole => open_lines(path, opts, None).await,
            Self::Sequential(obj, compression) => {
                let raw = BufReader::new(StreamReader::new(
                    obj.stream_single_request()
                        .await
                        .map_err(io::Error::other)?,
                ));
                match compression {
                    CompressionType::NONE => Ok(Box::new(raw)),
                    CompressionType::BGZF => Ok(Box::new(BufReader::new(
                        bgzf::r#async::io::Reader::new(raw),
                    ))),
                    CompressionType::GZIP => {
                        Ok(Box::new(BufReader::new(gzip_multi_member_decoder(raw))))
                    }
                    other => Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!("unsupported compression {other:?}"),
                    )),
                }
            }
        }
    }
}

/// Derive the same block offsets as a GZI from BSIZE and ISIZE, without
/// inflating the blocks. Retain only offsets and one compressed block.
async fn block_index(source: &Source, size: u64) -> io::Result<(Vec<(u64, u64)>, u64)> {
    let mut offsets = Vec::new();
    let (mut physical, mut logical) = (0u64, 0u64);
    if let Source::Local(path) = source {
        match bgzf::gzi::r#async::fs::read(format!("{path}.gzi")).await {
            Ok(index) => {
                for &(next_physical, next_logical) in index.as_ref() {
                    if next_physical <= physical
                        || next_physical >= size
                        || next_logical <= logical
                        || next_logical - logical > 65_536
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid GZI offset",
                        ));
                    }
                    offsets.push((logical, physical));
                    (physical, logical) = (next_physical, next_logical);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    let mut src = source.bytes(physical..size).await?;
    let mut block = Vec::new();
    while physical < size {
        let mut header = [0u8; 18];
        src.read_exact(&mut header).await?;
        // The noodles decoder expects an 18-byte BGZF header. XLEN is six,
        // with the BC subfield at byte 12 and its two-byte BSIZE at byte 16.
        if header[..4] != [31, 139, 8, 4] || header[10..16] != [6, 0, b'B', b'C', 2, 0] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid BGZF block header",
            ));
        }
        let n = usize::from(u16::from_le_bytes([header[16], header[17]])) + 1;
        if n < 26 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid BGZF block size",
            ));
        }
        block.resize(n - 18, 0);
        src.read_exact(&mut block).await?;
        let length = u32::from_le_bytes(block[block.len() - 4..].try_into().unwrap());
        if length > 65536 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid BGZF uncompressed size",
            ));
        }
        if length > 0 {
            offsets.push((logical, physical));
        }
        physical += n as u64;
        logical += u64::from(length);
    }
    Ok((offsets, logical))
}

pub(crate) async fn partitions(
    path: &str,
    opts: &ObjectStorageOptions,
    n: usize,
) -> io::Result<Vec<Slice>> {
    let serial = || vec![Slice::Whole];
    let http = matches!(get_storage_type(path.into()), StorageType::HTTP);
    if n <= 1 && !http {
        return Ok(serial());
    }
    let compression = resolve_compression(path, opts).await?;
    if http && (n <= 1 || compression == CompressionType::GZIP) {
        let obj = RemoteObject::open(path.into(), opts.clone())
            .await
            .map_err(io::Error::other)?;
        return Ok(vec![Slice::Sequential(obj, compression)]);
    }
    if !matches!(compression, CompressionType::NONE | CompressionType::BGZF) {
        return Ok(serial());
    }
    let (src, physical) = if is_local(path) {
        let path = local_path(path);
        (
            Source::Local(path.to_owned()),
            tokio::fs::metadata(path).await?.len(),
        )
    } else {
        let obj = RemoteObject::open(path.to_owned(), opts.clone())
            .await
            .map_err(io::Error::other)?;
        let Ok(size) = obj.size().await else {
            return Ok(vec![Slice::Sequential(obj, compression)]);
        };
        // GET-only servers can support streaming while rejecting range requests.
        if size < 2 || !matches!(obj.read_range(1..2).await, Ok(buf) if buf.len() == 1) {
            return Ok(vec![Slice::Sequential(obj, compression)]);
        }
        (Source::Remote(obj), size)
    };
    let (blocks, logical) = if compression == CompressionType::BGZF {
        block_index(&src, physical).await?
    } else {
        (Vec::new(), physical)
    };
    if logical == 0 {
        return Ok(serial());
    }
    let layout = Arc::new(Layout {
        src,
        physical,
        logical,
        blocks,
    });
    let stride = logical.div_ceil(n as u64);
    let mut slices = Vec::new();
    let mut start = 0;
    for i in 1..n {
        let target = stride.saturating_mul(i as u64);
        if target <= start {
            continue;
        }
        if target >= logical {
            break;
        }
        let end = layout.align(target).await?;
        if end == logical {
            break;
        }
        slices.push(Slice::Range(layout.clone(), start..end));
        start = end;
    }
    if slices.is_empty() {
        return Ok(serial());
    }
    slices.push(Slice::Range(layout, start..logical));
    Ok(slices)
}
