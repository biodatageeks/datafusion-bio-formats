use std::io::Read;
use std::path::Path;

use bytes::{Buf, Bytes};
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::companion::sanitize_location;
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, RemoteObject, StorageType, get_storage_type,
};
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Remote chunks buffered ahead of a companion decoder.
const STREAM_CHANNEL_CHUNKS: usize = 4;

#[derive(Clone, Debug)]
pub(crate) enum ObjectAccess {
    Local(String),
    Remote(RemoteObject),
}

pub(crate) enum ObjectRangeReader {
    Local {
        file: tokio::fs::File,
        display_path: String,
        buffer: Vec<u8>,
    },
    Remote {
        object: RemoteObject,
        display_path: String,
        buffer: Bytes,
    },
}

impl ObjectAccess {
    pub(crate) async fn open(path: &str, options: &ObjectStorageOptions) -> Result<Self> {
        match get_storage_type(path.to_string()) {
            StorageType::LOCAL => {
                let local = local_path(path)?.to_string();
                if !Path::new(&local).is_file() {
                    return Err(DataFusionError::Plan(format!(
                        "object does not exist: {}",
                        sanitize_location(path)
                    )));
                }
                Ok(Self::Local(local))
            }
            _ => {
                let object = RemoteObject::open(path.to_string(), options.clone())
                    .await
                    .map_err(|error| external_error("open", path, error))?;
                object
                    .size()
                    .await
                    .map_err(|error| external_error("stat", path, error))?;
                Ok(Self::Remote(object))
            }
        }
    }

    pub(crate) async fn exists(path: &str, options: &ObjectStorageOptions) -> Result<bool> {
        match get_storage_type(path.to_string()) {
            StorageType::LOCAL => Ok(Path::new(local_path(path)?).is_file()),
            _ => match RemoteObject::open(path.to_string(), options.clone()).await {
                Ok(object) => match object.size().await {
                    Ok(_) => Ok(true),
                    Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(false),
                    Err(error) => Err(external_error("stat", path, error)),
                },
                Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(external_error("open", path, error)),
            },
        }
    }

    pub(crate) async fn size(&self, display_path: &str) -> Result<u64> {
        match self {
            Self::Local(path) => std::fs::metadata(path)
                .map(|metadata| metadata.len())
                .map_err(|error| io_error("stat", display_path, error)),
            Self::Remote(object) => object
                .size()
                .await
                .map_err(|error| external_error("stat", display_path, error)),
        }
    }

    pub(crate) async fn read_range(
        &self,
        display_path: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes> {
        let mut reader = self.range_reader(display_path).await?;
        reader.read_range(range).await?;
        Ok(reader.into_bytes())
    }

    pub(crate) async fn range_reader(&self, display_path: &str) -> Result<ObjectRangeReader> {
        match self {
            Self::Local(path) => Ok(ObjectRangeReader::Local {
                file: tokio::fs::File::open(path)
                    .await
                    .map_err(|error| io_error("open", display_path, error))?,
                display_path: display_path.to_string(),
                buffer: Vec::new(),
            }),
            Self::Remote(object) => Ok(ObjectRangeReader::Remote {
                object: object.clone(),
                display_path: display_path.to_string(),
                buffer: Bytes::new(),
            }),
        }
    }

    /// The object as a blocking reader, for a decoder on a worker thread.
    ///
    /// The size is checked against `max_bytes` first, so an oversized companion
    /// fails before a byte is read. A remote object is streamed through a
    /// bounded channel rather than fetched whole.
    pub(crate) async fn companion_reader(
        &self,
        display_path: &str,
        max_bytes: usize,
    ) -> Result<Box<dyn Read + Send>> {
        let size = self.size(display_path).await?;
        if size > max_bytes as u64 {
            return Err(DataFusionError::Plan(format!(
                "object {} is {size} bytes, exceeding max_companion_bytes {max_bytes}",
                sanitize_location(display_path)
            )));
        }
        match self {
            Self::Local(path) => Ok(Box::new(
                std::fs::File::open(path).map_err(|error| io_error("open", display_path, error))?,
            )),
            Self::Remote(object) => {
                let mut stream = object
                    .stream()
                    .await
                    .map_err(|error| external_error("read", display_path, error))?;
                let (sender, receiver) = tokio::sync::mpsc::channel(STREAM_CHANNEL_CHUNKS);
                tokio::spawn(async move {
                    while let Some(chunk) = stream.next().await {
                        if sender.send(chunk).await.is_err() {
                            break;
                        }
                    }
                });
                Ok(Box::new(ChannelReader {
                    receiver,
                    current: Bytes::new(),
                }))
            }
        }
    }
}

/// A blocking `Read` over chunks a runtime task streams into a channel.
struct ChannelReader {
    receiver: tokio::sync::mpsc::Receiver<std::io::Result<Bytes>>,
    current: Bytes,
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.current.is_empty() {
            match self.receiver.blocking_recv() {
                Some(Ok(chunk)) => self.current = chunk,
                Some(Err(error)) => return Err(error),
                None => return Ok(0),
            }
        }
        let count = buf.len().min(self.current.len());
        buf[..count].copy_from_slice(&self.current[..count]);
        self.current.advance(count);
        Ok(count)
    }
}

impl ObjectRangeReader {
    pub(crate) async fn read_range(&mut self, range: std::ops::Range<u64>) -> Result<&[u8]> {
        if range.end < range.start {
            return Err(DataFusionError::Execution(format!(
                "invalid object range {}..{}",
                range.start, range.end
            )));
        }
        let expected = usize::try_from(range.end - range.start).map_err(|_| {
            DataFusionError::Execution("object range does not fit usize".to_string())
        })?;
        if expected == 0 {
            return match self {
                Self::Local { buffer, .. } => {
                    buffer.clear();
                    Ok(buffer)
                }
                Self::Remote { buffer, .. } => {
                    *buffer = Bytes::new();
                    Ok(buffer)
                }
            };
        }
        match self {
            Self::Local {
                file,
                display_path,
                buffer,
            } => {
                file.seek(std::io::SeekFrom::Start(range.start))
                    .await
                    .map_err(|error| io_error("seek", display_path, error))?;
                buffer.resize(expected, 0);
                file.read_exact(buffer)
                    .await
                    .map_err(|error| io_error("read", display_path, error))?;
                Ok(buffer)
            }
            Self::Remote {
                object,
                display_path,
                buffer,
            } => {
                *buffer = object
                    .read_range(range)
                    .await
                    .map_err(|error| external_error("read", display_path, error))?;
                Ok(buffer)
            }
        }
    }

    /// The bytes of the most recent [`Self::read_range`].
    ///
    /// Lets a caller hand the loaded range to a decoder without copying it out,
    /// and lets the reader's buffer be reused for the next range.
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Self::Local { buffer, .. } => buffer,
            Self::Remote { buffer, .. } => buffer,
        }
    }

    fn into_bytes(self) -> Bytes {
        match self {
            Self::Local { buffer, .. } => Bytes::from(buffer),
            Self::Remote { buffer, .. } => buffer,
        }
    }
}

fn local_path(path: &str) -> Result<&str> {
    if let Some(path) = path.strip_prefix("file://") {
        if path.is_empty() {
            return Err(DataFusionError::Plan(
                "local file URL has an empty path".to_string(),
            ));
        }
        Ok(path)
    } else {
        Ok(path)
    }
}

fn io_error(action: &str, path: &str, error: std::io::Error) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::new(
        error.kind(),
        format!("{action} {}: {error}", sanitize_location(path)),
    )))
}

fn external_error(
    action: &str,
    path: &str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(format!(
        "{action} {}: {error}",
        sanitize_location(path)
    ))))
}

#[cfg(test)]
mod tests {
    use super::ObjectAccess;

    #[cfg(unix)]
    #[tokio::test]
    async fn local_range_reader_reuses_its_open_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("ranges.bin");
        std::fs::write(&path, b"0123456789").unwrap();
        let source = ObjectAccess::Local(path.to_string_lossy().into_owned());
        let mut reader = source.range_reader("ranges.bin").await.unwrap();

        // An open Unix file remains readable after unlinking. A range reader
        // that reopened by path for every request would fail this regression.
        std::fs::remove_file(path).unwrap();
        assert_eq!(reader.read_range(1..4).await.unwrap(), b"123");
        assert_eq!(reader.read_range(7..10).await.unwrap(), b"789");
    }
}
