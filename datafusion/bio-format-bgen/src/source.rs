use std::path::Path;

use bytes::Bytes;
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::companion::sanitize_location;
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, RemoteObject, StorageType, get_storage_type,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Clone, Debug)]
pub(crate) enum ObjectAccess {
    Local(String),
    Remote(RemoteObject),
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
            return Ok(Bytes::new());
        }
        match self {
            Self::Local(path) => {
                let mut file = tokio::fs::File::open(path)
                    .await
                    .map_err(|error| io_error("open", display_path, error))?;
                file.seek(std::io::SeekFrom::Start(range.start))
                    .await
                    .map_err(|error| io_error("seek", display_path, error))?;
                let mut bytes = vec![0; expected];
                file.read_exact(&mut bytes)
                    .await
                    .map_err(|error| io_error("read", display_path, error))?;
                Ok(Bytes::from(bytes))
            }
            Self::Remote(object) => object
                .read_range(range)
                .await
                .map_err(|error| external_error("read", display_path, error)),
        }
    }

    pub(crate) async fn read_all_bounded(
        &self,
        display_path: &str,
        max_bytes: usize,
    ) -> Result<Bytes> {
        let size = self.size(display_path).await?;
        if size > max_bytes as u64 {
            return Err(DataFusionError::Plan(format!(
                "object {} is {size} bytes, exceeding configured limit {max_bytes}",
                sanitize_location(display_path)
            )));
        }
        self.read_range(display_path, 0..size).await
    }

    pub(crate) fn local_path(&self) -> Option<&str> {
        match self {
            Self::Local(path) => Some(path),
            Self::Remote(_) => None,
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

pub(crate) fn io_error(action: &str, path: &str, error: std::io::Error) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::new(
        error.kind(),
        format!("{action} {}: {error}", sanitize_location(path)),
    )))
}

pub(crate) fn external_error(
    action: &str,
    path: &str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(format!(
        "{action} {}: {error}",
        sanitize_location(path)
    ))))
}
