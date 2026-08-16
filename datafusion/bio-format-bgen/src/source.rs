use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::companion::sanitize_location;
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, RemoteObject, StorageType, get_storage_type,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// A handle to one object, counting the requests made through it.
///
/// The count is shared by clones, so it totals every request against the object
/// however the handle was passed around. Counting here rather than at each call
/// site means a new read cannot be added without being counted.
#[derive(Clone, Debug)]
pub(crate) struct ObjectAccess {
    backing: Backing,
    requests: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
enum Backing {
    Local(String),
    Remote(RemoteObject),
}

impl ObjectAccess {
    fn new(backing: Backing) -> Self {
        Self {
            backing,
            requests: Arc::new(AtomicU64::new(0)),
            bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A local handle, for tests that never read through it.
    #[cfg(test)]
    pub(crate) fn local_for_test(path: String) -> Self {
        Self::new(Backing::Local(path))
    }

    /// Object requests issued through this handle and its clones.
    pub(crate) fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// Bytes returned through this handle and its clones.
    ///
    /// Counted where the bytes arrive rather than tallied by each caller, so a
    /// read added later cannot be left out of what a scan reports.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    fn count_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }
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
                Ok(Self::new(Backing::Local(local)))
            }
            _ => {
                let object = RemoteObject::open(path.to_string(), options.clone())
                    .await
                    .map_err(|error| external_error("open", path, error))?;
                // No existence probe here: reading the header asks for the
                // object's size as its first act, so a missing object still
                // fails immediately, and a second stat only doubles the round
                // trips against remote storage.
                Ok(Self::new(Backing::Remote(object)))
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
        self.count_request();
        match &self.backing {
            Backing::Local(path) => std::fs::metadata(path)
                .map(|metadata| metadata.len())
                .map_err(|error| io_error("stat", display_path, error)),
            Backing::Remote(object) => object
                .size()
                .await
                .map_err(|error| external_error("stat", display_path, error)),
        }
    }

    /// Returns the object's length and a validator identifying this version of
    /// it, without reading its contents. See [`RemoteObject::identity`].
    pub(crate) async fn identity(&self, display_path: &str) -> Result<(u64, Option<String>)> {
        self.count_request();
        match &self.backing {
            Backing::Local(path) => {
                let metadata = std::fs::metadata(path)
                    .map_err(|error| io_error("stat", display_path, error))?;
                let validator = metadata
                    .modified()
                    .ok()
                    .and_then(|modified| {
                        modified
                            .duration_since(std::time::SystemTime::UNIX_EPOCH)
                            .ok()
                    })
                    .map(|since_epoch| {
                        format!("len={};modified={}", metadata.len(), since_epoch.as_nanos())
                    });
                Ok((metadata.len(), validator))
            }
            Backing::Remote(object) => object
                .identity()
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
        self.count_request();
        let counted = |bytes: Bytes| {
            self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            bytes
        };
        match &self.backing {
            Backing::Local(path) => {
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
                Ok(counted(Bytes::from(bytes)))
            }
            Backing::Remote(object) => object
                .read_range(range)
                .await
                .map(counted)
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
        match &self.backing {
            Backing::Local(path) => Some(path),
            Backing::Remote(_) => None,
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
