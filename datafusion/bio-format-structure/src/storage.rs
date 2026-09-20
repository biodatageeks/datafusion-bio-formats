use crate::{error, options::StructureOptions};
use datafusion::common::Result;
use datafusion_bio_format_core::object_storage::{ObjectStorageOptions, RemoteObject};
use futures::StreamExt;
use std::io::Read;
pub async fn read(
    path: &str,
    options: &StructureOptions,
    storage: Option<ObjectStorageOptions>,
) -> Result<(Vec<u8>, usize)> {
    let data = if path.contains("://") {
        let object = RemoteObject::open(path.to_owned(), storage.unwrap_or_default())
            .await
            .map_err(|e| error(e.to_string()))?;
        // Chunked reads need a size preflight (HEAD). A GET-only endpoint may answer
        // that with 403 or 405; nothing has been consumed at that point, so any
        // preflight failure retries as one sequential GET, whose own error is the
        // one reported. The byte ceiling is enforced while consuming either stream.
        let mut stream = match object.stream().await {
            Ok(stream) => stream,
            Err(_) => object
                .stream_single_request()
                .await
                .map_err(|e| error(e.to_string()))?,
        };
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| error(e.to_string()))?;
            if data.len() + chunk.len() > options.max_input_bytes {
                return Err(error("input exceeds max_input_bytes"));
            }
            data.extend_from_slice(&chunk);
        }
        data
    } else {
        let mut data = Vec::new();
        std::fs::File::open(path)?
            .take(options.max_input_bytes as u64 + 1)
            .read_to_end(&mut data)?;
        data
    };
    if data.len() > options.max_input_bytes {
        return Err(error("input exceeds max_input_bytes"));
    }
    let encoded_bytes = data.len();
    let decoded = if data.starts_with(&[0x1f, 0x8b]) {
        let mut decoded = Vec::new();
        flate2::read::MultiGzDecoder::new(data.as_slice())
            .take(options.max_decoded_bytes as u64 + 1)
            .read_to_end(&mut decoded)?;
        decoded
    } else {
        data
    };
    if decoded.len() > options.max_decoded_bytes {
        return Err(error("input exceeds max_decoded_bytes"));
    }
    Ok((decoded, encoded_bytes))
}
