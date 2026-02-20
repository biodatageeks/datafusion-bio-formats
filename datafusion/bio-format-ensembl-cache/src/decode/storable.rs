use crate::errors::{Result, exec_err};
use serde_json::Value;

/// Minimal storable decoder subset for Ensembl cache fixtures.
///
/// v1 supports payloads encoded as: `JSON:{...}`.
pub(crate) fn decode(payload: &str) -> Result<Value> {
    let Some(json_payload) = payload.strip_prefix("JSON:") else {
        return Err(exec_err(
            "Unsupported storable payload in v1 decoder (expected JSON: prefix)",
        ));
    };

    serde_json::from_str(json_payload).map_err(|e| {
        exec_err(format!(
            "Failed decoding storable JSON payload in v1 subset decoder: {}",
            e
        ))
    })
}
