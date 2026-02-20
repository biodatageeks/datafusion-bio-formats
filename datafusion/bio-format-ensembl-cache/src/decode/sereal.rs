use crate::errors::{Result, exec_err};
use serde_json::Value;

/// Minimal sereal decoder subset for Ensembl cache fixtures.
///
/// v1 supports payloads encoded as: `SRL1{...}`.
pub(crate) fn decode(payload: &str) -> Result<Value> {
    let Some(json_payload) = payload.strip_prefix("SRL1") else {
        return Err(exec_err(
            "Unsupported sereal payload in v1 decoder (expected SRL1 prefix)",
        ));
    };

    serde_json::from_str(json_payload).map_err(|e| {
        exec_err(format!(
            "Failed decoding sereal JSON payload in v1 subset decoder: {}",
            e
        ))
    })
}
