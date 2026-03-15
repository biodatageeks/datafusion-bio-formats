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
            "Failed decoding storable JSON payload in v1 subset decoder: {e}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_valid_json_object() {
        let val = decode("JSON:{\"key\":\"value\"}").unwrap();
        assert_eq!(val["key"], "value");
    }

    #[test]
    fn decode_valid_json_array() {
        let val = decode("JSON:[1,2,3]").unwrap();
        assert_eq!(val.as_array().unwrap().len(), 3);
    }

    #[test]
    fn decode_missing_prefix() {
        assert!(decode("{\"key\":\"value\"}").is_err());
    }

    #[test]
    fn decode_invalid_json() {
        assert!(decode("JSON:{invalid}").is_err());
    }

    #[test]
    fn decode_nested_object() {
        let val = decode("JSON:{\"a\":{\"b\":42}}").unwrap();
        assert_eq!(val["a"]["b"], 42);
    }

    #[test]
    fn decode_empty_object() {
        let val = decode("JSON:{}").unwrap();
        assert!(val.as_object().unwrap().is_empty());
    }
}
