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
            "Failed decoding sereal JSON payload in v1 subset decoder: {e}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_valid_json_object() {
        let val = decode("SRL1{\"key\":\"value\"}").unwrap();
        assert_eq!(val["key"], "value");
    }

    #[test]
    fn decode_valid_json_array() {
        let val = decode("SRL1[1,2,3]").unwrap();
        assert_eq!(val.as_array().unwrap().len(), 3);
    }

    #[test]
    fn decode_missing_prefix() {
        assert!(decode("{\"key\":\"value\"}").is_err());
    }

    #[test]
    fn decode_wrong_prefix() {
        assert!(decode("JSON:{\"key\":\"value\"}").is_err());
    }

    #[test]
    fn decode_invalid_json() {
        assert!(decode("SRL1{invalid}").is_err());
    }

    #[test]
    fn decode_nested() {
        let val = decode("SRL1{\"a\":{\"b\":42}}").unwrap();
        assert_eq!(val["a"]["b"], 42);
    }
}
