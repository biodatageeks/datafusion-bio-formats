use crate::errors::{Result, exec_err};
use serde_json::Value;

pub(crate) mod sereal;
pub(crate) mod storable;
pub(crate) mod storable_binary;

pub(crate) fn decode_payload(serializer_type: &str, payload: &str) -> Result<Value> {
    match serializer_type {
        "storable" => storable::decode(payload),
        "sereal" => sereal::decode(payload),
        other => Err(exec_err(format!("Unknown serializer type: {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_storable() {
        let val = decode_payload("storable", "JSON:{\"x\":1}").unwrap();
        assert_eq!(val["x"], 1);
    }

    #[test]
    fn dispatch_sereal() {
        let val = decode_payload("sereal", "SRL1{\"x\":1}").unwrap();
        assert_eq!(val["x"], 1);
    }

    #[test]
    fn dispatch_unknown_errors() {
        assert!(decode_payload("msgpack", "data").is_err());
    }
}
