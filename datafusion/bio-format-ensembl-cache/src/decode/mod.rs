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
