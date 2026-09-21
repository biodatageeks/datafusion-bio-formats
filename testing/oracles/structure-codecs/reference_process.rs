//! Test-only client for the pinned external reference; never linked into readers.
use crate::error;
use datafusion::common::Result;
use serde_json::Value;

pub fn text(value: &Value) -> Result<String> {
    let hex = value.as_str().expect("reference hex string");
    let bytes = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect::<Vec<_>>();
    String::from_utf8(bytes).map_err(|e| error(format!("reference UTF-8: {e}")))
}

pub fn query(mode: &str, data: &[u8], max_atoms: usize) -> Result<Value> {
    let executable = std::env::var_os("BIO_CODEC_REFERENCE")
        .expect("run testing/oracles/structure-codecs/check_provider_reference.py first");
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("input");
    std::fs::write(&input, data)?;
    let output = std::process::Command::new(executable)
        .arg(mode)
        .arg(input)
        .arg(max_atoms.to_string())
        .output()?;
    assert!(output.status.success(), "reference failed: {output:?}");
    let result: Value = serde_json::from_slice(&output.stdout).expect("reference JSON");
    if result.get("error_hex").is_some() {
        return Err(error(text(&result["error_hex"])?));
    }
    Ok(result)
}
