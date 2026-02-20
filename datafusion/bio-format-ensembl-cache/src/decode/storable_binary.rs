use crate::errors::{Result, exec_err};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub(crate) enum SValue {
    Null,
    Int(i64),
    String(String),
    Array(Vec<SValue>),
    Hash(BTreeMap<String, SValue>),
    Blessed { class: String, value: Box<SValue> },
}

impl SValue {
    pub(crate) fn unbless(&self) -> &SValue {
        match self {
            Self::Blessed { value, .. } => value.unbless(),
            _ => self,
        }
    }

    pub(crate) fn as_hash(&self) -> Option<&BTreeMap<String, SValue>> {
        match self.unbless() {
            Self::Hash(v) => Some(v),
            _ => None,
        }
    }

    pub(crate) fn as_array(&self) -> Option<&[SValue]> {
        match self.unbless() {
            Self::Array(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    pub(crate) fn as_i64(&self) -> Option<i64> {
        match self.unbless() {
            Self::Int(v) => Some(*v),
            Self::String(v) => v.parse().ok(),
            _ => None,
        }
    }

    pub(crate) fn as_bool(&self) -> Option<bool> {
        match self.unbless() {
            Self::Int(v) => Some(*v != 0),
            Self::String(v) => match v.to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" => Some(true),
                "0" | "false" | "no" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    pub(crate) fn as_string(&self) -> Option<String> {
        match self.unbless() {
            Self::String(v) => Some(v.clone()),
            Self::Int(v) => Some(v.to_string()),
            Self::Array(items) => {
                let values: Vec<String> = items.iter().filter_map(SValue::as_string).collect();
                if values.is_empty() {
                    None
                } else {
                    Some(values.join(","))
                }
            }
            _ => None,
        }
    }
}

/// Decodes a Perl Storable nstore payload (persistent format, `pst0`) into
/// a typed native Rust representation.
///
/// This parser intentionally supports only the opcode subset currently observed in
/// Ensembl VEP cache files.
pub(crate) fn decode_nstore(bytes: &[u8]) -> Result<SValue> {
    let mut parser = Parser::new(bytes);
    parser.consume_header()?;
    parser.parse_value()
}

pub(crate) fn canonical_json_string(value: &SValue) -> String {
    fn write_escaped_str(out: &mut String, value: &str) {
        out.push('"');
        for ch in value.chars() {
            match ch {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '\u{08}' => out.push_str("\\b"),
                '\u{0c}' => out.push_str("\\f"),
                c if c.is_control() => {
                    let code = c as u32;
                    out.push_str(&format!("\\u{code:04x}"));
                }
                c => out.push(c),
            }
        }
        out.push('"');
    }

    fn write_value(out: &mut String, value: &SValue) {
        match value {
            SValue::Null => out.push_str("null"),
            SValue::Int(v) => out.push_str(&v.to_string()),
            SValue::String(v) => write_escaped_str(out, v),
            SValue::Array(items) => {
                out.push('[');
                for (idx, item) in items.iter().enumerate() {
                    if idx > 0 {
                        out.push(',');
                    }
                    write_value(out, item);
                }
                out.push(']');
            }
            SValue::Hash(map) => {
                out.push('{');
                let mut first = true;
                for (k, v) in map {
                    if !first {
                        out.push(',');
                    }
                    first = false;
                    write_escaped_str(out, k);
                    out.push(':');
                    write_value(out, v);
                }
                out.push('}');
            }
            SValue::Blessed { class, value } => {
                out.push('{');
                write_escaped_str(out, "__class");
                out.push(':');
                write_escaped_str(out, class);
                out.push(',');
                write_escaped_str(out, "__value");
                out.push(':');
                write_value(out, value);
                out.push('}');
            }
        }
    }

    let mut out = String::new();
    write_value(&mut out, value);
    out
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
    classes: Vec<String>,
    refs: Vec<SValue>,
}

impl<'a> Parser<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            pos: 0,
            classes: Vec::new(),
            refs: Vec::new(),
        }
    }

    fn consume_header(&mut self) -> Result<()> {
        if self.bytes.len() < 6 {
            return Err(exec_err("Storable payload too short"));
        }

        if &self.bytes[0..4] != b"pst0" {
            return Err(exec_err(
                "Unsupported storable payload (missing pst0 header)",
            ));
        }

        // Byte 4 encodes (major << 1 | netorder). We currently support major=2 netorder=1.
        let version_and_order = self.bytes[4];
        let major = version_and_order >> 1;
        let netorder = (version_and_order & 0x01) == 1;
        let minor = self.bytes[5];

        if major != 2 {
            return Err(exec_err(format!(
                "Unsupported storable major version: {}",
                major
            )));
        }
        if !netorder {
            return Err(exec_err(
                "Unsupported non-network-order storable payload in v1 decoder",
            ));
        }

        // VEP 115 currently stores 2.10. Keep permissive on minor to tolerate small bumps.
        if minor < 7 {
            return Err(exec_err(format!(
                "Unsupported storable minor version: {}",
                minor
            )));
        }

        self.pos = 6;
        Ok(())
    }

    fn parse_value(&mut self) -> Result<SValue> {
        let opcode = self.read_u8()?;

        if opcode == 0x00 {
            let idx = self.read_u32()? as usize;
            return self.refs.get(idx).cloned().ok_or_else(|| {
                exec_err(format!(
                    "Invalid storable reference alias index {} at byte offset {}",
                    idx,
                    self.pos.saturating_sub(1)
                ))
            });
        }

        let slot = self.refs.len();
        self.refs.push(SValue::Null);

        let value = match opcode {
            0x01 => {
                let len = self.read_u32()? as usize;
                let bytes = self.read_bytes(len)?;
                SValue::String(String::from_utf8_lossy(bytes).to_string())
            }
            0x02 => {
                let len = self.read_u32()? as usize;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.parse_value()?);
                }
                SValue::Array(values)
            }
            0x03 => {
                let len = self.read_u32()? as usize;
                let mut map = BTreeMap::new();
                for _ in 0..len {
                    let value = self.parse_value()?;
                    let key = self.parse_hash_key()?;
                    map.insert(key, value);
                }
                SValue::Hash(map)
            }
            0x04 => {
                let value = self.parse_value()?;
                value
            }
            0x05 => SValue::Null,
            0x08 => {
                let byte = self.read_u8()? as i16;
                SValue::Int((byte - 128) as i64)
            }
            0x09 => SValue::Int(self.read_i32()? as i64),
            0x0a => {
                let len = self.read_u8()? as usize;
                let bytes = self.read_bytes(len)?;
                SValue::String(String::from_utf8_lossy(bytes).to_string())
            }
            0x11 => {
                let class_len = self.read_u8()? as usize;
                let class = String::from_utf8_lossy(self.read_bytes(class_len)?).to_string();
                let class_idx = self.classes.len();
                self.classes.push(class.clone());
                let value = self.parse_value()?;
                if self.classes.get(class_idx).is_none() {
                    return Err(exec_err("Storable class table corruption detected"));
                }
                SValue::Blessed {
                    class,
                    value: Box::new(value),
                }
            }
            0x12 => {
                let class_idx = self.read_u8()? as usize;
                let class = self.classes.get(class_idx).cloned().ok_or_else(|| {
                    exec_err(format!("Invalid storable class index: {}", class_idx))
                })?;
                let value = self.parse_value()?;
                SValue::Blessed {
                    class,
                    value: Box::new(value),
                }
            }
            // weak references can appear in cached object graphs; for tabular extraction
            // we treat them as regular references.
            0x1b => self.parse_value()?,
            other => {
                return Err(exec_err(format!(
                    "Unsupported storable opcode 0x{other:02x} at byte offset {}",
                    self.pos.saturating_sub(1)
                )));
            }
        };

        self.refs[slot] = value.clone();
        Ok(value)
    }

    fn parse_hash_key(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        Ok(String::from_utf8_lossy(bytes).to_string())
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.pos >= self.bytes.len() {
            return Err(exec_err("Unexpected EOF while decoding storable payload"));
        }
        let value = self.bytes[self.pos];
        self.pos += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.pos.saturating_add(len);
        if end > self.bytes.len() {
            return Err(exec_err(format!(
                "Unexpected EOF while decoding storable payload at offset {}",
                self.pos
            )));
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_simple_nstore_hash() {
        // Generated via Perl Storable::nstore({a=>1,b=>'x'})
        let hex = "70737430050b03000000020a0178000000016208810000000161";
        let bytes = hex::decode(hex).expect("hex decode");
        let decoded = decode_nstore(&bytes).expect("decode nstore");
        let obj = decoded.as_hash().expect("object");
        assert_eq!(obj.get("a").and_then(SValue::as_i64), Some(1));
        assert_eq!(
            obj.get("b").and_then(SValue::as_string),
            Some("x".to_string())
        );
    }

    #[test]
    fn resolves_reference_alias() {
        // Generated via Perl:
        // my $v = { q => "x" };
        // nstore({ a => $v, b => $v });
        let hex =
            "70737430050b03000000020403000000010a0178000000017100000001620400000000020000000161";
        let bytes = hex::decode(hex).expect("hex decode");
        let decoded = decode_nstore(&bytes).expect("decode nstore");
        let obj = decoded.as_hash().expect("object");
        let a = obj.get("a").and_then(SValue::as_hash).expect("a hash");
        let b = obj.get("b").and_then(SValue::as_hash).expect("b hash");
        assert_eq!(
            a.get("q").and_then(SValue::as_string),
            Some("x".to_string())
        );
        assert_eq!(
            b.get("q").and_then(SValue::as_string),
            Some("x".to_string())
        );
    }
}
