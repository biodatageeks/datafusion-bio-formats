use crate::errors::{Result, exec_err};
use std::collections::{BTreeMap, HashSet};
use std::io::Read;
use std::sync::Arc;

#[cfg(test)]
use std::io::Cursor;

#[derive(Debug, Clone)]
pub(crate) enum SValue {
    Null,
    Int(i64),
    String(Arc<str>),
    Array(Arc<Vec<SValue>>),
    Hash(Arc<BTreeMap<String, SValue>>),
    Blessed { class: Arc<str>, value: Arc<SValue> },
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
            Self::String(v) => Some(v.to_string()),
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
#[cfg(test)]
pub(crate) fn decode_nstore(bytes: &[u8]) -> Result<SValue> {
    decode_nstore_from_reader(Cursor::new(bytes))
}

#[cfg(test)]
pub(crate) fn decode_nstore_from_reader<R: Read>(reader: R) -> Result<SValue> {
    let mut parser = Parser::new(reader);
    parser.consume_header()?;
    parser.parse_value()
}

/// Collect all alias slots (`REFP` / opcode `0x00` targets) referenced in an
/// nstore payload. This is used by streaming decoders to retain only slots that
/// can be addressed by future aliases.
pub(crate) fn collect_nstore_alias_slots_from_reader<R: Read>(reader: R) -> Result<HashSet<usize>> {
    let mut collector = AliasCollector::new(reader);
    collector.consume_header()?;
    collector.collect_value()?;
    Ok(collector.alias_slots)
}

/// Streams top-level hash entries from an nstore payload.
///
/// Callback returns `Ok(true)` to continue or `Ok(false)` to stop early.
#[cfg(test)]
pub(crate) fn stream_nstore_top_hash_entries_from_reader<R, F>(
    reader: R,
    mut on_entry: F,
) -> Result<()>
where
    R: Read,
    F: FnMut(String, SValue) -> Result<bool>,
{
    let mut parser = Parser::new(reader);
    parser.consume_header()?;
    parser.stream_top_hash_entries(&mut on_entry)
}

/// Same as `stream_nstore_top_hash_entries_from_reader`, but keeps only slots
/// present in `alias_slots` in the internal reference table.
pub(crate) fn stream_nstore_top_hash_entries_with_alias_slots_from_reader<R, F>(
    reader: R,
    alias_slots: HashSet<usize>,
    mut on_entry: F,
) -> Result<()>
where
    R: Read,
    F: FnMut(String, SValue) -> Result<bool>,
{
    let mut parser = Parser::with_alias_slots(reader, alias_slots);
    parser.consume_header()?;
    parser.stream_top_hash_entries(&mut on_entry)
}

pub(crate) enum TopHashArrayEvent {
    Item(SValue),
    EntryKey(String),
}

/// Streams top-level hash entries where each value is expected to be an array.
///
/// Events are emitted in sequence as:
/// `Item(...)` repeated for each entry value, then `EntryKey(...)` for that entry.
/// Callback can return `Ok(false)` to stop early.
#[cfg(test)]
pub(crate) fn stream_nstore_top_hash_array_items_from_reader<R, F>(
    reader: R,
    mut on_event: F,
) -> Result<()>
where
    R: Read,
    F: FnMut(TopHashArrayEvent) -> Result<bool>,
{
    let mut parser = Parser::new(reader);
    parser.consume_header()?;
    parser.stream_top_hash_array_items(&mut on_event)
}

/// Same as `stream_nstore_top_hash_array_items_from_reader`, but keeps only
/// slots present in `alias_slots` in the internal reference table.
pub(crate) fn stream_nstore_top_hash_array_items_with_alias_slots_from_reader<R, F>(
    reader: R,
    alias_slots: HashSet<usize>,
    mut on_event: F,
) -> Result<()>
where
    R: Read,
    F: FnMut(TopHashArrayEvent) -> Result<bool>,
{
    let mut parser = Parser::with_alias_slots(reader, alias_slots);
    parser.consume_header()?;
    parser.stream_top_hash_array_items(&mut on_event)
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
                for (k, v) in map.iter() {
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

struct Parser<R> {
    reader: R,
    pos: usize,
    classes: Vec<Arc<str>>,
    refs: Vec<Option<SValue>>,
    alias_slots: Option<HashSet<usize>>,
}

impl<R: Read> Parser<R> {
    #[cfg(test)]
    fn new(reader: R) -> Self {
        Self {
            reader,
            pos: 0,
            classes: Vec::new(),
            refs: Vec::new(),
            alias_slots: None,
        }
    }

    fn with_alias_slots(reader: R, alias_slots: HashSet<usize>) -> Self {
        Self {
            reader,
            pos: 0,
            classes: Vec::new(),
            refs: Vec::new(),
            alias_slots: Some(alias_slots),
        }
    }

    fn push_ref(&mut self, value: Option<SValue>) -> usize {
        let slot = self.refs.len();
        self.refs.push(value);
        slot
    }

    fn should_retain_slot(&self, slot: usize) -> bool {
        match &self.alias_slots {
            Some(slots) => slots.contains(&slot),
            None => true,
        }
    }

    fn consume_header(&mut self) -> Result<()> {
        let mut header = [0u8; 6];
        self.read_exact_into(&mut header)?;

        if &header[0..4] != b"pst0" {
            return Err(exec_err(
                "Unsupported storable payload (missing pst0 header)",
            ));
        }

        // Byte 4 encodes (major << 1 | netorder). We currently support major=2 netorder=1.
        let version_and_order = header[4];
        let major = version_and_order >> 1;
        let netorder = (version_and_order & 0x01) == 1;
        let minor = header[5];

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

        Ok(())
    }

    fn stream_top_hash_entries<F>(&mut self, on_entry: &mut F) -> Result<()>
    where
        F: FnMut(String, SValue) -> Result<bool>,
    {
        let opcode = self.read_u8()?;
        if opcode != 0x03 {
            return Err(exec_err(format!(
                "Decoded storable root must be hash for streaming path (opcode 0x{opcode:02x})"
            )));
        }

        let root_slot = self.push_ref(Some(SValue::Null));
        let len = self.read_u32()? as usize;

        for _ in 0..len {
            let value = self.parse_value()?;
            let key = self.parse_hash_key()?;
            if !on_entry(key, value)? {
                break;
            }
        }

        self.refs[root_slot] = None;
        Ok(())
    }

    fn stream_top_hash_array_items<F>(&mut self, on_event: &mut F) -> Result<()>
    where
        F: FnMut(TopHashArrayEvent) -> Result<bool>,
    {
        let opcode = self.read_u8()?;
        if opcode != 0x03 {
            return Err(exec_err(format!(
                "Decoded storable root must be hash for streaming array path (opcode 0x{opcode:02x})"
            )));
        }

        let root_slot = self.push_ref(Some(SValue::Null));
        let len = self.read_u32()? as usize;

        for _ in 0..len {
            if !self
                .stream_array_value_items(&mut |item| on_event(TopHashArrayEvent::Item(item)))?
            {
                break;
            }

            let key = self.parse_hash_key()?;
            if !on_event(TopHashArrayEvent::EntryKey(key))? {
                break;
            }
        }

        self.refs[root_slot] = None;
        Ok(())
    }

    fn stream_array_value_items<F>(&mut self, on_item: &mut F) -> Result<bool>
    where
        F: FnMut(SValue) -> Result<bool>,
    {
        let opcode = self.read_u8()?;
        self.stream_array_value_items_with_opcode(opcode, on_item)
    }

    fn stream_array_value_items_with_opcode<F>(
        &mut self,
        opcode: u8,
        on_item: &mut F,
    ) -> Result<bool>
    where
        F: FnMut(SValue) -> Result<bool>,
    {
        if opcode == 0x00 {
            let idx = self.read_u32()? as usize;
            let aliased = self.resolve_alias(idx)?;
            let Some(items) = aliased.as_array() else {
                return Err(exec_err(format!(
                    "Expected aliased array value in streaming mode at byte offset {}",
                    self.pos.saturating_sub(1)
                )));
            };
            for item in items {
                if !on_item(item.clone())? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }

        let slot = self.push_ref(None);
        let keep_slot = self.should_retain_slot(slot);

        let should_continue = match opcode {
            0x02 => {
                let len = self.read_u32()? as usize;
                self.stream_inline_array_items(slot, len, keep_slot, on_item)?
            }
            0x04 | 0x1b => {
                let inner = self.read_u8()?;
                if inner == 0x02 {
                    let len = self.read_u32()? as usize;
                    self.stream_inline_array_items(slot, len, keep_slot, on_item)?
                } else {
                    let value = self.parse_value_from_opcode(inner)?;
                    let Some(items) = value.as_array() else {
                        return Err(exec_err(format!(
                            "Expected array value in streaming mode, found wrapped opcode 0x{inner:02x} at byte offset {}",
                            self.pos.saturating_sub(1)
                        )));
                    };
                    for item in items {
                        if !on_item(item.clone())? {
                            if keep_slot {
                                self.refs[slot] = Some(value.clone());
                            }
                            return Ok(false);
                        }
                    }
                    if keep_slot {
                        self.refs[slot] = Some(value);
                    }
                    true
                }
            }
            other => {
                let value = self.parse_value_from_opcode(other)?;
                let Some(items) = value.as_array() else {
                    return Err(exec_err(format!(
                        "Expected array value in streaming mode, found opcode 0x{other:02x} at byte offset {}",
                        self.pos.saturating_sub(1)
                    )));
                };
                for item in items {
                    if !on_item(item.clone())? {
                        if keep_slot {
                            self.refs[slot] = Some(value.clone());
                        }
                        return Ok(false);
                    }
                }
                if keep_slot {
                    self.refs[slot] = Some(value);
                }
                true
            }
        };

        Ok(should_continue)
    }

    fn stream_inline_array_items<F>(
        &mut self,
        slot: usize,
        len: usize,
        keep_slot: bool,
        on_item: &mut F,
    ) -> Result<bool>
    where
        F: FnMut(SValue) -> Result<bool>,
    {
        let mut retained_values = if keep_slot {
            Some(Vec::with_capacity(len))
        } else {
            None
        };

        for _ in 0..len {
            let item = self.parse_value()?;
            if let Some(values) = retained_values.as_mut() {
                values.push(item.clone());
            }
            if !on_item(item)? {
                if let Some(values) = retained_values {
                    self.refs[slot] = Some(SValue::Array(Arc::new(values)));
                }
                return Ok(false);
            }
        }

        if let Some(values) = retained_values {
            self.refs[slot] = Some(SValue::Array(Arc::new(values)));
        }

        Ok(true)
    }

    fn parse_value(&mut self) -> Result<SValue> {
        let opcode = self.read_u8()?;
        self.parse_value_from_opcode(opcode)
    }

    fn parse_value_from_opcode(&mut self, opcode: u8) -> Result<SValue> {
        if opcode == 0x00 {
            let idx = self.read_u32()? as usize;
            return self.resolve_alias(idx);
        }

        let slot = self.push_ref(None);

        let value = match opcode {
            0x01 => {
                let len = self.read_u32()? as usize;
                let bytes = self.read_bytes(len)?;
                SValue::String(String::from_utf8_lossy(&bytes).into_owned().into())
            }
            0x02 => {
                let len = self.read_u32()? as usize;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.parse_value()?);
                }
                SValue::Array(Arc::new(values))
            }
            0x03 => {
                let len = self.read_u32()? as usize;
                let mut map = BTreeMap::new();
                for _ in 0..len {
                    let value = self.parse_value()?;
                    let key = self.parse_hash_key()?;
                    map.insert(key, value);
                }
                SValue::Hash(Arc::new(map))
            }
            0x04 => self.parse_value()?,
            0x05 => SValue::Null,
            0x08 => {
                let byte = self.read_u8()? as i16;
                SValue::Int((byte - 128) as i64)
            }
            0x09 => SValue::Int(self.read_i32()? as i64),
            0x0a => {
                let len = self.read_u8()? as usize;
                let bytes = self.read_bytes(len)?;
                SValue::String(String::from_utf8_lossy(&bytes).into_owned().into())
            }
            0x11 => {
                let class_len = self.read_u8()? as usize;
                let class_bytes = self.read_bytes(class_len)?;
                let class: Arc<str> = String::from_utf8_lossy(&class_bytes).into_owned().into();
                self.classes.push(class.clone());
                let value = self.parse_value()?;
                SValue::Blessed {
                    class,
                    value: Arc::new(value),
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
                    value: Arc::new(value),
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

        if self.should_retain_slot(slot) {
            self.refs[slot] = Some(value.clone());
        }

        Ok(value)
    }

    fn resolve_alias(&self, idx: usize) -> Result<SValue> {
        match self.refs.get(idx) {
            Some(Some(value)) => Ok(value.clone()),
            Some(None) => Err(exec_err(format!(
                "Storable reference alias index {} at byte offset {} points to an evicted value in streaming mode",
                idx,
                self.pos.saturating_sub(1)
            ))),
            None => Err(exec_err(format!(
                "Invalid storable reference alias index {} at byte offset {}",
                idx,
                self.pos.saturating_sub(1)
            ))),
        }
    }

    fn parse_hash_key(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact_into(&mut buf)?;
        Ok(buf[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact_into(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let mut buf = [0u8; 4];
        self.read_exact_into(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0u8; len];
        self.read_exact_into(&mut bytes)?;
        Ok(bytes)
    }

    fn read_exact_into(&mut self, buf: &mut [u8]) -> Result<()> {
        self.reader.read_exact(buf).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                exec_err(format!(
                    "Unexpected EOF while decoding storable payload at offset {}",
                    self.pos
                ))
            } else {
                exec_err(format!(
                    "Failed reading storable payload at offset {}: {}",
                    self.pos, e
                ))
            }
        })?;
        self.pos = self.pos.saturating_add(buf.len());
        Ok(())
    }
}

struct AliasCollector<R> {
    reader: R,
    pos: usize,
    class_count: usize,
    alias_slots: HashSet<usize>,
}

impl<R: Read> AliasCollector<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            pos: 0,
            class_count: 0,
            alias_slots: HashSet::new(),
        }
    }

    fn consume_header(&mut self) -> Result<()> {
        let mut header = [0u8; 6];
        self.read_exact_into(&mut header)?;

        if &header[0..4] != b"pst0" {
            return Err(exec_err(
                "Unsupported storable payload (missing pst0 header)",
            ));
        }

        let version_and_order = header[4];
        let major = version_and_order >> 1;
        let netorder = (version_and_order & 0x01) == 1;
        let minor = header[5];

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
        if minor < 7 {
            return Err(exec_err(format!(
                "Unsupported storable minor version: {}",
                minor
            )));
        }

        Ok(())
    }

    fn collect_value(&mut self) -> Result<()> {
        let opcode = self.read_u8()?;

        if opcode == 0x00 {
            let idx = self.read_u32()? as usize;
            self.alias_slots.insert(idx);
            return Ok(());
        }

        match opcode {
            0x01 => {
                let len = self.read_u32()? as usize;
                self.skip_bytes(len)?;
            }
            0x02 => {
                let len = self.read_u32()? as usize;
                for _ in 0..len {
                    self.collect_value()?;
                }
            }
            0x03 => {
                let len = self.read_u32()? as usize;
                for _ in 0..len {
                    self.collect_value()?;
                    self.collect_hash_key()?;
                }
            }
            0x04 | 0x1b => self.collect_value()?,
            0x05 => {}
            0x08 => {
                self.skip_bytes(1)?;
            }
            0x09 => {
                self.skip_bytes(4)?;
            }
            0x0a => {
                let len = self.read_u8()? as usize;
                self.skip_bytes(len)?;
            }
            0x11 => {
                let class_len = self.read_u8()? as usize;
                self.skip_bytes(class_len)?;
                self.class_count = self.class_count.saturating_add(1);
                self.collect_value()?;
            }
            0x12 => {
                let class_idx = self.read_u8()? as usize;
                if class_idx >= self.class_count {
                    return Err(exec_err(format!(
                        "Invalid storable class index: {}",
                        class_idx
                    )));
                }
                self.collect_value()?;
            }
            other => {
                return Err(exec_err(format!(
                    "Unsupported storable opcode 0x{other:02x} at byte offset {}",
                    self.pos.saturating_sub(1)
                )));
            }
        }

        Ok(())
    }

    fn collect_hash_key(&mut self) -> Result<()> {
        let len = self.read_u32()? as usize;
        self.skip_bytes(len)
    }

    fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact_into(&mut buf)?;
        Ok(buf[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact_into(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    fn skip_bytes(&mut self, len: usize) -> Result<()> {
        let mut remaining = len;
        let mut scratch = [0u8; 8192];
        while remaining > 0 {
            let take = remaining.min(scratch.len());
            self.read_exact_into(&mut scratch[..take])?;
            remaining -= take;
        }
        Ok(())
    }

    fn read_exact_into(&mut self, buf: &mut [u8]) -> Result<()> {
        self.reader.read_exact(buf).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                exec_err(format!(
                    "Unexpected EOF while decoding storable payload at offset {}",
                    self.pos
                ))
            } else {
                exec_err(format!(
                    "Failed reading storable payload at offset {}: {}",
                    self.pos, e
                ))
            }
        })?;
        self.pos = self.pos.saturating_add(buf.len());
        Ok(())
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

    #[test]
    fn streams_top_hash_entries() {
        // Generated via Perl Storable::nstore({a=>1,b=>'x'})
        let hex = "70737430050b03000000020a0178000000016208810000000161";
        let bytes = hex::decode(hex).expect("hex decode");

        let mut seen = BTreeMap::new();
        stream_nstore_top_hash_entries_from_reader(Cursor::new(bytes), |key, value| {
            seen.insert(key, value.as_string());
            Ok(true)
        })
        .expect("stream entries");

        assert_eq!(seen.get("a").cloned().flatten(), Some("1".to_string()));
        assert_eq!(seen.get("b").cloned().flatten(), Some("x".to_string()));
    }

    #[test]
    fn streams_immediate_sibling_alias_entries() {
        // Generated via Perl:
        // my $v = { q => "x" };
        // nstore({ a => $v, b => $v });
        let hex =
            "70737430050b03000000020403000000010a0178000000017100000001620400000000020000000161";
        let bytes = hex::decode(hex).expect("hex decode");

        let mut keys = Vec::new();
        stream_nstore_top_hash_entries_from_reader(Cursor::new(bytes), |key, value| {
            keys.push(key);
            assert!(value.as_hash().is_some());
            Ok(true)
        })
        .expect("stream entries");

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"a".to_string()));
        assert!(keys.contains(&"b".to_string()));
    }

    #[test]
    fn streams_top_hash_array_items() {
        // Generated shape: { a => [1] } with an indirection opcode before the array.
        let hex = "70737430050b030000000104020000000108810000000161";
        let bytes = hex::decode(hex).expect("hex decode");

        let mut seen_items = Vec::new();
        let mut seen_keys = Vec::new();
        stream_nstore_top_hash_array_items_from_reader(Cursor::new(bytes), |event| {
            match event {
                TopHashArrayEvent::Item(item) => seen_items.push(item.as_i64()),
                TopHashArrayEvent::EntryKey(key) => seen_keys.push(key),
            }
            Ok(true)
        })
        .expect("stream array events");

        assert_eq!(seen_items, vec![Some(1)]);
        assert_eq!(seen_keys, vec!["a".to_string()]);
    }

    #[test]
    fn collects_alias_slots() {
        // Generated via Perl:
        // my $v = { q => "x" };
        // nstore({ a => $v, b => $v });
        let hex =
            "70737430050b03000000020403000000010a0178000000017100000001620400000000020000000161";
        let bytes = hex::decode(hex).expect("hex decode");

        let slots = collect_nstore_alias_slots_from_reader(Cursor::new(bytes)).expect("collect");
        assert!(slots.contains(&2));
    }
}
