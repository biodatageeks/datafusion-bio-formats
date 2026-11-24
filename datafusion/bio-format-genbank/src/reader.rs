use crate::Filter;
use crate::batch::Batch;
use crate::location::{Location, decode};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{DataFusionError, Result};
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(crate) struct Record {
    pub name: String,
    pub length: i64,
    pub header: HashMap<&'static str, String>,
    pub taxonomy: String,
    pub sequence: Option<String>,
    circular: bool,
}
#[derive(Default)]
pub(crate) struct Feature {
    pub kind: String,
    pub location: String,
    pub qualifiers: IndexMap<String, Vec<Option<String>>>,
    matched: bool,
    saw_qualifier: bool,
    // Outer option marks an unquoted value. The index is absent for skipped keys.
    unquoted: Option<Option<usize>>,
}
#[derive(Default, PartialEq)]
enum Section {
    #[default]
    Outside,
    Header,
    Features,
    Origin,
    Contig,
}

/// Keeping quote state separate from retained text allows a skipped translation
/// to cross any number of input lines without building its value.
struct Pending {
    key: Option<String>,
    text: String,
    translation: bool,
}

pub(crate) struct Parser {
    section: Section,
    record: Record,
    feature: Option<Feature>,
    pending: Option<Pending>,
    columns: HashSet<String>,
    filters: Vec<Filter>,
    features: bool,
    zero: bool,
    record_matches: bool,
    needs_location: bool,
    derived: bool,
    continuation: Option<&'static str>,
}

impl Parser {
    pub fn new(schema: &Schema, features: bool, zero: bool, filters: Vec<Filter>) -> Self {
        let columns: HashSet<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let derived = ["parts", "start", "end", "strand"]
            .iter()
            .any(|&n| columns.contains(n));
        let needs_location = derived || columns.contains("location");
        Self {
            section: Section::Outside,
            record: Record::default(),
            feature: None,
            pending: None,
            columns,
            filters,
            features,
            zero,
            record_matches: true,
            needs_location,
            derived,
            continuation: None,
        }
    }
    pub fn name(&self) -> &str {
        &self.record.name
    }
    pub fn complete(&self) -> bool {
        self.section == Section::Outside
    }
    fn start(&mut self, s: &str) -> Result<()> {
        if !self.complete() {
            return Err(err("missing // before LOCUS"));
        }
        let mut fields = s.split_ascii_whitespace().skip(1);
        let name = fields
            .next()
            .ok_or_else(|| err("missing LOCUS identifier"))?;
        let length = fields
            .next()
            .and_then(|n| n.parse::<i64>().ok())
            .filter(|&n| n >= 0)
            .ok_or_else(|| err("invalid LOCUS length"))?;
        let protein = match fields.next() {
            Some("aa") => true,
            Some("bp") => false,
            _ => return Err(err("expected LOCUS length in bp or aa")),
        };
        self.record = Record {
            name: name.into(),
            length,
            ..Default::default()
        };
        self.record_matches = self
            .filters
            .iter()
            .filter(|f| f.column != "type")
            .all(|f| f.value == name);
        if protein
            && !self.features
            && self.record_matches
            && self.columns.contains("molecule_type")
        {
            self.record.header.insert("molecule_type", "protein".into());
        }
        for token in fields {
            let key = if matches!(token, "linear" | "circular") {
                self.record.circular = token == "circular";
                "topology"
            } else if token.len() == 11
                && token.as_bytes()[2] == b'-'
                && token.as_bytes()[6] == b'-'
            {
                "date"
            } else if token.contains("DNA") || token.contains("RNA") {
                "molecule_type"
            } else if token.len() == 3 && token.bytes().all(|c| c.is_ascii_uppercase()) {
                "division"
            } else {
                continue;
            };
            if !self.features && self.record_matches && self.columns.contains(key) {
                self.record.header.insert(key, token.into());
            }
        }
        self.section = Section::Header;
        self.continuation = None;
        Ok(())
    }
    fn header(&mut self, s: &str) {
        if self.features || !self.record_matches {
            return;
        }
        let key = s.get(..12).unwrap_or(s).trim();
        let value = s.get(12..).unwrap_or("").trim();
        if key.is_empty() {
            match self.continuation {
                Some("taxonomy") if self.columns.contains("taxonomy") => {
                    append_words(&mut self.record.taxonomy, value)
                }
                Some("definition") => {
                    append_words(self.record.header.entry("definition").or_default(), value)
                }
                _ => {}
            }
            return;
        }
        let key = match key {
            "DEFINITION" => "definition",
            "ACCESSION" => "accession",
            "VERSION" => "version",
            "ORGANISM" => "organism",
            _ => {
                self.continuation = None;
                return;
            }
        };
        self.continuation = match key {
            "organism" => Some("taxonomy"),
            "definition" if self.columns.contains(key) => Some(key),
            _ => None,
        };
        if self.columns.contains(key) {
            let value = if matches!(key, "version" | "accession") {
                value.split_whitespace().next().unwrap_or("")
            } else {
                value
            };
            self.record.header.insert(key, value.to_string());
        }
    }
    fn finish_feature(&mut self, batch: &mut Batch) -> Result<()> {
        if self.pending.is_some() {
            return Err(err("unterminated qualifier"));
        }
        if let Some(f) = self.feature.take()
            && f.matched
        {
            let loc = if self.derived {
                decode(
                    &f.location,
                    self.zero,
                    self.record.circular,
                    self.record.length,
                )
                .map_err(err)?
            } else {
                Location::default()
            };
            batch.push(&self.record, Some(&f), &loc)?;
        }
        Ok(())
    }
    fn qualifier(&mut self, content: &str) -> Result<()> {
        let f = self
            .feature
            .as_mut()
            .ok_or_else(|| err("qualifier outside feature"))?;
        if self.pending.is_none() {
            let (key, value) = content
                .strip_prefix('/')
                .ok_or_else(|| err("expected qualifier"))?
                .split_once('=')
                .map_or((&content[1..], None), |(k, v)| (k, Some(v)));
            if key.is_empty() || key.bytes().any(|c| c.is_ascii_whitespace()) {
                return Err(err("invalid qualifier name"));
            }
            f.saw_qualifier = true;
            f.unquoted = None;
            let keep = f.matched
                && (self.columns.contains("qualifiers")
                    || (self.columns.contains(key) && !f.qualifiers.contains_key(key)));
            match value.and_then(|v| v.strip_prefix('"')) {
                Some(fragment) => {
                    self.pending = Some(Pending {
                        key: keep.then(|| key.to_owned()),
                        text: String::new(),
                        translation: key == "translation",
                    });
                    return self.quoted(fragment);
                }
                None => {
                    let index = if keep {
                        let entry = f.qualifiers.entry(key.into());
                        let i = entry.index();
                        let values = entry.or_default();
                        values.push(value.map(str::to_owned));
                        Some(i)
                    } else {
                        None
                    };
                    f.unquoted = value.map(|_| index);
                }
            }
        } else {
            return self.quoted(content);
        }
        Ok(())
    }
    fn quoted(&mut self, content: &str) -> Result<()> {
        let pending = self.pending.as_mut().expect("quoted qualifier");
        let mut rest = content;
        if pending.key.is_some()
            && !pending.translation
            && !pending.text.is_empty()
            && !rest.is_empty()
        {
            pending.text.push(' ');
        }
        loop {
            let (fragment, tail) = rest
                .split_once('"')
                .map_or((rest, None), |(a, b)| (a, Some(b)));
            if pending.key.is_some() {
                if pending.translation {
                    pending
                        .text
                        .extend(fragment.chars().filter(|c| !c.is_ascii_whitespace()));
                } else {
                    pending.text.push_str(fragment);
                }
            }
            match tail {
                None => return Ok(()),
                Some(t) if t.starts_with('"') => {
                    if pending.key.is_some() {
                        pending.text.push('"');
                    }
                    rest = &t[1..];
                }
                Some(t) if t.trim().is_empty() => {
                    let p = self.pending.take().unwrap();
                    if let Some(key) = p.key {
                        self.feature
                            .as_mut()
                            .unwrap()
                            .qualifiers
                            .entry(key)
                            .or_default()
                            .push(Some(p.text));
                    }
                    return Ok(());
                }
                _ => return Err(err("unexpected text after qualifier quote")),
            }
        }
    }
    fn feature_line(&mut self, s: &str, batch: &mut Batch) -> Result<()> {
        if !self.features {
            return Ok(());
        }
        if !s.starts_with("     ") {
            return Err(err("invalid feature indentation"));
        }
        let kind = s
            .get(5..21)
            .ok_or_else(|| err("invalid feature columns"))?
            .trim();
        let content = s[21..].trim();
        if !kind.is_empty() {
            self.finish_feature(batch)?;
            if content.is_empty() {
                return Err(err("missing feature location"));
            }
            let matched = self.record_matches
                && self
                    .filters
                    .iter()
                    .filter(|f| f.column == "type")
                    .all(|f| f.value == kind);
            self.feature = Some(Feature {
                kind: kind.into(),
                matched,
                ..Default::default()
            });
        } else if self.pending.is_some() || content.starts_with('/') {
            return self.qualifier(content);
        }
        let f = self
            .feature
            .as_mut()
            .ok_or_else(|| err("continuation outside feature"))?;
        if let Some(index) = f.unquoted {
            if let Some(index) = index {
                let (_, values) = f
                    .qualifiers
                    .get_index_mut(index)
                    .expect("retained qualifier");
                if let Some(Some(value)) = values.last_mut() {
                    append_words(value, content);
                }
            }
            return Ok(());
        }
        if f.saw_qualifier {
            return Err(err("unexpected continuation after qualifier"));
        }
        if f.matched && self.needs_location {
            f.location.push_str(content);
        }
        Ok(())
    }
    pub fn line(&mut self, s: &str, batch: &mut Batch) -> Result<()> {
        if s.trim().is_empty() {
            return Ok(());
        }
        if s.split_ascii_whitespace().next() == Some("LOCUS") && s.starts_with("LOCUS") {
            return self.start(s);
        }
        if self.complete() {
            return Err(err("expected LOCUS"));
        }
        if s.trim_end() == "//" {
            self.finish_feature(batch)?;
            if !self.features && self.record_matches {
                batch.push(&self.record, None, &Location::default())?;
            }
            self.section = Section::Outside;
            return Ok(());
        }
        let keyword = s.split_once(char::is_whitespace).map_or(s, |(k, _)| k);
        match keyword {
            "FEATURES" => self.section = Section::Features,
            "ORIGIN" | "CONTIG" => {
                self.finish_feature(batch)?;
                self.section = if keyword == "ORIGIN" {
                    Section::Origin
                } else {
                    Section::Contig
                };
                if self.section == Section::Origin
                    && !self.features
                    && self.record_matches
                    && self.columns.contains("sequence")
                {
                    self.record.sequence = Some(String::new());
                }
            }
            _ => match self.section {
                Section::Header => self.header(s),
                Section::Features if !s.starts_with(char::is_whitespace) => {
                    self.finish_feature(batch)?;
                    self.section = Section::Header;
                    self.header(s);
                }
                Section::Features => self.feature_line(s, batch)?,
                Section::Origin => {
                    if let Some(seq) = &mut self.record.sequence {
                        seq.extend(
                            s.bytes()
                                .filter(u8::is_ascii_alphabetic)
                                .map(|c| c.to_ascii_uppercase() as char),
                        );
                    }
                }
                _ => {}
            },
        }
        Ok(())
    }
}
fn append_words(out: &mut String, s: &str) {
    if !out.is_empty() && !s.is_empty() {
        out.push(' ');
    }
    out.push_str(s);
}
fn err(s: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(s.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::schema;

    #[test]
    fn reject_unknown_or_missing_locus_unit() {
        let all = schema(false, None, true).unwrap();
        let mut parser = Parser::new(&all, false, true, vec![]);
        for locus in ["LOCUS A 12", "LOCUS A 12 kb"] {
            assert!(
                parser
                    .start(locus)
                    .unwrap_err()
                    .to_string()
                    .contains("bp or aa")
            );
        }
    }

    #[test]
    fn narrow_projection_retains_no_translation_or_sequence_bytes() {
        for features in [true, false] {
            let all = schema(features, None, true).unwrap();
            let selected = std::sync::Arc::new(
                all.project(&[all
                    .index_of(if features { "start" } else { "length" })
                    .unwrap()])
                    .unwrap(),
            );
            let mut parser = Parser::new(&selected, features, true, vec![]);
            let mut batch = Batch::new(selected);
            for line in [
                "LOCUS       A 100 bp DNA",
                "FEATURES             Location/Qualifiers",
                "     CDS             1..4",
                "                     /translation=\"AAAA",
            ] {
                parser.line(line, &mut batch).unwrap();
            }
            let protein_line = format!("                     {}", "A".repeat(4096));
            for _ in 0..100 {
                parser.line(&protein_line, &mut batch).unwrap();
            }
            if features {
                let pending = parser.pending.as_ref().unwrap();
                assert_eq!(pending.text.capacity(), 0);
                assert!(parser.feature.as_ref().unwrap().qualifiers.is_empty());
            }
            for line in ["                     A\"", "ORIGIN", "        1 acgt", "//"] {
                parser.line(line, &mut batch).unwrap();
            }
            assert!(parser.record.sequence.is_none());
            assert_eq!((batch.rows, batch.bytes), (1, 0));
        }
    }
}
