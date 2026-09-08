//! Line-oriented Stockholm parser.
//!
//! Rules (Easel behaviour is the tie-breaker where the Sonnhammer page is silent):
//!
//! * The first non-blank line of the input MUST be `# STOCKHOLM 1.0`; later
//!   alignments in the same input may start with a header or directly with data.
//! * `//` ends an alignment. Input that ends without `//` still yields the
//!   pending alignment (`terminated == false`).
//! * `#=GF` / `#=GS` are one record per line; `#=GC` / `#=GR` values and
//!   sequence lines are concatenated across interleaved blocks by (name, feature).
//! * Any other `#` line is a comment. Blank lines are ignored.
//! * Rows are ordered by the first appearance of a sequence name in any line.

use crate::storage::{LineSource, read_line};
use datafusion::common::DataFusionError;
use std::collections::HashMap;

/// One sequence row of an alignment.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SequenceRecord {
    /// Sequence name, verbatim (`name/start-end` is not split).
    pub name: String,
    /// Aligned sequence, concatenated across blocks. Empty when the reader was
    /// opened in annotations-only mode.
    pub sequence: String,
    /// `#=GS` annotations in file order.
    pub gs: Vec<(String, String)>,
    /// `#=GR` annotations, each concatenated across blocks.
    pub gr: Vec<(String, String)>,
}

/// One alignment (`# STOCKHOLM 1.0` … `//`).
#[derive(Clone, Debug, Default)]
pub struct Alignment {
    /// 0-based position of this alignment in the input.
    pub ordinal: u64,
    /// `#=GF` annotations in file order (repeats preserved).
    pub gf: Vec<(String, String)>,
    /// `#=GC` annotations, each concatenated across blocks.
    pub gc: Vec<(String, String)>,
    /// Sequence rows in first-appearance order.
    pub sequences: Vec<SequenceRecord>,
    /// Whether a `//` terminator was seen.
    pub terminated: bool,
    /// Length of the first sequence (tracked even in annotations-only mode).
    first_sequence_len: usize,
}

impl Alignment {
    /// `#=GF ID`, else `#=GF AC`, else the ordinal as a string.
    pub fn id(&self) -> String {
        for key in ["ID", "AC"] {
            if let Some((_, v)) = self.gf.iter().find(|(k, _)| k == key) {
                return v.clone();
            }
        }
        self.ordinal.to_string()
    }

    /// Number of sequence rows.
    pub fn n_sequences(&self) -> usize {
        self.sequences.len()
    }

    /// Number of aligned columns, taken from the first sequence.
    pub fn alignment_length(&self) -> usize {
        self.first_sequence_len
    }
}

/// Streaming reader yielding one [`Alignment`] at a time.
pub struct StockholmReader {
    src: LineSource,
    path: String,
    line: Vec<u8>,
    line_no: u64,
    next_ordinal: u64,
    seen_alignment: bool,
    collect_sequences: bool,
    pushed_back: bool,
}

fn split_ws(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    match s.find([' ', '\t']) {
        Some(i) => (&s[..i], s[i..].trim_start()),
        None => (s, ""),
    }
}

fn append_or_push(list: &mut Vec<(String, String)>, key: &str, value: &str) {
    match list.iter_mut().find(|(k, _)| k == key) {
        Some((_, v)) => v.push_str(value),
        None => list.push((key.to_string(), value.to_string())),
    }
}

impl StockholmReader {
    /// Creates a reader over `src`. `first_ordinal` seeds alignment numbering
    /// (non-zero when reading a partition that does not start at the file's
    /// first alignment). With `collect_sequences == false` rows are still
    /// created but sequence text is not stored.
    pub fn new(src: LineSource, path: String, first_ordinal: u64, collect_sequences: bool) -> Self {
        Self {
            src,
            path,
            line: Vec::new(),
            line_no: 0,
            next_ordinal: first_ordinal,
            seen_alignment: false,
            collect_sequences,
            pushed_back: false,
        }
    }

    fn err(&self, msg: impl std::fmt::Display) -> DataFusionError {
        DataFusionError::Execution(format!("{}:{}: {}", self.path, self.line_no, msg))
    }

    /// Reads the next line into `self.line`; returns `false` at EOF.
    async fn next_line(&mut self) -> Result<bool, DataFusionError> {
        if self.pushed_back {
            self.pushed_back = false;
            return Ok(true);
        }
        let more = read_line(&mut self.src, &mut self.line)
            .await
            .map_err(|e| self.err(format!("read error: {e}")))?;
        if more {
            self.line_no += 1;
        }
        Ok(more)
    }

    fn line_str(&self) -> Result<&str, DataFusionError> {
        std::str::from_utf8(&self.line).map_err(|e| self.err(format!("line is not UTF-8: {e}")))
    }

    /// Returns the next alignment, or `None` at end of input.
    pub async fn next_alignment(&mut self) -> Result<Option<Alignment>, DataFusionError> {
        // Locate the start: skip blanks, require a header for the first alignment.
        loop {
            if !self.next_line().await? {
                return Ok(None);
            }
            let text = self.line_str()?;
            if text.trim().is_empty() {
                continue;
            }
            if text.starts_with("# STOCKHOLM") {
                break;
            }
            if !self.seen_alignment {
                return Err(self.err(
                    "expected a '# STOCKHOLM 1.0' header line at the start of the Stockholm input",
                ));
            }
            // Lenient: a later alignment without its own header line.
            self.pushed_back = true;
            break;
        }
        self.seen_alignment = true;

        let mut alignment = Alignment {
            ordinal: self.next_ordinal,
            ..Default::default()
        };
        self.next_ordinal += 1;
        let mut index: HashMap<String, usize> = HashMap::new();

        loop {
            if !self.next_line().await? {
                break;
            }
            let text = self.line_str()?;
            let trimmed = text.trim_end();
            if trimmed.is_empty() {
                continue;
            }
            if trimmed == "//" {
                alignment.terminated = true;
                break;
            }
            if let Some(rest) = trimmed.strip_prefix("#=GF") {
                let (feature, value) = split_ws(rest);
                alignment.gf.push((feature.to_string(), value.to_string()));
            } else if let Some(rest) = trimmed.strip_prefix("#=GC") {
                let (feature, value) = split_ws(rest);
                append_or_push(&mut alignment.gc, feature, value);
            } else if let Some(rest) = trimmed.strip_prefix("#=GS") {
                let (name, rest) = split_ws(rest);
                let (feature, value) = split_ws(rest);
                let row = row_index(&mut alignment, &mut index, name);
                alignment.sequences[row]
                    .gs
                    .push((feature.to_string(), value.to_string()));
            } else if let Some(rest) = trimmed.strip_prefix("#=GR") {
                let (name, rest) = split_ws(rest);
                let (feature, value) = split_ws(rest);
                let row = row_index(&mut alignment, &mut index, name);
                append_or_push(&mut alignment.sequences[row].gr, feature, value);
            } else if trimmed.starts_with("# STOCKHOLM") {
                // A new alignment began without a terminator: emit what we have.
                self.pushed_back = true;
                break;
            } else if trimmed.starts_with('#') {
                continue;
            } else {
                let (name, seq) = split_ws(trimmed);
                let row = row_index(&mut alignment, &mut index, name);
                if row == 0 {
                    alignment.first_sequence_len += seq.len();
                }
                if self.collect_sequences {
                    alignment.sequences[row].sequence.push_str(seq);
                }
            }
        }

        if !alignment.terminated {
            log::warn!(
                "{}: alignment {} ended without a '//' terminator",
                self.path,
                alignment.id()
            );
        }
        Ok(Some(alignment))
    }
}

fn row_index(alignment: &mut Alignment, index: &mut HashMap<String, usize>, name: &str) -> usize {
    if let Some(&i) = index.get(name) {
        return i;
    }
    let i = alignment.sequences.len();
    alignment.sequences.push(SequenceRecord {
        name: name.to_string(),
        ..Default::default()
    });
    index.insert(name.to_string(), i);
    i
}
