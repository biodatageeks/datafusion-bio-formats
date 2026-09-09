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

/// The only first line this reader accepts, trailing whitespace aside. Easel
/// requires exactly this spelling: it rejects `# STOCKHOLM1.0` and
/// `# STOCKHOLM  1.0` as readily as `# STOCKHOLM 2.0`.
const STOCKHOLM_HEADER: &str = "# STOCKHOLM 1.0";
/// Anything opening with this is meant to be the header, so a mismatch is
/// reported as an unsupported header rather than treated as a comment.
const STOCKHOLM_HEADER_PREFIX: &str = "# STOCKHOLM";

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

/// Which markup an alignment-level annotation came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnotationKind {
    /// `#=GF`: per-file (per-alignment) free text.
    Gf,
    /// `#=GC`: per-column, one character per alignment column.
    Gc,
}

impl AnnotationKind {
    /// The two-letter label used in the long-format output.
    pub fn as_str(&self) -> &'static str {
        match self {
            AnnotationKind::Gf => "GF",
            AnnotationKind::Gc => "GC",
        }
    }
}

/// One alignment-level annotation line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileAnnotation {
    /// `#=GF` or `#=GC`.
    pub kind: AnnotationKind,
    /// The feature name, e.g. `ID`, `DR`, `SS_cons`.
    pub feature: String,
    /// Free text for `#=GF`; for `#=GC` the value concatenated across blocks.
    pub value: String,
}

/// One alignment (`# STOCKHOLM 1.0` … `//`).
#[derive(Clone, Debug, Default)]
pub struct Alignment {
    /// 0-based position of this alignment in the input.
    pub ordinal: u64,
    /// `#=GF` and `#=GC` annotations in one list, in file order, so a consumer
    /// can reconstruct the alignment header as written. `#=GF` repeats are kept
    /// as separate entries; a `#=GC` feature appears once, at the position of
    /// its first block, with the later blocks appended to its value.
    pub annotations: Vec<FileAnnotation>,
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
            if let Some(a) = self
                .annotations
                .iter()
                .find(|a| a.kind == AnnotationKind::Gf && a.feature == key)
            {
                return a.value.clone();
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

/// Which of an alignment's bulky parts the caller will actually read.
///
/// Sequence text, `#=GC` tracks and `#=GR` tracks are each alignment-width, so
/// materialising one a caller never looks at costs memory proportional to the
/// alignment. Names, `#=GS` values and `#=GF` lines are short and always kept —
/// `#=GF` also carries the identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Collect {
    /// Aligned sequence text.
    pub sequences: bool,
    /// `#=GC` per-column tracks. Only the annotation reader exposes these; the
    /// table schema has no column for them.
    pub column_annotations: bool,
    /// `#=GR` per-residue tracks, exposed as the `gr` column.
    pub residue_annotations: bool,
}

impl Collect {
    /// A table scan projecting `sequence` and `gr`.
    pub const ROWS: Self = Self {
        sequences: true,
        column_annotations: false,
        residue_annotations: true,
    };

    /// What [`read_stockholm_annotations`](crate::read_stockholm_annotations)
    /// needs: alignment-level annotations and the row count, nothing wider.
    pub const ANNOTATIONS: Self = Self {
        sequences: false,
        column_annotations: true,
        residue_annotations: false,
    };
}

/// Streaming reader yielding one [`Alignment`] at a time.
pub struct StockholmReader {
    src: LineSource,
    path: String,
    line: Vec<u8>,
    line_no: u64,
    next_ordinal: u64,
    seen_alignment: bool,
    collect: Collect,
    pushed_back: bool,
}

fn split_ws(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    match s.find([' ', '\t']) {
        Some(i) => (&s[..i], s[i..].trim_start()),
        None => (s, ""),
    }
}

/// The four markup kinds this reader understands. Any other `#=G…` label is an
/// unknown extension, which the parse loop ignores like any other comment.
const MARKUP_PREFIXES: [&str; 4] = ["#=GF", "#=GS", "#=GC", "#=GR"];

/// Whether `line` ends an alignment. Easel accepts surrounding whitespace, so
/// `  //` terminates just as `//` does.
///
/// Shared with the partition-boundary scan: if planning split on a line the
/// parse loop read as sequence data, the rows after it would change alignment
/// with the partition count.
pub fn is_terminator(line: &str) -> bool {
    line.trim() == "//"
}

/// Whether `line` is a comment the start search skips between alignments: a
/// `#` line that is neither known markup nor a header (`# STOCKHOLM…`).
///
/// Only the four supported markup labels are reserved. An unrecognised one such
/// as `#=GX` is a comment here because the parse loop treats it as one, and
/// disagreeing would emit an empty alignment and shift every later ordinal.
///
/// Shared with the partition-boundary scan so the bytes it calls an alignment
/// and the bytes this reader calls an alignment cannot drift apart.
pub fn is_skippable_comment(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.starts_with('#')
        && !MARKUP_PREFIXES.iter().any(|p| trimmed.starts_with(p))
        && !trimmed.starts_with(STOCKHOLM_HEADER_PREFIX)
}

/// Whether `text` holds anything the reader would treat as an alignment, as
/// opposed to only blank lines and ordinary comments.
pub fn has_alignment_content(text: &str) -> bool {
    text.lines()
        .any(|line| !line.trim().is_empty() && !is_skippable_comment(line))
}

fn append_or_push(list: &mut Vec<(String, String)>, key: &str, value: &str) {
    match list.iter_mut().find(|(k, _)| k == key) {
        Some((_, v)) => v.push_str(value),
        None => list.push((key.to_string(), value.to_string())),
    }
}

impl StockholmReader {
    /// Creates a reader over `src` positioned at the start of the input,
    /// materialising the parts named by `collect`.
    pub fn new(src: LineSource, path: String, collect: Collect) -> Self {
        Self::new_at(src, path, 0, true, collect)
    }

    /// Creates a reader over one slice of an input.
    ///
    /// `first_ordinal` seeds alignment numbering, and `at_input_start` says
    /// whether `src` begins at the first alignment of the whole input. Only
    /// there is a `# STOCKHOLM 1.0` header compulsory: a partition that starts
    /// mid-file may legitimately open on an alignment that omits its header,
    /// and rejecting it would make the result depend on `target_partitions`.
    pub fn new_at(
        src: LineSource,
        path: String,
        first_ordinal: u64,
        at_input_start: bool,
        collect: Collect,
    ) -> Self {
        Self {
            src,
            path,
            line: Vec::new(),
            line_no: 0,
            next_ordinal: first_ordinal,
            seen_alignment: !at_input_start,
            collect,
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
            // Easel tolerates trailing whitespace around the header and nothing
            // else — leading whitespace is rejected — so the comparison sees a
            // line trimmed only at the end.
            let line = text.trim_end();
            let trimmed = line.trim_start();
            if trimmed.is_empty() {
                continue;
            }
            if line == STOCKHOLM_HEADER {
                break;
            }
            if trimmed.starts_with(STOCKHOLM_HEADER_PREFIX) {
                return Err(self.err(format!(
                    "unsupported Stockholm header {line:?}; expected '{STOCKHOLM_HEADER}'"
                )));
            }
            if !self.seen_alignment {
                return Err(self.err(
                    "expected a '# STOCKHOLM 1.0' header line at the start of the Stockholm input",
                ));
            }
            // Past the first alignment a bare `#` line is an ordinary comment,
            // not the start of a headerless alignment. Treating it as data
            // would emit an empty alignment and shift every later ordinal.
            if is_skippable_comment(line) {
                continue;
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
            if is_terminator(trimmed) {
                alignment.terminated = true;
                break;
            }
            if let Some(rest) = trimmed.strip_prefix("#=GF") {
                let (feature, value) = split_ws(rest);
                alignment.annotations.push(FileAnnotation {
                    kind: AnnotationKind::Gf,
                    feature: feature.to_string(),
                    value: value.to_string(),
                });
            } else if let Some(rest) = trimmed.strip_prefix("#=GC") {
                // Alignment-width, and no table column exposes it, so a scan
                // skips the payload entirely.
                if self.collect.column_annotations {
                    let (feature, value) = split_ws(rest);
                    // Later blocks extend the entry made at the feature's first
                    // appearance, so its position in file order is preserved.
                    match alignment
                        .annotations
                        .iter_mut()
                        .find(|a| a.kind == AnnotationKind::Gc && a.feature == feature)
                    {
                        Some(existing) => existing.value.push_str(value),
                        None => alignment.annotations.push(FileAnnotation {
                            kind: AnnotationKind::Gc,
                            feature: feature.to_string(),
                            value: value.to_string(),
                        }),
                    }
                }
            } else if let Some(rest) = trimmed.strip_prefix("#=GS") {
                let (name, rest) = split_ws(rest);
                let (feature, value) = split_ws(rest);
                let row = row_index(&mut alignment, &mut index, name);
                alignment.sequences[row]
                    .gs
                    .push((feature.to_string(), value.to_string()));
            } else if let Some(rest) = trimmed.strip_prefix("#=GR") {
                let (name, rest) = split_ws(rest);
                // Also alignment-width. The row itself still has to exist, so
                // that a sequence mentioned only by markup is counted.
                let row = row_index(&mut alignment, &mut index, name);
                if self.collect.residue_annotations {
                    let (feature, value) = split_ws(rest);
                    append_or_push(&mut alignment.sequences[row].gr, feature, value);
                }
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
                if self.collect.sequences {
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
