use crate::error;
use datafusion::common::{DataFusionError, Result};
use std::ops::Range;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Flavor {
    Bare,
    SingleQuoted,
    DoubleQuoted,
    Text,
    Dot,
    Question,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Data,
    Global,
    Loop,
    Stop,
    Frame,
    EndFrame,
    Tag,
    Value(Flavor),
}

#[derive(Clone, Debug)]
pub(super) struct Token {
    pub kind: Kind,
    pub span: Range<usize>,
    pub separated: bool,
}

pub(super) struct Tokenizer<'a> {
    data: &'a [u8],
    position: usize,
    in_frame: bool,
}

fn whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}

impl<'a> Tokenizer<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            position: 0,
            in_frame: false,
        }
    }

    pub fn set_in_frame(&mut self, in_frame: bool) {
        self.in_frame = in_frame;
    }

    fn keyword(&self, word: &[u8]) -> bool {
        let remaining = &self.data[self.position..];
        remaining
            .get(..word.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(word))
            && remaining
                .get(word.len())
                .is_none_or(|&byte| whitespace(byte) || byte == b'#')
    }

    pub fn error_at(&self, position: usize, message: &str) -> DataFusionError {
        let prefix = &self.data[..position.min(self.data.len())];
        let line = prefix.iter().filter(|&&byte| byte == b'\n').count() + 1;
        let column = prefix
            .iter()
            .rposition(|&byte| byte == b'\n')
            .map_or(prefix.len() + 1, |offset| prefix.len() - offset);
        error(format!(
            "CIF line {line}, column {column}, byte {position}: {message}"
        ))
    }

    pub fn position(&self) -> usize {
        self.position
    }

    fn boundary(&self) -> bool {
        self.data
            .get(self.position)
            .is_none_or(|&byte| whitespace(byte) || byte == b'#')
    }

    fn skip_trivia(&mut self) {
        while let Some(&byte) = self.data.get(self.position) {
            if whitespace(byte) {
                self.position += 1;
            } else if byte == b'#' {
                while self.data.get(self.position).is_some_and(|&b| b != b'\n') {
                    self.position += 1;
                }
            } else {
                break;
            }
        }
    }

    pub fn next(&mut self) -> Result<Option<Token>> {
        self.skip_trivia();
        let start = self.position;
        let Some(&first) = self.data.get(start) else {
            return Ok(None);
        };
        // Reserved words can be adjacent to comments. A frame's closing save_
        // is contextual: outside a frame, save_#name can name a new frame.
        for (word, kind) in [
            (b"global_".as_slice(), Kind::Global),
            (b"loop_".as_slice(), Kind::Loop),
            (b"stop_".as_slice(), Kind::Stop),
            (b"save_".as_slice(), Kind::EndFrame),
        ] {
            let frame_name = kind == Kind::EndFrame
                && !self.in_frame
                && self.data.get(start + word.len()) == Some(&b'#');
            if !frame_name && self.keyword(word) {
                self.position += word.len();
                return Ok(Some(Token {
                    kind,
                    span: start..self.position,
                    separated: self.position < self.data.len(),
                }));
            }
        }
        let kind = match first {
            b'\'' | b'"' => {
                self.position += 1;
                loop {
                    match self.data.get(self.position) {
                        None | Some(b'\n') => {
                            return Err(self.error_at(start, "unterminated quoted value"));
                        }
                        Some(&byte) if byte == first => {
                            self.position += 1;
                            if self.boundary() {
                                break;
                            }
                        }
                        Some(_) => self.position += 1,
                    }
                }
                Kind::Value(if first == b'\'' {
                    Flavor::SingleQuoted
                } else {
                    Flavor::DoubleQuoted
                })
            }
            b';' if start == 0 || self.data[start - 1] == b'\n' => {
                self.position += 1;
                loop {
                    let Some(&byte) = self.data.get(self.position) else {
                        return Err(self.error_at(start, "unterminated semicolon text field"));
                    };
                    if byte == b';' && self.data[self.position - 1] == b'\n' {
                        self.position += 1;
                        break;
                    }
                    self.position += 1;
                }
                Kind::Value(Flavor::Text)
            }
            _ => {
                while self
                    .data
                    .get(self.position)
                    .is_some_and(|b| (b'!'..=b'~').contains(b))
                {
                    self.position += 1;
                }
                let word = &self.data[start..self.position];
                if word.is_empty() {
                    return Err(self.error_at(start, "invalid byte outside a quoted value"));
                }
                if word
                    .get(..5)
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"data_"))
                {
                    Kind::Data
                } else if word
                    .get(..5)
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"save_"))
                {
                    Kind::Frame
                } else if first == b'_' {
                    if word.len() == 1 {
                        return Err(self.error_at(start, "empty tag name"));
                    }
                    Kind::Tag
                } else if first == b'$' {
                    return Err(self.error_at(start, "unquoted frame references are unsupported"));
                } else {
                    Kind::Value(match word {
                        b"." => Flavor::Dot,
                        b"?" => Flavor::Question,
                        _ => Flavor::Bare,
                    })
                }
            }
        };
        if !self.boundary() {
            return Err(self.error_at(self.position, "expected whitespace after token"));
        }
        Ok(Some(Token {
            kind,
            span: start..self.position,
            separated: self.position < self.data.len(),
        }))
    }
}
