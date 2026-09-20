use super::{
    CategoryBlock,
    tokenizer::{Flavor, Kind, Token, Tokenizer},
};
use crate::error;
use datafusion::common::Result;
use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    str,
};

struct Cell {
    raw: Range<usize>,
    flavor: Flavor,
}

struct Column {
    name: String,
    cells: Vec<Cell>,
}

struct Block {
    name: String,
    columns: Vec<Column>,
}

pub struct Document {
    data: Box<[u8]>,
    blocks: Vec<Block>,
}

impl Document {
    pub fn parse(data: &[u8]) -> Result<Self> {
        let mut parser = Parser {
            data,
            tokenizer: Tokenizer::new(data),
            lookahead: None,
        };
        let mut blocks = Vec::new();
        let mut names = HashSet::new();
        while let Some(heading) = parser.take()? {
            let name = match heading.kind {
                Kind::Data => {
                    let label = parser.text(heading.span.start + 5..heading.span.end)?;
                    if label.is_empty() { " " } else { label }
                }
                Kind::Global => "",
                _ => {
                    return Err(parser
                        .tokenizer
                        .error_at(heading.span.start, "expected a data_ block header"));
                }
            };
            if !name.is_empty() && !names.insert(name.to_ascii_lowercase()) {
                return Err(parser
                    .tokenizer
                    .error_at(heading.span.start, "duplicate block name"));
            }
            let block = parser
                .block(name.to_owned())
                .map_err(|e| error(format!("mmCIF block {name:?}: {e}")))?;
            blocks.push(block);
        }
        Ok(Self {
            data: data.into(),
            blocks,
        })
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn block(&self, index: usize) -> Result<CategoryBlock<'_>> {
        let block = self
            .blocks
            .get(index)
            .ok_or_else(|| error("CIF block index out of range"))?;
        let mut columns = HashMap::with_capacity(block.columns.len());
        for column in &block.columns {
            let cells = column
                .cells
                .iter()
                .map(|cell| {
                    cell.value(&self.data).map_err(|e| {
                        error(format!(
                            "mmCIF block {:?}, tag {:?}, byte {}: {e}",
                            block.name, column.name, cell.raw.start
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            columns.insert(column.name.as_str(), cells);
        }
        Ok(CategoryBlock {
            name: &block.name,
            columns,
        })
    }
}

impl Cell {
    fn value<'a>(&self, data: &'a [u8]) -> Result<Option<&'a str>> {
        if matches!(self.flavor, Flavor::Dot | Flavor::Question) {
            return Ok(None);
        }
        // These private ranges come only from successfully delimited tokens.
        let raw = data
            .get(self.raw.clone())
            .ok_or_else(|| error("invalid CIF cell range"))?;
        let value = match self.flavor {
            Flavor::SingleQuoted | Flavor::DoubleQuoted => &raw[1..raw.len() - 1],
            Flavor::Text => {
                let mut end = raw.len() - 2; // closing semicolon and preceding LF
                if end > 1 && raw[end - 1] == b'\r' {
                    end -= 1;
                }
                &raw[1..end]
            }
            _ => raw,
        };
        str::from_utf8(value)
            .map(Some)
            .map_err(|e| error(format!("invalid CIF UTF-8: {e}")))
    }
}

struct Parser<'a> {
    data: &'a [u8],
    tokenizer: Tokenizer<'a>,
    lookahead: Option<Token>,
}

impl<'a> Parser<'a> {
    fn text(&self, span: Range<usize>) -> Result<&'a str> {
        str::from_utf8(&self.data[span]).map_err(|e| error(format!("invalid CIF name: {e}")))
    }

    fn peek(&mut self) -> Result<Option<Kind>> {
        if self.lookahead.is_none() {
            self.lookahead = self.tokenizer.next()?;
        }
        Ok(self.lookahead.as_ref().map(|token| token.kind))
    }

    fn take(&mut self) -> Result<Option<Token>> {
        match self.lookahead.take() {
            Some(token) => Ok(Some(token)),
            None => self.tokenizer.next(),
        }
    }

    fn require_separator(&self, token: &Token) -> Result<()> {
        if token.separated {
            Ok(())
        } else {
            Err(self
                .tokenizer
                .error_at(token.span.end, "expected whitespace and a following item"))
        }
    }

    fn block(&mut self, name: String) -> Result<Block> {
        let mut block = Block {
            name,
            columns: Vec::new(),
        };
        let mut tags = HashSet::new();
        let mut frames = HashSet::new();
        let mut frame = None;
        loop {
            if matches!(self.peek()?, None | Some(Kind::Data | Kind::Global)) {
                if frame.is_some() {
                    return Err(self
                        .tokenizer
                        .error_at(self.tokenizer.position(), "unterminated save frame"));
                }
                return Ok(block);
            }
            let Some(token) = self.take()? else {
                return Ok(block);
            };
            let keep = frame.is_none();
            match token.kind {
                Kind::Tag => {
                    self.require_separator(&token)?;
                    let name = self.text(token.span.clone())?.to_ascii_lowercase();
                    let value = self.take()?.ok_or_else(|| {
                        self.tokenizer
                            .error_at(token.span.start, "tag has no value")
                    })?;
                    let Kind::Value(flavor) = value.kind else {
                        return Err(self
                            .tokenizer
                            .error_at(token.span.start, "tag has no value"));
                    };
                    if keep {
                        if !tags.insert(name.clone()) {
                            return Err(self.tokenizer.error_at(token.span.start, "duplicate tag"));
                        }
                        block.columns.push(Column {
                            name,
                            cells: vec![Cell {
                                raw: value.span,
                                flavor,
                            }],
                        });
                    }
                }
                Kind::Loop => {
                    self.require_separator(&token)?;
                    let columns = self.read_loop(keep, token.span.start)?;
                    if keep {
                        for column in columns {
                            if !tags.insert(column.name.clone()) {
                                return Err(self
                                    .tokenizer
                                    .error_at(token.span.start, "duplicate loop tag"));
                            }
                            block.columns.push(column);
                        }
                    }
                }
                Kind::Frame if keep => {
                    self.require_separator(&token)?;
                    let name = self
                        .text(token.span.start + 5..token.span.end)?
                        .to_ascii_lowercase();
                    if !frames.insert(name.clone()) {
                        return Err(self
                            .tokenizer
                            .error_at(token.span.start, "duplicate save frame name"));
                    }
                    frame = Some(name);
                    self.tokenizer.set_in_frame(true);
                }
                Kind::EndFrame if !keep => {
                    frame = None;
                    self.tokenizer.set_in_frame(false);
                }
                _ => {
                    return Err(self
                        .tokenizer
                        .error_at(token.span.start, "expected a tag, loop or save frame"));
                }
            }
        }
    }

    fn read_loop(&mut self, keep: bool, start: usize) -> Result<Vec<Column>> {
        let mut columns = Vec::new();
        while self.peek()? == Some(Kind::Tag) {
            if let Some(token) = self.take()? {
                self.require_separator(&token)?;
                columns.push(Column {
                    name: self.text(token.span)?.to_ascii_lowercase(),
                    cells: Vec::new(),
                });
            }
        }
        if columns.is_empty() {
            return Err(self.tokenizer.error_at(start, "loop has no tags"));
        }
        let mut count = 0;
        while let Some(Kind::Value(flavor)) = self.peek()? {
            if let Some(token) = self.take()? {
                if keep {
                    let column = count % columns.len();
                    columns[column].cells.push(Cell {
                        raw: token.span,
                        flavor,
                    });
                }
                count += 1;
            }
        }
        if count % columns.len() != 0 {
            return Err(self.tokenizer.error_at(start, "incomplete loop row"));
        }
        if self.peek()? == Some(Kind::Stop) {
            self.take()?;
        }
        Ok(columns)
    }
}
