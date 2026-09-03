//! Columnar PVAR variant table and its streaming, block-parallel parser.
//!
//! A PVAR is read as a stream of newline-aligned blocks. Each block is parsed
//! into its own columnar piece on a worker thread and the pieces are kept in
//! file order, so the decoded text in memory is bounded by the number of blocks
//! in flight rather than by the companion's size, and the parsed table costs a
//! few tens of bytes per variant instead of a handful of heap allocations.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::companion::sanitize_location;
use datafusion_bio_format_core::genotype::CoordinateSystem;
use flate2::read::MultiGzDecoder;

/// Decoded text handed to one parse worker at a time.
pub(crate) const PVAR_BLOCK_BYTES: usize = 16 << 20;
/// Parsed blocks waiting for a worker; small, so the producer cannot run ahead.
const BLOCK_CHANNEL_CAPACITY: usize = 2;
/// Parse workers are CPU-bound; more than this only adds contention.
const MAX_PVAR_WORKERS: usize = 16;

/// Workers to parse with on this host.
pub(crate) fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .clamp(1, MAX_PVAR_WORKERS)
}

/// The most decoded blocks alive at once: one per worker, the channel's
/// backlog, and the one the producer is filling.
#[cfg(test)]
pub(crate) fn block_window(workers: usize) -> usize {
    workers.max(1) + BLOCK_CHANNEL_CAPACITY + 1
}

/// How a PVAR stream is parsed.
#[derive(Clone, Debug)]
pub(crate) struct PvarParseConfig {
    pub(crate) coordinates: CoordinateSystem,
    pub(crate) max_variants: usize,
    pub(crate) max_decoded_bytes: usize,
    pub(crate) block_bytes: usize,
    pub(crate) workers: usize,
}

/// Counts decoded blocks alive at once, so a test can pin the window.
#[derive(Debug, Default)]
pub(crate) struct BlockGauge {
    live: AtomicUsize,
    peak: AtomicUsize,
    seen: AtomicUsize,
}

impl BlockGauge {
    fn acquire(&self) {
        self.seen.fetch_add(1, Ordering::Relaxed);
        let live = self.live.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(live, Ordering::Relaxed);
    }

    fn release(&self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }

    /// The most blocks alive at any moment.
    #[cfg(test)]
    pub(crate) fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Blocks produced in total.
    #[cfg(test)]
    pub(crate) fn blocks_seen(&self) -> usize {
        self.seen.load(Ordering::Relaxed)
    }
}

/// One newline-aligned piece of decoded text.
struct Block<'g> {
    index: usize,
    /// File line index of the first line this block's parse starts at.
    line_offset: usize,
    /// Where parsing starts; past the header in the first block, else zero.
    body_offset: usize,
    bytes: Vec<u8>,
    gauge: &'g BlockGauge,
}

impl Drop for Block<'_> {
    fn drop(&mut self) {
        self.gauge.release();
    }
}

/// Column positions a PVAR body is parsed against, resolved once from the header.
#[derive(Clone, Debug)]
struct PvarLayout {
    chrom: usize,
    position: usize,
    id: usize,
    reference: usize,
    alternate: usize,
    width: usize,
}

/// What stopped a block parser short of consuming its whole range.
enum PvarStop {
    /// `max_variants` was already reached and another variant line followed.
    Limit,
    /// A line was malformed; the payload is the detail for `pvar_line_error`.
    Malformed(String),
}

/// Variants of one block in columnar form, with block-local contig indices.
#[derive(Debug, Default)]
struct PvarBlock {
    chrom: Vec<u32>,
    position: Vec<u64>,
    /// `len + 1` offsets into `id_text`; an empty span is a missing ID.
    id_offsets: Vec<u32>,
    id_text: String,
    /// `len + 1` offsets into `allele_offsets`: REF first, then ALTs in order.
    allele_starts: Vec<u32>,
    /// `alleles + 1` offsets into `allele_text`.
    allele_offsets: Vec<u32>,
    allele_text: String,
}

impl PvarBlock {
    fn new() -> Self {
        Self {
            id_offsets: vec![0],
            allele_starts: vec![0],
            allele_offsets: vec![0],
            ..Self::default()
        }
    }

    fn len(&self) -> usize {
        self.chrom.len()
    }

    fn shrink_to_fit(&mut self) {
        self.chrom.shrink_to_fit();
        self.position.shrink_to_fit();
        self.id_offsets.shrink_to_fit();
        self.id_text.shrink_to_fit();
        self.allele_starts.shrink_to_fit();
        self.allele_offsets.shrink_to_fit();
        self.allele_text.shrink_to_fit();
    }

    #[cfg(test)]
    fn heap_bytes(&self) -> usize {
        self.chrom.capacity() * std::mem::size_of::<u32>()
            + self.position.capacity() * std::mem::size_of::<u64>()
            + self.id_offsets.capacity() * std::mem::size_of::<u32>()
            + self.id_text.capacity()
            + self.allele_starts.capacity() * std::mem::size_of::<u32>()
            + self.allele_offsets.capacity() * std::mem::size_of::<u32>()
            + self.allele_text.capacity()
    }

    fn id(&self, row: usize) -> Option<&str> {
        let start = self.id_offsets[row] as usize;
        let end = self.id_offsets[row + 1] as usize;
        (start != end).then(|| &self.id_text[start..end])
    }

    fn allele(&self, slot: usize) -> &str {
        let start = self.allele_offsets[slot] as usize;
        let end = self.allele_offsets[slot + 1] as usize;
        &self.allele_text[start..end]
    }

    fn allele_slots(&self, row: usize) -> std::ops::Range<usize> {
        self.allele_starts[row] as usize..self.allele_starts[row + 1] as usize
    }
}

/// One variant's filterable columns, borrowed from the table.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PvarRow<'a> {
    pub(crate) chrom: &'a str,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) id: Option<&'a str>,
}

/// Every PVAR variant, in file order, in columnar blocks.
#[derive(Debug)]
pub(crate) struct PvarTable {
    coordinates: CoordinateSystem,
    contigs: Vec<String>,
    blocks: Vec<PvarBlock>,
    /// `blocks.len() + 1` prefix sums of block row counts.
    row_starts: Vec<usize>,
}

impl PvarTable {
    pub(crate) fn empty(coordinates: CoordinateSystem) -> Self {
        Self {
            coordinates,
            contigs: Vec::new(),
            blocks: Vec::new(),
            row_starts: vec![0],
        }
    }

    pub(crate) fn len(&self) -> usize {
        *self.row_starts.last().unwrap_or(&0)
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn locate(&self, index: usize) -> (&PvarBlock, usize) {
        let total = self.len();
        assert!(
            index < total,
            "PVAR row {index} is out of bounds for {total} variants"
        );
        let block = self.row_starts.partition_point(|&start| start <= index) - 1;
        (&self.blocks[block], index - self.row_starts[block])
    }

    pub(crate) fn chrom(&self, index: usize) -> &str {
        let (block, row) = self.locate(index);
        &self.contigs[block.chrom[row] as usize]
    }

    /// The one-based position as written in the PVAR.
    pub(crate) fn position(&self, index: usize) -> u64 {
        let (block, row) = self.locate(index);
        block.position[row]
    }

    pub(crate) fn start(&self, index: usize) -> u64 {
        let position = self.position(index);
        match self.coordinates {
            CoordinateSystem::ZeroBasedHalfOpen => position - 1,
            CoordinateSystem::OneBasedClosed => position,
        }
    }

    pub(crate) fn end(&self, index: usize) -> u64 {
        self.position(index)
    }

    pub(crate) fn id(&self, index: usize) -> Option<&str> {
        let (block, row) = self.locate(index);
        block.id(row)
    }

    pub(crate) fn reference(&self, index: usize) -> &str {
        let (block, row) = self.locate(index);
        block.allele(block.allele_slots(row).start)
    }

    pub(crate) fn alternates(&self, index: usize) -> impl Iterator<Item = &str> + '_ {
        let (block, row) = self.locate(index);
        let mut slots = block.allele_slots(row);
        slots.next();
        slots.map(move |slot| block.allele(slot))
    }

    pub(crate) fn allele_count(&self, index: usize) -> usize {
        let (block, row) = self.locate(index);
        block.allele_slots(row).len()
    }

    pub(crate) fn row(&self, index: usize) -> PvarRow<'_> {
        PvarRow {
            chrom: self.chrom(index),
            start: self.start(index),
            end: self.end(index),
            id: self.id(index),
        }
    }

    /// Start positions in row order.
    pub(crate) fn starts(&self) -> impl Iterator<Item = u64> + '_ {
        let coordinates = self.coordinates;
        self.blocks
            .iter()
            .flat_map(|block| block.position.iter())
            .map(move |&position| match coordinates {
                CoordinateSystem::ZeroBasedHalfOpen => position - 1,
                CoordinateSystem::OneBasedClosed => position,
            })
    }

    /// Bytes the table owns on the heap.
    #[cfg(test)]
    pub(crate) fn heap_bytes(&self) -> usize {
        self.contigs
            .iter()
            .map(|contig| contig.capacity() + std::mem::size_of::<String>())
            .sum::<usize>()
            + self.blocks.iter().map(PvarBlock::heap_bytes).sum::<usize>()
            + self.row_starts.capacity() * std::mem::size_of::<usize>()
            + self.blocks.capacity() * std::mem::size_of::<PvarBlock>()
    }

    /// Appends a block, remapping its contig indices into the table's list.
    fn push_block(&mut self, mut block: PvarBlock, contigs: Vec<String>) {
        let map = contigs
            .into_iter()
            .map(|contig| {
                if let Some(found) = self.contigs.iter().position(|known| *known == contig) {
                    found as u32
                } else {
                    self.contigs.push(contig);
                    (self.contigs.len() - 1) as u32
                }
            })
            .collect::<Vec<_>>();
        if map
            .iter()
            .enumerate()
            .any(|(local, &global)| local as u32 != global)
        {
            for chrom in &mut block.chrom {
                *chrom = map[*chrom as usize];
            }
        }
        let next = self.len() + block.len();
        self.blocks.push(block);
        self.row_starts.push(next);
    }
}

/// Wraps a companion's bytes in the decoder its magic calls for.
fn decode_reader<'a>(
    path: &str,
    reader: impl Read + Send + 'a,
) -> Result<Box<dyn Read + Send + 'a>> {
    let mut buffered = BufReader::with_capacity(1 << 16, reader);
    let head = buffered
        .fill_buf()
        .map_err(|error| companion_read_error(path, &error))?
        .to_vec();
    if head.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        let decoder = zstd::stream::read::Decoder::with_buffer(buffered).map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to decompress text companion {} as zstd: {error}",
                sanitize_location(path)
            ))
        })?;
        Ok(Box::new(decoder))
    } else if head.starts_with(&[0x1f, 0x8b]) {
        Ok(Box::new(MultiGzDecoder::new(buffered)))
    } else {
        Ok(Box::new(buffered))
    }
}

fn companion_read_error(path: &str, error: &std::io::Error) -> DataFusionError {
    DataFusionError::Plan(format!(
        "failed to read text companion {}: {error}",
        sanitize_location(path)
    ))
}

fn decoded_cap_error(path: &str, max_decoded: usize) -> DataFusionError {
    DataFusionError::Plan(format!(
        "decompressed text companion {} exceeds max_decompressed_companion_bytes {max_decoded}",
        sanitize_location(path)
    ))
}

/// Reads a whole small text companion, decoding it, within the decoded cap.
pub(crate) fn read_text_companion(
    path: &str,
    reader: impl Read + Send,
    max_decoded: usize,
) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    decode_reader(path, reader)?
        .take((max_decoded as u64).saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| companion_read_error(path, &error))?;
    if decoded.len() > max_decoded {
        return Err(decoded_cap_error(path, max_decoded));
    }
    Ok(decoded)
}

/// Parses a PVAR stream into a table.
pub(crate) fn parse_pvar(
    path: &str,
    reader: impl Read + Send,
    config: &PvarParseConfig,
) -> Result<PvarTable> {
    parse_pvar_gauged(path, reader, config, &BlockGauge::default())
}

/// Fills `block` with the next `block_bytes`, extended to a newline.
///
/// Returns whether the stream is exhausted afterwards.
fn read_block(
    reader: &mut impl BufRead,
    block_bytes: usize,
    block: &mut Vec<u8>,
) -> std::io::Result<bool> {
    block.clear();
    reader
        .by_ref()
        .take(block_bytes as u64)
        .read_to_end(block)?;
    if block.len() < block_bytes {
        return Ok(true);
    }
    if block.last() != Some(&b'\n') {
        reader.read_until(b'\n', block)?;
    }
    Ok(reader.fill_buf()?.is_empty())
}

/// The header of the first block: its line count, where the body starts, and
/// the column layout, or `None` when the header runs past the block.
fn parse_header(
    path: &str,
    text: &str,
    complete: bool,
) -> Result<Option<(usize, usize, PvarLayout)>> {
    let mut header: Option<(usize, &str)> = None;
    let mut header_lines = 0;
    let mut body_start = 0;
    let mut offset = 0;
    let mut saw_body = false;
    while offset < text.len() {
        let rest = &text[offset..];
        let (raw, next) = match rest.find('\n') {
            Some(newline) => (&rest[..newline], offset + newline + 1),
            None => (rest, text.len()),
        };
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if !line.starts_with('#') {
            saw_body = true;
            break;
        }
        if line.starts_with("#CHROM") {
            header = Some((header_lines, line));
        }
        header_lines += 1;
        offset = next;
        body_start = offset;
    }
    if !saw_body && !complete {
        return Err(DataFusionError::Plan(format!(
            "PVAR {} header does not end within the first {} bytes",
            sanitize_location(path),
            text.len()
        )));
    }
    let body = &text[body_start..];
    let columns = if let Some((header_index, header)) = header {
        if header_index + 1 != header_lines {
            return Err(DataFusionError::Plan(format!(
                "PVAR {} #CHROM line must be the final header line",
                sanitize_location(path)
            )));
        }
        header
            .trim_start_matches('#')
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>()
    } else {
        if header_lines > 0 {
            return Err(DataFusionError::Plan(format!(
                "PVAR {} header does not end with #CHROM",
                sanitize_location(path)
            )));
        }
        if !saw_body {
            return Ok(None);
        }
        let first_width = body
            .lines()
            .find(|line| !line.is_empty())
            .map(|line| line.split_whitespace().count())
            .unwrap_or(0);
        // PLINK 2 specifies BIM order for a headerless PVAR. The five-column
        // form omits CM: CHROM, ID, POS, ALT, REF.
        match first_width {
            5 => vec!["CHROM", "ID", "POS", "ALT", "REF"],
            6.. => vec!["CHROM", "ID", "CM", "POS", "ALT", "REF"],
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "headerless PVAR {} must have at least five columns",
                    sanitize_location(path)
                )));
            }
        }
        .into_iter()
        .map(str::to_string)
        .collect()
    };
    let column = |name: &str| {
        columns
            .iter()
            .position(|value| value == name)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "PVAR {} is missing required {name} column",
                    sanitize_location(path)
                ))
            })
    };
    let (chrom, position, id, reference, alternate) = (
        column("CHROM")?,
        column("POS")?,
        column("ID")?,
        column("REF")?,
        column("ALT")?,
    );
    let layout = PvarLayout {
        chrom,
        position,
        id,
        reference,
        alternate,
        width: [chrom, position, id, reference, alternate]
            .into_iter()
            .max()
            .unwrap_or(0)
            + 1,
    };
    Ok(Some((header_lines, body_start, layout)))
}

/// A parsed block, its contigs, and where it stopped short, if it did.
type BlockOutcome = (PvarBlock, Vec<String>, Option<(usize, PvarStop)>);

/// What a worker made of one block.
struct ParsedBlock {
    line_offset: usize,
    outcome: std::result::Result<BlockOutcome, String>,
}

/// Parses one block's body in block-local line numbers.
fn parse_block(body: &str, layout: &PvarLayout, max_variants: usize) -> BlockOutcome {
    let mut block = PvarBlock::new();
    let mut contigs: Vec<String> = Vec::new();
    let mut fields: Vec<&str> = Vec::new();
    for (line_index, line) in body.lines().enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // A block cannot know the running total, so the exact limit is the
        // collector's job. This bounds a block's own work on a file far over
        // the limit, and changes no output: the collector re-derives the same
        // answer from the counts.
        if block.len() >= max_variants {
            return (block, contigs, Some((line_index, PvarStop::Limit)));
        }
        fields.clear();
        fields.extend(line.split_whitespace());
        if let Err(detail) = push_record(&mut block, &mut contigs, &fields, layout) {
            return (
                block,
                contigs,
                Some((line_index, PvarStop::Malformed(detail))),
            );
        }
    }
    block.shrink_to_fit();
    (block, contigs, None)
}

fn push_record(
    block: &mut PvarBlock,
    contigs: &mut Vec<String>,
    fields: &[&str],
    layout: &PvarLayout,
) -> std::result::Result<(), String> {
    if fields.len() < layout.width {
        return Err(format!(
            "has {} columns; at least {} required",
            fields.len(),
            layout.width
        ));
    }
    let position = fields[layout.position]
        .parse::<u64>()
        .map_err(|error| format!("has invalid POS: {error}"))?;
    if position == 0 {
        return Err(
            "has invalid POS: genotype positions must be one-based positive integers".to_string(),
        );
    }
    let reference = fields[layout.reference];
    if reference.is_empty() || reference == "." || reference.contains(',') {
        return Err("has malformed REF allele".to_string());
    }
    let alternates = fields[layout.alternate];
    if alternates.is_empty()
        || alternates
            .split(',')
            .any(|allele| allele.is_empty() || allele == ".")
    {
        return Err("has malformed ALT allele list".to_string());
    }

    let chrom = fields[layout.chrom];
    let contig = match contigs.last() {
        Some(last) if last == chrom => contigs.len() - 1,
        _ => match contigs.iter().position(|known| known == chrom) {
            Some(found) => found,
            None => {
                contigs.push(chrom.to_string());
                contigs.len() - 1
            }
        },
    };
    block.chrom.push(contig as u32);
    block.position.push(position);
    let id = fields[layout.id];
    if id != "." {
        block.id_text.push_str(id);
    }
    block.id_offsets.push(block.id_text.len() as u32);
    block.allele_text.push_str(reference);
    block.allele_offsets.push(block.allele_text.len() as u32);
    for allele in alternates.split(',') {
        block.allele_text.push_str(allele);
        block.allele_offsets.push(block.allele_text.len() as u32);
    }
    block
        .allele_starts
        .push((block.allele_offsets.len() - 1) as u32);
    Ok(())
}

fn pvar_line_error(path: &str, line_index: usize, detail: String) -> DataFusionError {
    DataFusionError::Plan(format!(
        "PVAR {} line {} {detail}",
        sanitize_location(path),
        line_index + 1
    ))
}

fn limit_error(path: &str, max_variants: usize) -> DataFusionError {
    DataFusionError::Plan(format!(
        "PVAR {} exceeds configured max_variants {max_variants}",
        sanitize_location(path)
    ))
}

/// Parses a PVAR stream, reporting block occupancy to `gauge`.
pub(crate) fn parse_pvar_gauged(
    path: &str,
    reader: impl Read + Send,
    config: &PvarParseConfig,
    gauge: &BlockGauge,
) -> Result<PvarTable> {
    let block_bytes = config.block_bytes.max(1);
    let workers = config.workers.max(1);
    let mut reader = BufReader::with_capacity(1 << 16, decode_reader(path, reader)?);
    let mut decoded_bytes = 0_usize;

    let mut first = Vec::new();
    let complete = read_block(&mut reader, block_bytes, &mut first)
        .map_err(|error| companion_read_error(path, &error))?;
    decoded_bytes += first.len();
    if decoded_bytes > config.max_decoded_bytes {
        return Err(decoded_cap_error(path, config.max_decoded_bytes));
    }
    let text = std::str::from_utf8(&first).map_err(|error| {
        DataFusionError::Plan(format!(
            "PVAR {} is not valid UTF-8: {error}",
            sanitize_location(path)
        ))
    })?;
    let Some((header_lines, body_start, layout)) = parse_header(path, text, complete)? else {
        return Ok(PvarTable::empty(config.coordinates));
    };
    let first_lines = count_newlines(&first);

    let mut table = PvarTable::empty(config.coordinates);
    let cancelled = AtomicBool::new(false);
    let outcome: Result<()> = std::thread::scope(|scope| {
        let (block_tx, block_rx) = mpsc::sync_channel::<Block<'_>>(BLOCK_CHANNEL_CAPACITY);
        let block_rx = Arc::new(Mutex::new(block_rx));
        let (done_tx, done_rx) = mpsc::channel::<(usize, ParsedBlock)>();
        let cancelled = &cancelled;
        let layout = &layout;

        for _ in 0..workers {
            let block_rx = Arc::clone(&block_rx);
            let done_tx = done_tx.clone();
            scope.spawn(move || {
                loop {
                    let next = block_rx.lock().map(|receiver| receiver.recv());
                    let Ok(Ok(block)) = next else {
                        break;
                    };
                    if cancelled.load(Ordering::Relaxed) {
                        continue;
                    }
                    let outcome = match std::str::from_utf8(&block.bytes[block.body_offset..]) {
                        Ok(body) => Ok(parse_block(body, layout, config.max_variants)),
                        Err(error) => Err(format!("is not valid UTF-8: {error}")),
                    };
                    let parsed = ParsedBlock {
                        line_offset: block.line_offset,
                        outcome,
                    };
                    let index = block.index;
                    drop(block);
                    if done_tx.send((index, parsed)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(done_tx);

        let producer = scope.spawn(move || -> Result<usize> {
            gauge.acquire();
            let mut block = Block {
                index: 0,
                line_offset: header_lines,
                body_offset: body_start,
                bytes: first,
                gauge,
            };
            let mut next_line = first_lines;
            let mut count = 1;
            let mut exhausted = complete;
            loop {
                if block_tx.send(block).is_err() {
                    return Ok(count);
                }
                if exhausted || cancelled.load(Ordering::Relaxed) {
                    return Ok(count);
                }
                let mut bytes = Vec::new();
                exhausted = read_block(&mut reader, block_bytes, &mut bytes)
                    .map_err(|error| companion_read_error(path, &error))?;
                if bytes.is_empty() {
                    return Ok(count);
                }
                decoded_bytes += bytes.len();
                if decoded_bytes > config.max_decoded_bytes {
                    return Err(decoded_cap_error(path, config.max_decoded_bytes));
                }
                let lines = count_newlines(&bytes);
                gauge.acquire();
                block = Block {
                    index: count,
                    line_offset: next_line,
                    body_offset: 0,
                    bytes,
                    gauge,
                };
                next_line += lines;
                count += 1;
            }
        });

        let mut pending = BTreeMap::new();
        let mut next_index = 0;
        let mut first_error: Option<DataFusionError> = None;
        for (index, parsed) in done_rx {
            if first_error.is_some() {
                continue;
            }
            pending.insert(index, parsed);
            while let Some(parsed) = pending.remove(&next_index) {
                next_index += 1;
                if let Err(error) = collect_block(path, &mut table, parsed, config.max_variants) {
                    first_error = Some(error);
                    cancelled.store(true, Ordering::Relaxed);
                    break;
                }
            }
        }
        let produced = producer.join().map_err(|_| {
            DataFusionError::Plan(format!("PVAR {} reader panicked", sanitize_location(path)))
        })?;
        if let Some(error) = first_error {
            return Err(error);
        }
        let produced = produced?;
        if next_index != produced {
            return Err(DataFusionError::Plan(format!(
                "PVAR {} parser lost {} of {produced} blocks",
                sanitize_location(path),
                produced - next_index
            )));
        }
        Ok(())
    });
    outcome?;
    table.blocks.shrink_to_fit();
    table.row_starts.shrink_to_fit();
    Ok(table)
}

/// Folds one block into the table, keeping first-in-file error order.
fn collect_block(
    path: &str,
    table: &mut PvarTable,
    parsed: ParsedBlock,
    max_variants: usize,
) -> Result<()> {
    let (block, contigs, stop) = parsed.outcome.map_err(|detail| {
        DataFusionError::Plan(format!("PVAR {} {detail}", sanitize_location(path)))
    })?;
    // Whichever condition comes first in the file wins, exactly as it does
    // when one pass reads the whole body. The limit trips inside this block's
    // rows when they would carry the total past it, which is earlier than any
    // error the block reports, because a block stops at its first bad line.
    if table.len() + block.len() > max_variants {
        return Err(limit_error(path, max_variants));
    }
    table.push_block(block, contigs);
    if let Some((block_line, stop)) = stop {
        // The block stopped, so a further line exists. A serial pass would
        // check the limit before reading it.
        if table.len() >= max_variants {
            return Err(limit_error(path, max_variants));
        }
        return Err(match stop {
            PvarStop::Limit => limit_error(path, max_variants),
            PvarStop::Malformed(detail) => {
                pvar_line_error(path, parsed.line_offset + block_line, detail)
            }
        });
    }
    Ok(())
}

fn count_newlines(bytes: &[u8]) -> usize {
    bytes.iter().filter(|&&byte| byte == b'\n').count()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use datafusion_bio_format_core::genotype::CoordinateSystem;

    use super::*;

    fn config(block_bytes: usize, workers: usize) -> PvarParseConfig {
        PvarParseConfig {
            coordinates: CoordinateSystem::ZeroBasedHalfOpen,
            max_variants: usize::MAX,
            max_decoded_bytes: usize::MAX,
            block_bytes,
            workers,
        }
    }

    fn parse(text: &str, config: &PvarParseConfig) -> Result<PvarTable> {
        parse_pvar("cohort.pvar", text.as_bytes(), config)
    }

    /// `rows` records with an optional malformed line, plus the header line count.
    fn large_pvar(rows: usize, malformed_at: Option<usize>) -> (String, usize) {
        let mut text = String::from("##fileformat=PVARv1.0\n#CHROM\tPOS\tID\tREF\tALT\n");
        let header_lines = 2;
        for row in 0..rows {
            if Some(row) == malformed_at {
                text.push_str("22\tnot-a-position\tbad\tA\tC\n");
            } else {
                let position = row + 1;
                text.push_str(&format!("22\t{position}\trs{row}\tA\tC\n"));
            }
        }
        (text, header_lines)
    }

    fn assert_same(left: &PvarTable, right: &PvarTable) {
        assert_eq!(left.len(), right.len());
        for index in 0..left.len() {
            assert_eq!(left.chrom(index), right.chrom(index), "chrom {index}");
            assert_eq!(left.start(index), right.start(index), "start {index}");
            assert_eq!(left.end(index), right.end(index), "end {index}");
            assert_eq!(left.id(index), right.id(index), "id {index}");
            assert_eq!(left.reference(index), right.reference(index), "ref {index}");
            assert_eq!(
                left.alternates(index).collect::<Vec<_>>(),
                right.alternates(index).collect::<Vec<_>>(),
                "alt {index}"
            );
        }
    }

    #[test]
    fn accessors_expose_every_pvar_column() {
        let text = "##fileformat=PVARv1.0\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\n\
                    1\t10\trs1\tA\tC\t.\n\
                    1\t20\t.\tG\tT,GA\t.\n\
                    X\t5\trs3\tCT\tC\t.\n";
        let table = parse(text, &config(1 << 20, 2)).unwrap();
        assert_eq!(table.len(), 3);
        assert_eq!(table.chrom(0), "1");
        assert_eq!(table.chrom(2), "X");
        assert_eq!((table.start(0), table.end(0)), (9, 10));
        assert_eq!((table.start(2), table.end(2)), (4, 5));
        assert_eq!(table.id(0), Some("rs1"));
        assert_eq!(table.id(1), None);
        assert_eq!(table.reference(1), "G");
        assert_eq!(table.alternates(1).collect::<Vec<_>>(), vec!["T", "GA"]);
        assert_eq!(table.allele_count(1), 3);
        assert_eq!(table.allele_count(0), 2);
        assert_eq!(table.reference(2), "CT");

        let one_based = PvarParseConfig {
            coordinates: CoordinateSystem::OneBasedClosed,
            ..config(1 << 20, 2)
        };
        let table = parse(text, &one_based).unwrap();
        assert_eq!((table.start(0), table.end(0)), (10, 10));
    }

    #[test]
    fn parses_bim_order_headerless_pvar() {
        let table = parse("1 v1 10 C A\n2 v2 20 G T\n", &config(1 << 20, 1)).unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(table.start(0), 9);
        assert_eq!(table.id(0), Some("v1"));
        assert_eq!(table.reference(0), "A");
        assert_eq!(table.alternates(0).collect::<Vec<_>>(), vec!["C"]);
        assert_eq!(table.chrom(1), "2");
        let six = parse("1 v1 0.5 10 C A\n", &config(1 << 20, 1)).unwrap();
        assert_eq!(six.start(0), 9);
        assert_eq!(six.reference(0), "A");
    }

    #[test]
    fn empty_and_header_only_inputs_yield_empty_tables() {
        assert!(parse("", &config(1 << 20, 2)).unwrap().is_empty());
        let header_only = "##fileformat=PVARv1.0\n#CHROM\tPOS\tID\tREF\tALT\n";
        assert!(parse(header_only, &config(1 << 20, 2)).unwrap().is_empty());
    }

    #[test]
    fn table_costs_under_eighty_bytes_per_biallelic_variant_beyond_its_text() {
        let (text, _) = large_pvar(50_000, None);
        let table = parse(&text, &config(64 << 10, 4)).unwrap();
        let text_bytes: usize = (0..table.len())
            .map(|index| {
                table.id(index).map_or(0, str::len)
                    + table.reference(index).len()
                    + table.alternates(index).map(str::len).sum::<usize>()
            })
            .sum();
        let overhead = table.heap_bytes() - text_bytes;
        assert!(
            overhead <= 80 * table.len(),
            "{} bytes per variant beyond the text",
            overhead / table.len()
        );
    }

    #[test]
    fn parses_a_multi_block_stream_identically_to_one_block() {
        let (text, _) = large_pvar(60_000, None);
        let whole = parse(&text, &config(usize::MAX, 1)).unwrap();
        let blocks = parse(&text, &config(64 << 10, 4)).unwrap();
        assert!(
            text.len() > 10 * (64 << 10),
            "fixture must span many blocks"
        );
        assert_eq!(whole.len(), 60_000);
        assert_same(&whole, &blocks);
        for row in [0, 1, 30_000, 59_999] {
            assert_eq!(blocks.chrom(row), "22");
            assert_eq!(blocks.start(row), row as u64);
            assert_eq!(blocks.id(row), Some(format!("rs{row}").as_str()));
        }
    }

    #[test]
    fn parses_compressed_streams_in_blocks() {
        let (text, _) = large_pvar(60_000, None);
        let whole = parse(&text, &config(usize::MAX, 1)).unwrap();

        let zstd_bytes = zstd::encode_all(text.as_bytes(), 3).unwrap();
        let from_zstd =
            parse_pvar("cohort.pvar.zst", &zstd_bytes[..], &config(64 << 10, 4)).unwrap();
        assert_same(&whole, &from_zstd);

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(text.as_bytes()).unwrap();
        let gz_bytes = encoder.finish().unwrap();
        let from_gz = parse_pvar("cohort.pvar.gz", &gz_bytes[..], &config(64 << 10, 4)).unwrap();
        assert_same(&whole, &from_gz);
    }

    #[test]
    fn reports_the_file_line_of_a_malformed_row_in_a_later_block() {
        for malformed_at in [0_usize, 1, 30_000, 59_999] {
            let (text, header_lines) = large_pvar(60_000, Some(malformed_at));
            let error = parse(&text, &config(64 << 10, 4)).unwrap_err().to_string();
            let expected = format!("line {} has invalid POS", header_lines + malformed_at + 1);
            assert!(error.contains(&expected), "{error} (expected {expected})");
        }
    }

    #[test]
    fn enforces_the_row_limit_across_block_boundaries() {
        let (text, _) = large_pvar(400, None);
        let limited = PvarParseConfig {
            max_variants: 400,
            ..config(1 << 10, 4)
        };
        assert_eq!(parse(&text, &limited).unwrap().len(), 400);
        let limited = PvarParseConfig {
            max_variants: 399,
            ..config(1 << 10, 4)
        };
        let error = parse(&text, &limited).unwrap_err().to_string();
        assert!(error.contains("max_variants 399"), "{error}");
    }

    #[test]
    fn the_row_limit_beats_a_malformed_row_reached_at_the_same_moment() {
        // A serial pass checks the limit before reading the bad line, so the
        // limit wins; one more allowed row and the bad line is read instead.
        let (mut text, header_lines) = large_pvar(400, None);
        text.push_str("22\tnot-a-position\tbad\tA\tC\n");
        let at_limit = PvarParseConfig {
            max_variants: 400,
            ..config(1 << 10, 4)
        };
        let error = parse(&text, &at_limit).unwrap_err().to_string();
        assert!(error.contains("max_variants 400"), "{error}");
        let past_limit = PvarParseConfig {
            max_variants: 401,
            ..config(1 << 10, 4)
        };
        let error = parse(&text, &past_limit).unwrap_err().to_string();
        assert!(
            error.contains(&format!("line {} has invalid POS", header_lines + 401)),
            "{error}"
        );
    }

    #[test]
    fn the_earlier_of_limit_and_malformed_row_wins_across_blocks() {
        let (text, header_lines) = large_pvar(60_000, Some(50_000));
        let limited = PvarParseConfig {
            max_variants: 10_000,
            ..config(64 << 10, 4)
        };
        let error = parse(&text, &limited).unwrap_err().to_string();
        assert!(error.contains("max_variants 10000"), "{error}");

        let (text, _) = large_pvar(60_000, Some(100));
        let limited = PvarParseConfig {
            max_variants: 50_000,
            ..config(64 << 10, 4)
        };
        let error = parse(&text, &limited).unwrap_err().to_string();
        assert!(
            error.contains(&format!("line {} has invalid POS", header_lines + 101)),
            "{error}"
        );
    }

    #[test]
    fn rejects_a_header_that_does_not_end_within_the_first_block() {
        let mut text = String::new();
        for line in 0..2_000 {
            text.push_str(&format!("##contig=<ID=contig{line},length=1000>\n"));
        }
        text.push_str("#CHROM\tPOS\tID\tREF\tALT\n1\t10\trs1\tA\tC\n");
        let error = parse(&text, &config(1 << 10, 2)).unwrap_err().to_string();
        assert!(error.contains("header"), "{error}");
        assert!(error.contains("cohort.pvar"), "{error}");
        assert_eq!(parse(&text, &config(1 << 20, 2)).unwrap().len(), 1);
    }

    #[test]
    fn enforces_the_decoded_byte_cap_naming_the_option() {
        let (text, _) = large_pvar(10_000, None);
        let capped = PvarParseConfig {
            max_decoded_bytes: 4096,
            ..config(1 << 10, 2)
        };
        let error = parse(&text, &capped).unwrap_err().to_string();
        assert!(
            error.contains("max_decompressed_companion_bytes 4096"),
            "{error}"
        );
        let capped = PvarParseConfig {
            max_decoded_bytes: text.len(),
            ..config(1 << 10, 2)
        };
        assert_eq!(parse(&text, &capped).unwrap().len(), 10_000);
    }

    #[test]
    fn holds_at_most_the_configured_window_of_blocks_in_flight() {
        let (text, _) = large_pvar(200_000, None);
        let workers = 3;
        let gauge = BlockGauge::default();
        let table = parse_pvar_gauged(
            "cohort.pvar",
            text.as_bytes(),
            &config(16 << 10, workers),
            &gauge,
        )
        .unwrap();
        assert_eq!(table.len(), 200_000);
        assert!(
            gauge.blocks_seen() > 4 * workers,
            "fixture must produce many more blocks than the window"
        );
        assert!(
            gauge.peak() <= block_window(workers),
            "{} blocks in flight, window is {}",
            gauge.peak(),
            block_window(workers)
        );
    }

    #[test]
    fn reads_a_text_companion_within_the_decoded_cap() {
        let text = b"#FID IID\nf1 i1\n";
        let bytes = read_text_companion("cohort.psam", &text[..], 1024).unwrap();
        assert_eq!(bytes, text);
        let zstd_bytes = zstd::encode_all(&text[..], 3).unwrap();
        let bytes = read_text_companion("cohort.psam.zst", &zstd_bytes[..], 1024).unwrap();
        assert_eq!(bytes, text);
        let error = read_text_companion("cohort.psam", &text[..], 4)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("max_decompressed_companion_bytes 4"),
            "{error}"
        );
    }
}
