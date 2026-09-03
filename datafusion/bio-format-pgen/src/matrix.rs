//! Decoding one genotype field straight into a caller-owned matrix.
//!
//! The DataFusion scan builds Arrow batches and something else copies them into
//! whatever the caller actually wanted. For a dense matrix that copy is pure
//! overhead — on a whole chromosome it is 10 GB read and 10 GB written — and it
//! is the stage that stops a materializing read from scaling, because it
//! saturates memory bandwidth long before it runs out of threads.
//!
//! This path skips it. Decoders write genotypes at their final address, so the
//! values are touched once, by the same threads that produced them.
//!
//! Two things make that simpler here than in the Arrow path rather than harder:
//! a matrix has no validity bitmap, so a missing call is just a sentinel the
//! caller chooses; and a full-file read gives every variant a row index equal to
//! its position in the selection, so partitions own disjoint row ranges and need
//! no coordination to write them.

use std::collections::HashSet;
use std::iter::Peekable;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::range_planning::ByteRange;

use crate::decode::{
    GenotypeProjection, GtDecodeWorkspace, decode_biallelic_gt_into, decode_common_difflist_into,
    decode_main_track_and_validate, decode_record_and_main, supports_biallelic_gt_fast_path,
    supports_common_difflist_fast_path, validated_dense_hardcalls,
};
use crate::fileset::{PgenFileset, PgenMode};
use crate::physical_exec::{MergedIndices, PgenPartition, alt_count_from_code, record_payload};
use crate::selection::{SelectionSlice, VariantSelection};
use crate::table_provider::{PgenReadOptions, plan_payload_partitions};

/// The internal code for a missing call.
const MISSING_CODE: u8 = 3;

/// A genotype field with one value per sample, which is what a matrix can hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatrixField {
    /// Hardcall ALT allele count, one byte per genotype.
    AltCount,
    /// ALT dosage. Fractional when the fileset carries a dosage track.
    Dosage,
}

impl MatrixField {
    /// The genotype child this field corresponds to in the Arrow schema.
    pub fn name(self) -> &'static str {
        match self {
            Self::AltCount => "ALT_COUNT",
            Self::Dosage => "DS",
        }
    }
}

/// The caller's matrix, row-major, one row per variant.
pub enum MatrixData<'a> {
    /// `int8` destination, for [`MatrixField::AltCount`].
    AltCount {
        /// Row-major values, `variants * samples` long.
        values: &'a mut [i8],
        /// Written where a genotype is missing.
        missing: i8,
    },
    /// `float32` destination, for [`MatrixField::Dosage`].
    Dosage {
        /// Row-major values, `variants * samples` long.
        values: &'a mut [f32],
        /// Written where a genotype is missing.
        missing: f32,
    },
}

impl MatrixData<'_> {
    fn len(&self) -> usize {
        match self {
            Self::AltCount { values, .. } => values.len(),
            Self::Dosage { values, .. } => values.len(),
        }
    }

    fn field(&self) -> MatrixField {
        match self {
            Self::AltCount { .. } => MatrixField::AltCount,
            Self::Dosage { .. } => MatrixField::Dosage,
        }
    }
}

/// The shape a fileset will produce, reported before any genotype is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MatrixShape {
    /// Rows, one per PVAR variant.
    pub variants: usize,
    /// Columns, one per selected sample.
    pub samples: usize,
}

/// Reports the matrix shape a fileset would produce under `options`.
///
/// Reads only the companions, so a caller can allocate before deciding to scan.
pub async fn genotype_matrix_shape(
    pgen_path: String,
    options: &PgenReadOptions,
) -> Result<MatrixShape> {
    let fileset = PgenFileset::open(pgen_path, options).await?;
    Ok(MatrixShape {
        variants: fileset.variants.len(),
        samples: fileset.selected_samples.source_indices().len(),
    })
}

/// Decodes one genotype field of a whole fileset into `destination`.
///
/// `destination` must be exactly `variants * samples` long; use
/// [`genotype_matrix_shape`] to size it. `threads` bounds the decoders, which
/// write disjoint row ranges and so never contend.
///
/// Returns the shape and the variant start positions, in row order. The
/// positions come from the fileset this already opened, so a caller that needs
/// them does not have to parse the PVAR a second time to get them.
pub async fn read_genotype_matrix(
    pgen_path: String,
    options: &PgenReadOptions,
    destination: MatrixData<'_>,
    threads: usize,
) -> Result<(MatrixShape, Vec<u64>)> {
    let reader = GenotypeMatrixReader::open(pgen_path, options).await?;
    let positions = reader.positions();
    reader.read_into(destination, threads).await?;
    Ok((reader.shape(), positions))
}

/// An opened fileset, ready to decode into a destination.
///
/// Opening parses the PVAR and PSAM — 108 MB of text on a whole chromosome. A
/// caller that must learn the shape before it can allocate would otherwise pay
/// that twice, once to ask and once to decode, and on the hardcall workload the
/// second parse is a fifth of the whole operation.
pub struct GenotypeMatrixReader {
    fileset: Arc<PgenFileset>,
    /// Read-coalescing bounds, kept because they belong to the request rather
    /// than to the fileset and the decode still needs them.
    max_range_gap: u64,
    max_range_bytes: u64,
}

impl GenotypeMatrixReader {
    /// Opens a fileset and reads its companions.
    pub async fn open(pgen_path: String, options: &PgenReadOptions) -> Result<Self> {
        Ok(Self {
            fileset: Arc::new(PgenFileset::open(pgen_path, options).await?),
            max_range_gap: options.max_range_gap,
            max_range_bytes: options.max_range_bytes,
        })
    }

    /// The shape a decode will produce.
    pub fn shape(&self) -> MatrixShape {
        MatrixShape {
            variants: self.fileset.variants.len(),
            samples: self.fileset.selected_samples.source_indices().len(),
        }
    }

    /// The selected sample names, in column order.
    pub fn sample_names(&self) -> &[String] {
        self.fileset.selected_samples.names()
    }

    /// The variant start positions, in row order.
    pub fn positions(&self) -> Vec<u64> {
        self.fileset.variants.starts().collect()
    }

    /// Decodes into `destination`, which must be exactly `variants * samples`
    /// long.
    pub async fn read_into(&self, destination: MatrixData<'_>, threads: usize) -> Result<()> {
        decode_matrix(
            &self.fileset,
            destination,
            threads,
            self.max_range_gap,
            self.max_range_bytes,
        )
        .await
    }
}

async fn decode_matrix(
    fileset: &Arc<PgenFileset>,
    mut destination: MatrixData<'_>,
    threads: usize,
    max_range_gap: u64,
    max_range_bytes: u64,
) -> Result<()> {
    let field = destination.field();
    let samples = fileset.selected_samples.source_indices().len();
    let variants = fileset.variants.len();

    let expected = variants
        .checked_mul(samples)
        .ok_or_else(|| DataFusionError::Execution("PGEN matrix size overflowed".to_string()))?;
    if destination.len() != expected {
        return Err(DataFusionError::Execution(format!(
            "PGEN matrix destination has {} values; expected {expected} ({variants} variants x {samples} samples)",
            destination.len()
        )));
    }
    if expected == 0 {
        return Ok(());
    }

    let threads = threads.max(1);
    let selection = VariantSelection::All(variants);
    let partitions =
        plan_payload_partitions(selection, fileset, threads, max_range_gap, max_range_bytes)?;

    // Partitions own contiguous, ascending row ranges, so the destination
    // splits into disjoint pieces and the workers never share a byte.
    let mut cursor = 0;
    for partition in &partitions {
        if partition.owned.start != cursor {
            return Err(DataFusionError::Execution(
                "PGEN matrix partitions are not contiguous in variant order".to_string(),
            ));
        }
        cursor = partition.owned.end;
    }
    if cursor != variants {
        return Err(DataFusionError::Execution(format!(
            "PGEN matrix partitions cover {cursor} of {variants} variants"
        )));
    }

    match &mut destination {
        MatrixData::AltCount { values, missing } => {
            let missing = *missing;
            decode_rounds(
                fileset,
                field,
                &partitions,
                values,
                samples,
                move |code| {
                    if code == MISSING_CODE {
                        missing
                    } else {
                        alt_count_from_code(code)
                    }
                },
                // ALT_COUNT never reads the dosage track, so the general path
                // narrows the hardcall it already has.
                move |dosage| dosage.map_or(missing, |value| value as i8),
            )
            .await?;
        }
        MatrixData::Dosage { values, missing } => {
            let missing = *missing;
            decode_rounds(
                fileset,
                field,
                &partitions,
                values,
                samples,
                move |code| {
                    if code == MISSING_CODE {
                        missing
                    } else {
                        f32::from(alt_count_from_code(code))
                    }
                },
                move |dosage| dosage.unwrap_or(missing),
            )
            .await?;
        }
    }
    Ok(())
}

/// Fetches and decodes the fileset a round at a time: one byte range per
/// partition, read concurrently, then decoded in parallel.
///
/// Reading every range of every partition up front was simpler, but it held the
/// whole compressed PGEN in memory on top of the destination matrix — and on a
/// sample subset the destination is small while the input is not — and it left
/// object-store latency serial no matter how many decoders were asked for. A
/// round holds `partitions * max_range_bytes` instead, and its reads overlap.
///
/// The decoders keep their per-partition state across rounds, so a variant that
/// depends on an LD base decoded in an earlier round still finds it.
async fn decode_rounds<'a, T, F, D>(
    fileset: &'a PgenFileset,
    field: MatrixField,
    partitions: &'a [PgenPartition],
    values: &'a mut [T],
    samples: usize,
    value_of: F,
    dosage_of: D,
) -> Result<()>
where
    T: Copy + Send + Sync,
    F: Fn(u8) -> T + Copy + Send + Sync,
    D: Fn(Option<f32>) -> T + Copy + Send + Sync,
{
    let mut rest = values;
    let mut decoders = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let (head, tail) = rest.split_at_mut(partition.owned.len() * samples);
        rest = tail;
        decoders.push(PartitionDecoder::new(
            fileset, field, partition, head, samples, value_of, dosage_of,
        )?);
    }

    // One reader per partition: they seek independently, so a round's reads do
    // not have to wait on each other.
    let mut readers = Vec::with_capacity(partitions.len());
    for _ in partitions {
        readers.push(fileset.source.range_reader(&fileset.pgen_path).await?);
    }

    let rounds = partitions
        .iter()
        .map(|partition| partition.ranges.len())
        .max()
        .unwrap_or(0);
    for round in 0..rounds {
        let fetches = readers
            .iter_mut()
            .zip(partitions)
            .map(|(reader, partition)| async move {
                match partition.ranges.get(round).copied() {
                    Some(range) => {
                        reader.read_range(range.start..range.end).await?;
                        Ok::<_, DataFusionError>(Some(range))
                    }
                    // A partition with fewer ranges than the widest one sits
                    // this round out.
                    None => Ok(None),
                }
            });
        let fetched = futures::future::try_join_all(fetches).await?;
        // The decoders read out of the readers' own buffers, so a range is
        // never copied and the buffers are reused from one round to the next.
        let loaded = readers
            .iter()
            .zip(&fetched)
            .map(|(reader, range)| range.map(|range| (range, reader.bytes())))
            .collect::<Vec<_>>();

        std::thread::scope(|scope| {
            let handles = decoders
                .iter_mut()
                .zip(&loaded)
                .filter_map(|(decoder, slot)| {
                    slot.map(|(range, bytes)| {
                        scope.spawn(move || decoder.decode_range(range, bytes))
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        DataFusionError::Execution("PGEN matrix decoder panicked".to_string())
                    })?
                })
                .collect::<Result<Vec<_>>>()
        })?;
    }
    Ok(())
}

/// One partition's decode, resumable across byte ranges.
///
/// Everything the record loop carries between ranges lives here — the
/// workspace, the retained LD base, the position in the required-variant list
/// and the next destination row — because a partition is now handed its input
/// a range at a time rather than all at once.
struct PartitionDecoder<'a, T, F, D> {
    fileset: &'a PgenFileset,
    field: MatrixField,
    values: &'a mut [T],
    samples: usize,
    value_of: F,
    dosage_of: D,
    workspace: GtDecodeWorkspace,
    projection: GenotypeProjection,
    owned: SelectionSlice<'a>,
    retained: HashSet<usize>,
    /// Lazy rather than collected: the whole selection is a usize per variant,
    /// and materialising it per partition costs more than the walk it saves.
    required: Peekable<MergedIndices<'a>>,
    ld_base_index: Option<usize>,
    ld_base: Vec<u8>,
    row: usize,
}

impl<'a, T, F, D> PartitionDecoder<'a, T, F, D>
where
    T: Copy,
    F: Fn(u8) -> T,
    D: Fn(Option<f32>) -> T,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        fileset: &'a PgenFileset,
        field: MatrixField,
        partition: &'a PgenPartition,
        values: &'a mut [T],
        samples: usize,
        value_of: F,
        dosage_of: D,
    ) -> Result<Self> {
        let selected = fileset.selected_samples.source_indices();
        let projection = match field {
            MatrixField::AltCount => GenotypeProjection::alt_count_only(),
            MatrixField::Dosage => GenotypeProjection::from_fields(&["DS".to_string()]),
        };
        let mut retained = HashSet::new();
        for index in partition.required() {
            if let Some(base) = fileset.records.record(index)?.ld_base {
                retained.insert(base);
            }
        }
        Ok(Self {
            fileset,
            field,
            values,
            samples,
            value_of,
            dosage_of,
            workspace: GtDecodeWorkspace::new(fileset.sample_count, selected)?,
            projection,
            owned: partition.owned(),
            retained,
            required: partition.required().peekable(),
            ld_base_index: None,
            ld_base: Vec::with_capacity(fileset.sample_count),
            row: 0,
        })
    }

    /// Decodes every variant of this partition that lives in `range`.
    ///
    /// Mirrors the scan's per-record dispatch — direct dense, fused
    /// common-value, the biallelic fast path, then the general decoder — but
    /// each branch writes its values at their final address instead of into an
    /// Arrow builder.
    fn decode_range(&mut self, range: ByteRange, bytes: &[u8]) -> Result<()> {
        // Destructured so the loop can borrow the LD base and the workspace at
        // the same time, the way it could when these were locals.
        let Self {
            fileset,
            field,
            values,
            samples,
            value_of,
            dosage_of,
            workspace,
            projection,
            owned,
            retained,
            required,
            ld_base_index,
            ld_base,
            row,
        } = self;
        // The row cursor runs in a local rather than through a `&mut` field:
        // the loop is per-record, and reaching through a reference for each
        // step costs measurably at whole-chromosome scale. It is written back
        // once the range is done; an error aborts the whole matrix read, so a
        // cursor left stale on that path is never observed.
        let fileset: &PgenFileset = fileset;
        let field = *field;
        let samples = *samples;
        let values: &mut [T] = values;
        let mut next_row = *row;
        let selected = fileset.selected_samples.source_indices();

        while let Some(&variant_index) = required.peek() {
            let record = fileset.records.record(variant_index)?;
            if record.offset >= range.end {
                break;
            }
            let payload = record_payload(range, bytes, record.offset, record.end(), variant_index)?;
            let base = record
                .ld_base
                .map(|base_index| {
                    if *ld_base_index != Some(base_index) {
                        return Err(DataFusionError::Execution(format!(
                            "PGEN variant {variant_index} dependency base {base_index} was not decoded first"
                        )));
                    }
                    Ok(ld_base.as_slice())
                })
                .transpose()?;

            // A record this partition does not emit is decoded only far enough
            // to serve as the next one's LD base.
            if !owned.contains(variant_index) {
                let main = decode_main_track_and_validate(
                    payload,
                    fileset.mode,
                    record.record_type,
                    variant_index,
                    fileset.sample_count,
                    fileset.variants.allele_count(variant_index),
                    base,
                )?;
                if retained.contains(&variant_index) {
                    *ld_base = main;
                    *ld_base_index = Some(variant_index);
                }
                required.next();
                continue;
            }

            let out = values
                .get_mut(next_row * samples..(next_row + 1) * samples)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "PGEN variant {variant_index} row {next_row} is outside the matrix"
                    ))
                })?;
            let allele_count = fileset.variants.allele_count(variant_index);
            let retain_main = retained.contains(&variant_index);
            let identity = workspace.has_identity_selection();
            let biallelic = supports_biallelic_gt_fast_path(record.record_type, allele_count);

            if identity
                && !retain_main
                && biallelic
                && (fileset.mode == PgenMode::Plink1 || record.record_type & 7 == 0)
            {
                let packed = validated_dense_hardcalls(
                    payload,
                    fileset.mode,
                    record.record_type,
                    variant_index,
                    fileset.sample_count,
                )?;
                for (sample, slot) in out.iter_mut().enumerate() {
                    *slot = value_of(dense_code(packed, sample, fileset.mode));
                }
            } else if identity
                && !retain_main
                && biallelic
                && supports_common_difflist_fast_path(fileset.mode, record.record_type)
            {
                let common = decode_common_difflist_into(
                    workspace,
                    payload,
                    fileset.mode,
                    record.record_type,
                    variant_index,
                    fileset.sample_count,
                )?;
                // One write per sample and nothing read back, which is the whole
                // point of decoding at the destination.
                out.fill(value_of(common));
                for &(sample, value) in workspace.patches() {
                    out[sample] = value_of(value);
                }
            } else if biallelic {
                decode_biallelic_gt_into(
                    workspace,
                    payload,
                    fileset.mode,
                    record.record_type,
                    variant_index,
                    fileset.sample_count,
                    selected,
                    base,
                    retain_main,
                    false,
                )?;
                for (slot, &code) in out.iter_mut().zip(workspace.selected_codes()) {
                    *slot = value_of(code);
                }
                if retain_main {
                    workspace.swap_main_track(ld_base);
                    *ld_base_index = Some(variant_index);
                }
            } else {
                let (decoded, main) = decode_record_and_main(
                    payload,
                    fileset.mode,
                    record.record_type,
                    variant_index,
                    fileset.sample_count,
                    allele_count,
                    *projection,
                    selected,
                    base,
                )?;
                write_general(out, field, &decoded, value_of, dosage_of);
                if retain_main {
                    *ld_base = main;
                    *ld_base_index = Some(variant_index);
                }
            }
            next_row += 1;
            required.next();
        }
        *row = next_row;
        Ok(())
    }
}

/// The internal code of one sample in a packed dense hardcall track.
#[inline]
fn dense_code(packed: &[u8], sample: usize, mode: PgenMode) -> u8 {
    let raw = (packed[sample / 4] >> ((sample % 4) * 2)) & 3;
    if mode == PgenMode::Plink1 {
        match raw {
            0 => 2,
            1 => MISSING_CODE,
            2 => 1,
            _ => 0,
        }
    } else {
        raw
    }
}

/// Writes a record the fast paths could not take, from the general decoder's
/// output rather than from internal codes.
fn write_general<T: Copy, F: Fn(u8) -> T, D: Fn(Option<f32>) -> T>(
    out: &mut [T],
    field: MatrixField,
    decoded: &crate::decode::DecodedRecord,
    value_of: &F,
    dosage_of: &D,
) {
    match field {
        MatrixField::AltCount => {
            for (slot, call) in out.iter_mut().zip(&decoded.gt) {
                *slot = match call {
                    Some(call) => value_of(u8::from(call[0] == 1) + u8::from(call[1] == 1)),
                    None => value_of(MISSING_CODE),
                };
            }
        }
        // This is the branch a record with a real dosage track takes, and its
        // values are genuinely fractional — 0.125 is a dosage a fileset holds.
        // They cannot go through the internal-code mapping, which only has
        // 0, 1 and 2 to say.
        MatrixField::Dosage => {
            for (slot, dosage) in out.iter_mut().zip(&decoded.ds) {
                *slot = dosage_of(*dosage);
            }
        }
    }
}
