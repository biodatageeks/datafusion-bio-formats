//! Decoding BGEN dosages straight into a caller-owned matrix.
//!
//! The scan builds Arrow batches, and a caller that wants one dense array then
//! consolidates them. On a whole chromosome that consolidation is a serial pass
//! over 10 GB — measured at 1.2–1.8 s, against a scan that divides 6.3× across
//! eight partitions — so it becomes the ceiling as partitions are added.
//!
//! This path removes it. Each partition decodes its variants and writes each
//! row at its final address, so the values cross one buffer that stays in cache
//! rather than a 10 GB one that does not.
//!
//! The decode itself is the scan's, variant for variant: a matrix row is one
//! variant's dosages, so the existing per-variant buffer is filled and moved
//! into the destination rather than accumulated into a batch. Every layout,
//! codec and fast path the scan supports is therefore supported here without a
//! second implementation to keep in step.

use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};

use crate::buffers::{BufferLayout, GenotypeBuffers};
use crate::catalog::resolve_variant;
use crate::decode::{DecodeScratch, decode_variant};
use crate::physical_exec::{BgenReadRange, slice_from_range};
use crate::table_provider::{
    BgenFileset, BgenOutputMode, BgenReadOptions, BgenTableProvider, plan_payload_partitions,
};

/// The shape a fileset will produce, reported before any genotype is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MatrixShape {
    /// Rows, one per variant.
    pub variants: usize,
    /// Columns, one per selected sample.
    pub samples: usize,
}

/// An opened BGEN, ready to decode into a destination.
///
/// Opening reads the header and builds or validates the catalog, which for a
/// file without a usable index is a walk of every record. A caller must learn
/// the shape before it can allocate, and asking must not cost that twice.
pub struct GenotypeMatrixReader {
    provider: BgenTableProvider,
    options: BgenReadOptions,
}

impl GenotypeMatrixReader {
    /// Opens a fileset and reads its metadata.
    pub async fn open(path: impl Into<String>, options: BgenReadOptions) -> Result<Self> {
        if options.output_mode != BgenOutputMode::Dosage {
            return Err(DataFusionError::Plan(
                "BGEN matrix reads are dosage only; probabilities are variable width and have \
                 no single dense shape"
                    .to_string(),
            ));
        }
        let provider = BgenTableProvider::try_new(path, options.clone()).await?;
        Ok(Self { provider, options })
    }

    /// The shape a decode will produce.
    pub fn shape(&self) -> MatrixShape {
        let fileset = self.provider.fileset();
        MatrixShape {
            variants: fileset.catalog.variants.len(),
            samples: fileset.selected_samples.source_indices().len(),
        }
    }

    /// The selected sample names, in column order.
    pub fn sample_names(&self) -> &[String] {
        self.provider.fileset().selected_samples.names()
    }

    /// The variant start positions, in row order.
    ///
    /// These are the `start` the scan emits, which the configured coordinate
    /// system has already adjusted — not the raw one-based BGEN position, which
    /// would label every row one base later than the DataFrame path under the
    /// zero-based default. Allocated per call; a caller that wants them once
    /// should keep the result rather than ask again.
    pub fn positions(&self) -> Vec<u64> {
        self.provider
            .fileset()
            .catalog
            .variants
            .iter()
            .map(|variant| variant.start)
            .collect()
    }

    /// Decodes every variant's dosages into `values`, row-major.
    ///
    /// `values` must be exactly `variants * samples` long; `missing` is written
    /// where a sample has no called genotype.
    pub async fn read_into(
        &self,
        values: &mut [f32],
        missing: f32,
        threads: usize,
    ) -> Result<MatrixShape> {
        let shape = self.shape();
        let expected = shape
            .variants
            .checked_mul(shape.samples)
            .ok_or_else(|| DataFusionError::Execution("BGEN matrix size overflowed".to_string()))?;
        if values.len() != expected {
            return Err(DataFusionError::Execution(format!(
                "BGEN matrix destination has {} values; expected {expected} ({} variants x {} samples)",
                values.len(),
                shape.variants,
                shape.samples
            )));
        }
        if expected == 0 {
            return Ok(shape);
        }

        let fileset = Arc::clone(self.provider.fileset());
        let selected = (0..shape.variants).collect::<Vec<_>>();
        let partitions = plan_payload_partitions(
            &selected,
            &fileset.catalog,
            threads.max(1),
            self.options.max_range_gap,
            self.options.max_range_bytes,
        )?;

        // A whole-file read gives every variant a row index equal to its
        // position, so a partition owns a contiguous row range and the
        // destination splits into pieces the workers never share.
        // A partition's variants hang off its ranges rather than the partition,
        // because a range is what the scan fetches and a variant belongs to
        // exactly one of them.
        let mut cursor = 0_usize;
        let mut owned = Vec::with_capacity(partitions.len());
        for partition in &partitions {
            let rows: usize = partition
                .ranges
                .iter()
                .map(|range| range.variants.len())
                .sum();
            if rows == 0 {
                owned.push(0);
                continue;
            }
            let first = partition
                .ranges
                .iter()
                .flat_map(|range| range.variants.iter())
                .copied()
                .min()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "BGEN matrix partition reports rows but names no variants".to_string(),
                    )
                })?;
            if first != cursor {
                return Err(DataFusionError::Execution(
                    "BGEN matrix partitions are not contiguous in variant order".to_string(),
                ));
            }
            cursor += rows;
            owned.push(rows);
        }
        if cursor != shape.variants {
            return Err(DataFusionError::Execution(format!(
                "BGEN matrix partitions cover {cursor} of {} variants",
                shape.variants
            )));
        }

        // Precomputed so a range carries no state from the one before it: a
        // range's rows are its variants, in order, so where each starts is
        // known before any of them is read. That is what lets the input be
        // fetched a round at a time rather than all at once.
        let mut slices = Vec::with_capacity(partitions.len());
        let mut rest = values;
        for rows in &owned {
            let (head, tail) = rest.split_at_mut(rows * shape.samples);
            slices.push(head);
            rest = tail;
        }
        let starts = partitions
            .iter()
            .map(|partition| {
                partition
                    .ranges
                    .iter()
                    .scan(0_usize, |row, range| {
                        let start = *row;
                        *row += range.variants.len();
                        Some(start)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        // One range per partition per round. Buffering every range of every
        // partition first would hold the whole compressed file — and on a
        // sample subset the destination is the small half — so resident input
        // is bounded by the range planner instead.
        let rounds = partitions
            .iter()
            .map(|partition| partition.ranges.len())
            .max()
            .unwrap_or(0);
        for round in 0..rounds {
            // `Bytes` is Arc-backed, so a range is shared with the decoder
            // rather than copied out of the read.
            let mut loaded: Vec<Option<(&BgenReadRange, bytes::Bytes)>> =
                Vec::with_capacity(partitions.len());
            for partition in &partitions {
                match partition.ranges.get(round) {
                    Some(planned) => {
                        let bytes = fileset
                            .source
                            .read_range(&fileset.path, planned.range.start..planned.range.end)
                            .await?;
                        loaded.push(Some((planned, bytes)));
                    }
                    None => loaded.push(None),
                }
            }
            let borrowed = loaded
                .iter()
                .map(|slot| slot.as_ref().map(|(planned, bytes)| (*planned, &bytes[..])))
                .collect::<Vec<_>>();

            std::thread::scope(|scope| {
                let handles = borrowed
                    .iter()
                    .zip(slices.iter_mut())
                    .zip(&starts)
                    .filter_map(|((slot, slice), partition_starts)| {
                        slot.map(|(planned, bytes)| {
                            let fileset = Arc::clone(&fileset);
                            let options = self.options.clone();
                            let start = partition_starts[round];
                            let samples = shape.samples;
                            scope.spawn(move || {
                                decode_range(
                                    &fileset, &options, planned, bytes, slice, samples, missing,
                                    start,
                                )
                            })
                        })
                    })
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .map(|handle| {
                        handle.join().map_err(|_| {
                            DataFusionError::Execution("BGEN matrix decoder panicked".to_string())
                        })?
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
        }
        Ok(shape)
    }
}

/// Reports the shape a fileset would produce, without decoding anything.
pub async fn genotype_matrix_shape(
    path: impl Into<String>,
    options: BgenReadOptions,
) -> Result<MatrixShape> {
    Ok(GenotypeMatrixReader::open(path, options).await?.shape())
}

/// Decodes a whole fileset's dosages into `values`.
pub async fn read_genotype_matrix(
    path: impl Into<String>,
    options: BgenReadOptions,
    values: &mut [f32],
    missing: f32,
    threads: usize,
) -> Result<(MatrixShape, Vec<u64>)> {
    let reader = GenotypeMatrixReader::open(path, options).await?;
    let positions = reader.positions();
    let shape = reader.read_into(values, missing, threads).await?;
    Ok((shape, positions))
}

/// Decodes one byte range's variants into the partition's slice of the matrix.
///
/// Carries nothing between calls: `start` is where this range's rows begin, and
/// a range's variants are contiguous rows in order, so the input can be fetched
/// a round at a time without a resumable decoder.
#[allow(clippy::too_many_arguments)]
fn decode_range(
    fileset: &BgenFileset,
    options: &BgenReadOptions,
    planned: &BgenReadRange,
    bytes: &[u8],
    values: &mut [f32],
    samples: usize,
    missing: f32,
    start: usize,
) -> Result<()> {
    let selected_samples = fileset.selected_samples.source_indices();
    let mut scratch = DecodeScratch::new();
    // Dosage layout, no PLOIDY: this path emits one value per sample and
    // nothing else, so the buffer holds exactly one row at a time.
    let mut buffers = GenotypeBuffers::new(BufferLayout::Dosage, false);

    for (offset, &variant_index) in planned.variants.iter().enumerate() {
        let catalogued = &fileset.catalog.variants[variant_index];
        let record = slice_from_range(
            bytes,
            planned.range.start,
            catalogued.scan_span(),
            variant_index,
        )?;
        let owned;
        let resolved = if catalogued.is_resolved() {
            catalogued
        } else {
            owned = resolve_variant(&fileset.path, catalogued, record, &fileset.header, options)?;
            &owned
        };
        let payload_span = resolved.payload_span().ok_or_else(|| {
            DataFusionError::Internal(
                "BGEN variant stayed unresolved after being parsed".to_string(),
            )
        })?;
        let payload = slice_from_range(bytes, planned.range.start, payload_span, variant_index)?;

        buffers.reset();
        decode_variant(
            &fileset.path,
            resolved,
            &fileset.header,
            payload,
            selected_samples,
            options,
            &mut scratch,
            &mut buffers,
        )?;

        let row = start + offset;
        let out = values
            .get_mut(row * samples..(row + 1) * samples)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "BGEN variant {variant_index} row {row} is outside the matrix"
                ))
            })?;
        let decoded = buffers.values();
        if decoded.len() != samples {
            return Err(DataFusionError::Execution(format!(
                "BGEN variant {variant_index} decoded {} values; expected {samples}",
                decoded.len()
            )));
        }
        // A missing dosage is NaN in the buffer; the caller chooses what it
        // wants to see instead.
        for (slot, &value) in out.iter_mut().zip(decoded) {
            *slot = if value.is_nan() { missing } else { value };
        }
    }
    Ok(())
}
