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
use crate::physical_exec::{BgenPartition, slice_from_range};
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
    pub fn positions(&self) -> Vec<u64> {
        self.provider
            .fileset()
            .catalog
            .variants
            .iter()
            .map(|variant| variant.position as u64)
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
                .unwrap_or(cursor);
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

        // The ranges are fetched here because the object source is async;
        // decoding is what runs in parallel afterwards. Input is bounded by the
        // range planner rather than by the file.
        let mut loaded = Vec::with_capacity(partitions.len());
        for partition in &partitions {
            let mut ranges = Vec::with_capacity(partition.ranges.len());
            for range in &partition.ranges {
                let bytes = fileset
                    .source
                    .read_range(&fileset.path, range.range.start..range.range.end)
                    .await?;
                ranges.push((range.range, bytes.to_vec()));
            }
            loaded.push(ranges);
        }

        let mut rest = values;
        let mut slices = Vec::with_capacity(partitions.len());
        for rows in &owned {
            let (head, tail) = rest.split_at_mut(rows * shape.samples);
            slices.push(head);
            rest = tail;
        }

        std::thread::scope(|scope| {
            let handles = partitions
                .iter()
                .zip(loaded.iter())
                .zip(slices)
                .map(|((partition, ranges), slice)| {
                    let fileset = Arc::clone(&fileset);
                    let options = self.options.clone();
                    scope.spawn(move || {
                        decode_partition(
                            &fileset,
                            &options,
                            partition,
                            ranges,
                            slice,
                            shape.samples,
                            missing,
                        )
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

type LoadedRanges = Vec<(
    datafusion_bio_format_core::range_planning::ByteRange,
    Vec<u8>,
)>;

/// Decodes one partition's variants into its own slice of the matrix.
fn decode_partition(
    fileset: &BgenFileset,
    options: &BgenReadOptions,
    partition: &BgenPartition,
    ranges: &LoadedRanges,
    values: &mut [f32],
    samples: usize,
    missing: f32,
) -> Result<()> {
    let selected_samples = fileset.selected_samples.source_indices();
    let mut scratch = DecodeScratch::new();
    // Dosage layout, no PLOIDY: this path emits one value per sample and
    // nothing else, so the buffer holds exactly one row at a time.
    let mut buffers = GenotypeBuffers::new(BufferLayout::Dosage, false);
    let mut row = 0_usize;

    for ((range, bytes), planned) in ranges.iter().zip(&partition.ranges) {
        for &variant_index in &planned.variants {
            let catalogued = &fileset.catalog.variants[variant_index];
            let span = catalogued.scan_span();
            let record = slice_from_range(bytes, range.start, span, variant_index)?;
            let owned;
            let resolved = if catalogued.is_resolved() {
                catalogued
            } else {
                owned =
                    resolve_variant(&fileset.path, catalogued, record, &fileset.header, options)?;
                &owned
            };
            let payload_span = resolved.payload_span().ok_or_else(|| {
                DataFusionError::Internal(
                    "BGEN variant stayed unresolved after being parsed".to_string(),
                )
            })?;
            let payload = slice_from_range(bytes, range.start, payload_span, variant_index)?;

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
            row += 1;
        }
    }
    Ok(())
}
