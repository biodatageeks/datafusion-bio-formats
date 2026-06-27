//! Shared helpers for the BigWig and BigBed table providers.
//!
//! These utilities are format-agnostic (schema projection, batch construction,
//! genomic-region planning, path validation) and are reused by both
//! [`crate::bigwig`] and [`crate::bigbed`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion_bio_format_core::genomic_filter::{GenomicRegion, extract_genomic_regions};

/// Maximum number of rows emitted per [`RecordBatch`] while streaming a region.
///
/// Region iterators are pulled in chunks of this size so peak memory stays
/// bounded (a whole chromosome is never buffered) and `LIMIT`/`COUNT` queries
/// can stop early instead of reading the entire region.
pub(crate) const BBI_BATCH_ROWS: usize = 8192;

/// Native BBI interval query region in 0-based half-open coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BbiScanRegion {
    pub(crate) chrom: String,
    pub(crate) start: u32,
    pub(crate) end: u32,
    /// Exclusive upper bound on interval `start` for early termination, in the
    /// same 0-based coordinate space as the native interval `start`. `None`
    /// means "no upper bound — drain the whole region".
    ///
    /// Used only by the BigWig provider: it scans whole chromosomes to avoid
    /// coordinate clipping, but the streamed intervals are start-sorted, so it
    /// can stop as soon as `start >= stop_at` rather than reading to the end.
    pub(crate) stop_at: Option<u32>,
}

pub(crate) fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        Some(indices) => Arc::new(Schema::new_with_metadata(
            indices
                .iter()
                .map(|&index| schema.field(index).clone())
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        )),
        None => schema.clone(),
    }
}

pub(crate) fn projected_indices(projection: Option<&[usize]>, width: usize) -> Vec<usize> {
    projection
        .map(|indices| indices.to_vec())
        .unwrap_or_else(|| (0..width).collect())
}

pub(crate) fn build_batch(
    schema: SchemaRef,
    arrays: Vec<ArrayRef>,
    row_count: usize,
) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    RecordBatch::try_new_with_options(schema, arrays, &options)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

pub(crate) fn to_external_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

pub(crate) fn projection_display(schema: &SchemaRef) -> String {
    if schema.fields().is_empty() {
        String::new()
    } else {
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Plan the native interval-query regions for a BBI scan.
///
/// `widen_to_chromosome` controls how a positional coordinate filter is turned
/// into a query window:
///
/// * `false` (BigBed) — honor the filter's start/end bounds. `BigBedRead`
///   returns full BED entries that *overlap* the window, so narrowing the window
///   prunes work without altering the emitted coordinates.
/// * `true` (BigWig) — ignore the positional bounds and scan each matched
///   chromosome in full. `BigWigRead` *clips* interval values to the query
///   window, so a sub-range would emit truncated start/end coordinates. Because
///   coordinate filters are pushed down as `Inexact` (DataFusion re-applies
///   them), scanning the whole chromosome is still correct — it only trades
///   within-chromosome seek pruning for unclipped intervals.
pub(crate) fn plan_bbi_scan_regions(
    filters: &[Expr],
    chroms: &[(String, u32)],
    coordinate_system_zero_based: bool,
    widen_to_chromosome: bool,
) -> Vec<BbiScanRegion> {
    let analysis = extract_genomic_regions(filters, coordinate_system_zero_based);
    if analysis.unsatisfiable {
        return Vec::new();
    }

    let source_regions = if analysis.regions.is_empty() {
        chroms
            .iter()
            .map(|(chrom, _)| GenomicRegion {
                chrom: chrom.clone(),
                start: None,
                end: None,
                unmapped_tail: false,
            })
            .collect::<Vec<_>>()
    } else {
        analysis.regions
    };

    // O(1) chromosome-length lookup; the `chroms` vec still drives ordering of
    // the no-filter, whole-genome expansion above.
    let chrom_lengths: HashMap<&str, u32> = chroms
        .iter()
        .map(|(chrom, len)| (chrom.as_str(), *len))
        .collect();

    if widen_to_chromosome {
        // De-duplicate by chromosome so overlapping or OR'd ranges never scan the
        // same chromosome twice (which would duplicate rows).
        let mut seen = HashSet::new();
        return source_regions
            .into_iter()
            .filter(|region| seen.insert(region.chrom.clone()))
            .filter_map(|region| {
                let length = *chrom_lengths.get(region.chrom.as_str())?;
                // Scan the whole chromosome (so intervals are never clipped) but
                // remember the filter's upper bound: `region.end` is 1-based
                // inclusive, which equals the 0-based exclusive bound on `start`,
                // letting the start-sorted stream stop early. `None` (no upper
                // bound) means drain the whole chromosome.
                let stop_at = region.end.map(|end| end.min(length as u64) as u32);
                (length > 0).then_some(BbiScanRegion {
                    chrom: region.chrom,
                    start: 0,
                    end: length,
                    stop_at,
                })
            })
            .collect();
    }

    source_regions
        .into_iter()
        .filter_map(|region| convert_genomic_region_to_bbi(region, &chrom_lengths))
        .collect()
}

fn convert_genomic_region_to_bbi(
    region: GenomicRegion,
    chrom_lengths: &HashMap<&str, u32>,
) -> Option<BbiScanRegion> {
    let length = *chrom_lengths.get(region.chrom.as_str())?;
    let start = region
        .start
        .map(|start| start.saturating_sub(1).min(length as u64) as u32)
        .unwrap_or(0);
    let end = region
        .end
        .map(|end| end.min(length as u64) as u32)
        .unwrap_or(length);

    // The narrow (BigBed) path bounds the scan with the query window itself, so
    // it needs no separate early-termination cursor.
    (start < end).then_some(BbiScanRegion {
        chrom: region.chrom,
        start,
        end,
        stop_at: None,
    })
}

pub(crate) fn region_display(regions: &[BbiScanRegion]) -> String {
    regions
        .iter()
        .map(|region| format!("{}:{}-{}", region.chrom, region.start, region.end))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Validate a user-supplied path for the local-only BBI providers.
///
/// Remote URI schemes (`s3://`, `gs://`, ...) are rejected with a clear error.
/// A `file://` URI is accepted and normalized to a bare filesystem path, since
/// `BigWigRead`/`BigBedRead` open plain paths rather than URIs.
pub(crate) fn normalize_local_path(path: &str, format_name: &str) -> Result<String> {
    if let Some(rest) = path.strip_prefix("file://") {
        // Per RFC 8089 the authority must be empty or `localhost` for a local
        // path: `file:///abs/path` or `file://localhost/abs/path`. A non-empty,
        // non-`localhost` authority names a remote host and is unsupported.
        let local = if rest.starts_with('/') {
            rest
        } else if let Some(after_host) = rest.strip_prefix("localhost") {
            // Keep `after_host`'s leading slash so the result stays absolute.
            // Reject `localhostfoo/...` (authority that merely starts with it).
            if after_host.is_empty() || after_host.starts_with('/') {
                after_host
            } else {
                return Err(remote_host_error(format_name));
            }
        } else {
            return Err(remote_host_error(format_name));
        };
        return Ok(local.to_string());
    }
    if path.contains("://") {
        return Err(DataFusionError::NotImplemented(format!(
            "{format_name} only supports local filesystem paths in this version"
        )));
    }
    Ok(path.to_string())
}

fn remote_host_error(format_name: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "{format_name} does not support remote file:// URIs with a host authority"
    ))
}
