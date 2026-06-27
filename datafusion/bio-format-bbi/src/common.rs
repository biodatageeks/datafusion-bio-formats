//! Shared helpers for the BigWig and BigBed table providers.
//!
//! These utilities are format-agnostic (schema projection, batch construction,
//! genomic-region planning, path validation) and are reused by both
//! [`crate::bigwig`] and [`crate::bigbed`].

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion_bio_format_core::genomic_filter::{GenomicRegion, extract_genomic_regions};

/// Native BBI interval query region in 0-based half-open coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BbiScanRegion {
    pub(crate) chrom: String,
    pub(crate) start: u32,
    pub(crate) end: u32,
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

pub(crate) fn plan_bbi_scan_regions(
    filters: &[Expr],
    chroms: &[(String, u32)],
    coordinate_system_zero_based: bool,
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

    (start < end).then_some(BbiScanRegion {
        chrom: region.chrom,
        start,
        end,
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
        // Accept `file:///abs/path` and the optional `file://localhost/abs/path`
        // authority form, both of which map to a local filesystem path.
        let local = rest.strip_prefix("localhost").unwrap_or(rest);
        return Ok(local.to_string());
    }
    if path.contains("://") {
        return Err(DataFusionError::NotImplemented(format!(
            "{format_name} only supports local filesystem paths in this version"
        )));
    }
    Ok(path.to_string())
}
