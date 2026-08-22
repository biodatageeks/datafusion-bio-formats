//! Shared helpers for the BigWig and BigBed table providers.
//!
//! These utilities are format-agnostic (schema projection, batch construction,
//! genomic-region planning, path validation) and are reused by both
//! [`crate::bigwig`] and [`crate::bigbed`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bigtools::{BBIDataBlock, ChromInfo};
use datafusion::arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion_bio_format_core::genomic_filter::{GenomicRegion, extract_genomic_regions};
use datafusion_bio_format_core::partition_balancer::{RegionSizeEstimate, balance_partitions};

/// Maximum number of rows emitted per [`RecordBatch`] while streaming a region.
///
/// Region iterators are pulled in chunks of this size so peak memory stays
/// bounded (a whole chromosome is never buffered) and `LIMIT`/`COUNT` queries
/// can stop early instead of reading the entire region.
pub(crate) const BBI_BATCH_ROWS: usize = 8192;

/// Empty projections carry only a logical row count, so substantially larger
/// batches reduce scheduler and bridge overhead without allocating Arrow value
/// buffers. This is the common `count(*)` path.
pub(crate) const BBI_EMPTY_PROJECTION_BATCH_ROWS: usize = 4_194_304;

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
    /// Inclusive lower bound on the original interval start owned by this
    /// region. Partitioned queries can return an interval that overlaps the
    /// lower query boundary and is also returned by the preceding partition;
    /// ownership filtering keeps it in exactly one partition.
    pub(crate) ownership_start: Option<u32>,
    /// Exclusive upper bound on the original interval start owned by this
    /// region. This also permits early termination because BBI interval streams
    /// are ordered by start position.
    pub(crate) ownership_end: Option<u32>,
}

/// Index-derived work estimate for one primary BBI data block on one
/// chromosome. Blocks that span a chromosome boundary produce one estimate per
/// covered chromosome; this is rare and reflects that either query can require
/// reading the shared physical block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BbiBlockWork {
    chrom: String,
    start: u32,
    end: u32,
    data_size: u64,
}

/// Convert BigTools' cir-tree leaf layout into chromosome-local work units.
pub(crate) fn bbi_block_work(chroms: &[ChromInfo], blocks: &[BBIDataBlock]) -> Vec<BbiBlockWork> {
    let mut work = Vec::new();
    for block in blocks {
        for chrom in chroms
            .iter()
            .filter(|chrom| (block.start_chrom_id..=block.end_chrom_id).contains(&chrom.id()))
        {
            let start = if chrom.id() == block.start_chrom_id {
                block.start_base
            } else {
                0
            };
            let end = if chrom.id() == block.end_chrom_id {
                block.end_base.min(chrom.length)
            } else {
                chrom.length
            };
            if start < end {
                work.push(BbiBlockWork {
                    chrom: chrom.name.clone(),
                    start,
                    end,
                    data_size: block.data_size,
                });
            }
        }
    }
    work.sort_unstable_by(|left, right| {
        (&left.chrom, left.start, left.end).cmp(&(&right.chrom, right.start, right.end))
    });
    work
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
                    ownership_start: None,
                    ownership_end: None,
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
        ownership_start: None,
        ownership_end: None,
    })
}

/// Balance BBI regions across DataFusion partitions using primary-data
/// cir-tree block sizes and positions as the workload estimate.
///
/// A single selected region deliberately remains serial unless
/// `allow_single_region_split` is true: a narrow chromosome-filtered lookup
/// should not fan out into several independent file opens, while an unfiltered
/// one-chromosome file must still be able to use the requested parallelism.
/// Multi-region scans may always split a dominant chromosome so it does not
/// monopolize one partition.
///
/// When `clips_query_boundaries` is true (BigWig), a split query starts one base
/// before its ownership boundary and extends to the original region end. This
/// prevents `bigtools` from clipping the coordinates of the first/last owned
/// interval. BigBed returns original coordinates, so each shard can query only
/// its own coordinate window.
pub(crate) fn plan_bbi_partitions(
    regions: Vec<BbiScanRegion>,
    target_partitions: usize,
    allow_single_region_split: bool,
    clips_query_boundaries: bool,
    block_work: &[BbiBlockWork],
) -> Vec<Vec<BbiScanRegion>> {
    if regions.is_empty() {
        return vec![Vec::new()];
    }
    if target_partitions <= 1 || (regions.len() == 1 && !allow_single_region_split) {
        return vec![regions];
    }

    let effective_ends = regions
        .iter()
        .map(|region| region.stop_at.unwrap_or(region.end).min(region.end))
        .collect::<Vec<_>>();
    let estimates = regions
        .iter()
        .zip(&effective_ends)
        .enumerate()
        .map(|(index, (region, &effective_end))| {
            let overlapping_blocks = block_work
                .iter()
                .filter(|block| {
                    block.chrom == region.chrom
                        && block.start < effective_end
                        && block.end > region.start
                })
                .collect::<Vec<_>>();
            let estimated_bytes = overlapping_blocks.iter().map(|block| block.data_size).sum();
            let nonempty_block_positions = overlapping_blocks
                .iter()
                .map(|block| u64::from(block.start.max(region.start)) + 1)
                .collect();

            RegionSizeEstimate {
                // The balancer treats coordinates as 1-based inclusive. Use the
                // source index as an opaque key so multiple ranges on one
                // chromosome still map back to the correct original query.
                region: GenomicRegion {
                    chrom: index.to_string(),
                    start: Some(u64::from(region.start) + 1),
                    end: Some(u64::from(effective_end)),
                    unmapped_tail: false,
                },
                estimated_bytes,
                contig_length: Some(u64::from(effective_end)),
                unmapped_count: 0,
                // Cir-tree blocks are variable-width rather than fixed genomic
                // bins. A span of one makes the generic balancer place cuts at
                // observed block positions; encoded data bytes supply the weight.
                nonempty_bin_positions: nonempty_block_positions,
                leaf_bin_span: 1,
            }
        })
        .collect();

    balance_partitions(estimates, target_partitions)
        .into_iter()
        .map(|assignment| {
            assignment
                .regions
                .into_iter()
                .map(|balanced| {
                    let index = balanced
                        .chrom
                        .parse::<usize>()
                        .expect("BBI partition planner generated a non-numeric region key");
                    let original = &regions[index];
                    let effective_end = effective_ends[index];
                    let ownership_start = balanced
                        .start
                        .map(|start| start.saturating_sub(1) as u32)
                        .unwrap_or(original.start);
                    let ownership_end = balanced.end.map(|end| end as u32).unwrap_or(effective_end);

                    if ownership_start == original.start && ownership_end == effective_end {
                        return original.clone();
                    }

                    let first_shard = ownership_start == original.start;
                    if clips_query_boundaries {
                        BbiScanRegion {
                            chrom: original.chrom.clone(),
                            start: if first_shard {
                                original.start
                            } else {
                                ownership_start.saturating_sub(1)
                            },
                            // BigWig clips values at the query end, so retain the
                            // original end and stop after the last owned start.
                            end: original.end,
                            stop_at: Some(ownership_end),
                            ownership_start: Some(ownership_start),
                            ownership_end: Some(ownership_end),
                        }
                    } else {
                        BbiScanRegion {
                            chrom: original.chrom.clone(),
                            start: if first_shard {
                                original.start
                            } else {
                                ownership_start
                            },
                            end: ownership_end,
                            stop_at: None,
                            // The first shard owns records that start before a
                            // filtered query window but overlap that window.
                            ownership_start: (!first_shard).then_some(ownership_start),
                            ownership_end: Some(ownership_end),
                        }
                    }
                })
                .collect()
        })
        .collect()
}

/// Estimate on-disk data bytes read by each planned partition. Boundary blocks
/// can appear in two partitions because both independent index queries may
/// legitimately read them; retaining that duplication makes the diagnostic
/// closer to actual I/O than an ownership-only estimate.
pub(crate) fn partition_estimated_bytes(
    partitions: &[Vec<BbiScanRegion>],
    block_work: &[BbiBlockWork],
) -> Vec<u64> {
    partitions
        .iter()
        .map(|regions| {
            regions
                .iter()
                .map(|region| {
                    let effective_end = region
                        .ownership_end
                        .or(region.stop_at)
                        .unwrap_or(region.end)
                        .min(region.end);
                    block_work
                        .iter()
                        .filter(|block| {
                            block.chrom == region.chrom
                                && block.start < effective_end
                                && block.end > region.start
                        })
                        .map(|block| block.data_size)
                        .sum::<u64>()
                })
                .sum()
        })
        .collect()
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
