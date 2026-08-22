//! Shared helpers for the BigWig and BigBed table providers.
//!
//! These utilities are format-agnostic (schema projection, batch construction,
//! genomic-region planning, path validation) and are reused by both
//! [`crate::bigwig`] and [`crate::bigbed`].

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bigtools::{BBIDataBlock, ChromInfo};
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

/// Native regions plus whether they came from an indexable genomic selection.
pub(crate) struct BbiScanPlan {
    pub(crate) regions: Vec<BbiScanRegion>,
    pub(crate) has_explicit_region: bool,
}

/// Index-derived work estimate for one primary BBI data block on one
/// chromosome. Blocks that span a chromosome boundary produce one estimate per
/// covered chromosome; this is rare and reflects that either query can require
/// reading the shared physical block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BbiBlockWork {
    start: u32,
    end: u32,
    data_size: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BbiBlockIndex {
    by_chrom: HashMap<String, BbiChromWork>,
}

#[derive(Clone, Debug)]
struct BbiChromWork {
    blocks: Vec<BbiBlockWork>,
    prefix_max_end: Vec<u32>,
}

/// Convert BigTools' cir-tree leaf layout into chromosome-local work units.
pub(crate) fn bbi_block_index(chroms: &[ChromInfo], blocks: &[BBIDataBlock]) -> BbiBlockIndex {
    let mut chroms_by_id = chroms.iter().collect::<Vec<_>>();
    chroms_by_id.sort_unstable_by_key(|chrom| chrom.id());

    let mut by_chrom: HashMap<String, Vec<BbiBlockWork>> = HashMap::new();
    for block in blocks {
        let first = chroms_by_id.partition_point(|chrom| chrom.id() < block.start_chrom_id);
        let last = chroms_by_id.partition_point(|chrom| chrom.id() <= block.end_chrom_id);
        if first >= last {
            continue;
        }
        for chrom in &chroms_by_id[first..last] {
            let start = if chrom.id() == block.start_chrom_id {
                block.start_base.min(chrom.length)
            } else {
                0
            };
            let end = if chrom.id() == block.end_chrom_id {
                block.end_base.min(chrom.length)
            } else {
                chrom.length
            };
            // A zero-width BigBed insertion can be the only record in a real
            // encoded block. Retain it as work even though it has no span.
            if start <= end {
                by_chrom
                    .entry(chrom.name.clone())
                    .or_default()
                    .push(BbiBlockWork {
                        start,
                        end,
                        data_size: block.data_size,
                    });
            }
        }
    }

    let by_chrom = by_chrom
        .into_iter()
        .map(|(chrom, mut blocks)| {
            blocks.sort_unstable_by_key(|block| (block.start, block.end));
            let mut maximum_end = 0;
            let prefix_max_end = blocks
                .iter()
                .map(|block| {
                    maximum_end = maximum_end.max(block.end);
                    maximum_end
                })
                .collect();
            (
                chrom,
                BbiChromWork {
                    blocks,
                    prefix_max_end,
                },
            )
        })
        .collect();
    BbiBlockIndex { by_chrom }
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
/// Both providers preserve original record coordinates for overlapping values,
/// so extracted positional bounds can be passed directly to their index query.
pub(crate) fn plan_bbi_scan_regions(
    filters: &[Expr],
    chroms: &[(String, u32)],
    coordinate_system_zero_based: bool,
) -> BbiScanPlan {
    let analysis = extract_genomic_regions(filters, coordinate_system_zero_based);
    let has_explicit_region = !analysis.regions.is_empty();
    if analysis.unsatisfiable {
        return BbiScanPlan {
            regions: Vec::new(),
            has_explicit_region,
        };
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

    let regions = source_regions
        .into_iter()
        .filter_map(|region| convert_genomic_region_to_bbi(region, &chrom_lengths))
        .collect();
    BbiScanPlan {
        regions,
        has_explicit_region,
    }
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
        ownership_start: None,
        ownership_end: None,
    })
}

#[derive(Clone, Copy, Debug)]
struct BbiWorkSegment {
    region_index: usize,
    start: u32,
    end: u32,
    estimated_bytes: u64,
}

/// Balance BBI regions across DataFusion partitions at primary-data block
/// boundaries, using each block's encoded byte size as its weight.
///
/// A single selected region deliberately remains serial unless
/// `allow_single_region_split` is true: a narrow chromosome-filtered lookup
/// should not fan out into several independent file opens, while an unfiltered
/// one-chromosome file must still be able to use the requested parallelism.
/// Multi-region scans may always split a dominant chromosome so it does not
/// monopolize one partition.
///
/// The number of partitions is capped at the number of coordinate-overlap
/// connected block components. This avoids opening multiple readers that all
/// decode a long block spanning several later block starts. A missing layout
/// means traversal hit its safety limit, so the optimization falls back to one
/// serial partition.
pub(crate) fn plan_bbi_partitions(
    regions: Vec<BbiScanRegion>,
    target_partitions: usize,
    allow_single_region_split: bool,
    block_index: Option<&BbiBlockIndex>,
) -> Vec<Vec<BbiScanRegion>> {
    if regions.is_empty() {
        return vec![Vec::new()];
    }
    let Some(block_index) = block_index else {
        return vec![regions];
    };
    if target_partitions <= 1 || (regions.len() == 1 && !allow_single_region_split) {
        return vec![regions];
    }

    let segments = regions
        .iter()
        .enumerate()
        .flat_map(|(region_index, region)| region_work_segments(region_index, region, block_index))
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return vec![Vec::new()];
    }
    let ranges = weighted_segment_ranges(&segments, target_partitions);

    ranges
        .into_iter()
        .map(|range| segments_to_regions(&segments[range], &regions))
        .collect()
}

fn candidate_blocks<'a>(
    block_index: &'a BbiBlockIndex,
    region: &BbiScanRegion,
) -> &'a [BbiBlockWork] {
    let Some(work) = block_index.by_chrom.get(&region.chrom) else {
        return &[];
    };
    let upper = work
        .blocks
        .partition_point(|block| block.start <= region.end);
    let lower = work.prefix_max_end[..upper].partition_point(|&end| end < region.start);
    &work.blocks[lower..upper]
}

#[derive(Clone, Copy, Debug)]
struct BbiBlockComponent {
    start: u32,
    maximum_end: u32,
    estimated_bytes: u64,
}

fn region_work_segments(
    region_index: usize,
    region: &BbiScanRegion,
    block_index: &BbiBlockIndex,
) -> Vec<BbiWorkSegment> {
    let mut components: Vec<BbiBlockComponent> = Vec::new();
    for block in candidate_blocks(block_index, region)
        .iter()
        .filter(|block| block.end >= region.start)
    {
        let start = block.start.max(region.start).min(region.end);
        let end = block.end.max(start).min(region.end);
        if let Some(component) = components.last_mut()
            && (component.start == start || start < component.maximum_end)
        {
            component.maximum_end = component.maximum_end.max(end);
            component.estimated_bytes = component.estimated_bytes.saturating_add(block.data_size);
        } else {
            components.push(BbiBlockComponent {
                start,
                maximum_end: end,
                estimated_bytes: block.data_size,
            });
        }
    }

    if components.is_empty() {
        return Vec::new();
    }
    // Coordinates before the first block contain no independent work, so keep
    // them with the first block instead of creating an empty shard.
    components[0].start = region.start;

    // A zero-width insertion at the region's inclusive lookup endpoint is read
    // by the preceding query. Charge that work there because it cannot form a
    // non-empty coordinate segment of its own.
    if components.len() > 1
        && components
            .last()
            .is_some_and(|component| component.start == region.end)
    {
        let endpoint = components.pop().expect("endpoint component is present");
        let preceding = components
            .last_mut()
            .expect("preceding component is present");
        preceding.estimated_bytes = preceding
            .estimated_bytes
            .saturating_add(endpoint.estimated_bytes);
    }

    components
        .iter()
        .enumerate()
        .filter_map(|(index, component)| {
            let end = components
                .get(index + 1)
                .map_or(region.end, |next| next.start);
            (component.start < end).then_some(BbiWorkSegment {
                region_index,
                start: component.start,
                end,
                estimated_bytes: component.estimated_bytes,
            })
        })
        .collect()
}

fn weighted_segment_ranges(
    segments: &[BbiWorkSegment],
    target_partitions: usize,
) -> Vec<Range<usize>> {
    let partition_count = target_partitions.max(1).min(segments.len());
    let mut prefix_bytes = Vec::with_capacity(segments.len() + 1);
    prefix_bytes.push(0u128);
    for segment in segments {
        let next = prefix_bytes.last().copied().unwrap_or(0) + u128::from(segment.estimated_bytes);
        prefix_bytes.push(next);
    }
    let total_bytes = *prefix_bytes.last().unwrap_or(&0);

    let mut ranges = Vec::with_capacity(partition_count);
    let mut start = 0;
    for partition in 0..partition_count {
        let remaining_partitions = partition_count - partition - 1;
        if remaining_partitions == 0 {
            ranges.push(start..segments.len());
            break;
        }

        let latest_end = segments.len() - remaining_partitions;
        let end = if total_bytes == 0 {
            (segments.len() * (partition + 1) / partition_count).clamp(start + 1, latest_end)
        } else {
            let target = total_bytes * (partition + 1) as u128 / partition_count as u128;
            closest_prefix_cut(&prefix_bytes, start + 1, latest_end, target)
        };
        ranges.push(start..end);
        start = end;
    }
    ranges
}

fn closest_prefix_cut(prefix: &[u128], lower: usize, upper: usize, target: u128) -> usize {
    let candidates = &prefix[lower..=upper];
    let offset = candidates.partition_point(|&value| value < target);
    let upper_candidate = if offset < candidates.len() {
        lower + offset
    } else {
        upper
    };
    let lower_candidate = upper_candidate.saturating_sub(1).max(lower);
    if prefix[lower_candidate].abs_diff(target) <= prefix[upper_candidate].abs_diff(target) {
        lower_candidate
    } else {
        upper_candidate
    }
}

fn segments_to_regions(
    segments: &[BbiWorkSegment],
    originals: &[BbiScanRegion],
) -> Vec<BbiScanRegion> {
    let mut regions = Vec::new();
    let mut start = 0;
    while start < segments.len() {
        let region_index = segments[start].region_index;
        let mut end = start + 1;
        while end < segments.len() && segments[end].region_index == region_index {
            end += 1;
        }

        let original = &originals[region_index];
        let shard_start = segments[start].start;
        let shard_end = segments[end - 1].end;
        if shard_start == original.start && shard_end == original.end {
            regions.push(original.clone());
        } else {
            regions.push(BbiScanRegion {
                chrom: original.chrom.clone(),
                start: shard_start,
                end: shard_end,
                // The first shard keeps records that begin before a filtered
                // query window but overlap it. Later shards discard that overlap
                // because the preceding shard owns those record starts.
                ownership_start: (shard_start != original.start).then_some(shard_start),
                ownership_end: (shard_end != original.end).then_some(shard_end),
            });
        }
        start = end;
    }
    regions
}

/// Estimate on-disk data bytes read by each planned partition. Boundary blocks
/// can appear in two partitions because both independent index queries may
/// legitimately read them; retaining that duplication makes the diagnostic
/// closer to actual I/O than an ownership-only estimate.
pub(crate) fn partition_estimated_bytes(
    partitions: &[Vec<BbiScanRegion>],
    block_index: Option<&BbiBlockIndex>,
) -> Vec<u64> {
    let Some(block_index) = block_index else {
        return vec![0; partitions.len()];
    };
    partitions
        .iter()
        .map(|regions| {
            regions
                .iter()
                .map(|region| {
                    candidate_blocks(block_index, region)
                        .iter()
                        .filter(|block| block.end >= region.start)
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

#[cfg(test)]
mod partition_tests {
    use super::*;

    fn region() -> BbiScanRegion {
        BbiScanRegion {
            chrom: "chr1".into(),
            start: 0,
            end: 400,
            ownership_start: None,
            ownership_end: None,
        }
    }

    fn block_index(blocks: &[(u32, u32, u64)]) -> BbiBlockIndex {
        let blocks = blocks
            .iter()
            .map(|&(start, end, data_size)| BbiBlockWork {
                start,
                end,
                data_size,
            })
            .collect::<Vec<_>>();
        let mut maximum_end = 0;
        let prefix_max_end = blocks
            .iter()
            .map(|block| {
                maximum_end = maximum_end.max(block.end);
                maximum_end
            })
            .collect();
        BbiBlockIndex {
            by_chrom: HashMap::from([(
                "chr1".into(),
                BbiChromWork {
                    blocks,
                    prefix_max_end,
                },
            )]),
        }
    }

    #[test]
    fn weighted_split_uses_cumulative_encoded_bytes() {
        let index = block_index(&[
            (0, 100, 400),
            (100, 200, 100),
            (200, 300, 100),
            (300, 400, 400),
        ]);

        let partitions = plan_bbi_partitions(vec![region()], 2, true, Some(&index));

        assert_eq!(partitions.len(), 2);
        assert_eq!((partitions[0][0].start, partitions[0][0].end), (0, 200));
        assert_eq!((partitions[1][0].start, partitions[1][0].end), (200, 400));
        assert_eq!(
            partition_estimated_bytes(&partitions, Some(&index)),
            // BigTools' cir-tree lookup is inclusive at the coordinate
            // boundary, so each shard also touches its neighbor's edge block.
            vec![600, 600]
        );
    }

    #[test]
    fn partition_count_does_not_exceed_block_granularity() {
        let index = block_index(&[(0, 400, 1_000)]);
        let partitions = plan_bbi_partitions(vec![region()], 8, true, Some(&index));
        assert_eq!(partitions, vec![vec![region()]]);
    }

    #[test]
    fn overlapping_blocks_form_one_indivisible_component() {
        let index = block_index(&[(0, 250, 100), (100, 300, 200), (200, 400, 300)]);

        let partitions = plan_bbi_partitions(vec![region()], 8, true, Some(&index));

        assert_eq!(partitions, vec![vec![region()]]);
        assert_eq!(
            partition_estimated_bytes(&partitions, Some(&index)),
            vec![600]
        );
    }

    #[test]
    fn zero_width_block_contributes_real_work() {
        let index = block_index(&[(100, 100, 50)]);

        let partitions = plan_bbi_partitions(vec![region()], 8, true, Some(&index));

        assert_eq!(partitions, vec![vec![region()]]);
        assert_eq!(
            partition_estimated_bytes(&partitions, Some(&index)),
            vec![50]
        );
    }

    #[test]
    fn blockless_parallel_scan_uses_one_empty_partition() {
        let partitions =
            plan_bbi_partitions(vec![region()], 8, true, Some(&BbiBlockIndex::default()));
        assert_eq!(partitions, vec![Vec::new()]);
    }

    #[test]
    fn weighted_cut_binary_search_uses_lower_candidate_on_ties() {
        let prefix = [0, 400, 500, 600, 1_000];
        assert_eq!(closest_prefix_cut(&prefix, 1, 3, 550), 2);
        assert_eq!(closest_prefix_cut(&prefix, 1, 3, 590), 3);
    }

    #[test]
    fn unavailable_layout_falls_back_to_serial_scan() {
        let partitions = plan_bbi_partitions(vec![region()], 8, true, None);
        assert_eq!(partitions, vec![vec![region()]]);
        assert_eq!(partition_estimated_bytes(&partitions, None), vec![0]);
    }
}
