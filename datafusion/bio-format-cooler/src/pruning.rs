//! First-axis predicate pruning and partition planning.
//!
//! The pixels table is sorted by `bin1_id` and CSR-indexed by
//! `indexes/bin1_offset`, so predicates on `chrom1`/`start1`/`end1` map to
//! contiguous pixel row ranges: chrom → bin span via `indexes/chrom_offset`,
//! positions → bins via binary search on the bins table, bins → pixel rows via
//! `bin1_offset`. Pruning is a superset (filters are reported `Inexact` and
//! DataFusion re-applies them), so bins overlapping the query edge are kept.

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::Expr;
use datafusion_bio_format_core::genomic_filter::extract_genomic_regions;

use crate::collection::{BinData, IndexData};

/// True when every column the expression references belongs to the first
/// (row) axis, making the filter a candidate for row-range pruning.
pub(crate) fn is_first_axis_filter(expr: &Expr) -> bool {
    let columns = expr.column_refs();
    !columns.is_empty()
        && columns
            .iter()
            .all(|column| matches!(column.name.as_str(), "chrom1" | "start1" | "end1"))
}

/// Rewrite `chrom1`/`start1`/`end1` column references to the `chrom`/`start`/
/// `end` names the shared genomic-filter extractor understands. Second-axis
/// and value columns keep their names, so the extractor routes them to
/// residual filters and they contribute no (incorrect) constraints.
fn rename_first_axis_columns(expr: &Expr) -> Result<Expr> {
    expr.clone()
        .transform(|node| {
            Ok(match node {
                Expr::Column(mut column) => {
                    let renamed = match column.name.as_str() {
                        "chrom1" => Some("chrom"),
                        "start1" => Some("start"),
                        "end1" => Some("end"),
                        _ => None,
                    };
                    match renamed {
                        Some(name) => {
                            column.name = name.to_string();
                            Transformed::yes(Expr::Column(column))
                        }
                        None => Transformed::no(Expr::Column(column)),
                    }
                }
                other => Transformed::no(other),
            })
        })
        .map(|transformed| transformed.data)
}

/// Map first-axis genomic filters to the disjoint, ascending pixel row
/// ranges to scan.
pub(crate) fn plan_first_axis_ranges(
    filters: &[Expr],
    coordinate_system_zero_based: bool,
    bins: &BinData,
    index: &IndexData,
    nnz: usize,
) -> Result<Vec<(usize, usize)>> {
    let renamed = filters
        .iter()
        .map(rename_first_axis_columns)
        .collect::<Result<Vec<_>>>()?;
    let analysis = extract_genomic_regions(&renamed, coordinate_system_zero_based);
    if analysis.unsatisfiable {
        return Ok(Vec::new());
    }
    if analysis.regions.is_empty() {
        return Ok(vec![(0, nnz)]);
    }

    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for region in &analysis.regions {
        let Some(chrom_index) = bins
            .chrom_names
            .iter()
            .position(|name| name == &region.chrom)
        else {
            // Unknown chromosome matches no rows.
            continue;
        };
        let chrom_bin_lo = index.chrom_offset[chrom_index] as usize;
        let chrom_bin_hi = index.chrom_offset[chrom_index + 1] as usize;
        // Region bounds are 1-based inclusive; bins are stored 0-based
        // half-open. Keep every bin overlapping [query_start0, query_end0).
        let query_start0 = region.start.map_or(0, |start| start.saturating_sub(1));
        let query_end0 = region.end.unwrap_or(u64::MAX);
        let chrom_starts = &bins.start[chrom_bin_lo..chrom_bin_hi];
        let chrom_ends = &bins.end[chrom_bin_lo..chrom_bin_hi];
        let bin_lo = chrom_bin_lo + chrom_ends.partition_point(|&end| end <= query_start0);
        let bin_hi = chrom_bin_lo + chrom_starts.partition_point(|&start| start < query_end0);
        if bin_lo >= bin_hi {
            continue;
        }
        let row_lo = index.bin1_offset[bin_lo] as usize;
        let row_hi = index.bin1_offset[bin_hi] as usize;
        if row_lo < row_hi {
            ranges.push((row_lo, row_hi));
        }
    }

    // Regions may target adjacent chromosomes; merge into disjoint ranges so
    // no pixel row is ever emitted twice.
    ranges.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for (lo, hi) in ranges {
        match merged.last_mut() {
            Some((_, last_hi)) if lo <= *last_hi => *last_hi = (*last_hi).max(hi),
            _ => merged.push((lo, hi)),
        }
    }
    Ok(merged)
}

/// Split disjoint row ranges into up to `target` partitions of roughly equal
/// row counts, cutting inside ranges at bin1 boundaries when the index is
/// available so future per-partition work stays aligned with the CSR layout.
pub(crate) fn plan_partitions(
    ranges: &[(usize, usize)],
    target: usize,
    bin1_offset: Option<&[i64]>,
) -> Vec<Vec<(usize, usize)>> {
    let ranges: Vec<(usize, usize)> = ranges.iter().copied().filter(|(lo, hi)| lo < hi).collect();
    let total: usize = ranges.iter().map(|(lo, hi)| hi - lo).sum();
    if total == 0 || target <= 1 {
        return vec![ranges];
    }
    let per_partition = total.div_ceil(target);
    let mut partitions: Vec<Vec<(usize, usize)>> = vec![Vec::new()];
    let mut filled = 0usize;
    for &(range_lo, range_hi) in &ranges {
        let mut lo = range_lo;
        while lo < range_hi {
            if filled >= per_partition {
                partitions.push(Vec::new());
                filled = 0;
            }
            let want = range_hi.min(lo + (per_partition - filled));
            let cut = if want >= range_hi {
                range_hi
            } else {
                match bin1_offset {
                    Some(offsets) => {
                        let index = offsets.partition_point(|&offset| (offset as usize) < want);
                        offsets
                            .get(index)
                            .map_or(range_hi, |&offset| (offset as usize).min(range_hi))
                            .max(lo + 1)
                    }
                    None => want,
                }
            };
            partitions
                .last_mut()
                .expect("partition list is non-empty")
                .push((lo, cut));
            filled += cut - lo;
            lo = cut;
        }
    }
    partitions.retain(|partition| !partition.is_empty());
    if partitions.is_empty() {
        partitions.push(Vec::new());
    }
    partitions
}
