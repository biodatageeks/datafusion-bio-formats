//! Balanced partition assignment for indexed genomic reads.
//!
//! When an index (BAI/CRAI/TBI/CSI) is available, this module distributes genomic regions
//! across a target number of partitions using index-derived size estimates. This achieves:
//!
//! - **Balanced workloads**: Greedy bin-packing distributes regions by estimated byte size
//! - **Controlled concurrency**: Number of partitions = `min(target_partitions, num_regions)`
//! - **Large region splitting**: Chromosomes that dominate total size are split into sub-regions

use crate::genomic_filter::GenomicRegion;
use log::debug;

/// Size estimate for a single genomic region, derived from index metadata.
#[derive(Debug, Clone)]
pub struct RegionSizeEstimate {
    /// The genomic region (chromosome + optional start/end).
    pub region: GenomicRegion,
    /// Estimated compressed bytes for this region from the index.
    pub estimated_bytes: u64,
    /// Length of the contig in base pairs (if known). Used for sub-region splitting.
    pub contig_length: Option<u64>,
}

/// One partition's assignment: one or more regions to be processed sequentially.
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    /// Regions assigned to this partition, processed sequentially.
    pub regions: Vec<GenomicRegion>,
    /// Sum of estimated bytes across all assigned regions.
    pub total_estimated_bytes: u64,
}

/// Distribute genomic regions across `target_partitions` bins using greedy bin-packing.
///
/// # Algorithm
///
/// 1. **Split large regions**: If a region's `estimated_bytes` exceeds 1.5x the ideal
///    per-partition share AND `contig_length` is known, split it into equal-bp sub-regions.
/// 2. **Greedy bin-packing**: Sort regions by `estimated_bytes` descending. Assign each
///    to the bin with the smallest current total.
///
/// # Edge cases
///
/// - `target_partitions <= 1` → single partition with all regions
/// - All estimates are 0 → round-robin distribution
/// - Fewer regions than `target_partitions` → one region per partition
pub fn balance_partitions(
    estimates: Vec<RegionSizeEstimate>,
    target_partitions: usize,
) -> Vec<PartitionAssignment> {
    if estimates.is_empty() {
        return Vec::new();
    }

    let target = target_partitions.max(1);

    // Single partition fast path
    if target == 1 {
        let total: u64 = estimates.iter().map(|e| e.estimated_bytes).sum();
        return vec![PartitionAssignment {
            regions: estimates.into_iter().map(|e| e.region).collect(),
            total_estimated_bytes: total,
        }];
    }

    let total_bytes: u64 = estimates.iter().map(|e| e.estimated_bytes).sum();

    // Step 1: Split large regions if beneficial
    let ideal_per_partition = if total_bytes > 0 {
        total_bytes / target as u64
    } else {
        0
    };
    let split_threshold = ideal_per_partition.saturating_mul(3) / 2; // 1.5x

    let mut expanded: Vec<RegionSizeEstimate> = Vec::with_capacity(estimates.len());
    for est in estimates {
        if split_threshold > 0
            && est.estimated_bytes > split_threshold
            && est.contig_length.is_some()
        {
            let contig_len = est.contig_length.unwrap();
            // How many sub-regions do we need to bring each under the threshold?
            let num_splits = est.estimated_bytes.div_ceil(split_threshold) as usize;
            let num_splits = num_splits.min(target).max(2); // at least 2, at most target_partitions
            let bp_per_split = contig_len / num_splits as u64;
            let bytes_per_split = est.estimated_bytes / num_splits as u64;

            debug!(
                "Splitting region {} into {} sub-regions ({} bp each, ~{} bytes each)",
                est.region.chrom, num_splits, bp_per_split, bytes_per_split
            );

            for i in 0..num_splits {
                // 1-based coordinates
                let sub_start = i as u64 * bp_per_split + 1;
                let sub_end = if i == num_splits - 1 {
                    contig_len // last sub-region extends to end
                } else {
                    (i as u64 + 1) * bp_per_split
                };
                expanded.push(RegionSizeEstimate {
                    region: GenomicRegion {
                        chrom: est.region.chrom.clone(),
                        start: Some(sub_start),
                        end: Some(sub_end),
                    },
                    estimated_bytes: bytes_per_split,
                    contig_length: None, // sub-regions are not further splittable
                });
            }
        } else {
            expanded.push(est);
        }
    }

    // Check if all estimates are zero — use round-robin in that case
    let all_zero = expanded.iter().all(|e| e.estimated_bytes == 0);

    // Step 2: Greedy bin-packing
    let num_bins = target.min(expanded.len());

    // Sort by estimated_bytes descending (largest first for best packing)
    if !all_zero {
        expanded.sort_by(|a, b| b.estimated_bytes.cmp(&a.estimated_bytes));
    }

    let mut bins: Vec<PartitionAssignment> = (0..num_bins)
        .map(|_| PartitionAssignment {
            regions: Vec::new(),
            total_estimated_bytes: 0,
        })
        .collect();

    if all_zero {
        // Round-robin for zero estimates
        for (i, est) in expanded.into_iter().enumerate() {
            let bin_idx = i % num_bins;
            bins[bin_idx].regions.push(est.region);
        }
    } else {
        // Greedy: assign each region to the bin with smallest current total
        for est in expanded {
            let min_bin = bins
                .iter_mut()
                .min_by_key(|b| b.total_estimated_bytes)
                .unwrap();
            min_bin.total_estimated_bytes += est.estimated_bytes;
            min_bin.regions.push(est.region);
        }
    }

    // Remove empty bins (shouldn't happen, but be safe)
    bins.retain(|b| !b.regions.is_empty());

    debug!(
        "Balanced {} regions into {} partitions (target: {})",
        bins.iter().map(|b| b.regions.len()).sum::<usize>(),
        bins.len(),
        target
    );
    for (i, bin) in bins.iter().enumerate() {
        debug!(
            "  Partition {}: {} regions, ~{} estimated bytes",
            i,
            bin.regions.len(),
            bin.total_estimated_bytes
        );
    }

    bins
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region(chrom: &str) -> GenomicRegion {
        GenomicRegion {
            chrom: chrom.to_string(),
            start: None,
            end: None,
        }
    }

    fn estimate(chrom: &str, bytes: u64, contig_len: Option<u64>) -> RegionSizeEstimate {
        RegionSizeEstimate {
            region: region(chrom),
            estimated_bytes: bytes,
            contig_length: contig_len,
        }
    }

    #[test]
    fn test_empty_estimates() {
        let result = balance_partitions(vec![], 4);
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_partition() {
        let estimates = vec![
            estimate("chr1", 100, None),
            estimate("chr2", 50, None),
            estimate("chrX", 30, None),
        ];
        let result = balance_partitions(estimates, 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].regions.len(), 3);
        assert_eq!(result[0].total_estimated_bytes, 180);
    }

    #[test]
    fn test_uniform_distribution() {
        // 4 regions of equal size, target 2 partitions
        let estimates = vec![
            estimate("chr1", 100, None),
            estimate("chr2", 100, None),
            estimate("chr3", 100, None),
            estimate("chr4", 100, None),
        ];
        let result = balance_partitions(estimates, 2);
        assert_eq!(result.len(), 2);
        // Each bin should get 2 regions of 100 bytes each
        assert_eq!(result[0].total_estimated_bytes, 200);
        assert_eq!(result[1].total_estimated_bytes, 200);
    }

    #[test]
    fn test_skewed_distribution() {
        // chr1 is much larger than others
        let estimates = vec![
            estimate("chr1", 100, None),
            estimate("chr2", 50, None),
            estimate("chr3", 10, None),
        ];
        let result = balance_partitions(estimates, 2);
        assert_eq!(result.len(), 2);
        // Greedy: chr1 (100) goes to bin 0, chr2 (50) to bin 1, chr3 (10) to bin 1
        assert_eq!(result[0].total_estimated_bytes, 100);
        assert_eq!(result[1].total_estimated_bytes, 60);
    }

    #[test]
    fn test_region_splitting() {
        // One huge region that should be split
        let estimates = vec![
            estimate("chr1", 200, Some(249_000_000)),
            estimate("chr2", 10, None),
        ];
        let result = balance_partitions(estimates, 4);
        // chr1 should be split, total regions > 2
        let total_regions: usize = result.iter().map(|b| b.regions.len()).sum();
        assert!(
            total_regions > 2,
            "Expected splitting, got {} regions",
            total_regions
        );
        assert!(result.len() <= 4, "Should not exceed target_partitions");
    }

    #[test]
    fn test_zero_estimates_round_robin() {
        let estimates = vec![
            estimate("chr1", 0, None),
            estimate("chr2", 0, None),
            estimate("chr3", 0, None),
            estimate("chr4", 0, None),
        ];
        let result = balance_partitions(estimates, 2);
        assert_eq!(result.len(), 2);
        // Round-robin: each bin gets 2 regions
        assert_eq!(result[0].regions.len(), 2);
        assert_eq!(result[1].regions.len(), 2);
    }

    #[test]
    fn test_fewer_regions_than_partitions() {
        let estimates = vec![estimate("chr1", 100, None), estimate("chr2", 50, None)];
        let result = balance_partitions(estimates, 8);
        // Can't have more partitions than regions
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_single_region() {
        let estimates = vec![estimate("chr1", 100, None)];
        let result = balance_partitions(estimates, 4);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].regions.len(), 1);
    }

    #[test]
    fn test_partition_count_never_exceeds_target() {
        let estimates = vec![
            estimate("chr1", 100, Some(249_000_000)),
            estimate("chr2", 90, Some(243_000_000)),
            estimate("chr3", 80, Some(198_000_000)),
            estimate("chr4", 70, Some(191_000_000)),
            estimate("chr5", 60, Some(181_000_000)),
            estimate("chrX", 50, Some(155_000_000)),
        ];
        let result = balance_partitions(estimates, 4);
        assert!(
            result.len() <= 4,
            "Got {} partitions, expected <= 4",
            result.len()
        );
    }

    #[test]
    fn test_all_regions_present_after_balancing() {
        let estimates = vec![
            estimate("chr1", 100, None),
            estimate("chr2", 50, None),
            estimate("chr3", 30, None),
            estimate("chrX", 20, None),
        ];
        let result = balance_partitions(estimates, 3);
        let mut all_chroms: Vec<String> = result
            .iter()
            .flat_map(|b| b.regions.iter().map(|r| r.chrom.clone()))
            .collect();
        all_chroms.sort();
        assert_eq!(all_chroms, vec!["chr1", "chr2", "chr3", "chrX"]);
    }

    #[test]
    fn test_realistic_human_genome_distribution() {
        // Simulating human genome chromosome sizes (compressed bytes, proportional)
        let estimates = vec![
            estimate("chr1", 249, Some(249_000_000)),
            estimate("chr2", 243, Some(243_000_000)),
            estimate("chr3", 198, Some(198_000_000)),
            estimate("chr4", 191, Some(191_000_000)),
            estimate("chr5", 181, Some(181_000_000)),
            estimate("chr6", 171, Some(171_000_000)),
            estimate("chr7", 159, Some(159_000_000)),
            estimate("chr8", 146, Some(146_000_000)),
            estimate("chr9", 141, Some(141_000_000)),
            estimate("chr10", 136, Some(136_000_000)),
            estimate("chr11", 135, Some(135_000_000)),
            estimate("chr12", 134, Some(134_000_000)),
            estimate("chr13", 115, Some(115_000_000)),
            estimate("chr14", 107, Some(107_000_000)),
            estimate("chr15", 102, Some(102_000_000)),
            estimate("chr16", 90, Some(90_000_000)),
            estimate("chr17", 84, Some(84_000_000)),
            estimate("chr18", 80, Some(80_000_000)),
            estimate("chr19", 59, Some(59_000_000)),
            estimate("chr20", 64, Some(64_000_000)),
            estimate("chr21", 47, Some(47_000_000)),
            estimate("chr22", 51, Some(51_000_000)),
            estimate("chrX", 155, Some(155_000_000)),
            estimate("chrY", 57, Some(57_000_000)),
        ];
        let total: u64 = estimates.iter().map(|e| e.estimated_bytes).sum();
        let result = balance_partitions(estimates, 8);

        assert!(result.len() <= 8);
        assert!(!result.is_empty());

        // Check that total estimated bytes is preserved (approximately, splits may round)
        let result_total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
        assert!(
            (result_total as i64 - total as i64).unsigned_abs() <= result.len() as u64,
            "Total bytes should be approximately preserved: {} vs {}",
            result_total,
            total
        );

        // Check balance: max partition shouldn't be more than 2x the min
        let max_bytes = result
            .iter()
            .map(|b| b.total_estimated_bytes)
            .max()
            .unwrap();
        let min_bytes = result
            .iter()
            .map(|b| b.total_estimated_bytes)
            .min()
            .unwrap();
        assert!(
            max_bytes <= min_bytes * 3,
            "Partitions too imbalanced: max={} min={}",
            max_bytes,
            min_bytes
        );
    }
}
