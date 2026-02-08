//! Balanced partition assignment for indexed genomic reads.
//!
//! When an index (BAI/CRAI/TBI/CSI) is available, this module distributes genomic regions
//! across a target number of partitions using index-derived size estimates. This achieves:
//!
//! - **Balanced workloads**: Linear-scan partitioning places boundaries at byte thresholds
//! - **Controlled concurrency**: Number of partitions = `min(target_partitions, effective_regions)`
//! - **Precise region splitting**: Chromosomes are split at exact byte-budget boundaries

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
    /// Number of unmapped reads for this reference (from BAI metadata pseudobin).
    /// Unmapped reads have a reference_sequence_id but no alignment position.
    /// They are NOT returned by ranged BAI queries and require a separate seek.
    pub unmapped_count: u64,
}

/// One partition's assignment: one or more regions to be processed sequentially.
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    /// Regions assigned to this partition, processed sequentially.
    pub regions: Vec<GenomicRegion>,
    /// Sum of estimated bytes across all assigned regions.
    pub total_estimated_bytes: u64,
}

/// Distribute genomic regions across `target_partitions` using linear-scan partitioning.
///
/// # Algorithm
///
/// Walk through regions in order, maintaining a byte budget per partition.
/// When the budget is exhausted, start a new partition. If a region exceeds
/// the remaining budget AND has a known `contig_length`, split it at the
/// byte-budget boundary (proportional to base pairs). This guarantees each
/// partition receives approximately `total_bytes / target` estimated bytes.
///
/// When a chromosome is split and has unmapped reads (from BAI metadata),
/// an `unmapped_tail` region is emitted after the last sub-region for
/// direct-seek reading of unmapped records.
///
/// # Edge cases
///
/// - `target_partitions <= 1` → single partition with all regions
/// - All estimates are 0 → round-robin distribution
/// - Regions without `contig_length` are treated as atomic (unsplittable)
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

    // All-zero: round-robin distribution
    if total_bytes == 0 {
        let num_bins = target.min(estimates.len());
        let mut bins: Vec<PartitionAssignment> = (0..num_bins)
            .map(|_| PartitionAssignment {
                regions: Vec::new(),
                total_estimated_bytes: 0,
            })
            .collect();
        for (i, est) in estimates.into_iter().enumerate() {
            bins[i % num_bins].regions.push(est.region);
        }
        return bins;
    }

    // Cap effective target: can't create more useful partitions than total bytes
    let effective_target = target.min(total_bytes as usize);

    // Compute per-partition budgets: distribute total_bytes evenly with remainder
    // First `extra` partitions get base_budget+1, rest get base_budget
    let base_budget = total_bytes / effective_target as u64;
    let extra = (total_bytes % effective_target as u64) as usize;

    let budget_for = |idx: usize| -> u64 {
        if idx < extra {
            base_budget + 1
        } else {
            base_budget
        }
    };

    // Linear scan: walk through regions placing partition boundaries at byte thresholds
    let mut partitions: Vec<PartitionAssignment> = Vec::with_capacity(effective_target);
    partitions.push(PartitionAssignment {
        regions: Vec::new(),
        total_estimated_bytes: 0,
    });
    let mut budget: u64 = budget_for(0);

    for est in &estimates {
        let mut remaining: u64 = est.estimated_bytes;

        // Determine the splittable range for this region (1-based inclusive coordinates)
        let (eff_start, eff_end) = match (est.region.start, est.region.end, est.contig_length) {
            (Some(s), Some(e), _) if e >= s => (s, e),
            (_, _, Some(cl)) if cl > 0 => (1u64, cl),
            _ => (0, 0), // not splittable
        };
        let can_split = eff_end > 0 && eff_end >= eff_start;
        let mut pos: u64 = eff_start; // current 1-based start position
        let mut was_split = false;

        // 0-byte regions still need to be assigned (they may have data
        // despite having 0 estimated bytes — e.g., when all contigs share
        // a single BGZF block and the compressed offset range is 0).
        if remaining == 0 {
            let p = partitions.last_mut().unwrap();
            p.regions.push(est.region.clone());
            continue;
        }

        while remaining > 0 {
            // If budget exhausted, start a new partition
            if budget == 0 && partitions.len() < effective_target {
                let new_idx = partitions.len();
                partitions.push(PartitionAssignment {
                    regions: Vec::new(),
                    total_estimated_bytes: 0,
                });
                budget = budget_for(new_idx);
            }

            let is_last = partitions.len() >= effective_target;
            // Check if we have enough base pairs remaining to split
            let remaining_bp = if can_split && pos <= eff_end {
                eff_end - pos + 1
            } else {
                0
            };
            let splittable = remaining_bp > 1; // need >=2 bp for a meaningful split

            if remaining <= budget || is_last || !splittable {
                // Take all remaining bytes: fits in budget, last partition, or can't split
                // Use end=None for the last sub-region to avoid capping at contig_length —
                // reads beyond contig_length (if any) would be lost otherwise.
                let region = if can_split && pos <= eff_end && was_split {
                    GenomicRegion {
                        chrom: est.region.chrom.clone(),
                        start: Some(pos),
                        end: None,
                        unmapped_tail: false,
                    }
                } else {
                    est.region.clone()
                };
                let p = partitions.last_mut().unwrap();
                p.regions.push(region);
                p.total_estimated_bytes += remaining;
                budget = budget.saturating_sub(remaining);
                remaining = 0;
            } else {
                // Split: take budget's worth of base pairs from this chromosome
                was_split = true;
                let bp_to_take = (remaining_bp as u128 * budget as u128 / remaining as u128) as u64;
                // Clamp: at least 1bp, leave at least 1bp for next partition
                let bp_to_take = bp_to_take.clamp(1, remaining_bp - 1);
                let sub_end = pos + bp_to_take - 1;

                debug!(
                    "Splitting {} at bp {} (budget={}, remaining={})",
                    est.region.chrom, sub_end, budget, remaining
                );

                let region = GenomicRegion {
                    chrom: est.region.chrom.clone(),
                    start: Some(pos),
                    end: Some(sub_end),
                    unmapped_tail: false,
                };
                let p = partitions.last_mut().unwrap();
                p.regions.push(region);
                p.total_estimated_bytes += budget;
                remaining -= budget;
                pos = sub_end + 1;

                // Start new partition
                if partitions.len() < effective_target {
                    let new_idx = partitions.len();
                    partitions.push(PartitionAssignment {
                        regions: Vec::new(),
                        total_estimated_bytes: 0,
                    });
                    budget = budget_for(new_idx);
                } else {
                    budget = 0; // is_last will handle on next iteration
                }
            }
        }

        // Emit unmapped tail if chromosome was split and has unmapped reads
        if was_split && est.unmapped_count > 0 {
            let tail = GenomicRegion {
                chrom: est.region.chrom.clone(),
                start: None,
                end: None,
                unmapped_tail: true,
            };
            let p = partitions.last_mut().unwrap();
            p.regions.push(tail);
            p.total_estimated_bytes += 1;
            budget = budget.saturating_sub(1);
        }
    }

    // Remove empty partitions
    partitions.retain(|p| !p.regions.is_empty());

    debug!(
        "Balanced {} regions into {} partitions (target: {})",
        partitions.iter().map(|b| b.regions.len()).sum::<usize>(),
        partitions.len(),
        target
    );
    for (i, bin) in partitions.iter().enumerate() {
        debug!(
            "  Partition {}: {} regions, ~{} estimated bytes",
            i,
            bin.regions.len(),
            bin.total_estimated_bytes
        );
    }

    partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region(chrom: &str) -> GenomicRegion {
        GenomicRegion {
            chrom: chrom.to_string(),
            start: None,
            end: None,
            unmapped_tail: false,
        }
    }

    fn estimate(chrom: &str, bytes: u64, contig_len: Option<u64>) -> RegionSizeEstimate {
        RegionSizeEstimate {
            region: region(chrom),
            estimated_bytes: bytes,
            contig_length: contig_len,
            unmapped_count: 0,
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
        // Each partition should get 2 regions of 100 bytes each
        assert_eq!(result[0].total_estimated_bytes, 200);
        assert_eq!(result[1].total_estimated_bytes, 200);
    }

    #[test]
    fn test_skewed_unsplittable() {
        // chr1 is much larger than others, but none have contig_length (unsplittable)
        // Linear scan: chr1(100) exceeds budget(80), can't split → P0=100.
        // chr2+chr3 fill P1=60.
        let estimates = vec![
            estimate("chr1", 100, None),
            estimate("chr2", 50, None),
            estimate("chr3", 10, None),
        ];
        let result = balance_partitions(estimates, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].total_estimated_bytes, 100);
        assert_eq!(result[1].total_estimated_bytes, 60);
    }

    #[test]
    fn test_skewed_splittable() {
        // Same sizes but with contig_length → chr1 can be split at budget boundary
        // total=160, target=2, budget=80 each
        // chr1(100): take 80 → P0, remaining 20 → P1
        // chr2(50): add to P1 → P1=70
        // chr3(10): add to P1 → P1=80
        let estimates = vec![
            estimate("chr1", 100, Some(249_000_000)),
            estimate("chr2", 50, None),
            estimate("chr3", 10, None),
        ];
        let result = balance_partitions(estimates, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].total_estimated_bytes, 80);
        assert_eq!(result[1].total_estimated_bytes, 80);
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
    fn test_fewer_regions_than_partitions_unsplittable() {
        // Without contig_length, can't split → only 2 partitions
        let estimates = vec![estimate("chr1", 100, None), estimate("chr2", 50, None)];
        let result = balance_partitions(estimates, 8);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_fewer_regions_than_partitions_splittable() {
        // With contig_length, can split to fill all target partitions
        let estimates = vec![
            estimate("chr1", 100, Some(249_000_000)),
            estimate("chr2", 50, Some(243_000_000)),
        ];
        let result = balance_partitions(estimates, 8);
        assert!(
            result.len() <= 8,
            "Should not exceed target: got {}",
            result.len()
        );
        assert!(
            result.len() > 2,
            "Splittable regions should fill more than 2 partitions: got {}",
            result.len()
        );
    }

    #[test]
    fn test_single_region_unsplittable() {
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
        all_chroms.dedup();
        assert_eq!(all_chroms, vec!["chr1", "chr2", "chr3", "chrX"]);
    }

    #[test]
    fn test_single_contig_splits_to_target_partitions() {
        // Single contig with known length: should split to exactly target_partitions
        let estimates = vec![estimate("chr1", 1000, Some(249_000_000))];

        for target in [2, 4, 8] {
            let result = balance_partitions(estimates.clone(), target);
            assert_eq!(
                result.len(),
                target,
                "Single contig with target={} should produce {} partitions, got {}",
                target,
                target,
                result.len()
            );
            // All sub-regions should be chr1 with a start position
            let all_regions: Vec<&GenomicRegion> =
                result.iter().flat_map(|b| b.regions.iter()).collect();
            for region in &all_regions {
                assert_eq!(region.chrom, "chr1");
                assert!(region.start.is_some(), "Sub-regions should have start");
            }
            // Intermediate sub-regions should have end; last sub-region has end=None
            for region in &all_regions[..all_regions.len() - 1] {
                assert!(
                    region.end.is_some(),
                    "Intermediate sub-regions should have end"
                );
            }
            assert!(
                all_regions.last().unwrap().end.is_none(),
                "Last sub-region should have end=None (open-ended)"
            );
        }
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

        // Total bytes should be exactly preserved (integer arithmetic)
        let result_total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
        assert_eq!(
            result_total, total,
            "Total bytes should be exactly preserved: {} vs {}",
            result_total, total
        );

        // Linear scan guarantees near-perfect balance
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
        // With linear scan, max/min ratio should be very close to 1
        assert!(
            max_bytes <= min_bytes * 2,
            "Partitions too imbalanced: max={} min={}",
            max_bytes,
            min_bytes
        );
    }

    fn estimate_with_unmapped(
        chrom: &str,
        bytes: u64,
        contig_len: Option<u64>,
        unmapped: u64,
    ) -> RegionSizeEstimate {
        RegionSizeEstimate {
            region: region(chrom),
            estimated_bytes: bytes,
            contig_length: contig_len,
            unmapped_count: unmapped,
        }
    }

    #[test]
    fn test_unmapped_tail_emitted_when_unmapped_count_nonzero() {
        // chr1 is large and has unmapped reads; chr2 is small with no unmapped
        let estimates = vec![
            estimate_with_unmapped("chr1", 200, Some(249_000_000), 1000),
            estimate_with_unmapped("chr2", 10, None, 0),
        ];
        let result = balance_partitions(estimates, 4);

        // Collect all regions
        let all_regions: Vec<&GenomicRegion> =
            result.iter().flat_map(|b| b.regions.iter()).collect();

        // There should be an unmapped_tail region for chr1
        let unmapped_tails: Vec<&&GenomicRegion> =
            all_regions.iter().filter(|r| r.unmapped_tail).collect();
        assert_eq!(
            unmapped_tails.len(),
            1,
            "Expected exactly 1 unmapped_tail region"
        );
        assert_eq!(unmapped_tails[0].chrom, "chr1");
        assert!(unmapped_tails[0].start.is_none());
        assert!(unmapped_tails[0].end.is_none());

        // No unmapped_tail for chr2 (unmapped_count = 0)
        let chr2_unmapped: Vec<&&GenomicRegion> = all_regions
            .iter()
            .filter(|r| r.chrom == "chr2" && r.unmapped_tail)
            .collect();
        assert!(chr2_unmapped.is_empty());
    }

    #[test]
    fn test_no_unmapped_tail_when_no_split() {
        // chrM is small relative to ideal (total=200, target=4, ideal=50, chrM=5 < 50)
        // so it won't be split and no unmapped_tail should be emitted
        // (unmapped reads are already captured by the whole-chromosome query)
        let estimates = vec![
            estimate_with_unmapped("chr1", 100, Some(249_000_000), 500),
            estimate_with_unmapped("chr2", 95, Some(243_000_000), 300),
            estimate_with_unmapped("chrM", 5, Some(16_569), 100),
        ];
        let result = balance_partitions(estimates, 4);

        let all_regions: Vec<&GenomicRegion> =
            result.iter().flat_map(|b| b.regions.iter()).collect();
        let chrm_unmapped: Vec<&&GenomicRegion> = all_regions
            .iter()
            .filter(|r| r.chrom == "chrM" && r.unmapped_tail)
            .collect();
        assert!(
            chrm_unmapped.is_empty(),
            "Should not emit unmapped_tail for unsplit regions (chrM)"
        );
    }

    #[test]
    fn test_linear_scan_perfect_balance() {
        // Linear scan should produce near-perfect balance for splittable regions
        // total = 750, target = 4 → budget = 187 or 188 each
        let estimates = vec![
            estimate("chr1", 249, Some(249_000_000)),
            estimate("chr2", 243, Some(243_000_000)),
            estimate("chr3", 198, Some(198_000_000)),
            estimate("chrX", 60, Some(155_000_000)),
        ];
        let result = balance_partitions(estimates, 4);

        assert_eq!(result.len(), 4);
        let total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
        assert_eq!(total, 750);

        // Each partition should be within ±1 of ideal (187.5)
        for (i, p) in result.iter().enumerate() {
            assert!(
                p.total_estimated_bytes >= 187 && p.total_estimated_bytes <= 189,
                "Partition {} has {} bytes, expected ~188",
                i,
                p.total_estimated_bytes
            );
        }
    }

    #[test]
    fn test_zero_byte_regions_not_dropped() {
        // When some contigs share a BGZF block, their compressed byte range is 0.
        // These regions must still be included in partition assignments.
        let estimates = vec![
            estimate("chr1", 0, None),
            estimate("chr2", 100, None),
            estimate("chrX", 0, None),
        ];
        let result = balance_partitions(estimates, 4);

        let mut all_chroms: Vec<String> = result
            .iter()
            .flat_map(|b| b.regions.iter().map(|r| r.chrom.clone()))
            .collect();
        all_chroms.sort();
        assert_eq!(
            all_chroms,
            vec!["chr1", "chr2", "chrX"],
            "All regions must be present even with 0-byte estimates"
        );
    }

    #[test]
    fn test_linear_scan_preserves_total_bytes() {
        // Verify total bytes are exactly preserved across various configurations
        for target in [2, 3, 4, 8, 16] {
            let estimates = vec![
                estimate("chr1", 249, Some(249_000_000)),
                estimate("chr2", 243, Some(243_000_000)),
                estimate("chr3", 198, Some(198_000_000)),
            ];
            let total: u64 = estimates.iter().map(|e| e.estimated_bytes).sum();
            let result = balance_partitions(estimates, target);
            let result_total: u64 = result.iter().map(|b| b.total_estimated_bytes).sum();
            assert_eq!(
                result_total, total,
                "Total bytes not preserved for target={}: {} vs {}",
                target, result_total, total
            );
        }
    }
}
