//! Checked byte-range coalescing and partition assignment.

use datafusion::common::{DataFusionError, Result};

/// An end-exclusive byte range in a source object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteRange {
    /// Inclusive byte offset.
    pub start: u64,
    /// Exclusive byte offset.
    pub end: u64,
}

impl ByteRange {
    /// Creates a nonempty end-exclusive range.
    pub fn new(start: u64, end: u64) -> Result<Self> {
        if end <= start {
            return Err(DataFusionError::Plan(format!(
                "invalid byte range {start}..{end}: end must be greater than start"
            )));
        }
        Ok(Self { start, end })
    }

    /// Returns the number of bytes in this range.
    pub fn len(self) -> u64 {
        self.end - self.start
    }

    /// Returns false because validated byte ranges are never empty.
    pub fn is_empty(self) -> bool {
        false
    }
}

/// Coalesces sorted or unsorted byte ranges without hiding large sparse gaps.
///
/// Overlapping ranges are always merged to guarantee unique physical
/// ownership. Non-overlapping ranges are merged only when both the gap and
/// combined range size are within their configured limits.
pub fn coalesce_byte_ranges(
    ranges: impl IntoIterator<Item = ByteRange>,
    max_gap: u64,
    max_range_size: u64,
) -> Result<Vec<ByteRange>> {
    if max_range_size == 0 {
        return Err(DataFusionError::Plan(
            "max_range_size must be greater than zero".to_string(),
        ));
    }

    let mut ranges: Vec<_> = ranges.into_iter().collect();
    ranges.sort_unstable();
    let mut coalesced: Vec<ByteRange> = Vec::with_capacity(ranges.len());

    for next in ranges {
        let Some(current) = coalesced.last_mut() else {
            coalesced.push(next);
            continue;
        };

        let overlaps = next.start < current.end;
        let gap = next.start.saturating_sub(current.end);
        let combined_end = current.end.max(next.end);
        let combined_size = combined_end.checked_sub(current.start).ok_or_else(|| {
            DataFusionError::Plan("byte range size arithmetic overflowed".to_string())
        })?;
        let nearby = next.start >= current.end && gap <= max_gap && combined_size <= max_range_size;

        if overlaps || nearby {
            current.end = combined_end;
        } else {
            coalesced.push(next);
        }
    }

    Ok(coalesced)
}

/// Byte ranges assigned to one DataFusion physical partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteRangePartition {
    /// Ranges owned by this partition in ascending source order.
    pub ranges: Vec<ByteRange>,
    /// Sum of range lengths assigned to this partition.
    pub total_bytes: u64,
}

/// Splits indivisible byte ranges into at most `target_partitions` contiguous,
/// byte-balanced runs.
///
/// Unlike [`balance_byte_ranges`], partition `n` only holds ranges that precede
/// every range in partition `n + 1`. A reader whose rows follow source order can
/// therefore concatenate partitions in order and reproduce that order exactly,
/// which a least-loaded assignment cannot guarantee. Every input range is
/// assigned exactly once.
pub fn partition_byte_ranges_in_order(
    ranges: impl IntoIterator<Item = ByteRange>,
    target_partitions: usize,
) -> Vec<ByteRangePartition> {
    let mut ranges: Vec<_> = ranges.into_iter().collect();
    if ranges.is_empty() {
        return Vec::new();
    }
    ranges.sort_unstable();

    let partition_count = target_partitions.max(1).min(ranges.len());
    let total_bytes = ranges
        .iter()
        .map(|range| range.len())
        .fold(0_u64, u64::saturating_add);

    let mut partitions: Vec<ByteRangePartition> = Vec::with_capacity(partition_count);
    let mut current = ByteRangePartition {
        ranges: Vec::new(),
        total_bytes: 0,
    };
    let mut assigned_bytes = 0_u64;

    for (index, range) in ranges.iter().copied().enumerate() {
        let remaining_ranges = ranges.len() - index;
        let remaining_partitions = partition_count - partitions.len();
        // Close the current run once it holds its byte share, while leaving at
        // least one range for each partition that has not been produced yet.
        let share_complete = partition_count > 1
            && !current.ranges.is_empty()
            && assigned_bytes.saturating_mul(partition_count as u64)
                >= total_bytes.saturating_mul(partitions.len() as u64 + 1);
        let must_close = remaining_ranges < remaining_partitions;
        if remaining_partitions > 1 && (share_complete || must_close) {
            partitions.push(std::mem::replace(
                &mut current,
                ByteRangePartition {
                    ranges: Vec::new(),
                    total_bytes: 0,
                },
            ));
        }
        current.total_bytes = current.total_bytes.saturating_add(range.len());
        current.ranges.push(range);
        assigned_bytes = assigned_bytes.saturating_add(range.len());
    }
    partitions.push(current);
    partitions
}

/// Balances indivisible byte ranges across at most `target_partitions`.
///
/// The largest ranges are assigned first to the currently lightest partition.
/// Every input range is assigned exactly once.
pub fn balance_byte_ranges(
    ranges: impl IntoIterator<Item = ByteRange>,
    target_partitions: usize,
) -> Vec<ByteRangePartition> {
    let mut indexed: Vec<_> = ranges.into_iter().enumerate().collect();
    if indexed.is_empty() {
        return Vec::new();
    }

    indexed.sort_unstable_by(|left, right| {
        right
            .1
            .len()
            .cmp(&left.1.len())
            .then_with(|| left.0.cmp(&right.0))
    });

    let partition_count = target_partitions.max(1).min(indexed.len());
    let mut partitions = vec![
        ByteRangePartition {
            ranges: Vec::new(),
            total_bytes: 0,
        };
        partition_count
    ];

    for (_, range) in indexed {
        let partition_index = partitions
            .iter()
            .enumerate()
            .min_by_key(|(index, partition)| (partition.total_bytes, *index))
            .map(|(index, _)| index)
            .expect("there is at least one partition");
        let partition = &mut partitions[partition_index];
        partition.total_bytes = partition.total_bytes.saturating_add(range.len());
        partition.ranges.push(range);
    }

    for partition in &mut partitions {
        partition.ranges.sort_unstable();
    }
    partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(start: u64, end: u64) -> ByteRange {
        ByteRange::new(start, end).unwrap()
    }

    #[test]
    fn rejects_empty_or_reversed_ranges() {
        assert!(ByteRange::new(1, 1).is_err());
        assert!(ByteRange::new(2, 1).is_err());
    }

    #[test]
    fn coalesces_overlaps_and_bounded_gaps() {
        let ranges = vec![range(30, 40), range(0, 10), range(8, 15), range(18, 20)];
        assert_eq!(
            coalesce_byte_ranges(ranges, 3, 25).unwrap(),
            vec![range(0, 20), range(30, 40)]
        );
    }

    #[test]
    fn does_not_bridge_large_sparse_intervals() {
        let ranges = vec![range(0, 10), range(1_000, 1_010)];
        assert_eq!(
            coalesce_byte_ranges(ranges.clone(), 10, 10_000).unwrap(),
            ranges
        );
    }

    #[test]
    fn overlapping_ranges_merge_even_above_size_threshold() {
        assert_eq!(
            coalesce_byte_ranges(vec![range(0, 100), range(50, 150)], 0, 10).unwrap(),
            vec![range(0, 150)]
        );
    }

    #[test]
    fn balances_ranges_once_across_target_partitions() {
        let ranges = vec![
            range(0, 100),
            range(200, 260),
            range(300, 340),
            range(400, 420),
        ];
        let partitions = balance_byte_ranges(ranges.clone(), 2);
        assert_eq!(partitions.len(), 2);

        let mut assigned: Vec<_> = partitions
            .iter()
            .flat_map(|partition| partition.ranges.iter().copied())
            .collect();
        assigned.sort_unstable();
        assert_eq!(assigned, ranges);
        assert_eq!(
            partitions.iter().map(|part| part.total_bytes).sum::<u64>(),
            220
        );
    }

    #[test]
    fn caps_partition_count_to_work_units() {
        assert_eq!(balance_byte_ranges(vec![range(0, 10)], 8).len(), 1);
        assert!(balance_byte_ranges(Vec::<ByteRange>::new(), 8).is_empty());
    }

    #[test]
    fn in_order_partitions_are_contiguous_and_cover_every_range() {
        let ranges: Vec<_> = (0..17_u64)
            .map(|index| range(index * 100, index * 100 + 10 + index * 5))
            .collect();
        for target in 1..=20_usize {
            let partitions = partition_byte_ranges_in_order(ranges.clone(), target);
            assert!(!partitions.is_empty());
            assert!(partitions.len() <= target.max(1).min(ranges.len()));
            assert!(
                partitions
                    .iter()
                    .all(|partition| !partition.ranges.is_empty()),
                "target {target} produced an empty partition"
            );
            let flattened: Vec<_> = partitions
                .iter()
                .flat_map(|partition| partition.ranges.iter().copied())
                .collect();
            assert_eq!(flattened, ranges, "target {target} changed source order");
            for partition in &partitions {
                assert_eq!(
                    partition.total_bytes,
                    partition.ranges.iter().map(|r| r.len()).sum::<u64>()
                );
            }
        }
    }

    #[test]
    fn in_order_partitions_split_even_work_evenly() {
        let ranges: Vec<_> = (0..8_u64)
            .map(|index| range(index * 10, index * 10 + 10))
            .collect();
        let partitions = partition_byte_ranges_in_order(ranges, 4);
        assert_eq!(partitions.len(), 4);
        assert!(
            partitions
                .iter()
                .all(|partition| partition.ranges.len() == 2)
        );
    }

    #[test]
    fn in_order_partitions_handle_an_empty_input() {
        assert!(partition_byte_ranges_in_order(Vec::new(), 4).is_empty());
    }
}
