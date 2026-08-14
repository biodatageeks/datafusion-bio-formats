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
/// ownership, even when their union exceeds `max_range_size`; that limit is
/// not an upper bound for overlapping input. Non-overlapping ranges are merged
/// only when both the gap and combined range size are within their configured
/// limits.
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
}
