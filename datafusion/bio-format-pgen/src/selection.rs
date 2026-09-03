//! Which PVAR rows a scan or matrix read covers.
//!
//! A full scan of a large panel is the common case, and it must not cost an
//! index per row just to say "every row". A contiguous filtered run is the
//! next most common; only a genuinely sparse selection stores its indices,
//! and it stores them as `u32`, which `max_variants` keeps sufficient.

use std::ops::Range;
use std::sync::Arc;

/// Selected PVAR row indices in ascending file order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum VariantSelection {
    /// Every row of a table with this many variants.
    All(usize),
    /// One contiguous run of rows.
    Range(Range<usize>),
    /// Ascending, distinct row indices.
    Sparse(Arc<[u32]>),
}

impl VariantSelection {
    /// The compact form of ascending, distinct indices into `total` rows.
    pub(crate) fn from_sorted_indices(indices: Vec<u32>, total: usize) -> Self {
        debug_assert!(indices.windows(2).all(|pair| pair[0] < pair[1]));
        match (indices.first(), indices.last()) {
            (None, _) | (_, None) => Self::Range(0..0),
            (Some(&first), Some(&last)) if (last - first) as usize + 1 == indices.len() => {
                if first == 0 && indices.len() == total {
                    Self::All(total)
                } else {
                    Self::Range(first as usize..last as usize + 1)
                }
            }
            _ => Self::Sparse(Arc::from(indices)),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::All(total) => *total,
            Self::Range(range) => range.len(),
            Self::Sparse(indices) => indices.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The row index at `position` in the selection.
    pub(crate) fn get(&self, position: usize) -> Option<usize> {
        match self {
            Self::All(total) => (position < *total).then_some(position),
            Self::Range(range) => {
                let index = range.start + position;
                (index < range.end).then_some(index)
            }
            Self::Sparse(indices) => indices.get(position).map(|&index| index as usize),
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).map(move |position| match self {
            Self::All(_) => position,
            Self::Range(range) => range.start + position,
            Self::Sparse(indices) => indices[position] as usize,
        })
    }

    /// Keeps the first `limit` rows.
    pub(crate) fn truncate(&mut self, limit: usize) {
        if limit >= self.len() {
            return;
        }
        *self = match self {
            Self::All(_) => Self::Range(0..limit),
            Self::Range(range) => Self::Range(range.start..range.start + limit),
            Self::Sparse(indices) => Self::Sparse(Arc::from(&indices[..limit])),
        };
    }

    /// The rows at positions `range` of the selection.
    pub(crate) fn slice(&self, range: Range<usize>) -> SelectionSlice<'_> {
        debug_assert!(range.end <= self.len());
        SelectionSlice {
            selection: self,
            range,
        }
    }
}

/// A contiguous run of positions within a selection.
#[derive(Clone, Debug)]
pub(crate) struct SelectionSlice<'a> {
    selection: &'a VariantSelection,
    range: Range<usize>,
}

impl SelectionSlice<'_> {
    pub(crate) fn len(&self) -> usize {
        self.range.len()
    }

    /// The row index at `position` within the slice.
    pub(crate) fn get(&self, position: usize) -> Option<usize> {
        (position < self.range.len())
            .then(|| self.selection.get(self.range.start + position))
            .flatten()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.range
            .clone()
            .filter_map(move |position| self.selection.get(position))
    }

    /// Whether the slice covers row `index`.
    pub(crate) fn contains(&self, index: usize) -> bool {
        match self.selection {
            VariantSelection::All(_) => self.range.contains(&index),
            VariantSelection::Range(range) => {
                (range.start + self.range.start..range.start + self.range.end).contains(&index)
            }
            VariantSelection::Sparse(indices) => u32::try_from(index)
                .ok()
                .is_some_and(|index| indices[self.range.clone()].binary_search(&index).is_ok()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_the_compact_form() {
        assert_eq!(
            VariantSelection::from_sorted_indices((0..5).collect(), 5),
            VariantSelection::All(5)
        );
        assert_eq!(
            VariantSelection::from_sorted_indices((2..5).collect(), 5),
            VariantSelection::Range(2..5)
        );
        assert_eq!(
            VariantSelection::from_sorted_indices(vec![0, 2, 4], 5),
            VariantSelection::Sparse(Arc::from([0, 2, 4]))
        );
        assert_eq!(
            VariantSelection::from_sorted_indices(Vec::new(), 5),
            VariantSelection::Range(0..0)
        );
    }

    #[test]
    fn slices_and_membership_agree_with_the_index_list() {
        for selection in [
            VariantSelection::All(6),
            VariantSelection::Range(3..9),
            VariantSelection::Sparse(Arc::from([1, 4, 5, 9, 12, 20])),
        ] {
            let rows = selection.iter().collect::<Vec<_>>();
            assert_eq!(rows.len(), 6);
            let slice = selection.slice(2..5);
            assert_eq!(slice.iter().collect::<Vec<_>>(), rows[2..5].to_vec());
            assert_eq!(slice.get(0), Some(rows[2]));
            assert_eq!(slice.get(3), None);
            for index in 0..25 {
                assert_eq!(
                    slice.contains(index),
                    rows[2..5].contains(&index),
                    "{selection:?} {index}"
                );
            }
            let mut truncated = selection.clone();
            truncated.truncate(4);
            assert_eq!(truncated.iter().collect::<Vec<_>>(), rows[..4].to_vec());
        }
    }
}
