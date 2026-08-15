//! Arrow-shaped output buffers for one BGEN batch.
//!
//! A decoder that stages each variant in its own allocation pays for it three
//! times: the allocation itself, the per-sample bookkeeping, and a copy into
//! the batch's buffers. These buffers are written directly by the decoder and
//! moved into Arrow arrays when the batch is emitted, so a probability makes
//! one trip from the bitstream to the output.

// Nothing calls this yet: the decoder is moved onto it in the next commit, and
// this attribute goes with that move. It exists so this commit is one
// reviewable unit rather than a rewrite of the decoder and its buffers at once.
#![allow(dead_code)]

use datafusion::arrow::array::BooleanBufferBuilder;
use datafusion::arrow::buffer::NullBuffer;
use datafusion::common::{DataFusionError, Result};

/// How a batch's values are laid out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferLayout {
    /// Probabilities padded to a fixed number of states per sample.
    FixedProbability(usize),
    /// Probabilities with one variable-length list per sample.
    NestedProbability,
    /// One dosage per sample.
    Dosage,
}

/// Buffer lengths at a point in time, so one variant's writes can be undone.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BufferMark {
    values: usize,
    sample_offsets: usize,
    samples: usize,
    variant_offsets: usize,
    ploidy: usize,
    ploidy_offsets: usize,
}

/// Arrow buffers for the batch currently being built.
#[derive(Debug)]
pub(crate) struct GenotypeBuffers {
    layout: BufferLayout,
    values: Vec<f32>,
    /// Empty unless `layout` is [`BufferLayout::NestedProbability`].
    sample_offsets: Vec<i32>,
    /// Materialized on the first missing sample and backfilled; a fully called
    /// cohort writes no validity at all.
    valid: Option<BooleanBufferBuilder>,
    samples: usize,
    /// Index into `values` where the sample being written began.
    sample_start: usize,
    variant_offsets: Vec<i32>,
    ploidy: Vec<u8>,
    ploidy_offsets: Vec<i32>,
}

/// One batch's buffers, moved out for Arrow construction.
#[derive(Debug)]
pub(crate) struct TakenBuffers {
    pub(crate) values: Vec<f32>,
    pub(crate) sample_offsets: Vec<i32>,
    pub(crate) nulls: Option<NullBuffer>,
    pub(crate) variant_offsets: Vec<i32>,
    pub(crate) ploidy: Vec<u8>,
    pub(crate) ploidy_offsets: Vec<i32>,
}

impl GenotypeBuffers {
    pub(crate) fn new(layout: BufferLayout) -> Self {
        Self {
            layout,
            values: Vec::new(),
            sample_offsets: match layout {
                BufferLayout::NestedProbability => vec![0],
                _ => Vec::new(),
            },
            valid: None,
            samples: 0,
            sample_start: 0,
            variant_offsets: vec![0],
            ploidy: Vec::new(),
            ploidy_offsets: vec![0],
        }
    }

    pub(crate) fn rows(&self) -> usize {
        self.variant_offsets.len() - 1
    }

    /// The values buffer, for a decoder that appends a whole sample at once.
    #[inline]
    pub(crate) fn values_mut(&mut self) -> &mut Vec<f32> {
        &mut self.values
    }

    #[inline]
    pub(crate) fn push_state(&mut self, value: f32) {
        self.values.push(value);
    }

    #[inline]
    pub(crate) fn extend_states(&mut self, values: impl IntoIterator<Item = f32>) {
        self.values.extend(values);
    }

    /// Closes a called sample.
    #[inline]
    pub(crate) fn finish_sample(&mut self) -> Result<()> {
        self.close_sample(true)
    }

    /// Closes a sample with no called genotype.
    ///
    /// A fixed layout still reserves the sample's slots, because Arrow sizes a
    /// fixed-size list's values buffer from the entry count rather than from
    /// offsets. Those slots are NaN so a consumer reading the values buffer
    /// directly — which is the point of the fixed layout — does not see a real
    /// 0.0 where there is no genotype.
    #[inline]
    pub(crate) fn finish_missing_sample(&mut self) -> Result<()> {
        self.close_sample(false)
    }

    fn close_sample(&mut self, valid: bool) -> Result<()> {
        match self.layout {
            BufferLayout::FixedProbability(width) => {
                let written = self.values.len() - self.sample_start;
                if written > width {
                    return Err(DataFusionError::Execution(format!(
                        "BGEN fixed probability layout has {width} states per sample, but a \
                         sample stores {written}; use the nested layout for this file"
                    )));
                }
                self.values.resize(self.sample_start + width, f32::NAN);
            }
            BufferLayout::NestedProbability => {
                let end = i32::try_from(self.values.len()).map_err(|_| {
                    DataFusionError::Execution(
                        "BGEN probability offsets exceed the 32-bit Arrow list limit".to_string(),
                    )
                })?;
                self.sample_offsets.push(end);
            }
            BufferLayout::Dosage => {
                debug_assert_eq!(
                    self.values.len() - self.sample_start,
                    usize::from(valid),
                    "a dosage sample writes exactly one value when called and none when missing"
                );
                if !valid {
                    self.values.push(f32::NAN);
                }
            }
        }
        if !valid && self.valid.is_none() {
            let mut builder = BooleanBufferBuilder::new(self.samples + 1);
            builder.append_n(self.samples, true);
            self.valid = Some(builder);
        }
        if let Some(builder) = self.valid.as_mut() {
            builder.append(valid);
        }
        self.samples += 1;
        self.sample_start = self.values.len();
        Ok(())
    }

    #[inline]
    pub(crate) fn push_ploidy(&mut self, ploidy: u8) {
        self.ploidy.push(ploidy);
    }

    /// Closes a variant's samples into one output row.
    pub(crate) fn finish_variant(&mut self) -> Result<()> {
        self.variant_offsets
            .push(i32::try_from(self.samples).map_err(|_| {
                DataFusionError::Execution(
                    "BGEN sample offsets exceed the 32-bit Arrow list limit".to_string(),
                )
            })?);
        self.ploidy_offsets
            .push(i32::try_from(self.ploidy.len()).map_err(|_| {
                DataFusionError::Execution(
                    "BGEN ploidy offsets exceed the 32-bit Arrow list limit".to_string(),
                )
            })?);
        Ok(())
    }

    pub(crate) fn mark(&self) -> BufferMark {
        BufferMark {
            values: self.values.len(),
            sample_offsets: self.sample_offsets.len(),
            samples: self.samples,
            variant_offsets: self.variant_offsets.len(),
            ploidy: self.ploidy.len(),
            ploidy_offsets: self.ploidy_offsets.len(),
        }
    }

    /// Undoes every write since `mark`, leaving a valid Arrow prefix.
    ///
    /// `valid` is a builder and cannot be truncated, so it is rebuilt. This runs
    /// only when a variant fails to decode, which aborts the scan.
    pub(crate) fn rollback(&mut self, mark: BufferMark) {
        self.values.truncate(mark.values);
        self.sample_offsets.truncate(mark.sample_offsets);
        self.variant_offsets.truncate(mark.variant_offsets);
        self.ploidy.truncate(mark.ploidy);
        self.ploidy_offsets.truncate(mark.ploidy_offsets);
        if let Some(mut builder) = self.valid.take() {
            let buffer = builder.finish();
            let mut rebuilt = BooleanBufferBuilder::new(mark.samples);
            for index in 0..mark.samples {
                rebuilt.append(buffer.value(index));
            }
            self.valid = Some(rebuilt);
        }
        self.samples = mark.samples;
        self.sample_start = self.values.len();
    }

    /// Arrow bytes written since `mark`.
    ///
    /// Deliberately the same formula the staged `estimated_arrow_bytes` used —
    /// values, ploidy, and one offset per ploidy entry — even though per-sample
    /// offsets are now countable too. `GenotypeMetric::GenotypeBytes` is
    /// reported from this, and three rounds of review on #220 were spent
    /// settling what those counters mean.
    pub(crate) fn bytes_since(&self, mark: BufferMark) -> usize {
        let values = (self.values.len() - mark.values).saturating_mul(size_of::<f32>());
        let ploidy = self.ploidy.len() - mark.ploidy;
        values
            .saturating_add(ploidy)
            .saturating_add(ploidy.saturating_mul(size_of::<i32>()))
    }

    /// Moves the batch out, leaving empty buffers that keep their capacity.
    pub(crate) fn take(&mut self) -> TakenBuffers {
        let values = std::mem::take(&mut self.values);
        let sample_offsets = std::mem::take(&mut self.sample_offsets);
        let variant_offsets = std::mem::take(&mut self.variant_offsets);
        let ploidy = std::mem::take(&mut self.ploidy);
        let ploidy_offsets = std::mem::take(&mut self.ploidy_offsets);
        let nulls = self
            .valid
            .take()
            .map(|mut builder| NullBuffer::new(builder.finish()));

        // A batch is very likely the same shape as the one before it, so the
        // next batch starts at the previous batch's size. Without this the
        // buffers reallocate their way up from zero on every batch, which is
        // the allocator churn this design exists to remove.
        self.values = Vec::with_capacity(values.len());
        self.sample_offsets = match self.layout {
            BufferLayout::NestedProbability => {
                let mut offsets = Vec::with_capacity(sample_offsets.len());
                offsets.push(0);
                offsets
            }
            _ => Vec::new(),
        };
        self.variant_offsets = Vec::with_capacity(variant_offsets.len());
        self.variant_offsets.push(0);
        self.ploidy = Vec::with_capacity(ploidy.len());
        self.ploidy_offsets = Vec::with_capacity(ploidy_offsets.len());
        self.ploidy_offsets.push(0);
        self.samples = 0;
        self.sample_start = 0;

        TakenBuffers {
            values,
            sample_offsets,
            nulls,
            variant_offsets,
            ploidy,
            ploidy_offsets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fixed_layout_pads_a_short_sample_with_nan_and_writes_no_offsets() {
        let mut buffers = GenotypeBuffers::new(BufferLayout::FixedProbability(4));
        buffers.extend_states([0.5, 0.25, 0.25]);
        buffers.finish_sample().unwrap();
        buffers.push_ploidy(2);
        buffers.finish_variant().unwrap();

        let taken = buffers.take();
        assert_eq!(taken.values.len(), 4, "the sample is padded to the width");
        assert_eq!(&taken.values[..3], &[0.5, 0.25, 0.25]);
        assert!(taken.values[3].is_nan(), "padding is NaN, not zero");
        assert!(
            taken.sample_offsets.is_empty(),
            "a fixed-size list reads no per-sample offsets, so none are built"
        );
        assert_eq!(taken.variant_offsets, vec![0, 1]);
        assert_eq!(taken.ploidy, vec![2]);
        assert_eq!(taken.ploidy_offsets, vec![0, 1]);
        assert!(taken.nulls.is_none(), "every sample was called");
    }

    #[test]
    fn a_fixed_layout_rejects_a_sample_wider_than_its_width() {
        let mut buffers = GenotypeBuffers::new(BufferLayout::FixedProbability(3));
        buffers.extend_states([0.25, 0.25, 0.25, 0.25]);
        let error = buffers.finish_sample().unwrap_err().to_string();
        assert!(
            error.contains("fixed probability layout") && error.contains("nested layout"),
            "{error}"
        );
    }

    #[test]
    fn a_fixed_layout_pads_a_missing_sample_to_the_full_width() {
        let mut buffers = GenotypeBuffers::new(BufferLayout::FixedProbability(3));
        buffers.finish_missing_sample().unwrap();
        buffers.extend_states([1.0, 0.0, 0.0]);
        buffers.finish_sample().unwrap();
        buffers.push_ploidy(2);
        buffers.push_ploidy(2);
        buffers.finish_variant().unwrap();

        let taken = buffers.take();
        assert_eq!(
            taken.values.len(),
            6,
            "Arrow sizes the values buffer by entry count"
        );
        assert!(
            taken.values[..3].iter().all(|value| value.is_nan()),
            "a missing sample's slots read as NaN, not as a real 0.0"
        );
        let nulls = taken
            .nulls
            .expect("a missing sample needs a validity buffer");
        assert!(nulls.is_null(0));
        assert!(nulls.is_valid(1));
    }

    #[test]
    fn a_nested_layout_writes_one_offset_per_sample_and_no_padding() {
        let mut buffers = GenotypeBuffers::new(BufferLayout::NestedProbability);
        buffers.extend_states([0.5, 0.5]);
        buffers.finish_sample().unwrap();
        buffers.finish_missing_sample().unwrap();
        buffers.extend_states([0.2, 0.3, 0.5]);
        buffers.finish_sample().unwrap();
        for _ in 0..3 {
            buffers.push_ploidy(2);
        }
        buffers.finish_variant().unwrap();

        let taken = buffers.take();
        assert_eq!(taken.values, vec![0.5, 0.5, 0.2, 0.3, 0.5]);
        assert_eq!(
            taken.sample_offsets,
            vec![0, 2, 2, 5],
            "a missing sample occupies no states in the nested layout"
        );
        assert_eq!(taken.variant_offsets, vec![0, 3]);
    }

    #[test]
    fn validity_appears_only_once_a_sample_is_missing() {
        let mut buffers = GenotypeBuffers::new(BufferLayout::Dosage);
        for value in [0.0_f32, 1.0, 2.0] {
            buffers.push_state(value);
            buffers.finish_sample().unwrap();
        }
        assert!(
            buffers.take().nulls.is_none(),
            "a fully called cohort must write no validity bytes at all"
        );

        let mut buffers = GenotypeBuffers::new(BufferLayout::Dosage);
        buffers.push_state(0.0);
        buffers.finish_sample().unwrap();
        buffers.finish_missing_sample().unwrap();
        buffers.push_state(2.0);
        buffers.finish_sample().unwrap();
        let nulls = buffers
            .take()
            .nulls
            .expect("a missing dosage needs validity");
        assert!(
            nulls.is_valid(0),
            "samples before the first missing one are backfilled"
        );
        assert!(nulls.is_null(1));
        assert!(nulls.is_valid(2));
    }

    #[test]
    fn rollback_restores_every_buffer_to_its_mark() {
        let mut buffers = GenotypeBuffers::new(BufferLayout::NestedProbability);
        buffers.extend_states([1.0, 0.0]);
        buffers.finish_sample().unwrap();
        buffers.push_ploidy(2);
        buffers.finish_variant().unwrap();

        let mark = buffers.mark();
        buffers.extend_states([0.0, 1.0]);
        buffers.finish_missing_sample().unwrap();
        buffers.push_ploidy(2);
        buffers.rollback(mark);

        let taken = buffers.take();
        assert_eq!(taken.values, vec![1.0, 0.0]);
        assert_eq!(taken.sample_offsets, vec![0, 2]);
        assert_eq!(taken.variant_offsets, vec![0, 1]);
        assert_eq!(taken.ploidy, vec![2]);
    }

    #[test]
    fn take_resets_the_buffers_and_keeps_their_capacity() {
        let mut buffers = GenotypeBuffers::new(BufferLayout::FixedProbability(2));
        buffers.extend_states([0.5, 0.5]);
        buffers.finish_sample().unwrap();
        buffers.push_ploidy(2);
        buffers.finish_variant().unwrap();
        let taken = buffers.take();
        assert_eq!(taken.values.len(), 2);

        assert_eq!(buffers.rows(), 0, "take leaves an empty batch");
        let second = buffers.take();
        assert!(second.values.is_empty());
        assert_eq!(
            second.variant_offsets,
            vec![0],
            "a reset batch still carries its leading zero offset"
        );
    }
}
