# BGEN Probability Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a BGEN probability scan faster than snputils on both the uniform-width and the mixed-width fixture, by decoding straight into batch-level Arrow buffers and by letting the fixed layout NaN-pad a mixed-width file.

**Architecture:** Today `decode_variant` stages each variant in a freshly allocated `ProbabilityValues` and `build_genotypes` copies every staged variant into batch-level buffers. That single decision accounts for three profile frames — `memmove` 12%, `finish_sample` 9.5%, allocator churn ~10%. A new `GenotypeBuffers` (in a new `src/buffers.rs`) becomes the only place probabilities, dosages, ploidy, and offsets are written; the decoder appends into it and `build_batch` moves its `Vec`s into Arrow arrays with no copy. On top of that, the fixed layout gains per-sample NaN padding to a catalog-derived width, so the phased chr22 fixture can use the fixed layout at all.

**Tech Stack:** Rust 1.88, DataFusion 52.1, arrow-rs (via `datafusion::arrow`), criterion, tokio. Python side: NumPy, PyArrow, polars-bio's venv.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-15-bgen-probability-performance-design.md`. Read it before task 1.
- Branch `agent/bgen-probability-perf` in `~/CLionProjects/datafusion-bio-formats-bgen-perf`, off `agent/add-bgen-provider` (`2c06f23`). Do not commit to `agent/add-bgen-provider`; `~/CLionProjects/datafusion-bio-formats-bgen` stays pinned to `2c06f23` as the A/B baseline.
- Every task ends green on: `cargo test -p datafusion-bio-format-bgen --all-targets` and `cargo clippy -p datafusion-bio-format-bgen --all-targets -- -D warnings`. Tasks touching `bio-format-core` add `-p datafusion-bio-format-core` to both.
- Values already emitted must not change, with exactly one exception: task 9 (NaN padding, NaN for missing samples in the fixed layout). A value diff anywhere else is a bug, not a trade-off.
- Never claim an edit landed without re-reading the file. On PR #220 a `checked_add` fix was silently reverted by a later edit to the same block and the build gave no signal.
- Benchmarks: measure interleaved with a warm-up; compare fresh-process numbers only against fresh-process numbers. `datafusion.execution.batch_size` makes no difference — an apparent win from raising it was a cold-versus-warm artifact.
- Fixtures live in `~/research/data/BGEN/`: `chr22.full.bgen` and `chr22.first-25000.bgen` (993,881 / 25,000 variants x 2,548 samples, mixed widths 3/4), plus `.unphased.` counterparts (uniform width 3). Python is `~/CLionProjects/polars-bio/.venv/bin/python`.

---

### Task 1: Real-file benchmark harness

Without this the loop is a `maturin develop --release` rebuild per change. The existing bench fixture is a synthetic 2,048 x 256 file — a fine regression guard, useless for guiding this work.

**Files:**
- Modify: `datafusion/bio-format-bgen/benches/bgen_scan.rs:250-354`

**Interfaces:**
- Produces: a criterion group `bgen_real_file` with benches named `real_<mode>_<layout>_p<partitions>`, active only when `BGEN_BENCH_PATH` is set.

- [ ] **Step 1: Add the opt-in group**

Append to `benchmarks()` in `benches/bgen_scan.rs`, immediately before `group.finish();`:

```rust
    // A real cohort file is the only fixture that can guide probability-path
    // work; the synthetic one above is 2,048 x 256 and dominated by fixed
    // costs. Opt in with BGEN_BENCH_PATH so CI, which has no such file, keeps
    // running the synthetic benches alone.
    if let Ok(real_path) = std::env::var("BGEN_BENCH_PATH") {
        for (mode_name, output_mode) in [
            ("probability", BgenOutputMode::Probability),
            ("dosage", BgenOutputMode::Dosage),
        ] {
            for (layout_name, layout) in [
                ("nested", BgenProbabilityLayout::Nested),
                ("fixed", BgenProbabilityLayout::Fixed),
            ] {
                if output_mode == BgenOutputMode::Dosage
                    && layout == BgenProbabilityLayout::Fixed
                {
                    continue;
                }
                for partitions in [1_usize, 8] {
                    let table = format!("real_{mode_name}_{layout_name}_p{partitions}");
                    let context = runtime.block_on(context(
                        &real_path,
                        &table,
                        BgenReadOptions {
                            output_mode,
                            probability_layout: layout,
                            ..Default::default()
                        },
                        partitions,
                    ));
                    let sql = format!("SELECT genotypes FROM {table}");
                    group.bench_function(&table, |bencher| {
                        bencher
                            .to_async(&runtime)
                            .iter(|| async { black_box(execute(&context, &sql).await) });
                    });
                }
            }
        }
    }
```

Add `BgenProbabilityLayout` to the `datafusion_bio_format_bgen` import at line 8.

- [ ] **Step 2: Verify the bench still compiles and skips without the variable**

Run: `cargo bench -p datafusion-bio-format-bgen -- --list`
Expected: the synthetic bench names are listed and no `real_` name appears.

- [ ] **Step 3: Verify the opt-in path runs**

Run: `BGEN_BENCH_PATH=~/research/data/BGEN/chr22.first-25000.unphased.bgen cargo bench -p datafusion-bio-format-bgen -- --list`
Expected: names including `real_probability_fixed_p8` appear.

- [ ] **Step 4: Record the baseline**

Run each of these to completion and paste the criterion medians into `docs/superpowers/plans/2026-08-15-bgen-baseline.md`, one table with columns fixture / mode / layout / partitions / median:

```bash
cd ~/CLionProjects/datafusion-bio-formats-bgen-perf
for f in chr22.first-25000.unphased chr22.first-25000; do
  BGEN_BENCH_PATH=~/research/data/BGEN/$f.bgen \
    cargo bench -p datafusion-bio-format-bgen -- real_
done
```

Note in that file which fixture is which: `chr22.first-25000.unphased` is uniform width 3, `chr22.first-25000` is mixed 3/4 and therefore **cannot** run the `fixed` benches yet — expect those to error, and record that they error. That error disappearing is task 9's deliverable.

- [ ] **Step 5: Commit**

```bash
git add datafusion/bio-format-bgen/benches/bgen_scan.rs docs/superpowers/plans/2026-08-15-bgen-baseline.md
git commit -m "bench(bgen): add an opt-in real-file scan benchmark and record the baseline"
```

---

### Task 2: `should_flush_after` on the batch sizer

Decoding into batch buffers inverts the flush decision: the row is already in the buffers by the time its size is known, so the sizer must answer "flush now?" after the append rather than before it.

**Files:**
- Modify: `datafusion/bio-format-core/src/genotype.rs:405-433`
- Test: `datafusion/bio-format-core/src/genotype.rs` (the existing `#[cfg(test)] mod tests` at line 436)

**Interfaces:**
- Produces: `GenotypeBatchSizer::should_flush_after(&self) -> bool` — true when the batch is non-empty and has reached `max_rows` or exceeded `soft_byte_limit`.

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `datafusion/bio-format-core/src/genotype.rs`:

```rust
    #[test]
    fn should_flush_after_reports_a_full_batch() {
        let mut sizer = GenotypeBatchSizer::new(2, 100).unwrap();
        assert!(!sizer.should_flush_after(), "an empty batch never flushes");

        sizer.push_row(10);
        assert!(!sizer.should_flush_after(), "one small row is not full");

        sizer.push_row(10);
        assert!(sizer.should_flush_after(), "max_rows reached");

        let mut sizer = GenotypeBatchSizer::new(100, 50).unwrap();
        sizer.push_row(60);
        assert!(
            sizer.should_flush_after(),
            "a single row over the soft limit flushes after it is appended, \
             because it is already in the buffers"
        );
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datafusion-bio-format-core should_flush_after_reports_a_full_batch`
Expected: FAIL, `no method named should_flush_after`.

- [ ] **Step 3: Implement**

Add to `impl GenotypeBatchSizer`, directly after `should_flush_before`:

```rust
    /// Returns true when the current batch should be emitted now that its rows
    /// are appended.
    ///
    /// A caller that writes a row directly into its output buffers cannot
    /// consult [`Self::should_flush_before`], because the row's size is not
    /// known until it is written. Such a batch can exceed `soft_byte_limit` by
    /// one row, which is the same slack the first row of every batch has.
    pub fn should_flush_after(&self) -> bool {
        self.rows > 0
            && (self.rows >= self.max_rows || self.estimated_bytes > self.soft_byte_limit)
    }
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p datafusion-bio-format-core && cargo clippy -p datafusion-bio-format-core --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add datafusion/bio-format-core/src/genotype.rs
git commit -m "feat(core): let a genotype batch sizer decide after a row is appended"
```

---

### Task 3: `GenotypeBuffers` — the batch-level Arrow buffers

Pure data structure, no wiring. Tasks 4-6 move callers onto it.

`decode.rs` is already 1,527 lines, so this goes in its own file.

**Files:**
- Create: `datafusion/bio-format-bgen/src/buffers.rs`
- Modify: `datafusion/bio-format-bgen/src/lib.rs` (add `mod buffers;`)

**Interfaces:**
- Produces:
  - `pub(crate) enum BufferLayout { FixedProbability(usize), NestedProbability, Dosage }`
  - `pub(crate) struct GenotypeBuffers`
  - `GenotypeBuffers::new(layout: BufferLayout) -> Self`
  - `GenotypeBuffers::layout(&self) -> BufferLayout`
  - `GenotypeBuffers::push_state(&mut self, value: f32)`
  - `GenotypeBuffers::extend_states(&mut self, values: impl IntoIterator<Item = f32>)`
  - `GenotypeBuffers::reserve_states(&mut self, additional: usize)`
  - `GenotypeBuffers::finish_sample(&mut self) -> Result<()>`
  - `GenotypeBuffers::finish_missing_sample(&mut self) -> Result<()>`
  - `GenotypeBuffers::push_ploidy(&mut self, ploidy: u8)`
  - `GenotypeBuffers::finish_variant(&mut self) -> Result<()>`
  - `GenotypeBuffers::mark(&self) -> BufferMark` / `rollback(&mut self, mark: BufferMark)` / `bytes_since(&self, mark: BufferMark) -> usize`
  - `GenotypeBuffers::rows(&self) -> usize`
  - `GenotypeBuffers::take(&mut self) -> TakenBuffers`
  - `pub(crate) struct TakenBuffers { pub values: Vec<f32>, pub sample_offsets: Vec<i32>, pub nulls: Option<NullBuffer>, pub variant_offsets: Vec<i32>, pub ploidy: Vec<u8>, pub ploidy_offsets: Vec<i32> }`

- [ ] **Step 1: Write the failing tests**

Create `datafusion/bio-format-bgen/src/buffers.rs` containing only this test module for now:

```rust
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
        assert_eq!(taken.values.len(), 6, "Arrow sizes the values buffer by entry count");
        assert!(
            taken.values[..3].iter().all(|value| value.is_nan()),
            "a missing sample's slots read as NaN, not as a real 0.0"
        );
        let nulls = taken.nulls.expect("a missing sample needs a validity buffer");
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
        let nulls = buffers.take().nulls.expect("a missing dosage needs validity");
        assert!(nulls.is_valid(0), "samples before the first missing one are backfilled");
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion-bio-format-bgen --lib buffers`
Expected: FAIL to compile — `cannot find type GenotypeBuffers`. (Add `mod buffers;` to `src/lib.rs` first if the module is not compiled at all.)

- [ ] **Step 3: Implement**

Prepend to `datafusion/bio-format-bgen/src/buffers.rs`:

```rust
//! Arrow-shaped output buffers for one BGEN batch.
//!
//! A decoder that stages each variant in its own allocation pays for it three
//! times: the allocation itself, the per-sample bookkeeping, and a copy into
//! the batch's buffers. These buffers are written directly by the decoder and
//! moved into Arrow arrays when the batch is emitted, so a probability makes
//! one trip from the bitstream to the output.

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

    pub(crate) fn layout(&self) -> BufferLayout {
        self.layout
    }

    pub(crate) fn rows(&self) -> usize {
        self.variant_offsets.len() - 1
    }

    pub(crate) fn reserve_states(&mut self, additional: usize) {
        self.values.reserve(additional);
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
        if let Some(builder) = self.valid.take() {
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
```

Add to `datafusion/bio-format-bgen/src/lib.rs`, next to the other module declarations:

```rust
mod buffers;
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p datafusion-bio-format-bgen --lib buffers`
Expected: 7 passing tests.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p datafusion-bio-format-bgen --all-targets -- -D warnings
git add datafusion/bio-format-bgen/src/buffers.rs datafusion/bio-format-bgen/src/lib.rs
git commit -m "feat(bgen): add batch-level Arrow genotype buffers"
```

---

### Task 4: Decode probabilities into the buffers

`decode_variant` stops returning staged values and appends to the buffers instead. Dosage keeps its `Vec<Option<f32>>` for one more task so this change stays reviewable.

**Files:**
- Modify: `datafusion/bio-format-bgen/src/decode.rs:8-89` (`DecodedGenotypes`, delete `ProbabilityValues`, delete `DecodedValues::Probabilities`)
- Modify: `datafusion/bio-format-bgen/src/decode.rs:125-154` (`decode_variant` signature)
- Modify: `datafusion/bio-format-bgen/src/decode.rs:221-277` (layout 1)
- Modify: `datafusion/bio-format-bgen/src/decode.rs:625-779` (layout 2, both the fast path and the general path)
- Modify: `datafusion/bio-format-bgen/src/physical_exec.rs:137-357` (stream and `build_batch`)
- Modify: `datafusion/bio-format-bgen/src/table_provider.rs:461-503` (`probe_probability_width`)

**Interfaces:**
- Consumes: `GenotypeBuffers`, `BufferLayout`, `TakenBuffers`, `BufferMark` from task 3; `should_flush_after` from task 2.
- Produces:
  - `decode_variant(path: &str, variant: &BgenVariant, header: &BgenHeader, payload: &[u8], selected_samples: &[usize], options: &BgenReadOptions, scratch: &mut DecodeScratch, buffers: &mut GenotypeBuffers) -> Result<DecodedGenotypes>`
  - `DecodedGenotypes { phased: bool, bits: u8, decompressed_bytes: usize, state_width: Option<usize> }` — `ploidy` and `values` are gone; both now live in the buffers.

- [ ] **Step 1: Run the existing tests to capture the green baseline**

Run: `cargo test -p datafusion-bio-format-bgen`
Expected: PASS. These integration tests are the specification for this task — every one of them must still pass at step 4 without being edited. If a test needs editing to pass, stop: that is a behavior change, and only task 9 is allowed one.

- [ ] **Step 2: Change the shape**

In `decode.rs`:

1. Delete `struct ProbabilityValues` and its `impl` (lines 21-69), and delete `enum DecodedValues` (lines 71-75).
2. Reduce `DecodedGenotypes` to:

```rust
/// What a decoded variant reports beyond the values it wrote into the batch
/// buffers.
#[derive(Debug)]
pub(crate) struct DecodedGenotypes {
    pub(crate) phased: bool,
    pub(crate) bits: u8,
    pub(crate) decompressed_bytes: usize,
    /// Probability states every sample of this variant stores, when the variant
    /// declares one ploidy. `None` for a variable-ploidy variant.
    pub(crate) state_width: Option<usize>,
}
```

3. Delete `estimated_arrow_bytes` — `GenotypeBuffers::bytes_since` replaces it.
4. Add `buffers: &mut GenotypeBuffers` as the last parameter of `decode_variant`, `decode_layout1`, `decode_layout2`, and `decode_layout2_block`, and thread it through.

In layout 1 (lines 221-264), replace the `probabilities`/`dosages` locals so the probability arm writes to the buffers and the dosage arm keeps its `Vec<Option<f32>>`:

```rust
    let mut dosages = Vec::with_capacity(selected_samples.len());
    for &sample in selected_samples {
        // ... unchanged offset, values, and sum validation ...
        if sum == 0 {
            match options.output_mode {
                BgenOutputMode::Probability => buffers.finish_missing_sample()?,
                BgenOutputMode::Dosage => dosages.push(None),
            }
            buffers.push_ploidy(2);
            continue;
        }
        // ... unchanged 32767..=32769 check ...
        match options.output_mode {
            BgenOutputMode::Probability => {
                buffers.extend_states(values.iter().map(|value| *value as f32 / 32_768.0));
                buffers.finish_sample()?;
            }
            BgenOutputMode::Dosage => {
                dosages.push(Some((values[1] + 2 * values[2]) as f32 / 32_768.0));
            }
        }
        buffers.push_ploidy(2);
    }
```

Note the ploidy move: `ploidy: vec![2; selected_samples.len()]` in the returned struct becomes a `push_ploidy(2)` on **every** path through the loop, missing samples included. Layout 1 is always diploid.

In layout 2, apply the same transformation at all three sites — the fast path (lines 646-694), the general path's missing branch (line 728-733), and the general path's probability arm (lines 739-755) — replacing `probabilities.finish_missing_sample(missing_pad)` with `buffers.finish_missing_sample()`, `probabilities.values.extend(...)` with `buffers.extend_states(...)`, `probabilities.finish_sample(true)` with `buffers.finish_sample()`, and `ploidies.push(x)` with `buffers.push_ploidy(x)`. Delete the `missing_pad` local (lines 514-522) and the `fixed_layout` local in layout 1 (line 221): the buffers own padding now. In the fast path, keep `byte_probabilities_into` writing into a `&mut Vec<f32>` by passing `buffers.values_mut()` — add that accessor to `GenotypeBuffers`:

```rust
    /// The values buffer, for a decoder that appends a whole sample at once.
    #[inline]
    pub(crate) fn values_mut(&mut self) -> &mut Vec<f32> {
        &mut self.values
    }
```

`byte_probabilities_into` already truncates its own partial write when it returns `None`, so a rejected sample leaves the buffer as it found it.

- [ ] **Step 3: Rewire the stream and the batch builder**

In `physical_exec.rs`:

1. `struct DecodedRow` becomes metadata only:

```rust
#[derive(Debug)]
struct DecodedRow {
    variant_index: usize,
    phased: bool,
    bits: u8,
}
```

2. In `execute`, build the buffers once per partition, before the loop:

```rust
            let layout = match (fileset.options.output_mode, fileset.probability_width) {
                (BgenOutputMode::Dosage, _) => BufferLayout::Dosage,
                (BgenOutputMode::Probability, Some(width)) => {
                    BufferLayout::FixedProbability(width)
                }
                (BgenOutputMode::Probability, None) => BufferLayout::NestedProbability,
            };
            let mut buffers = GenotypeBuffers::new(layout);
```

3. Replace the decode-then-flush block with mark, decode, rollback-on-error, finish, flush-after:

```rust
                        let mark = buffers.mark();
                        let decoded = match decode_variant(
                            &fileset.path,
                            variant,
                            &fileset.header,
                            payload,
                            decode_samples,
                            &fileset.options,
                            &mut scratch,
                            &mut buffers,
                        ) {
                            Ok(decoded) => decoded,
                            Err(error) => {
                                // A failed variant leaves a partial row behind;
                                // drop it so the buffers stay a valid Arrow
                                // prefix rather than a torn row.
                                buffers.rollback(mark);
                                Err(error)?
                            }
                        };
                        buffers.finish_variant()?;
                        let row_bytes = buffers.bytes_since(mark);
                        metrics.add(
                            GenotypeMetric::DecompressedBytes,
                            decoded.decompressed_bytes as u64,
                        );
                        metrics.add(GenotypeMetric::SamplesDecoded, decode_samples.len() as u64);
                        metrics.add(
                            GenotypeMetric::SampleValuesSkipped,
                            (fileset.header.sample_count as usize)
                                .saturating_sub(decode_samples.len()) as u64,
                        );
                        rows.push(DecodedRow {
                            variant_index,
                            phased: decoded.phased,
                            bits: decoded.bits,
                        });
                        sizer.push_row(row_bytes);
                        if sizer.should_flush_after() {
                            let row_count = rows.len();
                            let batch =
                                build_batch(&fileset, schema.clone(), &rows, buffers.take())?;
                            record_batch_metrics(&metrics, row_count, sizer.estimated_bytes());
                            yield batch;
                            rows.clear();
                            sizer.reset();
                        }
```

The metadata-only branch (`assignment.ranges.is_empty()`, lines 144-164) keeps `should_flush_before` — it decodes nothing and writes nothing to the buffers — but must still call `buffers.finish_variant()?` per row so the row count matches, and must pass `buffers.take()` to `build_batch`. Delete `empty_genotypes` (lines 272-286); an empty row is now just a `finish_variant` with no samples.

4. `build_batch` and `build_genotypes` take the taken buffers and stop copying:

```rust
fn build_batch(
    fileset: &BgenFileset,
    schema: SchemaRef,
    rows: &[DecodedRow],
    buffers: TakenBuffers,
) -> Result<RecordBatch>
```

`build_genotypes(data_type, buffers, mode)` becomes construction only — every loop over `rows` in it disappears:

```rust
fn build_genotypes(
    data_type: &DataType,
    buffers: TakenBuffers,
    mode: BgenOutputMode,
) -> Result<ArrayRef> {
    let DataType::Struct(fields) = data_type else {
        return Err(DataFusionError::Execution(
            "BGEN genotypes field is not a struct".to_string(),
        ));
    };
    let TakenBuffers {
        values,
        sample_offsets,
        nulls,
        variant_offsets,
        ploidy,
        ploidy_offsets,
    } = buffers;
    let states = Arc::new(Float32Array::from(values)) as ArrayRef;
    let genotype_values: ArrayRef = match mode {
        BgenOutputMode::Probability => {
            let state_field = Arc::new(Field::new("state", DataType::Float32, false));
            let samples: ArrayRef = match fixed_probability_width(data_type) {
                Some(width) => Arc::new(FixedSizeListArray::try_new(
                    state_field.clone(),
                    width,
                    states,
                    nulls,
                )?),
                None => Arc::new(ListArray::try_new(
                    state_field.clone(),
                    OffsetBuffer::new(ScalarBuffer::from(sample_offsets)),
                    states,
                    nulls,
                )?),
            };
            let sample_field = Arc::new(Field::new("sample", samples.data_type().clone(), true));
            Arc::new(ListArray::try_new(
                sample_field,
                OffsetBuffer::new(ScalarBuffer::from(variant_offsets)),
                samples,
                None,
            )?)
        }
        BgenOutputMode::Dosage => unreachable!("dosage still stages its own values"),
    };
    let ploidy_values = Arc::new(UInt8Array::from(ploidy)) as ArrayRef;
    let ploidy_array = Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt8, false)),
        OffsetBuffer::new(ScalarBuffer::from(ploidy_offsets)),
        ploidy_values,
        None,
    )?);
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        vec![genotype_values, ploidy_array],
        None,
    )?))
}
```

Keep the dosage arm working by leaving the old dosage code path in place for now — pass `rows` alongside the buffers and branch on `mode` at the call site. Task 5 deletes it.

The `phased` and `bits` columns now read `row.phased` / `row.bits` instead of `row.genotypes.as_ref().map(...)`. Delete the `payload_derived_projected` local: those two values are always present on the row now.

5. In `table_provider.rs`, `probe_probability_width` passes a throwaway buffer:

```rust
    let mut buffers = GenotypeBuffers::new(BufferLayout::NestedProbability);
    let decoded = decode_variant(
        path, variant, header, &payload, &[], options, &mut scratch, &mut buffers,
    )?;
```

The probe selects no samples, so nothing is written to it.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p datafusion-bio-format-bgen`
Expected: PASS, with no test file edited. Pay attention to `honors_row_and_soft_byte_batch_limits` — it is the test that pins the flush behavior this task inverts. If it fails on batch boundaries, the fix is in how `should_flush_after` is called, not in the test.

- [ ] **Step 5: Confirm the metrics did not drift**

Run: `cargo test -p datafusion-bio-format-bgen exact_filters_limits_and_metadata_projection_skip_payloads empty_sample_selection_emits_empty_values_without_payload_reads`
Expected: PASS. `GenotypeBytes` is now exact rather than estimated; these tests assert on counters and will catch a formula that drifted.

- [ ] **Step 6: Lint and commit**

```bash
cargo clippy -p datafusion-bio-format-bgen --all-targets -- -D warnings
git add datafusion/bio-format-bgen/src
git commit -m "perf(bgen): decode probabilities straight into the batch buffers"
```

---

### Task 5: Decode dosages into the buffers

**Files:**
- Modify: `datafusion/bio-format-bgen/src/decode.rs` (both layouts' dosage arms)
- Modify: `datafusion/bio-format-bgen/src/physical_exec.rs` (`build_genotypes` dosage arm)

**Interfaces:**
- Consumes: `GenotypeBuffers` in `BufferLayout::Dosage` from task 3.
- Produces: `DecodedValues` and the last `Vec<Option<f32>>` staging are gone from `decode.rs`.

- [ ] **Step 1: Replace the dosage staging in the decoder**

In `decode.rs`, at every `dosages.push(...)` site (layout 1 lines ~236 and ~261; layout 2 lines ~655, ~680-691, ~731, ~756-763), replace:

```rust
                    dosages.push(None);
```

with:

```rust
                    buffers.finish_missing_sample()?;
```

and:

```rust
                    dosages.push(Some(numerator as f32 / denominator as f32));
```

with:

```rust
                    buffers.push_state(numerator as f32 / denominator as f32);
                    buffers.finish_sample()?;
```

Delete every `let mut dosages = Vec::with_capacity(...)` and the `DecodedValues` construction in the returned `DecodedGenotypes`.

- [ ] **Step 2: Replace the dosage arm of `build_genotypes`**

```rust
        BgenOutputMode::Dosage => Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            OffsetBuffer::new(ScalarBuffer::from(variant_offsets)),
            states,
            None,
        )?),
```

The item field stays `Float32` and **nullable**, and the null buffer belongs to the values array, not the list — `Float32Array::from(values)` must be rebuilt with `nulls`:

```rust
    let states = Arc::new(Float32Array::new(
        ScalarBuffer::from(values),
        nulls,
    )) as ArrayRef;
```

Move that construction into each arm so probability keeps putting `nulls` on the sample list and dosage puts them on the values. Getting this backwards produces a schema that no longer matches `build_schema`, so `RecordBatch::try_new` will reject it — the tests will say so immediately.

Remove the `rows` parameter from `build_genotypes`; it no longer reads any row.

- [ ] **Step 3: Run the tests**

Run: `cargo test -p datafusion-bio-format-bgen`
Expected: PASS with no test edited. `emits_biallelic_dosage_and_rejects_multiallelic_selection` and `decodes_layout1_uncompressed_and_zlib` are the ones that pin dosage nullability.

- [ ] **Step 4: Check the dosage benchmark did not regress**

Run:
```bash
BGEN_BENCH_PATH=~/research/data/BGEN/chr22.first-25000.unphased.bgen \
  cargo bench -p datafusion-bio-format-bgen -- real_dosage
```
Expected: within noise of, or faster than, the task 1 baseline. Dosage is 1.9x ahead of snputils today; this refactor must not spend that.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p datafusion-bio-format-bgen --all-targets -- -D warnings
git add datafusion/bio-format-bgen/src
git commit -m "perf(bgen): decode dosages straight into the batch buffers"
```

---

### Task 6: Measure the refactor

A checkpoint, not a code change. Phase 3 of the spec is conditional on this number.

**Files:**
- Modify: `docs/superpowers/plans/2026-08-15-bgen-baseline.md`

- [ ] **Step 1: Re-run the benchmark**

```bash
cd ~/CLionProjects/datafusion-bio-formats-bgen-perf
BGEN_BENCH_PATH=~/research/data/BGEN/chr22.first-25000.unphased.bgen \
  cargo bench -p datafusion-bio-format-bgen -- real_
```

- [ ] **Step 2: Record before/after**

Add an "after tasks 4-5" column to the table in `docs/superpowers/plans/2026-08-15-bgen-baseline.md`. State the percentage change per row. If any row got slower, say so in the file — a benchmark that only records wins is not a benchmark.

- [ ] **Step 3: Re-profile**

```bash
cargo bench -p datafusion-bio-format-bgen --no-run
# then sample the bench binary with your profiler of choice, e.g.
# samply record ./target/release/deps/bgen_scan-<hash> --bench real_probability_fixed_p8
```

Record the new frame shares next to the spec's table (`byte_probabilities_into` 23%, libdeflate 23%, `decode_variant` 16%, `memmove` 12%, `finish_sample` 9.5%, allocator ~10%, `build_genotypes` 2%). `finish_sample` and `memmove` should have collapsed; whatever is now on top decides whether task 12 happens.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/plans/2026-08-15-bgen-baseline.md
git commit -m "docs(bgen): record the batch-buffer benchmark and profile"
```

---

### Task 7: Report the probed variant's ploidy and phasing

Task 8 derives the schema width from the probe plus the catalog, and needs more from the probe than a width.

**Files:**
- Modify: `datafusion/bio-format-bgen/src/decode.rs` (`DecodedGenotypes`, both layouts' return values)
- Modify: `datafusion/bio-format-bgen/src/table_provider.rs:461-503`
- Test: `datafusion/bio-format-bgen/tests/provider_test.rs`

**Interfaces:**
- Produces: `DecodedGenotypes.declared_ploidy: Option<u8>` — the single ploidy every sample of the variant declares, or `None` when the variant declares a range.

- [ ] **Step 1: Write the failing test**

Add to `tests/provider_test.rs`:

```rust
#[tokio::test]
async fn the_width_probe_reports_the_first_variants_shape() {
    // v1 is unphased biallelic diploid, so a fixed-layout schema for this
    // fixture is derived from ploidy 2, unphased. v3 is triallelic, and an
    // unphased triallelic diploid sample stores six states, so the schema has
    // to be six wide rather than v1's three.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let schema = TableProvider::schema(&provider);
    let genotypes = schema.field_with_name("genotypes").unwrap();
    assert!(
        format!("{:?}", genotypes.data_type()).contains("FixedSizeList(Field { name: \"state\""),
        "{:?}",
        genotypes.data_type()
    );
    assert!(
        format!("{:?}", genotypes.data_type()).contains(", 6)"),
        "the width must cover the widest catalog variant, not just variant 0: {:?}",
        genotypes.data_type()
    );
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p datafusion-bio-format-bgen the_width_probe_reports_the_first_variants_shape`
Expected: FAIL — the schema is 3 wide, because the width comes from variant 0 alone.

- [ ] **Step 3: Add `declared_ploidy`**

In `decode.rs`, add the field to `DecodedGenotypes` with this doc comment:

```rust
    /// The one ploidy every sample of this variant declares, or `None` when the
    /// variant declares a range. The probe reads this without selecting any
    /// sample, so it cannot come from the per-sample ploidy the buffers hold.
    pub(crate) declared_ploidy: Option<u8>,
```

Set it in every `DecodedGenotypes` construction: `Some(2)` in layout 1 (always diploid), and in layout 2 `uniform_stride_bits.map(|_| min_ploidy)` — the same condition that produces `uniform_state_width`, so a variable-ploidy variant reports `None` for both.

- [ ] **Step 4: Widen the probe's return**

In `table_provider.rs`, change `probe_probability_width` to return `(ProbeShape, u64, u64)` where:

```rust
/// What variant 0 says about the file's probability shape.
#[derive(Debug, Clone, Copy)]
struct ProbeShape {
    width: usize,
    ploidy: u8,
    phased: bool,
}
```

Keep the existing `max_states_per_sample` check and the existing "declares a variable ploidy" error for `state_width == None`; add the same `ok_or_else` for `declared_ploidy`, reusing that message. Store the shape on `BgenFileset` as `probability_shape: Option<ProbeShape>`, replacing `probability_width`, and have `build_schema` read `fileset.probability_shape.map(|shape| shape.width)` for now — task 8 changes what that width is.

`probability_width` has a second reader: the `BufferLayout` selection added to `physical_exec.rs` in task 4. Update it in the same commit:

```rust
                (BgenOutputMode::Probability, Some(shape)) => {
                    BufferLayout::FixedProbability(shape.width)
                }
```

matching on `fileset.probability_shape`. `ProbeShape` therefore needs `pub(crate)` visibility, not private.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p datafusion-bio-format-bgen`
Expected: everything passes except `the_width_probe_reports_the_first_variants_shape`, which still fails on the width being 3. That is task 8.

- [ ] **Step 6: Commit**

```bash
git add datafusion/bio-format-bgen/src
git commit -m "refactor(bgen): report the probed variant's ploidy and phasing"
```

---

### Task 8: Derive the fixed width from the catalog

**Files:**
- Modify: `datafusion/bio-format-bgen/src/decode.rs` (make `complete_probability_count` visible)
- Modify: `datafusion/bio-format-bgen/src/table_provider.rs:203-215`
- Test: `datafusion/bio-format-bgen/tests/provider_test.rs` (task 7's test)

**Interfaces:**
- Consumes: `ProbeShape` from task 7.
- Produces: `pub(crate) fn complete_probability_count(ploidy: u8, allele_count: usize, phased: bool) -> Result<u64>` — already exists at `decode.rs:789`, needs `pub(crate)`.

- [ ] **Step 1: Implement the derivation**

In `table_provider.rs`, after the probe:

```rust
/// Widest sample any catalog variant can store, given the shape variant 0
/// declares.
///
/// A Layout 2 block header lives inside the compressed payload, so learning
/// every variant's exact width would mean decompressing the whole file at plan
/// time. Allele counts are already in the catalog, and ploidy and phasing are
/// constant across a file in practice, so the widest state count follows from
/// the probe plus the catalog at no I/O cost. A variant that turns out to store
/// more than this is rejected during the scan rather than silently truncated.
fn derive_fixed_width(catalog: &BgenCatalog, shape: ProbeShape) -> Result<usize> {
    let mut width = shape.width as u64;
    for variant in &catalog.variants {
        width = width.max(complete_probability_count(
            shape.ploidy,
            variant.alleles.len(),
            shape.phased,
        )?);
    }
    usize::try_from(width).map_err(|_| {
        DataFusionError::Plan("BGEN probability width does not fit usize".to_string())
    })
}
```

Call it where the probe result is stored, and **overwrite `ProbeShape::width` with the derived width** before the shape goes onto the `BgenFileset`:

```rust
                let (mut shape, bytes, decompressed) =
                    probe_probability_width(&path, &source, &header, &catalog, &options).await?;
                shape.width = derive_fixed_width(&catalog, shape)?;
                (Some(shape), bytes, decompressed)
```

That keeps one width in play. `build_schema` and the `BufferLayout` selection both read `shape.width` and both get the derived one, so neither has to know which is which. Keep `ProbeShape`'s `ploidy`/`phased` — the scan does not need them, but the error message in task 9 is clearer with them and a future exact-width pass will.

- [ ] **Step 2: Run the test**

Run: `cargo test -p datafusion-bio-format-bgen the_width_probe_reports_the_first_variants_shape`
Expected: PASS — the schema is now 6 wide.

- [ ] **Step 3: Run the whole suite**

Run: `cargo test -p datafusion-bio-format-bgen`
Expected: `fixed_probability_layout_rejects_a_mixed_width_file` now fails differently — the schema is wide enough, so the scan gets further before rejecting. Leave it failing; task 9 rewrites it. Every other test passes.

- [ ] **Step 4: Commit**

```bash
git add datafusion/bio-format-bgen/src
git commit -m "feat(bgen): derive the fixed probability width from the catalog"
```

---

### Task 9: NaN-pad the fixed layout

The one task in this plan that changes emitted values.

**Files:**
- Modify: `datafusion/bio-format-bgen/src/physical_exec.rs` (delete the per-variant `state_width` check, lines 428-443 of the original)
- Modify: `datafusion/bio-format-bgen/src/table_provider.rs:58-64` (the `Fixed` doc comment)
- Test: `datafusion/bio-format-bgen/tests/provider_test.rs:1443-1471`

**Interfaces:**
- Consumes: per-sample padding from `GenotypeBuffers::close_sample` (task 3), catalog width (task 8).

- [ ] **Step 1: Rewrite the mixed-width test to assert padding**

Replace `fixed_probability_layout_rejects_a_mixed_width_file` in `tests/provider_test.rs` with:

```rust
#[tokio::test]
async fn fixed_probability_layout_pads_a_narrower_variant_with_nan() {
    // The fixture mixes widths on purpose: rs1 is unphased biallelic (three
    // states), rs3 is triallelic (six for a diploid sample, three for a
    // haploid one). The schema is six wide and every narrower sample pads.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("f", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM f WHERE rsid = 'rs1'")
        .await
        .unwrap()
        .collect()
        .await
        .expect("a mixed-width file is padded, not rejected");

    let samples = probability_values_any(&batches[0], 0, 0);
    let called = samples[0].as_ref().expect("sample 0 is called");
    assert_eq!(called.len(), 6, "every sample is the schema width");
    assert_eq!(&called[..3], &[1.0, 0.0, 0.0]);
    assert!(
        called[3..].iter().all(|value| value.is_nan()),
        "padding is NaN: {called:?}"
    );
    assert!(
        samples[2].is_none(),
        "the third sample is missing and stays null"
    );
}

#[tokio::test]
async fn fixed_probability_layout_reports_a_sample_wider_than_the_schema() {
    // The width is derived from variant 0's ploidy, so a later variant whose
    // samples are triploid stores four states where the schema has three.
    // Padding cannot represent that, and truncating would emit a distribution
    // that is not the file's.
    let dir = TempDir::new().unwrap();
    let bgen = dir.path().join("wider.bgen");
    let variants = vec![
        Variant {
            id: "v1",
            rsid: "rs1",
            chrom: "1",
            position: 10,
            alleles: vec!["A", "C"],
            phased: false,
            bits: 8,
            samples: vec![
                sample(2, false, &[255, 0]),
                sample(2, false, &[0, 255]),
                sample(2, false, &[0, 0]),
            ],
        },
        Variant {
            id: "v2",
            rsid: "rs2",
            chrom: "1",
            position: 20,
            alleles: vec!["A", "C"],
            phased: false,
            bits: 8,
            samples: vec![
                sample(3, false, &[255, 0, 0]),
                sample(3, false, &[0, 255, 0]),
                sample(3, false, &[0, 0, 255]),
            ],
        },
    ];
    let (bytes, _rows) = encode_layout2(Codec::Zlib, true, &variants);
    fs::write(&bgen, bytes).unwrap();

    let provider = BgenTableProvider::try_new(
        path(&bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let error = context
        .sql("SELECT genotypes FROM b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("fixed probability layout") && error.contains("nested layout"),
        "{error}"
    );
}

#[tokio::test]
async fn fixed_probability_layout_pads_a_variable_ploidy_variant() {
    // rs3 declares ploidy 1..=2, so it has no single width and the old
    // per-variant check rejected it outright. Per-sample padding represents it:
    // the haploid samples pad to the schema width alongside the diploid one.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("f", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM f WHERE rsid = 'rs3'")
        .await
        .unwrap()
        .collect()
        .await
        .expect("a variable-ploidy variant pads per sample");
    let samples = probability_values_any(&batches[0], 0, 0);
    let haploid = samples[0].as_ref().expect("sample 0 is called");
    assert_eq!(haploid.len(), 6);
    assert!(
        haploid[3..].iter().all(|value| value.is_nan()),
        "a haploid sample pads to the schema width: {haploid:?}"
    );
}
```

- [ ] **Step 2: Run them to see which fail**

Run: `cargo test -p datafusion-bio-format-bgen fixed_probability_layout`
Expected: the padding tests FAIL with the "expects N states per sample" error from `build_genotypes`; the wider-sample test may already pass for the wrong reason. Read the failure text before proceeding.

- [ ] **Step 3: Delete the per-variant width check**

In `physical_exec.rs`, delete the whole `if let Some(width) = width { ... }` block that compares `decoded.state_width` to the schema width (original lines 428-443). `GenotypeBuffers` already enforces the bound per sample, with the message these tests assert on. The `else` branch that accumulated nested offsets is already gone as of task 4.

- [ ] **Step 4: Update the contract documentation**

In `table_provider.rs`, replace the `Fixed` variant's doc comment:

```rust
    /// One fixed-width list per sample.
    ///
    /// Drops the per-sample list offsets, which are a quarter of the emitted
    /// bytes for a diploid biallelic cohort. The width covers the widest sample
    /// the catalog allows, and a narrower sample is padded with NaN — including
    /// a missing sample, whose slots are never read but must exist because
    /// Arrow sizes the values buffer from the entry count. A sample storing
    /// more states than the width is rejected; use [`Self::Nested`] for such a
    /// file.
    Fixed,
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p datafusion-bio-format-bgen`
Expected: PASS, all of it.

- [ ] **Step 6: Verify the padded fixed layout still equals the nested one**

Run: `cargo test -p datafusion-bio-format-bgen fixed_probability_layout_matches_the_nested_layout`
Expected: PASS. This test compares the two layouts value by value on rs1, which is now padded — if it passes, padding did not disturb the real states.

- [ ] **Step 7: Lint and commit**

```bash
cargo clippy -p datafusion-bio-format-bgen --all-targets -- -D warnings
git add datafusion/bio-format-bgen
git commit -m "feat(bgen): NaN-pad the fixed probability layout instead of rejecting mixed widths"
```

---

### Task 10: Verify against the oracle

Correctness gate. The provider's whole claim is being bit-identical to the `bgen` package where snputils is not.

**Files:**
- Modify: `~/CLionProjects/bioformats-benchmark/benchmarks/bgen_matrix.py:58-61` (stale comment)
- Modify: `~/CLionProjects/polars-bio/Cargo.toml` (repin the provider rev)

- [ ] **Step 1: Push and repin**

```bash
cd ~/CLionProjects/datafusion-bio-formats-bgen-perf
git push -u origin agent/bgen-probability-perf
git rev-parse HEAD    # the SHA to pin
```

In `~/CLionProjects/polars-bio/Cargo.toml`, repin **every** `datafusion-bio-format-*` dependency to that SHA — they share a workspace and a partial repin compiles two copies of the core crate.

```bash
cd ~/CLionProjects/polars-bio
unset CONDA_PREFIX; source .venv/bin/activate
RUSTFLAGS="-C target-cpu=native" maturin develop --release
```

- [ ] **Step 2: Fix the stale comment**

In `~/CLionProjects/bioformats-benchmark/benchmarks/bgen_matrix.py`, replace the comment at lines 58-61:

```python
    # The fixed layout drops the per-sample offsets and NaN-pads a narrower
    # sample to the file's widest, so it applies to mixed-width files too. It is
    # still chosen explicitly rather than attempted and retried: a failed
    # attempt costs a whole scan.
```

- [ ] **Step 3: Verify every fixture, both layouts, zero tolerance**

```bash
cd ~/CLionProjects/bioformats-benchmark
PY=~/CLionProjects/polars-bio/.venv/bin/python
for f in chr22.first-25000.unphased chr22.first-25000; do
  for layout in fixed nested; do
    BGEN_PROBABILITY_LAYOUT=$layout BGEN_READER=bgen BGEN_MODE=probabilities \
    BGEN_VERIFY_LEFT=polars-bio BGEN_VERIFY_RIGHT=bgen \
    BGEN_PATH=~/research/data/BGEN/$f.bgen \
    BGEN_EXPECTED_ROWS=25000 BGEN_EXPECTED_SAMPLES=2548 \
    THREAD_NUM=8 POLARS_MAX_THREADS=8 TQDM_DISABLE=1 \
    $PY -m benchmarks.bgen_verify
  done
done
```

Expected: `"value_differences": 0` on all four runs. `chr22.first-25000` with `fixed` is the new capability — it errored before task 9.

Check `bitwise_differences` too. `value_differences` excludes cells where both sides are NaN, `bitwise_differences` does not; a nonzero bitwise count with a zero value count means the NaN payloads differ, which is worth understanding before it is dismissed.

- [ ] **Step 4: Verify across partition counts**

Repeat step 3 for `THREAD_NUM` and `POLARS_MAX_THREADS` of 1, 2, and 4 on `chr22.first-25000.unphased.bgen` with `fixed`. Expected: `value_differences` 0 every time. Row order is not stable above one partition, which is why `bgen_verify` sorts by position — do not remove that sort.

- [ ] **Step 5: Verify dosage did not move**

```bash
BGEN_READER=bgen BGEN_MODE=dosage \
BGEN_VERIFY_LEFT=polars-bio BGEN_VERIFY_RIGHT=bgen \
BGEN_PATH=~/research/data/BGEN/chr22.first-25000.bgen \
BGEN_EXPECTED_ROWS=25000 BGEN_EXPECTED_SAMPLES=2548 \
THREAD_NUM=8 POLARS_MAX_THREADS=8 TQDM_DISABLE=1 \
$PY -m benchmarks.bgen_verify
```

Expected: `"value_differences": 0`.

- [ ] **Step 6: Commit the benchmark repo**

```bash
cd ~/CLionProjects/bioformats-benchmark
git add benchmarks/bgen_matrix.py
git commit -m "docs(bgen): the fixed layout now pads mixed widths"
```

---

### Task 11: End-to-end benchmark against snputils

**Files:**
- Modify: `~/CLionProjects/bioformats-benchmark/GENOTYPE_READER_BENCHMARK.md`

- [ ] **Step 1: Run the suite, fresh process, three runs**

```bash
cd ~/CLionProjects/bioformats-benchmark
PY=~/CLionProjects/polars-bio/.venv/bin/python
for f in chr22.first-25000.unphased chr22.first-25000; do
  $PY run_bgen_benchmarks.py --runs 3 \
    --workloads dosage probabilities \
    --polars-bio-partitions 1 2 4 8 \
    --bgen ~/research/data/BGEN/$f.bgen \
    --output results/bgen_reader_${f}.json
done
```

`results/` is gitignored by design; published numbers live in the markdown.

- [ ] **Step 2: Compare against the goal**

The targets from the spec, 25,000-variant slice at 8 partitions, fresh process: unphased was 0.393 s against snputils' 0.361 s; phased was 0.660 s against 0.386 s. Write the new numbers into `GENOTYPE_READER_BENCHMARK.md` next to snputils'.

- [ ] **Step 3: Record the outcome honestly**

If either fixture is still behind, say so in the markdown in the same sentence as the win. The benchmark already reports polars-bio losing at one partition; that credibility is worth more than a closed gap.

- [ ] **Step 4: Decide on task 12**

Task 12 is conditional. Run it only if a fixture is still behind snputils after this measurement. If both are ahead, skip to task 13 and note in `GENOTYPE_READER_BENCHMARK.md` that the inner loop was left alone deliberately.

- [ ] **Step 5: Commit**

```bash
git add GENOTYPE_READER_BENCHMARK.md
git commit -m "bench(bgen): record probability results after the batch-buffer rework"
```

---

### Task 12 (conditional): The inner loop

Run only if task 11 step 4 says so. `byte_probabilities_into` was 23% of the profile before this plan and nothing so far has touched it.

**Files:**
- Modify: `datafusion/bio-format-bgen/src/decode.rs:893-926` (`byte_probabilities_into`)
- Modify: `datafusion/bio-format-bgen/src/decode.rs:635-694` (the fast path's sample loop)

- [ ] **Step 1: Confirm the target with the task 6 profile**

Re-read the profile recorded in task 6 step 3. Implement whichever of these the profile actually supports, in this order, measuring after each — a change that does not move the benchmark gets reverted, not kept:

1. Write through reserved capacity instead of `push` per state: `out.resize(out.len() + n, 0.0)` then indexed stores into the tail slice, so the inner loop carries no length update.
2. When `selected_samples` is the whole cohort in order, walk `probability_bytes.chunks_exact(stride)` instead of indexing through `selected_samples`. Detect it once per variant with `selected_samples.len() == sample_count as usize && selected_samples.first() == Some(&0) && selected_samples.last() == Some(&(sample_count as usize - 1))` plus a `windows(2).all(|pair| pair[1] == pair[0] + 1)` check hoisted to the fileset, not recomputed per variant.
3. Fuse the per-sample ploidy validation (the `uniform_stride_bits.is_some()` loop over `ploidy_bytes`) into the decode loop so the ploidy bytes are read once.

- [ ] **Step 2: Keep the exactness property**

`EIGHT_BIT_PROBABILITY` exists so the fast path produces exactly `numerator as f32 / 255.0`, identical to the general path rather than merely close. Any rewrite keeps the table lookup. Do not introduce a reciprocal multiply.

- [ ] **Step 3: Test after each change**

Run: `cargo test -p datafusion-bio-format-bgen`
Expected: PASS. Then re-run task 10 steps 3-5 before committing: this is the code path that produces every probability, and a unit test suite of three samples will not catch a rounding change that 191 million cells will.

- [ ] **Step 4: Commit each accepted change separately**

```bash
git add datafusion/bio-format-bgen/src/decode.rs
git commit -m "perf(bgen): <the specific change>, <the measured delta>"
```

---

### Task 13: Open the follow-up PR

**Files:**
- Modify: `docs/superpowers/plans/2026-08-15-bgen-baseline.md` (final numbers)

- [ ] **Step 1: Full verification, one more time**

```bash
cd ~/CLionProjects/datafusion-bio-formats-bgen-perf
cargo test -p datafusion-bio-format-bgen --all-targets
cargo test -p datafusion-bio-format-core
cargo clippy -p datafusion-bio-format-bgen --all-targets -- -D warnings
cargo clippy -p datafusion-bio-format-core --all-targets -- -D warnings
cargo fmt --all -- --check
```

Expected: all green. Paste the actual output into the PR body rather than asserting it.

- [ ] **Step 2: Confirm every edit is really in the tree**

```bash
git diff agent/add-bgen-provider --stat
```

Read the diff, not just the stat. On #220 a `checked_add` fix was reported as landed when a later edit to the same block had reverted it and the build gave no signal.

- [ ] **Step 3: Open the PR**

```bash
gh pr create --base agent/add-bgen-provider --head agent/bgen-probability-perf \
  --title "Speed up the BGEN probability path" --body "$(cat <<'EOF'
Stacked on #220. Decodes probabilities and dosages straight into batch-level
Arrow buffers instead of staging each variant, and lets the fixed probability
layout NaN-pad a mixed-width file so the phased fixture can use it at all.

## Numbers

<before/after table from docs/superpowers/plans/2026-08-15-bgen-baseline.md>

## Correctness

Element-wise against the `bgen` package, zero tolerance, both fixtures, both
layouts, 1/2/4/8 partitions: <value_differences per run>.

## Behavior change

The fixed layout pads a narrower sample with NaN rather than rejecting the
file, and a missing sample's reserved slots are NaN rather than 0.0. Validity
remains authoritative. Everything else is byte-identical.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01DJHDeWngFTP4GnRWNP2RTq
EOF
)"
```

- [ ] **Step 4: Request both bots**

Comment `@claude review` and `@codex review` on the new PR. Both are needed; #220 took nine rounds and rounds 3-5 were all second-order breakage from performance work exactly like this.

---

## Notes for the implementer

**Why the flush inverted.** Task 2 exists because of task 4. Once the decoder writes into the batch buffers, the row's size is not known until it is already in them, so `should_flush_before` cannot be asked. The consequence is that a batch may exceed `batch_soft_byte_limit` by one variant. That is deliberate and matches the slack the first row of every batch already had.

**Why padding is per sample.** It could have been per variant, matching the old `state_width` check. Per sample falls out of `close_sample` having to know the width anyway, and it buys the variable-ploidy case for free — a variant that declares ploidy 1..=2 has no single width and was rejected outright, but each of its samples can pad to the schema width.

**What must not change.** Everything except task 9. If a test needs editing in tasks 4, 5, 7, or 8, that is a signal the refactor changed behavior, not a signal the test was wrong.
