# BGEN probability-path performance

Date: 2026-08-15
Branch: `agent/bgen-probability-perf`, off `agent/add-bgen-provider` (PR #220, commit `2c06f23`)

## Goal

Make a BGEN probability scan faster than snputils on both fixtures. Every
probability a scan already emits keeps its exact value; the one deliberate
output change is phase 2, which fills slots that are unreadable today — padding
in a mixed-width file, and the never-read bytes of a missing sample.

Two measured gaps, 25,000-variant slice, 8 partitions, fresh process:

| Fixture | polars-bio | snputils | Behind by |
| --- | --- | --- | --- |
| unphased, uniform width | 0.393 s | 0.361 s | 9% |
| phased, mixed width | 0.660 s | 0.386 s | 71% |

Dosage is already 1.9x ahead on the full chromosome and must not regress.

## Non-goals

- Decompression. libdeflate plus its decode table is 23% of the profile and is
  the floor of this design; nothing here touches it.
- The single-partition decoder deficit (0.546x snputils on dosage at one
  partition). Real, separately scoped.
- The open BGI cache decision (a normal remote index is downloaded in full
  before the cache hit is detected). Stays open for a human call.

## Where the time goes

Profile of the current probability path, full unphased file, 8 partitions,
sampled:

| Frame | Share |
| --- | --- |
| `byte_probabilities_into` | 23% |
| libdeflate + decode table | 23% |
| `decode_variant` | 16% |
| `memmove` | 12% |
| `ProbabilityValues::finish_sample` | 9.5% |
| allocator churn | ~10% |
| `build_genotypes` | 2% |

Stage split for the same slice, warm, in process: Rust scan 0.27 s (~69%),
Arrow to Polars ~0.03 s, Polars to Arrow 0.003 s, `combine_chunks`
0.015-0.09 s, NumPy ~0 s. The scan is the target; the Python round trip is not.

Three of those frames — `memmove`, `finish_sample`, and most of the allocator
churn — are one design decision seen three ways: `decode_variant` stages each
variant in a freshly allocated `ProbabilityValues`, and `build_genotypes` then
copies every staged variant into batch-level buffers. Removing the staging
struct removes all three at once.

## Design

### Phase 0 — measurement harness, before any edit

The current criterion bench builds a synthetic 2,048 x 256 fixture. That is a
fine regression guard and useless for guiding this work, and the alternative —
re-running `maturin develop --release` for every Rust change — makes the loop
minutes long.

Add an opt-in bench to `datafusion/bio-format-bgen/benches/bgen_scan.rs`: when
`BGEN_BENCH_PATH` names a file it is scanned directly; when the variable is
unset the bench is skipped, so CI is unaffected. Parameterise over output mode,
probability layout, and partition count.

Baseline before touching anything, and keep the numbers in the PR body:
all four `~/research/data/BGEN/` fixtures, {1, 8} partitions, {fixed, nested}.

Two traps the handover already paid for, restated because this phase is where
they bite:

- Measure interleaved, with a warm-up. An apparent 0.368 s to 0.283 s win from
  raising `datafusion.execution.batch_size` was a cold-versus-warm artifact and
  collapsed when re-run interleaved. `batch_size` makes no difference.
- Compare fresh-process numbers only against fresh-process numbers.

### Phase 1 — decode into batch-level buffers

Replace the per-variant `ProbabilityValues`, `ploidies`, and `dosages` with a
`GenotypeBuffers` struct owned by the partition stream and reused across
batches. `decode_variant` takes `&mut GenotypeBuffers` and appends into it
directly; `build_batch` takes the buffers with `mem::take` and moves the `Vec`s
into Arrow arrays.

`GenotypeBuffers` holds:

- `states: Vec<f32>` — every emitted probability of the batch.
- `sample_offsets: Vec<i32>` — **only populated in the nested layout.** A
  `FixedSizeList` never reads them. This is the 9.5%.
- `sample_valid: Option<BooleanBufferBuilder>` — `None` until the first missing
  sample appears, then backfilled with `true` for the samples already emitted.
  A fully-called cohort writes no validity bytes and does no per-batch
  `extend_from_slice`.
- `variant_offsets: Vec<i32>`, `ploidy: Vec<u8>`, `ploidy_offsets: Vec<i32>`.
- `dosages: Vec<f32>` plus the same optional validity, for dosage mode.

Per-row metadata that is not a buffer (`variant_index`, `phased`, `bits`) stays
in a small `Vec<RowMeta>`, which is what `DecodedRow` degenerates into.

Consequences to get right:

- **Flush ordering.** Today the row is decoded, its size estimated, and the
  batch flushed *before* the row is appended. Decoding straight into the batch
  buffers inverts that: append, then decide. Add `should_flush_after()` to
  `GenotypeBatchSizer`. BGEN is its only consumer today, so no other provider's
  batching shifts. A batch may now exceed the soft byte limit by one variant —
  the same slack the first row of every batch already has.
- **Torn rows.** Record each buffer's length before decoding a variant and
  truncate back to those marks if decoding fails. A decode error aborts the
  scan today, so this is defensive rather than load-bearing; it costs six
  `usize` copies per variant and keeps the buffers a valid Arrow prefix.
- **Capacity reuse.** `mem::take` leaves an empty `Vec`. Re-reserve the
  previous batch's length so allocation is amortised per batch, not per
  variant. This is the allocator-churn half of the win, and it is easy to write
  the version that silently reallocates per batch instead.
- **Dosage.** Dosage moves onto the same buffers, replacing the per-sample
  `ListBuilder::append_option`. Not to chase dosage — it is 1.9x ahead — but
  because it is the same function and the 1.9x must survive the refactor.
- **Metrics.** `estimated_arrow_bytes()` currently reads the staged struct. It
  becomes a function of the buffer growth across the row (end marks minus start
  marks), which is exact rather than estimated. Rounds 6-9 of review were
  metrics accounting; keep the counters meaning what they meant.

### Phase 2 — NaN-padded fixed layout for mixed widths

Today `BgenProbabilityLayout::Fixed` requires every variant to store the same
number of states and rejects a file that mixes widths. That is why the phased
chr22 fixture — where plink2 left 461 of 25,000 variants unphased, so widths
mix 3 and 4 — cannot use the fixed layout at all, and why it is the furthest
behind.

**Contract change:** padding is decided **per sample**, not per variant. A
sample storing fewer states than the schema width is padded with NaN to that
width; a sample storing more still errors, with the existing "use the nested
layout for this file" message. Per-sample rather than per-variant falls out of
where the padding has to happen anyway, and it comes with a bonus: a
variable-ploidy variant, which today has no single width and is rejected
outright, becomes representable because each of its samples pads to the same
schema width.

This retires the per-variant `state_width` equality check in
`build_genotypes`. `fixed_probability_layout_rejects_a_mixed_width_file` is
therefore testing a contract that no longer exists and must be rewritten to
assert padding, with a new test covering the case that still errors.

**Deriving the width without a full pre-scan.** A Layout 2 block header is
inside the compressed payload, so learning each variant's exact width would
mean decompressing every block at plan time. Instead:

1. Probe variant 0 as today, and return `(phased, ploidy, width)` rather than
   just `width`. This needs a `declared_ploidy: Option<u8>` on
   `DecodedGenotypes` — the probe selects no samples, so the existing `ploidy`
   vector is empty and carries nothing.
2. Schema width = `max` over every catalog variant of
   `complete_probability_count(probed_ploidy, variant.alleles.len(), probed_phased)`.
   That function already exists in `decode.rs`, and allele counts are already in
   the catalog, so this costs no I/O.

The result is exact whenever ploidy and phasedness are uniform across the file,
which covers every fixture here including the phased one with its 461
stragglers, and it handles multiallelic width variation exactly. It is computed
over all catalog variants rather than the filtered subset, because the schema is
built in `try_new`, before any `scan`. A filter that selects only narrow
variants therefore gets a slightly over-wide schema; the alternative is a schema
that changes per query, which is worse.

The perverse case that still errors: a file whose variant 0 is unphased but
which contains phased variants. The message tells the caller to use the nested
layout, which is correct and always available.

**Missing samples pad with NaN, not 0.0.** The fixed layout exists so a
consumer can read the values buffer zero-copy; under that access pattern a
missing sample currently reads as a real 0.0. NaN matches the `bgen` oracle and
matches the new width padding. The validity buffer stays authoritative for
anyone reading Arrow properly. This changes emitted bytes for files with
missing genotypes, so existing fixed-layout tests need review, not just reruns.

**Documentation to update:** the `BgenProbabilityLayout::Fixed` doc comment
promises a mixed file "is rejected rather than silently padded". That promise is
what this phase deletes; the replacement must say the padding is NaN and that
the width comes from the catalog rather than from variant 0 alone.

### Phase 3 — inner loop, only if the numbers demand it

`byte_probabilities_into` is 23% and untouched by phases 1 and 2. Candidates,
in order of risk:

- Write through reserved spare capacity (`resize` then indexed stores) instead
  of `push` per state, dropping the length bookkeeping from the inner loop.
- Special-case a contiguous full-cohort selection so the loop walks
  `probability_bytes.chunks_exact(stride)` instead of indirecting through
  `selected_samples`, which today blocks vectorisation.
- Fuse the per-sample ploidy validation pass into that same loop; it is
  currently a separate full pass over the ploidy bytes of every variant.

**This phase is conditional.** Re-profile after phase 1 and 2 land. The
handover's estimate that the offset and copy removals take the scan from 0.27 s
toward 0.21 s is explicitly unverified; if the measured result already clears
snputils on both fixtures, phase 3 is a separate decision made on its own
merits, not a foregone conclusion. Rounds 3-5 of review on #220 were entirely
second-order breakage caused by performance work, and this is the phase most
likely to repeat that.

## Verification

Correctness first, and correctness is not negotiable here: the value of this
provider is that it is bit-identical to the `bgen` oracle where snputils is not
(126,259,603 differing cells on phased dosage).

- `cargo test -p datafusion-bio-format-bgen --all-targets`
- `cargo clippy -p datafusion-bio-format-bgen --all-targets -- -D warnings`
- `benchmarks.bgen_verify`, zero tolerance, element-wise against the `bgen`
  package: all four fixtures x {1, 2, 4, 8} partitions x {dosage,
  probabilities} x {fixed, nested}.
- **NaN comparison is already handled, in one of the two counters.**
  `bgen_verify.py` excludes `isnan(left) & isnan(right)` from
  `value_differences`, so padded slots compare equal there. `bitwise_differences`
  does not: it compares raw `uint32` bit patterns, so it stays zero only while
  every NaN this design emits carries the same payload as the oracle's
  (`f32::NAN` and NumPy's float32 `nan` are both `0x7FC00000`, so it should —
  but check the counter rather than assuming it).
- **The Python side needs almost nothing.** `bgen_matrix.py` already takes a
  zero-copy path for `FixedSizeList` and already NaN-pads mixed widths by hand
  for the nested layout; phase 2 moves that padding into Rust. The change is to
  run the phased fixture with `BGEN_PROBABILITY_LAYOUT=fixed`, which is already
  an environment variable, and to update the now-false comment at
  `benchmarks/bgen_matrix.py:58-61` claiming the fixed layout "requires every
  variant to store the same number of states".
- Row order is not stable above one partition. Sort by variant position before
  hashing or comparing across partition counts.
- Re-run `run_bgen_benchmarks.py --runs 3` fresh-process on all four fixtures,
  and publish before/after in the PR body.

Verify each edit is actually in the tree before reporting it landed. Twice on
#220 a fix was reported as done when a later edit to the same block had
silently reverted it and the build gave no signal.

## Risks

- **Phase 1 is a refactor of the code all nine review rounds concentrated on.**
  The offsets, the validity buffer, and the metrics counters each carry a fix
  from a specific round. Re-read those commits before rewriting the block they
  touched.
- **Phase 2 changes emitted values** for missing genotypes and for mixed-width
  files. It is the only part of this design that does; everything else must be
  byte-identical, and any diff elsewhere is a bug, not a trade-off.
- **The gap may not close.** libdeflate is 23% and out of scope. If phases 1-3
  land and the unphased scan is still behind, the honest outcome is a benchmark
  that says so — the same way it already reports polars-bio losing at one
  partition.

## Reproducing

Fixtures persist in `~/research/data/BGEN/`; plink2 does not and must be
re-fetched from cog-genomics before regenerating any of them. The Python
environment is polars-bio's own venv,
`~/CLionProjects/polars-bio/.venv/bin/python`, which already holds `snputils`,
`bgen`, and `pysnptools[bgen]`.

Baseline A/B is easy here: `~/CLionProjects/datafusion-bio-formats-bgen` stays
pinned to `2c06f23`, and this work happens in
`~/CLionProjects/datafusion-bio-formats-bgen-perf`.
