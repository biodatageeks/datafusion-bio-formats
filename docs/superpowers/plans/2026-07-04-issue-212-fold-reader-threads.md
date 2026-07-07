# Issue #212 — Fold Reader Threads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make each scan use exactly `target_partitions` OS threads (not ~2×) by folding decompress+parse into each partition's own stream, removing the per-partition off-runtime `std::thread` reader + `mpsc` channel across all format crates.

**Architecture:** Introduce one shared helper in `bio-format-core` — `sync_batch_stream(schema, next_batch)` — that turns a synchronous, blocking "produce one RecordBatch" closure into a `SendableRecordBatchStream`. Each partition's decompress+parse runs on the consuming tokio worker inside `poll_next`, so no reader thread escapes the runtime. Every crate keeps its own batch-producing closure (different noodles readers) but shares the adapter. Roll out one crate per phase, committing and stopping after each.

**Tech Stack:** Rust, DataFusion 52/53, noodles 0.93, `futures::stream::unfold`, Arrow.

## Global Constraints

- Rust toolchain 1.88.0 (`rust-toolchain.toml`); code must pass `cargo fmt --all -- --check` and `cargo clippy`.
- The contract change is intentional and must be documented: **`target_partitions` == number of OS threads a scan uses.** To saturate N cores, set `target_partitions = N` (previously N gave ~2N). Callers whose tokio runtime has fewer worker threads than `target_partitions` cap decompression parallelism at the worker count (this is the desired bounded behavior — note it in docs).
- No new non-dev dependencies. Remove the throwaway benchmark scaffolding (`FASTQ_EXEC_MODE`, `FASTQ_READER_POOL`, `libc` dev-dep, `examples/thread_model_bench.rs`) as part of Phase 1.
- Behavior must be identical to today for row counts, values, projection, `COUNT(*)` empty-projection, and `LIMIT`. Only the threading model and per-`target_partitions` wall time change.
- Each phase ends with `cargo fmt`, `cargo clippy`, `cargo test -p <crate>` green, one commit, then STOP for review.

---

## Design rationale (from benchmarking, issue #212)

Measured on `partial_reads.fastq.bgz` (523 MB BGZF, 26.5M reads), release + `target-cpu=native`, runtime workers = `target_partitions`, balanced reader∥consumer workload:

| target_partitions | baseline eff-cores | fold eff-cores |
|---:|---:|---:|
| 1 | 2.00 | 1.00 |
| 2 | 3.99 | 2.00 |
| 4 | 7.96 | 3.99 |
| 8 | 13.83 | 7.93 |

At **equal core budget**, fold is ~18–19% *faster* than baseline (fold t=8 @ 8 cores = 2.37s vs baseline t=4 @ 8 cores = 2.91s) and uses less total CPU (~18s vs ~22s) — it avoids the cross-thread channel handoff. Fold also fixes the root complaint: decompression no longer escapes the caller's tokio runtime. Chosen over the bounded-pool option (which preserves the 2× and only caps it) because fold is simpler ("N == N"), more CPU-efficient, and directly bounds the escape.

---

## File Structure

- **Create** `datafusion/bio-format-core/src/sync_stream.rs` — `sync_batch_stream` helper (shared by all crates).
- **Modify** `datafusion/bio-format-core/src/lib.rs` — export `sync_stream`.
- **Modify** each crate's `physical_exec.rs` — replace `thread::spawn` + `mpsc` reader with a `sync_batch_stream` + batch-producer closure.
- **Modify** docs: `CLAUDE.md`, per-crate `physical_exec.rs` module doc, README thread-usage note.

---

## Phase 1 — Core helper + FASTQ (fold), cleanup scaffolding

**Files:**
- Create: `datafusion/bio-format-core/src/sync_stream.rs`
- Modify: `datafusion/bio-format-core/src/lib.rs`
- Modify: `datafusion/bio-format-fastq/src/physical_exec.rs`
- Modify: `datafusion/bio-format-fastq/Cargo.toml` (remove `libc` dev-dep + `thread_model_bench` example entry)
- Delete: `datafusion/bio-format-fastq/examples/thread_model_bench.rs`
- Test: `datafusion/bio-format-fastq/src/physical_exec.rs` (`#[cfg(test)]` mod) or existing test file

**Interfaces:**
- Produces (core): `pub fn sync_batch_stream<F>(schema: SchemaRef, next_batch: F) -> SendableRecordBatchStream where F: FnMut() -> Option<Result<RecordBatch, DataFusionError>> + Send + 'static`
- Consumes (fastq): the above, plus existing `build_batch_from_builders`.

- [ ] **Step 1: Write failing test for the core helper**

Add `datafusion/bio-format-core/src/sync_stream.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn yields_batches_then_stops() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let s = schema.clone();
        let mut remaining = vec![3, 2, 1];
        let stream = sync_batch_stream(schema, move || {
            remaining.pop().map(|v| {
                Ok(RecordBatch::try_new(s.clone(), vec![Arc::new(Int32Array::from(vec![v]))]).unwrap())
            })
        });
        let batches: Vec<_> = stream.collect().await;
        assert_eq!(batches.len(), 3);
        let total: i32 = batches.iter().map(|b| b.as_ref().unwrap().num_rows() as i32).sum();
        assert_eq!(total, 3);
    }
}
```

- [ ] **Step 2: Run test, verify it fails to compile (`sync_batch_stream` undefined)**

Run: `cargo test -p datafusion-bio-format-core sync_stream`
Expected: FAIL — `cannot find function sync_batch_stream`.

- [ ] **Step 3: Implement the helper**

Top of `datafusion/bio-format-core/src/sync_stream.rs`:

```rust
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

/// Build a [`SendableRecordBatchStream`] that pulls batches **synchronously** on
/// the consuming task's thread — no dedicated reader thread, no channel.
///
/// Each `poll_next` invokes `next_batch` exactly once. Because the closure does
/// its (blocking) decompression/parse work inline, that work runs on the same
/// tokio worker that consumes the partition. A scan therefore uses exactly
/// `target_partitions` OS threads instead of ~2× (issue #212).
///
/// `next_batch` returns `Some(Ok(batch))` for each batch, `Some(Err(_))` to
/// surface an error (after which it should return `None`), and `None` when the
/// partition is exhausted.
pub fn sync_batch_stream<F>(schema: SchemaRef, next_batch: F) -> SendableRecordBatchStream
where
    F: FnMut() -> Option<Result<RecordBatch, DataFusionError>> + Send + 'static,
{
    let stream = futures::stream::unfold(next_batch, |mut next_batch| async move {
        let item = next_batch()?;
        Some((item, next_batch))
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}
```

Add `pub mod sync_stream;` to `datafusion/bio-format-core/src/lib.rs` (alongside the other `pub mod` lines).

- [ ] **Step 4: Run test, verify it passes**

Run: `cargo test -p datafusion-bio-format-core sync_stream`
Expected: PASS.

- [ ] **Step 5: Commit the core helper**

```bash
git add datafusion/bio-format-core/src/sync_stream.rs datafusion/bio-format-core/src/lib.rs
git commit -m "feat(core): add sync_batch_stream helper for single-thread partition reads (#212)"
```

- [ ] **Step 6: Add a FASTQ multi-partition correctness test (guards the refactor)**

In `datafusion/bio-format-fastq` tests, add a test that scans a BGZF+GZI fixture with `target_partitions = 4` and asserts the total row count equals a single-partition read. Use an existing fixture if present (search `datafusion/bio-format-fastq` for `*.bgz`/`*.gzi`); otherwise generate a small BGZF fixture in the test via `noodles_bgzf`. Assert: (a) total rows across partitions == expected; (b) `SELECT sequence FROM t` values concatenated are identical between 1-partition and 4-partition scans.

Run: `cargo test -p datafusion-bio-format-fastq` — Expected: PASS on current (baseline) code, so it locks in behavior before the refactor.

- [ ] **Step 7: Replace the FASTQ reader model with fold**

In `datafusion/bio-format-fastq/src/physical_exec.rs`:

1. Delete the experimental scaffolding added during benchmarking: the `ExecMode` enum, `exec_mode()`, `reader_gate()`, `GatePermit`, `gate_acquire()`, and the standalone `execute_bgzf_partition_fold`. Delete `PARTITION_CHANNEL_BUFFER`, `read_and_send_batches`, and the `use futures::channel::mpsc;`, `use std::sync::{Condvar, Mutex, OnceLock};`, and `use std::thread;` imports.
2. Add a shared, generic batch-producer that both strategies use:

```rust
/// Build a closure that decodes one RecordBatch per call from a positioned
/// FASTQ reader. All work is synchronous; the closure is driven by
/// `sync_batch_stream`, so it runs on the consuming tokio worker (issue #212).
fn batch_producer<R: BufRead + Send + 'static>(
    mut fastq_reader: fastq::io::Reader<R>,
    mut is_past_end: impl FnMut(&mut fastq::io::Reader<R>) -> bool + Send + 'static,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    batch_size: usize,
) -> impl FnMut() -> Option<Result<RecordBatch, DataFusionError>> + Send + 'static {
    let mut record = fastq::Record::default();
    let mut total: usize = 0;
    let mut done = false;
    move || {
        if done {
            return None;
        }
        let proj = projection.as_ref();
        let mut names = proj.is_none_or(|p| p.contains(&0)).then(StringBuilder::new);
        let mut descriptions = proj.is_none_or(|p| p.contains(&1)).then(StringBuilder::new);
        let mut sequences = proj.is_none_or(|p| p.contains(&2)).then(StringBuilder::new);
        let mut quality_scores = proj.is_none_or(|p| p.contains(&3)).then(StringBuilder::new);

        let mut count = 0usize;
        while count < batch_size {
            if limit.is_some_and(|l| total >= l) || is_past_end(&mut fastq_reader) {
                done = true;
                break;
            }
            match fastq_reader.read_record(&mut record) {
                Ok(0) => {
                    done = true;
                    break;
                }
                Ok(_) => {
                    if let Some(b) = &mut names {
                        b.append_value(std::str::from_utf8(record.name()).unwrap());
                    }
                    if let Some(b) = &mut descriptions {
                        if record.description().is_empty() {
                            b.append_null();
                        } else {
                            b.append_value(std::str::from_utf8(record.description()).unwrap());
                        }
                    }
                    if let Some(b) = &mut sequences {
                        b.append_value(std::str::from_utf8(record.sequence()).unwrap());
                    }
                    if let Some(b) = &mut quality_scores {
                        b.append_value(std::str::from_utf8(record.quality_scores()).unwrap());
                    }
                    count += 1;
                    total += 1;
                }
                Err(e) => {
                    done = true;
                    return Some(Err(DataFusionError::External(Box::new(e))));
                }
            }
        }
        if count == 0 {
            return None;
        }
        Some(build_batch_from_builders(
            &schema, &projection,
            &mut names, &mut descriptions, &mut sequences, &mut quality_scores,
            count,
        ))
    }
}
```

3. Rewrite `execute_bgzf_partition` to open+seek+synchronize the reader (as today), then return `sync_batch_stream(schema, batch_producer(...))` with `is_past_end = |r| r.get_ref().virtual_position().compressed() >= end_comp`. Drop the `partition` and `gated` params. No `thread::spawn`, no channel.
4. Rewrite `execute_byte_range_partition` likewise with `is_past_end = |r| r.get_mut().stream_position().map(|p| p >= end_byte).unwrap_or(true)`.
5. Update `execute()`'s BGZF/ByteRange match arms to call the simplified signatures (remove the `exec_mode()` dispatch added during benchmarking).
6. `use datafusion_bio_format_core::sync_stream::sync_batch_stream;`.

Note: `build_batch_from_builders` already handles the empty-projection (`COUNT(*)`) case by emitting a zero-column batch with `row_count`. The unified loop above counts rows for empty projections too, so the fast path in the old `read_and_send_batches` is no longer needed.

- [ ] **Step 8: Remove benchmark scaffolding from Cargo.toml + delete example**

In `datafusion/bio-format-fastq/Cargo.toml` remove the `libc = "0.2"` dev-dep and the `[[example]] name = "thread_model_bench"` block. Then:

```bash
rm datafusion/bio-format-fastq/examples/thread_model_bench.rs
```

- [ ] **Step 9: fmt, clippy, test**

Run:
```bash
cargo fmt --all
cargo clippy -p datafusion-bio-format-fastq --all-targets 2>&1 | grep -E "warning|error" || echo clean
cargo test -p datafusion-bio-format-fastq
```
Expected: fmt clean, no new clippy warnings, all tests PASS (including Step 6's multi-partition test — proving fold matches baseline output).

- [ ] **Step 10: Update FASTQ + top-level docs with the thread-usage contract**

In `datafusion/bio-format-fastq/src/physical_exec.rs` module/`FastqExec` doc, and in `CLAUDE.md` (new "Thread usage" subsection), state: a scan uses one OS thread per partition (`target_partitions`); decompression runs on the consuming runtime worker; to use N cores set `target_partitions = N`; parallelism is capped by the caller's tokio worker-thread count.

- [ ] **Step 11: Commit Phase 1**

```bash
git add -A
git commit -m "feat(fastq): fold decompression into partition stream; drop per-partition reader thread (#212)"
```

**STOP — await review.**

---

## Phase 2 — VCF

**Files:** Modify `datafusion/bio-format-vcf/src/physical_exec.rs` (indexed reader thread at `:2659` in `get_indexed_vcf_stream`; single-partition sync at `:819` in `get_local_vcf_sync`; `STREAM_CHANNEL_BUFFERED_BATCHES` at `:73`).

Apply Phase 1's transformation: replace each `thread::spawn` + `futures::channel::mpsc` reader with a synchronous `batch_producer`-style closure driven by `bio-format-core`'s `sync_batch_stream`. The VCF closure owns the noodles indexed reader and iterates its assigned genomic regions, yielding one Arrow batch per call (persist region-iterator state across calls). Remove now-dead channel/thread imports and the buffer const if unused. Keep all schema/projection/`INFO`/`FORMAT` handling byte-identical.

**Acceptance:** `cargo test -p datafusion-bio-format-vcf` green; a 4-partition indexed scan returns the same rows as a 1-partition scan; `cargo clippy`/`fmt` clean. Commit `feat(vcf): fold reader into partition stream (#212)`. **STOP.**

> Detailed TDD steps for this phase to be expanded at execution start, after reading `get_indexed_vcf_stream` in full (region-iteration state machine differs from FASTQ's linear read).

---

## Phase 3 — BAM

**Files:** `datafusion/bio-format-bam/src/physical_exec.rs` (indexed thread `:888` in `get_indexed_stream`; sync `:383` in `get_local_bam_sync`; `mpsc::channel(2)` at `:381`/`:886`).

Same transformation as Phase 2. BAM's closure owns the noodles BAM indexed reader + region assignment and yields one batch per call (tags/CIGAR/flags handling unchanged). **Acceptance/commit/STOP** as Phase 2 (`feat(bam): ...`).

> Detailed steps expanded at execution start.

---

## Phase 4 — CRAM

**Files:** `datafusion/bio-format-cram/src/physical_exec.rs` (indexed thread `:867` in `get_indexed_stream`; `mpsc::channel(2)` `:865`). Full-scan `get_local_cram` is already async — leave it.

Same transformation. CRAM decoding stays on the consuming worker. Watch the noodles-fork `no_ref` behavior noted in project memory — do not change decode logic, only the threading wrapper. **Acceptance/commit/STOP** (`feat(cram): ...`).

> Detailed steps expanded at execution start.

---

## Phase 5 — GFF

**Files:** `datafusion/bio-format-gff/src/physical_exec.rs` (indexed thread `:1284` in `get_indexed_gff_stream`; `mpsc::channel(2)` `:1282`). **Acceptance/commit/STOP** (`feat(gff): ...`).

> Detailed steps expanded at execution start.

---

## Phase 6 — GTF

**Files:** `datafusion/bio-format-gtf/src/physical_exec.rs` (indexed thread `:729` in `get_indexed_gtf_stream`; `mpsc::channel(2)` `:727`). **Acceptance/commit/STOP** (`feat(gtf): ...`).

> Detailed steps expanded at execution start.

---

## Phase 7 — Pairs

**Files:** `datafusion/bio-format-pairs/src/physical_exec.rs` (indexed thread `:440` in `get_indexed_pairs_stream`; `mpsc::channel(2)` `:438`). **Acceptance/commit/STOP** (`feat(pairs): ...`).

> Detailed steps expanded at execution start.

---

## Phase 8 — FASTA single-partition + BED verify + repo-wide contract doc

**Files:** `datafusion/bio-format-fasta/src/physical_exec.rs` (sync reader thread `:291` in `get_local_fasta_sync`, single partition); `datafusion/bio-format-bed` (already fully async — verify, no code change); `CLAUDE.md`, workspace `README`.

Convert FASTA's single-partition sync reader to `sync_batch_stream` for consistency (removes its 1 extra reader thread). Confirm BED needs no change. Add a repo-wide "Thread usage" doc section stating the uniform contract now holds across FASTQ/VCF/BAM/CRAM/GFF/GTF/pairs/FASTA. **Acceptance:** `cargo test` workspace-wide green; `cargo clippy`/`fmt` clean. Commit `docs+feat(fasta): uniform one-thread-per-partition reader contract (#212)`. **STOP.**

> Detailed steps expanded at execution start.

---

## Self-Review

- **Spec coverage:** every per-partition `thread::spawn` reader site from the issue's crate map (VCF `:2659`, BAM `:888`, CRAM `:867`, GFF `:1284`, GTF `:729`, pairs `:440`, FASTQ `:548`/`:610`) plus single-partition fallbacks (VCF `:819`, BAM `:383`, FASTA `:291`) is assigned to a phase. BED (no thread) is verified in Phase 8. ✓
- **Contract documented:** Phase 1 Step 10 + Phase 8. ✓
- **Scaffolding removed:** Phase 1 Steps 7–8 remove `FASTQ_EXEC_MODE`/`FASTQ_READER_POOL`/`libc`/bench example. ✓
- **Type consistency:** `sync_batch_stream` signature is fixed in Phase 1 and reused verbatim by Phases 2–8. ✓
- **Note:** Phases 2–8 intentionally carry a just-in-time expansion marker rather than fabricated per-crate code — each crate's read loop (region-iteration state) must be read before writing its exact TDD steps. The transformation and target sites are fully specified.
