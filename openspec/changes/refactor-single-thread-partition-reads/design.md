## Context

Confirmed via a repo-wide audit (issue #212): each format crate's `physical_exec.rs` spawns one raw `std::thread` per DataFusion partition inside `execute()` on the indexed path, plus a single-thread fallback on the full-scan path. The thread opens the file, seeks to its region/byte range, decodes BGZF/format records, builds Arrow batches, and pushes them through a bounded `futures::channel::mpsc` to a `RecordBatchStreamAdapter`. Per-partition reader-thread sites: VCF `physical_exec.rs:2659`, BAM `:888`, CRAM `:867`, GFF `:1284`, GTF `:729`, pairs `:440`, FASTQ `:548`/`:610`. Single-partition fallbacks: VCF `:819`, BAM `:383`, FASTA `:291`. BED spawns nothing (fully async). The only shared infrastructure today is `bio-format-core::partition_balancer::balance_partitions`; the reader/channel loop is copy-reimplemented per crate.

Because the reader threads are `std::thread`s, they run outside the consumer's Tokio runtime and are not counted against its worker budget. A caller that sizes its runtime to `target_partitions` still gets `target_partitions` extra decompressor threads → ~2·N cores.

## Goals / Non-Goals

- Goals:
  - `target_partitions == number of OS threads a scan uses` (predictable, cappable by the caller's runtime).
  - No regression in per-core throughput; ideally an improvement.
  - One shared mechanism reused by every crate; minimal per-crate diff.
  - Byte-identical query results.
- Non-Goals:
  - Removing parallelism (parallel decompression stays — it is just bounded by the runtime).
  - Guaranteeing global row order across partitions (already unordered today).
  - Changing partition planning (`balance_partitions`, BGZF block/byte-range splitting) or any decode logic.

## Decisions

- **Decision: Fold decompress+parse into the partition's own stream (one thread per partition).** Each `poll_next` synchronously decodes one batch on the consuming Tokio worker via a shared `sync_batch_stream(schema, next_batch)` helper. No reader thread, no channel.
- **Alternatives considered:**
  - *Baseline (status quo):* reader `std::thread` + `mpsc` per partition. Rejected — the ~2·N escape is the reported defect.
  - *Bounded pool (Option 1):* keep the two-stage design but draw reader threads from a shared semaphore-bounded pool (default sized to `target_partitions`). Caps total threads but keeps the channel-handoff overhead and, at default, still uses ~2·N. Rejected as the primary design; fold is simpler and more efficient. (Prototyped and measured for comparison.)
  - *`spawn_blocking`:* move readers onto the Tokio blocking pool. Still a second pool, not inherently tied to `target_partitions`. Rejected.

### Supporting data

523 MB BGZF FASTQ (26.5M reads), release + `target-cpu=native`, runtime workers = `target_partitions`, balanced reader∥consumer workload:

| target_partitions | baseline eff-cores / wall | fold eff-cores / wall |
|---:|---:|---:|
| 1 | 2.00 / 11.01s | 1.00 / 17.84s |
| 2 | 3.99 / 5.76s | 2.00 / 9.14s |
| 4 | 7.96 / 2.91s | 3.99 / 4.66s |
| 8 | 13.83 / 1.52s | 7.93 / 2.37s |

At equal **core budget**, fold wins: fold t=8 (8 cores, 2.37s) vs baseline t=4 (8 cores, 2.91s) — ~18% faster, and lower total CPU (~18s vs ~22s) because it drops the cross-thread channel handoff. The bounded-pool prototype reproduced baseline numbers at default and capped total cores to ≈2·pool_size when throttled (e.g. `pool=2, t=8 → 4 eff-cores`), confirming it only bounds the escape rather than removing it.

## Risks / Trade-offs

- **Behavior change: wall time at a fixed `target_partitions` rises ~1.6×** (one thread instead of two). → Mitigation: documented contract change — set `target_partitions` to the desired core count (previously half). At equal cores fold is faster, so no throughput regression.
- **`target_partitions > runtime worker_threads`** caps effective decompression parallelism at the worker count (fold blocks a worker during decode). → This is the *intended* bounded behavior; documented. Callers wanting N-way scan parallelism must provide ≥ N worker threads.
- **Blocking a Tokio worker with CPU-heavy sync decode** is undesirable if the same runtime serves latency-sensitive async work. → Acceptable for the scan-dedicated runtimes these providers target; documented.

## Migration Plan

- Rollout is phased, one crate per commit, each independently testable and revertable on the feature branch (`fix/212-fold-reader-threads`): core helper + FASTQ first, then VCF, BAM, CRAM, GFF, GTF, pairs, then FASTA + docs.
- Rollback: revert the per-crate commit; crates are independent after the shared helper lands.
- Consumer migration: callers relying on the implicit 2× (e.g. benchmarks comparing "-t N" against single-reader tools) should set `target_partitions` to the intended core count.

## Open Questions

- Should `bio-format-core` also expose a small generic "region-iterating batch producer" to further DRY the noodles-indexed crates, or is per-crate closure state clearer? (Lean: keep per-crate closures; only the stream adapter is shared.)
