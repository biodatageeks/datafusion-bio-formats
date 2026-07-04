## ADDED Requirements

### Requirement: Single OS Thread Per Scan Partition

Format scans SHALL decode each DataFusion physical partition on the Tokio worker that consumes it, and SHALL NOT spawn a dedicated off-runtime `std::thread` reader per partition. Decompression and record parsing MUST run inline in the partition's stream (`poll_next`), not on a separate thread bridged by a channel.

#### Scenario: No off-runtime reader thread on the indexed path
- **WHEN** an indexed or block/byte-range partitioned file is scanned
- **THEN** the partition's `SendableRecordBatchStream` produces batches by decoding synchronously on the consuming worker
- **AND** no `std::thread` is spawned and no `mpsc` channel bridges a reader thread to the stream.

#### Scenario: Single-partition fallback
- **WHEN** a file is read through a single-partition fallback path (non-seekable compression, remote store, or `target_partitions = 1`)
- **THEN** the scan uses at most one OS thread for that partition's read and decode.

### Requirement: Shared Synchronous Batch Stream Helper

`bio-format-core` SHALL provide `sync_batch_stream(schema, next_batch)` that adapts a synchronous batch-producing closure into a `SendableRecordBatchStream`. Each poll SHALL invoke `next_batch` exactly once; `Some(Ok(batch))` yields a batch, `Some(Err(_))` surfaces an error, and `None` ends the stream. Every affected format crate SHALL construct its partition streams through this helper.

#### Scenario: Yields batches then terminates
- **WHEN** `next_batch` returns two batches followed by `None`
- **THEN** the resulting stream yields exactly those two batches and then completes.

#### Scenario: Error propagation
- **WHEN** `next_batch` returns `Some(Err(e))`
- **THEN** the stream yields that error to the consumer.

### Requirement: Target-Partition Core Contract

A scan SHALL use one OS thread per physical partition, so that setting `target_partitions = N` uses N cores for decode+compute. Decompression parallelism SHALL be bounded by the caller's Tokio runtime worker-thread count; when `target_partitions` exceeds the worker count, effective parallelism is capped at the worker count.

#### Scenario: N cores for N target partitions
- **WHEN** a BGZF-indexed file is scanned with `target_partitions = N` on a runtime with at least N worker threads
- **THEN** the scan uses approximately N busy OS threads (not approximately 2·N).

#### Scenario: Parallelism bounded by runtime workers
- **WHEN** `target_partitions` is greater than the runtime's worker-thread count
- **THEN** concurrent partition decode is limited to the worker-thread count
- **AND** results remain complete and correct.

### Requirement: Result Parity Across Partition Counts

The threading change SHALL NOT alter query results. Row set, column values, empty-projection `COUNT(*)`, and `LIMIT` behavior MUST be identical to the previous reader-thread model and independent of `target_partitions`.

#### Scenario: Multi-partition equals single-partition
- **WHEN** the same file is scanned with `target_partitions = 1` and with `target_partitions = 4`
- **THEN** both produce the same total row count and the same set of row values.

#### Scenario: Empty projection COUNT
- **WHEN** a `COUNT(*)`-style scan with an empty projection runs over multiple partitions
- **THEN** the summed batch row counts equal the file's record count.

#### Scenario: Limit honored
- **WHEN** a scan specifies a row `LIMIT`
- **THEN** the total emitted rows do not exceed the limit.

### Requirement: Uniform Contract Across Format Crates

The single-thread-per-partition contract SHALL apply uniformly to every format crate that reads records: FASTQ, VCF, BAM, CRAM, GFF, GTF, pairs, and FASTA. BED, which already executes fully asynchronously without a reader thread, SHALL remain unchanged and already satisfies the contract.

#### Scenario: Indexed readers use the shared helper
- **WHEN** any of FASTQ, VCF, BAM, CRAM, GFF, GTF, pairs, or FASTA executes a partition read
- **THEN** it produces its stream via `sync_batch_stream` with no per-partition `std::thread` reader.

#### Scenario: BED already compliant
- **WHEN** a BED table is scanned
- **THEN** it decodes on the consuming runtime worker with no dedicated reader thread and requires no code change.
