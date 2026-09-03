## Context

The columnar `PvarTable` is a handful of flat vectors: contig names, a `u32`
contig index, `u64` positions, and offset-plus-byte arenas for IDs and
alleles. That layout serializes to a flat file with a small header and can be
mapped directly, provided the accessors read from slices rather than owned
vectors.

## Goals / Non-Goals

- Goals: open a cached fileset without parsing; share the table across
  processes through the page cache; never serve a stale sidecar; never write
  unasked.
- Non-Goals: caching genotype records or the PGEN index (the record index is
  ~0.6 GB on the panel and parses in well under a second); remote sidecar
  writes.

## Decisions

- **Decision: flat little-endian layout with a validated header.** Magic,
  format version, PVAR size and mtime (or ETag), coordinate system, row
  count, and per-column byte lengths. Columns follow, 8-byte aligned. A
  checksum of the header guards truncation; a mismatch means rebuild.
- **Decision: `PvarTable` generic over storage.** `PvarColumns<S: AsRef<[u8]>>`
  with `Vec<u8>` and `Mmap` instantiations; accessors are unchanged.
- **Decision: modes, not a boolean.** `Off` never reads or writes;
  `ReadOnly` (default) maps a valid sidecar and otherwise parses in memory;
  `ReadWrite` also writes one atomically (temp file plus rename) after a
  successful parse. `cache_dir` overrides the sidecar location.
- Alternatives: Arrow IPC (offsets are `i32`, needing chunking above 2 GB
  of allele bytes, and it adds no value over a flat layout); a SQLite or KV
  store (heavier, no mapping benefit).

## Risks / Trade-offs

- mtime granularity on some filesystems is one second; include size and, when
  available, inode/ETag in the key.
- A mapped table touched sparsely pays page faults instead of parse time;
  for a full scan the whole file is read once either way.
