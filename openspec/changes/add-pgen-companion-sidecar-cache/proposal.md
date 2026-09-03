# Change: Memory-mappable parsed PVAR sidecar cache

## Why

After `refactor-pgen-companion-memory-model` the parsed variant table is
still resident per open (~4.5 GB for the 75M-variant PGS Catalog panel) and
is rebuilt from the zstd stream on every open (~5 s). A registered table, a
`describe`, and a matrix read each pay it again. PLINK 2 users expect a
fileset to open instantly; the PVAR itself has no random-access index, but
our parsed columnar form does.

## What Changes

- On first open the provider may write the columnar variant table as a
  sidecar next to the fileset (`<basename>.pvar.pbidx`), keyed by the PVAR's
  size and mtime (or ETag on object storage). Later opens memory-map the
  sidecar and validate the key; a stale or unreadable sidecar is ignored and
  rebuilt.
- The variant table's accessors work over either owned vectors or mapped
  slices, so resident memory becomes page cache shared across processes.
- Sidecar writing is opt-in through `PgenReadOptions::companion_cache`
  (`Off`, `ReadOnly`, `ReadWrite`), defaulting to `ReadOnly` so a shipped
  sidecar is used and nothing is written unasked. A cache directory option
  covers read-only locations.

## Impact

- Affected specs: `pgen` (new companion cache requirement).
- Affected code: `fileset.rs` (table serialization, mapping, validation),
  `table_provider.rs` (options), `source.rs` (sidecar location).
- New dependency: `memmap2`, not yet in the workspace, for mapping.
- Depends on `refactor-pgen-companion-memory-model`.
