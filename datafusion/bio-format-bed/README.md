# datafusion-bio-format-bed

A streaming DataFusion `TableProvider` for tab-delimited BED records, including
plain, gzip (multiple members), and BGZF files. Local paths, `file://` paths,
HTTP(S), GCS, S3, and Azure Blob Storage use the same record validation.

## Output schema

The `BEDFields` argument selects the output columns:

| Mode | Columns |
| --- | --- |
| `BED3` | `chrom: Utf8`, `start: UInt32`, `end: UInt32` |
| `BED4` | BED3 plus `name: Utf8` |
| `BED5` | BED4 plus `score: UInt16` |
| `BED6` | BED5 plus `strand: Utf8` |

Every record must have `chrom`, `start`, and `end`. Optional output fields are
nullable: an absent field or `.` becomes null. An explicitly empty name remains
an empty string. BED4 accepts BED3 input without changing its four-column schema,
which preserves compatibility with polars-bio.

BED7–BED12 and custom suffix columns are accepted, but only the selected columns
are exposed. Fields beyond the selected mode are not interpreted; this reader
does not validate BED12 block structure. Mixed widths are accepted for
compatibility, though the UCSC format recommends consistent widths per track.

Coordinates must be unsigned integers fitting UInt32, with `end >= start`.
Zero-length intervals, including `0–0`, are supported. With `zero_based=true`,
coordinates are the on-disk half-open `(start, end)` values. With `false`, only
start changes to `start + 1`, using checked arithmetic. An empty interval remains
empty (`start > end` in this representation). Schema coordinate metadata is
preserved through column projections and count queries.

When included in the output mode, score must be 0–1000 (or missing), and strand
must be `+`, `-`, or `.` (or absent). See the
[UCSC BED definition](https://genome.ucsc.edu/FAQ/FAQformat.html#format1).

## Example

```rust
use datafusion::prelude::*;
use datafusion_bio_format_bed::table_provider::{BEDFields, BedTableProvider};
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let table = BedTableProvider::new(
        "regions.bed".to_owned(),
        BEDFields::BED4,
        None,
        true,
    )?;
    ctx.register_table("regions", Arc::new(table))?;
    let result = ctx.sql("SELECT chrom, start, \"end\", name FROM regions").await?;
    result.show().await?;
    Ok(())
}
```

Compression is detected from the content, independent of the suffix. Set
`ObjectStorageOptions.compression_type` to override detection; the override is
honored for both local and remote files. Passing `None` for storage options uses
defaults for remote files too.

## Errors and line handling

LF, CRLF, and an unterminated final record behave consistently across backends.
Blank lines, `#` comments, and space-delimited UCSC `track`/`browser` directives
are skipped. Tab-delimited records whose chromosome is `track` or `browser` are
still read as data. File contents must be UTF-8.

Missing required fields, invalid coordinates, selected invalid optional fields,
invalid UTF-8, and I/O/decompression failures return errors. The reader never
logs a failed record and continues with an incomplete successful result. Parsing
errors include the physical line number; conversion errors include the record
number, and query errors identify the file. As with other streaming readers,
rows may already have been yielded before a later error; a limited query only
validates the records it consumes.

## Low-level reader API

`BedLocalReader::<N>::new(path).await?` and
`BedRemoteReader::<N>::new(path, options).await?` return fallible readers.
The remote constructor now returns `Result` instead of panicking on open or
compression-detection errors; callers must handle it with `?` or equivalent.
`BedLocalReader::with_options` accepts explicit compression options.
The remote `read_records().await` and `lines().await` signatures are preserved
for compatibility; constructing these streams does not perform I/O.

HTTP scans keep chunked range reads when size discovery succeeds. If a
GET-scoped signed URL denies the HEAD preflight, they retry with a sequential
GET before applying the plain, multi-member gzip, or BGZF decoder. Missing
objects and failures while reading or decoding still return errors.
Automatic HTTP compression detection reads up to 18 bytes and accepts normal
EOF, so empty files and BED records shorter than the probe are supported.

Low-level `N=4` readers accept BED3 and pad its absent name with `.`. Low-level
`N=5`/`N=6` readers require at least that many fields. The table provider reads the
three required fields and supplies nulls for absent optional output columns.
The `get_local_bed_*_reader` helpers return raw Noodles readers and retain the
Noodles parser's strict width semantics.

## Tests

```sh
cargo test -p datafusion-bio-format-bed
cargo clippy -p datafusion-bio-format-bed --all-targets -- -D warnings
cargo fmt -p datafusion-bio-format-bed -- --check
```

The tests cover the width/output-mode matrix, line endings, names, coordinates,
malformed input, tiny buffers, injected read errors, compression boundaries,
projections/counts/filters/limits, metadata, and actual remote reads against a
local HTTP fixture server. No cloud account or external test service is required.
