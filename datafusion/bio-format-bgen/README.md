# datafusion-bio-format-bgen

Read-only Apache DataFusion provider for BGEN 1.2/1.3 genotype
probabilities. Layout 2 supports uncompressed, zlib, and zstd blocks; Layout 1
supports its biallelic diploid uncompressed and zlib encodings.

One row represents one BGEN variant. Encoded alleles remain ordered in
`alleles` and are not assigned reference/alternate semantics. The default
`genotypes.GP` output preserves every probability state and `PLOIDY` preserves
each selected sample's declared ploidy. `BgenOutputMode::Dosage` instead emits
`DS`, the expected copy count of `alleles[1]`, and rejects multiallelic rows.

`genotypes.GP` is a variable-length list per sample by default, because BGEN
lets each variant store a different number of probability states.
`BgenProbabilityLayout::Fixed` emits a fixed-width list instead and drops the
per-sample offsets. Those offsets are a quarter of the emitted probability bytes
for a diploid biallelic cohort.

The width is derived once, from the first variant's block header widened to
cover every allele count the catalog reports, and appears in the schema. A
sample storing fewer states than that is padded with `NaN`, so a file that mixes
widths — including one whose variants declare a variable ploidy — is still
representable. Only a sample storing *more* states than the derived width is
rejected, which is a file the derivation could not have predicted from its first
variant; use the default layout for it.

```rust,no_run
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_bio_format_bgen::{BgenReadOptions, BgenTableProvider};

# async fn example() -> datafusion::common::Result<()> {
let provider =
    BgenTableProvider::try_new("cohort.bgen", BgenReadOptions::default()).await?;
let context = SessionContext::new();
context.register_table("cohort", Arc::new(provider))?;
let batches = context
    .sql("SELECT rsid, alleles, genotypes FROM cohort WHERE chrom = '1'")
    .await?
    .collect()
    .await?;
# Ok(())
# }
```

Sample identifiers are resolved from the embedded BGEN sample block, an
explicit `.sample` companion, or deterministic `sample_1` through `sample_N`
names. `samples` selects and reorders output samples before Arrow construction.

A standard local `cohort.bgen.bgi` is discovered automatically. Remote BGI
objects are downloaded into a content-addressed, bounded SQLite cache while
the BGEN object remains range-read. Set `bgi_cache_directory` or
`DATAFUSION_BIO_BGI_CACHE_DIR` to choose the cache location. Explicit invalid
indexes always fail; conventionally discovered stale indexes follow
`stale_bgi_policy`.

Exact pushdown covers `chrom`, `id`, `rsid`, `start`, and `end` comparisons,
numeric ranges, `IN`, and conjunctions. Metadata-only scans and genotype scans
with an empty sample selection do not read or decompress selected probability
blocks.

See [CONFORMANCE.md](CONFORMANCE.md) for the reference-oracle matrix and
[BENCHMARKS.md](BENCHMARKS.md) for the generated performance baseline.
