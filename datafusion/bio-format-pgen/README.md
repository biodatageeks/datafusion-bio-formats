# datafusion-bio-format-pgen

Read-only Apache DataFusion provider for standard PLINK 2
PGEN/PVAR/PSAM filesets. The implementation is independent Rust and follows
the `plink-ng` PGEN specification at commit
`9ee41ce224ea7cd091760d69392a98835715b5b2` (2026-07-27).

One row represents one PVAR variant. `start` and `end` use the configured
coordinate system; the default is zero-based, half-open. PVAR `REF` is allele
index 0 and each `ALT` is preserved in source order as indices 1 through N.
The nested `genotypes` struct can contain:

| Field | Value |
| --- | --- |
| `GT` | Per-sample nullable fixed-size pair of encoded allele slots (`UInt16`) |
| `PHASED` | Per-sample nullable hardcall phase flag |
| `DS` | Effective nullable dosage of ALT allele index 1 (stored value, then hardcall fallback) |
| `DS_STORED` | Physically stored nullable dosage of ALT allele index 1 |
| `HDS` | Per-sample nullable pair of ALT-1 haplotype dosages |

PGEN does not carry biological ploidy. `GT` therefore exposes its two encoded
allele slots with `bio.pgen.ploidy_semantics=encoded_diploid` metadata and does
not infer chromosome-, sex-, build-, or PAR-dependent ploidy.

```rust,no_run
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_bio_format_pgen::{PgenReadOptions, PgenTableProvider};

# async fn example() -> datafusion::common::Result<()> {
let provider = PgenTableProvider::try_new(
    "cohort.pgen",
    PgenReadOptions {
        samples: Some(vec!["sample_3".to_string(), "sample_1".to_string()]),
        genotype_fields: Some(vec!["GT".to_string(), "DS".to_string()]),
        ..Default::default()
    },
)
.await?;
let context = SessionContext::new();
context.register_table("cohort", Arc::new(provider))?;
let batches = context
    .sql("SELECT id, ref, alt, genotypes FROM cohort WHERE chrom = '1'")
    .await?
    .collect()
    .await?;
# Ok(())
# }
```

The provider resolves `.pvar` (then `.pvar.zst`) and `.psam` beside the PGEN
file unless explicit locations are supplied. Plain, gzip, and zstd companions
are decoded as a bounded stream and parsed in parallel blocks into a columnar
variant table, so opening a panel of tens of millions of variants holds a
fixed window of text plus a few tens of bytes per variant. The companion
caps (`max_companion_bytes`, `max_decompressed_companion_bytes`,
`max_variants`, `max_samples`) default high enough for the published 1000
Genomes reference panels and name themselves in their errors. Modes `0x01`, `0x02`,
`0x03`, `0x04`, `0x10`, `0x11`, `0x20`, and `0x21` are supported, including
standard `.pgen.pgi` external indexes. BED/BIM/FAM hybrid companions are
rejected.

`PsamIdMode` controls selectable sample names. IID is the strict default;
`FidIid` and `FidIidSid` create escaped composite names. Requested samples are
emitted in request order, and absent names fail unless
`missing_sample_policy` is changed.

Exact pushdown covers `chrom` and `id` equality, inequality, and `IN`, plus
`start` and `end` comparisons, ranges, and `IN`. Conjunctions of exact
predicates remain exact. Metadata-only projections and empty sample
selections validate the fileset but do not read variant payloads. Sparse scans
range-read only selected records and any required LD base; adjacent reads are
coalesced within configured limits.

Parallel partitions have no implicit global row order. Add an SQL `ORDER BY`
when order is required. Multiallelic hardcalls are supported; multiallelic
dosage tracks are rejected explicitly because their output contract is not yet
implemented.

See [CONFORMANCE.md](CONFORMANCE.md) for reference-oracle testing and
[BENCHMARKS.md](BENCHMARKS.md) for the generated performance matrix.
