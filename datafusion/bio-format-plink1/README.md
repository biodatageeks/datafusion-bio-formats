# datafusion-bio-format-plink1

Read-only Apache DataFusion table provider for current, variant-major PLINK 1
binary filesets (`.bed`, `.bim`, and `.fam`).

One output row represents one BIM variant. The `genotypes.GT` list contains
nullable A1 dosages in selected FAM sample order:

| BED code | A1 dosage |
| --- | --- |
| `00` | 2 |
| `10` | 1 |
| `11` | 0 |
| `01` | null |

```rust,no_run
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_bio_format_plink1::{PlinkReadOptions, PlinkTableProvider};

# async fn example() -> datafusion::common::Result<()> {
let provider = PlinkTableProvider::try_new(
    "cohort.bed",
    PlinkReadOptions::default(),
)
.await?;

let context = SessionContext::new();
context.register_table("cohort", Arc::new(provider))?;
let batches = context
    .sql("SELECT id, genotypes FROM cohort WHERE chrom = '1'")
    .await?
    .collect()
    .await?;
# Ok(())
# }
```

The default sample identifier is the FAM IID and must be unique. Use
`SampleIdMode::FidIid` for an escaped `FID:IID` identifier when IIDs are
ambiguous. Percent and colon characters are escaped as `%25` and `%3A`.
