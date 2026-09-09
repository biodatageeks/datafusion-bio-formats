# Foldcomp table provider

Read local standalone FCZ files or selected entries of an indexed Foldcomp database
into the same atom/residue schemas as `datafusion-bio-format-structure`.

```rust,no_run
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion_bio_format_foldcomp::{FoldcompOptions, FoldcompTableProvider};
use datafusion_bio_format_structure::{StructureOptions, StructureLevel};
# async fn example() -> datafusion::common::Result<()> {
let ctx = SessionContext::new();
let options = FoldcompOptions {
    ids: Some(vec!["d1asha_".into(), "d1it2a_".into()]),
    structure: StructureOptions { level: StructureLevel::Residue, ..Default::default() },
    ..Default::default()
};
ctx.register_table("proteins", Arc::new(FoldcompTableProvider::new("example_db".into(), options)?))?;
ctx.sql("SELECT entry_name, auth_seq_id, phi_deg FROM proteins").await?.show().await?;
# Ok(())
```

`ids` selects `.lookup` names; `entry_keys` selects numeric `.index` keys without
requiring a lookup. Selectors are mutually exclusive. None means all entries, an
empty selector means zero, duplicates select once, and missing/ambiguous requests
raise errors. Standalone FCZ has no index selectors. `entry_key`, `entry_name`,
`entry_id` (internal codec title), and `entry_index` (database ordinal) stay separate.

Supported databases are uncompressed type 12, one local payload file plus a text
`.index` containing **unique increasing keys**, offsets and lengths including the
NUL record terminator. A `.lookup` is required for name selection and optional for
numeric selection. Optional `.source` files are not needed to decode coordinates.
Split, outer-compressed, remote and other database types are rejected. FCZ requires
little-endian targets and at least two residues, matching the checked codec path.

Selection scans metadata in O(N) time with O(K) retained selection state. Only K
selected payloads decode, partitioned across the configured DataFusion workers.
Selected bounds, terminators, header sizes, residue/sidechain counts, anchor
indices and finite coordinates are checked before entering the upstream codec.
Metadata lines are capped at 1 MiB. Source/sidecar size and modification time are
checked again at execution; modify the database only after its scans finish.
Encoded entry size and reconstructed atom count use the shared input/atom limits.

The pinned MIT Foldcomp codec returns arrays directly, without a CLI, Python
runtime, PDB text round-trip, OpenMP or Gemmi dependency. Coordinates are lossy
reconstructions; all six angles are recomputed using the shared geometry code.
Missing source fields remain null, including occupancy, label identifiers, charge
and element. B factors are retained without assuming pLDDT provenance.

See the structure crate README for the schemas, conformer/link/angle conventions,
query semantics and memory model. Native sources and notices are under `native/`.
`testing/native/check-codec.sh` runs truncation/mutation smoke tests with ASan/UBSan.
