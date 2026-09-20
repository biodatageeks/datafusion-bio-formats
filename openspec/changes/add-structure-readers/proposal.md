# Change: Native PDB/mmCIF and Foldcomp atom/residue providers

## Why

[polars-bio #455](https://github.com/biodatageeks/polars-bio/issues/455) requests structural file collections and Foldcomp subsets as DataFrames with residue identity, coordinates, and bond angles. The native provider should supply both atom and residue tables, allowing polars-bio to expose those results through its existing lazy/SQL interfaces.

## What Changes

- Add `datafusion-bio-format-structure` for PDB/mmCIF parsing, normalized identity, atom/residue schemas, collection execution, conformer selection, connectivity, and pure geometry modules.
- Add `datafusion-bio-format-foldcomp` for standalone FCZ and indexed local databases, reusing the structure crate's normalized-entry and atom/residue batch builder.
- Use a checked native Gemmi low-level CIF adapter and upstream Foldcomp codec, subject to fixture and cross-platform feasibility gates.
- Keep neighbor-dependent residue calculation inside each provider's scan path. Expose complete Arrow rows to DataFusion and polars-bio without a functions repository dependency.
- Own the independent oracle generator, canonical input/output corpus, native parity tests, and provider work/memory metrics here. Supply a versioned fixture subset to polars-bio.

## Impact

- Affected capability: new `structure-providers` specification covering the native implementation of the shared public contract.
- Affected code: two new `datafusion/bio-format-*` crates, workspace/release metadata, fixture/oracle tooling, and targeted CI/benchmark updates. Reuse bio-format-core storage/partition helpers; change them only for demonstrated missing functionality.
- Shared public schemas and policies: [polars-bio design](../../../../polars-bio/openspec/changes/add-structure-readers/design.md) and its `structure-io`, `residue-descriptors`, and `foldcomp-io` deltas. Do not independently redefine numerical conventions here.
- Native implementation details: [design.md](design.md). Native PR/task sequence: [tasks.md](tasks.md). Python integration: [polars-bio tasks](../../../../polars-bio/openspec/changes/add-structure-readers/tasks.md).
- New native dependencies must satisfy current workspace toolchain/DataFusion/Arrow pins and supported wheel platforms; parser and codec features are isolated where possible. No functions crate or functions release is required.

Status: planned on 2026-09-09. The shared research probes have been executed; native adapters, providers, general fixture generators, and performance claims remain implementation work.
