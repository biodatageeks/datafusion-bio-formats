# Add Parallel Partitioning for VCF Zarr Scans

## Why

VCF Zarr scans currently execute as a single DataFusion physical partition even when the session target partition count is greater than one. Large Zarr stores should be able to read independent variant chunks in parallel using the same `target_partitions` control model already used by indexed VCF reads.

## What Changes

- Add chunk-aligned partition planning for VCF Zarr row selections.
- Make `VcfZarrExec` expose multiple DataFusion partitions bounded by `SessionConfig::target_partitions()`.
- Preserve existing projection pruning, genomic predicate pruning, sample selection, and logical schema behavior.
- Document that parallel VCF Zarr scans do not guarantee global row order.
- Document that this change does not split inside a selected Zarr variant chunk.

## Impact

- Affected specs: **NEW** `vcf-zarr` capability.
- Affected code:
  - `datafusion/bio-format-vcf/src/zarr/planning.rs`
  - `datafusion/bio-format-vcf/src/zarr/pruning.rs`
  - `datafusion/bio-format-vcf/src/zarr/table_provider.rs`
  - `datafusion/bio-format-vcf/src/zarr/physical_exec.rs`
  - `datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`
- No breaking API changes. Existing VCF Zarr queries remain valid; only physical execution partitioning changes when `target_partitions > 1`.
