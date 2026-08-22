## 1. Upstream metadata API

- [x] 1.1 Expose chromosome IDs and primary cir-tree block layout from BigTools.
- [x] 1.2 Cover BigWig and BigBed layout metadata with fixture tests.

## 2. Partition planning

- [x] 2.1 Normalize cir-tree blocks into chromosome-local compressed-work units.
- [x] 2.2 Balance selected regions using target partitions and block positions.
- [x] 2.3 Preserve boundary row ownership and unclipped coordinates.
- [x] 2.4 Advertise and execute the planned source partitions.

## 3. Correctness and diagnostics

- [x] 3.1 Verify row counts and complete content across partition counts.
- [x] 3.2 Verify filtered, empty, one-chromosome, and skewed-region behavior.
- [x] 3.3 Report partition count and estimated compressed bytes in physical plans.
- [x] 3.4 Add a source-stage partition profiling example.

## 4. Verification

- [x] 4.1 Run formatting, crate tests, clippy, and strict OpenSpec validation.
- [ ] 4.2 Record whole-file t=1 through t=8 benchmark evidence downstream.
