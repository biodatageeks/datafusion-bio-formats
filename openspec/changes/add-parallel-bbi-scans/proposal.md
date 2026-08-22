## Why

BigWig and BigBed are the only indexed format providers that always advertise a
single physical partition. DataFusion can redistribute their output but cannot
parallelize index traversal, decompression, or decoding at the source.

## What Changes

- Partition whole-file BigWig and BigBed scans according to the configured
  DataFusion target partition count.
- Weight and split scan regions using primary cir-tree block coordinates and
  encoded on-disk sizes rather than chromosome length alone.
- Preserve exact row ownership and original coordinates across independently
  queried shard boundaries with bounded, unclipped BigWig reads.
- Keep narrow single-region queries serial while allowing unfiltered
  single-chromosome files to use multiple partitions.
- Add plan diagnostics, correctness coverage, and a source-partition profiling
  example for count and decoded-column workloads.

## Impact

- Affected specs: `bbi-parallel-scan` (new)
- Affected code: `datafusion/bio-format-bbi`
- Dependency: a pinned BigTools revision exposes read-only primary cir-tree
  block layout metadata; replace the revision with a released version after
  upstream publication.
