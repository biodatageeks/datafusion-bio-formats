# PGEN benchmark snapshot

The Criterion fixture is generated at runtime and contains 2,048 biallelic
variants by 128 samples. It commits no genotype data.

The matrix measures dense, one-bit, difflist, and LD-heavy hardcalls; metadata
projection; 1-in-16 sparse variant selection; three selected samples; dosage
output; and a four-partition dense scan.

Snapshot from Apple M3 Max, arm64, macOS 15.6, Rust 1.91.0:

| Scan | Median |
| --- | ---: |
| Dense hardcalls | 6.84 ms |
| One-bit hardcalls | 6.55 ms |
| Difflist hardcalls | 6.56 ms |
| LD-heavy hardcalls | 6.47 ms |
| Metadata only | 112.00 us |
| Sparse variants, 1/16 selected | 5.99 ms |
| Three selected samples | 2.69 ms |
| Dosage output | 5.37 ms |
| Dense hardcalls, four partitions | 1.93 ms |

Run it with:

```shell
cargo bench -p datafusion-bio-format-pgen --bench pgen_scan
```

Treat these as same-machine regression baselines, not cross-machine targets.
