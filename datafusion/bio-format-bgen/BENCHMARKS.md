# BGEN benchmark snapshot

The Criterion fixture is generated at runtime and contains 2,048 biallelic
variants by 256 samples. It uses deterministic 8-bit probabilities and does
not commit genotype data.

Snapshot from Apple M3 Max, arm64, macOS 15.6, Rust 1.91.0:

| Scan | Median |
| --- | ---: |
| Layout 2 uncompressed probabilities | 107.19 ms |
| Layout 2 zlib probabilities | 111.22 ms |
| Layout 2 zstd probabilities | 158.81 ms |
| Layout 1 uncompressed probabilities | 75.60 ms |
| Metadata only | 98.05 us |
| Sparse BGI variants, 1/16 selected | 7.08 ms |
| Three selected samples, Layout 2 zlib | 66.37 ms |
| Layout 2 zstd dosage | 130.97 ms |
| Layout 2 zstd, four partitions | 56.31 ms |

These are implementation baselines rather than cross-machine targets. Run the
same generated matrix with:

```shell
cargo bench -p datafusion-bio-format-bgen --bench bgen_scan
```
