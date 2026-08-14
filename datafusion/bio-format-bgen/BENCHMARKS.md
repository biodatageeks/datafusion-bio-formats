# BGEN benchmark snapshot

The Criterion fixture is generated at runtime and contains 2,048 biallelic
variants by 256 samples. It uses deterministic 8-bit probabilities and does
not commit genotype data.

Snapshot from Apple M3 Max, arm64, macOS 15.6, Rust 1.91.0:

| Scan | Median |
| --- | ---: |
| Layout 2 uncompressed probabilities | 19.07 ms |
| Layout 2 zlib probabilities | 19.53 ms |
| Layout 2 zstd probabilities | 18.87 ms |
| Layout 1 uncompressed probabilities | 12.67 ms |
| Metadata only | 98.51 us |
| Sparse BGI variants, 1/16 selected | 2.70 ms |
| Three selected samples, Layout 2 zlib | 1.97 ms |
| Layout 2 zstd dosage | 3.74 ms |
| Layout 2 zstd, four partitions | 6.40 ms |

These are implementation baselines rather than cross-machine targets. Run the
same generated matrix with:

```shell
cargo bench -p datafusion-bio-format-bgen --bench bgen_scan
```

## Whole-chromosome scan

The generated matrix above is too small to show planning or I/O behaviour, so
the provider is also measured on a real whole-chromosome file: 1000 Genomes
GRCh38 chromosome 22, exported to BGEN 1.2 with `plink2 --export bgen-1.2
bits=8`. The result is 993,881 phased biallelic variants by 2,548 samples in a
160,522,183-byte Layout 2 zlib file, and a full dosage scan materializes
2,532,408,788 `float32` values.

Dosage scan of that file, same host, by `target_partitions`:

| Partitions | Scan | Total | Peak RSS |
| ---: | ---: | ---: | ---: |
| 1 | 23.71 s | 25.90 s | 10.9 GiB |
| 2 | 12.87 s | 15.02 s | 10.9 GiB |
| 4 | 7.46 s | 9.64 s | 11.7 GiB |
| 8 | 4.94 s | 7.14 s | 12.5 GiB |

Peak RSS covers the retained Arrow output, which carries both the `DS` dosage
list and the `PLOIDY` list.

Opening the same file, which reads the header and builds the transient variant
catalog, takes 0.13 s.
