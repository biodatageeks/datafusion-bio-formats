## 1. Partition balance (first pull request)

- [ ] 1.1 Add `window_bytes` to `RegionSizeEstimate` and set it to empty in
  every existing constructor (BAM, CRAM, VCF, BCF, GFF, GTF, pairs, tests).
- [ ] 1.2 Add the byte-weighted split to `balance_partitions`, falling back to
  the existing logic when fewer than two weighted windows are in range.
- [ ] 1.3 Unit-test the balancer: skewed weights, empty weights reproduce
  today's assignments, single weighted window, several references, explicit
  sub-range.
- [ ] 1.4 Compute per-window weights for BAM from the BAI or CSI index in
  `estimate_sizes_from_binning_index`.
- [ ] 1.5 Add an uneven-coverage BAM fixture and an integration test for
  identical rows and bounded partition spread at 1, 2, 8 and 16 partitions,
  with BAI and CSI.
- [ ] 1.6 Record before and after timings with the protocol in `design.md`.

## 2. Sequence and quality decoding (second pull request)

- [ ] 2.1 Add the packed-base and quality helpers to
  `bio-format-core::alignment_utils`.
- [ ] 2.2 Unit-test the helpers against today's expressions over all packed
  byte values, lengths 0 to 65 and scores 0 to 255.
- [ ] 2.3 Switch the four decode sites in
  `bio-format-bam/src/physical_exec.rs` to the helpers.
- [ ] 2.4 Record before and after timings and a single-worker profile.

## 3. Verification

- [ ] 3.1 Run `cargo fmt --all -- --check`, `cargo clippy`, and the tests of
  every crate touched.
- [ ] 3.2 Run `openspec validate update-bam-scan-performance --strict`.
