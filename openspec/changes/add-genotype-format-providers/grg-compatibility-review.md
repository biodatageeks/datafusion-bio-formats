# GRG compatibility and licensing review

Review date: 2026-07-27

## Decision

**Blocked.** No GRG on-disk version is approved for an Apache-2.0 Rust
provider. Do not create `bio-format-grg`, enable a workspace feature, or port
the upstream serializer until the independent-format-contract conditions below
are satisfied.

This is a compatibility and provenance decision, not a statement that reading
GRG from Rust is technically impossible.

## Reviewed baseline

| Item | Reviewed value |
| --- | --- |
| Upstream repository | `aprilweilab/grgl` |
| Upstream commit | `7b896a00d8b23821e5a779048580f64ae9c34368` |
| Upstream release | `v2.10` |
| GRG library release date | 2026-07-17 |
| Declared file format | major 5, minor 3 |
| Upstream license | GPL-3.0-or-later source headers; GPL-3.0 license file |
| Public documentation | GRG model, traversal semantics, and C++/Python APIs |
| Standalone byte specification | Not found |

The current upstream source declares a 128-byte native header and a file magic,
and its loader requires the current major version. These facts are sufficient
to identify a file and reject unsupported versions. They are not a complete,
independently implementable contract for section ordering, integer encodings,
alignment, optional flags, graph arrays, strings, mutation records, or
missingness.

## Licensing boundary

The workspace is Apache-2.0. Upstream GRGL is GPL-3.0 and is not an approved
runtime dependency for this project. The genotype provider design also
prohibits vendoring, linking, line-by-line translation, or copying GRGL
serialization logic into default artifacts.

GRGL remains suitable as an external process-level conformance oracle. A test
may ask `pygrgl` or the GRGL CLI to create and inspect synthetic graphs, then
compare semantic output. No upstream source or binary is distributed with the
Rust crate.

## Compatibility gaps

The public documentation defines the graph model but does not currently define
the bytes needed by an independent reader. Before implementation, a reviewed
contract must specify at least:

- endianness, primitive widths, alignment, and header field offsets;
- the complete major/minor compatibility policy and feature-flag rules;
- every required and optional section in physical order;
- node, edge, sample, mutation, and population array encodings;
- variable-width integer and CSR index encodings;
- string length, character encoding, and long-allele representation;
- mutation-to-node references, recurrent-position behavior, and missingness;
- section offsets, file-size invariants, checksums if any, and allocation
  limits;
- historical fixtures for every version claimed as readable.

The format is implementation-versioned and has changed across releases. Guessing
an unrecorded layout would risk silent genotype corruption, which is
unacceptable for a query provider.

## Unblock conditions

Implementation may begin only after all of the following are true:

1. A byte-level GRG contract is published under terms compatible with an
   independent Apache-2.0 implementation, or project counsel explicitly
   approves another clean-room process.
2. The contract identifies the exact readable major/minor versions and
   required feature flags.
3. At least two people review provenance: one writes or supplies the contract
   and fixtures, while the Rust implementer works from those artifacts rather
   than GPL serializer source.
4. A synthetic conformance corpus covers shared subgraphs, recurrent
   positions, missingness, labeled and unlabeled samples, long alleles,
   corrupted offsets, cycles, and every supported version.
5. External `pygrgl` comparisons are pinned to the same compatibility baseline.
6. The licensing decision and compatibility matrix are approved in the change
   proposal before `bio-format-grg` is added to the workspace.

## Implementation sequence after approval

Once unblocked, tasks 7.2 through 7.16 remain the implementation plan:

1. Add an opt-in crate and local-only immutable loader.
2. Validate all file dimensions and graph invariants before allocation.
3. Expose the mutation catalog and exact metadata pushdowns.
4. Add selected downward traversal for haplotype presence.
5. Add explicit fixed-ploidy individual grouping.
6. Partition mutation ranges over shared immutable storage.
7. Differential-test through an external pinned `pygrgl` process.
8. Benchmark dense/sparse descendants, sample subsets, and parallel mutation
   ranges before considering default release inclusion.
