# datafusion-bio-format-bbi

BigWig and BigBed table providers for Apache DataFusion.

## Git dependency

This crate temporarily pins
[`biodatageeks/bigtools`](https://github.com/biodatageeks/bigtools) at revision
`0d7a5728eb39ee97fddef59cd3da469186bec90d`. The fork exposes the primary
cir-tree block layout, bounded unclipped BigWig reads, and traversal-limit
classification needed for safe block-aware source partitioning. The changes are
proposed upstream in [bigtools#106](https://github.com/jackh726/bigtools/pull/106);
the git pin should be replaced by a released upstream version after publication.

Cargo fetches this dependency during the build, so Git and network access are
required.
