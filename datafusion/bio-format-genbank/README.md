# GenBank table provider

`GenBankTableProvider` reads nucleotide (`bp`) and protein (`aa`) LOCUS records into selectable records and features tables. Protein records report their length in residues and their molecule type as `protein`. A custom streaming parser assembles sequence, qualifiers and location parts on demand. Local and remote files can be plain text, gzip or BGZF. BGZF support uses the workspace noodles fork with libdeflate. Building requires Git access to the pinned workspace dependencies.

A location such as `10^11` lies between two residues. In zero-based half-open coordinates its bounds are `[10, 10)`. One-based closed coordinates cannot represent an empty interval, so the feature and its part have null bounds. The `location` column preserves `10^11` in both modes.
