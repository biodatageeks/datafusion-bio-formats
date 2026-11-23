# GenBank table provider

GenBankTableProvider reads nucleotide GenBank flat files into selectable records and features tables. A custom streaming parser assembles sequence, qualifiers and location parts on demand. Local and remote files can be plain text, gzip or BGZF. BGZF support uses the workspace noodles fork with libdeflate. Building requires Git access to the pinned workspace dependencies.
