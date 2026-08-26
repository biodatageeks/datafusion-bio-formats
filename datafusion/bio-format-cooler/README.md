# datafusion-bio-format-cooler

Cooler (`.cool`/`.mcool`) Hi-C contact matrix support for Apache DataFusion.

Reads the pixels table of a cooler data collection as a DataFusion table:

- `.cool` (single resolution) and `.mcool` (multi-resolution, selected via a
  `resolution` argument or the cooler URI syntax `file.mcool::/resolutions/N`)
- joined output (`chrom1`, `start1`, `end1`, `chrom2`, `start2`, `end2`,
  `count`, optional `weight1`/`weight2` balancing weights) or the raw COO
  triple (`bin1_id`, `bin2_id`, `count`)
- streaming, fixed-size record batches; projection-aware dataset reads;
  `count(*)` served without touching pixel data
- parallel partitions split along bin1 boundaries via `indexes/bin1_offset`

HDF5 is provided by `hdf5-metno` with the `static` + `zlib` features: libhdf5
is built from source and statically linked, so no system HDF5 is required at
runtime. Local filesystem paths only.

Test fixtures are generated with the reference
[cooler](https://github.com/open2c/cooler) implementation
(`tests/data/generate_fixtures.py`).
