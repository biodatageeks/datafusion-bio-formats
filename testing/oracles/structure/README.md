# Frozen structure oracle corpus

Install requirements.txt into a separate Python 3.12 environment, then run
`python testing/oracles/structure/generate.py --check` from the repository root.
Omit --check only when intentionally regenerating and reviewing changed fixtures.
The generator never imports the Rust implementation or polars-bio.

Inputs: RCSB 1UBQ PDB/mmCIF; FCZ generated with `foldcomp.compress('1ubq-probe',
pdb_text)`; the upstream 24-entry example database at the commit in manifest.json.
Input/output SHA-256 digests and dependency versions are frozen in the manifest.
The two 1UBQ text inputs have 660 identical atom identities/coordinates, 76 peptide
residues, and 451 defined values across six angle columns. Gemmi and Biopython
calculations on identical Float64 coordinates agree within 1e-6 circular degrees.
Synthetic native tests separately cover conformers, insertion/missing tokens,
model/TER/label gaps, incomplete backbones, rigid transformations and malformed data.

Foldcomp expectations use direct get_data coordinates and independently parsed
decoded atom identities. PDB rounding is checked within 0.00051 Angstroms but is
never the native coordinate oracle. Geometry is independently recomputed from raw
reconstructions. Raw coordinate tolerance is 2e-5 Angstroms and circular angle
tolerance 0.01 degrees to permit compiler/platform differences in the upstream
Float32 codec; schema, identities, counts and null masks must match exactly.
This tests decoder parity, not fidelity against the original uncompressed model.

The polars-bio companion PR carries a hash-identical subset under tests/data/structure
and verifies the manifest before exercising Python/SQL scans. No network access or
oracle packages are required for ordinary native/Python correctness tests.

Site-specific modified-residue parents follow the wwPDB
[pdbx_struct_mod_residue category](https://mmcif.pdb.org/dictionaries/mmcif_ma.dic/Items/_pdbx_struct_mod_residue.parent_comp_id.html),
with author/label site identity matching before falling back to chem_comp metadata.
