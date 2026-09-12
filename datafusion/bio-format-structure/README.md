# Protein structure providers

Read PDB and mmCIF collections as atom or residue Arrow tables using DataFusion 53.

```rust,no_run
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion_bio_format_structure::{StructureTableProvider, StructureOptions, StructureLevel};

# async fn example() -> datafusion::common::Result<()> {
let ctx = SessionContext::new();
let options = StructureOptions { level: StructureLevel::Residue, ..Default::default() };
let table = StructureTableProvider::new(
    vec!["protein.pdb".into(), "predictions/*.cif.gz".into()], None, options, None)?;
ctx.register_table("residues", Arc::new(table))?;
ctx.sql("SELECT source_path, auth_seq_id, phi_deg, psi_deg FROM residues")
    .await?.show().await?;
# Ok(())
```

`StructureOptions` selects atom/residue output, all/first/numbered models,
all/best-backbone/explicit alternate sites, peptide/all residue populations, the
maximum peptide bond distance, and input limits. An all-altloc setting at residue
level uses best-backbone selection. The Python API makes that default explicit.

**Schema version 1:** `schema::schema()` returns the complete fixed schema without
opening coordinate payloads. Common columns preserve source occurrences, CIF data
blocks, entry/model/chain/segment/residue ordinals, and separate author/label IDs.
Author residue IDs are strings; PDB label IDs stay null. `atom_name` and
`residue_name` are the standardized `label_atom_id`/`label_comp_id` when a CIF
provides them (author spellings stay in `auth_atom_id`/`auth_comp_id`), and the
author names otherwise, as in PDB. Blank PDB chains stay empty
strings. CIF unquoted `.` and `?` are null; quoted versions remain literal strings.
Atom columns retain all atom records, alternate sites, coordinates, occupancy,
B factors and charge. Atom indices retain the original entry row ordering.

Residue output includes N/CA/C/O coordinate triples, component/parent/one-letter
identity, selected alternate ID, six angles, link/completeness flags and geometry
status. Peptides use standard residues, MSE/SEC/PYL, PDB MODRES, and CIF polymer /
chemical-component metadata. Unknown peptides map to X. Nonpeptides are excluded
unless `include_non_peptide` is enabled; their geometry is null.

Coordinates use **Angstroms**, Float64; angles use **degrees**, Float64. Neither the
schema nor the numeric values use genomic zero/one-based coordinate settings.
Dihedrals are in [-180, 180), bond angles in [0, 180]. Geometry is null for missing
atoms, termini, breaks, or degenerate vectors/planes (normalization cutoff 1e-12).

| Column on residue i | Points |
|---|---|
| phi_deg | C(i-1), N(i), CA(i), C(i) |
| psi_deg | N(i), CA(i), C(i), N(i+1) |
| omega_deg | CA(i), C(i), N(i+1), CA(i+1) |
| angle_n_ca_c_deg | N(i), CA(i), C(i) |
| angle_ca_c_n_deg | CA(i), C(i), N(i+1) |
| angle_c_n_ca_deg | C(i-1), N(i), CA(i) |

Conformer selection maximizes the number of N/CA/C atoms in one component/alternate
candidate, then their mean occupancy (missing = zero), then A, then lexical
alternate/component. Blank sites are shared; a named site supersedes a blank of
the same name. It never combines mutually exclusive alternate backbone atoms.
Connectivity requires compatible entry/model/chain/TER segment/entity/alternate,
adjacent label sequence numbers where available, and C–N distance <= 1.8 Angstroms
(configurable). Author numbering gaps alone do not break links. Neighbors are the
previous/next peptide residues in the ordered chain; interleaved water or ligand
sites are skipped rather than treated as breaks.

Local lists retain occurrence order; globs expand in sorted order. Explicit HTTP,
S3, GCS and Azure URLs use core OpenDAL options; an HTTP endpoint that refuses
HEAD (a GET-only pre-signed URL) is read with one sequential GET. Gzip is
detected by magic bytes. A PDB source with no ATOM/HETATM record and a CIF source
with no `atom_site` category are errors, not empty tables.
Remote globs, BinaryCIF, PDBXML, assembly expansion, atom repair and inferred bonds
are outside this reader. PDB serial/residue fields may contain hybrid-36 text;
coordinates must be finite decimal numbers. Unsupported/malformed coordinate
records raise contextual errors; duplicate atom sites are rejected.

The execution plan partitions whole files, bounded by DataFusion target partitions.
Each active worker holds one decoded entry and its projected Arrow batch. PDB holds
the file text while parsing its single entry. CIF holds the native parser document
for the whole file (the text buffer is released once parsed) and decodes data
blocks one at a time as the stream is polled, so a multi-block file never retains
more than one normalized entry. Defaults are 256 MiB encoded text input, 512 MiB
decompressed text, and 5 million atoms per entry (per data block). This is bounded
by the largest active structures, not constant memory within an arbitrary file.
Output batches respect the session batch size. No nested decoder thread pool is
created.

Projection builds only requested Arrow columns. Residue grouping/geometry is
computed with the original neighbors before row filters and limits. Row filters
remain in DataFusion and never prune parsing: a `WHERE` on `source_path`,
`entry_index` or `data_block` still decodes every entry. Global limits remain
above the provider. Repeated and
concurrent collections open fresh cursors. Plan metrics expose sources opened,
entries decoded, encoded bytes read, and output rows. Counts still decode entries.

`text-formats` (default) builds the pinned Gemmi CIF adapter. Disable default
features to use just the shared model/geometry/provider API, as Foldcomp does.
Native build inputs and licenses are packaged under `native/`; a C++17 compiler
is required, with no installed Gemmi library or runtime Python dependency.

Offline tests use `../../testing/oracles/structure/` goldens. The pinned generator
cross-checks Gemmi and Biopython geometry and has a `--check` mode. See the workspace
structure portability workflow for platform and sanitizer coverage.
