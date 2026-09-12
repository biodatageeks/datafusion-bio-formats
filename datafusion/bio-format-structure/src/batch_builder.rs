//! Typed Arrow columns built directly from normalized entries, with explicit row counts.
//!
//! Column indices follow `schema::schema`: indices `0..COMMON` are shared by both levels and
//! read from each row's [`Atom`]; the remaining indices are level-specific.
use crate::{
    model::{Atom, NormalizedEntry},
    options::{StructureLevel, StructureOptions},
    residue::{Residue, residues, selected_atoms},
};
use datafusion::{
    arrow::{
        array::{
            ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
            RecordBatchOptions, StringArray, UInt64Array,
        },
        datatypes::SchemaRef,
    },
    common::Result,
};
use std::sync::Arc;
/// Number of leading columns shared by the atom and residue schemas.
const COMMON: usize = 20;
/// One typed Arrow column from a per-row accessor.
macro_rules! column {
    ($rows:expr, $array:ty, |$r:ident| $value:expr) => {
        Arc::new(<$array>::from_iter($rows.iter().map(|$r| $value))) as ArrayRef
    };
}
fn common(index: usize, e: &NormalizedEntry, atoms: &[&Atom]) -> ArrayRef {
    match index {
        0 => column!(atoms, StringArray, |_a| Some(e.source_path.as_str())),
        1 => column!(atoms, StringArray, |_a| Some(e.source_format.as_str())),
        2 => column!(atoms, UInt64Array, |_a| Some(e.source_index)),
        3 => column!(atoms, UInt64Array, |_a| Some(e.entry_index)),
        4 => column!(atoms, StringArray, |_a| e.entry_id.as_deref()),
        5 => column!(atoms, StringArray, |_a| e.entry_name.as_deref()),
        6 => column!(atoms, UInt64Array, |_a| e.entry_key),
        7 => column!(atoms, StringArray, |_a| e.data_block.as_deref()),
        8 => column!(atoms, Int32Array, |a| Some(a.model_id)),
        9 => column!(atoms, UInt64Array, |a| Some(a.model_index)),
        10 => column!(atoms, UInt64Array, |a| Some(a.chain_index)),
        11 => column!(atoms, UInt64Array, |a| Some(a.segment_index)),
        12 => column!(atoms, UInt64Array, |a| Some(a.residue_index)),
        13 => column!(atoms, StringArray, |a| a.chain_id()),
        14 => column!(atoms, StringArray, |a| a.auth_asym_id.as_deref()),
        15 => column!(atoms, StringArray, |a| a.label_asym_id.as_deref()),
        16 => column!(atoms, StringArray, |a| a.label_entity_id.as_deref()),
        17 => column!(atoms, StringArray, |a| a.auth_seq_id.as_deref()),
        18 => column!(atoms, Int64Array, |a| a.label_seq_id),
        19 => column!(atoms, StringArray, |a| a.insertion_code.as_deref()),
        _ => unreachable!("common column index validated against schema"),
    }
}
fn atom_column(index: usize, atoms: &[&Atom]) -> ArrayRef {
    match index {
        20 => column!(atoms, UInt64Array, |a| Some(a.atom_index)),
        21 => column!(atoms, StringArray, |a| a.atom_id.as_deref()),
        22 => column!(atoms, StringArray, |a| Some(a.record_type.as_str())),
        23 => column!(atoms, StringArray, |a| Some(a.atom_name.as_str())),
        24 => column!(atoms, StringArray, |a| Some(a.residue_name.as_str())),
        25 => column!(atoms, StringArray, |a| a.auth_atom_id.as_deref()),
        26 => column!(atoms, StringArray, |a| a.label_atom_id.as_deref()),
        27 => column!(atoms, StringArray, |a| a.auth_comp_id.as_deref()),
        28 => column!(atoms, StringArray, |a| a.label_comp_id.as_deref()),
        29 => column!(atoms, StringArray, |a| a.alt_id.as_deref()),
        30 => column!(atoms, StringArray, |a| a.element.as_deref()),
        31 => column!(atoms, Float64Array, |a| Some(a.position[0])),
        32 => column!(atoms, Float64Array, |a| Some(a.position[1])),
        33 => column!(atoms, Float64Array, |a| Some(a.position[2])),
        34 => column!(atoms, Float64Array, |a| a.occupancy),
        35 => column!(atoms, Float64Array, |a| a.b_factor),
        36 => column!(atoms, Int32Array, |a| a.formal_charge),
        _ => unreachable!("atom projection validated against schema"),
    }
}
fn residue_column(index: usize, rows: &[Residue]) -> ArrayRef {
    // Backbone coordinate columns 25..=36 are N, CA, C, O triples of x, y, z.
    if (25..=36).contains(&index) {
        let (atom, axis) = ((index - 25) / 3, (index - 25) % 3);
        return column!(rows, Float64Array, |r| r.backbone[atom].map(|p| p[axis]));
    }
    // Angle columns 37..=42 are phi, psi, omega, N-CA-C, CA-C-N, C-N-CA.
    if (37..=42).contains(&index) {
        return column!(rows, Float64Array, |r| r.angles[index - 37]);
    }
    match index {
        20 => column!(rows, StringArray, |r| Some(r.atom.residue_name.as_str())),
        21 => column!(rows, StringArray, |r| r.parent_residue_name.as_deref()),
        22 => column!(rows, StringArray, |r| r.one_letter_code.as_deref()),
        23 => column!(rows, StringArray, |r| r.selected_alt_id.as_deref()),
        24 => column!(rows, StringArray, |r| Some(r.residue_kind)),
        43 => column!(rows, BooleanArray, |r| Some(r.peptide_link_prev)),
        44 => column!(rows, BooleanArray, |r| Some(r.peptide_link_next)),
        45 => column!(rows, BooleanArray, |r| Some(r.backbone_complete)),
        46 => column!(rows, Float64Array, |r| r.ca_b_factor),
        47 => column!(rows, StringArray, |r| Some(r.geometry_status)),
        _ => unreachable!("residue projection validated against schema"),
    }
}
pub fn build(
    entry: &NormalizedEntry,
    options: &StructureOptions,
    schema: SchemaRef,
    projection: &[usize],
) -> Result<RecordBatch> {
    let (columns, rows): (Vec<ArrayRef>, usize) = match options.level {
        StructureLevel::Atom => {
            let atoms = selected_atoms(entry, options);
            let columns = projection
                .iter()
                .map(|&i| {
                    if i < COMMON {
                        common(i, entry, &atoms)
                    } else {
                        atom_column(i, &atoms)
                    }
                })
                .collect();
            (columns, atoms.len())
        }
        StructureLevel::Residue => {
            let rows = residues(entry, options);
            let atoms: Vec<&Atom> = rows.iter().map(|r| &r.atom).collect();
            let columns = projection
                .iter()
                .map(|&i| {
                    if i < COMMON {
                        common(i, entry, &atoms)
                    } else {
                        residue_column(i, &rows)
                    }
                })
                .collect();
            (columns, rows.len())
        }
    };
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )?)
}
