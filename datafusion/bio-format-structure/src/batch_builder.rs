//! Typed Arrow columns built directly from normalized entries, with explicit row counts.
use crate::{
    model::NormalizedEntry,
    options::{StructureLevel, StructureOptions},
    residue::{residues, selected_atoms},
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
#[allow(unused_variables)]
pub fn build(
    entry: &NormalizedEntry,
    options: &StructureOptions,
    schema: SchemaRef,
    projection: &[usize],
) -> Result<RecordBatch> {
    let e = entry;
    match options.level {
        StructureLevel::Atom => {
            let rows = selected_atoms(entry, options);
            let columns: Vec<ArrayRef> = projection
                .iter()
                .map(|i| match *i {
                    0 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(e.source_path.as_str())
                    }))) as ArrayRef, // source_path
                    1 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(e.source_format.as_str())
                    }))) as ArrayRef, // source_format
                    2 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(e.source_index)
                    }))) as ArrayRef, // source_index
                    3 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(e.entry_index)
                    }))) as ArrayRef, // entry_index
                    4 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        e.entry_id.as_deref()
                    }))) as ArrayRef, // entry_id
                    5 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        e.entry_name.as_deref()
                    }))) as ArrayRef, // entry_name
                    6 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        e.entry_key
                    }))) as ArrayRef, // entry_key
                    7 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        e.data_block.as_deref()
                    }))) as ArrayRef, // data_block
                    8 => Arc::new(Int32Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.model_id)
                    }))) as ArrayRef, // model_id
                    9 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.model_index)
                    }))) as ArrayRef, // model_index
                    10 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.chain_index)
                    }))) as ArrayRef, // chain_index
                    11 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.segment_index)
                    }))) as ArrayRef, // segment_index
                    12 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.residue_index)
                    }))) as ArrayRef, // residue_index
                    13 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.chain_id()
                    }))) as ArrayRef, // chain_id
                    14 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.auth_asym_id.as_deref()
                    }))) as ArrayRef, // auth_asym_id
                    15 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.label_asym_id.as_deref()
                    }))) as ArrayRef, // label_asym_id
                    16 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.label_entity_id.as_deref()
                    }))) as ArrayRef, // label_entity_id
                    17 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.auth_seq_id.as_deref()
                    }))) as ArrayRef, // auth_seq_id
                    18 => Arc::new(Int64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.label_seq_id
                    }))) as ArrayRef, // label_seq_id
                    19 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.insertion_code.as_deref()
                    }))) as ArrayRef, // insertion_code
                    20 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.atom_index)
                    }))) as ArrayRef, // atom_index
                    21 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.atom_id.as_deref()
                    }))) as ArrayRef, // atom_id
                    22 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.record_type.as_str())
                    }))) as ArrayRef, // record_type
                    23 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.atom_name.as_str())
                    }))) as ArrayRef, // atom_name
                    24 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.residue_name.as_str())
                    }))) as ArrayRef, // residue_name
                    25 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.auth_atom_id.as_deref()
                    }))) as ArrayRef, // auth_atom_id
                    26 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.label_atom_id.as_deref()
                    }))) as ArrayRef, // label_atom_id
                    27 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.auth_comp_id.as_deref()
                    }))) as ArrayRef, // auth_comp_id
                    28 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.label_comp_id.as_deref()
                    }))) as ArrayRef, // label_comp_id
                    29 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.alt_id.as_deref()
                    }))) as ArrayRef, // alt_id
                    30 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.element.as_deref()
                    }))) as ArrayRef, // element
                    31 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.position[0])
                    }))) as ArrayRef, // x
                    32 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.position[1])
                    }))) as ArrayRef, // y
                    33 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        Some(a.position[2])
                    }))) as ArrayRef, // z
                    34 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.occupancy
                    }))) as ArrayRef, // occupancy
                    35 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.b_factor
                    }))) as ArrayRef, // b_factor
                    36 => Arc::new(Int32Array::from_iter(rows.iter().map(|r| {
                        let a = *r;
                        a.formal_charge
                    }))) as ArrayRef, // formal_charge
                    _ => unreachable!("projection validated against schema"),
                })
                .collect();
            Ok(RecordBatch::try_new_with_options(
                schema,
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows.len())),
            )?)
        }
        StructureLevel::Residue => {
            let rows = residues(entry, options);
            let columns: Vec<ArrayRef> = projection
                .iter()
                .map(|i| match *i {
                    0 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(e.source_path.as_str())
                    }))) as ArrayRef, // source_path
                    1 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(e.source_format.as_str())
                    }))) as ArrayRef, // source_format
                    2 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(e.source_index)
                    }))) as ArrayRef, // source_index
                    3 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(e.entry_index)
                    }))) as ArrayRef, // entry_index
                    4 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        e.entry_id.as_deref()
                    }))) as ArrayRef, // entry_id
                    5 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        e.entry_name.as_deref()
                    }))) as ArrayRef, // entry_name
                    6 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        e.entry_key
                    }))) as ArrayRef, // entry_key
                    7 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        e.data_block.as_deref()
                    }))) as ArrayRef, // data_block
                    8 => Arc::new(Int32Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(a.model_id)
                    }))) as ArrayRef, // model_id
                    9 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(a.model_index)
                    }))) as ArrayRef, // model_index
                    10 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(a.chain_index)
                    }))) as ArrayRef, // chain_index
                    11 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(a.segment_index)
                    }))) as ArrayRef, // segment_index
                    12 => Arc::new(UInt64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(a.residue_index)
                    }))) as ArrayRef, // residue_index
                    13 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        a.chain_id()
                    }))) as ArrayRef, // chain_id
                    14 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        a.auth_asym_id.as_deref()
                    }))) as ArrayRef, // auth_asym_id
                    15 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        a.label_asym_id.as_deref()
                    }))) as ArrayRef, // label_asym_id
                    16 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        a.label_entity_id.as_deref()
                    }))) as ArrayRef, // label_entity_id
                    17 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        a.auth_seq_id.as_deref()
                    }))) as ArrayRef, // auth_seq_id
                    18 => Arc::new(Int64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        a.label_seq_id
                    }))) as ArrayRef, // label_seq_id
                    19 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        a.insertion_code.as_deref()
                    }))) as ArrayRef, // insertion_code
                    20 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(a.residue_name.as_str())
                    }))) as ArrayRef, // residue_name
                    21 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.parent_residue_name.as_deref()
                    }))) as ArrayRef, // parent_residue_name
                    22 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.one_letter_code.as_deref()
                    }))) as ArrayRef, // one_letter_code
                    23 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.selected_alt_id.as_deref()
                    }))) as ArrayRef, // selected_alt_id
                    24 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(r.residue_kind)
                    }))) as ArrayRef, // residue_kind
                    25 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[0].map(|p| p[0])
                    }))) as ArrayRef, // n_x
                    26 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[0].map(|p| p[1])
                    }))) as ArrayRef, // n_y
                    27 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[0].map(|p| p[2])
                    }))) as ArrayRef, // n_z
                    28 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[1].map(|p| p[0])
                    }))) as ArrayRef, // ca_x
                    29 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[1].map(|p| p[1])
                    }))) as ArrayRef, // ca_y
                    30 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[1].map(|p| p[2])
                    }))) as ArrayRef, // ca_z
                    31 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[2].map(|p| p[0])
                    }))) as ArrayRef, // c_x
                    32 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[2].map(|p| p[1])
                    }))) as ArrayRef, // c_y
                    33 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[2].map(|p| p[2])
                    }))) as ArrayRef, // c_z
                    34 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[3].map(|p| p[0])
                    }))) as ArrayRef, // o_x
                    35 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[3].map(|p| p[1])
                    }))) as ArrayRef, // o_y
                    36 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.backbone[3].map(|p| p[2])
                    }))) as ArrayRef, // o_z
                    37 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.angles[0]
                    }))) as ArrayRef, // phi_deg
                    38 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.angles[1]
                    }))) as ArrayRef, // psi_deg
                    39 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.angles[2]
                    }))) as ArrayRef, // omega_deg
                    40 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.angles[3]
                    }))) as ArrayRef, // angle_n_ca_c_deg
                    41 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.angles[4]
                    }))) as ArrayRef, // angle_ca_c_n_deg
                    42 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.angles[5]
                    }))) as ArrayRef, // angle_c_n_ca_deg
                    43 => Arc::new(BooleanArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(r.peptide_link_prev)
                    }))) as ArrayRef, // peptide_link_prev
                    44 => Arc::new(BooleanArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(r.peptide_link_next)
                    }))) as ArrayRef, // peptide_link_next
                    45 => Arc::new(BooleanArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(r.backbone_complete)
                    }))) as ArrayRef, // backbone_complete
                    46 => Arc::new(Float64Array::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        r.ca_b_factor
                    }))) as ArrayRef, // ca_b_factor
                    47 => Arc::new(StringArray::from_iter(rows.iter().map(|r| {
                        let a = &r.atom;
                        Some(r.geometry_status)
                    }))) as ArrayRef, // geometry_status
                    _ => unreachable!("projection validated against schema"),
                })
                .collect();
            Ok(RecordBatch::try_new_with_options(
                schema,
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows.len())),
            )?)
        }
    }
}
