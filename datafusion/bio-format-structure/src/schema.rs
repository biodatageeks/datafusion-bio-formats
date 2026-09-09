//! Stable version 1 schemas for both text and Foldcomp providers.
use crate::options::{StructureLevel, StructureOptions};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::{collections::HashMap, sync::Arc};
pub fn schema(options: &StructureOptions) -> SchemaRef {
    let mut fields = vec![
        Field::new("source_path", DataType::Utf8, false),
        Field::new("source_format", DataType::Utf8, false),
        Field::new("source_index", DataType::UInt64, false),
        Field::new("entry_index", DataType::UInt64, false),
        Field::new("entry_id", DataType::Utf8, true),
        Field::new("entry_name", DataType::Utf8, true),
        Field::new("entry_key", DataType::UInt64, true),
        Field::new("data_block", DataType::Utf8, true),
        Field::new("model_id", DataType::Int32, false),
        Field::new("model_index", DataType::UInt64, false),
        Field::new("chain_index", DataType::UInt64, false),
        Field::new("segment_index", DataType::UInt64, false),
        Field::new("residue_index", DataType::UInt64, false),
        Field::new("chain_id", DataType::Utf8, true),
        Field::new("auth_asym_id", DataType::Utf8, true),
        Field::new("label_asym_id", DataType::Utf8, true),
        Field::new("label_entity_id", DataType::Utf8, true),
        Field::new("auth_seq_id", DataType::Utf8, true),
        Field::new("label_seq_id", DataType::Int64, true),
        Field::new("insertion_code", DataType::Utf8, true),
    ];
    fields.extend(match options.level {
        StructureLevel::Atom => vec![
            Field::new("atom_index", DataType::UInt64, false),
            Field::new("atom_id", DataType::Utf8, true),
            Field::new("record_type", DataType::Utf8, false),
            Field::new("atom_name", DataType::Utf8, false),
            Field::new("residue_name", DataType::Utf8, false),
            Field::new("auth_atom_id", DataType::Utf8, true),
            Field::new("label_atom_id", DataType::Utf8, true),
            Field::new("auth_comp_id", DataType::Utf8, true),
            Field::new("label_comp_id", DataType::Utf8, true),
            Field::new("alt_id", DataType::Utf8, true),
            Field::new("element", DataType::Utf8, true),
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("z", DataType::Float64, false),
            Field::new("occupancy", DataType::Float64, true),
            Field::new("b_factor", DataType::Float64, true),
            Field::new("formal_charge", DataType::Int32, true),
        ],
        StructureLevel::Residue => vec![
            Field::new("residue_name", DataType::Utf8, false),
            Field::new("parent_residue_name", DataType::Utf8, true),
            Field::new("one_letter_code", DataType::Utf8, true),
            Field::new("selected_alt_id", DataType::Utf8, true),
            Field::new("residue_kind", DataType::Utf8, false),
            Field::new("n_x", DataType::Float64, true),
            Field::new("n_y", DataType::Float64, true),
            Field::new("n_z", DataType::Float64, true),
            Field::new("ca_x", DataType::Float64, true),
            Field::new("ca_y", DataType::Float64, true),
            Field::new("ca_z", DataType::Float64, true),
            Field::new("c_x", DataType::Float64, true),
            Field::new("c_y", DataType::Float64, true),
            Field::new("c_z", DataType::Float64, true),
            Field::new("o_x", DataType::Float64, true),
            Field::new("o_y", DataType::Float64, true),
            Field::new("o_z", DataType::Float64, true),
            Field::new("phi_deg", DataType::Float64, true),
            Field::new("psi_deg", DataType::Float64, true),
            Field::new("omega_deg", DataType::Float64, true),
            Field::new("angle_n_ca_c_deg", DataType::Float64, true),
            Field::new("angle_ca_c_n_deg", DataType::Float64, true),
            Field::new("angle_c_n_ca_deg", DataType::Float64, true),
            Field::new("peptide_link_prev", DataType::Boolean, false),
            Field::new("peptide_link_next", DataType::Boolean, false),
            Field::new("backbone_complete", DataType::Boolean, false),
            Field::new("ca_b_factor", DataType::Float64, true),
            Field::new("geometry_status", DataType::Utf8, false),
        ],
    });
    let metadata = HashMap::from([
        ("bio.structure.schema_version".into(), "1".into()),
        ("bio.structure.coordinate_unit".into(), "angstrom".into()),
        ("bio.structure.angle_unit".into(), "degree".into()),
        ("bio.structure.omega_convention".into(), "outgoing".into()),
        (
            "bio.structure.level".into(),
            format!("{:?}", options.level).to_lowercase(),
        ),
        (
            "bio.structure.altloc".into(),
            format!(
                "{:?}",
                if options.level == StructureLevel::Residue
                    && options.altloc == crate::options::AltlocSelection::All
                {
                    &crate::options::AltlocSelection::BestBackbone
                } else {
                    &options.altloc
                }
            ),
        ),
        (
            "bio.structure.max_peptide_bond".into(),
            options.max_peptide_bond.to_string(),
        ),
    ]);
    Arc::new(Schema::new_with_metadata(fields, metadata))
}
