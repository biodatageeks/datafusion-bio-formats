//! Reproduce legacy array mapping from an external reference process, without FFI.
use crate::reference_process::{query, text};
use datafusion::common::Result;
use datafusion_bio_format_structure::{
    StructureOptions,
    model::{Atom, NormalizedEntry},
};

pub fn decode(data: &[u8], options: &StructureOptions) -> Result<NormalizedEntry> {
    let raw = query("fcz", data, options.max_atoms)?;
    let mut entry = NormalizedEntry {
        entry_id: Some(text(&raw["decoded_title_hex"])?),
        source_format: "foldcomp".into(),
        ..Default::default()
    };
    for (index, row) in raw["atoms"].as_array().unwrap().iter().enumerate() {
        let name = text(&row[0])?;
        let residue = text(&row[1])?;
        let float =
            |i: usize| f64::from(f32::from_bits(row[i].as_u64().unwrap().try_into().unwrap()));
        entry.atoms.push(Atom {
            atom_index: index as u64,
            atom_id: Some(row[3].to_string()),
            record_type: "ATOM".into(),
            model_id: 1,
            auth_asym_id: Some(text(&row[2])?),
            auth_seq_id: Some(row[4].to_string()),
            auth_atom_id: Some(name.clone()),
            auth_comp_id: Some(residue.clone()),
            atom_name: name,
            residue_name: residue,
            position: [float(5), float(6), float(7)],
            b_factor: Some(float(8)),
            peptide: true,
            ..Default::default()
        });
    }
    entry.normalize(options)?;
    Ok(entry)
}
