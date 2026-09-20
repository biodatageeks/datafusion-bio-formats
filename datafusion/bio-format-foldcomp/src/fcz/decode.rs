//! Full atom mapping; reconstruction algorithms are adapted from Foldcomp (MIT).
//! See LICENSE-FOLDCOMP.
use super::{
    backbone, discretize::Discretizer, geometry::place, header::EncodedEntry, tables::TABLES,
};
use datafusion::common::Result;
use datafusion_bio_format_structure::{
    StructureOptions, error,
    model::{Atom, NormalizedEntry},
};

pub(crate) fn decode(data: &[u8], options: &StructureOptions) -> Result<NormalizedEntry> {
    let encoded = EncodedEntry::parse(data, options.max_atoms)?;
    let chain = encoded.chain()?;
    let mut entry = NormalizedEntry {
        entry_id: Some(encoded.title()?.into()),
        source_format: "foldcomp".into(),
        atoms: Vec::with_capacity(encoded.reconstructed_atoms()),
        ..Default::default()
    };
    let backbone = backbone::reconstruct(&encoded);
    let mut side_offset = 0;
    for (index, record) in encoded.backbone().iter().enumerate() {
        let table = TABLES[usize::from(record.residue.code()).min(20)];
        let mut positions = Vec::with_capacity(record.residue.atom_count());
        positions.extend_from_slice(&backbone[index * 3..index * 3 + 3]);
        for atom in table {
            let torsion =
                Discretizer::sidechain().restore(u16::from(encoded.sidechain()[side_offset]));
            side_offset += 1;
            positions.push(place(
                atom.previous.map(|i| positions[i]),
                atom.length,
                atom.angle,
                torsion,
            ));
        }
        let b_factor = encoded
            .bfactor_discretizer()
            .restore(u16::from(encoded.bfactors()[index]));
        let residue_id = usize::from(encoded.header().first_residue) + index;
        for (name, position) in ["N", "CA", "C"]
            .into_iter()
            .chain(table.iter().map(|atom| atom.name))
            .zip(positions)
        {
            append(
                &mut entry,
                &encoded,
                chain,
                name,
                record.residue.name(),
                residue_id,
                position,
                b_factor,
            )?;
        }
    }
    if encoded.has_oxt() {
        let record = encoded
            .backbone()
            .last()
            .expect("validated nonempty backbone");
        let b_factor = encoded.bfactor_discretizer().restore(u16::from(
            *encoded.bfactors().last().expect("validated B-factor count"),
        ));
        // The legacy OXT uses the residue count even when numbering is shifted.
        append(
            &mut entry,
            &encoded,
            chain,
            "OXT",
            record.residue.name(),
            usize::from(encoded.header().residue_count),
            encoded.oxt(),
            b_factor,
        )?;
    }
    entry.normalize(options)?;
    Ok(entry)
}

#[allow(clippy::too_many_arguments)]
fn append(
    entry: &mut NormalizedEntry,
    encoded: &EncodedEntry<'_>,
    chain: &str,
    name: &str,
    residue: &str,
    residue_id: usize,
    position: [f32; 3],
    b_factor: f32,
) -> Result<()> {
    if !position.into_iter().all(f32::is_finite) || !b_factor.is_finite() {
        return Err(error("non-finite FCZ output"));
    }
    let index = entry.atoms.len();
    entry.atoms.push(Atom {
        atom_index: index as u64,
        atom_id: Some((usize::from(encoded.header().first_atom) + index).to_string()),
        record_type: "ATOM".into(),
        model_id: 1,
        auth_asym_id: Some(chain.into()),
        auth_seq_id: Some(residue_id.to_string()),
        auth_atom_id: Some(name.into()),
        auth_comp_id: Some(residue.into()),
        atom_name: name.into(),
        residue_name: residue.into(),
        position: position.map(f64::from),
        b_factor: Some(f64::from(b_factor)),
        peptide: true,
        ..Default::default()
    });
    Ok(())
}
