//! Raw mmCIF categories preserve label/auth namespaces and quoted missing tokens.
use crate::{
    error,
    model::{Atom, NormalizedEntry},
    native_cif::{CategoryBlock, Document},
    options::StructureOptions,
    residue::amino_acid,
};
use datafusion::common::Result;
fn value<'a>(b: &'a CategoryBlock<'a>, tag: &str, row: usize) -> Option<&'a str> {
    b.columns
        .get(tag)
        .and_then(|c| c.get(row))
        .copied()
        .flatten()
}
fn number<T: std::str::FromStr>(s: Option<&str>, tag: &str) -> Result<Option<T>> {
    s.map(|v| {
        v.parse()
            .map_err(|_| error(format!("invalid {tag}: {v:?}")))
    })
    .transpose()
}
fn float(s: Option<&str>, tag: &str) -> Result<Option<f64>> {
    let v = number::<f64>(s, tag)?;
    if v.is_some_and(|x| !x.is_finite()) {
        return Err(error(format!("non-finite {tag}")));
    }
    Ok(v)
}
/// A parsed document whose data blocks decode one at a time, so a multi-block source never
/// holds more than one normalized entry alongside the native document.
pub struct Blocks(Document);
impl Blocks {
    pub fn parse(data: &[u8]) -> Result<Self> {
        Document::parse(data).map(Self)
    }
    pub fn len(&self) -> usize {
        self.0.block_count()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Decode block `index`; `None` when it carries no `atom_site` category.
    pub fn entry(
        &self,
        index: usize,
        options: &StructureOptions,
    ) -> Result<Option<NormalizedEntry>> {
        let b = self.0.block(index)?;
        let result = decode(&b, index, options);
        result.map_err(|e| error(format!("mmCIF block {:?}: {e}", b.name)))
    }
}
/// Decode every block eagerly; convenient for tests and single-block callers.
pub fn parse(data: &[u8], options: &StructureOptions) -> Result<Vec<NormalizedEntry>> {
    let blocks = Blocks::parse(data)?;
    let entries = (0..blocks.len())
        .filter_map(|i| blocks.entry(i, options).transpose())
        .collect::<Result<Vec<_>>>()?;
    if entries.is_empty() {
        return Err(error("mmCIF contains no atom_site category"));
    }
    Ok(entries)
}
fn decode(
    b: &CategoryBlock<'_>,
    block_index: usize,
    options: &StructureOptions,
) -> Result<Option<NormalizedEntry>> {
    let atom_columns: Vec<_> = b
        .columns
        .iter()
        .filter(|(k, _)| k.starts_with("_atom_site."))
        .collect();
    if atom_columns.is_empty() {
        return Ok(None);
    }
    let n = atom_columns[0].1.len();
    if atom_columns.iter().any(|(_, c)| c.len() != n) {
        return Err(error("inconsistent atom_site column lengths"));
    }
    if n > options.max_atoms {
        return Err(error("atom count exceeds max_atoms"));
    }
    let mut entry = NormalizedEntry {
        entry_index: block_index as u64,
        data_block: Some(b.name.to_owned()),
        entry_id: value(b, "_entry.id", 0).map(str::to_owned),
        ..Default::default()
    };
    for row in 0..n {
        let result = (|| -> Result<Atom> {
            let get = |tag: &str| value(b, tag, row);
            let owned = |tag: &str| get(tag).map(str::to_owned);
            let atom_name = get("_atom_site.auth_atom_id")
                .or_else(|| get("_atom_site.label_atom_id"))
                .ok_or_else(|| error("missing atom name"))?
                .to_owned();
            let comp = get("_atom_site.auth_comp_id")
                .or_else(|| get("_atom_site.label_comp_id"))
                .ok_or_else(|| error("missing residue name"))?
                .to_owned();
            let entity = get("_atom_site.label_entity_id").or_else(|| {
                let asym = get("_atom_site.label_asym_id")?;
                let row = b
                    .columns
                    .get("_struct_asym.id")?
                    .iter()
                    .position(|id| *id == Some(asym))?;
                value(b, "_struct_asym.entity_id", row)
            });
            let peptide_entity = b
                .columns
                .get("_entity_poly.entity_id")
                .and_then(|c| c.iter().position(|v| v.is_some() && *v == entity))
                .and_then(|r| value(b, "_entity_poly.type", r))
                .is_some_and(|s| s.to_ascii_lowercase().starts_with("polypeptide"));
            let comp_row = b
                .columns
                .get("_chem_comp.id")
                .and_then(|c| c.iter().position(|v| *v == Some(comp.as_str())));
            let component_parent = comp_row
                .and_then(|r| value(b, "_chem_comp.mon_nstd_parent_comp_id", r))
                .map(str::to_owned);
            // wwPDB site-specific modifications can supply a parent absent from chem_comp.
            let site_parent = b
                .columns
                .get("_pdbx_struct_mod_residue.parent_comp_id")
                .and_then(|parents| {
                    parents.iter().enumerate().find_map(|(i, parent)| {
                        let auth_seq = value(b, "_pdbx_struct_mod_residue.auth_seq_id", i);
                        let same_author = auth_seq.is_some()
                            && auth_seq == get("_atom_site.auth_seq_id")
                            && value(b, "_pdbx_struct_mod_residue.auth_asym_id", i)
                                == get("_atom_site.auth_asym_id")
                            && value(b, "_pdbx_struct_mod_residue.auth_comp_id", i)
                                == get("_atom_site.auth_comp_id")
                            && value(b, "_pdbx_struct_mod_residue.pdb_ins_code", i)
                                == get("_atom_site.pdbx_pdb_ins_code");
                        let label_seq = value(b, "_pdbx_struct_mod_residue.label_seq_id", i);
                        let same_label = label_seq.is_some()
                            && label_seq == get("_atom_site.label_seq_id")
                            && value(b, "_pdbx_struct_mod_residue.label_asym_id", i)
                                == get("_atom_site.label_asym_id")
                            && value(b, "_pdbx_struct_mod_residue.label_comp_id", i)
                                == get("_atom_site.label_comp_id");
                        if same_author || same_label {
                            *parent
                        } else {
                            None
                        }
                    })
                })
                .map(str::to_owned);
            let parent = site_parent.or(component_parent);
            let peptide_comp = comp_row
                .and_then(|r| value(b, "_chem_comp.type", r))
                .is_some_and(|s| s.to_ascii_lowercase().contains("peptide"));
            let coord =
                |tag: &str| float(get(tag), tag)?.ok_or_else(|| error(format!("missing {tag}")));
            Ok(Atom {
                atom_index: row as u64,
                atom_id: owned("_atom_site.id"),
                record_type: get("_atom_site.group_pdb").unwrap_or("ATOM").to_owned(),
                model_id: number(get("_atom_site.pdbx_pdb_model_num"), "model")?.unwrap_or(1),
                auth_asym_id: owned("_atom_site.auth_asym_id"),
                label_asym_id: owned("_atom_site.label_asym_id"),
                label_entity_id: entity.map(str::to_owned),
                auth_seq_id: owned("_atom_site.auth_seq_id"),
                label_seq_id: number(get("_atom_site.label_seq_id"), "label_seq_id")?,
                insertion_code: owned("_atom_site.pdbx_pdb_ins_code"),
                auth_atom_id: owned("_atom_site.auth_atom_id"),
                label_atom_id: owned("_atom_site.label_atom_id"),
                auth_comp_id: owned("_atom_site.auth_comp_id"),
                label_comp_id: owned("_atom_site.label_comp_id"),
                atom_name,
                peptide: peptide_entity
                    || peptide_comp
                    || amino_acid(parent.as_deref().unwrap_or(&comp)).is_some()
                    || comp == "UNK",
                residue_name: comp,
                parent_residue_name: parent,
                alt_id: owned("_atom_site.label_alt_id"),
                element: owned("_atom_site.type_symbol"),
                position: [
                    coord("_atom_site.cartn_x")?,
                    coord("_atom_site.cartn_y")?,
                    coord("_atom_site.cartn_z")?,
                ],
                occupancy: float(get("_atom_site.occupancy"), "occupancy")?,
                b_factor: float(get("_atom_site.b_iso_or_equiv"), "B factor")?,
                formal_charge: number(get("_atom_site.pdbx_formal_charge"), "formal charge")?,
                ..Default::default()
            })
        })();
        entry
            .atoms
            .push(result.map_err(|e| error(format!("atom row {}: {e}", row + 1)))?);
    }
    entry.normalize(options)?;
    Ok(Some(entry))
}
