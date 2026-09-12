//! Raw mmCIF categories preserve label/auth namespaces and quoted missing tokens.
use crate::{
    error,
    model::{Atom, NormalizedEntry},
    native_cif::{CategoryBlock, Document},
    options::StructureOptions,
    residue::amino_acid,
};
use datafusion::common::Result;
use std::collections::{HashMap, HashSet};
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
#[derive(Default)]
struct Component<'a> {
    parent: Option<&'a str>,
    peptide: bool,
}
/// Per-block lookup tables built once, so each atom row resolves its metadata in O(1).
#[derive(Default)]
struct BlockMetadata<'a> {
    asym_entity: HashMap<&'a str, &'a str>,
    polypeptide_entities: HashSet<&'a str>,
    components: HashMap<&'a str, Component<'a>>,
    author_site_parent: HashMap<AuthorSite<'a>, &'a str>,
    label_site_parent: HashMap<LabelSite<'a>, &'a str>,
}
type AuthorSite<'a> = (
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
);
type LabelSite<'a> = (Option<&'a str>, Option<&'a str>, Option<&'a str>);
impl<'a> BlockMetadata<'a> {
    fn index(b: &'a CategoryBlock<'a>) -> Self {
        let mut m = Self::default();
        let rows = |tag: &str| b.columns.get(tag).map_or(0, Vec::len);
        for i in 0..rows("_struct_asym.id") {
            if let (Some(id), Some(entity)) = (
                value(b, "_struct_asym.id", i),
                value(b, "_struct_asym.entity_id", i),
            ) {
                m.asym_entity.entry(id).or_insert(entity);
            }
        }
        for i in 0..rows("_entity_poly.entity_id") {
            if let Some(entity) = value(b, "_entity_poly.entity_id", i)
                && value(b, "_entity_poly.type", i)
                    .is_some_and(|s| s.to_ascii_lowercase().starts_with("polypeptide"))
            {
                m.polypeptide_entities.insert(entity);
            }
        }
        for i in 0..rows("_chem_comp.id") {
            if let Some(id) = value(b, "_chem_comp.id", i) {
                m.components.entry(id).or_insert(Component {
                    parent: value(b, "_chem_comp.mon_nstd_parent_comp_id", i),
                    peptide: value(b, "_chem_comp.type", i)
                        .is_some_and(|s| s.to_ascii_lowercase().contains("peptide")),
                });
            }
        }
        // First matching row wins, as a linear scan would.
        for i in 0..rows("_pdbx_struct_mod_residue.parent_comp_id") {
            let Some(parent) = value(b, "_pdbx_struct_mod_residue.parent_comp_id", i) else {
                continue;
            };
            let auth_seq = value(b, "_pdbx_struct_mod_residue.auth_seq_id", i);
            if auth_seq.is_some() {
                m.author_site_parent
                    .entry((
                        auth_seq,
                        value(b, "_pdbx_struct_mod_residue.auth_asym_id", i),
                        value(b, "_pdbx_struct_mod_residue.auth_comp_id", i),
                        value(b, "_pdbx_struct_mod_residue.pdb_ins_code", i),
                    ))
                    .or_insert(parent);
            }
            let label_seq = value(b, "_pdbx_struct_mod_residue.label_seq_id", i);
            if label_seq.is_some() {
                m.label_site_parent
                    .entry((
                        label_seq,
                        value(b, "_pdbx_struct_mod_residue.label_asym_id", i),
                        value(b, "_pdbx_struct_mod_residue.label_comp_id", i),
                    ))
                    .or_insert(parent);
            }
        }
        m
    }
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
    let metadata = BlockMetadata::index(b);
    for row in 0..n {
        let result = (|| -> Result<Atom> {
            let get = |tag: &str| value(b, tag, row);
            let owned = |tag: &str| get(tag).map(str::to_owned);
            // Normalized names prefer the standardized label namespace, which residue assembly
            // and component lookups match against; author spellings stay in their own columns.
            let atom_name = get("_atom_site.label_atom_id")
                .or_else(|| get("_atom_site.auth_atom_id"))
                .ok_or_else(|| error("missing atom name"))?
                .to_owned();
            let comp = get("_atom_site.label_comp_id")
                .or_else(|| get("_atom_site.auth_comp_id"))
                .ok_or_else(|| error("missing residue name"))?
                .to_owned();
            let entity = get("_atom_site.label_entity_id").or_else(|| {
                let asym = get("_atom_site.label_asym_id")?;
                metadata.asym_entity.get(asym).copied()
            });
            let peptide_entity = entity.is_some_and(|e| metadata.polypeptide_entities.contains(e));
            let component = metadata.components.get(comp.as_str());
            let component_parent = component.and_then(|c| c.parent).map(str::to_owned);
            // wwPDB site-specific modifications can supply a parent absent from chem_comp.
            let site_parent = metadata
                .author_site_parent
                .get(&(
                    get("_atom_site.auth_seq_id"),
                    get("_atom_site.auth_asym_id"),
                    get("_atom_site.auth_comp_id"),
                    get("_atom_site.pdbx_pdb_ins_code"),
                ))
                .or_else(|| {
                    metadata.label_site_parent.get(&(
                        get("_atom_site.label_seq_id"),
                        get("_atom_site.label_asym_id"),
                        get("_atom_site.label_comp_id"),
                    ))
                })
                .map(|parent| (*parent).to_owned());
            let parent = site_parent.or(component_parent);
            let peptide_comp = component.is_some_and(|c| c.peptide);
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
