use crate::{
    error,
    options::{ModelSelection, StructureOptions},
};
use datafusion::common::Result;
use std::collections::{HashMap, HashSet};
pub type Point = [f64; 3];
/// Raw author and standardized identifiers are deliberately separate.
#[derive(Clone, Debug, Default)]
pub struct Atom {
    pub atom_index: u64,
    pub atom_id: Option<String>,
    pub record_type: String,
    pub model_id: i32,
    pub model_index: u64,
    pub chain_index: u64,
    pub segment_index: u64,
    pub residue_index: u64,
    pub auth_asym_id: Option<String>,
    pub label_asym_id: Option<String>,
    pub label_entity_id: Option<String>,
    pub auth_seq_id: Option<String>,
    pub label_seq_id: Option<i64>,
    pub insertion_code: Option<String>,
    pub atom_name: String,
    pub residue_name: String,
    pub auth_atom_id: Option<String>,
    pub label_atom_id: Option<String>,
    pub auth_comp_id: Option<String>,
    pub label_comp_id: Option<String>,
    pub alt_id: Option<String>,
    pub element: Option<String>,
    pub position: Point,
    pub occupancy: Option<f64>,
    pub b_factor: Option<f64>,
    pub formal_charge: Option<i32>,
    pub parent_residue_name: Option<String>,
    pub peptide: bool,
}
impl Atom {
    pub fn chain_id(&self) -> Option<&str> {
        self.auth_asym_id
            .as_deref()
            .or(self.label_asym_id.as_deref())
    }
}
/// A complete coordinate-bearing entry, bounded by configured input/atom limits.
#[derive(Clone, Debug, Default)]
pub struct NormalizedEntry {
    /// Encoded bytes read, counted once for a multi-block source.
    pub encoded_bytes: usize,
    pub source_path: String,
    pub source_format: String,
    pub source_index: u64,
    pub entry_index: u64,
    pub entry_id: Option<String>,
    pub entry_name: Option<String>,
    pub entry_key: Option<u64>,
    pub data_block: Option<String>,
    pub atoms: Vec<Atom>,
}
impl NormalizedEntry {
    /// Assign encounter-order ordinals before model/conformer filtering, so keys are stable.
    pub fn normalize(&mut self, options: &StructureOptions) -> Result<()> {
        if self.atoms.len() > options.max_atoms {
            return Err(error("atom count exceeds max_atoms"));
        }
        let mut models = HashMap::new();
        let mut chains = HashMap::new();
        let mut residues = HashMap::new();
        let mut sites = HashSet::new();
        for a in &mut self.atoms {
            if !a.position.iter().all(|v| v.is_finite())
                || a.occupancy.is_some_and(|v| !v.is_finite())
                || a.b_factor.is_some_and(|v| !v.is_finite())
            {
                return Err(error(format!(
                    "atom {} has non-finite coordinates",
                    a.atom_index
                )));
            }
            let m = models.len() as u64;
            a.model_index = *models.entry(a.model_id).or_insert(m);
            let c = chains.len() as u64;
            a.chain_index = *chains
                .entry((
                    a.model_index,
                    a.auth_asym_id.clone(),
                    a.label_asym_id.clone(),
                ))
                .or_insert(c);
            let r = residues.len() as u64;
            a.residue_index = *residues
                .entry((
                    a.model_index,
                    a.chain_index,
                    a.segment_index,
                    a.auth_seq_id.clone(),
                    a.label_seq_id,
                    a.insertion_code.clone(),
                ))
                .or_insert(r);
            if !sites.insert((
                a.residue_index,
                a.residue_name.clone(),
                a.atom_name.clone(),
                a.alt_id.clone(),
            )) {
                return Err(error(format!(
                    "duplicate atom site at atom {}",
                    a.atom_index
                )));
            }
        }
        self.atoms.retain(|a| match options.model {
            ModelSelection::All => true,
            ModelSelection::First => a.model_index == 0,
            ModelSelection::Id(id) => a.model_id == id,
        });
        Ok(())
    }
}
