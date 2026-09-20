use crate::error;
use datafusion::common::Result;
/// Output granularity. Atom preserves every site; residue selects one conformer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StructureLevel {
    #[default]
    Atom,
    Residue,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ModelSelection {
    #[default]
    All,
    First,
    Id(i32),
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum AltlocSelection {
    #[default]
    All,
    BestBackbone,
    Id(String),
}
/// Immutable scan policies; coordinates are Angstroms and angles are degrees.
#[derive(Clone, Debug)]
pub struct StructureOptions {
    pub level: StructureLevel,
    pub model: ModelSelection,
    /// Atom level emits every site for `All`. Residue level must pick one coherent conformer,
    /// so `All` behaves as `BestBackbone` there and the schema metadata reports `BestBackbone`.
    pub altloc: AltlocSelection,
    pub include_non_peptide: bool,
    pub max_peptide_bond: f64,
    pub max_input_bytes: usize,
    pub max_decoded_bytes: usize,
    pub max_atoms: usize,
}
impl Default for StructureOptions {
    fn default() -> Self {
        Self {
            level: StructureLevel::Atom,
            model: ModelSelection::All,
            altloc: AltlocSelection::All,
            include_non_peptide: false,
            max_peptide_bond: 1.8,
            max_input_bytes: 256 * 1024 * 1024,
            max_decoded_bytes: 512 * 1024 * 1024,
            max_atoms: 5_000_000,
        }
    }
}
impl StructureOptions {
    pub fn validate(&self) -> Result<()> {
        if !self.max_peptide_bond.is_finite() || self.max_peptide_bond <= 0.0 {
            return Err(error("max_peptide_bond must be finite and positive"));
        }
        if self.max_atoms == 0 || self.max_input_bytes == 0 || self.max_decoded_bytes == 0 {
            return Err(error("structure size limits must be positive"));
        }
        if matches!(&self.altloc, AltlocSelection::Id(s) if s.is_empty()) {
            return Err(error("alternate ID cannot be empty"));
        }
        Ok(())
    }
}
