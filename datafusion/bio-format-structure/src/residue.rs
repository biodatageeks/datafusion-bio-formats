//! Coherent conformer selection and neighbor-aware peptide geometry.
use crate::{
    geometry::{bond_angle, dihedral, distance},
    model::{Atom, NormalizedEntry, Point},
    options::{AltlocSelection, StructureOptions},
};
use std::collections::{BTreeMap, BTreeSet};
/// Pinned standard/modified peptide mapping; unrecognized peptide components map to X.
pub fn amino_acid(name: &str) -> Option<&'static str> {
    Some(match name {
        "ALA" => "A",
        "ARG" => "R",
        "ASN" => "N",
        "ASP" => "D",
        "CYS" => "C",
        "GLN" => "Q",
        "GLU" => "E",
        "GLY" => "G",
        "HIS" => "H",
        "ILE" => "I",
        "LEU" => "L",
        "LYS" => "K",
        "MET" | "MSE" => "M",
        "PHE" => "F",
        "PRO" => "P",
        "SER" => "S",
        "THR" => "T",
        "TRP" => "W",
        "TYR" => "Y",
        "VAL" => "V",
        "SEC" => "U",
        "PYL" => "O",
        _ => return None,
    })
}
#[derive(Clone, Debug)]
pub struct Residue {
    pub atom: Atom,
    pub selected_alt_id: Option<String>,
    pub parent_residue_name: Option<String>,
    pub one_letter_code: Option<String>,
    pub residue_kind: &'static str,
    pub backbone: [Option<Point>; 4],
    pub angles: [Option<f64>; 6],
    pub peptide_link_prev: bool,
    pub peptide_link_next: bool,
    pub backbone_complete: bool,
    pub ca_b_factor: Option<f64>,
    pub geometry_status: &'static str,
}
struct Candidate<'a> {
    atoms: Vec<&'a Atom>,
    complete: usize,
    occupancy: f64,
    alt: &'a str,
    component: &'a str,
}
impl Candidate<'_> {
    fn better_than(&self, other: &Self) -> bool {
        self.complete
            .cmp(&other.complete)
            .then_with(|| self.occupancy.total_cmp(&other.occupancy))
            .then_with(|| (self.alt == "A").cmp(&(other.alt == "A")))
            .then_with(|| other.alt.cmp(self.alt))
            .then_with(|| other.component.cmp(self.component))
            .is_gt()
    }
}
fn candidates<'a>(atoms: &[&'a Atom], options: &StructureOptions) -> Vec<&'a Atom> {
    let groups: BTreeSet<_> = atoms
        .iter()
        .map(|a| (a.residue_name.as_str(), a.alt_id.as_deref()))
        .collect();
    let mut best: Option<Candidate<'_>> = None;
    for (comp, alt) in groups.iter().copied() {
        if alt.is_none() && groups.iter().any(|(c, a)| *c == comp && a.is_some()) {
            continue;
        }
        if let AltlocSelection::Id(wanted) = &options.altloc
            && alt.is_some_and(|a| a != wanted)
        {
            continue;
        }
        // A named site supersedes a shared blank site of the same atom name.
        let mut picked = BTreeMap::<&str, &Atom>::new();
        for a in atoms.iter().copied().filter(|a| {
            a.residue_name == comp && (a.alt_id.is_none() || a.alt_id.as_deref() == alt)
        }) {
            if a.alt_id.is_some() || !picked.contains_key(a.atom_name.as_str()) {
                picked.insert(&a.atom_name, a);
            }
        }
        let backbone: Vec<_> = picked
            .values()
            .filter(|a| matches!(a.atom_name.as_str(), "N" | "CA" | "C"))
            .collect();
        let complete = backbone.len();
        let occupancy = backbone
            .iter()
            .map(|a| a.occupancy.unwrap_or(0.) / complete.max(1) as f64)
            .sum();
        let mut atoms: Vec<_> = picked.into_values().collect();
        atoms.sort_by_key(|a| a.atom_index);
        let candidate = Candidate {
            atoms,
            complete,
            occupancy,
            alt: alt.unwrap_or(""),
            component: comp,
        };
        if best.as_ref().is_none_or(|b| candidate.better_than(b)) {
            best = Some(candidate);
        }
    }
    best.map(|v| v.atoms).unwrap_or_default()
}
fn groups(entry: &NormalizedEntry) -> BTreeMap<u64, Vec<&Atom>> {
    let mut g = BTreeMap::<_, Vec<_>>::new();
    for a in &entry.atoms {
        g.entry(a.residue_index).or_default().push(a);
    }
    g
}
pub fn selected_atoms<'a>(entry: &'a NormalizedEntry, options: &StructureOptions) -> Vec<&'a Atom> {
    if options.altloc == AltlocSelection::All {
        return entry.atoms.iter().collect();
    }
    let mut result: Vec<_> = groups(entry)
        .values()
        .flat_map(|g| candidates(g, options))
        .collect();
    result.sort_by_key(|a| a.atom_index);
    result
}
pub fn residues(entry: &NormalizedEntry, options: &StructureOptions) -> Vec<Residue> {
    let mut result = Vec::new();
    for atoms in groups(entry).values() {
        let picked = candidates(atoms, options);
        let Some(first) = picked.first() else {
            continue;
        };
        let get = |name: &str| picked.iter().find(|a| a.atom_name == name).copied();
        let peptide = first.peptide;
        let backbone = ["N", "CA", "C", "O"].map(|name| get(name).map(|a| a.position));
        let parent = first
            .parent_residue_name
            .clone()
            .or_else(|| (first.residue_name == "MSE").then(|| "MET".into()));
        let complete = backbone[..3].iter().all(Option::is_some);
        result.push(Residue {
            atom: (*first).clone(),
            selected_alt_id: picked.iter().find_map(|a| a.alt_id.clone()),
            one_letter_code: peptide.then(|| {
                amino_acid(parent.as_deref().unwrap_or(&first.residue_name))
                    .unwrap_or("X")
                    .to_owned()
            }),
            parent_residue_name: parent,
            residue_kind: if peptide {
                "peptide"
            } else if matches!(first.residue_name.as_str(), "HOH" | "WAT" | "DOD") {
                "water"
            } else {
                "non_peptide"
            },
            backbone,
            angles: [None; 6],
            peptide_link_prev: false,
            peptide_link_next: false,
            backbone_complete: complete,
            ca_b_factor: get("CA").and_then(|a| a.b_factor),
            geometry_status: if !peptide {
                "non_peptide"
            } else if !complete {
                "incomplete_backbone"
            } else {
                "terminus_or_break"
            },
        });
    }
    // Standardized sequence numbers order polymer sites; author-only PDB retains encounter order.
    result.sort_by_key(|r| {
        (
            r.atom.model_index,
            r.atom.chain_index,
            r.atom.segment_index,
            r.atom.label_seq_id.unwrap_or(r.atom.residue_index as i64),
            r.atom.residue_index,
        )
    });
    for i in 0..result.len().saturating_sub(1) {
        let a = &result[i];
        let b = &result[i + 1];
        let compatible = a.atom.peptide
            && b.atom.peptide
            && a.atom.model_index == b.atom.model_index
            && a.atom.chain_index == b.atom.chain_index
            && a.atom.segment_index == b.atom.segment_index
            && a.atom.label_entity_id == b.atom.label_entity_id
            && match (a.atom.label_seq_id, b.atom.label_seq_id) {
                (Some(x), Some(y)) => x.checked_add(1) == Some(y),
                _ => true,
            }
            && match (&a.selected_alt_id, &b.selected_alt_id) {
                (Some(x), Some(y)) => x == y,
                _ => true,
            };
        let linked = compatible
            && matches!((a.backbone[2],b.backbone[0]),(Some(c),Some(n)) if distance(c,n)>1e-12&&distance(c,n)<=options.max_peptide_bond);
        result[i].peptide_link_next = linked;
        result[i + 1].peptide_link_prev = linked;
    }
    for i in 0..result.len() {
        if !result[i].atom.peptide {
            continue;
        }
        let [n, ca, c, _] = result[i].backbone;
        let prev = if result[i].peptide_link_prev {
            result[i - 1].backbone[2]
        } else {
            None
        };
        let (next_n, next_ca) = if result[i].peptide_link_next {
            (result[i + 1].backbone[0], result[i + 1].backbone[1])
        } else {
            (None, None)
        };
        let torsion =
            |a, b, c, d| Some([a?, b?, c?, d?]).and_then(|[a, b, c, d]| dihedral(a, b, c, d));
        let angle = |a, b, c| Some([a?, b?, c?]).and_then(|[a, b, c]| bond_angle(a, b, c));
        result[i].angles = [
            torsion(prev, n, ca, c),
            torsion(n, ca, c, next_n),
            torsion(ca, c, next_n, next_ca),
            angle(n, ca, c),
            angle(ca, c, next_n),
            angle(prev, n, ca),
        ];
        if result[i].backbone_complete {
            result[i].geometry_status = if result[i].angles[3].is_none()
                || (prev.is_some() && result[i].angles[0].is_none())
                || (next_n.is_some() && result[i].angles[1].is_none())
                || (next_n.is_some() && next_ca.is_some() && result[i].angles[2].is_none())
            {
                "degenerate"
            } else if result[i].peptide_link_prev && result[i].peptide_link_next {
                "complete"
            } else {
                "terminus_or_break"
            };
        }
    }
    result.retain(|r| options.include_non_peptide || r.atom.peptide);
    result
}
