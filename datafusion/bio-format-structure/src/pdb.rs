//! Fixed-column PDB records. Serial/residue IDs remain strings (including hybrid-36).
use crate::{
    error,
    model::{Atom, NormalizedEntry},
    options::StructureOptions,
    residue::amino_acid,
};
use datafusion::common::Result;
use std::collections::HashMap;
fn field(s: &str, start: usize, end: usize) -> &str {
    s.get(start..end.min(s.len())).unwrap_or("").trim()
}
fn optional(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_owned())
}
fn number(s: &str, name: &str) -> Result<f64> {
    let v: f64 = s
        .parse()
        .map_err(|_| error(format!("invalid {name}: {s:?}")))?;
    if !v.is_finite() {
        return Err(error(format!("non-finite {name}")));
    }
    Ok(v)
}
fn optional_number(s: &str, name: &str) -> Result<Option<f64>> {
    if s.is_empty() {
        Ok(None)
    } else {
        number(s, name).map(Some)
    }
}
pub fn parse(data: &str, options: &StructureOptions) -> Result<Vec<NormalizedEntry>> {
    let mut entry = NormalizedEntry::default();
    let mut model = 1;
    let mut segment = 0;
    let mut in_model = false;
    let mut explicit_models = false;
    let mut parents = HashMap::new();
    for (line_no, line) in data.lines().enumerate() {
        let result = (|| -> Result<()> {
            match field(line, 0, 6) {
                "HEADER" => entry.entry_id = optional(field(line, 62, 66)),
                "MODRES" => {
                    parents.insert(
                        (
                            field(line, 16, 17).to_owned(),
                            field(line, 18, 22).to_owned(),
                            field(line, 22, 23).to_owned(),
                            field(line, 12, 15).to_owned(),
                        ),
                        field(line, 24, 27).to_owned(),
                    );
                }
                "MODEL" => {
                    if in_model {
                        return Err(error("nested MODEL"));
                    }
                    model = field(line, 10, 14)
                        .parse()
                        .map_err(|_| error("invalid MODEL ID"))?;
                    in_model = true;
                    explicit_models = true;
                    segment = 0;
                }
                "ENDMDL" => {
                    if !in_model {
                        return Err(error("ENDMDL without MODEL"));
                    }
                    in_model = false;
                }
                "TER" => segment += 1,
                record @ ("ATOM" | "HETATM") => {
                    if !line.is_ascii() || line.len() < 54 {
                        return Err(error("short or non-ASCII atom record"));
                    }
                    if explicit_models && !in_model {
                        return Err(error("atom outside MODEL"));
                    }
                    let name = field(line, 12, 16).to_owned();
                    let comp = field(line, 17, 20).to_owned();
                    if name.is_empty() || comp.is_empty() || field(line, 22, 26).is_empty() {
                        return Err(error("missing atom/residue identity"));
                    }
                    let chain = field(line, 21, 22).to_owned();
                    let seq = field(line, 22, 26).to_owned();
                    let ins = field(line, 26, 27).to_owned();
                    let parent = parents
                        .get(&(chain.clone(), seq.clone(), ins.clone(), comp.clone()))
                        .cloned();
                    let charge = field(line, 78, 80);
                    let formal_charge = if charge.is_empty() {
                        None
                    } else {
                        let bytes = charge.as_bytes();
                        if bytes.len() != 2
                            || !bytes[0].is_ascii_digit()
                            || !matches!(bytes[1], b'+' | b'-')
                        {
                            return Err(error("invalid formal charge"));
                        }
                        Some((bytes[0] - b'0') as i32 * if bytes[1] == b'-' { -1 } else { 1 })
                    };
                    let a = Atom {
                        atom_index: entry.atoms.len() as u64,
                        atom_id: optional(field(line, 6, 11)),
                        record_type: record.to_owned(),
                        model_id: model,
                        segment_index: segment,
                        auth_asym_id: Some(chain),
                        auth_seq_id: Some(seq),
                        insertion_code: optional(&ins),
                        auth_atom_id: Some(name.clone()),
                        auth_comp_id: Some(comp.clone()),
                        atom_name: name,
                        peptide: amino_acid(parent.as_deref().unwrap_or(&comp)).is_some()
                            || comp == "UNK",
                        parent_residue_name: parent,
                        residue_name: comp,
                        alt_id: optional(field(line, 16, 17)),
                        element: optional(field(line, 76, 78)),
                        position: [
                            number(field(line, 30, 38), "x")?,
                            number(field(line, 38, 46), "y")?,
                            number(field(line, 46, 54), "z")?,
                        ],
                        occupancy: optional_number(field(line, 54, 60), "occupancy")?,
                        b_factor: optional_number(field(line, 60, 66), "B factor")?,
                        formal_charge,
                        ..Default::default()
                    };
                    entry.atoms.push(a);
                    // `normalize` re-checks this bound; failing here stops parsing early.
                    if entry.atoms.len() > options.max_atoms {
                        return Err(error("atom count exceeds max_atoms"));
                    }
                }
                _ => {}
            }
            Ok(())
        })();
        result.map_err(|e| error(format!("PDB line {}: {e}", line_no + 1)))?;
    }
    if in_model {
        return Err(error("unclosed MODEL"));
    }
    entry.normalize(options)?;
    Ok(vec![entry])
}
