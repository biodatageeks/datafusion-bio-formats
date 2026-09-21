//! Codes and decoded sizes from Foldcomp utility.h/foldcomp.cpp/amino_acid.cpp.
//! Adapted from Foldcomp (MIT); see LICENSE-FOLDCOMP.
use datafusion::common::Result;
use datafusion_bio_format_structure::error;

#[derive(Clone, Copy, Debug)]
pub(super) struct Residue {
    code: u8,
}

const NAMES: [&str; 20] = [
    "ALA", "ARG", "ASN", "ASP", "CYS", "GLN", "GLU", "GLY", "HIS", "ILE", "LEU", "LYS", "MET",
    "PHE", "PRO", "SER", "THR", "TRP", "TYR", "VAL",
];
const LETTERS: &[u8; 20] = b"ARNDCQEGHILKMFPSTWYV";
const SIDECHAINS: [usize; 20] = [2, 8, 5, 5, 3, 6, 6, 1, 7, 5, 5, 6, 5, 8, 4, 3, 4, 11, 9, 4];

impl Residue {
    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            0..=19 | 23..=31 => Ok(Self { code }),
            _ => Err(error(format!("unsupported FCZ residue code {code}"))),
        }
    }

    pub fn code(self) -> u8 {
        self.code
    }
    pub fn name(self) -> &'static str {
        NAMES.get(usize::from(self.code)).copied().unwrap_or("UNK")
    }
    pub fn letter(self) -> u8 {
        LETTERS.get(usize::from(self.code)).copied().unwrap_or(b'X')
    }
    pub fn sidechain_count(self) -> usize {
        SIDECHAINS.get(usize::from(self.code)).copied().unwrap_or(0)
    }
    pub fn atom_count(self) -> usize {
        3 + self.sidechain_count()
    }
}
