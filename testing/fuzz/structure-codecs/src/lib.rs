//! Compile the exact repository parser/decoder/model sources without DataFusion.
//! Only the error carrier is substituted; see README.md for scope and limits.
#![allow(dead_code)]
extern crate self as datafusion;
extern crate self as datafusion_bio_format_structure;

pub mod common {
    pub type DataFusionError = std::io::Error;
    pub type Result<T> = std::io::Result<T>;
}
pub fn error(message: impl Into<String>) -> common::DataFusionError {
    std::io::Error::other(message.into())
}
#[path = "../../../../datafusion/bio-format-structure/src/options.rs"]
mod options;
pub use options::StructureOptions;
#[path = "../../../../datafusion/bio-format-structure/src/cif/mod.rs"]
mod cif;
#[path = "../../../../datafusion/bio-format-foldcomp/src/fcz/mod.rs"]
mod fcz;
#[path = "../../../../datafusion/bio-format-foldcomp/src/index.rs"]
mod index;
#[path = "../../../../datafusion/bio-format-structure/src/model.rs"]
pub mod model;

pub fn cif_document(data: &[u8]) {
    if data.len() > 262_144 {
        return;
    }
    if let Ok(document) = cif::Document::parse(data) {
        for i in 0..document.block_count() {
            if let Ok(block) = document.block(i) {
                for (name, cells) in block.columns {
                    assert!(name.starts_with('_'));
                    assert!(cells.len() <= data.len());
                    for value in cells.into_iter().flatten() {
                        assert!(value.len() <= data.len());
                    }
                }
            }
        }
        assert!(document.block(document.block_count()).is_err());
    }
}

pub fn fcz_decode(data: &[u8]) {
    if data.len() > 65_536 {
        return;
    }
    let options = StructureOptions {
        max_atoms: 20_000,
        ..Default::default()
    };
    if let Ok(entry) = fcz::decode(data, &options) {
        assert!(entry.atoms.len() <= options.max_atoms);
        for atom in entry.atoms {
            assert!(atom.position.into_iter().all(f64::is_finite));
            assert!(atom.b_factor.is_none_or(f64::is_finite));
        }
    }
}

pub fn selected_range(data: &[u8]) {
    if data.len() < 24 || data.len() > 65_536 {
        return;
    }
    let number = |i| u64::from_le_bytes(data[i..i + 8].try_into().unwrap());
    let (previous, file_len, limit) = (number(0), number(8), number(16));
    if let Ok(line) = std::str::from_utf8(&data[24..]) {
        for previous in [None, Some(previous)] {
            if let Ok(row) = index::IndexRow::parse(line, previous) {
                assert!(previous.is_none_or(|p| p < row.key));
                if row.validate_selected(file_len, limit).is_ok() {
                    assert!(row.len >= 2 && row.len <= limit);
                    assert!(
                        row.offset
                            .checked_add(row.len)
                            .is_some_and(|end| end <= file_len)
                    );
                }
            }
        }
    }
}

#[cfg(feature = "probe")]
pub use cif::Document;
#[cfg(feature = "probe")]
pub fn decode_entry(data: &[u8], max_atoms: usize) -> common::Result<model::NormalizedEntry> {
    fcz::decode(
        data,
        &StructureOptions {
            max_atoms,
            ..Default::default()
        },
    )
}
