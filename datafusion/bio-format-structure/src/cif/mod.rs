//! Repository-owned CIF byte parser. The grammar follows the IUCr CIF 1.1
//! specification, with extensions measured in our pinned compatibility corpus:
//! <https://www.iucr.org/what-we-do/digital-standards/cif/cif1/file-syntax>
//!
//! Input ownership and raw cell provenance stay inside `Document`. UTF-8 is
//! checked on exposed views, since comments and ignored frames may contain
//! non-UTF-8 bytes. No structural/numeric interpretation happens here.
mod document;
mod tokenizer;

pub use document::Document;
use std::collections::HashMap;

pub struct CategoryBlock<'a> {
    pub name: &'a str,
    pub columns: HashMap<&'a str, Vec<Option<&'a str>>>,
}

#[cfg(test)]
mod tests;
