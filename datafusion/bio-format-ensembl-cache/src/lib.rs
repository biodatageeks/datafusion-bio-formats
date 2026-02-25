//! Ensembl VEP cache support for Apache DataFusion.
//!
//! This crate provides DataFusion table providers that read raw Ensembl VEP cache
//! directories directly and expose them as SQL-queryable tables.

#![warn(missing_docs)]

mod decode;
mod discovery;
mod entity;
mod errors;
mod filter;
mod info;
mod physical_exec;
mod regulatory;
mod schema;
mod table_provider;
mod transcript;
mod util;
mod variation;

pub use entity::EnsemblEntityKind;
pub use table_provider::{
    EnsemblCacheOptions, EnsemblCacheTableProvider, MotifFeatureTableProvider,
    RegulatoryFeatureTableProvider, TranscriptTableProvider, VariationTableProvider,
};
