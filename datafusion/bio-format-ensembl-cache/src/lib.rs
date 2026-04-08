//! Ensembl VEP cache support for Apache DataFusion.
//!
//! This crate provides DataFusion table providers that read raw Ensembl VEP cache
//! directories directly and expose them as SQL-queryable tables.

#![warn(missing_docs)]

mod decode;
mod discovery;
mod entity;
mod errors;
mod exon;
mod export_query;
mod filter;
mod info;
mod physical_exec;
mod regulatory;
pub(crate) mod schema;
mod tabix_reader;
mod table_provider;
mod transcript;
mod translation;
mod util;
mod variation;

pub use entity::EnsemblEntityKind;
pub use export_query::{
    VEP_CACHE_REGION_SIZE_BP, build_export_query, build_export_query_multi_chrom,
    build_translation_dedup_query, build_translation_dedup_query_multi_chrom,
};
pub use schema::{translation_core_schema, translation_sift_schema};
pub use table_provider::{
    EnsemblCacheOptions, EnsemblCacheTableProvider, ExonTableProvider, MotifFeatureTableProvider,
    RegulatoryFeatureTableProvider, TranscriptTableProvider, TranslationTableProvider,
    VEP_CHROMOSOMES_METADATA_KEY, VariationTableProvider,
};
