use crate::errors::{Result, exec_err};
use crate::info::CacheInfo;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const REQUIRED_VARIATION_COLUMNS: [&str; 6] = [
    "chr",
    "start",
    "end",
    "variation_name",
    "allele_string",
    "region_bin",
];

const CANONICAL_OPTIONAL_VARIATION_COLUMNS: [&str; 10] = [
    "failed",
    "somatic",
    "strand",
    "minor_allele",
    "minor_allele_freq",
    "clin_sig",
    "phenotype_or_disease",
    "clinical_impact",
    "pubmed",
    "var_synonyms",
];

pub(crate) fn variation_schema(
    cache_info: &CacheInfo,
    coordinate_system_zero_based: bool,
) -> Result<SchemaRef> {
    if cache_info.variation_cols.is_empty() {
        return Err(exec_err(
            "Malformed info.txt: variation_cols must be present for variation entity",
        ));
    }

    let mut fields = vec![
        Field::new("chr", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("variation_name", DataType::Utf8, false),
        Field::new("allele_string", DataType::Utf8, false),
        Field::new("region_bin", DataType::Int64, false),
        Field::new("failed", DataType::Int8, true),
        Field::new("somatic", DataType::Int8, true),
        Field::new("strand", DataType::Int8, true),
        Field::new("minor_allele", DataType::Utf8, true),
        Field::new("minor_allele_freq", DataType::Float64, true),
        Field::new("clin_sig", DataType::Utf8, true),
        Field::new("phenotype_or_disease", DataType::Int8, true),
        Field::new("clinical_impact", DataType::Utf8, true),
        Field::new("pubmed", DataType::Utf8, true),
        Field::new("var_synonyms", DataType::Utf8, true),
    ];

    let mut known: HashSet<String> = REQUIRED_VARIATION_COLUMNS
        .iter()
        .chain(CANONICAL_OPTIONAL_VARIATION_COLUMNS.iter())
        .map(|v| v.to_string())
        .collect();

    for source in &cache_info.source_descriptors {
        if !known.contains(&source.ids_column) {
            fields.push(Field::new(&source.ids_column, DataType::Utf8, true));
            known.insert(source.ids_column.clone());
        }
    }

    for col in &cache_info.variation_cols {
        if !known.contains(col.as_str()) {
            fields.push(Field::new(col, DataType::Utf8, true));
            known.insert(col.clone());
        }
    }

    fields.extend(provenance_fields(cache_info));

    Ok(new_schema(fields, coordinate_system_zero_based))
}

pub(crate) fn transcript_schema(
    cache_info: &CacheInfo,
    coordinate_system_zero_based: bool,
) -> SchemaRef {
    let mut fields = vec![
        Field::new("chr", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("strand", DataType::Int8, false),
        Field::new("stable_id", DataType::Utf8, false),
        Field::new("version", DataType::Int32, true),
        Field::new("biotype", DataType::Utf8, true),
        Field::new("source", DataType::Utf8, true),
        Field::new("is_canonical", DataType::Boolean, true),
        Field::new("gene_stable_id", DataType::Utf8, true),
        Field::new("gene_symbol", DataType::Utf8, true),
        Field::new("gene_symbol_source", DataType::Utf8, true),
        Field::new("gene_hgnc_id", DataType::Utf8, true),
        Field::new("refseq_id", DataType::Utf8, true),
        Field::new("coding_region_start", DataType::Int64, true),
        Field::new("coding_region_end", DataType::Int64, true),
        Field::new("cdna_coding_start", DataType::Int64, true),
        Field::new("cdna_coding_end", DataType::Int64, true),
        Field::new("translation_stable_id", DataType::Utf8, true),
        Field::new("translation_start", DataType::Int64, true),
        Field::new("translation_end", DataType::Int64, true),
        Field::new("exon_count", DataType::Int32, true),
        Field::new("raw_object_json", DataType::Utf8, false),
        Field::new("object_hash", DataType::Utf8, false),
    ];
    fields.extend(provenance_fields(cache_info));
    new_schema(fields, coordinate_system_zero_based)
}

pub(crate) fn regulatory_feature_schema(
    cache_info: &CacheInfo,
    coordinate_system_zero_based: bool,
) -> SchemaRef {
    let mut fields = vec![
        Field::new("chr", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("strand", DataType::Int8, false),
        Field::new("stable_id", DataType::Utf8, true),
        Field::new("db_id", DataType::Int64, true),
        Field::new("feature_type", DataType::Utf8, true),
        Field::new("epigenome_count", DataType::Int32, true),
        Field::new("regulatory_build_id", DataType::Int64, true),
        Field::new("cell_types", DataType::Utf8, true),
        Field::new("raw_object_json", DataType::Utf8, false),
        Field::new("object_hash", DataType::Utf8, false),
    ];
    fields.extend(provenance_fields(cache_info));
    new_schema(fields, coordinate_system_zero_based)
}

pub(crate) fn motif_feature_schema(
    cache_info: &CacheInfo,
    coordinate_system_zero_based: bool,
) -> SchemaRef {
    let mut fields = vec![
        Field::new("chr", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("strand", DataType::Int8, false),
        Field::new("stable_id", DataType::Utf8, true),
        Field::new("db_id", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("binding_matrix", DataType::Utf8, true),
        Field::new("cell_types", DataType::Utf8, true),
        Field::new("overlapping_regulatory_feature", DataType::Utf8, true),
        Field::new("raw_object_json", DataType::Utf8, false),
        Field::new("object_hash", DataType::Utf8, false),
    ];
    fields.extend(provenance_fields(cache_info));
    new_schema(fields, coordinate_system_zero_based)
}

fn provenance_fields(cache_info: &CacheInfo) -> Vec<Field> {
    let mut fields = vec![
        Field::new("species", DataType::Utf8, false),
        Field::new("assembly", DataType::Utf8, false),
        Field::new("cache_version", DataType::Utf8, false),
        Field::new("serializer_type", DataType::Utf8, true),
        Field::new("source_cache_path", DataType::Utf8, false),
        Field::new("source_file", DataType::Utf8, false),
    ];

    for source in &cache_info.source_descriptors {
        fields.push(Field::new(&source.source_column, DataType::Utf8, true));
    }

    fields
}

fn new_schema(fields: Vec<Field>, coordinate_system_zero_based: bool) -> SchemaRef {
    let mut metadata = HashMap::new();
    metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        coordinate_system_zero_based.to_string(),
    );
    Arc::new(Schema::new_with_metadata(fields, metadata))
}
