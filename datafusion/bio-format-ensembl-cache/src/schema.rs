use crate::errors::{Result, exec_err};
use crate::info::CacheInfo;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Returns the Arrow `DataType` for the exons column:
/// `List<Struct<start:Int64, end:Int64, phase:Int8>>`.
pub(crate) fn exon_list_data_type() -> DataType {
    let exon_fields = Fields::from(vec![
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("phase", DataType::Int8, false),
    ]);
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(exon_fields),
        true,
    )))
}

/// Returns the Arrow `DataType` for cDNA mapper segments:
/// `List<Struct<genomic_start:Int64, genomic_end:Int64, cdna_start:Int64, cdna_end:Int64, ori:Int8>>`.
pub(crate) fn cdna_mapper_segment_list_data_type() -> DataType {
    let fields = Fields::from(vec![
        Field::new("genomic_start", DataType::Int64, false),
        Field::new("genomic_end", DataType::Int64, false),
        Field::new("cdna_start", DataType::Int64, false),
        Field::new("cdna_end", DataType::Int64, false),
        Field::new("ori", DataType::Int8, false),
    ]);
    DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true)))
}

/// Returns the Arrow `DataType` for protein features:
/// `List<Struct<analysis:Utf8, hseqname:Utf8, start:Int64, end:Int64>>`.
pub(crate) fn protein_feature_list_data_type() -> DataType {
    let fields = Fields::from(vec![
        Field::new("analysis", DataType::Utf8, true),
        Field::new("hseqname", DataType::Utf8, true),
        Field::new("start", DataType::Int64, true),
        Field::new("end", DataType::Int64, true),
    ]);
    DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true)))
}

/// Returns the Arrow `DataType` for SIFT/PolyPhen prediction entries:
/// `List<Struct<position:Int32, amino_acid:Utf8, prediction:Utf8, score:Float32>>`.
pub(crate) fn prediction_list_data_type() -> DataType {
    let fields = Fields::from(vec![
        Field::new("position", DataType::Int32, false),
        Field::new("amino_acid", DataType::Utf8, false),
        Field::new("prediction", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
    ]);
    DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true)))
}

/// Returns the Arrow `DataType` for mature miRNA genomic regions:
/// `List<Struct<start:Int64, end:Int64>>`.
pub(crate) fn mirna_region_list_data_type() -> DataType {
    let region_fields = Fields::from(vec![
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
    ]);
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(region_fields),
        true,
    )))
}

const REQUIRED_VARIATION_COLUMNS: [&str; 6] = [
    "chrom",
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
        Field::new("chrom", DataType::Utf8, false),
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
    // Input cache rows often use "chr" while output schema standardizes on "chrom".
    known.insert("chr".to_string());

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
        Field::new("chrom", DataType::Utf8, false),
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
        Field::new("cds_start", DataType::Int64, true),
        Field::new("cds_end", DataType::Int64, true),
        Field::new("cdna_coding_start", DataType::Int64, true),
        Field::new("cdna_coding_end", DataType::Int64, true),
        Field::new("translation_stable_id", DataType::Utf8, true),
        Field::new("translation_start", DataType::Int64, true),
        Field::new("translation_end", DataType::Int64, true),
        Field::new("exon_count", DataType::Int32, true),
        // VEP-related structured columns
        Field::new("exons", exon_list_data_type(), true),
        Field::new("cdna_seq", DataType::Utf8, true),
        Field::new("peptide_seq", DataType::Utf8, true),
        Field::new("codon_table", DataType::Int32, true),
        Field::new("tsl", DataType::Int32, true),
        Field::new("appris", DataType::Utf8, true),
        Field::new("mane_select", DataType::Utf8, true),
        Field::new("mane_plus_clinical", DataType::Utf8, true),
        Field::new("gene_phenotype", DataType::Boolean, true),
        Field::new("ccds", DataType::Utf8, true),
        Field::new("swissprot", DataType::Utf8, true),
        Field::new("trembl", DataType::Utf8, true),
        Field::new("uniparc", DataType::Utf8, true),
        Field::new("uniprot_isoform", DataType::Utf8, true),
        Field::new("cds_start_nf", DataType::Boolean, true),
        Field::new("cds_end_nf", DataType::Boolean, true),
        Field::new("mature_mirna_regions", mirna_region_list_data_type(), true),
        Field::new("ncrna_structure", DataType::Utf8, true),
        // Promoted VEP fields (issue #125)
        Field::new("translateable_seq", DataType::Utf8, true),
        Field::new(
            "cdna_mapper_segments",
            cdna_mapper_segment_list_data_type(),
            true,
        ),
        Field::new("bam_edit_status", DataType::Utf8, true),
        Field::new("has_non_polya_rna_edit", DataType::Boolean, true),
        Field::new("spliced_seq", DataType::Utf8, true),
        Field::new("flags_str", DataType::Utf8, true),
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
        Field::new("chrom", DataType::Utf8, false),
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
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("strand", DataType::Int8, false),
        Field::new("motif_id", DataType::Utf8, true),
        Field::new("db_id", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("binding_matrix", DataType::Utf8, true),
        Field::new("cell_types", DataType::Utf8, true),
        Field::new("overlapping_regulatory_feature", DataType::Utf8, true),
        Field::new("transcription_factors", DataType::Utf8, true),
        Field::new("raw_object_json", DataType::Utf8, false),
        Field::new("object_hash", DataType::Utf8, false),
    ];
    fields.extend(provenance_fields(cache_info));
    new_schema(fields, coordinate_system_zero_based)
}

pub(crate) fn exon_schema(cache_info: &CacheInfo, coordinate_system_zero_based: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("strand", DataType::Int8, false),
        Field::new("stable_id", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
        Field::new("phase", DataType::Int8, true),
        Field::new("end_phase", DataType::Int8, true),
        Field::new("is_current", DataType::Boolean, true),
        Field::new("is_constitutive", DataType::Boolean, true),
        Field::new("transcript_id", DataType::Utf8, false),
        Field::new("gene_stable_id", DataType::Utf8, true),
        Field::new("exon_number", DataType::Int32, false),
        Field::new("raw_object_json", DataType::Utf8, false),
        Field::new("object_hash", DataType::Utf8, false),
    ];
    fields.extend(provenance_fields(cache_info));
    new_schema(fields, coordinate_system_zero_based)
}

pub(crate) fn translation_schema(
    cache_info: &CacheInfo,
    coordinate_system_zero_based: bool,
) -> SchemaRef {
    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("stable_id", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
        Field::new("translation_start", DataType::Int64, true),
        Field::new("translation_end", DataType::Int64, true),
        Field::new("protein_len", DataType::Int64, true),
        Field::new("transcript_id", DataType::Utf8, false),
        Field::new("gene_stable_id", DataType::Utf8, true),
        Field::new("cdna_coding_start", DataType::Int64, true),
        Field::new("cdna_coding_end", DataType::Int64, true),
        Field::new("cds_len", DataType::Int64, true),
        Field::new("translation_seq", DataType::Utf8, true),
        Field::new("cds_sequence", DataType::Utf8, true),
        Field::new("protein_features", protein_feature_list_data_type(), true),
        Field::new("sift_predictions", prediction_list_data_type(), true),
        Field::new("polyphen_predictions", prediction_list_data_type(), true),
        Field::new("raw_object_json", DataType::Utf8, false),
        Field::new("object_hash", DataType::Utf8, false),
    ];
    fields.extend(provenance_fields(cache_info));
    new_schema(fields, coordinate_system_zero_based)
}

/// Schema for the `translation_core` split file: identity, sequence, and protein features.
/// Sorted by `(transcript_id)` to enable RG pruning for `WHERE transcript_id IN (...)`.
pub fn translation_core_schema(coordinate_system_zero_based: bool) -> SchemaRef {
    let fields = vec![
        Field::new("transcript_id", DataType::Utf8, false),
        Field::new("stable_id", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
        Field::new("cds_len", DataType::Int64, true),
        Field::new("protein_len", DataType::Int64, true),
        Field::new("translation_seq", DataType::Utf8, true),
        Field::new("cds_sequence", DataType::Utf8, true),
        Field::new("protein_features", protein_feature_list_data_type(), true),
    ];
    new_schema(fields, coordinate_system_zero_based)
}

/// Schema for the `translation_sift` split file: position-range sift/polyphen data.
/// Sorted by `(chrom, start)` to enable RG pruning for windowed sift/polyphen loading.
pub fn translation_sift_schema(coordinate_system_zero_based: bool) -> SchemaRef {
    let fields = vec![
        Field::new("transcript_id", DataType::Utf8, false),
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("sift_predictions", prediction_list_data_type(), true),
        Field::new("polyphen_predictions", prediction_list_data_type(), true),
    ];
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::info::{CacheInfo, SourceDescriptor};
    use std::path::PathBuf;

    fn test_cache_info() -> CacheInfo {
        CacheInfo {
            cache_root: PathBuf::from("/tmp/test"),
            source_cache_path: "/tmp/test".to_string(),
            species: "homo_sapiens".to_string(),
            assembly: "GRCh38".to_string(),
            cache_version: "115".to_string(),
            serializer_type: Some("storable".to_string()),
            var_type: Some("region".to_string()),
            cache_region_size: Some(1_000_000),
            variation_cols: vec!["chr", "start", "end", "variation_name", "allele_string"]
                .into_iter()
                .map(String::from)
                .collect(),
            source_descriptors: vec![],
        }
    }

    fn test_cache_info_with_sources() -> CacheInfo {
        let mut info = test_cache_info();
        info.source_descriptors = vec![SourceDescriptor {
            source_key: "dbsnp".to_string(),
            source_column: "source_dbsnp".to_string(),
            ids_column: "dbsnp_ids".to_string(),
            value: "156".to_string(),
        }];
        info
    }

    // -----------------------------------------------------------------------
    // exon_list_data_type / mirna_region_list_data_type
    // -----------------------------------------------------------------------

    #[test]
    fn exon_list_type_is_list_of_struct() {
        let dt = exon_list_data_type();
        match &dt {
            DataType::List(field) => {
                assert_eq!(
                    field.data_type(),
                    &DataType::Struct(Fields::from(vec![
                        Field::new("start", DataType::Int64, false),
                        Field::new("end", DataType::Int64, false),
                        Field::new("phase", DataType::Int8, false),
                    ]))
                );
            }
            _ => panic!("expected List type"),
        }
    }

    #[test]
    fn mirna_region_list_type_is_list_of_struct() {
        let dt = mirna_region_list_data_type();
        match &dt {
            DataType::List(field) => {
                assert_eq!(
                    field.data_type(),
                    &DataType::Struct(Fields::from(vec![
                        Field::new("start", DataType::Int64, false),
                        Field::new("end", DataType::Int64, false),
                    ]))
                );
            }
            _ => panic!("expected List type"),
        }
    }

    // -----------------------------------------------------------------------
    // variation_schema
    // -----------------------------------------------------------------------

    #[test]
    fn variation_schema_required_fields() {
        let info = test_cache_info();
        let schema = variation_schema(&info, false).unwrap();
        assert!(schema.column_with_name("chrom").is_some());
        assert!(schema.column_with_name("start").is_some());
        assert!(schema.column_with_name("end").is_some());
        assert!(schema.column_with_name("variation_name").is_some());
        assert!(schema.column_with_name("allele_string").is_some());
        assert!(schema.column_with_name("region_bin").is_some());
    }

    #[test]
    fn variation_schema_optional_fields() {
        let info = test_cache_info();
        let schema = variation_schema(&info, false).unwrap();
        assert!(schema.column_with_name("failed").is_some());
        assert!(schema.column_with_name("clin_sig").is_some());
        assert!(schema.column_with_name("var_synonyms").is_some());
    }

    #[test]
    fn variation_schema_provenance_fields() {
        let info = test_cache_info();
        let schema = variation_schema(&info, false).unwrap();
        assert!(schema.column_with_name("species").is_some());
        assert!(schema.column_with_name("assembly").is_some());
        assert!(schema.column_with_name("cache_version").is_some());
        assert!(schema.column_with_name("source_file").is_some());
    }

    #[test]
    fn variation_schema_with_sources() {
        let info = test_cache_info_with_sources();
        let schema = variation_schema(&info, false).unwrap();
        assert!(schema.column_with_name("dbsnp_ids").is_some());
        assert!(schema.column_with_name("source_dbsnp").is_some());
    }

    #[test]
    fn variation_schema_empty_cols_errors() {
        let mut info = test_cache_info();
        info.variation_cols.clear();
        assert!(variation_schema(&info, false).is_err());
    }

    #[test]
    fn variation_schema_extra_cols() {
        let mut info = test_cache_info();
        info.variation_cols.push("AFR".to_string());
        info.variation_cols.push("AF".to_string());
        let schema = variation_schema(&info, false).unwrap();
        assert!(schema.column_with_name("AFR").is_some());
        assert!(schema.column_with_name("AF").is_some());
    }

    #[test]
    fn variation_schema_coordinate_metadata() {
        let info = test_cache_info();
        let schema_1based = variation_schema(&info, false).unwrap();
        assert_eq!(
            schema_1based.metadata().get(COORDINATE_SYSTEM_METADATA_KEY),
            Some(&"false".to_string())
        );

        let schema_0based = variation_schema(&info, true).unwrap();
        assert_eq!(
            schema_0based.metadata().get(COORDINATE_SYSTEM_METADATA_KEY),
            Some(&"true".to_string())
        );
    }

    // -----------------------------------------------------------------------
    // transcript_schema
    // -----------------------------------------------------------------------

    #[test]
    fn transcript_schema_has_expected_fields() {
        let info = test_cache_info();
        let schema = transcript_schema(&info, false);
        assert!(schema.column_with_name("chrom").is_some());
        assert!(schema.column_with_name("stable_id").is_some());
        assert!(schema.column_with_name("biotype").is_some());
        assert!(schema.column_with_name("gene_stable_id").is_some());
        assert!(schema.column_with_name("exons").is_some());
        assert!(schema.column_with_name("cdna_seq").is_some());
        assert!(schema.column_with_name("tsl").is_some());
        assert!(schema.column_with_name("mane_select").is_some());
        assert!(schema.column_with_name("raw_object_json").is_some());
        assert!(schema.column_with_name("object_hash").is_some());
    }

    #[test]
    fn transcript_schema_exons_type() {
        let info = test_cache_info();
        let schema = transcript_schema(&info, false);
        let (_, field) = schema.column_with_name("exons").unwrap();
        assert_eq!(field.data_type(), &exon_list_data_type());
    }

    // -----------------------------------------------------------------------
    // regulatory_feature_schema / motif_feature_schema
    // -----------------------------------------------------------------------

    #[test]
    fn regulatory_schema_fields() {
        let info = test_cache_info();
        let schema = regulatory_feature_schema(&info, false);
        assert!(schema.column_with_name("chrom").is_some());
        assert!(schema.column_with_name("stable_id").is_some());
        assert!(schema.column_with_name("feature_type").is_some());
        assert!(schema.column_with_name("cell_types").is_some());
    }

    #[test]
    fn motif_schema_fields() {
        let info = test_cache_info();
        let schema = motif_feature_schema(&info, false);
        assert!(schema.column_with_name("motif_id").is_some());
        assert!(schema.column_with_name("score").is_some());
        assert!(schema.column_with_name("binding_matrix").is_some());
        assert!(schema.column_with_name("transcription_factors").is_some());
    }

    // -----------------------------------------------------------------------
    // exon_schema / translation_schema
    // -----------------------------------------------------------------------

    #[test]
    fn exon_schema_fields() {
        let info = test_cache_info();
        let schema = exon_schema(&info, false);
        assert!(schema.column_with_name("chrom").is_some());
        assert!(schema.column_with_name("phase").is_some());
        assert!(schema.column_with_name("end_phase").is_some());
        assert!(schema.column_with_name("transcript_id").is_some());
        assert!(schema.column_with_name("exon_number").is_some());
    }

    #[test]
    fn translation_schema_fields() {
        let info = test_cache_info();
        let schema = translation_schema(&info, false);
        assert!(schema.column_with_name("stable_id").is_some());
        assert!(schema.column_with_name("protein_len").is_some());
        assert!(schema.column_with_name("transcript_id").is_some());
        assert!(schema.column_with_name("translation_seq").is_some());
        assert!(schema.column_with_name("cds_sequence").is_some());
    }

    // -----------------------------------------------------------------------
    // provenance_fields
    // -----------------------------------------------------------------------

    #[test]
    fn provenance_includes_source_columns() {
        let info = test_cache_info_with_sources();
        let fields = provenance_fields(&info);
        let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"species"));
        assert!(names.contains(&"assembly"));
        assert!(names.contains(&"cache_version"));
        assert!(names.contains(&"source_file"));
        assert!(names.contains(&"source_dbsnp"));
    }
}
