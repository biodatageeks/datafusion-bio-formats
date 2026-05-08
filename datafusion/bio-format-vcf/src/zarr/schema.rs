use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::metadata::{
    VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY, VCF_FIELD_NUMBER_KEY, VCF_FIELD_TYPE_KEY,
    VCF_FILE_FORMAT_KEY, VCF_SAMPLE_NAMES_KEY, to_json_string,
};
use serde_json::Value;

use super::metadata::VcfZarrMetadata;
use super::table_provider::VcfZarrReadOptions;

const VCF_ZARR_VERSION_METADATA_KEY: &str = "bio.vcf.zarr.version";

/// Builds the logical Arrow schema exposed by the VCF Zarr table provider.
pub fn build_logical_schema(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
) -> Result<SchemaRef> {
    validate_required_arrays(metadata)?;

    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, false),
        Field::new("alt", DataType::Utf8, false),
        Field::new("qual", DataType::Float64, true),
        Field::new("filter", DataType::Utf8, true),
    ];

    if let Some(info_fields) = &options.info_fields {
        for name in info_fields {
            validate_info_array(metadata, name)?;
            fields.push(
                Field::new(name, DataType::Utf8, true).with_metadata(
                    [
                        (VCF_FIELD_FIELD_TYPE_KEY.to_string(), "INFO".to_string()),
                        (VCF_FIELD_TYPE_KEY.to_string(), "String".to_string()),
                        (VCF_FIELD_NUMBER_KEY.to_string(), ".".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                ),
            );
        }
    }

    if let Some(format_fields) = &options.format_fields
        && !format_fields.is_empty()
    {
        let genotype_children = format_fields
            .iter()
            .map(|name| {
                validate_format_array(metadata, name)?;
                Ok(Field::new(
                    name,
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                )
                .with_metadata(
                    [
                        (VCF_FIELD_FIELD_TYPE_KEY.to_string(), "FORMAT".to_string()),
                        (VCF_FIELD_FORMAT_ID_KEY.to_string(), name.clone()),
                        (VCF_FIELD_TYPE_KEY.to_string(), "String".to_string()),
                        (VCF_FIELD_NUMBER_KEY.to_string(), ".".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        fields.push(Field::new(
            "genotypes",
            DataType::Struct(genotype_children.into()),
            true,
        ));
    }

    let mut schema_metadata = HashMap::new();
    schema_metadata.insert(
        VCF_ZARR_VERSION_METADATA_KEY.to_string(),
        metadata.vcf_zarr_version.clone(),
    );
    schema_metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        options.coordinate_system_zero_based.to_string(),
    );
    schema_metadata.insert(VCF_FILE_FORMAT_KEY.to_string(), vcf_file_format(metadata));
    schema_metadata.insert(
        VCF_SAMPLE_NAMES_KEY.to_string(),
        to_json_string(&Vec::<String>::new()),
    );

    Ok(Arc::new(Schema::new_with_metadata(fields, schema_metadata)))
}

fn validate_required_arrays(metadata: &VcfZarrMetadata) -> Result<()> {
    for required in [
        "variant_contig",
        "variant_position",
        "contig_id",
        "variant_allele",
    ] {
        metadata.open_array(required).map_err(|error| {
            DataFusionError::Execution(format!(
                "VCF Zarr store is missing required array '{required}' or its metadata is unreadable: {error}"
            ))
        })?;
    }

    Ok(())
}

fn validate_info_array(metadata: &VcfZarrMetadata, field_id: &str) -> Result<()> {
    let raw_array = format!("variant_{field_id}");
    metadata.open_array(&raw_array).map_err(|error| {
        DataFusionError::Execution(format!(
            "Requested VCF Zarr INFO field '{field_id}' requires readable raw array '{raw_array}': {error}"
        ))
    })?;
    Ok(())
}

fn validate_format_array(metadata: &VcfZarrMetadata, field_id: &str) -> Result<()> {
    let raw_array = format!("call_{field_id}");
    metadata.open_array(&raw_array).map_err(|error| {
        DataFusionError::Execution(format!(
            "Requested VCF Zarr FORMAT field '{field_id}' requires readable raw array '{raw_array}': {error}"
        ))
    })?;
    Ok(())
}

fn vcf_file_format(metadata: &VcfZarrMetadata) -> String {
    metadata
        .root_attributes
        .get("vcf_meta_information")
        .and_then(|value| match value {
            Value::Array(records) => records.iter().find_map(parse_fileformat_record),
            _ => None,
        })
        .unwrap_or_else(|| "VCFv4.3".to_string())
}

fn parse_fileformat_record(record: &Value) -> Option<String> {
    let values = record.as_array()?;
    let key = values.first()?.as_str()?;

    if key != "fileformat" {
        return None;
    }

    let file_format = values.get(1)?.as_str()?;
    (!file_format.is_empty()).then(|| file_format.to_string())
}
