use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::metadata::{
    VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY, VCF_FIELD_NUMBER_KEY, VCF_FIELD_TYPE_KEY,
    VCF_FILE_FORMAT_KEY, VCF_GENOTYPES_SAMPLE_NAMES_KEY, VCF_SAMPLE_NAMES_KEY, to_json_string,
};
use serde_json::Value;

use super::metadata::VcfZarrMetadata;
use super::samples::{SampleSelection, format_array_name};
use super::table_provider::VcfZarrReadOptions;

const VCF_ZARR_VERSION_METADATA_KEY: &str = "bio.vcf.zarr.version";
pub(crate) const VCF_ZARR_RAW_ARRAY_METADATA_KEY: &str = "bio.vcf.zarr.raw_array";

const INFO_DISCOVERY_EXCLUDED_RAW_ARRAYS: &[&str] = &[
    "variant_contig",
    "variant_position",
    "variant_allele",
    "variant_id",
    "variant_length",
    "variant_quality",
    "variant_filter",
];
const FORMAT_DISCOVERY_EXCLUDED_RAW_ARRAYS: &[&str] = &["call_genotype_phased"];

pub(crate) fn normalize_read_options(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
) -> Result<VcfZarrReadOptions> {
    Ok(VcfZarrReadOptions {
        info_fields: match &options.info_fields {
            Some(fields) => Some(fields.clone()),
            None => Some(discover_info_fields(metadata)?),
        },
        format_fields: match &options.format_fields {
            Some(fields) => Some(fields.clone()),
            None => Some(discover_format_fields(metadata)?),
        },
        samples: options.samples.clone(),
        coordinate_system_zero_based: options.coordinate_system_zero_based,
    })
}

pub(crate) fn build_logical_schema_with_sample_selection(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
    sample_selection: &SampleSelection,
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

    for name in options.info_fields.as_deref().unwrap_or(&[]) {
        let raw_array = validate_info_array(metadata, name)?;
        fields.push(
            Field::new(name, DataType::Utf8, true).with_metadata(
                [
                    (VCF_FIELD_FIELD_TYPE_KEY.to_string(), "INFO".to_string()),
                    (VCF_FIELD_TYPE_KEY.to_string(), "String".to_string()),
                    (VCF_FIELD_NUMBER_KEY.to_string(), ".".to_string()),
                    (VCF_ZARR_RAW_ARRAY_METADATA_KEY.to_string(), raw_array),
                ]
                .into_iter()
                .collect(),
            ),
        );
    }

    if let Some(format_fields) = &options.format_fields
        && !format_fields.is_empty()
    {
        let genotype_children = format_fields
            .iter()
            .map(|name| {
                let raw_array = validate_format_array(metadata, name)?;
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
                        (VCF_ZARR_RAW_ARRAY_METADATA_KEY.to_string(), raw_array),
                    ]
                    .into_iter()
                    .collect(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        fields.push(
            Field::new(
                "genotypes",
                DataType::Struct(genotype_children.into()),
                true,
            )
            .with_metadata(
                [(
                    VCF_GENOTYPES_SAMPLE_NAMES_KEY.to_string(),
                    to_json_string(&sample_selection.selected_names),
                )]
                .into_iter()
                .collect(),
            ),
        );
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
        to_json_string(&sample_selection.selected_names),
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

fn validate_info_array(metadata: &VcfZarrMetadata, field_id: &str) -> Result<String> {
    let raw_array = format!("variant_{field_id}");
    metadata.open_array(&raw_array).map_err(|error| {
        DataFusionError::Execution(format!(
            "Requested VCF Zarr INFO field '{field_id}' requires readable raw array '{raw_array}': {error}"
        ))
    })?;
    Ok(raw_array)
}

fn validate_format_array(metadata: &VcfZarrMetadata, field_id: &str) -> Result<String> {
    let raw_array = format_array_name(metadata, field_id)?;
    metadata.open_array(&raw_array).map_err(|error| {
        DataFusionError::Execution(format!(
            "Requested VCF Zarr FORMAT field '{field_id}' requires readable raw array '{raw_array}': {error}"
        ))
    })?;
    Ok(raw_array)
}

fn discover_info_fields(metadata: &VcfZarrMetadata) -> Result<Vec<String>> {
    let mut fields = Vec::new();
    for raw_array in direct_array_names(metadata)? {
        if !raw_array.starts_with("variant_")
            || raw_array.ends_with("_mask")
            || INFO_DISCOVERY_EXCLUDED_RAW_ARRAYS.contains(&raw_array.as_str())
        {
            continue;
        }
        fields.push(raw_array.trim_start_matches("variant_").to_string());
    }
    fields.sort();
    fields.dedup();
    Ok(fields)
}

fn discover_format_fields(metadata: &VcfZarrMetadata) -> Result<Vec<String>> {
    let mut fields = Vec::new();
    for raw_array in direct_array_names(metadata)? {
        if !raw_array.starts_with("call_")
            || raw_array.ends_with("_mask")
            || FORMAT_DISCOVERY_EXCLUDED_RAW_ARRAYS.contains(&raw_array.as_str())
        {
            continue;
        }

        let field_id = if raw_array == "call_genotype" {
            "GT"
        } else {
            raw_array.trim_start_matches("call_")
        };
        fields.push(field_id.to_string());
    }
    fields.sort_by(|left, right| match (left.as_str(), right.as_str()) {
        ("GT", "GT") => std::cmp::Ordering::Equal,
        ("GT", _) => std::cmp::Ordering::Less,
        (_, "GT") => std::cmp::Ordering::Greater,
        _ => left.cmp(right),
    });
    fields.dedup();
    Ok(fields)
}

fn direct_array_names(metadata: &VcfZarrMetadata) -> Result<Vec<String>> {
    let entries = std::fs::read_dir(&metadata.root_path).map_err(|error| {
        DataFusionError::Execution(format!(
            "Failed to list VCF Zarr store at {}: {error}",
            metadata.root_path.display()
        ))
    })?;

    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to list VCF Zarr store at {}: {error}",
                metadata.root_path.display()
            ))
        })?;
        let path = entry.path();
        if !path.is_dir() || !path.join(".zarray").is_file() {
            continue;
        }

        let name = entry.file_name().into_string().map_err(|_| {
            DataFusionError::Execution(format!(
                "VCF Zarr array path under {} is not valid UTF-8",
                metadata.root_path.display()
            ))
        })?;
        names.push(name);
    }
    names.sort();
    Ok(names)
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
