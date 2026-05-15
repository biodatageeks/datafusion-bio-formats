use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::metadata::{
    VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY, VCF_FIELD_NUMBER_KEY, VCF_FIELD_TYPE_KEY,
    VCF_FILE_FORMAT_KEY, VCF_GENOTYPES_SAMPLE_NAMES_KEY, VCF_SAMPLE_NAMES_KEY, to_json_string,
};
use serde_json::Value;
use zarrs::array::Array;
use zarrs::filesystem::FilesystemStore;

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
        genotype_encoding_raw: options.genotype_encoding_raw,
    })
}

/// Describes discoverable VCF Zarr INFO and FORMAT fields.
pub fn describe_fields(path: String) -> Result<arrow::array::RecordBatch> {
    let metadata = VcfZarrMetadata::open_local(&path)?;
    let options = normalize_read_options(&metadata, &VcfZarrReadOptions::default())?;
    let info_fields = options.info_fields.unwrap_or_default();
    let format_fields = options.format_fields.unwrap_or_default();

    let mut names = arrow::array::StringBuilder::new();
    let mut field_types = arrow::array::StringBuilder::new();
    let mut data_types = arrow::array::StringBuilder::new();
    let mut descriptions = arrow::array::StringBuilder::new();

    for name in info_fields {
        let raw_array = validate_info_array(&metadata, &name)?;
        let data_type = logical_info_data_type(&metadata, &raw_array)?;
        names.append_value(name);
        field_types.append_value("INFO");
        data_types.append_value(vcf_field_type_for_data_type(&data_type));
        descriptions.append_value("");
    }

    for name in format_fields {
        let raw_array = validate_format_array(&metadata, &name)?;
        let data_type = logical_format_data_type(
            &metadata,
            &name,
            &raw_array,
            VcfZarrReadOptions::default().genotype_encoding_raw,
        )?;
        names.append_value(name);
        field_types.append_value("FORMAT");
        data_types.append_value(vcf_field_type_for_data_type(&data_type));
        descriptions.append_value("");
    }

    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("field_type", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]);

    arrow::record_batch::RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(names.finish()),
            Arc::new(field_types.finish()),
            Arc::new(data_types.finish()),
            Arc::new(descriptions.finish()),
        ],
    )
    .map_err(DataFusionError::from)
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
        let data_type = logical_info_data_type(metadata, &raw_array)?;
        let vcf_field_type = vcf_field_type_for_data_type(&data_type);
        fields.push(
            Field::new(name, data_type, true).with_metadata(
                [
                    (VCF_FIELD_FIELD_TYPE_KEY.to_string(), "INFO".to_string()),
                    (VCF_FIELD_TYPE_KEY.to_string(), vcf_field_type.to_string()),
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
        let mut genotype_children = Vec::new();
        for name in format_fields {
            let raw_array = validate_format_array(metadata, name)?;
            let data_type = logical_format_data_type(
                metadata,
                name,
                &raw_array,
                options.genotype_encoding_raw,
            )?;
            let vcf_field_type = if is_gt_field(name, &raw_array) && !options.genotype_encoding_raw
            {
                "String"
            } else {
                vcf_field_type_for_data_type(&data_type)
            };
            genotype_children.push(
                Field::new(name, data_type, true).with_metadata(
                    [
                        (VCF_FIELD_FIELD_TYPE_KEY.to_string(), "FORMAT".to_string()),
                        (VCF_FIELD_FORMAT_ID_KEY.to_string(), name.clone()),
                        (VCF_FIELD_TYPE_KEY.to_string(), vcf_field_type.to_string()),
                        (VCF_FIELD_NUMBER_KEY.to_string(), ".".to_string()),
                        (
                            VCF_ZARR_RAW_ARRAY_METADATA_KEY.to_string(),
                            raw_array.clone(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
            );

            if options.genotype_encoding_raw && raw_array == "call_genotype" {
                if metadata.array_exists("call_genotype_phased") {
                    let data_type = logical_format_data_type(
                        metadata,
                        "GT_phased",
                        "call_genotype_phased",
                        true,
                    )?;
                    genotype_children.push(
                        Field::new("GT_phased", data_type, true).with_metadata(
                            [
                                (VCF_FIELD_FIELD_TYPE_KEY.to_string(), "FORMAT".to_string()),
                                (VCF_FIELD_FORMAT_ID_KEY.to_string(), "GT_phased".to_string()),
                                (VCF_FIELD_TYPE_KEY.to_string(), "Flag".to_string()),
                                (VCF_FIELD_NUMBER_KEY.to_string(), ".".to_string()),
                                (
                                    VCF_ZARR_RAW_ARRAY_METADATA_KEY.to_string(),
                                    "call_genotype_phased".to_string(),
                                ),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                    );
                }
                if metadata.array_exists("call_genotype_mask") {
                    let data_type =
                        logical_format_data_type(metadata, "GT_mask", "call_genotype_mask", true)?;
                    genotype_children.push(
                        Field::new("GT_mask", data_type, true).with_metadata(
                            [
                                (VCF_FIELD_FIELD_TYPE_KEY.to_string(), "FORMAT".to_string()),
                                (VCF_FIELD_FORMAT_ID_KEY.to_string(), "GT_mask".to_string()),
                                (VCF_FIELD_TYPE_KEY.to_string(), "Flag".to_string()),
                                (VCF_FIELD_NUMBER_KEY.to_string(), ".".to_string()),
                                (
                                    VCF_ZARR_RAW_ARRAY_METADATA_KEY.to_string(),
                                    "call_genotype_mask".to_string(),
                                ),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                    );
                }
            }
        }

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

fn logical_info_data_type(metadata: &VcfZarrMetadata, raw_array: &str) -> Result<DataType> {
    let array = metadata.open_array(raw_array)?;
    let primitive = zarr_primitive_arrow_type(&array, raw_array)?;
    match array.shape().len() {
        1 => Ok(primitive),
        2 => Ok(list_type(primitive)),
        _ => Err(DataFusionError::Execution(format!(
            "VCF Zarr INFO array '{raw_array}' has unsupported shape {:?}",
            array.shape()
        ))),
    }
}

fn logical_format_data_type(
    metadata: &VcfZarrMetadata,
    field_id: &str,
    raw_array: &str,
    genotype_encoding_raw: bool,
) -> Result<DataType> {
    let array = metadata.open_array(raw_array)?;
    if is_gt_field(field_id, raw_array) && !genotype_encoding_raw {
        return Ok(list_type(DataType::Utf8));
    }

    let primitive = zarr_primitive_arrow_type(&array, raw_array)?;
    match array.shape().len() {
        2 => Ok(list_type(primitive)),
        3 => Ok(list_type(list_type(primitive))),
        _ => Err(DataFusionError::Execution(format!(
            "VCF Zarr FORMAT array '{raw_array}' has unsupported shape {:?}",
            array.shape()
        ))),
    }
}

fn list_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item_type, true)))
}

fn zarr_primitive_arrow_type(array: &Array<FilesystemStore>, name: &str) -> Result<DataType> {
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "string", "|O") {
        Ok(DataType::Utf8)
    } else if is_dtype(&data_type, "bool", "|b1") {
        Ok(DataType::Boolean)
    } else if is_dtype(&data_type, "int8", "|i1") {
        Ok(DataType::Int8)
    } else if is_dtype(&data_type, "int16", "<i2") {
        Ok(DataType::Int16)
    } else if is_dtype(&data_type, "int32", "<i4") {
        Ok(DataType::Int32)
    } else if is_dtype(&data_type, "int64", "<i8") {
        Ok(DataType::Int64)
    } else if is_dtype(&data_type, "uint8", "|u1") {
        Ok(DataType::UInt8)
    } else if is_dtype(&data_type, "uint16", "<u2") {
        Ok(DataType::UInt16)
    } else if is_dtype(&data_type, "uint32", "<u4") {
        Ok(DataType::UInt32)
    } else if is_dtype(&data_type, "uint64", "<u8") {
        Ok(DataType::UInt64)
    } else if is_dtype(&data_type, "float32", "<f4") {
        Ok(DataType::Float32)
    } else if is_dtype(&data_type, "float64", "<f8") {
        Ok(DataType::Float64)
    } else {
        Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' has unsupported data type {data_type}"
        )))
    }
}

fn vcf_field_type_for_data_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "Flag",
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "Integer",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "Float",
        DataType::List(field) | DataType::LargeList(field) => {
            vcf_field_type_for_data_type(field.data_type())
        }
        _ => "String",
    }
}

fn is_gt_field(field_id: &str, raw_array: &str) -> bool {
    field_id == "GT" || raw_array == "call_genotype"
}

fn is_dtype(actual: &str, zarrs_name: &str, v2_name: &str) -> bool {
    actual == zarrs_name
        || actual == v2_name
        || actual.starts_with(&format!("{zarrs_name} /"))
        || actual.ends_with(&format!("/ {v2_name}"))
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
