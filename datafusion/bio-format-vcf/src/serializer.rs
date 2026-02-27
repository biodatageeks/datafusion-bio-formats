//! Serializer for converting Arrow RecordBatches to VCF records
//!
//! This module provides functionality for converting DataFusion Arrow data back
//! to VCF format for writing to files.

use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, LargeListArray, LargeStringArray,
    ListArray, RecordBatch, StringArray, StructArray, UInt32Array,
};
use datafusion::common::{DataFusionError, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// Enum to hold either StringArray or LargeStringArray reference
/// This allows handling both standard Arrow Utf8 and Polars LargeUtf8 types
enum StringColumnRef<'a> {
    Small(&'a StringArray),
    Large(&'a LargeStringArray),
}

impl StringColumnRef<'_> {
    fn value(&self, i: usize) -> &str {
        match self {
            StringColumnRef::Small(arr) => arr.value(i),
            StringColumnRef::Large(arr) => arr.value(i),
        }
    }

    fn is_null(&self, i: usize) -> bool {
        match self {
            StringColumnRef::Small(arr) => Array::is_null(*arr, i),
            StringColumnRef::Large(arr) => Array::is_null(*arr, i),
        }
    }
}

/// A serialized VCF record as a string line
pub struct VcfRecordLine {
    /// The VCF line (without newline)
    pub line: String,
}

/// Converts an Arrow RecordBatch to a vector of VCF record lines.
///
/// The RecordBatch must have columns matching VCF schema names. Columns are
/// looked up by name, so the order in the batch does not matter.
///
/// # Arguments
///
/// * `batch` - The Arrow RecordBatch to convert
/// * `info_fields` - Names of INFO fields to include
/// * `format_fields` - Names of FORMAT fields (unique list)
/// * `sample_names` - Names of samples
/// * `coordinate_system_zero_based` - If true, coordinates are 0-based half-open (need +1 for VCF)
///
/// # Returns
///
/// A vector of VCF record lines that can be written to a file
///
/// # Errors
///
/// Returns an error if required columns are missing or have wrong types
pub fn batch_to_vcf_lines(
    batch: &RecordBatch,
    info_fields: &[String],
    format_fields: &[String],
    sample_names: &[String],
    coordinate_system_zero_based: bool,
) -> Result<Vec<VcfRecordLine>> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    // Look up core columns by name
    let chroms = get_string_column_by_name(batch, "chrom")?;
    let starts = get_u32_column_by_name(batch, "start")?;
    let ids = get_string_column_by_name(batch, "id")?;
    let refs = get_string_column_by_name(batch, "ref")?;
    let alts = get_string_column_by_name(batch, "alt")?;
    let quals = get_optional_f64_column_by_name(batch, "qual")?;
    let filters = get_string_column_by_name(batch, "filter")?;

    // Build column index maps for INFO and FORMAT fields
    let info_columns = build_info_column_map(batch, info_fields);
    let format_columns = if sample_names.len() == 1 {
        Some(build_format_column_map(batch, format_fields, sample_names))
    } else {
        None
    };

    let mut records = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        // CHROM
        let chrom = chroms.value(row);

        // POS (convert from 0-based to 1-based if needed)
        let pos = if coordinate_system_zero_based {
            starts.value(row) + 1
        } else {
            starts.value(row)
        };

        // ID
        let id_str = if ids.is_null(row) || ids.value(row).is_empty() {
            ".".to_string()
        } else {
            ids.value(row).to_string()
        };

        // REF
        let ref_str = refs.value(row);

        // ALT (convert pipe separator back to comma)
        let alt_value = alts.value(row);
        let alt_str = if alt_value.is_empty() || alt_value == "." {
            ".".to_string()
        } else {
            alt_value.replace('|', ",")
        };

        // QUAL
        let qual_str = if quals.is_null(row) {
            ".".to_string()
        } else {
            format!("{:.2}", quals.value(row))
        };

        // FILTER
        let filter_str = if filters.is_null(row) || filters.value(row).is_empty() {
            ".".to_string()
        } else {
            filters.value(row).to_string()
        };

        // INFO
        let info_str = build_info_string(batch, row, info_fields, &info_columns)?;

        // FORMAT and samples
        let (format_str, samples_str) = build_format_and_samples(
            batch,
            row,
            format_fields,
            sample_names,
            format_columns.as_ref(),
        )?;

        // Build the VCF line
        let mut line = format!(
            "{chrom}\t{pos}\t{id_str}\t{ref_str}\t{alt_str}\t{qual_str}\t{filter_str}\t{info_str}"
        );

        if !format_str.is_empty() {
            line.push('\t');
            line.push_str(&format_str);
            for sample in &samples_str {
                line.push('\t');
                line.push_str(sample);
            }
        }

        records.push(VcfRecordLine { line });
    }

    Ok(records)
}

/// Gets a string column from the batch by name (supports both Utf8 and LargeUtf8)
fn get_string_column_by_name<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<StringColumnRef<'a>> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{name}' not found in batch"))
    })?;
    let column = batch.column(idx);

    // Try StringArray first, then LargeStringArray
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(StringColumnRef::Small(arr));
    }
    if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(StringColumnRef::Large(arr));
    }

    Err(DataFusionError::Execution(format!(
        "Column '{name}' must be Utf8 or LargeUtf8 type"
    )))
}

/// Gets a u32 column from the batch by name
fn get_u32_column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{name}' not found in batch"))
    })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("Column '{name}' must be UInt32 type")))
}

/// Gets an optional f64 column from the batch by name
fn get_optional_f64_column_by_name<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Float64Array> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{name}' not found in batch"))
    })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("Column '{name}' must be Float64 type")))
}

/// Builds a map from INFO field name to column index
fn build_info_column_map(
    batch: &RecordBatch,
    info_fields: &[String],
) -> std::collections::HashMap<String, usize> {
    let mut map = std::collections::HashMap::new();
    for field_name in info_fields {
        if let Ok(idx) = batch.schema().index_of(field_name) {
            map.insert(field_name.clone(), idx);
        }
    }
    map
}

/// Builds a map from (sample_name, format_field) to column index
fn build_format_column_map(
    batch: &RecordBatch,
    format_fields: &[String],
    sample_names: &[String],
) -> std::collections::HashMap<(String, String), usize> {
    let mut map = std::collections::HashMap::new();
    let single_sample = sample_names.len() == 1;

    for sample_name in sample_names {
        for format_field in format_fields {
            // Column naming: single sample uses just format name, multi-sample uses sample_format
            let column_name = if single_sample {
                format_field.clone()
            } else {
                format!("{sample_name}_{format_field}")
            };

            if let Ok(idx) = batch.schema().index_of(&column_name) {
                map.insert((sample_name.clone(), format_field.clone()), idx);
            }
        }
    }
    map
}

/// Builds the INFO string from INFO columns
fn build_info_string(
    batch: &RecordBatch,
    row: usize,
    info_fields: &[String],
    info_columns: &std::collections::HashMap<String, usize>,
) -> Result<String> {
    let mut info_parts = Vec::new();

    for field_name in info_fields {
        let col_idx = match info_columns.get(field_name) {
            Some(&idx) => idx,
            None => continue, // Column not in batch, skip
        };

        let column = batch.column(col_idx);
        if column.is_null(row) {
            continue;
        }

        if let Some(value_str) = extract_info_value_string(column.as_ref(), row)? {
            if value_str == "true" {
                // Flag type - just include the name
                info_parts.push(field_name.clone());
            } else if value_str != "false" {
                info_parts.push(format!("{field_name}={value_str}"));
            }
        }
    }

    if info_parts.is_empty() {
        Ok(".".to_string())
    } else {
        Ok(info_parts.join(";"))
    }
}

/// Extracts an INFO value as a string from an Arrow array at a specific row
/// Supports both standard Arrow types and Polars "Large" variants (LargeUtf8, LargeList)
fn extract_info_value_string(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }

    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(arr.value(row).to_string()));
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(Some(format!("{:.6}", arr.value(row))));
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(format!("{:.6}", arr.value(row))));
    }

    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }

    // Handle both Utf8 (StringArray) and LargeUtf8 (LargeStringArray)
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }

    // Handle both List and LargeList
    if let Some(arr) = array.as_any().downcast_ref::<ListArray>() {
        let values = arr.value(row);
        let value_strings = extract_list_values(&values)?;
        if value_strings.is_empty() {
            return Ok(None);
        }
        return Ok(Some(value_strings.join(",")));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeListArray>() {
        let values = arr.value(row);
        let value_strings = extract_list_values(&values)?;
        if value_strings.is_empty() {
            return Ok(None);
        }
        return Ok(Some(value_strings.join(",")));
    }

    Ok(None)
}

/// Extracts values from a list array as strings
/// Supports both standard Arrow types and Polars "Large" variants
fn extract_list_values(array: &dyn Array) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let len = array.len();

    if let Some(int_arr) = array.as_any().downcast_ref::<Int32Array>() {
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(int_arr.value(i).to_string());
            }
        }
    } else if let Some(float_arr) = array.as_any().downcast_ref::<Float32Array>() {
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(format!("{:.6}", float_arr.value(i)));
            }
        }
    } else if let Some(str_arr) = array.as_any().downcast_ref::<StringArray>() {
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(str_arr.value(i).to_string());
            }
        }
    } else if let Some(str_arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        // Handle LargeUtf8 (Polars default string type)
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(str_arr.value(i).to_string());
            }
        }
    }

    Ok(values)
}

/// Builds FORMAT string and sample values using name-based column lookup
fn build_format_and_samples(
    batch: &RecordBatch,
    row: usize,
    format_fields: &[String],
    sample_names: &[String],
    format_columns: Option<&std::collections::HashMap<(String, String), usize>>,
) -> Result<(String, Vec<String>)> {
    if sample_names.is_empty() || format_fields.is_empty() {
        return Ok((String::new(), Vec::new()));
    }

    // Multisample sources keep FORMAT data in nested `genotypes` even when
    // only a subset (including one sample) is selected for output.
    let has_nested_genotypes = batch.schema().column_with_name("genotypes").is_some();
    if has_nested_genotypes {
        let samples = build_nested_multisample_values(batch, row, format_fields, sample_names)?;
        return Ok((format_fields.join(":"), samples));
    }

    let format_columns = format_columns.ok_or_else(|| {
        DataFusionError::Execution("Missing single-sample FORMAT column mapping".to_string())
    })?;

    // Build FORMAT string
    let format_str = format_fields.join(":");

    // Build sample values
    let mut samples = Vec::with_capacity(sample_names.len());

    for sample_name in sample_names {
        let mut sample_values = Vec::with_capacity(format_fields.len());

        for format_field in format_fields {
            let key = (sample_name.clone(), format_field.clone());
            let value = match format_columns.get(&key) {
                Some(&col_idx) => {
                    let column = batch.column(col_idx);
                    extract_sample_value_string(column.as_ref(), row)?
                }
                None => ".".to_string(), // Column not found, use missing value
            };
            sample_values.push(value);
        }

        samples.push(sample_values.join(":"));
    }

    Ok((format_str, samples))
}

fn build_nested_multisample_values(
    batch: &RecordBatch,
    row: usize,
    format_fields: &[String],
    sample_names: &[String],
) -> Result<Vec<String>> {
    let genotypes_idx = batch.schema().index_of("genotypes").map_err(|_| {
        DataFusionError::Execution(
            "Multisample output requires a 'genotypes' column in nested schema".to_string(),
        )
    })?;
    let genotypes_col = batch.column(genotypes_idx);
    if genotypes_col.is_null(row) {
        let missing = vec!["."; format_fields.len()].join(":");
        return Ok(vec![missing; sample_names.len()]);
    }

    let genotype_rows: Arc<dyn Array> =
        if let Some(list) = genotypes_col.as_any().downcast_ref::<ListArray>() {
            list.value(row)
        } else if let Some(list) = genotypes_col.as_any().downcast_ref::<LargeListArray>() {
            list.value(row)
        } else {
            return Err(DataFusionError::Execution(
                "Column 'genotypes' must be List or LargeList".to_string(),
            ));
        };

    let genotype_struct = genotype_rows
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("Column 'genotypes' list values must be Struct".to_string())
        })?;

    let sample_id_col = genotype_struct.column_by_name("sample_id").ok_or_else(|| {
        DataFusionError::Execution("Missing 'sample_id' in genotypes struct".to_string())
    })?;
    let sample_ids = sample_id_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("'sample_id' in genotypes must be Utf8".to_string())
        })?;

    let values_col = genotype_struct.column_by_name("values").ok_or_else(|| {
        DataFusionError::Execution("Missing 'values' in genotypes struct".to_string())
    })?;
    let values_struct = values_col
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("'values' in genotypes must be Struct".to_string())
        })?;

    let mut sample_map: HashMap<String, String> = HashMap::new();
    for i in 0..genotype_struct.len() {
        if sample_ids.is_null(i) {
            continue;
        }
        let sample_id = sample_ids.value(i).to_string();
        let mut values = Vec::with_capacity(format_fields.len());
        for format_field in format_fields {
            if let Some(col) = values_struct.column_by_name(format_field) {
                values.push(extract_sample_value_string(col.as_ref(), i)?);
            } else {
                values.push(".".to_string());
            }
        }
        sample_map.insert(sample_id, values.join(":"));
    }

    let mut samples = Vec::with_capacity(sample_names.len());
    for sample_name in sample_names {
        if let Some(sample_val) = sample_map.get(sample_name) {
            samples.push(sample_val.clone());
        } else {
            samples.push(vec!["."; format_fields.len()].join(":"));
        }
    }
    Ok(samples)
}

/// Extracts a sample/FORMAT value as a string from an Arrow array
/// Supports both standard Arrow types and Polars "Large" variants (LargeUtf8, LargeList)
fn extract_sample_value_string(array: &dyn Array, row: usize) -> Result<String> {
    if array.is_null(row) {
        return Ok(".".to_string());
    }

    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(arr.value(row).to_string());
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(format!("{:.6}", arr.value(row)));
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(format!("{:.6}", arr.value(row)));
    }

    // Handle both Utf8 (StringArray) and LargeUtf8 (LargeStringArray)
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        let s = arr.value(row);
        if s.is_empty() {
            return Ok(".".to_string());
        }
        return Ok(s.to_string());
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        let s = arr.value(row);
        if s.is_empty() {
            return Ok(".".to_string());
        }
        return Ok(s.to_string());
    }

    // Handle both List and LargeList
    if let Some(arr) = array.as_any().downcast_ref::<ListArray>() {
        let values = arr.value(row);
        let value_strings = extract_list_values(&values)?;
        if value_strings.is_empty() {
            return Ok(".".to_string());
        }
        return Ok(value_strings.join(","));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeListArray>() {
        let values = arr.value(row);
        let value_strings = extract_list_values(&values)?;
        if value_strings.is_empty() {
            return Ok(".".to_string());
        }
        return Ok(value_strings.join(","));
    }

    Ok(".".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Builder, ListBuilder, StringBuilder, StructBuilder};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_batch_to_vcf_lines_basic() {
        let schema = create_test_schema();

        let chroms = StringArray::from(vec!["chr1"]);
        let starts = UInt32Array::from(vec![99u32]); // 0-based
        let ends = UInt32Array::from(vec![100u32]);
        let ids = StringArray::from(vec![Some("rs123")]);
        let refs = StringArray::from(vec!["A"]);
        let alts = StringArray::from(vec!["G"]);
        let quals = Float64Array::from(vec![Some(30.0)]);
        let filters = StringArray::from(vec![Some("PASS")]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(chroms),
                Arc::new(starts),
                Arc::new(ends),
                Arc::new(ids),
                Arc::new(refs),
                Arc::new(alts),
                Arc::new(quals),
                Arc::new(filters),
            ],
        )
        .unwrap();

        let lines = batch_to_vcf_lines(&batch, &[], &[], &[], true).unwrap();

        assert_eq!(lines.len(), 1);
        assert!(lines[0].line.starts_with("chr1\t100\t")); // Position should be 100 (1-based)
    }

    #[test]
    fn test_batch_to_vcf_lines_null_values() {
        let schema = create_test_schema();

        let chroms = StringArray::from(vec!["chr1"]);
        let starts = UInt32Array::from(vec![99u32]);
        let ends = UInt32Array::from(vec![100u32]);
        let ids = StringArray::from(vec![None::<&str>]);
        let refs = StringArray::from(vec!["A"]);
        let alts = StringArray::from(vec!["."]);
        let quals = Float64Array::from(vec![None]);
        let filters = StringArray::from(vec![None::<&str>]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(chroms),
                Arc::new(starts),
                Arc::new(ends),
                Arc::new(ids),
                Arc::new(refs),
                Arc::new(alts),
                Arc::new(quals),
                Arc::new(filters),
            ],
        )
        .unwrap();

        let lines = batch_to_vcf_lines(&batch, &[], &[], &[], true).unwrap();

        assert_eq!(lines.len(), 1);
        // Check that null values are represented as "."
        assert!(lines[0].line.contains("\t.\t.\t.\t.")); // id, alt, qual, filter, info
    }

    #[test]
    fn test_batch_to_vcf_lines_multi_sample() {
        // Schema with nested multisample FORMAT data in `genotypes`
        let value_fields = vec![
            Field::new("GT", DataType::Utf8, true),
            Field::new("DP", DataType::Int32, true),
        ];
        let genotype_item_type = DataType::Struct(
            vec![
                Field::new("sample_id", DataType::Utf8, false),
                Field::new(
                    "values",
                    DataType::Struct(value_fields.clone().into()),
                    true,
                ),
            ]
            .into(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new(
                "genotypes",
                DataType::List(Arc::new(Field::new("item", genotype_item_type, true))),
                true,
            ),
        ]));

        let values_builder = StructBuilder::new(
            value_fields.clone(),
            vec![
                Box::new(StringBuilder::new()) as Box<dyn datafusion::arrow::array::ArrayBuilder>,
                Box::new(Int32Builder::new()) as Box<dyn datafusion::arrow::array::ArrayBuilder>,
            ],
        );
        let item_builder = StructBuilder::new(
            vec![
                Field::new("sample_id", DataType::Utf8, false),
                Field::new("values", DataType::Struct(value_fields.into()), true),
            ],
            vec![
                Box::new(StringBuilder::new()) as Box<dyn datafusion::arrow::array::ArrayBuilder>,
                Box::new(values_builder) as Box<dyn datafusion::arrow::array::ArrayBuilder>,
            ],
        );
        let mut genotypes_builder = ListBuilder::new(item_builder);

        {
            let item = genotypes_builder.values();
            item.field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value("SAMPLE1");
            let values = item.field_builder::<StructBuilder>(1).unwrap();
            values
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value("0/1");
            values
                .field_builder::<Int32Builder>(1)
                .unwrap()
                .append_value(25);
            values.append(true);
            item.append(true);

            item.field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value("SAMPLE2");
            let values = item.field_builder::<StructBuilder>(1).unwrap();
            values
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value("1/1");
            values
                .field_builder::<Int32Builder>(1)
                .unwrap()
                .append_value(30);
            values.append(true);
            item.append(true);

            genotypes_builder.append(true);
        }
        let genotypes = Arc::new(genotypes_builder.finish());

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![Some("rs123")])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                genotypes,
            ],
        )
        .unwrap();

        let sample_names = vec!["SAMPLE1".to_string(), "SAMPLE2".to_string()];
        let format_fields = vec!["GT".to_string(), "DP".to_string()];

        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &sample_names, true).unwrap();

        assert_eq!(lines.len(), 1);
        let line = &lines[0].line;

        // Should have FORMAT column and two sample columns
        assert!(line.contains("GT:DP"));
        assert!(line.contains("0/1:25")); // SAMPLE1
        assert!(line.contains("1/1:30")); // SAMPLE2
    }

    #[test]
    fn test_batch_to_vcf_lines_nested_multisample_single_selected_sample() {
        let value_fields = vec![
            Field::new("GT", DataType::Utf8, true),
            Field::new("DP", DataType::Int32, true),
        ];
        let genotype_item_type = DataType::Struct(
            vec![
                Field::new("sample_id", DataType::Utf8, false),
                Field::new(
                    "values",
                    DataType::Struct(value_fields.clone().into()),
                    true,
                ),
            ]
            .into(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new(
                "genotypes",
                DataType::List(Arc::new(Field::new("item", genotype_item_type, true))),
                true,
            ),
        ]));

        let values_builder = StructBuilder::new(
            value_fields.clone(),
            vec![
                Box::new(StringBuilder::new()) as Box<dyn datafusion::arrow::array::ArrayBuilder>,
                Box::new(Int32Builder::new()) as Box<dyn datafusion::arrow::array::ArrayBuilder>,
            ],
        );
        let item_builder = StructBuilder::new(
            vec![
                Field::new("sample_id", DataType::Utf8, false),
                Field::new("values", DataType::Struct(value_fields.into()), true),
            ],
            vec![
                Box::new(StringBuilder::new()) as Box<dyn datafusion::arrow::array::ArrayBuilder>,
                Box::new(values_builder) as Box<dyn datafusion::arrow::array::ArrayBuilder>,
            ],
        );
        let mut genotypes_builder = ListBuilder::new(item_builder);
        {
            let item = genotypes_builder.values();
            item.field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value("SAMPLE1");
            let values = item.field_builder::<StructBuilder>(1).unwrap();
            values
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value("0/1");
            values
                .field_builder::<Int32Builder>(1)
                .unwrap()
                .append_value(25);
            values.append(true);
            item.append(true);

            item.field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value("SAMPLE2");
            let values = item.field_builder::<StructBuilder>(1).unwrap();
            values
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value("1/1");
            values
                .field_builder::<Int32Builder>(1)
                .unwrap()
                .append_value(30);
            values.append(true);
            item.append(true);

            genotypes_builder.append(true);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![Some("rs123")])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                Arc::new(genotypes_builder.finish()),
            ],
        )
        .unwrap();

        let sample_names = vec!["SAMPLE1".to_string()];
        let format_fields = vec!["GT".to_string(), "DP".to_string()];

        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &sample_names, true).unwrap();
        assert_eq!(lines.len(), 1);
        let line = &lines[0].line;
        assert!(line.contains("GT:DP"));
        assert!(line.contains("0/1:25"));
        assert!(!line.contains("1/1:30"));
    }

    #[test]
    fn test_batch_to_vcf_lines_single_sample() {
        // Schema with FORMAT fields for single sample (no sample prefix)
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("GT", DataType::Utf8, true),
            Field::new("DP", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![Some("rs123")])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                Arc::new(StringArray::from(vec![Some("0/1")])),
                Arc::new(Int32Array::from(vec![Some(25)])),
            ],
        )
        .unwrap();

        let sample_names = vec!["SAMPLE1".to_string()];
        let format_fields = vec!["GT".to_string(), "DP".to_string()];

        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &sample_names, true).unwrap();

        assert_eq!(lines.len(), 1);
        let line = &lines[0].line;

        // Should have FORMAT column and one sample column
        assert!(line.contains("GT:DP"));
        assert!(line.contains("0/1:25"));
    }

    #[test]
    fn test_batch_to_vcf_lines_large_string_array() {
        // Test with LargeUtf8 (LargeStringArray) - Polars default string type
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::LargeUtf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::LargeUtf8, true),
            Field::new("ref", DataType::LargeUtf8, false),
            Field::new("alt", DataType::LargeUtf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::LargeUtf8, true),
        ]));

        let chroms = LargeStringArray::from(vec!["chr1"]);
        let starts = UInt32Array::from(vec![99u32]); // 0-based
        let ends = UInt32Array::from(vec![100u32]);
        let ids = LargeStringArray::from(vec![Some("rs456")]);
        let refs = LargeStringArray::from(vec!["C"]);
        let alts = LargeStringArray::from(vec!["T"]);
        let quals = Float64Array::from(vec![Some(45.0)]);
        let filters = LargeStringArray::from(vec![Some("PASS")]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(chroms),
                Arc::new(starts),
                Arc::new(ends),
                Arc::new(ids),
                Arc::new(refs),
                Arc::new(alts),
                Arc::new(quals),
                Arc::new(filters),
            ],
        )
        .unwrap();

        let lines = batch_to_vcf_lines(&batch, &[], &[], &[], true).unwrap();

        assert_eq!(lines.len(), 1);
        // Position should be 100 (1-based), ID should be rs456
        assert!(
            lines[0]
                .line
                .starts_with("chr1\t100\trs456\tC\tT\t45.00\tPASS")
        );
    }
}
