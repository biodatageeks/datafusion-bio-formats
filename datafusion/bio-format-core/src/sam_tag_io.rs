use crate::BAM_TAG_TYPE_KEY;
use crate::table_utils::OptionalField;
use crate::tag_registry::{get_known_tags, parse_sam_tag_type, sam_array_subtype_from_arrow_type};
use datafusion::arrow::array::{
    Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result};
use log::{debug, warn};
use noodles_sam::alignment::Record;
use noodles_sam::alignment::record::data::field::value::Array as SamArray;
use noodles_sam::alignment::record::data::field::{Tag, Value};
use noodles_sam::alignment::record_buf::Data as RecordData;
use noodles_sam::alignment::record_buf::data::field::Value as TagValue;
use std::collections::HashMap;
use std::io;

/// Shared optional-tag builders for BAM and CRAM scans.
#[derive(Default)]
pub struct TagBuilders {
    expected_types: Vec<DataType>,
    builders: Vec<OptionalField>,
    parsed_tags: Vec<Tag>,
}

impl TagBuilders {
    /// Creates tag builders for the requested optional fields.
    ///
    /// Fields are resolved from the projected schema first and then from the
    /// standard tag registry as a fallback.
    pub fn from_schema(
        batch_size: usize,
        tag_fields: Option<Vec<String>>,
        schema: SchemaRef,
    ) -> Self {
        let mut tag_builders = Self::default();

        if let Some(tags) = tag_fields {
            for tag in tags {
                let arrow_type = schema
                    .fields()
                    .iter()
                    .find(|field| field.name() == &tag)
                    .map(|field| field.data_type().clone())
                    .or_else(|| {
                        get_known_tags()
                            .get(&tag)
                            .map(|tag_def| tag_def.arrow_type.clone())
                    })
                    .unwrap_or(DataType::Utf8);

                debug!("Creating builder for tag {tag}: {arrow_type:?}");

                match OptionalField::new(&arrow_type, batch_size) {
                    Ok(builder) => {
                        let tag_bytes = tag.as_bytes();
                        if tag_bytes.len() == 2 {
                            tag_builders.expected_types.push(arrow_type);
                            tag_builders.builders.push(builder);
                            tag_builders
                                .parsed_tags
                                .push(Tag::from([tag_bytes[0], tag_bytes[1]]));
                            debug!("Successfully created builder for tag {tag}");
                        } else {
                            debug!("Invalid tag name length for {tag}");
                        }
                    }
                    Err(_) => debug!("Failed to create builder for tag {tag}: {arrow_type:?}"),
                }
            }
        }

        tag_builders
    }

    /// Returns the number of configured tag builders.
    pub fn len(&self) -> usize {
        self.expected_types.len()
    }

    /// Returns whether there are no configured tag builders.
    pub fn is_empty(&self) -> bool {
        self.expected_types.is_empty()
    }

    /// Returns the parsed 2-byte SAM tags in builder order.
    pub fn parsed_tags(&self) -> &[Tag] {
        &self.parsed_tags
    }

    /// Returns the underlying Arrow builders.
    pub fn builders_mut(&mut self) -> &mut [OptionalField] {
        &mut self.builders
    }
}

/// Builds an index map for parsed SAM tags.
pub fn build_tag_to_index(parsed_tags: &[Tag]) -> HashMap<Tag, usize> {
    parsed_tags
        .iter()
        .enumerate()
        .map(|(idx, tag)| (*tag, idx))
        .collect()
}

/// Builds alignment tag data from a record-batch row.
pub fn build_tag_data(
    row: usize,
    batch: &RecordBatch,
    tag_fields: &[String],
    tag_columns: &HashMap<String, usize>,
) -> Result<RecordData> {
    let mut data = RecordData::default();

    for tag_name in tag_fields {
        let Some(&col_idx) = tag_columns.get(tag_name) else {
            continue;
        };

        let column = batch.column(col_idx);
        if column.is_null(row) {
            continue;
        }

        let schema = batch.schema();
        let field = schema.field(col_idx);
        let sam_type = field
            .metadata()
            .get(BAM_TAG_TYPE_KEY)
            .map(String::as_str)
            .unwrap_or("Z");

        let tag_bytes = tag_name.as_bytes();
        if tag_bytes.len() != 2 {
            continue;
        }
        let tag = Tag::new(tag_bytes[0], tag_bytes[1]);

        if let Some(value) = arrow_to_sam_tag_value(column.as_ref(), row, sam_type)? {
            data.insert(tag, value);
        }
    }

    Ok(data)
}

/// Loads optional tags from a BAM or CRAM record into Arrow builders.
///
/// `malformed_tag_context` is used in warnings, e.g. `"BAM"` or `"CRAM"`.
/// `on_missing` can synthesize tags that are absent from the record. Return
/// `Ok(true)` when the tag was populated and should not be backfilled with null.
pub fn load_record_tags<R, F>(
    record: &R,
    tag_builders: &mut TagBuilders,
    tag_to_index: &HashMap<Tag, usize>,
    tag_populated: &mut [bool],
    malformed_tag_context: &str,
    mut on_missing: F,
) -> Result<(), ArrowError>
where
    R: Record,
    F: FnMut(usize, &mut OptionalField) -> Result<bool, ArrowError>,
{
    let num_tags = tag_builders.len();
    if num_tags == 0 {
        return Ok(());
    }

    tag_populated
        .iter_mut()
        .take(num_tags)
        .for_each(|value| *value = false);

    for result in record.data().iter() {
        match result {
            Ok((tag, value)) => {
                if let Some(&idx) = tag_to_index.get(&tag) {
                    tag_populated[idx] = true;
                    let expected_type = &tag_builders.expected_types[idx];
                    let builder = &mut tag_builders.builders[idx];
                    append_tag_value(builder, expected_type, value)?;
                }
            }
            Err(e) => {
                warn!(
                    "Skipping malformed {malformed_tag_context} tag while loading optional fields: {e}"
                );
            }
        }
    }

    for (idx, populated) in tag_populated.iter().copied().enumerate().take(num_tags) {
        if !populated {
            let builder = &mut tag_builders.builders[idx];
            if !on_missing(idx, builder)? {
                builder.append_null()?;
            }
        }
    }

    Ok(())
}

fn arrow_to_sam_tag_value(
    array: &dyn Array,
    row: usize,
    sam_type_spec: &str,
) -> Result<Option<TagValue>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let (sam_type, array_subtype) = parse_sam_tag_type(sam_type_spec)
        .map_err(|e| DataFusionError::Execution(format!("Invalid SAM tag type metadata: {e}")))?;

    match sam_type {
        'i' | 'c' | 's' | 'C' | 'S' | 'I' => {
            Ok(Some(arrow_to_integer_tag_value(array, row, sam_type)?))
        }
        'f' => Ok(Some(arrow_to_float_tag_value(array, row)?)),
        'Z' => Ok(Some(arrow_to_string_tag_value(array, row)?)),
        'H' => Ok(Some(arrow_to_hex_tag_value(array, row)?)),
        'A' => Ok(Some(arrow_to_character_tag_value(array, row)?)),
        'B' => Ok(Some(arrow_to_array_tag_value(array, row, array_subtype)?)),
        _ => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                Ok(Some(TagValue::from(arr.value(row))))
            } else {
                Ok(None)
            }
        }
    }
}

fn arrow_to_integer_tag_value(array: &dyn Array, row: usize, sam_type: char) -> Result<TagValue> {
    if let Some(value) = extract_signed_int(array, row) {
        return convert_integer_tag_value(value, sam_type);
    }

    if let Some(value) = extract_unsigned_int(array, row) {
        return convert_integer_tag_value(value, sam_type);
    }

    Err(DataFusionError::Execution(format!(
        "Tag value type mismatch for integer: {:?}",
        array.data_type()
    )))
}

fn convert_integer_tag_value<T>(value: T, sam_type: char) -> Result<TagValue>
where
    T: Copy + std::fmt::Display,
    i8: TryFrom<T>,
    i16: TryFrom<T>,
    u8: TryFrom<T>,
    u16: TryFrom<T>,
    i32: TryFrom<T>,
    u32: TryFrom<T>,
{
    match sam_type {
        'c' => Ok(TagValue::Int8(i8::try_from(value).map_err(|_| {
            DataFusionError::Execution(format!("Integer value {value} does not fit SAM type 'c'"))
        })?)),
        's' => Ok(TagValue::Int16(i16::try_from(value).map_err(|_| {
            DataFusionError::Execution(format!("Integer value {value} does not fit SAM type 's'"))
        })?)),
        'C' => Ok(TagValue::UInt8(u8::try_from(value).map_err(|_| {
            DataFusionError::Execution(format!("Integer value {value} does not fit SAM type 'C'"))
        })?)),
        'S' => Ok(TagValue::UInt16(u16::try_from(value).map_err(|_| {
            DataFusionError::Execution(format!("Integer value {value} does not fit SAM type 'S'"))
        })?)),
        'i' => Ok(TagValue::Int32(i32::try_from(value).map_err(|_| {
            DataFusionError::Execution(format!("Integer value {value} does not fit SAM type 'i'"))
        })?)),
        'I' => Ok(TagValue::UInt32(u32::try_from(value).map_err(|_| {
            DataFusionError::Execution(format!("Integer value {value} does not fit SAM type 'I'"))
        })?)),
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported SAM integer type '{sam_type}'"
        ))),
    }
}

fn arrow_to_float_tag_value(array: &dyn Array, row: usize) -> Result<TagValue> {
    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        Ok(TagValue::from(arr.value(row)))
    } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        let value = arr.value(row);
        if !value.is_finite() || value < f32::MIN as f64 || value > f32::MAX as f64 {
            return Err(DataFusionError::Execution(format!(
                "Float value {value} does not fit SAM type 'f'"
            )));
        }
        Ok(TagValue::from(value as f32))
    } else {
        Err(DataFusionError::Execution(format!(
            "Tag value type mismatch for float: {:?}",
            array.data_type()
        )))
    }
}

fn arrow_to_string_tag_value(array: &dyn Array, row: usize) -> Result<TagValue> {
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        Ok(TagValue::from(arr.value(row)))
    } else {
        Err(DataFusionError::Execution(format!(
            "Tag value type mismatch for string: {:?}",
            array.data_type()
        )))
    }
}

fn arrow_to_hex_tag_value(array: &dyn Array, row: usize) -> Result<TagValue> {
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Tag value type mismatch for hex string: {:?}",
                array.data_type()
            ))
        })?;
    let normalized = arr.value(row).to_ascii_uppercase();
    if normalized.len() % 2 != 0
        || !normalized
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'A'..=b'F'))
    {
        return Err(DataFusionError::Execution(format!(
            "Invalid SAM hex tag value '{normalized}'"
        )));
    }

    Ok(TagValue::Hex(normalized.into_bytes().into()))
}

fn arrow_to_character_tag_value(array: &dyn Array, row: usize) -> Result<TagValue> {
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        let value = arr.value(row);
        let bytes = value.as_bytes();
        if bytes.len() != 1 || !bytes[0].is_ascii() {
            return Err(DataFusionError::Execution(format!(
                "Character tags must be a single ASCII byte, got '{value}'"
            )));
        }
        Ok(TagValue::Character(bytes[0]))
    } else if let Some(value) = extract_unsigned_int(array, row) {
        let byte = u8::try_from(value).map_err(|_| {
            DataFusionError::Execution(format!(
                "Character tag value {value} does not fit into a single byte"
            ))
        })?;
        Ok(TagValue::Character(byte))
    } else if let Some(value) = extract_signed_int(array, row) {
        let byte = u8::try_from(value).map_err(|_| {
            DataFusionError::Execution(format!(
                "Character tag value {value} does not fit into a single byte"
            ))
        })?;
        Ok(TagValue::Character(byte))
    } else {
        Err(DataFusionError::Execution(format!(
            "Tag value type mismatch for character: {:?}",
            array.data_type()
        )))
    }
}

fn arrow_to_array_tag_value(
    array: &dyn Array,
    row: usize,
    array_subtype: Option<char>,
) -> Result<TagValue> {
    let arr = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Tag value type mismatch for array: {:?}",
            array.data_type()
        ))
    })?;
    let list = arr.value(row);

    let subtype = array_subtype
        .or_else(|| sam_array_subtype_from_arrow_type(list.data_type()))
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Unable to determine SAM array subtype for Arrow type {:?}",
                list.data_type()
            ))
        })?;

    match subtype {
        'c' => Ok(TagValue::from(collect_array_as_i8(list.as_ref())?)),
        'C' => Ok(TagValue::from(collect_array_as_u8(list.as_ref())?)),
        's' => Ok(TagValue::from(collect_array_as_i16(list.as_ref())?)),
        'S' => Ok(TagValue::from(collect_array_as_u16(list.as_ref())?)),
        'i' => Ok(TagValue::from(collect_array_as_i32(list.as_ref())?)),
        'I' => Ok(TagValue::from(collect_array_as_u32(list.as_ref())?)),
        'f' => Ok(TagValue::from(collect_array_as_f32(list.as_ref())?)),
        _ => Err(DataFusionError::Execution(format!(
            "Unsupported SAM array subtype '{subtype}'"
        ))),
    }
}

fn extract_signed_int(array: &dyn Array, row: usize) -> Option<i64> {
    array
        .as_any()
        .downcast_ref::<Int8Array>()
        .map(|arr| i64::from(arr.value(row)))
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(|arr| i64::from(arr.value(row)))
        })
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(|arr| i64::from(arr.value(row)))
        })
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|arr| arr.value(row))
        })
}

fn extract_unsigned_int(array: &dyn Array, row: usize) -> Option<u64> {
    array
        .as_any()
        .downcast_ref::<UInt8Array>()
        .map(|arr| u64::from(arr.value(row)))
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .map(|arr| u64::from(arr.value(row)))
        })
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .map(|arr| u64::from(arr.value(row)))
        })
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .map(|arr| arr.value(row))
        })
}

fn ensure_non_null_list_elements(array: &dyn Array) -> Result<()> {
    if array.null_count() > 0 {
        return Err(DataFusionError::Execution(
            "SAM array tags cannot contain null elements".to_string(),
        ));
    }

    Ok(())
}

fn collect_array_as_i8(array: &dyn Array) -> Result<Vec<i8>> {
    ensure_non_null_list_elements(array)?;

    (0..array.len())
        .map(|idx| {
            if let Some(value) = extract_signed_int(array, idx) {
                i8::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'c'"
                    ))
                })
            } else if let Some(value) = extract_unsigned_int(array, idx) {
                i8::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'c'"
                    ))
                })
            } else {
                Err(DataFusionError::Execution(format!(
                    "Unsupported array element type for SAM subtype 'c': {:?}",
                    array.data_type()
                )))
            }
        })
        .collect()
}

fn collect_array_as_u8(array: &dyn Array) -> Result<Vec<u8>> {
    ensure_non_null_list_elements(array)?;

    (0..array.len())
        .map(|idx| {
            if let Some(value) = extract_unsigned_int(array, idx) {
                u8::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'C'"
                    ))
                })
            } else if let Some(value) = extract_signed_int(array, idx) {
                u8::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'C'"
                    ))
                })
            } else {
                Err(DataFusionError::Execution(format!(
                    "Unsupported array element type for SAM subtype 'C': {:?}",
                    array.data_type()
                )))
            }
        })
        .collect()
}

fn collect_array_as_i16(array: &dyn Array) -> Result<Vec<i16>> {
    ensure_non_null_list_elements(array)?;

    (0..array.len())
        .map(|idx| {
            if let Some(value) = extract_signed_int(array, idx) {
                i16::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 's'"
                    ))
                })
            } else if let Some(value) = extract_unsigned_int(array, idx) {
                i16::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 's'"
                    ))
                })
            } else {
                Err(DataFusionError::Execution(format!(
                    "Unsupported array element type for SAM subtype 's': {:?}",
                    array.data_type()
                )))
            }
        })
        .collect()
}

fn collect_array_as_u16(array: &dyn Array) -> Result<Vec<u16>> {
    ensure_non_null_list_elements(array)?;

    (0..array.len())
        .map(|idx| {
            if let Some(value) = extract_unsigned_int(array, idx) {
                u16::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'S'"
                    ))
                })
            } else if let Some(value) = extract_signed_int(array, idx) {
                u16::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'S'"
                    ))
                })
            } else {
                Err(DataFusionError::Execution(format!(
                    "Unsupported array element type for SAM subtype 'S': {:?}",
                    array.data_type()
                )))
            }
        })
        .collect()
}

fn collect_array_as_i32(array: &dyn Array) -> Result<Vec<i32>> {
    ensure_non_null_list_elements(array)?;

    (0..array.len())
        .map(|idx| {
            if let Some(value) = extract_signed_int(array, idx) {
                i32::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'i'"
                    ))
                })
            } else if let Some(value) = extract_unsigned_int(array, idx) {
                i32::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'i'"
                    ))
                })
            } else {
                Err(DataFusionError::Execution(format!(
                    "Unsupported array element type for SAM subtype 'i': {:?}",
                    array.data_type()
                )))
            }
        })
        .collect()
}

fn collect_array_as_u32(array: &dyn Array) -> Result<Vec<u32>> {
    ensure_non_null_list_elements(array)?;

    (0..array.len())
        .map(|idx| {
            if let Some(value) = extract_unsigned_int(array, idx) {
                u32::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'I'"
                    ))
                })
            } else if let Some(value) = extract_signed_int(array, idx) {
                u32::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'I'"
                    ))
                })
            } else {
                Err(DataFusionError::Execution(format!(
                    "Unsupported array element type for SAM subtype 'I': {:?}",
                    array.data_type()
                )))
            }
        })
        .collect()
}

fn collect_array_as_f32(array: &dyn Array) -> Result<Vec<f32>> {
    ensure_non_null_list_elements(array)?;

    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok((0..array.len()).map(|idx| arr.value(idx)).collect());
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return (0..array.len())
            .map(|idx| {
                let value = arr.value(idx);
                if !value.is_finite() || value < f32::MIN as f64 || value > f32::MAX as f64 {
                    return Err(DataFusionError::Execution(format!(
                        "Array element {value} does not fit SAM subtype 'f'"
                    )));
                }
                Ok(value as f32)
            })
            .collect();
    }

    Err(DataFusionError::Execution(format!(
        "Unsupported array element type for SAM subtype 'f': {:?}",
        array.data_type()
    )))
}

fn append_tag_value(
    builder: &mut OptionalField,
    expected_type: &DataType,
    value: Value<'_>,
) -> Result<(), ArrowError> {
    match value {
        Value::Int8(value) => append_int_value(builder, expected_type, i64::from(value)),
        Value::UInt8(value) => append_uint_value(builder, expected_type, u32::from(value)),
        Value::Int16(value) => append_int_value(builder, expected_type, i64::from(value)),
        Value::UInt16(value) => append_uint_value(builder, expected_type, u32::from(value)),
        Value::Int32(value) => append_int_value(builder, expected_type, i64::from(value)),
        Value::UInt32(value) => append_uint_value(builder, expected_type, value),
        Value::Float(value) => {
            if matches!(expected_type, DataType::Utf8) {
                builder.append_string(&value.to_string())
            } else {
                builder.append_float(value)
            }
        }
        Value::String(value) => match std::str::from_utf8(value.as_ref()) {
            Ok(string) => builder.append_string(string),
            Err(_) => builder.append_null(),
        },
        Value::Character(value) => {
            if matches!(expected_type, DataType::UInt32) {
                builder.append_uint(u32::from(value))
            } else if matches!(expected_type, DataType::Int32) {
                builder.append_int(i32::from(value))
            } else {
                builder.append_string(&char::from(value).to_string())
            }
        }
        Value::Hex(value) => match std::str::from_utf8(value.as_ref()) {
            Ok(hex_str) => builder.append_string(hex_str),
            Err(_) => builder.append_null(),
        },
        Value::Array(value) => append_array_value(builder, expected_type, value),
    }
}

fn append_int_value(
    builder: &mut OptionalField,
    expected_type: &DataType,
    value: i64,
) -> Result<(), ArrowError> {
    if matches!(expected_type, DataType::Utf8) {
        if let Some(ch) = u32::try_from(value).ok().and_then(char::from_u32) {
            builder.append_string(&ch.to_string())
        } else {
            builder.append_string(&value.to_string())
        }
    } else if matches!(expected_type, DataType::UInt32) {
        let converted = u32::try_from(value).map_err(|_| {
            ArrowError::SchemaError(format!("integer value {value} does not fit UInt32"))
        })?;
        builder.append_uint(converted)
    } else {
        let converted = i32::try_from(value).map_err(|_| {
            ArrowError::SchemaError(format!("integer value {value} does not fit Int32"))
        })?;
        builder.append_int(converted)
    }
}

fn append_uint_value(
    builder: &mut OptionalField,
    expected_type: &DataType,
    value: u32,
) -> Result<(), ArrowError> {
    if matches!(expected_type, DataType::Utf8) {
        if let Some(ch) = char::from_u32(value) {
            builder.append_string(&ch.to_string())
        } else {
            builder.append_string(&value.to_string())
        }
    } else if matches!(expected_type, DataType::UInt32) {
        builder.append_uint(value)
    } else {
        let converted = i32::try_from(value).map_err(|_| {
            ArrowError::SchemaError(format!("unsigned integer value {value} does not fit Int32"))
        })?;
        builder.append_int(converted)
    }
}

fn type_mismatch(expected_type: &DataType, actual: &str) -> ArrowError {
    ArrowError::SchemaError(format!(
        "tag value type mismatch: expected {expected_type:?}, got {actual}"
    ))
}

fn collect_array_values<T, U>(
    iter: impl Iterator<Item = io::Result<T>>,
    mut convert: impl FnMut(T) -> Result<U, ArrowError>,
) -> Result<Vec<U>, ArrowError> {
    iter.map(|result| {
        result
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))
            .and_then(&mut convert)
    })
    .collect()
}

fn append_array_value(
    builder: &mut OptionalField,
    expected_type: &DataType,
    arr: SamArray<'_>,
) -> Result<(), ArrowError> {
    let DataType::List(field) = expected_type else {
        return Err(type_mismatch(expected_type, "array"));
    };

    match field.data_type() {
        DataType::Int8 => match arr {
            SamArray::Int8(values) => {
                builder.append_array_int8_iter(collect_array_values(values.iter(), Ok)?.into_iter())
            }
            SamArray::UInt8(values) => builder.append_array_int8_iter(
                collect_array_values(values.iter(), |value| {
                    i8::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit Int8"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::Int16(values) => builder.append_array_int8_iter(
                collect_array_values(values.iter(), |value| {
                    i8::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit Int8"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt16(values) => builder.append_array_int8_iter(
                collect_array_values(values.iter(), |value| {
                    i8::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit Int8"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::Int32(values) => builder.append_array_int8_iter(
                collect_array_values(values.iter(), |value| {
                    i8::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit Int8"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt32(values) => builder.append_array_int8_iter(
                collect_array_values(values.iter(), |value| {
                    i8::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit Int8"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::Float(_) => Err(type_mismatch(expected_type, "float array")),
        },
        DataType::UInt8 => match arr {
            SamArray::Int8(values) => builder.append_array_uint8_iter(
                collect_array_values(values.iter(), |value| {
                    u8::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit UInt8"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt8(values) => builder
                .append_array_uint8_iter(collect_array_values(values.iter(), Ok)?.into_iter()),
            SamArray::Int16(values) => builder.append_array_uint8_iter(
                collect_array_values(values.iter(), |value| {
                    u8::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit UInt8"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt16(values) => builder.append_array_uint8_iter(
                collect_array_values(values.iter(), |value| {
                    u8::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit UInt8"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::Int32(values) => builder.append_array_uint8_iter(
                collect_array_values(values.iter(), |value| {
                    u8::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit UInt8"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt32(values) => builder.append_array_uint8_iter(
                collect_array_values(values.iter(), |value| {
                    u8::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit UInt8"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::Float(_) => Err(type_mismatch(expected_type, "float array")),
        },
        DataType::Int16 => match arr {
            SamArray::Int8(values) => builder.append_array_int16_iter(
                collect_array_values(values.iter(), |value| Ok(i16::from(value)))?.into_iter(),
            ),
            SamArray::UInt8(values) => builder.append_array_int16_iter(
                collect_array_values(values.iter(), |value| Ok(i16::from(value)))?.into_iter(),
            ),
            SamArray::Int16(values) => builder
                .append_array_int16_iter(collect_array_values(values.iter(), Ok)?.into_iter()),
            SamArray::UInt16(values) => builder.append_array_int16_iter(
                collect_array_values(values.iter(), |value| {
                    i16::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit Int16"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::Int32(values) => builder.append_array_int16_iter(
                collect_array_values(values.iter(), |value| {
                    i16::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit Int16"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt32(values) => builder.append_array_int16_iter(
                collect_array_values(values.iter(), |value| {
                    i16::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit Int16"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::Float(_) => Err(type_mismatch(expected_type, "float array")),
        },
        DataType::UInt16 => match arr {
            SamArray::Int8(values) => builder.append_array_uint16_iter(
                collect_array_values(values.iter(), |value| {
                    u16::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "array element {value} does not fit UInt16"
                        ))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt8(values) => builder.append_array_uint16_iter(
                collect_array_values(values.iter(), |value| Ok(u16::from(value)))?.into_iter(),
            ),
            SamArray::Int16(values) => builder.append_array_uint16_iter(
                collect_array_values(values.iter(), |value| {
                    u16::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "array element {value} does not fit UInt16"
                        ))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt16(values) => builder
                .append_array_uint16_iter(collect_array_values(values.iter(), Ok)?.into_iter()),
            SamArray::Int32(values) => builder.append_array_uint16_iter(
                collect_array_values(values.iter(), |value| {
                    u16::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "array element {value} does not fit UInt16"
                        ))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt32(values) => builder.append_array_uint16_iter(
                collect_array_values(values.iter(), |value| {
                    u16::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "array element {value} does not fit UInt16"
                        ))
                    })
                })?
                .into_iter(),
            ),
            SamArray::Float(_) => Err(type_mismatch(expected_type, "float array")),
        },
        DataType::Int32 => match arr {
            SamArray::Int8(values) => builder.append_array_int_iter(
                collect_array_values(values.iter(), |value| Ok(i32::from(value)))?.into_iter(),
            ),
            SamArray::UInt8(values) => builder.append_array_int_iter(
                collect_array_values(values.iter(), |value| Ok(i32::from(value)))?.into_iter(),
            ),
            SamArray::Int16(values) => builder.append_array_int_iter(
                collect_array_values(values.iter(), |value| Ok(i32::from(value)))?.into_iter(),
            ),
            SamArray::UInt16(values) => builder.append_array_int_iter(
                collect_array_values(values.iter(), |value| Ok(i32::from(value)))?.into_iter(),
            ),
            SamArray::Int32(values) => {
                builder.append_array_int_iter(collect_array_values(values.iter(), Ok)?.into_iter())
            }
            SamArray::UInt32(values) => builder.append_array_int_iter(
                collect_array_values(values.iter(), |value| {
                    i32::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!("array element {value} does not fit Int32"))
                    })
                })?
                .into_iter(),
            ),
            SamArray::Float(_) => Err(type_mismatch(expected_type, "float array")),
        },
        DataType::UInt32 => match arr {
            SamArray::Int8(values) => builder.append_array_uint32_iter(
                collect_array_values(values.iter(), |value| {
                    u32::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "array element {value} does not fit UInt32"
                        ))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt8(values) => builder.append_array_uint32_iter(
                collect_array_values(values.iter(), |value| Ok(u32::from(value)))?.into_iter(),
            ),
            SamArray::Int16(values) => builder.append_array_uint32_iter(
                collect_array_values(values.iter(), |value| {
                    u32::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "array element {value} does not fit UInt32"
                        ))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt16(values) => builder.append_array_uint32_iter(
                collect_array_values(values.iter(), |value| Ok(u32::from(value)))?.into_iter(),
            ),
            SamArray::Int32(values) => builder.append_array_uint32_iter(
                collect_array_values(values.iter(), |value| {
                    u32::try_from(value).map_err(|_| {
                        ArrowError::SchemaError(format!(
                            "array element {value} does not fit UInt32"
                        ))
                    })
                })?
                .into_iter(),
            ),
            SamArray::UInt32(values) => builder
                .append_array_uint32_iter(collect_array_values(values.iter(), Ok)?.into_iter()),
            SamArray::Float(_) => Err(type_mismatch(expected_type, "float array")),
        },
        DataType::Float32 => match arr {
            SamArray::Int8(values) => builder.append_array_float_iter(
                collect_array_values(values.iter(), |value| Ok(f32::from(value)))?.into_iter(),
            ),
            SamArray::UInt8(values) => builder.append_array_float_iter(
                collect_array_values(values.iter(), |value| Ok(f32::from(value)))?.into_iter(),
            ),
            SamArray::Int16(values) => builder.append_array_float_iter(
                collect_array_values(values.iter(), |value| Ok(f32::from(value)))?.into_iter(),
            ),
            SamArray::UInt16(values) => builder.append_array_float_iter(
                collect_array_values(values.iter(), |value| Ok(f32::from(value)))?.into_iter(),
            ),
            SamArray::Int32(values) => builder.append_array_float_iter(
                collect_array_values(values.iter(), |value| Ok(value as f32))?.into_iter(),
            ),
            SamArray::UInt32(values) => builder.append_array_float_iter(
                collect_array_values(values.iter(), |value| Ok(value as f32))?.into_iter(),
            ),
            SamArray::Float(values) => builder
                .append_array_float_iter(collect_array_values(values.iter(), Ok)?.into_iter()),
        },
        _ => Err(type_mismatch(expected_type, "unsupported array")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_utils::builders_to_arrays;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use noodles_sam::alignment::RecordBuf;
    use noodles_sam::alignment::record::data::field::Tag;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_arrow_to_integer_tag_value_uses_signed_type_for_i_metadata() -> Result<()> {
        let values = UInt32Array::from(vec![42u32]);
        let value = arrow_to_integer_tag_value(&values, 0, 'i')?;
        assert_eq!(value, TagValue::Int32(42));

        let values = UInt32Array::from(vec![u32::MAX]);
        assert!(arrow_to_integer_tag_value(&values, 0, 'i').is_err());

        Ok(())
    }

    #[test]
    fn test_build_tag_to_index_maps_tags() {
        let parsed_tags = [Tag::from([b'N', b'M']), Tag::from([b'M', b'D'])];
        let tag_to_index = build_tag_to_index(&parsed_tags);

        assert_eq!(tag_to_index[&Tag::from([b'N', b'M'])], 0);
        assert_eq!(tag_to_index[&Tag::from([b'M', b'D'])], 1);
    }

    #[test]
    fn test_build_tag_data_uses_schema_metadata() -> Result<()> {
        let mut field_metadata = HashMap::new();
        field_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), "i".to_string());

        let schema = Arc::new(Schema::new(vec![
            Field::new("NM", DataType::Int32, true).with_metadata(field_metadata),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![Some(7)]))]).unwrap();

        let tag_fields = vec!["NM".to_string()];
        let tag_columns = HashMap::from([(String::from("NM"), 0_usize)]);
        let data = build_tag_data(0, &batch, &tag_fields, &tag_columns)?;

        assert_eq!(
            data.get(&Tag::from([b'N', b'M'])),
            Some(&TagValue::Int32(7))
        );

        Ok(())
    }

    #[test]
    fn test_load_record_tags_reads_present_values_and_backfills_nulls() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("NM", DataType::Int32, true),
            Field::new("MD", DataType::Utf8, true),
        ]));
        let mut tag_builders =
            TagBuilders::from_schema(1, Some(vec!["NM".to_string(), "MD".to_string()]), schema);
        let tag_to_index = build_tag_to_index(tag_builders.parsed_tags());
        let mut tag_populated = vec![false; tag_builders.len()];

        let mut record = RecordBuf::default();
        record
            .data_mut()
            .insert(Tag::from([b'N', b'M']), TagValue::Int32(3));

        load_record_tags(
            &record,
            &mut tag_builders,
            &tag_to_index,
            &mut tag_populated,
            "BAM",
            |_, _| Ok(false),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        assert_eq!(tag_populated, vec![true, false]);

        let arrays = builders_to_arrays(tag_builders.builders_mut());
        let nm = arrays[0].as_any().downcast_ref::<Int32Array>().unwrap();
        let md = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(nm.value(0), 3);
        assert!(md.is_null(0));

        Ok(())
    }
}
