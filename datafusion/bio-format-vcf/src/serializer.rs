//! Serializer for converting Arrow RecordBatches to VCF records
//!
//! This module provides functionality for converting DataFusion Arrow data back
//! to VCF format for writing to files.

use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, LargeListArray, LargeStringArray,
    ListArray, RecordBatch, StringArray, StringViewArray, StructArray, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::{
    GENOTYPE_OUTPUT_MODE_KEY,
    metadata::{
        VCF_FIELD_FORMAT_ID_KEY, VCF_GENOTYPE_OUTPUT_MODE_KEY, VCF_RECORD_LAYOUT_FORMAT_KEYS,
        VCF_RECORD_LAYOUT_INFO_KEYS, VCF_RECORD_LAYOUT_KEY,
    },
};

const DOSAGE_MODE: &str = "dosage";

fn field_is_gt(field: &Field) -> bool {
    field.name() == "GT"
        || field
            .metadata()
            .get(VCF_FIELD_FORMAT_ID_KEY)
            .is_some_and(|format_id| format_id == "GT")
}

fn list_has_int8_items(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(item) | DataType::LargeList(item) => item.data_type() == &DataType::Int8,
        _ => false,
    }
}

fn schema_has_dosage_gt(schema: &SchemaRef) -> bool {
    let declares_dosage = [GENOTYPE_OUTPUT_MODE_KEY, VCF_GENOTYPE_OUTPUT_MODE_KEY]
        .into_iter()
        .any(|key| {
            schema
                .metadata()
                .get(key)
                .is_some_and(|mode| mode == DOSAGE_MODE)
        });

    declares_dosage
        || schema.fields().iter().any(|field| {
            (field_is_gt(field) && field.data_type() == &DataType::Int8)
                || (field.name() == "genotypes"
                    && matches!(field.data_type(), DataType::Struct(children) if children
                        .iter()
                        .any(|child| field_is_gt(child) && list_has_int8_items(child.data_type()))))
        })
}

pub(crate) fn validate_vcf_serializable_genotypes(schema: &SchemaRef) -> Result<()> {
    if schema_has_dosage_gt(schema) {
        return Err(DataFusionError::Plan(
            "VCF serialization does not support genotype dosage input; nullable Int8 ALT counts \
             cannot reconstruct the original GT allele calls, ploidy, or phase. Read BCF with \
             string GT representation before writing VCF."
                .into(),
        ));
    }

    Ok(())
}

/// Enum to hold StringArray, LargeStringArray, or StringViewArray reference.
/// This allows handling standard Arrow Utf8, Polars LargeUtf8, and DataFusion Utf8View types.
enum StringColumnRef<'a> {
    Small(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl StringColumnRef<'_> {
    fn value(&self, i: usize) -> &str {
        match self {
            StringColumnRef::Small(arr) => arr.value(i),
            StringColumnRef::Large(arr) => arr.value(i),
            StringColumnRef::View(arr) => arr.value(i),
        }
    }

    fn is_null(&self, i: usize) -> bool {
        match self {
            StringColumnRef::Small(arr) => Array::is_null(*arr, i),
            StringColumnRef::Large(arr) => Array::is_null(*arr, i),
            StringColumnRef::View(arr) => Array::is_null(*arr, i),
        }
    }
}

/// Formats a float value for VCF output, matching C's `%g` formatting
/// (6 significant digits, trailing zeros trimmed).
///
/// - Fixed notation when the exponent is in [-4, 6) (matching C `%g` rules)
/// - Scientific notation otherwise
/// - Trailing zeros and unnecessary decimal points removed
/// - Non-finite (`NaN`, `±Inf`) → `"."` (VCF missing value)
fn format_vcf_float(v: f64) -> String {
    // Every non-finite value renders as missing: VCF defines no textual form for
    // an infinity, and the exponent parsing below cannot handle one. Guarding
    // only NaN left `format!("{:.5e}", f64::INFINITY)` == "inf", which has no
    // 'e' to split on, so the `split_once` unwrap panicked on real data — any
    // INFO or FORMAT Float column carrying an infinity reached it.
    if !v.is_finite() {
        return ".".to_string();
    }
    // Use Rust's {:.*e} to get scientific form, then decide notation
    // C %g uses 6 significant digits by default
    let formatted = format!("{v:.5e}"); // 5 digits after point = 6 sig digits
    // Parse the exponent
    let (mantissa_str, exp_str) = formatted.split_once('e').unwrap();
    let exp: i32 = exp_str.parse().unwrap();

    if (-4..6).contains(&exp) {
        // Use fixed notation with enough decimal places for 6 sig digits
        let decimal_places = if exp >= 0 {
            let dp = 5 - exp; // 6 sig digits - (exp+1) integer digits
            if dp < 0 { 0 } else { dp as usize }
        } else {
            (5 - exp) as usize // more decimals needed for small numbers
        };
        let fixed = format!("{v:.decimal_places$}");
        // Trim trailing zeros after decimal point
        if fixed.contains('.') {
            let trimmed = fixed.trim_end_matches('0').trim_end_matches('.');
            if trimmed.is_empty() || trimmed == "-" {
                "0".to_string()
            } else {
                trimmed.to_string()
            }
        } else {
            fixed
        }
    } else {
        // Scientific notation: reconstruct from parsed parts
        let mantissa_trimmed = mantissa_str.trim_end_matches('0').trim_end_matches('.');
        if exp >= 0 {
            format!("{mantissa_trimmed}e+{exp:02}")
        } else {
            format!(
                "{mantissa_trimmed}e-{exp_abs:02}",
                exp_abs = exp.unsigned_abs()
            )
        }
    }
}

/// Formats a QUAL score for VCF output as the shortest text that parses back to
/// the same value, at the precision the value actually carries.
///
/// QUAL is deliberately not `format_vcf_float`: `%g` caps at six significant
/// digits, so a precise score like `123456.78` would be rewritten as `123457`.
///
/// The precision question is not academic. VCF QUAL is `f32` on the wire —
/// noodles returns `Option<f32>` and `physical_exec.rs` widens it with
/// `v as f64` — so an input of `29.99` reaches this function as
/// 29.989999771118164. Printing the f64 shortest form would write that binary
/// noise back out. A value that survives a round trip through `f32` is rendered
/// at `f32` precision, recovering the original text; anything needing more than
/// `f32` can represent is a caller-supplied `f64` and keeps full precision.
///
/// `NaN` and `±Inf` have no VCF representation and render as `"."`.
fn format_vcf_qual(qual: f64) -> String {
    if !qual.is_finite() {
        return ".".to_string();
    }
    let narrowed = qual as f32;
    if narrowed as f64 == qual {
        format!("{narrowed}")
    } else {
        format!("{qual}")
    }
}

// ---------------------------------------------------------------------------
// Batch-level resolved genotypes: eliminate per-row downcasts & array slices
// ---------------------------------------------------------------------------

/// Pre-resolved typed values from a list column's inner array.
/// Resolved once per batch to eliminate per-element downcast chains.
enum TypedValues<'a> {
    Int32(&'a Int32Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    /// Fallback for nested or uncommon types (e.g. List<List<T>>).
    Other(&'a dyn Array),
}

/// Pre-resolved field data for a single FORMAT field in the genotypes struct.
enum ResolvedFieldData<'a> {
    List {
        list: &'a ListArray,
        values: TypedValues<'a>,
    },
    LargeList {
        list: &'a LargeListArray,
        values: TypedValues<'a>,
    },
    /// Field not found in the genotypes struct.
    Missing,
}

/// Pre-resolved genotype field (name + resolved data).
struct ResolvedGenotypeField<'a> {
    name: &'a str,
    data: ResolvedFieldData<'a>,
}

/// Pre-resolved genotypes for an entire batch. Type resolution is done once
/// per batch instead of per-row x per-field x per-sample.
struct ResolvedGenotypes<'a> {
    struct_array: &'a StructArray,
    fields: Vec<ResolvedGenotypeField<'a>>,
}

/// Resolves the typed values array from an Arrow array (single downcast per batch per field).
fn resolve_typed_values(array: &dyn Array) -> TypedValues<'_> {
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        TypedValues::Int32(a)
    } else if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        TypedValues::Float32(a)
    } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        TypedValues::Float64(a)
    } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        TypedValues::Utf8(a)
    } else if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        TypedValues::LargeUtf8(a)
    } else if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        TypedValues::Utf8View(a)
    } else {
        TypedValues::Other(array)
    }
}

/// Resolves the field data for a single FORMAT column (list array + typed values).
fn resolve_field_data(array: &dyn Array) -> ResolvedFieldData<'_> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let values = resolve_typed_values(list.values().as_ref());
        ResolvedFieldData::List { list, values }
    } else if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        let values = resolve_typed_values(list.values().as_ref());
        ResolvedFieldData::LargeList { list, values }
    } else {
        ResolvedFieldData::Missing
    }
}

/// Pre-resolves all genotype columns for a batch. Returns `None` if there is
/// no `genotypes` struct column (single-sample flat schema).
fn resolve_batch_genotypes<'a>(
    batch: &'a RecordBatch,
    format_fields: &'a [String],
) -> Option<ResolvedGenotypes<'a>> {
    let idx = batch.schema().index_of("genotypes").ok()?;
    let col = batch.column(idx);
    let struct_array = col.as_any().downcast_ref::<StructArray>()?;

    let fields = format_fields
        .iter()
        .map(|field_name| {
            let data = match struct_array.column_by_name(field_name) {
                Some(list_col) => resolve_field_data(list_col.as_ref()),
                None => ResolvedFieldData::Missing,
            };
            ResolvedGenotypeField {
                name: field_name.as_str(),
                data,
            }
        })
        .collect();

    Some(ResolvedGenotypes {
        struct_array,
        fields,
    })
}

/// Checks if a single value at `flat_idx` is missing (would produce "." in VCF output).
fn is_value_missing(values: &TypedValues, flat_idx: usize) -> bool {
    match values {
        TypedValues::Int32(a) => a.is_null(flat_idx),
        // Non-finite floats (NaN and ±Inf alike) serialize as ".", so the
        // pruning predicate has to classify them as missing too — otherwise a
        // field that renders as "." for every sample is still emitted.
        TypedValues::Float32(a) => a.is_null(flat_idx) || !a.value(flat_idx).is_finite(),
        TypedValues::Float64(a) => a.is_null(flat_idx) || !a.value(flat_idx).is_finite(),
        TypedValues::Utf8(a) => {
            a.is_null(flat_idx) || {
                let s = a.value(flat_idx);
                s.is_empty() || s == "."
            }
        }
        TypedValues::LargeUtf8(a) => {
            a.is_null(flat_idx) || {
                let s = a.value(flat_idx);
                s.is_empty() || s == "."
            }
        }
        TypedValues::Utf8View(a) => {
            a.is_null(flat_idx) || {
                let s = a.value(flat_idx);
                s.is_empty() || s == "."
            }
        }
        TypedValues::Other(a) => a.is_null(flat_idx),
    }
}

/// Writes a single typed value at `flat_idx` directly into the line buffer.
fn write_typed_value(values: &TypedValues, flat_idx: usize, buf: &mut String) -> Result<()> {
    use std::fmt::Write;
    match values {
        TypedValues::Int32(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                write!(buf, "{}", a.value(flat_idx)).unwrap();
            }
        }
        TypedValues::Float32(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                buf.push_str(&format_vcf_float(a.value(flat_idx) as f64));
            }
        }
        TypedValues::Float64(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                buf.push_str(&format_vcf_float(a.value(flat_idx)));
            }
        }
        TypedValues::Utf8(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                let s = a.value(flat_idx);
                if s.is_empty() {
                    buf.push('.');
                } else {
                    buf.push_str(s);
                }
            }
        }
        TypedValues::LargeUtf8(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                let s = a.value(flat_idx);
                if s.is_empty() {
                    buf.push('.');
                } else {
                    buf.push_str(s);
                }
            }
        }
        TypedValues::Utf8View(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                let s = a.value(flat_idx);
                if s.is_empty() {
                    buf.push('.');
                } else {
                    buf.push_str(s);
                }
            }
        }
        TypedValues::Other(a) => {
            let val = extract_sample_value_string(*a, flat_idx)?;
            buf.push_str(&val);
        }
    }
    Ok(())
}

impl ResolvedFieldData<'_> {
    /// Checks whether all samples have missing values for this field at the given row.
    fn is_all_missing(&self, row: usize, num_samples: usize) -> bool {
        match self {
            ResolvedFieldData::Missing => true,
            ResolvedFieldData::List { list, values } => {
                if list.is_null(row) {
                    return true;
                }
                let offsets = list.offsets();
                let start = offsets[row] as usize;
                let count = offsets[row + 1] as usize - start;
                if count == 0 {
                    return true;
                }
                for i in 0..num_samples.min(count) {
                    if !is_value_missing(values, start + i) {
                        return false;
                    }
                }
                true
            }
            ResolvedFieldData::LargeList { list, values } => {
                if list.is_null(row) {
                    return true;
                }
                let offsets = list.offsets();
                let start = offsets[row] as usize;
                let count = offsets[row + 1] as usize - start;
                if count == 0 {
                    return true;
                }
                for i in 0..num_samples.min(count) {
                    if !is_value_missing(values, start + i) {
                        return false;
                    }
                }
                true
            }
        }
    }

    /// Writes a single sample's value directly to the line buffer.
    fn write_value(&self, row: usize, sample_idx: usize, buf: &mut String) -> Result<()> {
        match self {
            ResolvedFieldData::Missing => {
                buf.push('.');
                Ok(())
            }
            ResolvedFieldData::List { list, values } => {
                if list.is_null(row) {
                    buf.push('.');
                    return Ok(());
                }
                let offsets = list.offsets();
                let start = offsets[row] as usize;
                let count = offsets[row + 1] as usize - start;
                if sample_idx >= count {
                    buf.push('.');
                    return Ok(());
                }
                write_typed_value(values, start + sample_idx, buf)
            }
            ResolvedFieldData::LargeList { list, values } => {
                if list.is_null(row) {
                    buf.push('.');
                    return Ok(());
                }
                let offsets = list.offsets();
                let start = offsets[row] as usize;
                let count = offsets[row + 1] as usize - start;
                if sample_idx >= count {
                    buf.push('.');
                    return Ok(());
                }
                write_typed_value(values, start + sample_idx, buf)
            }
        }
    }
}

/// Writes FORMAT and sample columns directly to the line buffer using pre-resolved genotypes.
///
/// `carried_keys` is the record's own FORMAT key list when the reader carried
/// it. It is authoritative: those keys are emitted in that order, including a
/// key whose value is missing in every sample. Without it the keys come from
/// the schema and an all-missing one is dropped, because nothing distinguishes
/// it from a key the record never had.
fn write_resolved_format_and_samples(
    resolved: &ResolvedGenotypes,
    row: usize,
    num_samples: usize,
    carried_keys: Option<&str>,
    line: &mut String,
) -> Result<()> {
    if resolved.struct_array.is_null(row) {
        return Ok(());
    }

    let emit: Vec<usize> = match carried_keys {
        Some(keys) => {
            // A carried key needs a column, not just a name: `resolve_batch_genotypes`
            // makes an entry for every requested field, `Missing` when the batch
            // has no such child, and emitting one of those would invent a
            // FORMAT key the writer cannot fill.
            let mut emit: Vec<usize> = split_format_keys(keys)
                .filter_map(|key| {
                    resolved.fields.iter().position(|f| {
                        f.name == key && !matches!(f.data, ResolvedFieldData::Missing)
                    })
                })
                .collect();
            // A field the batch supplies that the record's own list does not
            // name is an addition made downstream of the read, so it follows
            // the carried keys — the same rule INFO uses. The all-missing test
            // still applies to it: the schema gives every record every key the
            // header declares, so a key this record never carried is present
            // and missing in every sample, and appending it would put a key on
            // the line that the source did not have.
            let carried = emit.len();
            let appended: Vec<usize> = (0..resolved.fields.len())
                .filter(|i| !emit[..carried].contains(i))
                .filter(|&i| !resolved.fields[i].data.is_all_missing(row, num_samples))
                .collect();
            emit.extend(appended);
            emit
        }
        None => (0..resolved.fields.len())
            .filter(|&i| !resolved.fields[i].data.is_all_missing(row, num_samples))
            .collect(),
    };

    if emit.is_empty() {
        return Ok(());
    }

    // Write FORMAT column
    line.push('\t');
    for (n, &i) in emit.iter().enumerate() {
        if n > 0 {
            line.push(':');
        }
        line.push_str(resolved.fields[i].name);
    }

    // Write each sample's values
    for sample_idx in 0..num_samples {
        line.push('\t');
        for (n, &i) in emit.iter().enumerate() {
            if n > 0 {
                line.push(':');
            }
            resolved.fields[i].data.write_value(row, sample_idx, line)?;
        }
    }

    Ok(())
}

/// Splits a carried FORMAT key list, dropping empty tokens.
fn split_format_keys(keys: &str) -> impl Iterator<Item = &str> {
    keys.split(':').filter(|key| !key.is_empty())
}

/// Splits a carried INFO key list, dropping empty tokens.
fn split_info_keys(keys: &str) -> impl Iterator<Item = &str> {
    keys.split(';').filter(|key| !key.is_empty())
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
    validate_vcf_serializable_genotypes(&batch.schema())?;

    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    // The layout columns are engine plumbing, not INFO data: a caller that
    // passes every batch column as an INFO field must not see them on the line.
    let layout_schema = batch.schema();
    let info_fields: Vec<String> = info_fields
        .iter()
        .filter(|name| !is_record_layout_field(&layout_schema, name))
        .cloned()
        .collect();
    let info_fields = info_fields.as_slice();
    let carried_info_keys = carried_layout_column(batch, VCF_RECORD_LAYOUT_INFO_KEYS)?;
    let carried_format_keys = carried_layout_column(batch, VCF_RECORD_LAYOUT_FORMAT_KEYS)?;

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
    let num_samples = sample_names.len();

    // Pre-resolve genotype columns for batch-level type resolution (multisample fast path)
    let resolved_genotypes = if !sample_names.is_empty() && !format_fields.is_empty() {
        resolve_batch_genotypes(batch, format_fields)
    } else {
        None
    };
    let format_columns = if resolved_genotypes.is_none() && num_samples == 1 {
        Some(build_format_column_map(batch, format_fields, sample_names))
    } else {
        None
    };

    // Position of each INFO field in `info_fields`, so a carried key list can be
    // resolved to schema order in one pass per record.
    let info_positions: std::collections::HashMap<&str, usize> = info_fields
        .iter()
        .enumerate()
        .map(|(i, name)| (name.as_str(), i))
        .collect();
    let mut field_order: Vec<&str> = Vec::with_capacity(info_fields.len());
    let mut ordered: Vec<bool> = Vec::with_capacity(info_fields.len());

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
            format_vcf_qual(quals.value(row))
        };

        // FILTER
        let filter_str = if filters.is_null(row) || filters.value(row).is_empty() {
            ".".to_string()
        } else {
            filters.value(row).to_string()
        };

        // INFO: a carried key list gives the source's own order. Keys it does
        // not mention follow in schema order — that is where an annotator's
        // newly added CSQ lands.
        field_order.clear();
        let mut carried_count = 0;
        match carried_info_keys
            .as_ref()
            .and_then(|col| value_at(col, row))
        {
            Some(keys) => {
                ordered.clear();
                ordered.resize(info_fields.len(), false);
                for key in split_info_keys(keys) {
                    if let Some(&pos) = info_positions.get(key)
                        && !ordered[pos]
                    {
                        ordered[pos] = true;
                        field_order.push(info_fields[pos].as_str());
                    }
                }
                carried_count = field_order.len();
                for (pos, name) in info_fields.iter().enumerate() {
                    if !ordered[pos] {
                        field_order.push(name.as_str());
                    }
                }
            }
            None => field_order.extend(info_fields.iter().map(String::as_str)),
        }
        let info_str = build_info_string(batch, row, &field_order, carried_count, &info_columns)?;

        // Build the VCF line
        let mut line = format!(
            "{chrom}\t{pos}\t{id_str}\t{ref_str}\t{alt_str}\t{qual_str}\t{filter_str}\t{info_str}"
        );

        // FORMAT and samples
        let row_format_keys = carried_format_keys
            .as_ref()
            .and_then(|col| value_at(col, row));
        if let Some(ref resolved) = resolved_genotypes {
            write_resolved_format_and_samples(
                resolved,
                row,
                num_samples,
                row_format_keys,
                &mut line,
            )?;
        } else if !sample_names.is_empty() && !format_fields.is_empty() {
            let (format_str, samples_str) = build_format_and_samples(
                batch,
                row,
                format_fields,
                sample_names,
                format_columns.as_ref(),
                row_format_keys,
            )?;
            if !format_str.is_empty() {
                line.push('\t');
                line.push_str(&format_str);
                for sample in &samples_str {
                    line.push('\t');
                    line.push_str(sample);
                }
            }
        }

        records.push(VcfRecordLine { line });
    }

    Ok(records)
}

/// True when `name` is a carried record-layout column of `schema`.
///
/// Decided by the field's marker metadata, never by its name: a VCF may
/// legitimately declare an INFO or FORMAT field called `_vcf_info_keys`, and
/// treating that field as plumbing would consume real data as ordering
/// information and drop it from the output.
///
/// A marked column is engine plumbing: the writer reads it to order INFO and
/// FORMAT and never puts it on a record, so it must reach neither the record
/// body nor the header's `##INFO` declarations, whichever list a caller hands
/// the writer.
pub(crate) fn is_record_layout_field(schema: &Schema, name: &str) -> bool {
    schema.index_of(name).is_ok_and(|idx| {
        schema
            .field(idx)
            .metadata()
            .contains_key(VCF_RECORD_LAYOUT_KEY)
    })
}

/// Resolves the carried record-layout column playing `role`, if the batch has
/// one.
fn carried_layout_column<'a>(
    batch: &'a RecordBatch,
    role: &str,
) -> Result<Option<StringColumnRef<'a>>> {
    let schema = batch.schema();
    let Some(idx) = schema.fields().iter().position(|field| {
        field
            .metadata()
            .get(VCF_RECORD_LAYOUT_KEY)
            .is_some_and(|marker| marker == role)
    }) else {
        return Ok(None);
    };
    get_string_column(batch.column(idx), schema.field(idx).name()).map(Some)
}

/// Reads a carried layout value, treating null as "this record carries none".
fn value_at<'a>(column: &'a StringColumnRef<'a>, row: usize) -> Option<&'a str> {
    if column.is_null(row) {
        None
    } else {
        Some(column.value(row))
    }
}

/// Gets a string column from the batch by name (supports both Utf8 and LargeUtf8)
fn get_string_column_by_name<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<StringColumnRef<'a>> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{name}' not found in batch"))
    })?;
    get_string_column(batch.column(idx), name)
}

/// Resolves an already-located column as a string column, whatever Arrow string
/// flavour it holds.
fn get_string_column<'a>(
    column: &'a datafusion::arrow::array::ArrayRef,
    name: &str,
) -> Result<StringColumnRef<'a>> {
    // Try StringArray, LargeStringArray, then StringViewArray
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(StringColumnRef::Small(arr));
    }
    if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(StringColumnRef::Large(arr));
    }
    if let Some(arr) = column.as_any().downcast_ref::<StringViewArray>() {
        return Ok(StringColumnRef::View(arr));
    }

    Err(DataFusionError::Execution(format!(
        "Column '{name}' must be Utf8, LargeUtf8, or Utf8View type"
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

/// Builds a map from (sample_name, format_field) to column index.
///
/// For single-sample VCFs, FORMAT columns may be prefixed with "fmt_" when their
/// name collides with an INFO column (e.g., both INFO and FORMAT define "DP").
/// This function checks the `bio.vcf.field.format_id` metadata to find format
/// columns regardless of whether they were renamed.
fn build_format_column_map(
    batch: &RecordBatch,
    format_fields: &[String],
    sample_names: &[String],
) -> std::collections::HashMap<(String, String), usize> {
    let mut map = std::collections::HashMap::new();
    let single_sample = sample_names.len() == 1;

    if single_sample {
        // Build a reverse lookup from format_id metadata → column index
        let schema = batch.schema();
        let mut format_id_to_idx: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            if let Some(format_id) = field.metadata().get(VCF_FIELD_FORMAT_ID_KEY) {
                format_id_to_idx.insert(format_id.as_str(), idx);
            }
        }

        let sample_name = &sample_names[0];
        for format_field in format_fields {
            if let Some(&idx) = format_id_to_idx.get(format_field.as_str()) {
                // Best: found via bio.vcf.field.format_id metadata
                map.insert((sample_name.clone(), format_field.clone()), idx);
            } else if let Ok(idx) = schema.index_of(&format!("fmt_{format_field}")) {
                // Renamed column (collision with INFO) — check before direct name to avoid
                // matching the INFO column when metadata was stripped (e.g., Polars → Arrow)
                map.insert((sample_name.clone(), format_field.clone()), idx);
            } else if let Ok(idx) = schema.index_of(&format!("format_{format_field}")) {
                // Secondary rename when fmt_ also collided
                map.insert((sample_name.clone(), format_field.clone()), idx);
            } else if let Ok(idx) = schema.index_of(format_field) {
                // Direct name lookup: no collision (legacy schemas or no rename needed)
                map.insert((sample_name.clone(), format_field.clone()), idx);
            }
        }
    } else {
        for sample_name in sample_names {
            for format_field in format_fields {
                let column_name = format!("{sample_name}_{format_field}");
                if let Ok(idx) = batch.schema().index_of(&column_name) {
                    map.insert((sample_name.clone(), format_field.clone()), idx);
                }
            }
        }
    }
    map
}

/// Builds the INFO string from INFO columns, in `field_order`.
///
/// The first `carried_count` entries came from the record's own key list, so
/// they are present by definition: a null there is `KEY=.` in the source, not an
/// absent key, and dropping it would lose the field. Everything after them is
/// dropped when null, which is what an absent key looks like once parsed.
fn build_info_string(
    batch: &RecordBatch,
    row: usize,
    field_order: &[&str],
    carried_count: usize,
    info_columns: &std::collections::HashMap<String, usize>,
) -> Result<String> {
    let mut info_parts = Vec::new();

    for (position, field_name) in field_order.iter().enumerate() {
        let field_name = *field_name;
        let col_idx = match info_columns.get(field_name) {
            Some(&idx) => idx,
            None => continue, // Column not in batch, skip
        };

        let column = batch.column(col_idx);
        if column.is_null(row) {
            if position < carried_count {
                // A Flag has no value to be missing; its key alone is the value.
                if matches!(column.data_type(), DataType::Boolean) {
                    info_parts.push(field_name.to_string());
                } else {
                    info_parts.push(format!("{field_name}=."));
                }
            }
            continue;
        }

        if let Some(value_str) = extract_info_value_string(column.as_ref(), row)? {
            if value_str == "true" {
                // Flag type - just include the name
                info_parts.push(field_name.to_string());
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
        return Ok(Some(format_vcf_float(arr.value(row) as f64)));
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(format_vcf_float(arr.value(row))));
    }

    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }

    // Handle Utf8 (StringArray), LargeUtf8 (LargeStringArray), and Utf8View (StringViewArray)
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
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
                values.push(format_vcf_float(float_arr.value(i) as f64));
            }
        }
    } else if let Some(float_arr) = array.as_any().downcast_ref::<Float64Array>() {
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(format_vcf_float(float_arr.value(i)));
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
    } else if let Some(str_arr) = array.as_any().downcast_ref::<StringViewArray>() {
        // Handle Utf8View (DataFusion default in certain operations)
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
    carried_keys: Option<&str>,
) -> Result<(String, Vec<String>)> {
    if sample_names.is_empty() || format_fields.is_empty() {
        return Ok((String::new(), Vec::new()));
    }

    // A carried key list is the record's own, so it decides which keys are
    // emitted first and in what order. Keys it names that were not selected for
    // output have no column to render and are dropped. Keys the batch supplies
    // that the list does not name are additions made downstream of the read;
    // they follow, exactly as an added INFO key does.
    let (selected, carried_count): (Vec<&str>, usize) = match carried_keys {
        Some(keys) => {
            let mut selected: Vec<&str> = split_format_keys(keys)
                .filter(|key| format_fields.iter().any(|field| field == key))
                .collect();
            let carried_count = selected.len();
            selected.extend(
                format_fields
                    .iter()
                    .map(String::as_str)
                    .filter(|field| !split_format_keys(keys).any(|key| key == *field)),
            );
            (selected, carried_count)
        }
        None => (format_fields.iter().map(String::as_str).collect(), 0),
    };
    if selected.is_empty() {
        return Ok((String::new(), Vec::new()));
    }

    // Multisample sources keep FORMAT data in nested `genotypes` even when
    // only a subset (including one sample) is selected for output.
    let has_nested_genotypes = batch.schema().column_with_name("genotypes").is_some();

    // A carried key is only reproducible while the batch still supplies its
    // column; a projection may drop one and keep the layout column. Where it
    // does, the key falls back to the all-missing rule below, which drops it
    // rather than emitting `.` for every sample.
    let supplied = |name: &str| -> bool {
        if has_nested_genotypes {
            batch
                .schema()
                .index_of("genotypes")
                .ok()
                .and_then(|idx| batch.column(idx).as_any().downcast_ref::<StructArray>())
                .is_some_and(|genotypes| genotypes.column_by_name(name).is_some())
        } else {
            format_columns.is_some_and(|columns| {
                sample_names
                    .iter()
                    .any(|sample| columns.contains_key(&(sample.clone(), name.to_string())))
            })
        }
    };
    // Keep a flag per position rather than a prefix length: an unsupplied
    // carried key does not disqualify the supplied ones after it.
    let must_keep: Vec<bool> = selected
        .iter()
        .enumerate()
        .map(|(i, name)| i < carried_count && supplied(name))
        .collect();

    let field_values = if has_nested_genotypes {
        collect_nested_multisample_values(batch, row, &selected, sample_names)?
    } else {
        let format_columns = format_columns.ok_or_else(|| {
            DataFusionError::Execution("Missing single-sample FORMAT column mapping".to_string())
        })?;

        // Collect values in field × sample order for filtering
        let mut field_values: Vec<Vec<String>> = Vec::with_capacity(selected.len());
        for format_field in &selected {
            let mut sample_vals = Vec::with_capacity(sample_names.len());
            for sample_name in sample_names {
                let key = (sample_name.clone(), (*format_field).to_string());
                let value = match format_columns.get(&key) {
                    Some(&col_idx) => {
                        let column = batch.column(col_idx);
                        extract_sample_value_string(column.as_ref(), row)?
                    }
                    None => ".".to_string(),
                };
                sample_vals.push(value);
            }
            field_values.push(sample_vals);
        }
        field_values
    };

    // A carried key is kept even when every sample is missing — that is the
    // whole point, since a `.` in the source parses to the same null as an
    // absent key. Everything after the carried ones is dropped when missing in
    // every sample: without the record's own list, that is indistinguishable
    // from a key it never carried.
    let keep: Vec<bool> = field_values
        .iter()
        .enumerate()
        .map(|(i, sample_vals)| must_keep[i] || sample_vals.iter().any(|v| v != "."))
        .collect();

    Ok(join_format_fields(
        &selected,
        &field_values,
        &keep,
        sample_names.len(),
    ))
}

/// Collects per-field, per-sample values from the nested genotypes column.
/// Returns a 2D structure: `values[field_idx][sample_idx]`.
fn collect_nested_multisample_values(
    batch: &RecordBatch,
    row: usize,
    format_fields: &[&str],
    sample_names: &[String],
) -> Result<Vec<Vec<String>>> {
    let genotypes_idx = batch.schema().index_of("genotypes").map_err(|_| {
        DataFusionError::Execution(
            "Multisample output requires a 'genotypes' column in nested schema".to_string(),
        )
    })?;
    let genotypes_col = batch.column(genotypes_idx);
    let num_samples = sample_names.len();

    if genotypes_col.is_null(row) {
        return Ok(vec![
            vec![".".to_string(); num_samples];
            format_fields.len()
        ]);
    }

    // Columnar layout: genotypes is Struct<GT: List<Utf8>, GQ: List<Int32>, ...>
    let genotypes_struct = genotypes_col
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Column 'genotypes' must be Struct (columnar layout)".to_string(),
            )
        })?;

    let mut field_values = Vec::with_capacity(format_fields.len());
    for format_field in format_fields {
        let mut sample_vals = Vec::with_capacity(num_samples);
        for sample_idx in 0..num_samples {
            let val = if let Some(list_col) = genotypes_struct.column_by_name(format_field) {
                extract_list_element_as_string(list_col.as_ref(), row, sample_idx)?
            } else {
                ".".to_string()
            };
            sample_vals.push(val);
        }
        field_values.push(sample_vals);
    }
    Ok(field_values)
}

/// Joins the kept FORMAT fields into the FORMAT column and one string per sample.
fn join_format_fields(
    format_fields: &[&str],
    field_values: &[Vec<String>],
    keep: &[bool],
    num_samples: usize,
) -> (String, Vec<String>) {
    let filtered_fields: Vec<&str> = format_fields
        .iter()
        .zip(keep.iter())
        .filter(|&(_, &k)| k)
        .map(|(f, _)| *f)
        .collect();
    let format_str = filtered_fields.join(":");

    let mut samples = Vec::with_capacity(num_samples);
    for sample_idx in 0..num_samples {
        let vals: Vec<&str> = field_values
            .iter()
            .zip(keep.iter())
            .filter(|&(_, &k)| k)
            .map(|(sv, _)| sv[sample_idx].as_str())
            .collect();
        samples.push(vals.join(":"));
    }

    (format_str, samples)
}

/// Extracts a single element from a list column at [row][element_idx] as a string.
fn extract_list_element_as_string(
    array: &dyn Array,
    row: usize,
    element_idx: usize,
) -> Result<String> {
    if array.is_null(row) {
        return Ok(".".to_string());
    }

    // Try List<T>
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let inner = list.value(row);
        if element_idx >= inner.len() || inner.is_null(element_idx) {
            return Ok(".".to_string());
        }
        return extract_sample_value_string(inner.as_ref(), element_idx);
    }
    // Try LargeList<T>
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        let inner = list.value(row);
        if element_idx >= inner.len() || inner.is_null(element_idx) {
            return Ok(".".to_string());
        }
        return extract_sample_value_string(inner.as_ref(), element_idx);
    }

    Ok(".".to_string())
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
        return Ok(format_vcf_float(arr.value(row) as f64));
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(format_vcf_float(arr.value(row)));
    }

    // Handle Utf8 (StringArray), LargeUtf8 (LargeStringArray), and Utf8View (StringViewArray)
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
    if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
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
    use datafusion::arrow::array::{
        ArrayRef, Float64Builder, Int32Builder, ListBuilder, StringBuilder, StructArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_bio_format_core::metadata::{
        VCF_FIELD_FIELD_TYPE_KEY, VCF_FORMAT_KEYS_COLUMN, VCF_INFO_KEYS_COLUMN,
    };

    /// A layout column as the reader builds it: named for humans, marked for
    /// machines. The marker is what the writer keys off.
    fn layout_field(name: &str, role: &str) -> Field {
        Field::new(name, DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            VCF_RECORD_LAYOUT_KEY.to_string(),
            role.to_string(),
        )]))
    }
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
    fn rejects_flat_and_columnar_dosage_gt_schemas() {
        let mut gt_metadata = std::collections::HashMap::new();
        gt_metadata.insert(VCF_FIELD_FORMAT_ID_KEY.to_string(), "GT".to_string());

        let flat = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("GT", DataType::Int8, true).with_metadata(gt_metadata.clone()),
        ]));
        let error = validate_vcf_serializable_genotypes(&flat)
            .unwrap_err()
            .to_string();
        assert!(error.contains("does not support genotype dosage input"));

        let nested_gt = Field::new(
            "GT",
            DataType::List(Arc::new(Field::new("item", DataType::Int8, true))),
            true,
        )
        .with_metadata(gt_metadata);
        let columnar = Arc::new(Schema::new(vec![Field::new(
            "genotypes",
            DataType::Struct(vec![nested_gt].into()),
            true,
        )]));
        let error = validate_vcf_serializable_genotypes(&columnar)
            .unwrap_err()
            .to_string();
        assert!(error.contains("cannot reconstruct the original GT allele calls"));
    }

    #[test]
    fn rejects_dosage_metadata_even_after_gt_projection() {
        for key in [GENOTYPE_OUTPUT_MODE_KEY, VCF_GENOTYPE_OUTPUT_MODE_KEY] {
            let mut metadata = std::collections::HashMap::new();
            metadata.insert(key.to_string(), DOSAGE_MODE.to_string());
            let schema = Arc::new(
                Schema::new(vec![Field::new("chrom", DataType::Utf8, false)])
                    .with_metadata(metadata),
            );
            assert!(validate_vcf_serializable_genotypes(&schema).is_err());
        }
    }

    #[test]
    fn accepts_lossless_string_gt_schema() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(VCF_FIELD_FORMAT_ID_KEY.to_string(), "GT".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("GT", DataType::Utf8, true).with_metadata(metadata),
        ]));
        assert!(validate_vcf_serializable_genotypes(&schema).is_ok());
    }

    #[test]
    fn batch_serializer_rejects_dosage_before_emitting_missing_gt_calls() {
        let mut fields = create_test_schema().fields().to_vec();
        fields.push(Arc::new(Field::new("GT", DataType::Int8, true)));
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            GENOTYPE_OUTPUT_MODE_KEY.to_string(),
            DOSAGE_MODE.to_string(),
        );
        let batch = RecordBatch::new_empty(Arc::new(Schema::new_with_metadata(fields, metadata)));

        let error =
            match batch_to_vcf_lines(&batch, &[], &["GT".to_string()], &["S1".to_string()], true) {
                Ok(_) => panic!("dosage GT must not be serialized as missing VCF calls"),
                Err(error) => error.to_string(),
            };
        assert!(error.contains("does not support genotype dosage input"));
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
        // Columnar schema: genotypes: Struct<GT: List<Utf8>, DP: List<Int32>>
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
                DataType::Struct(
                    vec![
                        Field::new(
                            "GT",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                        Field::new(
                            "DP",
                            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        // Build columnar genotypes: GT = ["0/1", "1/1"], DP = [25, 30]
        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("0/1");
        gt_builder.values().append_value("1/1");
        gt_builder.append(true);
        let gt_array = Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

        let mut dp_builder = ListBuilder::new(Int32Builder::new());
        dp_builder.values().append_value(25);
        dp_builder.values().append_value(30);
        dp_builder.append(true);
        let dp_array = Arc::new(dp_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

        let genotypes = Arc::new(
            StructArray::try_new(
                vec![
                    Field::new(
                        "GT",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                    Field::new(
                        "DP",
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                        true,
                    ),
                ]
                .into(),
                vec![gt_array, dp_array],
                None,
            )
            .unwrap(),
        );

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
    fn test_batch_to_vcf_lines_columnar_multisample_single_selected_sample() {
        // Columnar schema with 2 samples in genotypes, but only 1 sample_name selected for output
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
                DataType::Struct(
                    vec![
                        Field::new(
                            "GT",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                        Field::new(
                            "DP",
                            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        // Build columnar genotypes with 1 sample: GT = ["0/1"], DP = [25]
        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("0/1");
        gt_builder.append(true);
        let gt_array = Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

        let mut dp_builder = ListBuilder::new(Int32Builder::new());
        dp_builder.values().append_value(25);
        dp_builder.append(true);
        let dp_array = Arc::new(dp_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

        let genotypes = Arc::new(
            StructArray::try_new(
                vec![
                    Field::new(
                        "GT",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                    Field::new(
                        "DP",
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                        true,
                    ),
                ]
                .into(),
                vec![gt_array, dp_array],
                None,
            )
            .unwrap(),
        );

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

        let sample_names = vec!["SAMPLE1".to_string()];
        let format_fields = vec!["GT".to_string(), "DP".to_string()];

        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &sample_names, true).unwrap();
        assert_eq!(lines.len(), 1);
        let line = &lines[0].line;
        assert!(line.contains("GT:DP"));
        assert!(line.contains("0/1:25"));
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
                .starts_with("chr1\t100\trs456\tC\tT\t45\tPASS")
        );
    }

    /// QUAL must render the way Ensembl VEP (and htslib) writes it: the shortest
    /// faithful form, NOT a fixed 2-decimal rendering. VEP copies the input line
    /// verbatim, so an input QUAL of `50` comes back out as `50`; emitting
    /// `50.00` makes every record differ byte-for-byte from the reference.
    fn qual_line_for(qual: Option<f64>) -> String {
        let schema = create_test_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![qual])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
            ],
        )
        .unwrap();
        let lines = batch_to_vcf_lines(&batch, &[], &[], &[], true).unwrap();
        assert_eq!(lines.len(), 1);
        lines[0].line.split('\t').nth(5).unwrap().to_string()
    }

    #[test]
    fn qual_integral_renders_without_decimal_places() {
        assert_eq!(qual_line_for(Some(50.0)), "50");
    }

    #[test]
    fn qual_zero_renders_as_bare_zero() {
        assert_eq!(qual_line_for(Some(0.0)), "0");
    }

    #[test]
    fn qual_fractional_keeps_its_digits() {
        assert_eq!(qual_line_for(Some(50.5)), "50.5");
        assert_eq!(qual_line_for(Some(29.99)), "29.99");
    }

    #[test]
    fn qual_fractional_is_not_truncated_to_two_places() {
        // `{:.2}` would round this to "0.00" and destroy the value.
        assert_eq!(qual_line_for(Some(0.001)), "0.001");
    }

    #[test]
    fn qual_missing_renders_as_dot() {
        assert_eq!(qual_line_for(None), ".");
    }

    #[test]
    fn qual_large_integral_stays_integral() {
        assert_eq!(qual_line_for(Some(999999.0)), "999999");
    }

    #[test]
    fn qual_preserves_more_than_six_significant_digits() {
        // `format_vcf_float` rounds to 6 significant digits (%g semantics), which
        // is right for INFO/FORMAT floats but silently rewrites a precise QUAL.
        assert_eq!(qual_line_for(Some(123456.78)), "123456.78");
        assert_eq!(
            qual_line_for(Some(std::f64::consts::PI)),
            "3.141592653589793"
        );
    }

    #[test]
    fn qual_never_uses_scientific_notation() {
        // %g flips to scientific at 1e6; VCF QUAL in the wild is always plain.
        assert_eq!(qual_line_for(Some(1_000_000.0)), "1000000");
        assert_eq!(qual_line_for(Some(1_234_567.0)), "1234567");
    }

    #[test]
    fn qual_round_trips_through_the_written_text() {
        // The written QUAL must parse back to bit-identical f64. This is the
        // property that both `{:.2}` and 6-significant-digit `%g` violate.
        for value in [
            50.0,
            0.001,
            29.99,
            123456.78,
            1_234_567.0,
            std::f64::consts::PI,
            0.000312305,
            1e-5,
            f64::MIN_POSITIVE,
            f64::MAX,
        ] {
            let written = qual_line_for(Some(value));
            let parsed: f64 = written
                .parse()
                .unwrap_or_else(|e| panic!("QUAL {value} written as {written:?}: {e}"));
            assert_eq!(parsed, value, "QUAL {value} written as {written:?}");
        }
    }

    #[test]
    fn qual_from_the_reader_renders_at_source_precision() {
        // noodles parses QUAL as f32 and physical_exec.rs widens it with
        // `v as f64`, so a VCF carrying `29.99` reaches the serializer as
        // 29.989999771118164. Printing the f64 shortest form would write that
        // binary noise straight back out.
        for text in ["314.8", "29.99", "0.001", "99.9", "123456.78", "0.5", "50"] {
            let widened = text.parse::<f32>().unwrap() as f64;
            assert_eq!(
                qual_line_for(Some(widened)),
                text,
                "QUAL {text} widened from f32"
            );
        }
    }

    #[test]
    fn qual_keeps_f64_precision_when_it_exceeds_f32() {
        // A caller-supplied value that f32 cannot hold must not be narrowed.
        assert_eq!(
            qual_line_for(Some(std::f64::consts::PI)),
            "3.141592653589793"
        );
        assert_eq!(qual_line_for(Some(0.1)), "0.1");
        assert_eq!(qual_line_for(Some(1e-5)), "0.00001");
    }

    #[test]
    fn qual_non_finite_renders_as_missing() {
        // VCF has no representation for NaN or +/-Inf; "." is the missing value.
        // `format_vcf_float` panics outright on infinity.
        assert_eq!(qual_line_for(Some(f64::NAN)), ".");
        assert_eq!(qual_line_for(Some(f64::INFINITY)), ".");
        assert_eq!(qual_line_for(Some(f64::NEG_INFINITY)), ".");
    }

    #[test]
    fn format_vcf_float_renders_non_finite_as_missing() {
        // `format!("{:.5e}", f64::INFINITY)` is "inf", which has no exponent to
        // split on — the helper used to panic on `split_once('e').unwrap()`.
        // VCF defines no textual form for +/-Inf, so it joins NaN as missing.
        assert_eq!(format_vcf_float(f64::NAN), ".");
        assert_eq!(format_vcf_float(f64::INFINITY), ".");
        assert_eq!(format_vcf_float(f64::NEG_INFINITY), ".");
    }

    #[test]
    fn format_vcf_float_handles_the_float_extremes() {
        // Subnormal and maximal magnitudes take the scientific branch; neither
        // may panic.
        assert_eq!(format_vcf_float(f64::MIN_POSITIVE), "2.22507e-308");
        assert_eq!(format_vcf_float(f64::MAX), "1.79769e+308");
    }

    /// The panic is reachable from real data, not just a direct helper call:
    /// any INFO or FORMAT Float column carrying +/-Inf hits it (a Parquet or
    /// Polars source can produce one). This drives the whole serializer.
    #[test]
    fn info_float_column_with_infinity_serializes_as_missing() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("AF", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                Arc::new(Float64Array::from(vec![Some(f64::INFINITY)])),
            ],
        )
        .unwrap();

        let lines = batch_to_vcf_lines(&batch, &["AF".to_string()], &[], &[], true).unwrap();

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].line.split('\t').nth(7).unwrap(), "AF=.");
    }

    /// Builds a two-sample batch whose nested `AF` FORMAT field holds `af`, and
    /// returns the FORMAT column of the single emitted line.
    fn nested_format_keys_for(af: [f64; 2]) -> String {
        let af_field = Field::new(
            "AF",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        );
        let gt_field = Field::new(
            "GT",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
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
                DataType::Struct(vec![gt_field.clone(), af_field.clone()].into()),
                true,
            ),
        ]));

        let mut gt = ListBuilder::new(StringBuilder::new());
        gt.values().append_value("0/1");
        gt.values().append_value("1/1");
        gt.append(true);
        let gt_array = Arc::new(gt.finish()) as Arc<dyn datafusion::arrow::array::Array>;

        let mut af_b = ListBuilder::new(Float64Builder::new());
        af_b.values().append_value(af[0]);
        af_b.values().append_value(af[1]);
        af_b.append(true);
        let af_array = Arc::new(af_b.finish()) as Arc<dyn datafusion::arrow::array::Array>;

        let genotypes = Arc::new(
            StructArray::try_new(
                vec![gt_field, af_field].into(),
                vec![gt_array, af_array],
                None,
            )
            .unwrap(),
        );

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                genotypes,
            ],
        )
        .unwrap();

        let lines = batch_to_vcf_lines(
            &batch,
            &[],
            &["GT".to_string(), "AF".to_string()],
            &["S1".to_string(), "S2".to_string()],
            true,
        )
        .unwrap();
        assert_eq!(lines.len(), 1);
        lines[0].line.split('\t').nth(8).unwrap().to_string()
    }

    #[test]
    fn nested_format_field_of_only_infinities_is_pruned() {
        // Every value serializes as ".", so the key must be dropped — the same
        // way an all-NaN field is. `is_value_missing` checked only is_nan, so
        // the pruning predicate disagreed with the formatter.
        assert_eq!(
            nested_format_keys_for([f64::INFINITY, f64::NEG_INFINITY]),
            "GT"
        );
    }

    #[test]
    fn nested_format_field_of_only_nans_is_pruned() {
        // Reference behaviour the infinity case must match.
        assert_eq!(nested_format_keys_for([f64::NAN, f64::NAN]), "GT");
    }

    #[test]
    fn nested_format_field_with_one_finite_value_is_kept() {
        // Pruning must not over-reach: one real value keeps the key.
        assert_eq!(nested_format_keys_for([f64::INFINITY, 0.25]), "GT:AF");
    }

    #[test]
    fn test_format_vcf_float() {
        // 6 significant digits, trailing zeros trimmed
        assert_eq!(format_vcf_float(0.000312305), "0.000312305");
        assert_eq!(format_vcf_float(0.5), "0.5");
        assert_eq!(format_vcf_float(1.0), "1");
        assert_eq!(format_vcf_float(0.0), "0");
        assert_eq!(format_vcf_float(1234.57), "1234.57");
        assert_eq!(format_vcf_float(0.1), "0.1");
        assert_eq!(format_vcf_float(1e-5), "1e-05");
        assert_eq!(format_vcf_float(1e7), "1e+07");
        assert_eq!(format_vcf_float(f64::NAN), ".");
        // Edge cases
        assert_eq!(format_vcf_float(100.0), "100");
        assert_eq!(format_vcf_float(0.001), "0.001");
        assert_eq!(format_vcf_float(999999.0), "999999");
        assert_eq!(format_vcf_float(1e-4), "0.0001");
        assert_eq!(format_vcf_float(1e6), "1e+06");
        assert_eq!(format_vcf_float(-0.5), "-0.5");
        assert_eq!(format_vcf_float(123456.0), "123456");
    }

    #[test]
    fn test_format_drops_all_missing_fields() {
        // Multi-sample with GT, GQ, PL where PL is all-missing for all samples
        // PL should be dropped from FORMAT (matching bcftools behavior)
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
                DataType::Struct(
                    vec![
                        Field::new(
                            "GT",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                        Field::new(
                            "GQ",
                            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                            true,
                        ),
                        Field::new(
                            "PL",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        // GT = ["0/1", "1/1"], GQ = [30, 40], PL = [null, null] (all missing)
        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("0/1");
        gt_builder.values().append_value("1/1");
        gt_builder.append(true);

        let mut gq_builder = ListBuilder::new(Int32Builder::new());
        gq_builder.values().append_value(30);
        gq_builder.values().append_value(40);
        gq_builder.append(true);

        // PL: all null values for both samples
        let mut pl_builder = ListBuilder::new(StringBuilder::new());
        pl_builder.values().append_null();
        pl_builder.values().append_null();
        pl_builder.append(true);

        let genotypes = Arc::new(
            StructArray::try_new(
                vec![
                    Field::new(
                        "GT",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                    Field::new(
                        "GQ",
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                        true,
                    ),
                    Field::new(
                        "PL",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                ]
                .into(),
                vec![
                    Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>,
                    Arc::new(gq_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>,
                    Arc::new(pl_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>,
                ],
                None,
            )
            .unwrap(),
        );

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![Some(".")])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                genotypes,
            ],
        )
        .unwrap();

        let sample_names = vec!["S1".to_string(), "S2".to_string()];
        let format_fields = vec!["GT".to_string(), "GQ".to_string(), "PL".to_string()];

        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &sample_names, true).unwrap();
        let line = &lines[0].line;
        let parts: Vec<&str> = line.split('\t').collect();

        // FORMAT should drop all-missing PL field
        assert_eq!(parts[8], "GT:GQ");
        // Sample values should have 2 components (PL omitted)
        assert_eq!(parts[9], "0/1:30");
        assert_eq!(parts[10], "1/1:40");
    }

    // ---- carried record layout ------------------------------------------
    //
    // A VCF's INFO key order and FORMAT key list are per record, and neither
    // survives the typed columns: every record carries every key the header
    // declares, and a key whose value is missing in every sample is null in the
    // column exactly like an absent key. A reader asked to carry the layout
    // supplies both as `;`- and `:`-separated key lists.

    /// One record, one sample. INFO holds `AC` then `AF` in schema order;
    /// FORMAT holds `GT`, `PS` and `DP`, with `PS` null (missing in the only
    /// sample). `csq` stands in for a key an annotator added, which no source
    /// layout mentions.
    fn layout_batch(
        info_keys: Option<&str>,
        format_keys: Option<&str>,
        csq: Option<&str>,
        ps: Option<i32>,
        ac: Option<i32>,
    ) -> RecordBatch {
        let mut fields = vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("AC", DataType::Int32, true),
            Field::new("AF", DataType::Float64, true),
            Field::new("GT", DataType::Utf8, true),
            Field::new("PS", DataType::Int32, true),
            Field::new("DP", DataType::Int32, true),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![99u32])),
            Arc::new(UInt32Array::from(vec![100u32])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["G"])),
            Arc::new(Float64Array::from(vec![Some(50.0)])),
            Arc::new(StringArray::from(vec![Some("PASS")])),
            Arc::new(Int32Array::from(vec![ac])),
            Arc::new(Float64Array::from(vec![Some(0.5)])),
            Arc::new(StringArray::from(vec![Some("0/1")])),
            Arc::new(Int32Array::from(vec![ps])),
            Arc::new(Int32Array::from(vec![Some(25)])),
        ];
        if csq.is_some() {
            fields.push(Field::new("CSQ", DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(vec![csq])));
        }
        if info_keys.is_some() || format_keys.is_some() {
            fields.push(layout_field(
                VCF_INFO_KEYS_COLUMN,
                VCF_RECORD_LAYOUT_INFO_KEYS,
            ));
            columns.push(Arc::new(StringArray::from(vec![info_keys])));
            fields.push(layout_field(
                VCF_FORMAT_KEYS_COLUMN,
                VCF_RECORD_LAYOUT_FORMAT_KEYS,
            ));
            columns.push(Arc::new(StringArray::from(vec![format_keys])));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn layout_line(
        info_keys: Option<&str>,
        format_keys: Option<&str>,
        csq: Option<&str>,
        info_fields: &[&str],
        format_fields: &[&str],
    ) -> String {
        layout_line_with_ps(
            info_keys,
            format_keys,
            csq,
            None,
            info_fields,
            format_fields,
        )
    }

    fn layout_line_with_ps(
        info_keys: Option<&str>,
        format_keys: Option<&str>,
        csq: Option<&str>,
        ps: Option<i32>,
        info_fields: &[&str],
        format_fields: &[&str],
    ) -> String {
        let batch = layout_batch(info_keys, format_keys, csq, ps, Some(1));
        let info_fields: Vec<String> = info_fields.iter().map(|s| s.to_string()).collect();
        let format_fields: Vec<String> = format_fields.iter().map(|s| s.to_string()).collect();
        let lines = batch_to_vcf_lines(
            &batch,
            &info_fields,
            &format_fields,
            &["SAMPLE1".to_string()],
            true,
        )
        .unwrap();
        assert_eq!(lines.len(), 1);
        lines[0].line.clone()
    }

    fn columns_of(line: &str) -> Vec<&str> {
        line.split('\t').collect()
    }

    #[test]
    fn without_a_carried_layout_info_follows_schema_order() {
        let line = layout_line(None, None, None, &["AC", "AF"], &["GT", "PS", "DP"]);
        let cols = columns_of(&line);
        assert_eq!(cols[7], "AC=1;AF=0.5");
        // PS is null in the only sample, so it cannot be told from an absent key.
        assert_eq!(cols[8], "GT:DP");
        assert_eq!(cols[9], "0/1:25");
    }

    #[test]
    fn carried_info_keys_order_the_info_column() {
        let line = layout_line(
            Some("AF;AC"),
            Some("GT:DP"),
            None,
            &["AC", "AF"],
            &["GT", "PS", "DP"],
        );
        assert_eq!(columns_of(&line)[7], "AF=0.5;AC=1");
    }

    #[test]
    fn a_key_the_carried_order_omits_is_appended() {
        // An annotator's CSQ is not in the source's INFO, so it goes last.
        let line = layout_line(
            Some("AF;AC"),
            Some("GT:DP"),
            Some("T|missense"),
            &["AC", "AF", "CSQ"],
            &["GT", "PS", "DP"],
        );
        assert_eq!(columns_of(&line)[7], "AF=0.5;AC=1;CSQ=T|missense");
    }

    #[test]
    fn a_carried_key_the_batch_does_not_supply_is_skipped() {
        // The layout lists a key that was projected away; it cannot be rendered.
        let line = layout_line(Some("AF;AC"), Some("GT:DP"), None, &["AF"], &["GT", "DP"]);
        assert_eq!(columns_of(&line)[7], "AF=0.5");
    }

    #[test]
    fn carried_format_keys_restore_a_key_missing_in_every_sample() {
        // PS is Type=Integer, so a source "." parses to null — identical to the
        // key being absent. Only the carried list can tell them apart.
        let line = layout_line(
            Some("AC;AF"),
            Some("GT:PS:DP"),
            None,
            &["AC", "AF"],
            &["GT", "PS", "DP"],
        );
        let cols = columns_of(&line);
        assert_eq!(cols[8], "GT:PS:DP");
        assert_eq!(cols[9], "0/1:.:25");
    }

    #[test]
    fn carried_format_keys_are_emitted_in_their_own_order() {
        let line = layout_line(
            Some("AC;AF"),
            Some("DP:GT"),
            None,
            &["AC", "AF"],
            &["GT", "PS", "DP"],
        );
        let cols = columns_of(&line);
        assert_eq!(cols[8], "DP:GT");
        assert_eq!(cols[9], "25:0/1");
    }

    #[test]
    fn a_carried_format_key_that_was_not_selected_is_dropped() {
        // Selecting a subset of FORMAT fields must not resurrect the others as
        // all-missing columns.
        let line = layout_line(
            Some("AC;AF"),
            Some("GT:PS:DP"),
            None,
            &["AC", "AF"],
            &["GT", "DP"],
        );
        let cols = columns_of(&line);
        assert_eq!(cols[8], "GT:DP");
        assert_eq!(cols[9], "0/1:25");
    }

    #[test]
    fn a_null_layout_row_falls_back_to_reconstruction() {
        let line = layout_line(None, None, None, &["AC", "AF"], &["GT", "PS", "DP"]);
        let with_null_layout = layout_line(
            None::<&str>,
            None::<&str>,
            None,
            &["AC", "AF"],
            &["GT", "PS", "DP"],
        );
        assert_eq!(line, with_null_layout);
    }

    #[test]
    fn the_layout_columns_are_not_emitted_as_info_fields() {
        // They are engine plumbing, not data. A caller that passes every batch
        // column as an INFO field must not see them on the line.
        let line = layout_line(
            Some("AC;AF"),
            Some("GT:PS:DP"),
            None,
            &["AC", "AF", VCF_INFO_KEYS_COLUMN, VCF_FORMAT_KEYS_COLUMN],
            &["GT", "PS", "DP"],
        );
        assert_eq!(columns_of(&line)[7], "AC=1;AF=0.5");
    }

    /// Multisample sources keep FORMAT data in a nested `genotypes` struct and
    /// take a different serializer path, which must honour the layout too.
    fn nested_layout_line(format_keys: Option<&str>) -> String {
        let mut gt = ListBuilder::new(StringBuilder::new());
        gt.values().append_value("0/1");
        gt.values().append_value("1/1");
        gt.append(true);
        let mut ps = ListBuilder::new(Int32Builder::new());
        ps.values().append_null();
        ps.values().append_null();
        ps.append(true);
        let mut dp = ListBuilder::new(Int32Builder::new());
        dp.values().append_value(25);
        dp.values().append_value(30);
        dp.append(true);

        let genotypes = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "GT",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                )),
                Arc::new(gt.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "PS",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                )),
                Arc::new(ps.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "DP",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                )),
                Arc::new(dp.finish()) as ArrayRef,
            ),
        ]);

        let mut fields = create_test_schema().fields().to_vec();
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![99u32])),
            Arc::new(UInt32Array::from(vec![100u32])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["G"])),
            Arc::new(Float64Array::from(vec![Some(50.0)])),
            Arc::new(StringArray::from(vec![Some("PASS")])),
        ];
        fields.push(Arc::new(Field::new(
            "genotypes",
            genotypes.data_type().clone(),
            true,
        )));
        columns.push(Arc::new(genotypes));
        if format_keys.is_some() {
            fields.push(Arc::new(layout_field(
                VCF_INFO_KEYS_COLUMN,
                VCF_RECORD_LAYOUT_INFO_KEYS,
            )));
            columns.push(Arc::new(StringArray::from(vec![None::<&str>])));
            fields.push(Arc::new(layout_field(
                VCF_FORMAT_KEYS_COLUMN,
                VCF_RECORD_LAYOUT_FORMAT_KEYS,
            )));
            columns.push(Arc::new(StringArray::from(vec![format_keys])));
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let format_fields: Vec<String> = ["GT", "PS", "DP"].iter().map(|s| s.to_string()).collect();
        let samples: Vec<String> = ["S1", "S2"].iter().map(|s| s.to_string()).collect();
        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &samples, true).unwrap();
        assert_eq!(lines.len(), 1);
        lines[0].line.clone()
    }

    #[test]
    fn nested_genotypes_drop_an_all_missing_key_without_a_layout() {
        let line = nested_layout_line(None);
        let cols = columns_of(&line);
        assert_eq!(cols[8], "GT:DP");
        assert_eq!(cols[9], "0/1:25");
        assert_eq!(cols[10], "1/1:30");
    }

    #[test]
    fn nested_genotypes_honour_the_carried_format_keys() {
        let line = nested_layout_line(Some("GT:PS:DP"));
        let cols = columns_of(&line);
        assert_eq!(cols[8], "GT:PS:DP");
        assert_eq!(cols[9], "0/1:.:25");
        assert_eq!(cols[10], "1/1:.:30");
    }

    /// A pipeline that adds a FORMAT field supplies a column the source record
    /// never had, so no carried key list can mention it. Dropping it silently
    /// would lose annotation output; INFO already appends such keys.
    #[test]
    fn a_supplied_format_key_the_carried_list_omits_is_appended() {
        let line = layout_line_with_ps(
            Some("AC;AF"),
            Some("GT:DP"),
            None,
            Some(7),
            &["AC", "AF"],
            &["GT", "PS", "DP"],
        );
        let cols = columns_of(&line);
        assert_eq!(cols[8], "GT:DP:PS");
        assert_eq!(cols[9], "0/1:25:7");
    }

    /// The other half of that rule, and the one byte parity depends on: the
    /// schema gives every record every FORMAT key the header declares, so a key
    /// the record never carried is present but missing in every sample.
    /// Appending those would add keys the source line did not have.
    #[test]
    fn a_format_key_missing_in_every_sample_is_not_appended() {
        let line = layout_line_with_ps(
            Some("AC;AF"),
            Some("GT:DP"),
            None,
            None,
            &["AC", "AF"],
            &["GT", "PS", "DP"],
        );
        let cols = columns_of(&line);
        assert_eq!(cols[8], "GT:DP");
        assert_eq!(cols[9], "0/1:25");
    }

    #[test]
    fn nested_genotypes_append_a_supplied_key_the_carried_list_omits() {
        // The nested fixture's PS is missing in every sample, so carrying
        // "GT:DP" must not resurrect it, while DP — supplied and carried —
        // stays put.
        let line = nested_layout_line(Some("GT:DP"));
        let cols = columns_of(&line);
        assert_eq!(cols[8], "GT:DP");
        assert_eq!(cols[9], "0/1:25");
        assert_eq!(cols[10], "1/1:30");
    }

    /// `DP=.` in a source record is a key that is present with a missing value.
    /// The parser turns it into the same null as a key the record never had, so
    /// only the carried list can tell them apart — the INFO twin of the `PS`
    /// case, and the reason the carry exists.
    #[test]
    fn a_carried_info_key_with_a_missing_value_keeps_its_place() {
        let batch = layout_batch(Some("AC;AF"), Some("GT:DP"), None, None, None);
        let lines = batch_to_vcf_lines(
            &batch,
            &["AC".to_string(), "AF".to_string()],
            &["GT".to_string(), "PS".to_string(), "DP".to_string()],
            &["SAMPLE1".to_string()],
            true,
        )
        .unwrap();
        assert_eq!(columns_of(&lines[0].line)[7], "AC=.;AF=0.5");
    }

    /// The same column with no carried list stays absent: without the record's
    /// own key list, a null is indistinguishable from a key it never had.
    #[test]
    fn an_uncarried_info_key_with_a_null_value_stays_absent() {
        let batch = layout_batch(None, None, None, None, None);
        let lines = batch_to_vcf_lines(
            &batch,
            &["AC".to_string(), "AF".to_string()],
            &["GT".to_string(), "PS".to_string(), "DP".to_string()],
            &["SAMPLE1".to_string()],
            true,
        )
        .unwrap();
        assert_eq!(columns_of(&lines[0].line)[7], "AF=0.5");
    }

    /// A VCF may declare an INFO field with either reserved name. Without the
    /// carry, such a column is ordinary data: identifying plumbing by name
    /// alone would consume it as ordering information, drop it from the record,
    /// and — if it were not a string — fail serialization outright.
    #[test]
    fn a_source_field_named_like_a_layout_column_is_still_data() {
        let mut source_metadata = std::collections::HashMap::new();
        source_metadata.insert(VCF_FIELD_FIELD_TYPE_KEY.to_string(), "INFO".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new(VCF_INFO_KEYS_COLUMN, DataType::Int32, true).with_metadata(source_metadata),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(50.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                Arc::new(Int32Array::from(vec![Some(42)])),
            ],
        )
        .unwrap();

        let lines = batch_to_vcf_lines(&batch, &[VCF_INFO_KEYS_COLUMN.to_string()], &[], &[], true)
            .unwrap();
        assert_eq!(
            columns_of(&lines[0].line)[7],
            format!("{VCF_INFO_KEYS_COLUMN}=42")
        );
    }

    /// A projection can keep the layout column while dropping a FORMAT child.
    /// The carried list still names that key, but the batch has no column for
    /// it, so there is nothing to reproduce — emitting the key with `.` for
    /// every sample would invent a field the writer cannot fill.
    #[test]
    fn a_carried_format_key_the_batch_does_not_supply_is_dropped() {
        let mut gt = ListBuilder::new(StringBuilder::new());
        gt.values().append_value("0/1");
        gt.values().append_value("1/1");
        gt.append(true);
        let mut dp = ListBuilder::new(Int32Builder::new());
        dp.values().append_value(25);
        dp.values().append_value(30);
        dp.append(true);

        // `PS` is absent from the struct entirely — projected away upstream.
        let genotypes = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "GT",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                )),
                Arc::new(gt.finish()) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "DP",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                )),
                Arc::new(dp.finish()) as ArrayRef,
            ),
        ]);

        let mut fields = create_test_schema().fields().to_vec();
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["chr1"])),
            Arc::new(UInt32Array::from(vec![99u32])),
            Arc::new(UInt32Array::from(vec![100u32])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["G"])),
            Arc::new(Float64Array::from(vec![Some(50.0)])),
            Arc::new(StringArray::from(vec![Some("PASS")])),
        ];
        fields.push(Arc::new(Field::new(
            "genotypes",
            genotypes.data_type().clone(),
            true,
        )));
        columns.push(Arc::new(genotypes));
        fields.push(Arc::new(layout_field(
            VCF_INFO_KEYS_COLUMN,
            VCF_RECORD_LAYOUT_INFO_KEYS,
        )));
        columns.push(Arc::new(StringArray::from(vec![None::<&str>])));
        fields.push(Arc::new(layout_field(
            VCF_FORMAT_KEYS_COLUMN,
            VCF_RECORD_LAYOUT_FORMAT_KEYS,
        )));
        columns.push(Arc::new(StringArray::from(vec![Some("GT:PS:DP")])));

        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let format_fields: Vec<String> = ["GT", "PS", "DP"].iter().map(|s| s.to_string()).collect();
        let samples: Vec<String> = ["S1", "S2"].iter().map(|s| s.to_string()).collect();
        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &samples, true).unwrap();

        let cols = columns_of(&lines[0].line);
        assert_eq!(cols[8], "GT:DP");
        assert_eq!(cols[9], "0/1:25");
        assert_eq!(cols[10], "1/1:30");
    }
}
