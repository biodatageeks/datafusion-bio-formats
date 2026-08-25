//! Low-level HDF5 helpers shared by the cooler reader.

use datafusion::common::{DataFusionError, Result};
use hdf5_metno::types::{FixedAscii, TypeDescriptor, VarLenAscii, VarLenUnicode};
use hdf5_metno::{Attribute, Conversion, Dataset, Group};

/// Map any hdf5 error into an external DataFusion error with context.
pub(crate) fn h5_err(context: &str, error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(format!(
        "{context}: {error}"
    ))))
}

/// Read a 1-D string dataset regardless of fixed/variable-length encoding.
/// Cooler writes chromosome names as fixed-length ASCII (h5py `S{n}`), but
/// other writers may use variable-length strings.
pub(crate) fn read_string_dataset(ds: &Dataset, what: &str) -> Result<Vec<String>> {
    let td = ds
        .dtype()
        .and_then(|dtype| dtype.to_descriptor())
        .map_err(|error| h5_err(&format!("Failed to read dtype of {what}"), error))?;
    let values = match td {
        TypeDescriptor::FixedAscii(size) | TypeDescriptor::FixedUnicode(size) if size > 256 => {
            return Err(h5_err(
                &format!("Unsupported string dtype for {what}"),
                format!("fixed-length strings wider than 256 bytes (got {size})"),
            ));
        }
        TypeDescriptor::FixedAscii(_) | TypeDescriptor::FixedUnicode(_) => ds
            .read_1d::<FixedAscii<256>>()
            .map_err(|error| h5_err(&format!("Failed to read {what}"), error))?
            .iter()
            .map(|value| value.to_string())
            .collect(),
        TypeDescriptor::VarLenAscii => ds
            .read_1d::<VarLenAscii>()
            .map_err(|error| h5_err(&format!("Failed to read {what}"), error))?
            .iter()
            .map(|value| value.to_string())
            .collect(),
        TypeDescriptor::VarLenUnicode => ds
            .read_1d::<VarLenUnicode>()
            .map_err(|error| h5_err(&format!("Failed to read {what}"), error))?
            .iter()
            .map(|value| value.to_string())
            .collect(),
        other => {
            return Err(h5_err(
                &format!("Unsupported string dtype for {what}"),
                format!("{other:?}"),
            ));
        }
    };
    Ok(values)
}

fn read_attr_string_value(attr: &Attribute, what: &str) -> Result<String> {
    let td = attr
        .dtype()
        .and_then(|dtype| dtype.to_descriptor())
        .map_err(|error| h5_err(&format!("Failed to read dtype of attribute {what}"), error))?;
    let value = match td {
        TypeDescriptor::VarLenUnicode => attr
            .read_scalar::<VarLenUnicode>()
            .map_err(|error| h5_err(&format!("Failed to read attribute {what}"), error))?
            .to_string(),
        TypeDescriptor::VarLenAscii => attr
            .read_scalar::<VarLenAscii>()
            .map_err(|error| h5_err(&format!("Failed to read attribute {what}"), error))?
            .to_string(),
        TypeDescriptor::FixedAscii(_) | TypeDescriptor::FixedUnicode(_) => attr
            .read_scalar::<FixedAscii<256>>()
            .map_err(|error| h5_err(&format!("Failed to read attribute {what}"), error))?
            .to_string(),
        other => {
            return Err(h5_err(
                &format!("Unsupported dtype for string attribute {what}"),
                format!("{other:?}"),
            ));
        }
    };
    Ok(value)
}

/// Read an optional string attribute from a group.
pub(crate) fn attr_string(group: &Group, name: &str) -> Result<Option<String>> {
    if !group
        .attr_names()
        .is_ok_and(|names| names.iter().any(|n| n == name))
    {
        return Ok(None);
    }
    let attr = group
        .attr(name)
        .map_err(|error| h5_err(&format!("Failed to open attribute {name}"), error))?;
    Ok(Some(read_attr_string_value(&attr, name)?))
}

/// Read an optional integer attribute from a group (any integer width).
///
/// cooler <=0.8.x wrote some numeric attributes as JSON strings (e.g.
/// `format-version = '3'` on `.mcool` resolution groups), so string-typed
/// attributes are parsed rather than rejected.
pub(crate) fn attr_i64(group: &Group, name: &str) -> Result<Option<i64>> {
    if !group
        .attr_names()
        .is_ok_and(|names| names.iter().any(|n| n == name))
    {
        return Ok(None);
    }
    let attr = group
        .attr(name)
        .map_err(|error| h5_err(&format!("Failed to open attribute {name}"), error))?;
    let td = attr
        .dtype()
        .and_then(|dtype| dtype.to_descriptor())
        .map_err(|error| h5_err(&format!("Failed to read dtype of attribute {name}"), error))?;
    if matches!(
        td,
        TypeDescriptor::FixedAscii(_)
            | TypeDescriptor::FixedUnicode(_)
            | TypeDescriptor::VarLenAscii
            | TypeDescriptor::VarLenUnicode
    ) {
        let text = read_attr_string_value(&attr, name)?;
        let value = text.trim().parse::<i64>().map_err(|error| {
            h5_err(
                &format!("Failed to parse string attribute {name} ({text:?}) as integer"),
                error,
            )
        })?;
        return Ok(Some(value));
    }
    let value = attr
        .as_reader()
        .conversion(Conversion::Soft)
        .read_scalar::<i64>()
        .map_err(|error| h5_err(&format!("Failed to read attribute {name}"), error))?;
    Ok(Some(value))
}

/// Read an optional float attribute from a group. Integer-typed attributes
/// convert losslessly for the magnitudes cooler stores; `sum` in particular
/// is a float for float-count coolers and must not be truncated.
pub(crate) fn attr_f64(group: &Group, name: &str) -> Result<Option<f64>> {
    if !group
        .attr_names()
        .is_ok_and(|names| names.iter().any(|n| n == name))
    {
        return Ok(None);
    }
    let attr = group
        .attr(name)
        .map_err(|error| h5_err(&format!("Failed to open attribute {name}"), error))?;
    let td = attr
        .dtype()
        .and_then(|dtype| dtype.to_descriptor())
        .map_err(|error| h5_err(&format!("Failed to read dtype of attribute {name}"), error))?;
    if matches!(
        td,
        TypeDescriptor::FixedAscii(_)
            | TypeDescriptor::FixedUnicode(_)
            | TypeDescriptor::VarLenAscii
            | TypeDescriptor::VarLenUnicode
    ) {
        // cooler <=0.8.x string-typed numeric attrs, same as attr_i64.
        let text = read_attr_string_value(&attr, name)?;
        let value = text.trim().parse::<f64>().map_err(|error| {
            h5_err(
                &format!("Failed to parse string attribute {name} ({text:?}) as float"),
                error,
            )
        })?;
        return Ok(Some(value));
    }
    let value = attr
        .as_reader()
        .conversion(Conversion::Soft)
        .read_scalar::<f64>()
        .map_err(|error| h5_err(&format!("Failed to read attribute {name}"), error))?;
    Ok(Some(value))
}

/// Read a whole 1-D numeric dataset with soft conversion (handles enum-typed
/// `bins/chrom` and any integer width used by the writer).
pub(crate) fn read_numeric_1d<T: hdf5_metno::H5Type + Clone>(
    ds: &Dataset,
    what: &str,
) -> Result<Vec<T>> {
    Ok(ds
        .as_reader()
        .conversion(Conversion::Soft)
        .read_1d::<T>()
        .map_err(|error| h5_err(&format!("Failed to read {what}"), error))?
        .to_vec())
}

/// Read a row range of a 1-D numeric dataset with soft conversion.
pub(crate) fn read_numeric_slice<T: hdf5_metno::H5Type + Clone>(
    ds: &Dataset,
    lo: usize,
    hi: usize,
    what: &str,
) -> Result<Vec<T>> {
    Ok(ds
        .as_reader()
        .conversion(Conversion::Soft)
        .read_slice_1d::<T, _>(ndarray::s![lo..hi])
        .map_err(|error| h5_err(&format!("Failed to read rows {lo}..{hi} of {what}"), error))?
        .to_vec())
}
