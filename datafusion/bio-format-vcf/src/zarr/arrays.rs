use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::metadata::VCF_FIELD_FIELD_TYPE_KEY;
use zarrs::array::{Array, ArraySubset, ElementOwned};
use zarrs::filesystem::FilesystemStore;

use super::metadata::VcfZarrMetadata;
use super::table_provider::VcfZarrReadOptions;

pub(crate) fn read_projected_arrays(
    metadata: &VcfZarrMetadata,
    schema: &SchemaRef,
    options: &VcfZarrReadOptions,
    limit: Option<usize>,
) -> Result<Vec<ArrayRef>> {
    let row_count = projected_row_count(metadata, limit)?;
    let mut cache = ArrayCache::new(metadata, row_count, options.coordinate_system_zero_based);
    let mut arrays = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "chrom" => Arc::new(StringArray::from(cache.chrom()?)),
            "start" => Arc::new(UInt32Array::from(cache.start()?)),
            "end" => Arc::new(UInt32Array::from(cache.end()?)),
            "id" => Arc::new(StringArray::from(cache.id()?)),
            "ref" => Arc::new(StringArray::from(cache.ref_allele()?)),
            "alt" => Arc::new(StringArray::from(cache.alt()?)),
            "qual" => Arc::new(Float64Array::from(cache.qual()?)),
            "filter" => Arc::new(StringArray::from(cache.filter()?)),
            "genotypes" => {
                return Err(DataFusionError::NotImplemented(
                    "VCF Zarr FORMAT/genotypes execution is not implemented yet".to_string(),
                ));
            }
            info_field
                if field
                    .metadata()
                    .get(VCF_FIELD_FIELD_TYPE_KEY)
                    .is_some_and(|field_type| field_type == "INFO") =>
            {
                Arc::new(StringArray::from(read_info_values(
                    metadata, info_field, row_count,
                )?))
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "VCF Zarr logical column '{other}' is not implemented"
                )));
            }
        };
        arrays.push(array);
    }

    Ok(arrays)
}

fn projected_row_count(metadata: &VcfZarrMetadata, limit: Option<usize>) -> Result<usize> {
    let variant_position = metadata.open_array("variant_position")?;
    let variants = usize::try_from(*variant_position.shape().first().ok_or_else(|| {
        DataFusionError::Execution("variant_position is not 1-dimensional".to_string())
    })?)
    .map_err(|_| DataFusionError::Execution("variant count exceeds platform size".to_string()))?;
    Ok(limit.map_or(variants, |limit| limit.min(variants)))
}

struct ArrayCache<'a> {
    metadata: &'a VcfZarrMetadata,
    row_count: usize,
    coordinate_system_zero_based: bool,
    contig_ids: Option<Vec<String>>,
    contig_indices: Option<Vec<i64>>,
    positions: Option<Vec<i64>>,
    lengths: Option<Vec<i64>>,
    alleles: Option<Vec<String>>,
    allele_width: Option<usize>,
}

impl<'a> ArrayCache<'a> {
    fn new(
        metadata: &'a VcfZarrMetadata,
        row_count: usize,
        coordinate_system_zero_based: bool,
    ) -> Self {
        Self {
            metadata,
            row_count,
            coordinate_system_zero_based,
            contig_ids: None,
            contig_indices: None,
            positions: None,
            lengths: None,
            alleles: None,
            allele_width: None,
        }
    }

    fn chrom(&mut self) -> Result<Vec<String>> {
        let contig_ids = self.contig_ids()?.clone();
        let contig_indices = self.contig_indices()?.clone();
        contig_indices
            .iter()
            .map(|index| {
                let index = usize::try_from(*index).map_err(|_| {
                    DataFusionError::Execution(format!("negative VCF Zarr contig index {index}"))
                })?;
                contig_ids.get(index).cloned().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "VCF Zarr contig index {index} is out of range for contig_id"
                    ))
                })
            })
            .collect()
    }

    fn start(&mut self) -> Result<Vec<u32>> {
        let zero_based = self.coordinate_system_zero_based;
        self.positions()?
            .iter()
            .map(|position| {
                let value = if zero_based {
                    position.checked_sub(1).ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "VCF Zarr variant_position {position} cannot be converted to zero-based"
                        ))
                    })?
                } else {
                    *position
                };
                u32::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "VCF Zarr start coordinate {value} does not fit UInt32"
                    ))
                })
            })
            .collect()
    }

    fn end(&mut self) -> Result<Vec<u32>> {
        let starts = self.start()?;
        let zero_based = self.coordinate_system_zero_based;
        let lengths = self.lengths()?;
        starts
            .iter()
            .zip(lengths.iter())
            .map(|(start, length)| {
                let length = u32::try_from((*length).max(1)).map_err(|_| {
                    DataFusionError::Execution(format!("invalid VCF Zarr variant_length {length}"))
                })?;
                if zero_based {
                    start.checked_add(length).ok_or_else(|| {
                        DataFusionError::Execution("VCF Zarr end coordinate overflow".to_string())
                    })
                } else {
                    start
                        .checked_add(length)
                        .and_then(|value| value.checked_sub(1))
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "VCF Zarr end coordinate overflow".to_string(),
                            )
                        })
                }
            })
            .collect()
    }

    fn id(&mut self) -> Result<Vec<Option<String>>> {
        if !self.metadata.array_exists("variant_id") {
            return Ok(vec![None; self.row_count]);
        }
        Ok(
            read_strings_1d(self.metadata, "variant_id", self.row_count)?
                .into_iter()
                .map(|value| {
                    let value = value.trim().to_string();
                    (!value.is_empty() && value != ".").then_some(value)
                })
                .collect(),
        )
    }

    fn ref_allele(&mut self) -> Result<Vec<String>> {
        let allele_width = self.allele_width()?;
        let alleles = self.alleles()?.clone();
        Ok((0..self.row_count)
            .map(|row| alleles[row * allele_width].clone())
            .collect())
    }

    fn alt(&mut self) -> Result<Vec<String>> {
        let allele_width = self.allele_width()?;
        let alleles = self.alleles()?.clone();
        Ok((0..self.row_count)
            .map(|row| {
                let start = row * allele_width + 1;
                let end = (row + 1) * allele_width;
                let values: Vec<&str> = alleles[start..end]
                    .iter()
                    .map(String::as_str)
                    .filter(|value| !value.is_empty() && *value != ".")
                    .collect();
                if values.is_empty() {
                    ".".to_string()
                } else {
                    values.join(",")
                }
            })
            .collect())
    }

    fn qual(&mut self) -> Result<Vec<Option<f64>>> {
        if !self.metadata.array_exists("variant_quality") {
            return Ok(vec![None; self.row_count]);
        }
        Ok(
            read_f64_1d(self.metadata, "variant_quality", self.row_count)?
                .into_iter()
                .map(|value| value.is_finite().then_some(value))
                .collect(),
        )
    }

    fn filter(&mut self) -> Result<Vec<Option<String>>> {
        if !self.metadata.array_exists("variant_filter") || !self.metadata.array_exists("filter_id")
        {
            return Ok(vec![None; self.row_count]);
        }

        let filter_ids = read_strings_all(self.metadata, "filter_id")?;
        let filter_array = self.metadata.open_array("variant_filter")?;
        let width = second_dimension(&filter_array, "variant_filter")?;
        let values = read_bool_2d(self.metadata, "variant_filter", self.row_count, width)?;

        Ok((0..self.row_count)
            .map(|row| {
                let active: Vec<&str> = (0..width)
                    .filter(|col| values[row * width + col])
                    .filter_map(|col| filter_ids.get(col).map(String::as_str))
                    .filter(|value| !value.is_empty())
                    .collect();
                if active.is_empty() {
                    Some("PASS".to_string())
                } else {
                    Some(active.join(","))
                }
            })
            .collect())
    }

    fn contig_ids(&mut self) -> Result<&Vec<String>> {
        if self.contig_ids.is_none() {
            self.contig_ids = Some(read_strings_all(self.metadata, "contig_id")?);
        }
        Ok(self.contig_ids.as_ref().expect("contig ids initialized"))
    }

    fn contig_indices(&mut self) -> Result<&Vec<i64>> {
        if self.contig_indices.is_none() {
            self.contig_indices = Some(read_i64_1d(
                self.metadata,
                "variant_contig",
                self.row_count,
            )?);
        }
        Ok(self
            .contig_indices
            .as_ref()
            .expect("contig indices initialized"))
    }

    fn positions(&mut self) -> Result<&Vec<i64>> {
        if self.positions.is_none() {
            self.positions = Some(read_i64_1d(
                self.metadata,
                "variant_position",
                self.row_count,
            )?);
        }
        Ok(self.positions.as_ref().expect("positions initialized"))
    }

    fn lengths(&mut self) -> Result<&Vec<i64>> {
        if self.lengths.is_none() {
            self.lengths = if self.metadata.array_exists("variant_length") {
                Some(read_i64_1d(
                    self.metadata,
                    "variant_length",
                    self.row_count,
                )?)
            } else {
                Some(vec![1; self.row_count])
            };
        }
        Ok(self.lengths.as_ref().expect("lengths initialized"))
    }

    fn alleles(&mut self) -> Result<&Vec<String>> {
        if self.alleles.is_none() {
            let allele_array = self.metadata.open_array("variant_allele")?;
            let width = second_dimension(&allele_array, "variant_allele")?;
            self.allele_width = Some(width);
            self.alleles = Some(read_strings_2d(
                self.metadata,
                "variant_allele",
                self.row_count,
                width,
            )?);
        }
        Ok(self.alleles.as_ref().expect("alleles initialized"))
    }

    fn allele_width(&mut self) -> Result<usize> {
        if self.allele_width.is_none() {
            let _ = self.alleles()?;
        }
        Ok(self.allele_width.expect("allele width initialized"))
    }
}

fn read_info_values(
    metadata: &VcfZarrMetadata,
    field_id: &str,
    row_count: usize,
) -> Result<Vec<Option<String>>> {
    let raw_array = format!("variant_{field_id}");
    let array = metadata.open_array(&raw_array)?;
    let values = if array.shape().len() == 1 {
        read_any_1d_as_strings(metadata, &raw_array, row_count)?
            .into_iter()
            .map(|value| {
                let value = value.trim().to_string();
                (!value.is_empty() && value != ".").then_some(value)
            })
            .collect()
    } else if array.shape().len() == 2 {
        let width = second_dimension(&array, &raw_array)?;
        let flat = read_any_2d_as_strings(metadata, &raw_array, row_count, width)?;
        (0..row_count)
            .map(|row| {
                let start = row * width;
                let end = (row + 1) * width;
                let joined = flat[start..end]
                    .iter()
                    .map(String::as_str)
                    .filter(|value| !value.is_empty() && *value != ".")
                    .collect::<Vec<_>>()
                    .join(",");
                (!joined.is_empty()).then_some(joined)
            })
            .collect()
    } else {
        return Err(DataFusionError::Execution(format!(
            "VCF Zarr INFO array '{raw_array}' has unsupported shape {:?}",
            array.shape()
        )));
    };

    Ok(values)
}

fn read_i64_1d(metadata: &VcfZarrMetadata, name: &str, row_count: usize) -> Result<Vec<i64>> {
    let array = metadata.open_array(name)?;
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "int8", "|i1") {
        read_vec_1d::<i8>(&array, row_count).map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "int16", "<i2") {
        read_vec_1d::<i16>(&array, row_count).map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "int32", "<i4") {
        read_vec_1d::<i32>(&array, row_count).map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "int64", "<i8") {
        read_vec_1d::<i64>(&array, row_count)
    } else if is_dtype(&data_type, "uint8", "|u1") {
        read_vec_1d::<u8>(&array, row_count).map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "uint16", "<u2") {
        read_vec_1d::<u16>(&array, row_count).map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "uint32", "<u4") {
        read_vec_1d::<u32>(&array, row_count).map(|v| v.into_iter().map(i64::from).collect())
    } else {
        Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' has unsupported integer data type {data_type}"
        )))
    }
}

fn read_f64_1d(metadata: &VcfZarrMetadata, name: &str, row_count: usize) -> Result<Vec<f64>> {
    let array = metadata.open_array(name)?;
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "float32", "<f4") {
        read_vec_1d::<f32>(&array, row_count).map(|v| v.into_iter().map(f64::from).collect())
    } else if is_dtype(&data_type, "float64", "<f8") {
        read_vec_1d::<f64>(&array, row_count)
    } else {
        Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' has unsupported floating data type {data_type}"
        )))
    }
}

fn read_bool_2d(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_count: usize,
    width: usize,
) -> Result<Vec<bool>> {
    let array = metadata.open_array(name)?;
    read_vec_2d::<bool>(&array, row_count, width)
}

fn read_strings_all(metadata: &VcfZarrMetadata, name: &str) -> Result<Vec<String>> {
    let array = metadata.open_array(name)?;
    let row_count = usize::try_from(
        *array
            .shape()
            .first()
            .ok_or_else(|| DataFusionError::Execution(format!("{name} is not 1-dimensional")))?,
    )
    .map_err(|_| DataFusionError::Execution(format!("{name} length exceeds platform size")))?;
    read_vec_1d::<String>(&array, row_count)
}

fn read_strings_1d(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_count: usize,
) -> Result<Vec<String>> {
    let array = metadata.open_array(name)?;
    read_vec_1d::<String>(&array, row_count)
}

fn read_strings_2d(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_count: usize,
    width: usize,
) -> Result<Vec<String>> {
    let array = metadata.open_array(name)?;
    read_vec_2d::<String>(&array, row_count, width)
}

fn read_any_1d_as_strings(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_count: usize,
) -> Result<Vec<String>> {
    let array = metadata.open_array(name)?;
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "string", "|O") {
        read_vec_1d::<String>(&array, row_count)
    } else if is_dtype(&data_type, "bool", "|b1") {
        read_vec_1d::<bool>(&array, row_count)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int8", "|i1") {
        read_vec_1d::<i8>(&array, row_count)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int16", "<i2") {
        read_vec_1d::<i16>(&array, row_count)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int32", "<i4") {
        read_vec_1d::<i32>(&array, row_count)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int64", "<i8") {
        read_vec_1d::<i64>(&array, row_count)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "uint8", "|u1") {
        read_vec_1d::<u8>(&array, row_count)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "uint16", "<u2") {
        read_vec_1d::<u16>(&array, row_count)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "uint32", "<u4") {
        read_vec_1d::<u32>(&array, row_count)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "float32", "<f4") {
        read_vec_1d::<f32>(&array, row_count)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "float64", "<f8") {
        read_vec_1d::<f64>(&array, row_count)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else {
        Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' has unsupported INFO data type {data_type}"
        )))
    }
}

fn read_any_2d_as_strings(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_count: usize,
    width: usize,
) -> Result<Vec<String>> {
    let array = metadata.open_array(name)?;
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "string", "|O") {
        read_vec_2d::<String>(&array, row_count, width)
    } else if is_dtype(&data_type, "bool", "|b1") {
        read_vec_2d::<bool>(&array, row_count, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int8", "|i1") {
        read_vec_2d::<i8>(&array, row_count, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int16", "<i2") {
        read_vec_2d::<i16>(&array, row_count, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int32", "<i4") {
        read_vec_2d::<i32>(&array, row_count, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "float32", "<f4") {
        read_vec_2d::<f32>(&array, row_count, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "float64", "<f8") {
        read_vec_2d::<f64>(&array, row_count, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else {
        Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' has unsupported INFO data type {data_type}"
        )))
    }
}

fn is_dtype(actual: &str, zarrs_name: &str, v2_name: &str) -> bool {
    actual == zarrs_name
        || actual == v2_name
        || actual.starts_with(&format!("{zarrs_name} /"))
        || actual.ends_with(&format!("/ {v2_name}"))
}

fn read_vec_1d<T>(array: &Array<FilesystemStore>, row_count: usize) -> Result<Vec<T>>
where
    T: ElementOwned,
    Vec<T>: zarrs::array::FromArrayBytes,
{
    let end = u64::try_from(row_count)
        .map_err(|_| DataFusionError::Execution("row count exceeds u64".to_string()))?;
    let subset = ArraySubset::new_with_shape(vec![end]);
    array
        .retrieve_array_subset::<Vec<T>>(&subset)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to read VCF Zarr array '{}': {error}",
                array.path()
            ))
        })
}

fn read_vec_2d<T>(array: &Array<FilesystemStore>, row_count: usize, width: usize) -> Result<Vec<T>>
where
    T: ElementOwned,
    Vec<T>: zarrs::array::FromArrayBytes,
{
    let end = u64::try_from(row_count)
        .map_err(|_| DataFusionError::Execution("row count exceeds u64".to_string()))?;
    let width = u64::try_from(width)
        .map_err(|_| DataFusionError::Execution("array width exceeds u64".to_string()))?;
    let subset = ArraySubset::new_with_ranges(&[0..end, 0..width]);
    array
        .retrieve_array_subset::<Vec<T>>(&subset)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to read VCF Zarr array '{}': {error}",
                array.path()
            ))
        })
}

fn second_dimension(array: &Array<FilesystemStore>, name: &str) -> Result<usize> {
    if array.shape().len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' must be 2-dimensional; got shape {:?}",
            array.shape()
        )));
    }
    usize::try_from(array.shape()[1]).map_err(|_| {
        DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' width exceeds platform size"
        ))
    })
}
