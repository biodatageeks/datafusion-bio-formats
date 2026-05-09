use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Float64Array, ListBuilder, StringArray, StringBuilder, StructArray, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::metadata::{VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY};
use zarrs::array::{Array, ArraySubset, ElementOwned};
use zarrs::filesystem::FilesystemStore;

use super::metadata::VcfZarrMetadata;
use super::planning::RowSelection;
use super::samples::{format_array_name, resolve_sample_selection};
use super::table_provider::VcfZarrReadOptions;

pub(crate) fn read_projected_arrays(
    metadata: &VcfZarrMetadata,
    schema: &SchemaRef,
    options: &VcfZarrReadOptions,
    row_selection: &RowSelection,
) -> Result<Vec<ArrayRef>> {
    let mut cache = ArrayCache::new(
        metadata,
        row_selection.clone(),
        options.coordinate_system_zero_based,
    );
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
            "genotypes" => read_genotypes(metadata, field.data_type(), options, row_selection)?,
            info_field
                if field
                    .metadata()
                    .get(VCF_FIELD_FIELD_TYPE_KEY)
                    .is_some_and(|field_type| field_type == "INFO") =>
            {
                Arc::new(StringArray::from(read_info_values(
                    metadata,
                    info_field,
                    row_selection,
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

pub(crate) fn variant_count(metadata: &VcfZarrMetadata) -> Result<usize> {
    let variant_position = metadata.open_array("variant_position")?;
    let variants = usize::try_from(*variant_position.shape().first().ok_or_else(|| {
        DataFusionError::Execution("variant_position is not 1-dimensional".to_string())
    })?)
    .map_err(|_| DataFusionError::Execution("variant count exceeds platform size".to_string()))?;
    Ok(variants)
}

struct ArrayCache<'a> {
    metadata: &'a VcfZarrMetadata,
    row_selection: RowSelection,
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
        row_selection: RowSelection,
        coordinate_system_zero_based: bool,
    ) -> Self {
        Self {
            metadata,
            row_selection,
            coordinate_system_zero_based,
            contig_ids: None,
            contig_indices: None,
            positions: None,
            lengths: None,
            alleles: None,
            allele_width: None,
        }
    }

    fn row_count(&self) -> usize {
        self.row_selection.row_count()
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
            return Ok(vec![None; self.row_count()]);
        }
        Ok(
            read_strings_1d_for_pruning(self.metadata, "variant_id", &self.row_selection)?
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
        Ok((0..self.row_count())
            .map(|row| alleles[row * allele_width].clone())
            .collect())
    }

    fn alt(&mut self) -> Result<Vec<String>> {
        let allele_width = self.allele_width()?;
        let alleles = self.alleles()?.clone();
        Ok((0..self.row_count())
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
            return Ok(vec![None; self.row_count()]);
        }
        Ok(
            read_f64_1d(self.metadata, "variant_quality", &self.row_selection)?
                .into_iter()
                .map(|value| value.is_finite().then_some(value))
                .collect(),
        )
    }

    fn filter(&mut self) -> Result<Vec<Option<String>>> {
        if !self.metadata.array_exists("variant_filter") || !self.metadata.array_exists("filter_id")
        {
            return Ok(vec![None; self.row_count()]);
        }

        let filter_ids = read_strings_all(self.metadata, "filter_id")?;
        let filter_array = self.metadata.open_array("variant_filter")?;
        let width = second_dimension(&filter_array, "variant_filter")?;
        let values = read_bool_2d(self.metadata, "variant_filter", &self.row_selection, width)?;

        Ok((0..self.row_count())
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
                &self.row_selection,
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
                &self.row_selection,
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
                    &self.row_selection,
                )?)
            } else {
                Some(vec![1; self.row_count()])
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
                &self.row_selection,
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
    row_selection: &RowSelection,
) -> Result<Vec<Option<String>>> {
    let raw_array = format!("variant_{field_id}");
    let array = metadata.open_array(&raw_array)?;
    let values = if array.shape().len() == 1 {
        read_any_1d_as_strings(metadata, &raw_array, row_selection)?
            .into_iter()
            .map(|value| {
                let value = value.trim().to_string();
                (!value.is_empty() && value != ".").then_some(value)
            })
            .collect()
    } else if array.shape().len() == 2 {
        let width = second_dimension(&array, &raw_array)?;
        let flat = read_any_2d_as_strings(metadata, &raw_array, row_selection, width)?;
        (0..row_selection.row_count())
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

fn read_genotypes(
    metadata: &VcfZarrMetadata,
    data_type: &DataType,
    options: &VcfZarrReadOptions,
    row_selection: &RowSelection,
) -> Result<ArrayRef> {
    let DataType::Struct(children) = data_type else {
        return Err(DataFusionError::Execution(format!(
            "VCF Zarr genotypes column must be Struct, got {data_type:?}"
        )));
    };

    let sample_selection = resolve_sample_selection(metadata, options)?;
    let mut child_arrays = Vec::with_capacity(children.len());

    for child in children {
        let field_id = child
            .metadata()
            .get(VCF_FIELD_FORMAT_ID_KEY)
            .map(String::as_str)
            .unwrap_or_else(|| child.name().as_str());
        let raw_array = format_array_name(metadata, field_id)?;
        let values = read_format_values(
            metadata,
            &raw_array,
            field_id,
            row_selection,
            sample_selection.source_names.len(),
            &sample_selection.selected_indices,
        )?;
        child_arrays.push(build_format_list_array(
            values,
            row_selection.row_count(),
            sample_selection.selected_indices.len(),
        ));
    }

    Ok(Arc::new(StructArray::try_new(
        children.clone(),
        child_arrays,
        None,
    )?))
}

fn build_format_list_array(values: Vec<String>, row_count: usize, sample_count: usize) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());

    for row in 0..row_count {
        for sample in 0..sample_count {
            builder
                .values()
                .append_value(&values[row * sample_count + sample]);
        }
        builder.append(true);
    }

    Arc::new(builder.finish())
}

fn read_format_values(
    metadata: &VcfZarrMetadata,
    raw_array: &str,
    field_id: &str,
    row_selection: &RowSelection,
    source_sample_count: usize,
    sample_indices: &[usize],
) -> Result<Vec<String>> {
    let array = metadata.open_array(raw_array)?;
    if array.shape().len() < 2 {
        return Err(DataFusionError::Execution(format!(
            "VCF Zarr FORMAT array '{raw_array}' must have at least variants and samples dimensions; got shape {:?}",
            array.shape()
        )));
    }

    let array_sample_count = usize::try_from(array.shape()[1]).map_err(|_| {
        DataFusionError::Execution(format!(
            "VCF Zarr FORMAT array '{raw_array}' sample dimension exceeds platform size"
        ))
    })?;
    if source_sample_count != 0 && array_sample_count != source_sample_count {
        return Err(DataFusionError::Execution(format!(
            "VCF Zarr FORMAT array '{raw_array}' sample dimension {array_sample_count} does not match sample_id length {source_sample_count}"
        )));
    }
    if let Some(index) = sample_indices
        .iter()
        .find(|index| **index >= array_sample_count)
    {
        return Err(DataFusionError::Execution(format!(
            "VCF Zarr FORMAT array '{raw_array}' does not contain requested sample index {index}"
        )));
    }

    match array.shape().len() {
        2 => read_any_2d_selected_as_strings(&array, raw_array, row_selection, sample_indices),
        3 if is_gt_field(field_id, raw_array) => {
            read_genotype_3d_as_strings(metadata, &array, raw_array, row_selection, sample_indices)
        }
        3 => read_any_3d_selected_as_strings(&array, raw_array, row_selection, sample_indices),
        _ => Err(DataFusionError::Execution(format!(
            "VCF Zarr FORMAT array '{raw_array}' has unsupported shape {:?}",
            array.shape()
        ))),
    }
}

fn is_gt_field(field_id: &str, raw_array: &str) -> bool {
    field_id == "GT" || raw_array == "call_genotype"
}

fn read_any_2d_selected_as_strings(
    array: &Array<FilesystemStore>,
    name: &str,
    row_selection: &RowSelection,
    sample_indices: &[usize],
) -> Result<Vec<String>> {
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "string", "|O") {
        read_selected_2d_values::<String>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(format_string_scalar).collect())
    } else if is_dtype(&data_type, "bool", "|b1") {
        read_selected_2d_values::<bool>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int8", "|i1") {
        read_selected_2d_values::<i8>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(format_integer_scalar).collect())
    } else if is_dtype(&data_type, "int16", "<i2") {
        read_selected_2d_values::<i16>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(format_integer_scalar).collect())
    } else if is_dtype(&data_type, "int32", "<i4") {
        read_selected_2d_values::<i32>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(format_integer_scalar).collect())
    } else if is_dtype(&data_type, "int64", "<i8") {
        read_selected_2d_values::<i64>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(format_integer_scalar).collect())
    } else if is_dtype(&data_type, "uint8", "|u1") {
        read_selected_2d_values::<u8>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "uint16", "<u2") {
        read_selected_2d_values::<u16>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "uint32", "<u4") {
        read_selected_2d_values::<u32>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "uint64", "<u8") {
        read_selected_2d_values::<u64>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "float32", "<f4") {
        read_selected_2d_values::<f32>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(format_float_scalar).collect())
    } else if is_dtype(&data_type, "float64", "<f8") {
        read_selected_2d_values::<f64>(array, row_selection, sample_indices)
            .map(|values| values.into_iter().map(format_float_scalar).collect())
    } else {
        Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' has unsupported FORMAT data type {data_type}"
        )))
    }
}

fn read_any_3d_selected_as_strings(
    array: &Array<FilesystemStore>,
    name: &str,
    row_selection: &RowSelection,
    sample_indices: &[usize],
) -> Result<Vec<String>> {
    let width = third_dimension(array, name)?;
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "string", "|O") {
        read_selected_3d_collapsed_strings::<String, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |_, _, values| collapse_strings(values),
        )
    } else if is_dtype(&data_type, "bool", "|b1") {
        read_selected_3d_collapsed_strings::<bool, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |_, _, values| join_values(values.iter().map(|value| value.to_string())),
        )
    } else if is_dtype(&data_type, "int8", "|i1") {
        read_selected_3d_collapsed_strings::<i8, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |_, _, values| collapse_integers(values),
        )
    } else if is_dtype(&data_type, "int16", "<i2") {
        read_selected_3d_collapsed_strings::<i16, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |_, _, values| collapse_integers(values),
        )
    } else if is_dtype(&data_type, "int32", "<i4") {
        read_selected_3d_collapsed_strings::<i32, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |_, _, values| collapse_integers(values),
        )
    } else if is_dtype(&data_type, "int64", "<i8") {
        read_selected_3d_collapsed_strings::<i64, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |_, _, values| collapse_integers(values),
        )
    } else if is_dtype(&data_type, "float32", "<f4") {
        read_selected_3d_collapsed_strings::<f32, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |_, _, values| collapse_floats(values),
        )
    } else if is_dtype(&data_type, "float64", "<f8") {
        read_selected_3d_collapsed_strings::<f64, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |_, _, values| collapse_floats(values),
        )
    } else {
        Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' has unsupported FORMAT data type {data_type}"
        )))
    }
}

fn read_genotype_3d_as_strings(
    metadata: &VcfZarrMetadata,
    array: &Array<FilesystemStore>,
    name: &str,
    row_selection: &RowSelection,
    sample_indices: &[usize],
) -> Result<Vec<String>> {
    let width = third_dimension(array, name)?;
    let phased = if metadata.array_exists("call_genotype_phased") {
        let phased_array = metadata.open_array("call_genotype_phased")?;
        read_selected_2d_values::<bool>(&phased_array, row_selection, sample_indices)?
    } else {
        vec![
            false;
            row_selection
                .row_count()
                .saturating_mul(sample_indices.len())
        ]
    };

    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "int8", "|i1") {
        read_selected_3d_collapsed_strings::<i8, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |row, sample, values| {
                format_gt_values(
                    values.iter().map(|value| i64::from(*value)),
                    phased[row * sample_indices.len() + sample],
                )
            },
        )
    } else if is_dtype(&data_type, "int16", "<i2") {
        read_selected_3d_collapsed_strings::<i16, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |row, sample, values| {
                format_gt_values(
                    values.iter().map(|value| i64::from(*value)),
                    phased[row * sample_indices.len() + sample],
                )
            },
        )
    } else if is_dtype(&data_type, "int32", "<i4") {
        read_selected_3d_collapsed_strings::<i32, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |row, sample, values| {
                format_gt_values(
                    values.iter().map(|value| i64::from(*value)),
                    phased[row * sample_indices.len() + sample],
                )
            },
        )
    } else if is_dtype(&data_type, "int64", "<i8") {
        read_selected_3d_collapsed_strings::<i64, _>(
            array,
            row_selection,
            sample_indices,
            width,
            |row, sample, values| {
                format_gt_values(
                    values.iter().copied(),
                    phased[row * sample_indices.len() + sample],
                )
            },
        )
    } else {
        read_any_3d_selected_as_strings(array, name, row_selection, sample_indices)
    }
}

pub(crate) fn read_i64_1d(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_selection: &RowSelection,
) -> Result<Vec<i64>> {
    let array = metadata.open_array(name)?;
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "int8", "|i1") {
        read_vec_1d::<i8>(&array, row_selection).map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "int16", "<i2") {
        read_vec_1d::<i16>(&array, row_selection).map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "int32", "<i4") {
        read_vec_1d::<i32>(&array, row_selection).map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "int64", "<i8") {
        read_vec_1d::<i64>(&array, row_selection)
    } else if is_dtype(&data_type, "uint8", "|u1") {
        read_vec_1d::<u8>(&array, row_selection).map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "uint16", "<u2") {
        read_vec_1d::<u16>(&array, row_selection).map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "uint32", "<u4") {
        read_vec_1d::<u32>(&array, row_selection).map(|v| v.into_iter().map(i64::from).collect())
    } else {
        Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' has unsupported integer data type {data_type}"
        )))
    }
}

pub(crate) fn read_i64_2d(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_selection: &RowSelection,
    width: usize,
) -> Result<Vec<i64>> {
    let array = metadata.open_array(name)?;
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "int8", "|i1") {
        read_vec_2d::<i8>(&array, row_selection, width)
            .map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "int16", "<i2") {
        read_vec_2d::<i16>(&array, row_selection, width)
            .map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "int32", "<i4") {
        read_vec_2d::<i32>(&array, row_selection, width)
            .map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "int64", "<i8") {
        read_vec_2d::<i64>(&array, row_selection, width)
    } else if is_dtype(&data_type, "uint8", "|u1") {
        read_vec_2d::<u8>(&array, row_selection, width)
            .map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "uint16", "<u2") {
        read_vec_2d::<u16>(&array, row_selection, width)
            .map(|v| v.into_iter().map(i64::from).collect())
    } else if is_dtype(&data_type, "uint32", "<u4") {
        read_vec_2d::<u32>(&array, row_selection, width)
            .map(|v| v.into_iter().map(i64::from).collect())
    } else {
        Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' has unsupported integer data type {data_type}"
        )))
    }
}

fn read_f64_1d(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_selection: &RowSelection,
) -> Result<Vec<f64>> {
    let array = metadata.open_array(name)?;
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "float32", "<f4") {
        read_vec_1d::<f32>(&array, row_selection).map(|v| v.into_iter().map(f64::from).collect())
    } else if is_dtype(&data_type, "float64", "<f8") {
        read_vec_1d::<f64>(&array, row_selection)
    } else {
        Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' has unsupported floating data type {data_type}"
        )))
    }
}

fn read_bool_2d(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_selection: &RowSelection,
    width: usize,
) -> Result<Vec<bool>> {
    let array = metadata.open_array(name)?;
    read_vec_2d::<bool>(&array, row_selection, width)
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
    read_vec_1d::<String>(&array, &RowSelection::all(row_count))
}

pub(crate) fn read_strings_1d_for_pruning(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_selection: &RowSelection,
) -> Result<Vec<String>> {
    let array = metadata.open_array(name)?;
    read_vec_1d::<String>(&array, row_selection)
}

fn read_strings_2d(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_selection: &RowSelection,
    width: usize,
) -> Result<Vec<String>> {
    let array = metadata.open_array(name)?;
    read_vec_2d::<String>(&array, row_selection, width)
}

fn read_any_1d_as_strings(
    metadata: &VcfZarrMetadata,
    name: &str,
    row_selection: &RowSelection,
) -> Result<Vec<String>> {
    let array = metadata.open_array(name)?;
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "string", "|O") {
        read_vec_1d::<String>(&array, row_selection)
    } else if is_dtype(&data_type, "bool", "|b1") {
        read_vec_1d::<bool>(&array, row_selection)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int8", "|i1") {
        read_vec_1d::<i8>(&array, row_selection)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int16", "<i2") {
        read_vec_1d::<i16>(&array, row_selection)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int32", "<i4") {
        read_vec_1d::<i32>(&array, row_selection)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int64", "<i8") {
        read_vec_1d::<i64>(&array, row_selection)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "uint8", "|u1") {
        read_vec_1d::<u8>(&array, row_selection)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "uint16", "<u2") {
        read_vec_1d::<u16>(&array, row_selection)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "uint32", "<u4") {
        read_vec_1d::<u32>(&array, row_selection)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "float32", "<f4") {
        read_vec_1d::<f32>(&array, row_selection)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "float64", "<f8") {
        read_vec_1d::<f64>(&array, row_selection)
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
    row_selection: &RowSelection,
    width: usize,
) -> Result<Vec<String>> {
    let array = metadata.open_array(name)?;
    let data_type = array.data_type().to_string();
    if is_dtype(&data_type, "string", "|O") {
        read_vec_2d::<String>(&array, row_selection, width)
    } else if is_dtype(&data_type, "bool", "|b1") {
        read_vec_2d::<bool>(&array, row_selection, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int8", "|i1") {
        read_vec_2d::<i8>(&array, row_selection, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int16", "<i2") {
        read_vec_2d::<i16>(&array, row_selection, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "int32", "<i4") {
        read_vec_2d::<i32>(&array, row_selection, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "float32", "<f4") {
        read_vec_2d::<f32>(&array, row_selection, width)
            .map(|v| v.into_iter().map(|value| value.to_string()).collect())
    } else if is_dtype(&data_type, "float64", "<f8") {
        read_vec_2d::<f64>(&array, row_selection, width)
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

fn read_vec_1d<T>(array: &Array<FilesystemStore>, row_selection: &RowSelection) -> Result<Vec<T>>
where
    T: ElementOwned,
    Vec<T>: zarrs::array::FromArrayBytes,
{
    let mut values = Vec::with_capacity(row_selection.row_count());

    for range in &row_selection.ranges {
        let start = u64::try_from(range.start)
            .map_err(|_| DataFusionError::Execution("row range start exceeds u64".to_string()))?;
        let end = u64::try_from(range.end)
            .map_err(|_| DataFusionError::Execution("row range end exceeds u64".to_string()))?;
        let ranges = std::iter::once(start..end).collect::<Vec<_>>();
        let subset = ArraySubset::new_with_ranges(&ranges);
        let mut chunk = array
            .retrieve_array_subset::<Vec<T>>(&subset)
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "Failed to read VCF Zarr array '{}': {error}",
                    array.path()
                ))
            })?;
        values.append(&mut chunk);
    }

    Ok(values)
}

fn read_vec_2d<T>(
    array: &Array<FilesystemStore>,
    row_selection: &RowSelection,
    width: usize,
) -> Result<Vec<T>>
where
    T: ElementOwned,
    Vec<T>: zarrs::array::FromArrayBytes,
{
    let width_usize = width;
    let width = u64::try_from(width_usize)
        .map_err(|_| DataFusionError::Execution("array width exceeds u64".to_string()))?;
    let mut values = Vec::with_capacity(row_selection.row_count().saturating_mul(width_usize));

    for range in &row_selection.ranges {
        let start = u64::try_from(range.start)
            .map_err(|_| DataFusionError::Execution("row range start exceeds u64".to_string()))?;
        let end = u64::try_from(range.end)
            .map_err(|_| DataFusionError::Execution("row range end exceeds u64".to_string()))?;
        let subset = ArraySubset::new_with_ranges(&[start..end, 0..width]);
        let mut chunk = array
            .retrieve_array_subset::<Vec<T>>(&subset)
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "Failed to read VCF Zarr array '{}': {error}",
                    array.path()
                ))
            })?;
        values.append(&mut chunk);
    }

    Ok(values)
}

fn read_selected_2d_values<T>(
    array: &Array<FilesystemStore>,
    row_selection: &RowSelection,
    sample_indices: &[usize],
) -> Result<Vec<T>>
where
    T: Clone + Default + ElementOwned,
    Vec<T>: zarrs::array::FromArrayBytes,
{
    let sample_count = sample_indices.len();
    if sample_count == 0 {
        return Ok(Vec::new());
    }

    let row_count = row_selection.row_count();
    let mut values = vec![T::default(); row_count.saturating_mul(sample_count)];
    let mut row_offset = 0;

    for range in &row_selection.ranges {
        let row_start = u64::try_from(range.start)
            .map_err(|_| DataFusionError::Execution("row range start exceeds u64".to_string()))?;
        let row_end = u64::try_from(range.end)
            .map_err(|_| DataFusionError::Execution("row range end exceeds u64".to_string()))?;
        let rows = range.end.saturating_sub(range.start);

        for (sample_out, sample_index) in sample_indices.iter().copied().enumerate() {
            let sample_start = u64::try_from(sample_index)
                .map_err(|_| DataFusionError::Execution("sample index exceeds u64".to_string()))?;
            let subset =
                ArraySubset::new_with_ranges(&[row_start..row_end, sample_start..sample_start + 1]);
            let chunk = array
                .retrieve_array_subset::<Vec<T>>(&subset)
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "Failed to read VCF Zarr array '{}': {error}",
                        array.path()
                    ))
                })?;
            for (row, value) in chunk.into_iter().enumerate() {
                values[(row_offset + row) * sample_count + sample_out] = value;
            }
        }

        row_offset += rows;
    }

    Ok(values)
}

fn read_selected_3d_collapsed_strings<T, F>(
    array: &Array<FilesystemStore>,
    row_selection: &RowSelection,
    sample_indices: &[usize],
    width: usize,
    collapse: F,
) -> Result<Vec<String>>
where
    T: ElementOwned,
    Vec<T>: zarrs::array::FromArrayBytes,
    F: Fn(usize, usize, &[T]) -> String,
{
    let sample_count = sample_indices.len();
    if sample_count == 0 {
        return Ok(Vec::new());
    }

    let row_count = row_selection.row_count();
    let mut values = vec![String::new(); row_count.saturating_mul(sample_count)];
    let mut row_offset = 0;
    let width_u64 = u64::try_from(width)
        .map_err(|_| DataFusionError::Execution("array width exceeds u64".to_string()))?;

    for range in &row_selection.ranges {
        let row_start = u64::try_from(range.start)
            .map_err(|_| DataFusionError::Execution("row range start exceeds u64".to_string()))?;
        let row_end = u64::try_from(range.end)
            .map_err(|_| DataFusionError::Execution("row range end exceeds u64".to_string()))?;
        let rows = range.end.saturating_sub(range.start);

        for (sample_out, sample_index) in sample_indices.iter().copied().enumerate() {
            let sample_start = u64::try_from(sample_index)
                .map_err(|_| DataFusionError::Execution("sample index exceeds u64".to_string()))?;
            let subset = ArraySubset::new_with_ranges(&[
                row_start..row_end,
                sample_start..sample_start + 1,
                0..width_u64,
            ]);
            let chunk = array
                .retrieve_array_subset::<Vec<T>>(&subset)
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "Failed to read VCF Zarr array '{}': {error}",
                        array.path()
                    ))
                })?;
            for row in 0..rows {
                let value_start = row * width;
                let value_end = value_start + width;
                values[(row_offset + row) * sample_count + sample_out] =
                    collapse(row_offset + row, sample_out, &chunk[value_start..value_end]);
            }
        }

        row_offset += rows;
    }

    Ok(values)
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

fn third_dimension(array: &Array<FilesystemStore>, name: &str) -> Result<usize> {
    if array.shape().len() != 3 {
        return Err(DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' must be 3-dimensional; got shape {:?}",
            array.shape()
        )));
    }
    usize::try_from(array.shape()[2]).map_err(|_| {
        DataFusionError::Execution(format!(
            "VCF Zarr array '{name}' width exceeds platform size"
        ))
    })
}

fn format_string_scalar(value: String) -> String {
    let value = value.trim().to_string();
    if value.is_empty() {
        ".".to_string()
    } else {
        value
    }
}

fn format_integer_scalar<T>(value: T) -> String
where
    T: Into<i64>,
{
    let value = value.into();
    if value == -1 || value == -2 {
        ".".to_string()
    } else {
        value.to_string()
    }
}

fn format_float_scalar<T>(value: T) -> String
where
    T: Into<f64>,
{
    let value = value.into();
    if value.is_finite() {
        value.to_string()
    } else {
        ".".to_string()
    }
}

fn collapse_strings(values: &[String]) -> String {
    join_values(
        values
            .iter()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| {
                if value == "." {
                    ".".to_string()
                } else {
                    value.to_string()
                }
            }),
    )
}

fn collapse_integers<T>(values: &[T]) -> String
where
    T: Copy + Into<i64>,
{
    join_values(values.iter().filter_map(|value| {
        let value = (*value).into();
        match value {
            -2 => None,
            -1 => Some(".".to_string()),
            value => Some(value.to_string()),
        }
    }))
}

fn collapse_floats<T>(values: &[T]) -> String
where
    T: Copy + Into<f64>,
{
    join_values(values.iter().map(|value| format_float_scalar(*value)))
}

fn format_gt_values(values: impl Iterator<Item = i64>, phased: bool) -> String {
    let separator = if phased { "|" } else { "/" };
    let alleles = values
        .filter_map(|value| match value {
            -2 => None,
            -1 => Some(".".to_string()),
            value => Some(value.to_string()),
        })
        .collect::<Vec<_>>();
    if alleles.is_empty() {
        ".".to_string()
    } else {
        alleles.join(separator)
    }
}

fn join_values(values: impl Iterator<Item = String>) -> String {
    let joined = values.collect::<Vec<_>>().join(",");
    if joined.is_empty() {
        ".".to_string()
    } else {
        joined
    }
}
