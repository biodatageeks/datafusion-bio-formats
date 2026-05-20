use datafusion::common::{DataFusionError, Result};
use zarrs::array::CodecOptions;

use super::arrays::read_strings_1d_for_pruning;
use super::metadata::VcfZarrMetadata;
use super::planning::RowSelection;
use super::table_provider::VcfZarrReadOptions;

/// Store sample names and selected source indexes for FORMAT reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SampleSelection {
    pub source_names: Vec<String>,
    pub selected_names: Vec<String>,
    pub selected_indices: Vec<usize>,
}

pub(crate) fn resolve_sample_selection(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
) -> Result<SampleSelection> {
    let codec_options = CodecOptions::default();
    resolve_sample_selection_with_options(metadata, options, &codec_options)
}

pub(crate) fn resolve_sample_selection_with_options(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
    codec_options: &CodecOptions,
) -> Result<SampleSelection> {
    let source_names = read_source_sample_names(metadata, options, codec_options)?;

    let (selected_names, selected_indices) = match &options.samples {
        Some(requested) => {
            let mut names = Vec::new();
            let mut indices = Vec::new();
            for sample in requested {
                if let Some(index) = source_names.iter().position(|name| name == sample) {
                    names.push(sample.clone());
                    indices.push(index);
                }
            }
            (names, indices)
        }
        None => (
            source_names.clone(),
            (0..source_names.len()).collect::<Vec<_>>(),
        ),
    };

    Ok(SampleSelection {
        source_names,
        selected_names,
        selected_indices,
    })
}

pub(crate) fn format_array_name(metadata: &VcfZarrMetadata, field_id: &str) -> Result<String> {
    let raw_array = format!("call_{field_id}");
    if metadata.array_exists(&raw_array) {
        return Ok(raw_array);
    }

    if field_id == "GT" && metadata.array_exists("call_genotype") {
        return Ok("call_genotype".to_string());
    }

    let expected = if field_id == "GT" {
        "call_GT or call_genotype".to_string()
    } else {
        raw_array
    };
    Err(DataFusionError::Execution(format!(
        "Requested VCF Zarr FORMAT field '{field_id}' requires readable raw array '{expected}'"
    )))
}

fn read_source_sample_names(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
    codec_options: &CodecOptions,
) -> Result<Vec<String>> {
    if metadata.array_exists("sample_id") {
        let sample_id = metadata.open_array("sample_id")?;
        if sample_id.shape().len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "VCF Zarr sample_id must be 1-dimensional; got shape {:?}",
                sample_id.shape()
            )));
        }
        let row_count = usize::try_from(sample_id.shape()[0]).map_err(|_| {
            DataFusionError::Execution("sample_id length exceeds platform size".to_string())
        })?;
        return read_strings_1d_for_pruning(
            metadata,
            "sample_id",
            &RowSelection::all(row_count),
            codec_options,
        );
    }

    if options.samples.is_some() {
        return Err(DataFusionError::Execution(
            "VCF Zarr sample selection requested sample names, but the store does not contain a sample_id array".to_string(),
        ));
    }

    infer_anonymous_sample_names(metadata, options)
}

fn infer_anonymous_sample_names(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
) -> Result<Vec<String>> {
    let Some(format_fields) = &options.format_fields else {
        return Ok(Vec::new());
    };

    for field_id in format_fields {
        if let Ok(raw_array) = format_array_name(metadata, field_id) {
            let array = metadata.open_array(&raw_array)?;
            if array.shape().len() >= 2 {
                let sample_count = usize::try_from(array.shape()[1]).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "VCF Zarr FORMAT array '{raw_array}' sample dimension exceeds platform size"
                    ))
                })?;
                return Ok((0..sample_count)
                    .map(|index| format!("sample_{index}"))
                    .collect());
            }
        }
    }

    Ok(Vec::new())
}
