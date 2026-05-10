use std::collections::BTreeSet;

use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Expr;

use super::arrays::{read_i64_1d, read_i64_2d};
use super::metadata::VcfZarrMetadata;
use super::planning::{
    DeferredPositionPruning, PredicateConstraints, PruningMethod, RowPruning, RowSelection,
    predicate_constraints, zarr_read_options,
};
use super::table_provider::VcfZarrReadOptions;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VariantPositionLayout {
    pub row_count: usize,
    pub chunk_size: usize,
}

pub(crate) fn variant_position_layout(metadata: &VcfZarrMetadata) -> Result<VariantPositionLayout> {
    let variant_position = metadata.open_array("variant_position")?;
    if variant_position.shape().len() != 1 {
        return Err(DataFusionError::Execution(format!(
            "variant_position is not 1-dimensional: {:?}",
            variant_position.shape()
        )));
    }

    let row_count = usize::try_from(variant_position.shape()[0]).map_err(|_| {
        DataFusionError::Execution("variant count exceeds platform size".to_string())
    })?;
    let chunk_shape = variant_position.chunk_shape(&[0]).map_err(|error| {
        DataFusionError::Execution(format!(
            "Failed to read variant_position chunk shape for VCF Zarr pruning: {error}"
        ))
    })?;
    let chunk_size = chunk_shape
        .first()
        .ok_or_else(|| {
            DataFusionError::Execution("variant_position has empty chunk shape".to_string())
        })?
        .get();
    let chunk_size = usize::try_from(chunk_size).map_err(|_| {
        DataFusionError::Execution("variant_position chunk size exceeds platform size".to_string())
    })?;

    Ok(VariantPositionLayout {
        row_count,
        chunk_size,
    })
}

pub(crate) fn build_row_pruning(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
    filters: &[Expr],
    limit: Option<usize>,
    variant_position: VariantPositionLayout,
) -> Result<RowPruning> {
    let total_rows = variant_position.row_count;
    let Some(constraints) = predicate_constraints(filters) else {
        return Ok(RowPruning {
            selection: RowSelection::all(total_rows).limit(limit),
            method: PruningMethod::None,
            deferred_pruning: None,
        });
    };
    let codec_options = zarr_read_options();

    if let Some(candidate_rows) = region_index_candidate_rows(
        metadata,
        options,
        &constraints,
        total_rows,
        variant_position.chunk_size,
        &codec_options,
    )? {
        let selection = apply_position_array_pruning(
            metadata,
            options,
            &candidate_rows,
            &constraints,
            limit,
            &codec_options,
        )?;
        return Ok(RowPruning {
            selection,
            method: PruningMethod::RegionIndex,
            deferred_pruning: None,
        });
    }

    Ok(RowPruning {
        selection: RowSelection::all(total_rows),
        method: PruningMethod::PositionArrays,
        deferred_pruning: Some(DeferredPositionPruning { constraints, limit }),
    })
}

pub(crate) fn apply_position_array_pruning(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
    candidate_rows: &RowSelection,
    constraints: &PredicateConstraints,
    limit: Option<usize>,
    codec_options: &zarrs::array::CodecOptions,
) -> Result<RowSelection> {
    let positions = if !constraints.start.is_empty() || !constraints.end.is_empty() {
        Some(read_i64_1d(
            metadata,
            "variant_position",
            candidate_rows,
            codec_options,
        )?)
    } else {
        None
    };
    let lengths = if !constraints.end.is_empty() && metadata.array_exists("variant_length") {
        Some(read_i64_1d(
            metadata,
            "variant_length",
            candidate_rows,
            codec_options,
        )?)
    } else {
        None
    };
    let contig_indices = if constraints.chrom_values.is_some() {
        Some(read_i64_1d(
            metadata,
            "variant_contig",
            candidate_rows,
            codec_options,
        )?)
    } else {
        None
    };
    let contig_ids = if constraints.chrom_values.is_some() {
        Some(read_contig_ids(metadata, codec_options)?)
    } else {
        None
    };

    let mask = (0..candidate_rows.row_count())
        .map(|row| {
            if let (Some(values), Some(indices), Some(ids)) = (
                &constraints.chrom_values,
                contig_indices.as_ref(),
                contig_ids.as_ref(),
            ) {
                let index = usize::try_from(indices[row]).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "negative VCF Zarr contig index {}",
                        indices[row]
                    ))
                })?;
                let Some(chrom) = ids.get(index) else {
                    return Err(DataFusionError::Execution(format!(
                        "VCF Zarr contig index {index} is out of range for contig_id"
                    )));
                };
                if !values.contains(chrom) {
                    return Ok(false);
                }
            }

            if let Some(positions) = &positions {
                let start = logical_start(positions[row], options.coordinate_system_zero_based)?;
                if !constraints.start.contains(start) {
                    return Ok(false);
                }

                if !constraints.end.is_empty() {
                    let length = lengths.as_ref().map_or(1, |values| values[row]).max(1);
                    let end = logical_end(start, length, options.coordinate_system_zero_based)?;
                    if !constraints.end.contains(end) {
                        return Ok(false);
                    }
                }
            }

            Ok(true)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(candidate_rows.filter_mask(&mask)?.limit(limit))
}

fn region_index_candidate_rows(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
    constraints: &PredicateConstraints,
    total_rows: usize,
    chunk_size: usize,
    codec_options: &zarrs::array::CodecOptions,
) -> Result<Option<RowSelection>> {
    if !metadata.array_exists("region_index")
        || (constraints.chrom_values.is_none()
            && constraints.start.is_empty()
            && constraints.end.is_empty())
    {
        return Ok(None);
    }

    let region_index = metadata.open_array("region_index")?;
    if region_index.shape().len() != 2 || region_index.shape()[1] < 6 {
        return Err(DataFusionError::Execution(format!(
            "VCF Zarr region_index must have shape (N, 6); got {:?}",
            region_index.shape()
        )));
    }

    let row_count = usize::try_from(region_index.shape()[0]).map_err(|_| {
        DataFusionError::Execution("region_index row count exceeds platform size".to_string())
    })?;
    let width = usize::try_from(region_index.shape()[1]).map_err(|_| {
        DataFusionError::Execution("region_index width exceeds platform size".to_string())
    })?;
    let values = read_i64_2d(
        metadata,
        "region_index",
        &RowSelection::all(row_count),
        width,
        codec_options,
    )?;
    let allowed_contigs = allowed_contig_indices(metadata, constraints, codec_options)?;
    if allowed_contigs.as_ref().is_some_and(BTreeSet::is_empty) {
        return Ok(Some(RowSelection { ranges: Vec::new() }));
    }

    let mut chunks = BTreeSet::new();

    for row in 0..row_count {
        let offset = row * width;
        let chunk_index = values[offset];
        let contig_index = values[offset + 1];
        let region_start = values[offset + 2];
        let region_end = values[offset + 3];
        let region_max_end = values[offset + 4];
        let record_count = values[offset + 5];

        if record_count <= 0 {
            continue;
        }

        if let Some(allowed) = &allowed_contigs
            && !allowed.contains(&contig_index)
        {
            continue;
        }

        let logical_region_start =
            logical_start(region_start, options.coordinate_system_zero_based)?;
        let logical_region_end = logical_start(region_end, options.coordinate_system_zero_based)?;
        if !range_intersects(&constraints.start, logical_region_start, logical_region_end) {
            continue;
        }

        // VCF Zarr region_index max_end is already comparable to the exposed
        // end coordinate: one-based inclusive in one-based mode, and equal to
        // the zero-based half-open end after start conversion.
        let logical_region_max_end = region_max_end;
        if !range_intersects(
            &constraints.end,
            logical_region_start,
            logical_region_max_end,
        ) {
            continue;
        }

        let chunk_index = usize::try_from(chunk_index).map_err(|_| {
            DataFusionError::Execution(format!("negative VCF Zarr chunk index {chunk_index}"))
        })?;
        chunks.insert(chunk_index);
    }

    Ok(Some(row_selection_from_chunks(
        &chunks, chunk_size, total_rows,
    )))
}

fn allowed_contig_indices(
    metadata: &VcfZarrMetadata,
    constraints: &PredicateConstraints,
    codec_options: &zarrs::array::CodecOptions,
) -> Result<Option<BTreeSet<i64>>> {
    let Some(chrom_values) = &constraints.chrom_values else {
        return Ok(None);
    };

    let contig_ids = read_contig_ids(metadata, codec_options)?;
    Ok(Some(
        contig_ids
            .iter()
            .enumerate()
            .filter_map(|(index, name)| {
                chrom_values
                    .contains(name)
                    .then(|| i64::try_from(index).ok())
                    .flatten()
            })
            .collect(),
    ))
}

fn row_selection_from_chunks(
    chunks: &BTreeSet<usize>,
    chunk_size: usize,
    total_rows: usize,
) -> RowSelection {
    let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
    for chunk in chunks {
        let start = chunk.saturating_mul(chunk_size);
        if start >= total_rows {
            continue;
        }
        let end = start.saturating_add(chunk_size).min(total_rows);
        if let Some(last) = ranges.last_mut()
            && last.end == start
        {
            last.end = end;
            continue;
        }
        ranges.push(start..end);
    }
    RowSelection { ranges }
}

fn range_intersects(bounds: &super::planning::NumericBounds, min: i64, max: i64) -> bool {
    if bounds.is_empty() {
        return true;
    }
    bounds.min().is_none_or(|bound| bound <= max) && bounds.max().is_none_or(|bound| bound >= min)
}

fn read_contig_ids(
    metadata: &VcfZarrMetadata,
    codec_options: &zarrs::array::CodecOptions,
) -> Result<Vec<String>> {
    let row_count = {
        let array = metadata.open_array("contig_id")?;
        usize::try_from(*array.shape().first().ok_or_else(|| {
            DataFusionError::Execution("contig_id is not 1-dimensional".to_string())
        })?)
        .map_err(|_| {
            DataFusionError::Execution("contig_id length exceeds platform size".to_string())
        })?
    };
    let selection = RowSelection::all(row_count);
    super::arrays::read_strings_1d_for_pruning(metadata, "contig_id", &selection, codec_options)
}

fn logical_start(position: i64, zero_based: bool) -> Result<i64> {
    if zero_based {
        position.checked_sub(1).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "VCF Zarr variant_position {position} cannot be converted to zero-based"
            ))
        })
    } else {
        Ok(position)
    }
}

fn logical_end(start: i64, length: i64, zero_based: bool) -> Result<i64> {
    if zero_based {
        start
            .checked_add(length)
            .ok_or_else(|| DataFusionError::Execution("VCF Zarr end overflow".to_string()))
    } else {
        start
            .checked_add(length)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| DataFusionError::Execution("VCF Zarr end overflow".to_string()))
    }
}
