use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::Cursor;
use std::sync::Arc;

use async_stream::try_stream;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, RemoteObject, StorageType, get_remote_stream_bgzf_async, get_storage_type,
};
use datafusion_bio_format_core::partition_balancer::PartitionAssignment;
use datafusion_bio_format_core::partition_balancer::RegionSizeEstimate;
use datafusion_bio_format_core::record_filter::evaluate_record_filters;
use datafusion_bio_format_core::table_utils::{OptionalField, builders_to_arrays};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::TryStreamExt;
use log::info;
use noodles_bcf::{self as bcf, Record as BcfRecord};
use noodles_vcf::Header;
use noodles_vcf::header::record::value::map::format::{Number as FormatNumber, Type as FormatType};
use noodles_vcf::variant::Record as VariantRecord;
use noodles_vcf::variant::record::Samples as _;
use noodles_vcf::variant::record::{AlternateBases, Filters, Ids, Info as _, ReferenceBases};

use crate::physical_exec::{
    CoreBatchBuilders, FormatMode, ProjectionFlags,
    adjust_effective_batch_size_by_observed_format_bytes, build_noodles_region,
    build_record_batch_from_builders, choose_effective_batch_size,
    choose_initial_builder_batch_size, init_format_mode, join_into, load_infos_single_pass,
    set_info_builders,
};
use crate::storage::VcfRecordFields;

const SUPPORTED_BCF_VERSION: (u8, u8) = (2, 2);

fn execution_error(context: &str, error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("{context}: {error}"))
}

fn validate_version(version: (u8, u8)) -> Result<()> {
    if version == SUPPORTED_BCF_VERSION {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "unsupported BCF version {}.{}; expected 2.2",
            version.0, version.1
        )))
    }
}

#[derive(Clone, Copy)]
enum BcfEncodedType {
    Null,
    Int8,
    Int16,
    Int32,
    Float,
    String,
}

impl BcfEncodedType {
    fn width(self) -> usize {
        match self {
            Self::Null => 0,
            Self::Int8 | Self::String => 1,
            Self::Int16 => 2,
            Self::Int32 | Self::Float => 4,
        }
    }
}

fn take_bcf_bytes<'a>(src: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if src.len() < len {
        return Err(DataFusionError::Execution(
            "invalid BCF FORMAT encoding: unexpected end of record".into(),
        ));
    }

    let (value, rest) = src.split_at(len);
    *src = rest;
    Ok(value)
}

fn read_bcf_encoded_type(src: &mut &[u8]) -> Result<(BcfEncodedType, usize)> {
    let descriptor = take_bcf_bytes(src, 1)?[0];
    let mut len = usize::from(descriptor >> 4);
    if len == 0x0f {
        let extended_len = read_bcf_typed_integer(src)?;
        len = usize::try_from(extended_len).map_err(|_| {
            DataFusionError::Execution(format!(
                "invalid BCF FORMAT encoding: negative extended length {extended_len}"
            ))
        })?;
    }

    let encoded_type = match descriptor & 0x0f {
        0 => BcfEncodedType::Null,
        1 => BcfEncodedType::Int8,
        2 => BcfEncodedType::Int16,
        3 => BcfEncodedType::Int32,
        5 => BcfEncodedType::Float,
        7 => BcfEncodedType::String,
        code => {
            return Err(DataFusionError::Execution(format!(
                "invalid BCF FORMAT encoding: unsupported type code {code}"
            )));
        }
    };

    Ok((encoded_type, len))
}

fn read_bcf_typed_integer(src: &mut &[u8]) -> Result<i32> {
    let (encoded_type, len) = read_bcf_encoded_type(src)?;
    if len != 1 {
        return Err(DataFusionError::Execution(format!(
            "invalid BCF FORMAT encoding: expected one integer, found {len} values"
        )));
    }

    let value = match encoded_type {
        BcfEncodedType::Int8 => i32::from(take_bcf_bytes(src, 1)?[0] as i8),
        BcfEncodedType::Int16 => i32::from(i16::from_le_bytes(
            take_bcf_bytes(src, 2)?.try_into().unwrap(),
        )),
        BcfEncodedType::Int32 => i32::from_le_bytes(take_bcf_bytes(src, 4)?.try_into().unwrap()),
        BcfEncodedType::Null | BcfEncodedType::Float | BcfEncodedType::String => {
            return Err(DataFusionError::Execution(
                "invalid BCF FORMAT encoding: expected integer value".into(),
            ));
        }
    };

    Ok(value)
}

fn validate_bcf_info_dictionary_references(
    info: &bcf::record::Info<'_>,
    header: &Header,
) -> Result<()> {
    let mut src = info.as_ref();

    for _ in 0..info.len() {
        let key_index = usize::try_from(read_bcf_typed_integer(&mut src)?).map_err(|_| {
            DataFusionError::Execution("invalid BCF INFO string-map index in record".into())
        })?;
        let key = header
            .string_maps()
            .strings()
            .get_index(key_index)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "invalid BCF INFO dictionary index {key_index} in record"
                ))
            })?;
        if !header.infos().contains_key(key) {
            return Err(DataFusionError::Execution(format!(
                "BCF INFO dictionary index {key_index} resolves to '{key}', which has no INFO \
                 header definition"
            )));
        }

        let (encoded_type, value_count) = read_bcf_encoded_type(&mut src)?;
        let payload_len = encoded_type
            .width()
            .checked_mul(value_count)
            .ok_or_else(|| DataFusionError::Execution("BCF INFO payload length overflow".into()))?;
        take_bcf_bytes(&mut src, payload_len)?;
    }

    if !src.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "invalid BCF INFO encoding: {} trailing bytes",
            src.len()
        )));
    }

    Ok(())
}

fn validate_bcf_format_encoding(samples: &bcf::record::Samples<'_>, header: &Header) -> Result<()> {
    let sample_count = samples.len();
    let mut src = samples.as_ref();

    for _ in 0..samples.format_count() {
        let key_index = usize::try_from(read_bcf_typed_integer(&mut src)?).map_err(|_| {
            DataFusionError::Execution("invalid BCF FORMAT dictionary index in record".into())
        })?;
        let key = header
            .string_maps()
            .strings()
            .get_index(key_index)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "invalid BCF FORMAT dictionary index {key_index} in record"
                ))
            })?;
        let format = header.formats().get(key).ok_or_else(|| {
            DataFusionError::Execution(format!("BCF FORMAT field '{key}' has no header definition"))
        })?;
        let (encoded_type, value_count) = read_bcf_encoded_type(&mut src)?;

        let scalar_requires_one_encoded_value = key != "GT"
            && matches!(format.number(), FormatNumber::Count(1))
            && matches!(
                format.ty(),
                FormatType::Integer | FormatType::Float | FormatType::Character
            );
        if scalar_requires_one_encoded_value && value_count != 1 {
            return Err(DataFusionError::Execution(format!(
                "FORMAT field '{key}' is declared scalar but the BCF record encodes \
                 {value_count} values per sample"
            )));
        }

        let payload_len = encoded_type
            .width()
            .checked_mul(value_count)
            .and_then(|len| len.checked_mul(sample_count))
            .ok_or_else(|| {
                DataFusionError::Execution("BCF FORMAT payload length overflow".into())
            })?;
        take_bcf_bytes(&mut src, payload_len)?;
    }

    if !src.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "invalid BCF FORMAT encoding: {} trailing bytes",
            src.len()
        )));
    }

    Ok(())
}

fn local_path(path: &str) -> &str {
    path.strip_prefix("file://").unwrap_or(path)
}

fn read_local_header(path: &str) -> Result<Header> {
    let path = local_path(path);

    let file = File::open(path).map_err(|e| execution_error("failed to open BCF", e))?;
    let mut version_reader = bcf::io::Reader::new(file);
    let mut header_reader = version_reader.header_reader();
    header_reader
        .read_magic_number()
        .map_err(|e| execution_error("invalid BCF magic", e))?;
    let version = header_reader
        .read_format_version()
        .map_err(|e| execution_error("failed to read BCF version", e))?;
    validate_version(version)?;

    let file = File::open(path).map_err(|e| execution_error("failed to reopen BCF", e))?;
    let mut reader = bcf::io::Reader::new(file);
    reader
        .read_header()
        .map_err(|e| execution_error("failed to parse BCF header", e))
}

async fn read_remote_header(
    path: &str,
    object_storage_options: ObjectStorageOptions,
) -> Result<Header> {
    let inner = get_remote_stream_bgzf_async(path.to_string(), object_storage_options.clone())
        .await
        .map_err(|e| execution_error("failed to open remote BCF", e))?;
    let mut version_reader = bcf::r#async::io::Reader::from(inner);
    let mut header_reader = version_reader.header_reader();
    header_reader
        .read_magic_number()
        .await
        .map_err(|e| execution_error("invalid BCF magic", e))?;
    let version = header_reader
        .read_format_version()
        .await
        .map_err(|e| execution_error("failed to read BCF version", e))?;
    validate_version(version)?;

    let inner = get_remote_stream_bgzf_async(path.to_string(), object_storage_options)
        .await
        .map_err(|e| execution_error("failed to reopen remote BCF", e))?;
    let mut reader = bcf::r#async::io::Reader::from(inner);
    reader
        .read_header()
        .await
        .map_err(|e| execution_error("failed to parse remote BCF header", e))
}

pub(crate) async fn read_header(
    path: &str,
    object_storage_options: Option<ObjectStorageOptions>,
) -> Result<Header> {
    match get_storage_type(path.to_string()) {
        StorageType::LOCAL => read_local_header(path),
        _ => read_remote_header(path, object_storage_options.unwrap_or_default()).await,
    }
}

async fn read_csi_index(
    index_path: &str,
    object_storage_options: Option<ObjectStorageOptions>,
) -> Result<noodles_csi::Index> {
    match get_storage_type(index_path.to_string()) {
        StorageType::LOCAL => noodles_csi::fs::read(local_path(index_path))
            .map_err(|error| execution_error("failed to read BCF CSI index", error)),
        _ => {
            let object = RemoteObject::open(
                index_path.to_string(),
                object_storage_options.unwrap_or_default(),
            )
            .await
            .map_err(|error| execution_error("failed to open remote BCF CSI index", error))?;
            let bytes = object.read_all().await.map_err(|error| {
                execution_error("failed to download remote BCF CSI index", error)
            })?;
            let mut reader = noodles_csi::io::Reader::new(Cursor::new(bytes));
            reader
                .read_index()
                .map_err(|error| execution_error("failed to parse remote BCF CSI index", error))
        }
    }
}

/// Returns the exclusive compressed end of the BGZF block starting at `start`
/// using only ranged GETs. This avoids a metadata/HEAD request for signed HTTP
/// URLs while still requesting an exact range for the indexed chunk.
async fn remote_bgzf_block_end(object: &RemoteObject, start: u64) -> Result<u64> {
    const GZIP_FIXED_HEADER_LEN: u64 = 12;
    const GZIP_TRAILER_LEN: u64 = 8;

    let fixed_end = start.checked_add(GZIP_FIXED_HEADER_LEN).ok_or_else(|| {
        DataFusionError::Execution("remote BCF BGZF header offset overflow".into())
    })?;
    let fixed = object.read_range(start..fixed_end).await.map_err(|error| {
        execution_error(
            "failed to read remote BCF BGZF block header; the CSI index does not match the file",
            error,
        )
    })?;
    if fixed.len() != GZIP_FIXED_HEADER_LEN as usize
        || fixed[0..3] != [0x1f, 0x8b, 0x08]
        || fixed[3] & 0x04 == 0
    {
        return Err(DataFusionError::Execution(format!(
            "remote BCF CSI offset {start} does not point to a BGZF block; the index does not \
             match the file"
        )));
    }

    let extra_len = u16::from_le_bytes([fixed[10], fixed[11]]) as u64;
    let extra_end = fixed_end.checked_add(extra_len).ok_or_else(|| {
        DataFusionError::Execution("remote BCF BGZF extra-header offset overflow".into())
    })?;
    let extra = object
        .read_range(fixed_end..extra_end)
        .await
        .map_err(|error| {
            execution_error(
                "failed to read remote BCF BGZF extra header; the CSI index does not match the \
                 file",
                error,
            )
        })?;

    let mut offset = 0usize;
    let mut block_size = None;
    while offset + 4 <= extra.len() {
        let subfield_len = u16::from_le_bytes([extra[offset + 2], extra[offset + 3]]) as usize;
        let payload_end = offset + 4 + subfield_len;
        if payload_end > extra.len() {
            break;
        }
        if &extra[offset..offset + 2] == b"BC" && subfield_len == 2 {
            block_size =
                Some(u16::from_le_bytes([extra[offset + 4], extra[offset + 5]]) as u64 + 1);
            break;
        }
        offset = payload_end;
    }

    let block_size = block_size.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "remote BCF BGZF block at offset {start} has no BC size subfield; the CSI index \
             does not match the file"
        ))
    })?;
    let minimum_size = GZIP_FIXED_HEADER_LEN + extra_len + GZIP_TRAILER_LEN;
    if block_size < minimum_size {
        return Err(DataFusionError::Execution(format!(
            "remote BCF BGZF block at offset {start} reports invalid size {block_size}; the CSI \
             index does not match the file"
        )));
    }
    start.checked_add(block_size).ok_or_else(|| {
        DataFusionError::Execution("remote BCF BGZF block-end offset overflow".into())
    })
}

/// Reads and parses the CSI once at planning time so that scan partitions can
/// share it instead of each re-downloading the full index.
pub(crate) async fn load_csi_index(
    index_path: &str,
    object_storage_options: Option<ObjectStorageOptions>,
) -> Option<Arc<noodles_csi::Index>> {
    match read_csi_index(index_path, object_storage_options).await {
        Ok(index) => Some(Arc::new(index)),
        Err(error) => {
            log::debug!("failed to read BCF CSI index at planning time: {error}");
            None
        }
    }
}

pub(crate) fn estimate_region_sizes(
    index: Option<&noodles_csi::Index>,
    regions: &[datafusion_bio_format_core::genomic_filter::GenomicRegion],
    contig_names: &[String],
    contig_lengths: &[u64],
) -> Vec<RegionSizeEstimate> {
    let Some(index) = index else {
        return regions
            .iter()
            .cloned()
            .map(|region| RegionSizeEstimate {
                region,
                estimated_bytes: 1,
                contig_length: None,
                unmapped_count: 0,
                nonempty_bin_positions: Vec::new(),
                leaf_bin_span: 0,
            })
            .collect();
    };

    let name_to_index: HashMap<&str, usize> = contig_names
        .iter()
        .enumerate()
        .map(|(index, name)| (name.as_str(), index))
        .collect();

    regions
        .iter()
        .cloned()
        .map(|region| {
            let reference_index = name_to_index.get(region.chrom.as_str()).copied();
            let estimated_bytes = reference_index
                .and_then(|index_value| index.reference_sequences().get(index_value))
                .map(|reference| {
                    let mut min_offset = u64::MAX;
                    let mut max_offset = 0;
                    for bin in reference.bins().values() {
                        for chunk in bin.chunks() {
                            min_offset = min_offset.min(chunk.start().compressed());
                            max_offset = max_offset.max(chunk.end().compressed());
                        }
                    }
                    if min_offset == u64::MAX {
                        1
                    } else {
                        max_offset.saturating_sub(min_offset).max(1)
                    }
                })
                .unwrap_or(1);
            let contig_length = reference_index
                .and_then(|index_value| contig_lengths.get(index_value))
                .copied()
                .filter(|length| *length > 0);

            RegionSizeEstimate {
                region,
                estimated_bytes,
                contig_length,
                unmapped_count: 0,
                nonempty_bin_positions: Vec::new(),
                leaf_bin_span: 0,
            }
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
struct RemoteChunkSpan {
    start: noodles_bgzf_vcf::VirtualPosition,
    end: noodles_bgzf_vcf::VirtualPosition,
}

fn plan_remote_chunks(
    index: &noodles_csi::Index,
    header: &Header,
    regions: &[datafusion_bio_format_core::genomic_filter::GenomicRegion],
) -> Result<Vec<RemoteChunkSpan>> {
    use noodles_csi::BinningIndex;

    const MAX_COALESCED_BYTES: u64 = 8 * 1024 * 1024;
    const MAX_COALESCING_GAP: u64 = 64 * 1024;

    let mut chunks = Vec::new();
    for region in regions {
        let query_region = build_noodles_region(region)?;
        // A filter such as `chrom = 'chrMissing'` legitimately produces a region
        // for a contig absent from this file; that matches no rows rather than
        // being an error (the indexed text-VCF path skips these the same way).
        let Some(reference_id) = header
            .string_maps()
            .contigs()
            .get_index_of(region.chrom.as_str())
        else {
            log::debug!(
                "skipping BCF region {}: contig not present in header dictionary",
                region.chrom
            );
            continue;
        };
        chunks.extend(
            index
                .query(reference_id, query_region.interval())
                .map_err(|error| execution_error("failed to query remote BCF CSI index", error))?
                .into_iter()
                .map(|chunk| RemoteChunkSpan {
                    start: chunk.start(),
                    end: chunk.end(),
                }),
        );
    }

    chunks.sort_unstable_by_key(|chunk| chunk.start);
    let mut merged: Vec<RemoteChunkSpan> = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        if let Some(current) = merged.last_mut() {
            let overlaps = chunk.start <= current.end;
            let gap = chunk
                .start
                .compressed()
                .saturating_sub(current.end.compressed());
            let merged_bytes = chunk
                .end
                .compressed()
                .saturating_sub(current.start.compressed());
            if overlaps || (gap <= MAX_COALESCING_GAP && merged_bytes <= MAX_COALESCED_BYTES) {
                current.end = current.end.max(chunk.end);
                continue;
            }
        }
        merged.push(chunk);
    }

    Ok(merged)
}

/// Returns true when the record's start position falls inside one of `regions`.
///
/// Ownership is decided by variant start (not interval overlap) so that a record
/// spanning the boundary between two adjacent partition sub-regions is emitted by
/// exactly one partition. This mirrors the indexed VCF path, which applies the
/// same start-containment check after each region query.
fn record_starts_in_regions(
    record: &BcfRecord,
    header: &Header,
    regions: &[datafusion_bio_format_core::genomic_filter::GenomicRegion],
) -> Result<bool> {
    let chrom = VariantRecord::reference_sequence_name(record, header)
        .map_err(|error| execution_error("invalid BCF contig dictionary index", error))?;
    let Some(start) = record
        .variant_start()
        .transpose()
        .map_err(|error| execution_error("invalid BCF position", error))?
    else {
        return Ok(false);
    };
    let start = start.get() as u64;

    Ok(regions.iter().any(|region| {
        !region.unmapped_tail
            && region.chrom == chrom
            && start >= region.start.unwrap_or(1)
            && start <= region.end.unwrap_or(u64::MAX)
    }))
}

struct BcfBatchDecoder {
    schema: SchemaRef,
    requested_batch_size: usize,
    effective_batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    source_sample_names: Vec<String>,
    flags: ProjectionFlags,
    core_builders: CoreBatchBuilders,
    info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>),
    info_name_to_index: HashMap<String, usize>,
    info_populated: Vec<bool>,
    format_mode: FormatMode,
    has_format_fields: bool,
    batch_row_count: usize,
    join_buf: String,
}

impl BcfBatchDecoder {
    #[allow(clippy::too_many_arguments)]
    fn new(
        header: &Header,
        schema: SchemaRef,
        batch_size: usize,
        info_fields: Option<Vec<String>>,
        format_fields: Option<Vec<String>>,
        sample_names: &[String],
        source_sample_names: Vec<String>,
        projection: Option<Vec<usize>>,
        coordinate_system_zero_based: bool,
        residual_filters: Vec<Expr>,
    ) -> Result<Self> {
        // First pass exists only to learn the INFO field count for
        // `ProjectionFlags`; the builders are rebuilt below once the
        // batch-size heuristics have settled on `initial_builder_batch_size`.
        let mut info_builders = (Vec::new(), Vec::new(), Vec::new());
        set_info_builders(
            batch_size,
            info_fields.clone(),
            header.infos(),
            &mut info_builders,
        );
        let flags = ProjectionFlags::new(&projection, info_builders.0.len());
        let effective_batch_size = choose_effective_batch_size(
            batch_size,
            flags.any_format,
            &format_fields,
            sample_names,
            &source_sample_names,
            header.formats(),
        );
        let initial_builder_batch_size = choose_initial_builder_batch_size(
            effective_batch_size,
            flags.any_format,
            &source_sample_names,
        );

        info_builders = (Vec::new(), Vec::new(), Vec::new());
        set_info_builders(
            initial_builder_batch_size,
            info_fields,
            header.infos(),
            &mut info_builders,
        );
        let info_name_to_index = info_builders
            .0
            .iter()
            .enumerate()
            .map(|(index, name)| (name.clone(), index))
            .collect();
        let info_populated = vec![false; info_builders.0.len()];

        let format_mode = init_format_mode(
            initial_builder_batch_size,
            format_fields,
            sample_names,
            &source_sample_names,
            header.formats(),
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let has_format_fields = format_mode.has_fields();
        let core_builders = CoreBatchBuilders::new(&flags, initial_builder_batch_size);

        Ok(Self {
            schema,
            requested_batch_size: batch_size,
            effective_batch_size,
            projection,
            coordinate_system_zero_based,
            residual_filters,
            source_sample_names,
            flags,
            core_builders,
            info_builders,
            info_name_to_index,
            info_populated,
            format_mode,
            has_format_fields,
            batch_row_count: 0,
            join_buf: String::with_capacity(64),
        })
    }

    fn append_record(
        &mut self,
        record: &BcfRecord,
        header: &Header,
    ) -> Result<Option<RecordBatch>> {
        let samples = record
            .samples()
            .map_err(|e| execution_error("invalid BCF sample count", e))?;
        let record_sample_count = samples.len();
        if record_sample_count != self.source_sample_names.len() {
            return Err(DataFusionError::Execution(format!(
                "BCF record sample count {record_sample_count} does not match header sample count {}",
                self.source_sample_names.len()
            )));
        }
        let reference_sequence_name = VariantRecord::reference_sequence_name(record, header)
            .map_err(|e| execution_error("invalid BCF contig dictionary index in record", e))?;
        for result in record.filters().iter(header) {
            let filter = result
                .map_err(|e| execution_error("invalid BCF FILTER dictionary index in record", e))?;
            if filter != "PASS" && !header.filters().contains_key(filter) {
                return Err(DataFusionError::Execution(format!(
                    "BCF FILTER dictionary entry '{filter}' has no FILTER header definition"
                )));
            }
        }
        validate_bcf_info_dictionary_references(&record.info(), header)?;
        validate_bcf_format_encoding(&samples, header)?;

        let has_filters = !self.residual_filters.is_empty();
        let needs_start = self.flags.start || has_filters;
        let needs_end = self.flags.end || has_filters;
        let needs_chrom = self.flags.chrom || has_filters;

        let start = if needs_start {
            let position = record
                .variant_start()
                .transpose()
                .map_err(|e| execution_error("invalid BCF position", e))?
                .ok_or_else(|| DataFusionError::Execution("BCF record has no position".into()))?;
            let position = u32::try_from(position.get()).map_err(|_| {
                DataFusionError::Execution("BCF position exceeds UInt32 range".into())
            })?;
            Some(if self.coordinate_system_zero_based {
                position - 1
            } else {
                position
            })
        } else {
            None
        };

        let chrom = if needs_chrom {
            Some(reference_sequence_name.to_string())
        } else {
            None
        };

        let end = if needs_end {
            let position = record
                .variant_end(header)
                .map_err(|e| execution_error("invalid BCF variant span", e))?;
            Some(u32::try_from(position.get()).map_err(|_| {
                DataFusionError::Execution("BCF end position exceeds UInt32 range".into())
            })?)
        } else {
            None
        };

        if has_filters {
            let fields = VcfRecordFields {
                chrom: chrom.clone(),
                start,
                end,
            };
            if !evaluate_record_filters(&fields, &self.residual_filters) {
                return Ok(None);
            }
        }

        if self.flags.chrom {
            self.core_builders
                .append_chrom(chrom.as_deref().expect("chrom was requested"));
        }
        if self.flags.start {
            self.core_builders
                .append_start(start.expect("start was requested"));
        }
        if self.flags.end {
            self.core_builders
                .append_end(end.expect("end was requested"));
        }
        if self.flags.id {
            join_into(&mut self.join_buf, record.ids().iter(), ';');
            self.core_builders.append_id(&self.join_buf);
        }
        if self.flags.reference {
            self.join_buf.clear();
            for result in record.reference_bases().iter() {
                self.join_buf.push(char::from(
                    result.map_err(|e| execution_error("invalid BCF reference allele", e))?,
                ));
            }
            self.core_builders.append_ref(&self.join_buf);
        }
        if self.flags.alt {
            self.join_buf.clear();
            let mut first = true;
            for result in record.alternate_bases().iter() {
                if !first {
                    self.join_buf.push('|');
                }
                first = false;
                self.join_buf.push_str(
                    result.map_err(|e| execution_error("invalid BCF alternate allele", e))?,
                );
            }
            self.core_builders.append_alt(&self.join_buf);
        }
        if self.flags.qual {
            let qual = VariantRecord::quality_score(record)
                .transpose()
                .map_err(|e| execution_error("invalid BCF quality score", e))?
                .map(f64::from);
            self.core_builders.append_qual(qual);
        }
        if self.flags.filter {
            self.join_buf.clear();
            let mut first = true;
            for result in record.filters().iter(header) {
                if !first {
                    self.join_buf.push(';');
                }
                first = false;
                self.join_buf.push_str(
                    result
                        .map_err(|e| execution_error("invalid BCF filter dictionary index", e))?,
                );
            }
            self.core_builders.append_filter(&self.join_buf);
        }
        if self.flags.any_info {
            load_infos_single_pass(
                record,
                header,
                &self.info_builders.1,
                &mut self.info_builders.2,
                &self.info_name_to_index,
                &mut self.info_populated,
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        }
        if self.has_format_fields && self.flags.any_format {
            self.format_mode
                .append_record(record, header)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        }

        self.batch_row_count += 1;
        if self.batch_row_count == self.effective_batch_size {
            self.finish_batch().map(Some)
        } else {
            Ok(None)
        }
    }

    fn finish_batch(&mut self) -> Result<RecordBatch> {
        let info_arrays = if self.flags.any_info {
            Some(builders_to_arrays(&mut self.info_builders.2))
        } else {
            None
        };
        let format_arrays = if self.has_format_fields && self.flags.any_format {
            Some(
                self.format_mode
                    .finish_arrays()
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            )
        } else {
            None
        };

        self.effective_batch_size = adjust_effective_batch_size_by_observed_format_bytes(
            self.requested_batch_size,
            self.effective_batch_size,
            self.flags.any_format,
            &self.source_sample_names,
            self.batch_row_count,
            format_arrays.as_ref(),
        );
        let row_count = self.batch_row_count;
        self.batch_row_count = 0;

        build_record_batch_from_builders(
            self.schema.clone(),
            self.core_builders.finish(),
            info_arrays.as_ref(),
            format_arrays.as_ref(),
            self.info_builders.0.len(),
            &self.projection,
            row_count,
        )
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.batch_row_count == 0 {
            Ok(None)
        } else {
            self.finish_batch().map(Some)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn full_local_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    limit: Option<usize>,
) -> SendableRecordBatchStream {
    let output_schema = schema.clone();
    let stream = try_stream! {
        let file = File::open(local_path(&file_path))
            .map_err(|e| execution_error("failed to open BCF", e))?;
        let mut reader = bcf::io::Reader::new(file);
        let header = reader
            .read_header()
            .map_err(|e| execution_error("failed to parse BCF header", e))?;
        let mut decoder = BcfBatchDecoder::new(
            &header,
            schema,
            batch_size,
            info_fields,
            format_fields,
            &sample_names,
            source_sample_names,
            projection,
            coordinate_system_zero_based,
            residual_filters,
        )?;
        let mut record = BcfRecord::default();
        let mut emitted = 0usize;

        loop {
            let record_size = reader
                .read_record(&mut record)
                .map_err(|e| execution_error("failed to decode BCF record", e))?;
            if record_size == 0 {
                break;
            }

            if let Some(batch) = decoder.append_record(&record, &header)? {
                emitted += batch.num_rows();
                yield batch;
            }

            let accepted = emitted + decoder.batch_row_count;
            if limit.is_some_and(|value| accepted >= value) {
                break;
            }
        }

        if let Some(batch) = decoder.finish()? {
            yield batch;
        }
    };

    Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
}

#[allow(clippy::too_many_arguments)]
async fn full_remote_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    limit: Option<usize>,
) -> Result<SendableRecordBatchStream> {
    let output_schema = schema.clone();
    let inner = get_remote_stream_bgzf_async(file_path, object_storage_options.unwrap_or_default())
        .await
        .map_err(|e| execution_error("failed to open remote BCF", e))?;
    let mut reader = bcf::r#async::io::Reader::from(inner);
    let header = reader
        .read_header()
        .await
        .map_err(|e| execution_error("failed to parse remote BCF header", e))?;

    let stream = try_stream! {
        let mut decoder = BcfBatchDecoder::new(
            &header,
            schema,
            batch_size,
            info_fields,
            format_fields,
            &sample_names,
            source_sample_names,
            projection,
            coordinate_system_zero_based,
            residual_filters,
        )?;
        let mut record = BcfRecord::default();
        let mut emitted = 0usize;

        loop {
            let record_size = reader
                .read_record(&mut record)
                .await
                .map_err(|e| execution_error("failed to decode remote BCF record", e))?;
            if record_size == 0 {
                break;
            }

            if let Some(batch) = decoder.append_record(&record, &header)? {
                emitted += batch.num_rows();
                yield batch;
            }

            let accepted = emitted + decoder.batch_row_count;
            if limit.is_some_and(|value| accepted >= value) {
                break;
            }
        }

        if let Some(batch) = decoder.finish()? {
            yield batch;
        }
    };

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        stream,
    )))
}

#[allow(clippy::too_many_arguments)]
fn indexed_local_stream(
    file_path: String,
    index_path: String,
    shared_index: Option<Arc<noodles_csi::Index>>,
    regions: Vec<datafusion_bio_format_core::genomic_filter::GenomicRegion>,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    limit: Option<usize>,
) -> SendableRecordBatchStream {
    let output_schema = schema.clone();
    let stream = try_stream! {
        let index = match shared_index {
            Some(index) => index,
            None => Arc::new(
                noodles_csi::fs::read(local_path(&index_path))
                    .map_err(|e| execution_error("failed to read BCF CSI index", e))?,
            ),
        };
        let file = File::open(local_path(&file_path))
            .map_err(|e| execution_error("failed to open indexed BCF", e))?;
        let mut reader = bcf::io::Reader::new(file);
        let header = reader
            .read_header()
            .map_err(|e| execution_error("failed to parse indexed BCF header", e))?;
        let mut decoder = BcfBatchDecoder::new(
            &header,
            schema,
            batch_size,
            info_fields,
            format_fields,
            &sample_names,
            source_sample_names,
            projection,
            coordinate_system_zero_based,
            residual_filters,
        )?;
        let mut emitted = 0usize;

        'regions: for region in regions {
            // A filter on a contig absent from this file matches no rows; skip it
            // instead of letting the query fail (the indexed text-VCF path skips
            // unknown contigs the same way).
            if header
                .string_maps()
                .contigs()
                .get_index_of(region.chrom.as_str())
                .is_none()
            {
                log::debug!(
                    "skipping BCF region {}: contig not present in header dictionary",
                    region.chrom
                );
                continue;
            }
            let noodles_region = build_noodles_region(&region)?;
            let query = reader
                .query(&header, index.as_ref(), &noodles_region)
                .map_err(|e| execution_error("failed to query BCF CSI index", e))?;

            for result in query {
                let record =
                    result.map_err(|e| execution_error("failed to decode indexed BCF record", e))?;
                // The CSI query matches by interval overlap; keep only records that
                // start inside this partition's sub-region so records spanning a
                // partition boundary are not emitted twice.
                if !record_starts_in_regions(&record, &header, std::slice::from_ref(&region))? {
                    continue;
                }
                if let Some(batch) = decoder.append_record(&record, &header)? {
                    emitted += batch.num_rows();
                    yield batch;
                }

                let accepted = emitted + decoder.batch_row_count;
                if limit.is_some_and(|value| accepted >= value) {
                    break 'regions;
                }
            }
        }

        if let Some(batch) = decoder.finish()? {
            yield batch;
        }
    };

    Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
}

#[allow(clippy::too_many_arguments)]
async fn indexed_remote_stream(
    file_path: String,
    index_path: String,
    shared_index: Option<Arc<noodles_csi::Index>>,
    shared_header: Option<Arc<Header>>,
    regions: Vec<datafusion_bio_format_core::genomic_filter::GenomicRegion>,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    limit: Option<usize>,
) -> Result<SendableRecordBatchStream> {
    let options = object_storage_options.unwrap_or_default();
    // Reuse the header parsed at provider construction; re-reading it here
    // would download and parse it once per partition.
    let header = match shared_header {
        Some(header) => header,
        None => Arc::new(read_header(&file_path, Some(options.clone())).await?),
    };
    // Reuse the CSI parsed at planning time; each partition re-downloading the
    // full index would transfer it N+1 times for an N-partition scan.
    let index = match shared_index {
        Some(index) => index,
        None => Arc::new(read_csi_index(&index_path, Some(options.clone())).await?),
    };
    let chunks = plan_remote_chunks(index.as_ref(), &header, &regions)?;
    let object = RemoteObject::open(file_path, options)
        .await
        .map_err(|error| execution_error("failed to open indexed remote BCF", error))?;
    let output_schema = schema.clone();

    let stream = try_stream! {
        let mut decoder = BcfBatchDecoder::new(
            &header,
            schema,
            batch_size,
            info_fields,
            format_fields,
            &sample_names,
            source_sample_names,
            projection,
            coordinate_system_zero_based,
            residual_filters,
        )?;
        let mut emitted = 0usize;

        'chunks: for chunk in chunks {
            let compressed_start = chunk.start.compressed();
            // Resolve the exact end of a partial BGZF block with range GETs.
            // Do not stat/HEAD the object first: signed HTTP URLs are often
            // authorized for GET (including ranges) only.
            let compressed_end = if chunk.end.uncompressed() == 0 {
                chunk.end.compressed()
            } else {
                remote_bgzf_block_end(&object, chunk.end.compressed()).await?
            };
            if compressed_end <= compressed_start {
                continue;
            }

            let bytes = object
                .read_range(compressed_start..compressed_end)
                .await
                .map_err(|error| {
                    let context = if error.kind() == opendal::ErrorKind::Unexpected {
                        "failed to read the complete remote BCF CSI range; the index does not \
                         match the file"
                    } else {
                        "failed to read remote BCF CSI range"
                    };
                    execution_error(context, error)
                })?;
            let expected_len = compressed_end - compressed_start;
            let actual_len = u64::try_from(bytes.len()).map_err(|_| {
                DataFusionError::Execution(
                    "remote BCF CSI range length does not fit in u64".into(),
                )
            })?;
            if actual_len != expected_len {
                Err(DataFusionError::Execution(format!(
                    "remote BCF CSI range returned {actual_len} bytes, expected {expected_len}; \
                     the index does not match the file"
                )))?;
            }
            let inner = noodles_bgzf_vcf::io::Reader::new(Cursor::new(bytes));
            let mut reader = bcf::io::Reader::from(inner);
            let local_start =
                noodles_bgzf_vcf::VirtualPosition::new(0, chunk.start.uncompressed())
                    .expect("zero compressed offset is valid");
            let local_end = noodles_bgzf_vcf::VirtualPosition::new(
                chunk.end.compressed().saturating_sub(compressed_start),
                chunk.end.uncompressed(),
            )
            .ok_or_else(|| {
                DataFusionError::Execution("remote BCF CSI virtual offset overflow".into())
            })?;
            reader
                .get_mut()
                .seek(local_start)
                .map_err(|error| execution_error("failed to seek remote BCF CSI range", error))?;
            let mut record = BcfRecord::default();

            while reader.get_ref().virtual_position() < local_end {
                let record_size = reader
                    .read_record(&mut record)
                    .map_err(|error| {
                        execution_error("failed to decode remote indexed BCF record", error)
                    })?;
                if record_size == 0 {
                    break;
                }
                if !record_starts_in_regions(&record, &header, &regions)? {
                    continue;
                }
                if let Some(batch) = decoder.append_record(&record, &header)? {
                    emitted += batch.num_rows();
                    yield batch;
                }
                let accepted = emitted + decoder.batch_row_count;
                if limit.is_some_and(|value| accepted >= value) {
                    break 'chunks;
                }
            }
        }

        if let Some(batch) = decoder.finish()? {
            yield batch;
        }
    };

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        stream,
    )))
}

pub(crate) struct BcfExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) info_fields: Option<Vec<String>>,
    pub(crate) format_fields: Option<Vec<String>>,
    pub(crate) sample_names: Vec<String>,
    pub(crate) source_sample_names: Vec<String>,
    pub(crate) cache: Arc<PlanProperties>,
    pub(crate) limit: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    pub(crate) coordinate_system_zero_based: bool,
    pub(crate) partition_assignments: Option<Vec<PartitionAssignment>>,
    pub(crate) index_path: Option<String>,
    /// CSI parsed once at planning time and shared by all scan partitions.
    pub(crate) index: Option<Arc<noodles_csi::Index>>,
    /// Header parsed once at provider construction and shared by all scan
    /// partitions (avoids re-downloading it per partition on remote scans).
    pub(crate) header: Option<Arc<Header>>,
    pub(crate) residual_filters: Vec<Expr>,
}

impl Debug for BcfExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BcfExec")
            .field("file_path", &self.file_path)
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for BcfExec {
    fn fmt_as(&self, _display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "BcfExec: projection={:?}", self.projection)
    }
}

impl ExecutionPlan for BcfExec {
    fn name(&self) -> &str {
        "BcfExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        info!(
            "BCF scan partition={partition}, projection={:?}",
            self.projection
        );
        let batch_size = context.session_config().batch_size();

        if let (Some(assignments), Some(index_path)) =
            (&self.partition_assignments, &self.index_path)
            && let Some(assignment) = assignments.get(partition)
        {
            if matches!(get_storage_type(self.file_path.clone()), StorageType::LOCAL) {
                return Ok(indexed_local_stream(
                    self.file_path.clone(),
                    index_path.clone(),
                    self.index.clone(),
                    assignment.regions.clone(),
                    self.schema.clone(),
                    batch_size,
                    self.info_fields.clone(),
                    self.format_fields.clone(),
                    self.sample_names.clone(),
                    self.source_sample_names.clone(),
                    self.projection.clone(),
                    self.coordinate_system_zero_based,
                    self.residual_filters.clone(),
                    self.limit,
                ));
            }

            let future = indexed_remote_stream(
                self.file_path.clone(),
                index_path.clone(),
                self.index.clone(),
                self.header.clone(),
                assignment.regions.clone(),
                self.schema.clone(),
                batch_size,
                self.info_fields.clone(),
                self.format_fields.clone(),
                self.sample_names.clone(),
                self.source_sample_names.clone(),
                self.projection.clone(),
                self.object_storage_options.clone(),
                self.coordinate_system_zero_based,
                self.residual_filters.clone(),
                self.limit,
            );
            let stream = futures::stream::once(future).try_flatten();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream,
            )));
        }

        match get_storage_type(self.file_path.clone()) {
            StorageType::LOCAL => Ok(full_local_stream(
                self.file_path.clone(),
                self.schema.clone(),
                batch_size,
                self.info_fields.clone(),
                self.format_fields.clone(),
                self.sample_names.clone(),
                self.source_sample_names.clone(),
                self.projection.clone(),
                self.coordinate_system_zero_based,
                self.residual_filters.clone(),
                self.limit,
            )),
            _ => {
                let future = full_remote_stream(
                    self.file_path.clone(),
                    self.schema.clone(),
                    batch_size,
                    self.info_fields.clone(),
                    self.format_fields.clone(),
                    self.sample_names.clone(),
                    self.source_sample_names.clone(),
                    self.projection.clone(),
                    self.object_storage_options.clone(),
                    self.coordinate_system_zero_based,
                    self.residual_filters.clone(),
                    self.limit,
                );
                let stream = futures::stream::once(future).try_flatten();
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    self.schema.clone(),
                    stream,
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_bio_format_core::genomic_filter::GenomicRegion;
    use noodles_vcf as vcf;
    use noodles_vcf::variant::io::Write as _;

    #[test]
    fn validates_only_bcf_2_2() {
        assert!(validate_version((2, 2)).is_ok());
        assert!(validate_version((2, 1)).is_err());
        assert!(validate_version((3, 0)).is_err());
    }

    fn region(start: Option<u64>, end: Option<u64>) -> GenomicRegion {
        GenomicRegion {
            chrom: "chr1".to_string(),
            start,
            end,
            unmapped_tail: false,
        }
    }

    #[test]
    fn spanning_record_is_owned_by_exactly_one_sub_region() {
        // One record at chr1:400 with a 201 bp REF, i.e. spanning [400, 600].
        let long_ref = "A".repeat(201);
        let vcf_text = format!(
            "##fileformat=VCFv4.3\n##contig=<ID=chr1,length=10000>\n\
             #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
             chr1\t400\t.\t{long_ref}\tA\t.\tPASS\t.\n"
        );
        let mut vcf_reader = vcf::io::Reader::new(vcf_text.as_bytes());
        let vcf_header = vcf_reader.read_header().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("span.bcf");
        let mut writer = bcf::io::Writer::new(File::create(&path).unwrap());
        writer.write_header(&vcf_header).unwrap();
        for result in vcf_reader.records() {
            writer
                .write_variant_record(&vcf_header, &result.unwrap())
                .unwrap();
        }
        writer.try_finish().unwrap();

        let mut reader = bcf::io::Reader::new(File::open(&path).unwrap());
        let header = reader.read_header().unwrap();
        let mut record = BcfRecord::default();
        assert!(reader.read_record(&mut record).unwrap() > 0);

        // Splitting chr1 at 500 assigns the record to the first sub-region only,
        // even though its interval overlaps both.
        assert!(record_starts_in_regions(&record, &header, &[region(Some(1), Some(500))]).unwrap());
        assert!(
            !record_starts_in_regions(&record, &header, &[region(Some(501), Some(1000))]).unwrap()
        );
        // Overlap without start containment does not confer ownership.
        assert!(
            !record_starts_in_regions(&record, &header, &[region(Some(450), Some(460))]).unwrap()
        );
        // Open-ended bounds contain every start on the contig.
        assert!(record_starts_in_regions(&record, &header, &[region(None, None)]).unwrap());
        // Boundary inclusivity: 1-based start 400 is inside [400, 400].
        assert!(
            record_starts_in_regions(&record, &header, &[region(Some(400), Some(400))]).unwrap()
        );
    }
}
