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
use noodles_vcf::variant::Record as VariantRecord;
use noodles_vcf::variant::record::{AlternateBases, Filters, Ids, ReferenceBases};

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

pub(crate) async fn estimate_region_sizes(
    index_path: &str,
    regions: &[datafusion_bio_format_core::genomic_filter::GenomicRegion],
    contig_names: &[String],
    contig_lengths: &[u64],
    object_storage_options: Option<ObjectStorageOptions>,
) -> Vec<RegionSizeEstimate> {
    let index = match read_csi_index(index_path, object_storage_options).await {
        Ok(index) => index,
        Err(error) => {
            log::debug!("failed to read BCF CSI for size estimation: {error}");
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
        }
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
        let reference_id = header
            .string_maps()
            .contigs()
            .get_index_of(region.chrom.as_str())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "BCF CSI region references an unknown contig: {}",
                    region.chrom
                ))
            })?;
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
            Some(
                VariantRecord::reference_sequence_name(record, header)
                    .map_err(|e| execution_error("invalid BCF contig dictionary index", e))?
                    .to_string(),
            )
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
        let index = noodles_csi::fs::read(local_path(&index_path))
            .map_err(|e| execution_error("failed to read BCF CSI index", e))?;
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
            let noodles_region = build_noodles_region(&region)?;
            let query = reader
                .query(&header, &index, &noodles_region)
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
    const BGZF_MAX_COMPRESSED_BLOCK_SIZE: u64 = 64 * 1024;

    let options = object_storage_options.unwrap_or_default();
    let header = read_header(&file_path, Some(options.clone())).await?;
    let index = read_csi_index(&index_path, Some(options.clone())).await?;
    let chunks = plan_remote_chunks(&index, &header, &regions)?;
    let object = RemoteObject::open(file_path, options)
        .await
        .map_err(|error| execution_error("failed to open indexed remote BCF", error))?;
    let object_size = object
        .size()
        .await
        .map_err(|error| execution_error("failed to stat indexed remote BCF", error))?;
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
            let compressed_end = if chunk.end.uncompressed() == 0 {
                chunk.end.compressed()
            } else {
                chunk
                    .end
                    .compressed()
                    .saturating_add(BGZF_MAX_COMPRESSED_BLOCK_SIZE)
            }
            .min(object_size);
            if compressed_end <= compressed_start {
                continue;
            }

            let bytes = object
                .read_range(compressed_start..compressed_end)
                .await
                .map_err(|error| execution_error("failed to read remote BCF CSI range", error))?;
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
