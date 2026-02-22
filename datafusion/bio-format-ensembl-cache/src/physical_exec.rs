use crate::entity::EnsemblEntityKind;
use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::regulatory::{
    RegulatoryColumnIndices, RegulatoryTarget, parse_regulatory_line_into,
    parse_regulatory_storable_file,
};
use crate::row::Row;
use crate::transcript::{
    TranscriptColumnIndices, parse_transcript_line_into, parse_transcript_storable_file,
};
use crate::util::{
    BatchBuilder, ColumnMap, ProvenanceWriter, is_storable_binary_payload, open_text_reader,
    rows_to_record_batch,
};
use crate::variation::{
    SourceIdWriter, VariationColumnIndices, VariationContext, parse_variation_line_into,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_execution::TaskContext;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub(crate) struct EnsemblCacheExec {
    pub(crate) kind: EnsemblEntityKind,
    pub(crate) cache_info: CacheInfo,
    pub(crate) partition_files: Vec<Vec<PathBuf>>,
    pub(crate) schema: SchemaRef,
    pub(crate) predicate: SimplePredicate,
    pub(crate) limit: Option<usize>,
    pub(crate) variation_region_size: Option<i64>,
    pub(crate) batch_size_hint: Option<usize>,
    pub(crate) coordinate_system_zero_based: bool,
    pub(crate) num_partitions: usize,
    pub(crate) cache: PlanProperties,
}

pub(crate) struct EnsemblCacheExecConfig {
    pub(crate) kind: EnsemblEntityKind,
    pub(crate) cache_info: CacheInfo,
    pub(crate) files: Vec<PathBuf>,
    pub(crate) schema: SchemaRef,
    pub(crate) predicate: SimplePredicate,
    pub(crate) limit: Option<usize>,
    pub(crate) variation_region_size: Option<i64>,
    pub(crate) batch_size_hint: Option<usize>,
    pub(crate) coordinate_system_zero_based: bool,
    pub(crate) num_partitions: usize,
}

impl Debug for EnsemblCacheExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let total_files: usize = self.partition_files.iter().map(|p| p.len()).sum();
        f.debug_struct("EnsemblCacheExec")
            .field("kind", &self.kind)
            .field("total_files", &total_files)
            .field("num_partitions", &self.num_partitions)
            .field("limit", &self.limit)
            .finish()
    }
}

impl DisplayAs for EnsemblCacheExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        let total_files: usize = self.partition_files.iter().map(|p| p.len()).sum();
        write!(
            f,
            "EnsemblCacheExec(kind={:?}, files={}, partitions={})",
            self.kind, total_files, self.num_partitions
        )
    }
}

/// Estimate file size for partition balancing. Falls back to 0 on error.
fn estimate_file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Sort files by descending size, then assign round-robin across partitions.
/// This spreads large files across different partitions while keeping file
/// counts even (±1), avoiding the greedy LPT pitfall where a few heavy
/// partitions dominate wall-clock time.
fn assign_files_balanced(files: Vec<PathBuf>, num_partitions: usize) -> Vec<Vec<PathBuf>> {
    let mut partition_files: Vec<Vec<PathBuf>> = (0..num_partitions).map(|_| Vec::new()).collect();
    if files.is_empty() {
        return partition_files;
    }

    // Sort descending by size so large files land in different partitions
    let mut sized: Vec<(u64, PathBuf)> = files
        .into_iter()
        .map(|p| {
            let size = estimate_file_size(&p);
            (size, p)
        })
        .collect();
    sized.sort_unstable_by(|a, b| b.0.cmp(&a.0));

    // Round-robin assignment after sorting
    for (i, (_, path)) in sized.into_iter().enumerate() {
        partition_files[i % num_partitions].push(path);
    }

    partition_files
}

impl EnsemblCacheExec {
    pub(crate) fn new(config: EnsemblCacheExecConfig) -> Self {
        let num_partitions = config.num_partitions.max(1);
        let cache = PlanProperties::new(
            EquivalenceProperties::new(config.schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        let partition_files = assign_files_balanced(config.files, num_partitions);

        Self {
            kind: config.kind,
            cache_info: config.cache_info,
            partition_files,
            schema: config.schema,
            predicate: config.predicate,
            limit: config.limit,
            variation_region_size: config.variation_region_size,
            batch_size_hint: config.batch_size_hint,
            coordinate_system_zero_based: config.coordinate_system_zero_based,
            num_partitions,
            cache,
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 8: File-level predicate pruning for variation files
// ---------------------------------------------------------------------------

fn file_matches_predicate(path: &Path, predicate: &SimplePredicate) -> bool {
    // Variation files are named {chrom}_{start}-{end}_var.gz (e.g. 1_1-1000000_var.gz).
    // We can prune files whose chrom/region can't match the predicate.
    let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
        return true; // can't parse, don't prune
    };

    let Some((file_chrom, file_start, file_end)) = parse_file_chrom_region(file_name) else {
        return true; // can't parse, don't prune
    };

    // Check chromosome
    if let Some(pred_chrom) = &predicate.chrom {
        if pred_chrom != file_chrom {
            return false;
        }
    }

    // Check region range overlap
    if let Some(start_min) = predicate.start_min {
        if file_end < start_min {
            return false;
        }
    }
    if let Some(end_max) = predicate.end_max {
        if file_start > end_max {
            return false;
        }
    }

    true
}

/// Parses `{chrom}_{start}-{end}_var.gz` → (chrom, start, end)
fn parse_file_chrom_region(name: &str) -> Option<(&str, i64, i64)> {
    let marker = name.find('_')?;
    let chrom = &name[..marker];
    let suffix = &name[marker + 1..];
    let (start_raw, rest) = suffix.split_once('-')?;
    let end_raw: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if end_raw.is_empty() {
        return None;
    }

    let start = start_raw.parse::<i64>().ok()?;
    let end = end_raw.parse::<i64>().ok()?;
    Some((chrom, start, end))
}

impl ExecutionPlan for EnsemblCacheExec {
    fn name(&self) -> &str {
        "EnsemblCacheExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
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
        if partition >= self.num_partitions {
            return Err(exec_err(format!(
                "EnsemblCacheExec has {} partitions, requested {partition}",
                self.num_partitions
            )));
        }

        let schema = self.schema.clone();
        let stream_schema = schema.clone();
        let kind = self.kind;
        let cache_info = self.cache_info.clone();
        let predicate = self.predicate.clone();

        // Files for this partition (pre-balanced by size), with predicate pruning
        let files: Vec<PathBuf> = self.partition_files[partition]
            .iter()
            .filter(|path| {
                if kind == EnsemblEntityKind::Variation {
                    file_matches_predicate(path, &predicate)
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        let limit = self.limit;
        let variation_region_size = self.variation_region_size.unwrap_or(1_000_000);
        let coordinate_system_zero_based = self.coordinate_system_zero_based;
        let batch_size = self
            .batch_size_hint
            .unwrap_or_else(|| context.session_config().batch_size());

        let mut builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 2);
        let tx = builder.tx();
        builder.spawn_blocking(move || {
            process_partition(
                tx,
                stream_schema,
                kind,
                cache_info,
                predicate,
                files,
                limit,
                variation_region_size,
                coordinate_system_zero_based,
                batch_size,
            )
        });
        Ok(builder.build())
    }
}

/// Runs the file-read+parse loop on a blocking thread pool thread.
/// Sends completed `RecordBatch`es through `tx`. Returns `Ok(())` on
/// completion or early exit (consumer dropped / LIMIT reached).
#[allow(clippy::too_many_arguments)]
fn process_partition(
    tx: Sender<DFResult<RecordBatch>>,
    stream_schema: SchemaRef,
    kind: EnsemblEntityKind,
    cache_info: CacheInfo,
    predicate: SimplePredicate,
    files: Vec<PathBuf>,
    limit: Option<usize>,
    variation_region_size: i64,
    coordinate_system_zero_based: bool,
    batch_size: usize,
) -> Result<()> {
    let col_map = ColumnMap::from_schema(&stream_schema);
    let provenance = ProvenanceWriter::new(&col_map, &cache_info);
    let variation_ctx = if kind == EnsemblEntityKind::Variation {
        Some(VariationContext::new(&cache_info))
    } else {
        None
    };
    let variation_col_idx = if kind == EnsemblEntityKind::Variation {
        Some(VariationColumnIndices::new(
            &col_map,
            variation_ctx.as_ref().unwrap(),
        ))
    } else {
        None
    };
    let mut source_id_writer = if kind == EnsemblEntityKind::Variation {
        let ctx = variation_ctx.as_ref().unwrap();
        Some(SourceIdWriter::new(&col_map, ctx.source_to_id_column()))
    } else {
        None
    };
    let transcript_col_idx = if kind == EnsemblEntityKind::Transcript {
        Some(TranscriptColumnIndices::new(&col_map))
    } else {
        None
    };
    let regulatory_col_idx = if kind == EnsemblEntityKind::RegulatoryFeature
        || kind == EnsemblEntityKind::MotifFeature
    {
        Some(RegulatoryColumnIndices::new(&col_map))
    } else {
        None
    };

    let mut batch_builder = BatchBuilder::new(stream_schema.clone(), batch_size.max(1))?;
    let mut buffered_rows: Vec<Row> = Vec::new();
    let mut emitted_rows: usize = 0;
    let mut stop = false;

    for source_file in &files {
        if stop {
            break;
        }

        // Pre-compute source file path string once per file
        let source_file_str = source_file.to_str().unwrap_or_default();
        let source_file_str: &str = if source_file_str.is_empty() {
            &source_file.to_string_lossy()
        } else {
            source_file_str
        };

        let use_native_storable = (kind == EnsemblEntityKind::Transcript
            || kind == EnsemblEntityKind::RegulatoryFeature
            || kind == EnsemblEntityKind::MotifFeature)
            && cache_info.serializer_type.as_deref() == Some("storable")
            && is_storable_binary_payload(source_file)?;

        if use_native_storable {
            // Storable binary path: still uses Vec<Row> + rows_to_record_batch
            let parsed_rows = match kind {
                EnsemblEntityKind::Transcript => {
                    let col_idx = transcript_col_idx.as_ref().unwrap();
                    parse_transcript_storable_file(
                        source_file,
                        &cache_info,
                        &predicate,
                        coordinate_system_zero_based,
                        col_idx.exons_projected(),
                        col_idx.sequences_projected(),
                    )?
                }
                EnsemblEntityKind::RegulatoryFeature => parse_regulatory_storable_file(
                    source_file,
                    &cache_info,
                    &predicate,
                    RegulatoryTarget::RegulatoryFeature,
                    coordinate_system_zero_based,
                )?,
                EnsemblEntityKind::MotifFeature => parse_regulatory_storable_file(
                    source_file,
                    &cache_info,
                    &predicate,
                    RegulatoryTarget::MotifFeature,
                    coordinate_system_zero_based,
                )?,
                EnsemblEntityKind::Variation => Vec::new(),
            };

            for row in parsed_rows {
                buffered_rows.push(row);
                emitted_rows += 1;

                if buffered_rows.len() >= batch_size.max(1) {
                    let batch = rows_to_record_batch(stream_schema.clone(), &buffered_rows)?;
                    buffered_rows.clear();
                    if tx.blocking_send(Ok(batch)).is_err() {
                        return Ok(()); // consumer dropped — graceful shutdown
                    }
                }

                if let Some(max_rows) = limit {
                    if emitted_rows >= max_rows {
                        stop = true;
                        break;
                    }
                }
            }
            continue;
        }

        // Text line path: uses BatchBuilder directly
        let mut reader = open_text_reader(source_file)?;
        let mut line = String::new();

        loop {
            line.clear();
            let bytes = reader.read_line(&mut line).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed reading line from {}: {}",
                    source_file.display(),
                    e
                ))
            })?;

            if bytes == 0 {
                break;
            }

            let line_trimmed = line.trim_end_matches(['\n', '\r']);
            let added = match kind {
                EnsemblEntityKind::Variation => parse_variation_line_into(
                    line_trimmed,
                    source_file_str,
                    &predicate,
                    variation_region_size,
                    coordinate_system_zero_based,
                    &mut batch_builder,
                    variation_col_idx.as_ref().unwrap(),
                    variation_ctx.as_ref().unwrap(),
                    &provenance,
                    source_id_writer.as_mut().unwrap(),
                )?,
                EnsemblEntityKind::Transcript => parse_transcript_line_into(
                    line_trimmed,
                    source_file_str,
                    &cache_info,
                    &predicate,
                    coordinate_system_zero_based,
                    &mut batch_builder,
                    transcript_col_idx.as_ref().unwrap(),
                    &provenance,
                )?,
                EnsemblEntityKind::RegulatoryFeature => parse_regulatory_line_into(
                    line_trimmed,
                    source_file_str,
                    &cache_info,
                    &predicate,
                    RegulatoryTarget::RegulatoryFeature,
                    coordinate_system_zero_based,
                    &mut batch_builder,
                    regulatory_col_idx.as_ref().unwrap(),
                    &provenance,
                )?,
                EnsemblEntityKind::MotifFeature => parse_regulatory_line_into(
                    line_trimmed,
                    source_file_str,
                    &cache_info,
                    &predicate,
                    RegulatoryTarget::MotifFeature,
                    coordinate_system_zero_based,
                    &mut batch_builder,
                    regulatory_col_idx.as_ref().unwrap(),
                    &provenance,
                )?,
            };

            if added {
                emitted_rows += 1;

                if batch_builder.len() >= batch_size.max(1) {
                    let batch = batch_builder.finish()?;
                    if tx.blocking_send(Ok(batch)).is_err() {
                        return Ok(()); // consumer dropped — graceful shutdown
                    }
                }

                if let Some(max_rows) = limit {
                    if emitted_rows >= max_rows {
                        stop = true;
                        break;
                    }
                }
            }
        }
    }

    // Flush remaining rows from BatchBuilder
    if batch_builder.len() > 0 {
        let batch = batch_builder.finish()?;
        if tx.blocking_send(Ok(batch)).is_err() {
            return Ok(());
        }
    }

    // Flush remaining rows from storable path
    if !buffered_rows.is_empty() {
        let batch = rows_to_record_batch(stream_schema.clone(), &buffered_rows)?;
        if tx.blocking_send(Ok(batch)).is_err() {
            return Ok(());
        }
    }

    Ok(())
}
