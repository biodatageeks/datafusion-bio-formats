use crate::decode::storable_binary::try_collect_nstore_alias_counts_and_top_keys;
use crate::discovery::extract_chrom_from_path;
use crate::entity::EnsemblEntityKind;
use crate::errors::{Result, exec_err};
use crate::exon::{ExonColumnIndices, parse_exon_line_into, parse_exon_storable_file_into};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::regulatory::{
    RegulatoryColumnIndices, RegulatoryTarget, parse_regulatory_line_into,
    parse_regulatory_storable_file_into,
};
use crate::transcript::{
    TranscriptColumnIndices, parse_transcript_line_into, parse_transcript_storable_file_into,
};
use crate::translation::{
    TranslationColumnIndices, parse_translation_line_into, parse_translation_storable_file_into,
};
use crate::util::{
    BatchBuilder, ColumnMap, ProvenanceWriter, open_binary_reader, open_text_reader,
};
use crate::variation::{
    SourceIdWriter, VariationColumnIndices, VariationContext, VariationParseResult,
    parse_variation_line_into,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_execution::TaskContext;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

/// Per-column UTF-8 threshold. A batch is flushed when any single string
/// column exceeds this limit, preventing Arrow i32 offset overflow.
const UTF8_FLUSH_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;

/// Total UTF-8 threshold across all string columns combined.  This bounds
/// per-batch memory when many large text columns are projected (e.g.
/// `SELECT *` including raw_object_json, cdna_seq, peptide_seq).
const UTF8_TOTAL_FLUSH_THRESHOLD_BYTES: usize = 128 * 1024 * 1024;

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
    /// When true, files are assigned to partitions in discovery order
    /// (consecutive chunks) and per-partition output ordering is declared.
    ///
    /// When combined with a single-chromosome predicate, the declared
    /// ordering is `(start ASC)` which is always correct.  Without a
    /// chromosome filter, no ordering is declared because karyotypic
    /// file order differs from lexicographic string comparison.
    pub(crate) preserve_sort_order: bool,
    /// When set, indicates the query filters to a single chromosome
    /// (e.g. `WHERE chrom = '1'`).  Used to declare `(start ASC)`
    /// per-partition ordering, enabling `SortPreservingMergeExec`.
    pub(crate) single_chrom_filter: Option<String>,
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
///
/// **Note:** This destroys the original file order.  Use
/// [`assign_files_ordered`] when output ordering must be preserved.
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

/// Assign files to partitions preserving discovery order.
///
/// Files are distributed in consecutive chunks so that each partition reads
/// a contiguous slice of the sorted file list.  This preserves the
/// within-partition sort invariant required for declaring output ordering.
fn assign_files_ordered(files: Vec<PathBuf>, num_partitions: usize) -> Vec<Vec<PathBuf>> {
    let mut partition_files: Vec<Vec<PathBuf>> = (0..num_partitions).map(|_| Vec::new()).collect();
    if files.is_empty() {
        return partition_files;
    }

    let chunk_size = files.len().div_ceil(num_partitions);
    for (i, path) in files.into_iter().enumerate() {
        let partition_idx = (i / chunk_size).min(num_partitions - 1);
        partition_files[partition_idx].push(path);
    }

    partition_files
}

impl EnsemblCacheExec {
    pub(crate) fn new(config: EnsemblCacheExecConfig) -> Self {
        let num_partitions = config.num_partitions.max(1);

        let partition_files = if config.preserve_sort_order {
            assign_files_ordered(config.files, num_partitions)
        } else {
            assign_files_balanced(config.files, num_partitions)
        };

        // Declare per-partition output ordering when preserve_sort_order is set.
        //
        // Single-chrom filter (WHERE chrom = '1'): declare (start ASC).
        //   Correct because within each partition, files are consecutive
        //   genomic chunks with monotonically increasing start positions.
        //
        // No chrom filter: do NOT declare ordering. Karyotypic file order
        //   (1, 2, ..., 10, ..., 22, X, Y, MT) differs from lexicographic
        //   string order ("1" < "10" < "2"), so declaring (chrom ASC, start ASC)
        //   would be incorrect and could produce wrong merge results.
        let eq_props = if config.preserve_sort_order && config.single_chrom_filter.is_some() {
            if let Ok(si) = config.schema.index_of("start") {
                let sort_exprs =
                    vec![PhysicalSortExpr::new_default(Arc::new(Column::new("start", si))).asc()];
                EquivalenceProperties::new_with_orderings(config.schema.clone(), [sort_exprs])
            } else {
                EquivalenceProperties::new(config.schema.clone())
            }
        } else {
            EquivalenceProperties::new(config.schema.clone())
        };

        let cache = PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );

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
// File-level predicate pruning
// ---------------------------------------------------------------------------

/// Prunes variation files by chrom AND genomic region using the
/// `{chrom}_{start}-{end}_var.gz` naming convention.
pub(crate) fn variation_file_matches_predicate(path: &Path, predicate: &SimplePredicate) -> bool {
    let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
        return true; // can't parse, don't prune
    };

    let Some((file_chrom, file_start, file_end)) = parse_file_chrom_region(file_name) else {
        // Fall back to chrom-only pruning (e.g. all_vars.gz in a chrom dir)
        return file_matches_chrom_predicate(path, predicate, EnsemblEntityKind::Variation);
    };

    // Check chromosome
    if let Some(pred_chrom) = &predicate.chrom
        && pred_chrom != file_chrom
    {
        return false;
    }

    // Check region range overlap
    if let Some(start_min) = predicate.start_min
        && file_end < start_min
    {
        return false;
    }
    if let Some(end_max) = predicate.end_max
        && file_start > end_max
    {
        return false;
    }

    true
}

/// Prunes files by chromosome using directory structure or filename
/// conventions. Works for all entity kinds. Returns `true` (don't prune)
/// if the chromosome cannot be determined from the path.
pub(crate) fn file_matches_chrom_predicate(
    path: &Path,
    predicate: &SimplePredicate,
    kind: EnsemblEntityKind,
) -> bool {
    let Some(pred_chrom) = &predicate.chrom else {
        return true; // no chrom filter → keep all files
    };

    let Some(file_chrom) = extract_chrom_from_path(path, kind) else {
        return true; // can't determine chrom → don't prune
    };

    file_chrom == *pred_chrom
}

/// Parses `{chrom}_{start}-{end}_var.gz` → (chrom, start, end)
pub(crate) fn parse_file_chrom_region(name: &str) -> Option<(&str, i64, i64)> {
    let marker = name.find('_')?;
    let chrom = &name[..marker];
    let suffix = &name[marker + 1..];
    let (start_raw, rest) = suffix.split_once('-')?;
    let end_len = rest
        .as_bytes()
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count();
    let end_raw = &rest[..end_len];
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

    fn statistics(&self) -> DFResult<datafusion::physical_plan::Statistics> {
        // Estimate row count from total compressed file sizes.
        // Rough heuristic: ~50 bytes per row in compressed VEP cache files.
        let total_bytes: u64 = self
            .partition_files
            .iter()
            .flatten()
            .map(|p| estimate_file_size(p))
            .sum();
        let estimated_rows = total_bytes / 50;
        Ok(datafusion::physical_plan::Statistics {
            num_rows: datafusion::common::stats::Precision::Inexact(estimated_rows as usize),
            total_byte_size: datafusion::common::stats::Precision::Inexact(total_bytes as usize),
            column_statistics: vec![],
        })
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

        // Files for this partition (pre-balanced by size), with predicate pruning.
        // Variation files support chrom + region pruning via filename parsing;
        // all other entities support chrom-only pruning via directory/filename.
        let files: Vec<PathBuf> = self.partition_files[partition]
            .iter()
            .filter(|path| {
                if kind == EnsemblEntityKind::Variation {
                    variation_file_matches_predicate(path, &predicate)
                } else {
                    file_matches_chrom_predicate(path, &predicate, kind)
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

        let mut builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 8);
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
enum RowDispatchState {
    Continue,
    Stop,
    ConsumerDropped,
}

fn dispatch_row(
    tx: &Sender<DFResult<RecordBatch>>,
    batch_builder: &mut BatchBuilder,
    emitted_rows: &mut usize,
    limit: Option<usize>,
    batch_size: usize,
) -> Result<RowDispatchState> {
    *emitted_rows += 1;

    if let Some(max_rows) = limit
        && *emitted_rows > max_rows
    {
        return Ok(RowDispatchState::Stop);
    }

    if batch_builder.len() >= batch_size
        || batch_builder.max_utf8_bytes() >= UTF8_FLUSH_THRESHOLD_BYTES
        || batch_builder.total_utf8_bytes() >= UTF8_TOTAL_FLUSH_THRESHOLD_BYTES
    {
        let batch = batch_builder.finish()?;
        if tx.blocking_send(Ok(batch)).is_err() {
            return Ok(RowDispatchState::ConsumerDropped);
        }
    }

    Ok(RowDispatchState::Continue)
}

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
    let exon_col_idx = if kind == EnsemblEntityKind::Exon {
        Some(ExonColumnIndices::new(&col_map))
    } else {
        None
    };
    let translation_col_idx = if kind == EnsemblEntityKind::Translation {
        Some(TranslationColumnIndices::new(&col_map))
    } else {
        None
    };

    // LIMIT 0: nothing to emit, skip all I/O.
    if limit == Some(0) {
        return Ok(());
    }

    let batch_size = batch_size.max(1);
    let mut batch_builder = BatchBuilder::new(stream_schema.clone(), batch_size)?;
    let mut emitted_rows: usize = 0;
    let mut malformed_rows: usize = 0;
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

        // Merge the pst0 header check with the alias-count scan to avoid
        // opening the (potentially gzip-compressed) file twice.
        let storable_prelude = if (kind == EnsemblEntityKind::Transcript
            || kind == EnsemblEntityKind::RegulatoryFeature
            || kind == EnsemblEntityKind::MotifFeature
            || kind == EnsemblEntityKind::Exon
            || kind == EnsemblEntityKind::Translation)
            && cache_info.serializer_type.as_deref() == Some("storable")
        {
            try_collect_nstore_alias_counts_and_top_keys(open_binary_reader(source_file)?)?
        } else {
            None
        };

        if let Some((alias_counts, entry_keys)) = storable_prelude {
            let mut reached_limit = false;
            let mut consumer_dropped = false;

            match kind {
                EnsemblEntityKind::Transcript => {
                    parse_transcript_storable_file_into(
                        source_file,
                        source_file_str,
                        &predicate,
                        coordinate_system_zero_based,
                        &mut batch_builder,
                        transcript_col_idx.as_ref().unwrap(),
                        &provenance,
                        Some((alias_counts.clone(), entry_keys.clone())),
                        |batch_builder| {
                            if let Some(err) = batch_builder.take_error() {
                                return Err(err);
                            }
                            match dispatch_row(
                                &tx,
                                batch_builder,
                                &mut emitted_rows,
                                limit,
                                batch_size,
                            )? {
                                RowDispatchState::Continue => Ok(true),
                                RowDispatchState::Stop => {
                                    reached_limit = true;
                                    Ok(false)
                                }
                                RowDispatchState::ConsumerDropped => {
                                    consumer_dropped = true;
                                    Ok(false)
                                }
                            }
                        },
                    )?;
                }
                EnsemblEntityKind::RegulatoryFeature => {
                    parse_regulatory_storable_file_into(
                        source_file,
                        source_file_str,
                        &predicate,
                        RegulatoryTarget::RegulatoryFeature,
                        coordinate_system_zero_based,
                        &mut batch_builder,
                        regulatory_col_idx.as_ref().unwrap(),
                        &provenance,
                        Some((alias_counts.clone(), entry_keys.clone())),
                        |batch_builder| {
                            if let Some(err) = batch_builder.take_error() {
                                return Err(err);
                            }
                            match dispatch_row(
                                &tx,
                                batch_builder,
                                &mut emitted_rows,
                                limit,
                                batch_size,
                            )? {
                                RowDispatchState::Continue => Ok(true),
                                RowDispatchState::Stop => {
                                    reached_limit = true;
                                    Ok(false)
                                }
                                RowDispatchState::ConsumerDropped => {
                                    consumer_dropped = true;
                                    Ok(false)
                                }
                            }
                        },
                    )?;
                }
                EnsemblEntityKind::MotifFeature => {
                    parse_regulatory_storable_file_into(
                        source_file,
                        source_file_str,
                        &predicate,
                        RegulatoryTarget::MotifFeature,
                        coordinate_system_zero_based,
                        &mut batch_builder,
                        regulatory_col_idx.as_ref().unwrap(),
                        &provenance,
                        Some((alias_counts.clone(), entry_keys.clone())),
                        |batch_builder| {
                            if let Some(err) = batch_builder.take_error() {
                                return Err(err);
                            }
                            match dispatch_row(
                                &tx,
                                batch_builder,
                                &mut emitted_rows,
                                limit,
                                batch_size,
                            )? {
                                RowDispatchState::Continue => Ok(true),
                                RowDispatchState::Stop => {
                                    reached_limit = true;
                                    Ok(false)
                                }
                                RowDispatchState::ConsumerDropped => {
                                    consumer_dropped = true;
                                    Ok(false)
                                }
                            }
                        },
                    )?;
                }
                EnsemblEntityKind::Exon => {
                    parse_exon_storable_file_into(
                        source_file,
                        source_file_str,
                        &predicate,
                        coordinate_system_zero_based,
                        &mut batch_builder,
                        exon_col_idx.as_ref().unwrap(),
                        &provenance,
                        Some((alias_counts.clone(), entry_keys.clone())),
                        |batch_builder| {
                            if let Some(err) = batch_builder.take_error() {
                                return Err(err);
                            }
                            match dispatch_row(
                                &tx,
                                batch_builder,
                                &mut emitted_rows,
                                limit,
                                batch_size,
                            )? {
                                RowDispatchState::Continue => Ok(true),
                                RowDispatchState::Stop => {
                                    reached_limit = true;
                                    Ok(false)
                                }
                                RowDispatchState::ConsumerDropped => {
                                    consumer_dropped = true;
                                    Ok(false)
                                }
                            }
                        },
                    )?;
                }
                EnsemblEntityKind::Translation => {
                    parse_translation_storable_file_into(
                        source_file,
                        source_file_str,
                        &predicate,
                        coordinate_system_zero_based,
                        &mut batch_builder,
                        translation_col_idx.as_ref().unwrap(),
                        &provenance,
                        Some((alias_counts.clone(), entry_keys.clone())),
                        |batch_builder| {
                            if let Some(err) = batch_builder.take_error() {
                                return Err(err);
                            }
                            match dispatch_row(
                                &tx,
                                batch_builder,
                                &mut emitted_rows,
                                limit,
                                batch_size,
                            )? {
                                RowDispatchState::Continue => Ok(true),
                                RowDispatchState::Stop => {
                                    reached_limit = true;
                                    Ok(false)
                                }
                                RowDispatchState::ConsumerDropped => {
                                    consumer_dropped = true;
                                    Ok(false)
                                }
                            }
                        },
                    )?;
                }
                EnsemblEntityKind::Variation => {}
            }

            if consumer_dropped {
                return Ok(()); // consumer dropped — graceful shutdown
            }
            if reached_limit {
                stop = true;
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
                EnsemblEntityKind::Variation => {
                    match parse_variation_line_into(
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
                    )? {
                        VariationParseResult::Added => true,
                        VariationParseResult::Skipped => false,
                        VariationParseResult::Malformed => {
                            malformed_rows += 1;
                            false
                        }
                    }
                }
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
                EnsemblEntityKind::Exon => {
                    let mut exon_stop = false;
                    let mut exon_consumer_dropped = false;
                    let _cont = parse_exon_line_into(
                        line_trimmed,
                        source_file_str,
                        &cache_info,
                        &predicate,
                        coordinate_system_zero_based,
                        &mut batch_builder,
                        exon_col_idx.as_ref().unwrap(),
                        &provenance,
                        |batch_builder| {
                            if let Some(err) = batch_builder.take_error() {
                                return Err(err);
                            }
                            match dispatch_row(
                                &tx,
                                batch_builder,
                                &mut emitted_rows,
                                limit,
                                batch_size,
                            )? {
                                RowDispatchState::Continue => Ok(true),
                                RowDispatchState::Stop => {
                                    exon_stop = true;
                                    Ok(false)
                                }
                                RowDispatchState::ConsumerDropped => {
                                    exon_consumer_dropped = true;
                                    Ok(false)
                                }
                            }
                        },
                    )?;
                    if exon_consumer_dropped {
                        return Ok(());
                    }
                    if exon_stop {
                        stop = true;
                        break;
                    }
                    // Rows dispatched internally via on_row_added; don't double-dispatch
                    false
                }
                EnsemblEntityKind::Translation => parse_translation_line_into(
                    line_trimmed,
                    source_file_str,
                    &cache_info,
                    &predicate,
                    coordinate_system_zero_based,
                    &mut batch_builder,
                    translation_col_idx.as_ref().unwrap(),
                    &provenance,
                )?,
            };

            if let Some(err) = batch_builder.take_error() {
                return Err(err);
            }

            if added {
                match dispatch_row(
                    &tx,
                    &mut batch_builder,
                    &mut emitted_rows,
                    limit,
                    batch_size,
                )? {
                    RowDispatchState::Continue => {}
                    RowDispatchState::Stop => {
                        stop = true;
                        break;
                    }
                    RowDispatchState::ConsumerDropped => {
                        return Ok(()); // consumer dropped — graceful shutdown
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

    if malformed_rows > 0 {
        log::warn!(
            "EnsemblCacheExec: skipped {malformed_rows} malformed variation line(s) \
             (missing required chrom/start/variation_name/allele_string)"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // -----------------------------------------------------------------------
    // parse_file_chrom_region
    // -----------------------------------------------------------------------

    #[test]
    fn parse_chrom_region_typical() {
        let (chrom, start, end) = parse_file_chrom_region("1_1-1000000_var.gz").unwrap();
        assert_eq!(chrom, "1");
        assert_eq!(start, 1);
        assert_eq!(end, 1000000);
    }

    #[test]
    fn parse_chrom_region_chr_prefix() {
        let (chrom, start, end) = parse_file_chrom_region("22_15000001-16000000_var.gz").unwrap();
        assert_eq!(chrom, "22");
        assert_eq!(start, 15000001);
        assert_eq!(end, 16000000);
    }

    #[test]
    fn parse_chrom_region_x() {
        let (chrom, _, _) = parse_file_chrom_region("X_1-1000000_var.gz").unwrap();
        assert_eq!(chrom, "X");
    }

    #[test]
    fn parse_chrom_region_all_vars_returns_none() {
        assert!(parse_file_chrom_region("all_vars.gz").is_none());
    }

    #[test]
    fn parse_chrom_region_no_dash() {
        assert!(parse_file_chrom_region("1_var.gz").is_none());
    }

    // -----------------------------------------------------------------------
    // variation_file_matches_predicate
    // -----------------------------------------------------------------------

    #[test]
    fn file_matches_no_predicate() {
        let pred = SimplePredicate::default();
        assert!(variation_file_matches_predicate(
            Path::new("1_1-1000000_var.gz"),
            &pred
        ));
    }

    #[test]
    fn file_matches_chrom_match() {
        let pred = SimplePredicate {
            chrom: Some("1".to_string()),
            ..Default::default()
        };
        assert!(variation_file_matches_predicate(
            Path::new("1_1-1000000_var.gz"),
            &pred
        ));
    }

    #[test]
    fn file_matches_chrom_mismatch() {
        let pred = SimplePredicate {
            chrom: Some("2".to_string()),
            ..Default::default()
        };
        assert!(!variation_file_matches_predicate(
            Path::new("1_1-1000000_var.gz"),
            &pred
        ));
    }

    #[test]
    fn file_matches_region_overlap() {
        let pred = SimplePredicate {
            start_min: Some(500000),
            end_max: Some(1500000),
            ..Default::default()
        };
        // File covers 1-1000000, predicate is 500000-1500000 → overlaps
        assert!(variation_file_matches_predicate(
            Path::new("1_1-1000000_var.gz"),
            &pred
        ));
    }

    #[test]
    fn file_matches_region_before() {
        let pred = SimplePredicate {
            start_min: Some(2000000),
            ..Default::default()
        };
        // File covers 1-1000000, predicate starts at 2000000 → no overlap
        assert!(!variation_file_matches_predicate(
            Path::new("1_1-1000000_var.gz"),
            &pred
        ));
    }

    #[test]
    fn file_matches_region_after() {
        let pred = SimplePredicate {
            end_max: Some(0),
            ..Default::default()
        };
        // File covers 1-1000000, predicate ends at 0 → no overlap
        assert!(!variation_file_matches_predicate(
            Path::new("1_1-1000000_var.gz"),
            &pred
        ));
    }

    #[test]
    fn file_matches_unparseable_always_matches() {
        let pred = SimplePredicate {
            chrom: Some("1".to_string()),
            ..Default::default()
        };
        // Unparseable name → don't prune
        assert!(variation_file_matches_predicate(
            Path::new("all_vars.gz"),
            &pred
        ));
    }

    // -----------------------------------------------------------------------
    // file_matches_chrom_predicate (all entity kinds)
    // -----------------------------------------------------------------------

    #[test]
    fn chrom_predicate_no_filter_matches_all() {
        let pred = SimplePredicate::default();
        assert!(file_matches_chrom_predicate(
            Path::new("/cache/1/1-1000000.gz"),
            &pred,
            EnsemblEntityKind::Transcript
        ));
    }

    #[test]
    fn chrom_predicate_transcript_merged_match() {
        let pred = SimplePredicate {
            chrom: Some("1".to_string()),
            ..Default::default()
        };
        assert!(file_matches_chrom_predicate(
            Path::new("/cache/1/1-1000000.gz"),
            &pred,
            EnsemblEntityKind::Transcript
        ));
    }

    #[test]
    fn chrom_predicate_transcript_merged_mismatch() {
        let pred = SimplePredicate {
            chrom: Some("2".to_string()),
            ..Default::default()
        };
        assert!(!file_matches_chrom_predicate(
            Path::new("/cache/1/1-1000000.gz"),
            &pred,
            EnsemblEntityKind::Transcript
        ));
    }

    #[test]
    fn chrom_predicate_transcript_explicit_match() {
        let pred = SimplePredicate {
            chrom: Some("1".to_string()),
            ..Default::default()
        };
        assert!(file_matches_chrom_predicate(
            Path::new("/cache/transcript/chr1_transcript.storable.gz"),
            &pred,
            EnsemblEntityKind::Transcript
        ));
    }

    #[test]
    fn chrom_predicate_transcript_explicit_mismatch() {
        let pred = SimplePredicate {
            chrom: Some("2".to_string()),
            ..Default::default()
        };
        assert!(!file_matches_chrom_predicate(
            Path::new("/cache/transcript/chr1_transcript.storable.gz"),
            &pred,
            EnsemblEntityKind::Transcript
        ));
    }

    #[test]
    fn chrom_predicate_regulatory_match() {
        let pred = SimplePredicate {
            chrom: Some("2".to_string()),
            ..Default::default()
        };
        assert!(file_matches_chrom_predicate(
            Path::new("/cache/regulatory/chr2_regulatory.storable.gz"),
            &pred,
            EnsemblEntityKind::RegulatoryFeature
        ));
    }

    #[test]
    fn chrom_predicate_exon_merged_match() {
        let pred = SimplePredicate {
            chrom: Some("X".to_string()),
            ..Default::default()
        };
        assert!(file_matches_chrom_predicate(
            Path::new("/cache/X/1-1000000.gz"),
            &pred,
            EnsemblEntityKind::Exon
        ));
    }

    #[test]
    fn chrom_predicate_exon_merged_mismatch() {
        let pred = SimplePredicate {
            chrom: Some("1".to_string()),
            ..Default::default()
        };
        assert!(!file_matches_chrom_predicate(
            Path::new("/cache/X/1-1000000.gz"),
            &pred,
            EnsemblEntityKind::Exon
        ));
    }

    // -----------------------------------------------------------------------
    // assign_files_balanced
    // -----------------------------------------------------------------------

    #[test]
    fn balanced_empty_files() {
        let result = assign_files_balanced(vec![], 4);
        assert_eq!(result.len(), 4);
        assert!(result.iter().all(|p| p.is_empty()));
    }

    #[test]
    fn balanced_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("a.gz");
        fs::write(&f, b"data").unwrap();

        let result = assign_files_balanced(vec![f], 4);
        let total: usize = result.iter().map(|p| p.len()).sum();
        assert_eq!(total, 1);
    }

    #[test]
    fn balanced_round_robin() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = Vec::new();
        for i in 0..6 {
            let f = dir.path().join(format!("f{i}.gz"));
            fs::write(&f, vec![0u8; (i + 1) * 100]).unwrap();
            files.push(f);
        }

        let result = assign_files_balanced(files, 3);
        assert_eq!(result.len(), 3);
        // Each partition should have exactly 2 files (6 / 3)
        for partition in &result {
            assert_eq!(partition.len(), 2);
        }
    }

    #[test]
    fn balanced_more_partitions_than_files() {
        let dir = tempfile::tempdir().unwrap();
        let f1 = dir.path().join("a.gz");
        let f2 = dir.path().join("b.gz");
        fs::write(&f1, b"data").unwrap();
        fs::write(&f2, b"data").unwrap();

        let result = assign_files_balanced(vec![f1, f2], 8);
        let total: usize = result.iter().map(|p| p.len()).sum();
        assert_eq!(total, 2);
        // Should have at most 1 file per partition
        assert!(result.iter().all(|p| p.len() <= 1));
    }

    // -----------------------------------------------------------------------
    // assign_files_ordered
    // -----------------------------------------------------------------------

    #[test]
    fn ordered_preserves_order() {
        let files: Vec<PathBuf> = (0..6).map(|i| PathBuf::from(format!("f{i}.gz"))).collect();

        let result = assign_files_ordered(files, 3);
        assert_eq!(result.len(), 3);
        // Each partition gets 2 consecutive files
        assert_eq!(
            result[0]
                .iter()
                .map(|p| p.to_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["f0.gz", "f1.gz"]
        );
        assert_eq!(
            result[1]
                .iter()
                .map(|p| p.to_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["f2.gz", "f3.gz"]
        );
        assert_eq!(
            result[2]
                .iter()
                .map(|p| p.to_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["f4.gz", "f5.gz"]
        );
    }

    #[test]
    fn ordered_uneven_distribution() {
        let files: Vec<PathBuf> = (0..7).map(|i| PathBuf::from(format!("f{i}.gz"))).collect();

        let result = assign_files_ordered(files, 3);
        assert_eq!(result.len(), 3);
        // ceil(7/3) = 3 files per chunk, last partition gets remainder
        let counts: Vec<usize> = result.iter().map(|p| p.len()).collect();
        assert_eq!(counts.iter().sum::<usize>(), 7);
        // First partitions get 3, last gets 1
        assert_eq!(counts, vec![3, 3, 1]);
    }

    #[test]
    fn ordered_empty() {
        let result = assign_files_ordered(Vec::new(), 4);
        assert_eq!(result.len(), 4);
        assert!(result.iter().all(|p| p.is_empty()));
    }

    #[test]
    fn ordered_single_file() {
        let files = vec![PathBuf::from("f0.gz")];
        let result = assign_files_ordered(files, 4);
        let total: usize = result.iter().map(|p| p.len()).sum();
        assert_eq!(total, 1);
        assert_eq!(result[0].len(), 1);
    }

    #[test]
    fn ordered_more_partitions_than_files() {
        let files: Vec<PathBuf> = (0..2).map(|i| PathBuf::from(format!("f{i}.gz"))).collect();

        let result = assign_files_ordered(files, 8);
        let total: usize = result.iter().map(|p| p.len()).sum();
        assert_eq!(total, 2);
    }

    // -----------------------------------------------------------------------
    // Ordering preserved with partitions > 1
    // -----------------------------------------------------------------------

    /// Helper: collect all files from partitions in partition order,
    /// simulating how DataFusion's SortPreservingMergeExec would see them.
    fn flatten_partitions(partitions: &[Vec<PathBuf>]) -> Vec<String> {
        partitions
            .iter()
            .flat_map(|p| p.iter().map(|f| f.to_str().unwrap().to_string()))
            .collect()
    }

    #[test]
    fn ordered_genomic_sort_preserved_across_partitions() {
        // Simulate variation files in karyotypic genomic order (as produced
        // by sort_variation_files_genomic). Verify that assign_files_ordered
        // distributes them so that flattening partitions in order recovers
        // the original genomic order.
        use crate::discovery::sort_variation_files_genomic;

        let mut files: Vec<PathBuf> = vec![
            "10_1-1000000_var.gz",
            "2_1-1000000_var.gz",
            "1_1-1000000_var.gz",
            "1_1000001-2000000_var.gz",
            "2_1000001-2000000_var.gz",
            "X_1-1000000_var.gz",
            "22_1-1000000_var.gz",
            "22_1000001-2000000_var.gz",
            "Y_1-1000000_var.gz",
        ]
        .into_iter()
        .map(PathBuf::from)
        .collect();

        sort_variation_files_genomic(&mut files);
        let expected_order: Vec<String> = files
            .iter()
            .map(|f| f.to_str().unwrap().to_string())
            .collect();

        // Distribute across 4 partitions
        let partitions = assign_files_ordered(files, 4);
        assert_eq!(partitions.len(), 4);

        // Each partition's files must be a contiguous sub-slice of the
        // genomic order.
        let flattened = flatten_partitions(&partitions);
        assert_eq!(flattened, expected_order);
    }

    #[test]
    fn ordered_within_partition_monotonic_chrom_start() {
        // Verify that within each partition, (chrom_sort_key, start) is
        // monotonically non-decreasing using karyotypic order.
        use crate::discovery::{chrom_sort_key, sort_variation_files_genomic};

        let mut files: Vec<PathBuf> = vec![
            "1_1-1000000_var.gz",
            "1_1000001-2000000_var.gz",
            "1_2000001-3000000_var.gz",
            "2_1-1000000_var.gz",
            "2_1000001-2000000_var.gz",
            "10_1-1000000_var.gz",
            "22_1-1000000_var.gz",
            "X_1-1000000_var.gz",
            "X_1000001-2000000_var.gz",
            "Y_1-1000000_var.gz",
        ]
        .into_iter()
        .map(PathBuf::from)
        .collect();

        sort_variation_files_genomic(&mut files);

        for num_partitions in [2, 3, 4, 5, 8] {
            let partitions = assign_files_ordered(files.clone(), num_partitions);

            for (p_idx, partition) in partitions.iter().enumerate() {
                let regions: Vec<_> = partition
                    .iter()
                    .filter_map(|f| {
                        let name = f.to_str()?;
                        parse_file_chrom_region(name)
                    })
                    .collect();

                for window in regions.windows(2) {
                    let (chrom_a, start_a, _) = window[0];
                    let (chrom_b, start_b, _) = window[1];
                    let key_a = (chrom_sort_key(chrom_a), start_a);
                    let key_b = (chrom_sort_key(chrom_b), start_b);
                    assert!(
                        key_a <= key_b,
                        "partition {p_idx} with {num_partitions} partitions: \
                         ({chrom_a}, {start_a}) > ({chrom_b}, {start_b})"
                    );
                }
            }
        }
    }

    #[test]
    fn ordered_pruned_subset_preserves_order() {
        // Simulate the scan() flow: genomic sort → predicate prune → ordered assign.
        // Verify ordering is preserved when only one chromosome's files remain.
        use crate::discovery::sort_variation_files_genomic;
        use crate::filter::SimplePredicate;

        let mut files: Vec<PathBuf> = vec![
            "1_1-1000000_var.gz",
            "1_1000001-2000000_var.gz",
            "2_1-1000000_var.gz",
            "2_1000001-2000000_var.gz",
            "2_2000001-3000000_var.gz",
            "10_1-1000000_var.gz",
            "X_1-1000000_var.gz",
        ]
        .into_iter()
        .map(PathBuf::from)
        .collect();

        sort_variation_files_genomic(&mut files);

        // Prune to chrom 2 only (simulating WHERE chrom = '2')
        let predicate = SimplePredicate {
            chrom: Some("2".to_string()),
            ..Default::default()
        };
        let pruned: Vec<PathBuf> = files
            .into_iter()
            .filter(|f| variation_file_matches_predicate(f, &predicate))
            .collect();

        assert_eq!(pruned.len(), 3);

        // Distribute across 3 partitions — each should get 1 file
        let partitions = assign_files_ordered(pruned.clone(), 3);
        let flattened = flatten_partitions(&partitions);
        let expected: Vec<String> = pruned
            .iter()
            .map(|f| f.to_str().unwrap().to_string())
            .collect();
        assert_eq!(flattened, expected);

        // All files should be chr2 in ascending start order
        let starts: Vec<i64> = flattened
            .iter()
            .filter_map(|name| parse_file_chrom_region(name).map(|(_, s, _)| s))
            .collect();
        assert_eq!(starts, vec![1, 1000001, 2000001]);
    }

    #[test]
    fn ordered_pruned_region_within_chrom_preserves_order() {
        // Prune to a specific region within a chromosome and verify
        // ordering is preserved across partitions.
        use crate::discovery::sort_variation_files_genomic;
        use crate::filter::SimplePredicate;

        let mut files: Vec<PathBuf> = vec![
            "1_1-1000000_var.gz",
            "1_1000001-2000000_var.gz",
            "1_2000001-3000000_var.gz",
            "1_3000001-4000000_var.gz",
            "1_4000001-5000000_var.gz",
            "1_5000001-6000000_var.gz",
            "2_1-1000000_var.gz",
        ]
        .into_iter()
        .map(PathBuf::from)
        .collect();

        sort_variation_files_genomic(&mut files);

        // Prune to chr1:2000001-4000000.
        // File overlap logic: file_end < start_min prunes, file_start > end_max prunes.
        // 1_2000001-3000000 → overlaps (start=2000001, end=3000000)
        // 1_3000001-4000000 → overlaps (start=3000001, end=4000000)
        // 1_1000001-2000000 → pruned (file_end=2000000 < start_min=2000001)
        // 1_4000001-5000000 → pruned (file_start=4000001 > end_max=4000000)
        let predicate = SimplePredicate {
            chrom: Some("1".to_string()),
            start_min: Some(2000001),
            end_max: Some(4000000),
            ..Default::default()
        };
        let pruned: Vec<PathBuf> = files
            .into_iter()
            .filter(|f| variation_file_matches_predicate(f, &predicate))
            .collect();

        assert_eq!(pruned.len(), 2);

        let partitions = assign_files_ordered(pruned.clone(), 2);
        let flattened = flatten_partitions(&partitions);
        let expected: Vec<String> = pruned
            .iter()
            .map(|f| f.to_str().unwrap().to_string())
            .collect();
        assert_eq!(flattened, expected);
    }
}
