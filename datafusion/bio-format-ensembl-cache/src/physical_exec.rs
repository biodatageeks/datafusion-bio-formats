use crate::decode::storable_binary::try_collect_nstore_alias_counts_and_top_keys;
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
    fn parse_chrom_region_no_underscore() {
        assert!(parse_file_chrom_region("all_vars.gz").is_none());
    }

    #[test]
    fn parse_chrom_region_no_dash() {
        assert!(parse_file_chrom_region("1_var.gz").is_none());
    }

    // -----------------------------------------------------------------------
    // file_matches_predicate
    // -----------------------------------------------------------------------

    #[test]
    fn file_matches_no_predicate() {
        let pred = SimplePredicate::default();
        assert!(file_matches_predicate(
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
        assert!(file_matches_predicate(
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
        assert!(!file_matches_predicate(
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
        assert!(file_matches_predicate(
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
        assert!(!file_matches_predicate(
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
        assert!(!file_matches_predicate(
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
        assert!(file_matches_predicate(Path::new("all_vars.gz"), &pred));
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
}
