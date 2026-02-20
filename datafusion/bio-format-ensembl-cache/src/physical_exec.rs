use crate::entity::EnsemblEntityKind;
use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::regulatory::{RegulatoryTarget, parse_regulatory_line, parse_regulatory_storable_file};
use crate::row::Row;
use crate::transcript::{parse_transcript_line, parse_transcript_storable_file};
use crate::util::{is_storable_binary_payload, open_text_reader, rows_to_record_batch};
use crate::variation::parse_variation_line;
use async_stream::try_stream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_execution::TaskContext;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct EnsemblCacheExec {
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
        f.debug_struct("EnsemblCacheExec")
            .field("kind", &self.kind)
            .field("files", &self.files)
            .field("num_partitions", &self.num_partitions)
            .field("limit", &self.limit)
            .finish()
    }
}

impl DisplayAs for EnsemblCacheExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EnsemblCacheExec(kind={:?}, files={}, partitions={})",
            self.kind,
            self.files.len(),
            self.num_partitions
        )
    }
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

        Self {
            kind: config.kind,
            cache_info: config.cache_info,
            files: config.files,
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
        let files = self
            .files
            .iter()
            .enumerate()
            .filter_map(|(idx, path)| {
                if idx % self.num_partitions == partition {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let predicate = self.predicate.clone();
        let limit = self.limit;
        let variation_region_size = self.variation_region_size.unwrap_or(1_000_000);
        let coordinate_system_zero_based = self.coordinate_system_zero_based;
        let batch_size = self
            .batch_size_hint
            .unwrap_or_else(|| context.session_config().batch_size());

        let stream = try_stream! {
            let mut buffered_rows: Vec<Row> = Vec::with_capacity(batch_size.max(1));
            let mut emitted_rows: usize = 0;
            let mut stop = false;

            for source_file in files {
                if stop {
                    break;
                }

                let use_native_storable = (kind == EnsemblEntityKind::Transcript
                    || kind == EnsemblEntityKind::RegulatoryFeature
                    || kind == EnsemblEntityKind::MotifFeature)
                    && cache_info.serializer_type.as_deref() == Some("storable")
                    && is_storable_binary_payload(&source_file)?;

                if use_native_storable {
                    let parsed_rows = match kind {
                        EnsemblEntityKind::Transcript => {
                            parse_transcript_storable_file(
                                &source_file,
                                &cache_info,
                                &predicate,
                                coordinate_system_zero_based,
                            )?
                        }
                        EnsemblEntityKind::RegulatoryFeature => parse_regulatory_storable_file(
                            &source_file,
                            &cache_info,
                            &predicate,
                            RegulatoryTarget::RegulatoryFeature,
                            coordinate_system_zero_based,
                        )?,
                        EnsemblEntityKind::MotifFeature => parse_regulatory_storable_file(
                            &source_file,
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
                            let batch =
                                rows_to_record_batch(stream_schema.clone(), &buffered_rows)?;
                            buffered_rows.clear();
                            yield batch;
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

                let mut reader = open_text_reader(&source_file)?;
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
                    let maybe_row = match kind {
                        EnsemblEntityKind::Variation => parse_variation_line(
                            line_trimmed,
                            &source_file,
                            &cache_info,
                            &predicate,
                            variation_region_size,
                            coordinate_system_zero_based,
                        )?,
                        EnsemblEntityKind::Transcript => {
                            parse_transcript_line(
                                line_trimmed,
                                &source_file,
                                &cache_info,
                                &predicate,
                                coordinate_system_zero_based,
                            )?
                        }
                        EnsemblEntityKind::RegulatoryFeature => parse_regulatory_line(
                            line_trimmed,
                            &source_file,
                            &cache_info,
                            &predicate,
                            RegulatoryTarget::RegulatoryFeature,
                            coordinate_system_zero_based,
                        )?,
                        EnsemblEntityKind::MotifFeature => parse_regulatory_line(
                            line_trimmed,
                            &source_file,
                            &cache_info,
                            &predicate,
                            RegulatoryTarget::MotifFeature,
                            coordinate_system_zero_based,
                        )?,
                    };

                    if let Some(row) = maybe_row {
                        buffered_rows.push(row);
                        emitted_rows += 1;

                        if buffered_rows.len() >= batch_size.max(1) {
                            let batch =
                                rows_to_record_batch(stream_schema.clone(), &buffered_rows)?;
                            buffered_rows.clear();
                            yield batch;
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

            if !buffered_rows.is_empty() {
                let batch = rows_to_record_batch(stream_schema.clone(), &buffered_rows)?;
                yield batch;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
