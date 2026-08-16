use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::genotype::{
    CoordinateSystem, GENOTYPE_ALLELE_ORDER_KEY, GENOTYPE_COUNTED_ALLELE_KEY,
    GENOTYPE_OUTPUT_MODE_KEY, GenotypeMetric, GenotypeScanMetrics, MissingSamplePolicy,
    PredicateGuarantee, can_push_limit_below_filters, resolve_genotype_fields,
};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_core::range_planning::{ByteRange, coalesce_byte_ranges};

use crate::fileset::{PGEN_SPEC_BASELINE, PgenFileset};
use crate::filter::{evaluate_exact_filter, supports_exact_filter};
use crate::physical_exec::{PgenExec, PgenPartition};

/// Field metadata containing selected original PSAM identities as JSON.
pub const PGEN_SAMPLE_IDENTITIES_KEY: &str = "bio.pgen.sample_identities";

/// Policy used to construct selectable sample names from PSAM identifiers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PsamIdMode {
    /// Use IID alone and reject duplicate selected IIDs.
    #[default]
    Iid,
    /// Use an escaped `FID:IID` name.
    FidIid,
    /// Use an escaped `FID:IID:SID` name.
    FidIidSid,
}

/// Read and planning options for a PGEN/PVAR/PSAM fileset.
#[derive(Debug, Clone)]
pub struct PgenReadOptions {
    /// Explicit PVAR location; `.pvar` then `.pvar.zst` are tried when absent.
    pub pvar_path: Option<String>,
    /// Explicit PSAM location; the shared-basename `.psam` is used when absent.
    pub psam_path: Option<String>,
    /// Explicit external PGEN index location.
    pub pgi_path: Option<String>,
    /// Requested sample names in output order, or all samples when absent.
    pub samples: Option<Vec<String>>,
    /// Behavior for requested sample names absent from PSAM.
    pub missing_sample_policy: MissingSamplePolicy,
    /// PSAM identifier policy.
    pub psam_id_mode: PsamIdMode,
    /// Selected genotype children from `GT`, `PHASED`, `DS`, `DS_STORED`, and `HDS`.
    pub genotype_fields: Option<Vec<String>>,
    /// Output coordinate presentation.
    pub coordinate_system: CoordinateSystem,
    /// Credentials and transport configuration for remote objects.
    pub object_storage_options: Option<ObjectStorageOptions>,
    /// Maximum compressed bytes accepted for either text companion.
    pub max_companion_bytes: usize,
    /// Maximum decoded bytes accepted for either text companion.
    pub max_decompressed_companion_bytes: usize,
    /// Maximum bytes accepted for a PGEN or PGI header.
    pub max_header_bytes: usize,
    /// Maximum encoded bytes accepted for one PGEN variant record.
    pub max_record_bytes: u64,
    /// Maximum accepted PVAR row count.
    pub max_variants: usize,
    /// Maximum accepted PSAM row count.
    pub max_samples: usize,
    /// Maximum unselected byte gap bridged by one PGEN range.
    pub max_range_gap: u64,
    /// Maximum size of a coalesced PGEN range.
    pub max_range_bytes: u64,
    /// Soft target for genotype bytes in one RecordBatch.
    pub batch_soft_byte_limit: usize,
}

impl Default for PgenReadOptions {
    fn default() -> Self {
        Self {
            pvar_path: None,
            psam_path: None,
            pgi_path: None,
            samples: None,
            missing_sample_policy: MissingSamplePolicy::Error,
            psam_id_mode: PsamIdMode::Iid,
            genotype_fields: None,
            coordinate_system: CoordinateSystem::ZeroBasedHalfOpen,
            object_storage_options: None,
            max_companion_bytes: 512 * 1024 * 1024,
            max_decompressed_companion_bytes: 1024 * 1024 * 1024,
            max_header_bytes: 1024 * 1024 * 1024,
            max_record_bytes: 512 * 1024 * 1024,
            max_variants: 100_000_000,
            max_samples: 10_000_000,
            max_range_gap: 0,
            max_range_bytes: 16 * 1024 * 1024,
            batch_soft_byte_limit: 64 * 1024 * 1024,
        }
    }
}

/// Read-only DataFusion table provider for a PLINK 2 fileset.
#[derive(Clone, Debug)]
pub struct PgenTableProvider {
    pub(crate) fileset: Arc<PgenFileset>,
    schema: SchemaRef,
    options: PgenReadOptions,
    genotype_fields: Arc<Vec<String>>,
}

impl PgenTableProvider {
    /// Opens and validates a local or remote PGEN/PVAR/PSAM fileset.
    pub async fn try_new(pgen_path: impl Into<String>, options: PgenReadOptions) -> Result<Self> {
        validate_options(&options)?;
        let available = ["GT", "PHASED", "DS", "DS_STORED", "HDS"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let selected = resolve_genotype_fields(&available, options.genotype_fields.as_deref())?;
        let genotype_fields = Arc::new(selected.names().to_vec());
        let fileset = Arc::new(PgenFileset::open(pgen_path.into(), &options).await?);
        let schema = build_schema(&fileset, &genotype_fields, &options)?;
        Ok(Self {
            fileset,
            schema,
            options,
            genotype_fields,
        })
    }

    /// Returns the resolved PVAR companion location.
    pub fn pvar_path(&self) -> &str {
        &self.fileset.pvar_path
    }

    /// Returns the resolved PSAM companion location.
    pub fn psam_path(&self) -> &str {
        &self.fileset.psam_path
    }

    /// Returns the resolved PGI location when the PGEN uses an external index.
    pub fn pgi_path(&self) -> Option<&str> {
        self.fileset.pgi_path.as_deref()
    }

    /// Returns selected sample names in emitted list order.
    pub fn sample_names(&self) -> &[String] {
        self.fileset.selected_samples.names()
    }
}

#[async_trait]
impl TableProvider for PgenTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if supports_exact_filter(filter) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = project_schema(&self.schema, projection)?;
        let guarantees = filters
            .iter()
            .map(|filter| {
                if supports_exact_filter(filter) {
                    PredicateGuarantee::Exact
                } else {
                    PredicateGuarantee::Unsupported
                }
            })
            .collect::<Vec<_>>();
        let exact_filters = filters
            .iter()
            .filter(|filter| supports_exact_filter(filter))
            .collect::<Vec<_>>();
        let mut selected = self
            .fileset
            .variants
            .iter()
            .enumerate()
            .filter(|(_, variant)| {
                exact_filters
                    .iter()
                    .all(|filter| evaluate_exact_filter(variant, filter))
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if can_push_limit_below_filters(&guarantees)
            && let Some(limit) = limit
        {
            selected.truncate(limit);
        }
        if selected.is_empty() {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema,
            )));
        }

        let genotype_projected = schema.index_of("genotypes").is_ok();
        let needs_payload = genotype_projected && !self.fileset.selected_samples.is_empty();
        let partitions = if needs_payload {
            plan_payload_partitions(
                &selected,
                &self.fileset,
                state.config().target_partitions(),
                self.options.max_range_gap,
                self.options.max_range_bytes,
            )?
        } else {
            plan_metadata_partitions(&selected, state.config().target_partitions())
        };
        let metrics = Arc::new(GenotypeScanMetrics::default());
        // CoalescedRanges describes the planned post-coalescing ranges;
        // RangeRequests is incremented only when execution issues each read.
        metrics.add(
            GenotypeMetric::CoalescedRanges,
            partitions
                .iter()
                .map(|partition| partition.ranges.len() as u64)
                .sum(),
        );
        metrics.add(GenotypeMetric::PrimaryBytesRead, self.fileset.header_bytes);
        metrics.add(
            GenotypeMetric::CompanionBytesRead,
            self.fileset.companion_bytes,
        );
        metrics.add(
            GenotypeMetric::MetadataCandidates,
            self.fileset.variants.len() as u64,
        );
        metrics.add(GenotypeMetric::SelectedVariants, selected.len() as u64);
        metrics.add(
            GenotypeMetric::SamplesRequested,
            self.fileset.selected_samples.source_indices().len() as u64,
        );
        if !needs_payload {
            metrics.add(GenotypeMetric::PayloadsSkipped, selected.len() as u64);
        }
        let partition_count = partitions.len();
        Ok(Arc::new(PgenExec {
            fileset: self.fileset.clone(),
            schema: schema.clone(),
            partitions: Arc::new(partitions),
            genotype_fields: self.genotype_fields.clone(),
            metrics,
            batch_soft_byte_limit: self.options.batch_soft_byte_limit,
            cache: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partition_count),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
}

fn validate_options(options: &PgenReadOptions) -> Result<()> {
    for (name, value) in [
        ("max_companion_bytes", options.max_companion_bytes),
        (
            "max_decompressed_companion_bytes",
            options.max_decompressed_companion_bytes,
        ),
        ("max_header_bytes", options.max_header_bytes),
        ("max_variants", options.max_variants),
        ("max_samples", options.max_samples),
        ("batch_soft_byte_limit", options.batch_soft_byte_limit),
    ] {
        if value == 0 {
            return Err(DataFusionError::Plan(format!(
                "{name} must be greater than zero"
            )));
        }
    }
    if options.max_range_bytes == 0 {
        return Err(DataFusionError::Plan(
            "max_range_bytes must be greater than zero".to_string(),
        ));
    }
    if options.max_record_bytes == 0 {
        return Err(DataFusionError::Plan(
            "max_record_bytes must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn build_schema(
    fileset: &PgenFileset,
    genotype_fields: &[String],
    options: &PgenReadOptions,
) -> Result<SchemaRef> {
    let sample_metadata = fileset.selected_samples.field_metadata();
    let mut children = Vec::with_capacity(genotype_fields.len());
    for name in genotype_fields {
        let field = match name.as_str() {
            "GT" => Field::new(
                "GT",
                DataType::List(Arc::new(Field::new(
                    "sample",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("allele", DataType::UInt16, false)),
                        2,
                    ),
                    true,
                ))),
                false,
            )
            .with_metadata(HashMap::from([
                (
                    GENOTYPE_OUTPUT_MODE_KEY.to_string(),
                    "raw_alleles".to_string(),
                ),
                (
                    GENOTYPE_ALLELE_ORDER_KEY.to_string(),
                    "PVAR REF=0, ALT source order=1..n".to_string(),
                ),
                (
                    "bio.pgen.ploidy_semantics".to_string(),
                    "encoded_diploid".to_string(),
                ),
            ])),
            "PHASED" => Field::new(
                "PHASED",
                DataType::List(Arc::new(Field::new("sample", DataType::Boolean, true))),
                false,
            )
            .with_metadata(HashMap::from([(
                "bio.pgen.phase_semantics".to_string(),
                "null=missing call, false=unphased, true=phased".to_string(),
            )])),
            "DS" => Field::new(
                "DS",
                DataType::List(Arc::new(Field::new("sample", DataType::Float32, true))),
                false,
            )
            .with_metadata(HashMap::from([
                (GENOTYPE_OUTPUT_MODE_KEY.to_string(), "dosage".to_string()),
                (
                    GENOTYPE_COUNTED_ALLELE_KEY.to_string(),
                    "PVAR ALT allele index 1".to_string(),
                ),
                (
                    "bio.pgen.dosage_scale".to_string(),
                    "uint16/16384".to_string(),
                ),
            ])),
            "DS_STORED" => Field::new(
                "DS_STORED",
                DataType::List(Arc::new(Field::new("sample", DataType::Float32, true))),
                false,
            )
            .with_metadata(HashMap::from([
                (
                    GENOTYPE_OUTPUT_MODE_KEY.to_string(),
                    "stored_dosage".to_string(),
                ),
                (
                    GENOTYPE_COUNTED_ALLELE_KEY.to_string(),
                    "PVAR ALT allele index 1".to_string(),
                ),
                (
                    "bio.pgen.dosage_scale".to_string(),
                    "uint16/16384".to_string(),
                ),
            ])),
            "HDS" => Field::new(
                "HDS",
                DataType::List(Arc::new(Field::new(
                    "sample",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("haplotype", DataType::Float32, false)),
                        2,
                    ),
                    true,
                ))),
                false,
            )
            .with_metadata(HashMap::from([(
                GENOTYPE_COUNTED_ALLELE_KEY.to_string(),
                "PVAR ALT allele index 1".to_string(),
            )])),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "unsupported PGEN genotype field {name}"
                )));
            }
        };
        children.push(field);
    }
    let identities =
        serde_json::to_string(fileset.selected_identities.as_ref()).map_err(|error| {
            DataFusionError::Plan(format!("failed to serialize PSAM identities: {error}"))
        })?;
    let mut genotype_metadata = sample_metadata;
    genotype_metadata.insert(PGEN_SAMPLE_IDENTITIES_KEY.to_string(), identities);
    let genotypes = Field::new("genotypes", DataType::Struct(Fields::from(children)), false)
        .with_metadata(genotype_metadata);
    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt64, false),
            Field::new("end", DataType::UInt64, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new(
                "alt",
                DataType::List(Arc::new(Field::new("allele", DataType::Utf8, false))),
                false,
            ),
            genotypes,
        ],
        HashMap::from([
            (
                COORDINATE_SYSTEM_METADATA_KEY.to_string(),
                options.coordinate_system.metadata_value().to_string(),
            ),
            (
                "bio.pgen.storage_mode".to_string(),
                format!("0x{:02x}", fileset.mode.byte()),
            ),
            (
                "bio.pgen.index".to_string(),
                if fileset.pgi_path.is_some() {
                    "external"
                } else {
                    "embedded"
                }
                .to_string(),
            ),
            (
                "bio.pgen.specification_baseline".to_string(),
                PGEN_SPEC_BASELINE.to_string(),
            ),
        ]),
    )))
}

fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    match projection {
        Some(indices) => Ok(Arc::new(Schema::new_with_metadata(
            indices
                .iter()
                .map(|&index| {
                    schema.fields().get(index).cloned().ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "PGEN projection column index {index} is out of bounds"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            schema.metadata().clone(),
        ))),
        None => Ok(schema.clone()),
    }
}

fn plan_payload_partitions(
    selected: &[usize],
    fileset: &PgenFileset,
    target_partitions: usize,
    max_gap: u64,
    max_range_bytes: u64,
) -> Result<Vec<PgenPartition>> {
    let selected_ranges = selected
        .iter()
        .map(|&index| {
            let record = fileset.records.record(index)?;
            ByteRange::new(record.offset, record.end()).map(|range| (index, range))
        })
        .collect::<Result<Vec<_>>>()?;
    let assignments = contiguous_byte_partitions(&selected_ranges, target_partitions)?;
    assignments
        .into_iter()
        .map(|assignment| {
            let owned = assignment
                .iter()
                .map(|(index, _)| *index)
                .collect::<Vec<_>>();
            let mut required = owned.iter().copied().collect::<HashSet<_>>();
            for &index in &owned {
                if let Some(base) = fileset.records.record(index)?.ld_base {
                    required.insert(base);
                }
            }
            let mut required = required.into_iter().collect::<Vec<_>>();
            required.sort_unstable();
            let required_ranges = required
                .iter()
                .map(|&index| {
                    let record = fileset.records.record(index)?;
                    Ok(ByteRange {
                        start: record.offset,
                        end: record.end(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let ranges =
                coalesce_byte_ranges(required_ranges.into_iter(), max_gap, max_range_bytes)?;
            Ok(PgenPartition {
                owned,
                required,
                ranges,
            })
        })
        .collect()
}

fn contiguous_byte_partitions(
    ranges: &[(usize, ByteRange)],
    target_partitions: usize,
) -> Result<Vec<Vec<(usize, ByteRange)>>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let partition_count = target_partitions.max(1).min(ranges.len());
    let mut remaining_bytes = ranges.iter().try_fold(0_u64, |total, (_, range)| {
        total.checked_add(range.len()).ok_or_else(|| {
            DataFusionError::Plan("PGEN selected payload byte count overflowed".to_string())
        })
    })?;
    let mut cursor = 0;
    let mut partitions = Vec::with_capacity(partition_count);
    for partition_index in 0..partition_count {
        let remaining_partitions = partition_count - partition_index;
        let max_take = ranges.len() - cursor - (remaining_partitions - 1);
        let target_bytes = remaining_bytes.div_ceil(remaining_partitions as u64);
        let mut bytes = 0_u64;
        let mut take = 0;
        while take < max_take && (take == 0 || bytes < target_bytes) {
            bytes = bytes
                .checked_add(ranges[cursor + take].1.len())
                .ok_or_else(|| {
                    DataFusionError::Plan("PGEN partition byte count overflowed".to_string())
                })?;
            take += 1;
        }
        partitions.push(ranges[cursor..cursor + take].to_vec());
        cursor += take;
        remaining_bytes -= bytes;
    }
    debug_assert_eq!(cursor, ranges.len());
    Ok(partitions)
}

fn plan_metadata_partitions(selected: &[usize], target_partitions: usize) -> Vec<PgenPartition> {
    let partition_count = target_partitions.max(1).min(selected.len());
    let chunk_size = selected.len().div_ceil(partition_count);
    selected
        .chunks(chunk_size)
        .map(|variants| PgenPartition {
            owned: variants.to_vec(),
            required: Vec::new(),
            ranges: Vec::new(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fileset::RecordInfo;

    #[test]
    fn plans_ld_dependencies_without_transferring_ownership() {
        let fileset = PgenFileset {
            pgen_path: String::new(),
            pvar_path: String::new(),
            psam_path: String::new(),
            pgi_path: None,
            source: crate::source::ObjectAccess::Local(String::new()),
            variants: Arc::new(Vec::new()),
            selected_samples: datafusion_bio_format_core::genotype::resolve_samples(
                &[],
                None,
                MissingSamplePolicy::Error,
            )
            .unwrap(),
            selected_identities: Arc::new(Vec::new()),
            sample_count: 0,
            records: Arc::new(crate::fileset::RecordIndex::explicit(vec![
                RecordInfo {
                    offset: 100,
                    length: 10,
                    record_type: 0,
                    ld_base: None,
                },
                RecordInfo {
                    offset: 110,
                    length: 4,
                    record_type: 2,
                    ld_base: Some(0),
                },
            ])),
            mode: crate::fileset::PgenMode::Variable,
            companion_bytes: 0,
            header_bytes: 0,
        };
        let partitions = plan_payload_partitions(&[1], &fileset, 4, 0, 1024).unwrap();
        assert_eq!(partitions[0].owned, vec![1]);
        assert_eq!(partitions[0].required, vec![0, 1]);
    }

    #[test]
    fn preserves_locality_while_balancing_equal_records() {
        let ranges = (0..8)
            .map(|index| {
                (
                    index,
                    ByteRange {
                        start: index as u64 * 10,
                        end: (index as u64 + 1) * 10,
                    },
                )
            })
            .collect::<Vec<_>>();
        let partitions = contiguous_byte_partitions(&ranges, 4).unwrap();
        assert_eq!(
            partitions
                .iter()
                .map(|partition| {
                    partition
                        .iter()
                        .map(|(index, _)| *index)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
            vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6, 7]]
        );
    }
}
