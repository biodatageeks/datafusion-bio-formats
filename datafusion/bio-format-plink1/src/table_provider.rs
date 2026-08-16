use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::fileset::{BED_HEADER_LEN, PlinkFileset};
use crate::filter::{evaluate_exact_filter, supports_exact_filter};
use crate::physical_exec::{PlinkExec, PlinkPartition, PlinkReadRange};
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
    CoordinateSystem, GENOTYPE_COUNTED_ALLELE_KEY, GENOTYPE_OUTPUT_MODE_KEY, GenotypeMetric,
    GenotypeScanMetrics, MissingSamplePolicy, PredicateGuarantee, can_push_limit_below_filters,
};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_core::range_planning::{
    ByteRange, balance_byte_ranges, coalesce_byte_ranges,
};

/// Field metadata containing selected original FAM `(FID, IID)` pairs as JSON.
pub const PLINK_SAMPLE_IDENTITIES_KEY: &str = "bio.plink1.sample_identities";

/// Policy used to construct selectable sample names from FAM rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SampleIdMode {
    /// Use IID alone and reject duplicate IIDs.
    #[default]
    Iid,
    /// Use the collision-free escaped representation `FID:IID`.
    FidIid,
}

/// Read and planning options for a PLINK 1 fileset.
#[derive(Debug, Clone)]
pub struct PlinkReadOptions {
    /// Explicit BIM location. The shared-basename `.bim` is used when absent.
    pub bim_path: Option<String>,
    /// Explicit FAM location. The shared-basename `.fam` is used when absent.
    pub fam_path: Option<String>,
    /// Requested sample names in output order, or all samples when absent.
    pub samples: Option<Vec<String>>,
    /// Behavior for requested samples absent from FAM.
    pub missing_sample_policy: MissingSamplePolicy,
    /// FAM identifier policy.
    pub sample_id_mode: SampleIdMode,
    /// Output coordinate presentation.
    pub coordinate_system: CoordinateSystem,
    /// Credentials and transport configuration for remote objects.
    pub object_storage_options: Option<ObjectStorageOptions>,
    /// Maximum bytes accepted for either text companion.
    pub max_companion_bytes: usize,
    /// Maximum accepted number of BIM variants.
    pub max_variants: usize,
    /// Maximum accepted number of FAM samples.
    pub max_samples: usize,
    /// Maximum unselected byte gap bridged by one BED range.
    pub max_range_gap: u64,
    /// Maximum size of a coalesced BED range.
    pub max_range_bytes: u64,
    /// Soft target for genotype bytes in one RecordBatch.
    pub batch_soft_byte_limit: usize,
}

impl Default for PlinkReadOptions {
    fn default() -> Self {
        Self {
            bim_path: None,
            fam_path: None,
            samples: None,
            missing_sample_policy: MissingSamplePolicy::Error,
            sample_id_mode: SampleIdMode::Iid,
            coordinate_system: CoordinateSystem::ZeroBasedHalfOpen,
            object_storage_options: None,
            max_companion_bytes: 256 * 1024 * 1024,
            max_variants: 100_000_000,
            max_samples: 10_000_000,
            max_range_gap: 0,
            max_range_bytes: 8 * 1024 * 1024,
            batch_soft_byte_limit: 64 * 1024 * 1024,
        }
    }
}

/// Read-only DataFusion table provider for a PLINK 1 BED/BIM/FAM fileset.
#[derive(Clone, Debug)]
pub struct PlinkTableProvider {
    fileset: Arc<PlinkFileset>,
    schema: SchemaRef,
    options: PlinkReadOptions,
}

impl PlinkTableProvider {
    /// Opens and validates a local or remote PLINK 1 fileset.
    pub async fn try_new(bed_path: impl Into<String>, options: PlinkReadOptions) -> Result<Self> {
        if options.max_companion_bytes == 0 {
            return Err(DataFusionError::Plan(
                "max_companion_bytes must be greater than zero".to_string(),
            ));
        }
        if options.max_range_bytes == 0 {
            return Err(DataFusionError::Plan(
                "max_range_bytes must be greater than zero".to_string(),
            ));
        }
        if options.batch_soft_byte_limit == 0 {
            return Err(DataFusionError::Plan(
                "batch_soft_byte_limit must be greater than zero".to_string(),
            ));
        }
        let fileset = Arc::new(PlinkFileset::open(bed_path.into(), &options).await?);
        let schema = build_schema(&fileset, options.coordinate_system)?;
        Ok(Self {
            fileset,
            schema,
            options,
        })
    }

    /// Returns the resolved BIM companion location.
    pub fn bim_path(&self) -> &str {
        &self.fileset.bim_path
    }

    /// Returns the resolved FAM companion location.
    pub fn fam_path(&self) -> &str {
        &self.fileset.fam_path
    }

    /// Returns selected sample names in emitted list order.
    pub fn sample_names(&self) -> &[String] {
        self.fileset.selected_samples.names()
    }
}

#[async_trait]
impl TableProvider for PlinkTableProvider {
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
        let guarantees: Vec<_> = filters
            .iter()
            .map(|filter| {
                if supports_exact_filter(filter) {
                    PredicateGuarantee::Exact
                } else {
                    PredicateGuarantee::Unsupported
                }
            })
            .collect();
        let exact_filters: Vec<_> = filters
            .iter()
            .filter(|filter| supports_exact_filter(filter))
            .collect();
        let mut selected: Vec<usize> = self
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
            .collect();
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

        let needs_genotypes = projection
            .map(|indices| indices.contains(&7))
            .unwrap_or(true)
            && !self.fileset.selected_samples.is_empty();
        let partitions = if needs_genotypes {
            plan_payload_partitions(
                &selected,
                self.fileset.bytes_per_variant,
                state.config().target_partitions(),
                self.options.max_range_gap,
                self.options.max_range_bytes,
            )?
        } else {
            plan_metadata_partitions(&selected, state.config().target_partitions())
        };
        let metrics = Arc::new(GenotypeScanMetrics::default());
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
        if !needs_genotypes {
            metrics.add(GenotypeMetric::PayloadsSkipped, selected.len() as u64);
        }

        let partition_count = partitions.len();
        Ok(Arc::new(PlinkExec {
            fileset: self.fileset.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            partitions: Arc::new(partitions),
            batch_soft_byte_limit: self.options.batch_soft_byte_limit,
            metrics,
            cache: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partition_count),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
}

fn build_schema(fileset: &PlinkFileset, coordinates: CoordinateSystem) -> Result<SchemaRef> {
    let gt = Field::new(
        "GT",
        DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
        false,
    )
    .with_metadata(HashMap::from([
        (GENOTYPE_COUNTED_ALLELE_KEY.to_string(), "A1".to_string()),
        (
            GENOTYPE_OUTPUT_MODE_KEY.to_string(),
            "a1_dosage".to_string(),
        ),
    ]));
    let selected_identities = serde_json::to_string(fileset.selected_identities.as_ref())
        .map_err(|error| DataFusionError::Plan(format!("serialize FAM identities: {error}")))?;
    let mut genotype_metadata = fileset.selected_samples.field_metadata();
    genotype_metadata.insert(PLINK_SAMPLE_IDENTITIES_KEY.to_string(), selected_identities);
    let genotypes = Field::new("genotypes", DataType::Struct(Fields::from(vec![gt])), false)
        .with_metadata(genotype_metadata);

    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt64, false),
            Field::new("end", DataType::UInt64, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("cm", DataType::Float64, false),
            Field::new("a1", DataType::Utf8, false),
            Field::new("a2", DataType::Utf8, false),
            genotypes,
        ],
        HashMap::from([(
            COORDINATE_SYSTEM_METADATA_KEY.to_string(),
            coordinates.metadata_value().to_string(),
        )]),
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
                            "PLINK projection column index {index} is out of bounds"
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
    bytes_per_variant: u64,
    target_partitions: usize,
    max_gap: u64,
    max_range_size: u64,
) -> Result<Vec<PlinkPartition>> {
    let ranges = selected
        .iter()
        .map(|&index| variant_byte_range(index, bytes_per_variant))
        .collect::<Result<Vec<_>>>()?;
    let ranges = coalesce_byte_ranges(ranges, max_gap, max_range_size)?;
    let balanced = balance_byte_ranges(ranges, target_partitions);
    Ok(balanced
        .into_iter()
        .map(|partition| {
            let ranges = partition
                .ranges
                .into_iter()
                .map(|range| PlinkReadRange {
                    range,
                    variants: selected
                        .iter()
                        .copied()
                        .filter(|&index| {
                            variant_byte_range(index, bytes_per_variant).is_ok_and(|variant| {
                                variant.start >= range.start && variant.end <= range.end
                            })
                        })
                        .collect(),
                })
                .collect();
            PlinkPartition {
                variants: Vec::new(),
                ranges,
            }
        })
        .collect())
}

fn plan_metadata_partitions(selected: &[usize], target_partitions: usize) -> Vec<PlinkPartition> {
    let partition_count = target_partitions.max(1).min(selected.len());
    let chunk_size = selected.len().div_ceil(partition_count);
    selected
        .chunks(chunk_size)
        .map(|variants| PlinkPartition {
            variants: variants.to_vec(),
            ranges: Vec::new(),
        })
        .collect()
}

fn variant_byte_range(index: usize, bytes_per_variant: u64) -> Result<ByteRange> {
    let index = u64::try_from(index)
        .map_err(|_| DataFusionError::Plan("PLINK variant index does not fit u64".to_string()))?;
    let start = index
        .checked_mul(bytes_per_variant)
        .and_then(|offset| offset.checked_add(BED_HEADER_LEN))
        .ok_or_else(|| {
            DataFusionError::Plan("PLINK BED offset arithmetic overflowed".to_string())
        })?;
    let end = start.checked_add(bytes_per_variant).ok_or_else(|| {
        DataFusionError::Plan("PLINK BED range arithmetic overflowed".to_string())
    })?;
    ByteRange::new(start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesces_adjacent_variants_and_owns_every_selection_once() {
        let partitions = plan_payload_partitions(&[0, 1, 3], 2, 2, 0, 8).unwrap();
        assert_eq!(partitions.len(), 2);
        let mut selected: Vec<_> = partitions
            .iter()
            .flat_map(|partition| partition.ranges.iter())
            .flat_map(|range| range.variants.iter().copied())
            .collect();
        selected.sort_unstable();
        assert_eq!(selected, vec![0, 1, 3]);

        let mut ranges: Vec<_> = partitions
            .iter()
            .flat_map(|partition| partition.ranges.iter().map(|range| range.range))
            .collect();
        ranges.sort_unstable();
        assert_eq!(
            ranges,
            vec![
                ByteRange::new(3, 7).unwrap(),
                ByteRange::new(9, 11).unwrap()
            ]
        );
    }

    #[test]
    fn bounded_gap_can_coalesce_sparse_variants() {
        let partitions = plan_payload_partitions(&[0, 2], 2, 1, 2, 8).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].ranges.len(), 1);
        assert_eq!(partitions[0].ranges[0].range, ByteRange::new(3, 9).unwrap());
        assert_eq!(partitions[0].ranges[0].variants, vec![0, 2]);
    }
}
