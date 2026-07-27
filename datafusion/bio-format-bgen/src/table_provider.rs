use std::any::Any;
use std::collections::HashMap;
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
    CoordinateSystem, GENOTYPE_COUNTED_ALLELE_KEY, GENOTYPE_OUTPUT_MODE_KEY,
    GENOTYPE_SOURCE_BIT_PRECISION_KEY, GENOTYPE_STATE_ORDER_KEY, GenotypeMetric,
    GenotypeScanMetrics, MissingSamplePolicy, PredicateGuarantee, can_push_limit_below_filters,
    resolve_samples,
};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_core::range_planning::{
    ByteRange, balance_byte_ranges, coalesce_byte_ranges,
};

use crate::bgi::{BgiIndex, open_optional_bgi};
use crate::catalog::{BgenCatalog, build_transient_catalog};
use crate::filter::{evaluate_exact_filter, supports_exact_filter};
use crate::header::{BgenHeader, BgenLayout};
use crate::physical_exec::{BgenExec, BgenPartition, BgenReadRange};
use crate::source::ObjectAccess;

/// Schema metadata marking generated ordinal sample names.
pub const BGEN_SAMPLE_NAMES_SYNTHETIC_KEY: &str = "bio.bgen.sample_names.synthetic";

/// Genotype values emitted by the BGEN provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BgenOutputMode {
    /// Preserve every format-defined probability state.
    #[default]
    Probability,
    /// Emit expected copies of encoded allele index one for biallelic variants.
    Dosage,
}

/// Policy for an inconsistent optional BGI.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StaleBgiPolicy {
    /// Ignore an inconsistent conventionally discovered BGI and use a transient catalog.
    #[default]
    Ignore,
    /// Fail planning when the BGI is inconsistent.
    Error,
}

/// BGEN read and resource-limit options.
#[derive(Debug, Clone)]
pub struct BgenReadOptions {
    /// Probability-preserving or biallelic-dosage output.
    pub output_mode: BgenOutputMode,
    /// Explicit external sample file used only when IDs are not embedded.
    pub sample_path: Option<String>,
    /// Explicit BGI location.
    pub bgi_path: Option<String>,
    /// Requested sample names in output order.
    pub samples: Option<Vec<String>>,
    /// Behavior for absent requested samples.
    pub missing_sample_policy: MissingSamplePolicy,
    /// Output coordinate presentation.
    pub coordinate_system: CoordinateSystem,
    /// Remote object-store settings.
    pub object_storage_options: Option<ObjectStorageOptions>,
    /// Policy for an inconsistent BGI.
    pub stale_bgi_policy: StaleBgiPolicy,
    /// Maximum declared variants.
    pub max_variants: usize,
    /// Maximum declared samples.
    pub max_samples: usize,
    /// Maximum bytes in an embedded or external sample block.
    pub max_sample_block_bytes: usize,
    /// Maximum bytes in the BGEN header, including free data.
    pub max_header_bytes: usize,
    /// Maximum UTF-8 identifier or allele length.
    pub max_string_bytes: usize,
    /// Maximum alleles in one variant.
    pub max_alleles: usize,
    /// Maximum metadata bytes accepted for one variant.
    pub max_variant_metadata_bytes: usize,
    /// Maximum decompressed probability block size.
    pub max_decompressed_block_bytes: usize,
    /// Maximum probability states reconstructed for one sample.
    pub max_states_per_sample: usize,
    /// Maximum BGI object size.
    pub max_bgi_bytes: usize,
    /// Maximum shared remote BGI cache size.
    pub max_bgi_cache_bytes: usize,
    /// Optional local directory for the shared remote BGI cache.
    pub bgi_cache_directory: Option<String>,
    /// Maximum gap bridged by a coalesced BGEN payload range.
    pub max_range_gap: u64,
    /// Maximum coalesced BGEN payload range size.
    pub max_range_bytes: u64,
    /// Soft genotype bytes per output batch.
    pub batch_soft_byte_limit: usize,
}

impl Default for BgenReadOptions {
    fn default() -> Self {
        Self {
            output_mode: BgenOutputMode::Probability,
            sample_path: None,
            bgi_path: None,
            samples: None,
            missing_sample_policy: MissingSamplePolicy::Error,
            coordinate_system: CoordinateSystem::ZeroBasedHalfOpen,
            object_storage_options: None,
            stale_bgi_policy: StaleBgiPolicy::Ignore,
            max_variants: 100_000_000,
            max_samples: 10_000_000,
            max_sample_block_bytes: 256 * 1024 * 1024,
            max_header_bytes: 64 * 1024 * 1024,
            max_string_bytes: 16 * 1024 * 1024,
            max_alleles: 65_535,
            max_variant_metadata_bytes: 64 * 1024 * 1024,
            max_decompressed_block_bytes: 512 * 1024 * 1024,
            max_states_per_sample: 10_000_000,
            max_bgi_bytes: 4 * 1024 * 1024 * 1024,
            max_bgi_cache_bytes: 8 * 1024 * 1024 * 1024,
            bgi_cache_directory: None,
            max_range_gap: 0,
            max_range_bytes: 16 * 1024 * 1024,
            batch_soft_byte_limit: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BgenFileset {
    pub(crate) path: String,
    pub(crate) source: ObjectAccess,
    pub(crate) header: Arc<BgenHeader>,
    pub(crate) catalog: BgenCatalog,
    pub(crate) bgi: Option<BgiIndex>,
    pub(crate) selected_samples: datafusion_bio_format_core::genotype::SampleSelection,
    pub(crate) options: BgenReadOptions,
}

/// Read-only DataFusion table provider for BGEN 1.2/1.3 files.
#[derive(Clone, Debug)]
pub struct BgenTableProvider {
    fileset: Arc<BgenFileset>,
    schema: SchemaRef,
}

impl BgenTableProvider {
    /// Opens a local or remote BGEN file and builds its validated metadata catalog.
    pub async fn try_new(path: impl Into<String>, options: BgenReadOptions) -> Result<Self> {
        validate_options(&options)?;
        let path = path.into();
        let storage_options = options.object_storage_options.clone().unwrap_or_default();
        let source = ObjectAccess::open(&path, &storage_options).await?;
        let header = Arc::new(BgenHeader::read(&path, &source, &options).await?);
        let selected_samples = resolve_samples(
            &header.sample_names,
            options.samples.as_deref(),
            options.missing_sample_policy,
        )?;
        let catalog = build_transient_catalog(&path, &source, &header, &options).await?;
        let bgi = open_optional_bgi(&path, &source, &header, &catalog, &options).await?;
        let fileset = Arc::new(BgenFileset {
            path,
            source,
            header,
            catalog,
            bgi,
            selected_samples,
            options,
        });
        let schema = build_schema(&fileset)?;
        Ok(Self { fileset, schema })
    }

    /// Returns selected sample names in emitted list order.
    pub fn sample_names(&self) -> &[String] {
        self.fileset.selected_samples.names()
    }
}

#[async_trait]
impl TableProvider for BgenTableProvider {
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
        let candidates = self
            .fileset
            .bgi
            .as_ref()
            .map(|index| {
                index.candidate_indices(&exact_filters, self.fileset.options.coordinate_system)
            })
            .transpose()?
            .unwrap_or_else(|| (0..self.fileset.catalog.variants.len()).collect());
        let mut selected: Vec<usize> = candidates
            .into_iter()
            .filter(|&index| {
                let variant = &self.fileset.catalog.variants[index];
                exact_filters
                    .iter()
                    .all(|filter| evaluate_exact_filter(variant, filter))
            })
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

        let projected_names: std::collections::HashSet<_> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let genotype_projected = projected_names.contains("genotypes");
        let needs_payload = projected_names.contains("phased")
            || projected_names.contains("bits")
            || (genotype_projected && !self.fileset.selected_samples.is_empty());
        let partitions = if needs_payload {
            plan_payload_partitions(
                &selected,
                &self.fileset.catalog,
                state.config().target_partitions(),
                self.fileset.options.max_range_gap,
                self.fileset.options.max_range_bytes,
            )?
        } else {
            plan_metadata_partitions(&selected, state.config().target_partitions())
        };
        let metrics = Arc::new(GenotypeScanMetrics::default());
        metrics.add(
            GenotypeMetric::PrimaryBytesRead,
            self.fileset.header.header_bytes_read
                + self.fileset.catalog.bytes_read
                + self
                    .fileset
                    .bgi
                    .as_ref()
                    .map_or(0, |index| index.primary_bytes_read),
        );
        metrics.add(
            GenotypeMetric::CompanionBytesRead,
            self.fileset
                .bgi
                .as_ref()
                .map_or(0, |index| index.bytes_read),
        );
        metrics.add(
            GenotypeMetric::MetadataCandidates,
            self.fileset.catalog.variants.len() as u64,
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
        Ok(Arc::new(BgenExec {
            fileset: self.fileset.clone(),
            schema: schema.clone(),
            partitions: Arc::new(partitions),
            metrics,
            batch_soft_byte_limit: self.fileset.options.batch_soft_byte_limit,
            cache: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partition_count),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
}

fn validate_options(options: &BgenReadOptions) -> Result<()> {
    for (name, value) in [
        ("max_sample_block_bytes", options.max_sample_block_bytes),
        ("max_header_bytes", options.max_header_bytes),
        ("max_string_bytes", options.max_string_bytes),
        (
            "max_variant_metadata_bytes",
            options.max_variant_metadata_bytes,
        ),
        (
            "max_decompressed_block_bytes",
            options.max_decompressed_block_bytes,
        ),
        ("max_states_per_sample", options.max_states_per_sample),
        ("max_bgi_bytes", options.max_bgi_bytes),
        ("max_bgi_cache_bytes", options.max_bgi_cache_bytes),
        ("batch_soft_byte_limit", options.batch_soft_byte_limit),
    ] {
        if value == 0 {
            return Err(DataFusionError::Plan(format!(
                "{name} must be greater than zero"
            )));
        }
    }
    if options.max_alleles == 0 || options.max_alleles > u16::MAX as usize {
        return Err(DataFusionError::Plan(
            "max_alleles must be in 1..=65535".to_string(),
        ));
    }
    if options.max_range_bytes == 0 {
        return Err(DataFusionError::Plan(
            "max_range_bytes must be greater than zero".to_string(),
        ));
    }
    if options
        .bgi_cache_directory
        .as_ref()
        .is_some_and(|path| path.is_empty())
    {
        return Err(DataFusionError::Plan(
            "bgi_cache_directory must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn build_schema(fileset: &BgenFileset) -> Result<SchemaRef> {
    let sample_metadata = fileset.selected_samples.field_metadata();
    let ploidy = Field::new(
        "PLOIDY",
        DataType::List(Arc::new(Field::new("item", DataType::UInt8, false))),
        false,
    );
    let genotype_child = match fileset.options.output_mode {
        BgenOutputMode::Probability => Field::new(
            "GP",
            DataType::List(Arc::new(Field::new(
                "sample",
                DataType::List(Arc::new(Field::new("state", DataType::Float32, false))),
                true,
            ))),
            false,
        )
        .with_metadata(HashMap::from([
            (
                GENOTYPE_STATE_ORDER_KEY.to_string(),
                "unphased=colex allele-count vectors; phased=haplotype-major then encoded allele order"
                    .to_string(),
            ),
            (
                GENOTYPE_SOURCE_BIT_PRECISION_KEY.to_string(),
                "per_variant_column:bits".to_string(),
            ),
            (
                GENOTYPE_OUTPUT_MODE_KEY.to_string(),
                "probability".to_string(),
            ),
        ])),
        BgenOutputMode::Dosage => Field::new(
            "DS",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            false,
        )
        .with_metadata(HashMap::from([
            (
                GENOTYPE_COUNTED_ALLELE_KEY.to_string(),
                "alleles[1]".to_string(),
            ),
            (GENOTYPE_OUTPUT_MODE_KEY.to_string(), "dosage".to_string()),
        ])),
    };
    let genotypes = Field::new(
        "genotypes",
        DataType::Struct(Fields::from(vec![genotype_child, ploidy])),
        false,
    )
    .with_metadata(sample_metadata);
    let fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt64, false),
        Field::new("end", DataType::UInt64, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("rsid", DataType::Utf8, true),
        Field::new(
            "alleles",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            false,
        ),
        Field::new("phased", DataType::Boolean, false),
        Field::new("bits", DataType::UInt8, false),
        genotypes,
    ];
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([
            (
                COORDINATE_SYSTEM_METADATA_KEY.to_string(),
                fileset
                    .options
                    .coordinate_system
                    .metadata_value()
                    .to_string(),
            ),
            (
                BGEN_SAMPLE_NAMES_SYNTHETIC_KEY.to_string(),
                fileset.header.synthetic_sample_names.to_string(),
            ),
            (
                "bio.bgen.layout".to_string(),
                match fileset.header.layout {
                    BgenLayout::Layout1 => "1",
                    BgenLayout::Layout2 => "2",
                }
                .to_string(),
            ),
            (
                "bio.bgen.index".to_string(),
                if fileset.bgi.is_some() {
                    "bgi"
                } else {
                    "transient"
                }
                .to_string(),
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
                            "BGEN projection column index {index} is out of bounds"
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
    catalog: &BgenCatalog,
    target_partitions: usize,
    max_gap: u64,
    max_range_bytes: u64,
) -> Result<Vec<BgenPartition>> {
    let ranges = selected
        .iter()
        .map(|&index| {
            let variant = &catalog.variants[index];
            ByteRange::new(
                variant.payload_offset,
                variant
                    .payload_offset
                    .checked_add(variant.payload_size)
                    .ok_or_else(|| {
                        DataFusionError::Plan("BGEN payload range overflowed".to_string())
                    })?,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let coalesced = coalesce_byte_ranges(ranges, max_gap, max_range_bytes)?;
    Ok(balance_byte_ranges(coalesced, target_partitions)
        .into_iter()
        .map(|partition| BgenPartition {
            variants: Vec::new(),
            ranges: partition
                .ranges
                .into_iter()
                .map(|range| BgenReadRange {
                    range,
                    variants: selected
                        .iter()
                        .copied()
                        .filter(|&index| {
                            let variant = &catalog.variants[index];
                            variant.payload_offset >= range.start
                                && variant
                                    .payload_offset
                                    .checked_add(variant.payload_size)
                                    .is_some_and(|end| end <= range.end)
                        })
                        .collect(),
                })
                .collect(),
        })
        .collect())
}

fn plan_metadata_partitions(selected: &[usize], target_partitions: usize) -> Vec<BgenPartition> {
    let partition_count = target_partitions.max(1).min(selected.len());
    let chunk_size = selected.len().div_ceil(partition_count);
    selected
        .chunks(chunk_size)
        .map(|variants| BgenPartition {
            variants: variants.to_vec(),
            ranges: Vec::new(),
        })
        .collect()
}
