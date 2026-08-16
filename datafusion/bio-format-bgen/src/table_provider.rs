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
    ByteRange, coalesce_byte_ranges, partition_byte_ranges_in_order,
};

use datafusion_bio_format_core::companion::sanitize_location;
use log::debug;

use crate::bgi::{BgiIndex, open_optional_bgi};
use crate::buffers::{BufferLayout, GenotypeBuffers};
use crate::catalog::{
    BgenCatalog, BgenVariant, PAYLOAD_FRAMING_BYTES, ResolveOutcome, build_transient_catalog,
    catalog_from_index, resolve_variant, try_resolve_variant,
};
use crate::decode::{DecodeScratch, complete_probability_count, decode_variant};
use crate::filter::{evaluate_exact_filter, references_id, supports_exact_filter};
use crate::header::{BgenHeader, BgenLayout};
use crate::physical_exec::{BgenExec, BgenPartition, BgenReadRange};
use crate::source::ObjectAccess;

/// Schema metadata marking generated ordinal sample names.
pub const BGEN_SAMPLE_NAMES_SYNTHETIC_KEY: &str = "bio.bgen.sample_names.synthetic";

/// Coalesced payload ranges aimed at each partition.
///
/// Aiming for one range per partition cannot balance them, because a payload is
/// indivisible: see [`plan_payload_partitions`]. Four bounds the error at about
/// one range, a quarter of a partition's share, and costs at most four object
/// reads per partition.
const PAYLOAD_RANGES_PER_PARTITION: u64 = 4;

/// Smallest range size the partition split will ask for.
///
/// Below this, splitting trades balance for object-store requests, which is a
/// poor trade against remote storage: a small file scanned at a high partition
/// count would otherwise be cut into ranges far under any sensible read size.
/// Measured on a 4.9 MB slice at eight partitions, this floor keeps most of the
/// available balance — 4.65x against 5.09x for a 128 KiB floor — at half the
/// requests. An explicit `max_range_bytes` below this still wins, because the
/// caller asked for it.
const MIN_PAYLOAD_RANGE_BYTES: u64 = 256 * 1024;

/// Genotype values emitted by the BGEN provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BgenOutputMode {
    /// Preserve every format-defined probability state.
    #[default]
    Probability,
    /// Emit expected copies of encoded allele index one for biallelic variants.
    Dosage,
}

/// Arrow layout used for probability output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BgenProbabilityLayout {
    /// One variable-length list per sample.
    ///
    /// Always valid, including for a file whose variants store different
    /// numbers of probability states.
    #[default]
    Nested,
    /// One fixed-width list per sample.
    ///
    /// Drops the per-sample list offsets, which are a quarter of the emitted
    /// bytes for a diploid biallelic cohort.
    ///
    /// The width covers the widest sample the catalog allows, and a narrower
    /// sample is padded with NaN — including a missing sample, whose slots are
    /// never read through Arrow but must exist because a fixed-size list sizes
    /// its values buffer from the entry count. Padding is decided per sample,
    /// so a file that mixes widths, and even a variant that declares a variable
    /// ploidy, are both representable. A sample storing more states than the
    /// width is rejected; use [`Self::Nested`] for such a file.
    Fixed,
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
    /// Arrow layout used when `output_mode` is [`BgenOutputMode::Probability`].
    pub probability_layout: BgenProbabilityLayout,
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
    /// Check every index row against the record it describes when opening.
    ///
    /// Opening an indexed BGEN normally checks the object's size, its
    /// identifying prefix, the row count, and that the rows tile the variant
    /// region; each row's contents are then checked against its record when a
    /// scan reads it. That leaves one gap: predicates are pushed into the index,
    /// so a row whose recorded chromosome, position or RS identifier is stale
    /// can be pruned before its record is ever read, and the query silently
    /// omits a variant that does match.
    ///
    /// Closing that gap means reading every variant's metadata at open, which is
    /// what the index exists to avoid — for a large object it is the difference
    /// between a few kilobytes and the whole file. So it is off by default and
    /// available to callers who would rather pay for it than trust the index.
    pub verify_index_records: bool,
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
    ///
    /// Consecutive probability blocks are separated by the next variant's
    /// metadata, so a zero gap budget would issue one object read per variant.
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
            probability_layout: BgenProbabilityLayout::Nested,
            sample_path: None,
            bgi_path: None,
            samples: None,
            missing_sample_policy: MissingSamplePolicy::Error,
            coordinate_system: CoordinateSystem::ZeroBasedHalfOpen,
            object_storage_options: None,
            stale_bgi_policy: StaleBgiPolicy::Ignore,
            verify_index_records: false,
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
            max_range_gap: 64 * 1024,
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
    /// Probability shape when the fixed layout is in use.
    pub(crate) probability_shape: Option<ProbeShape>,
    /// Payload bytes read to resolve [`Self::probability_shape`].
    pub(crate) probability_probe_bytes: u64,
    /// Bytes the width probe decompressed.
    pub(crate) probability_probe_decompressed: u64,
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
        // The index is opened first because it already holds every variant's
        // location and metadata. Walking the object to rebuild that would
        // duplicate the index, and since metadata and genotype payloads are
        // interleaved, the walk pulls the payloads with it — so opening a table
        // could download the whole file before a single query ran.
        let mut bgi = open_optional_bgi(&path, &source, &header, &options).await?;
        // Building the catalog validates the index's rows one by one — their
        // coordinates, allele counts and record ranges — and that happens after
        // the index has been opened. A discovered index whose rows turn out to
        // be unusable is stale in exactly the sense the policy governs, so it
        // follows the same rule rather than failing the table outright. An index
        // the caller named explicitly always reports why it cannot be used.
        //
        // The index is dropped along with its catalog: its rows are positional,
        // so pushing predicates through it against a walked catalog would
        // resolve to the wrong variants.
        let mut catalog = None;
        if let Some(index) = &bgi {
            // No bytes of the object go into building this catalog: the index
            // supplied it, and the prefix its identity check read is already
            // reported against the index itself.
            match catalog_from_index(&path, &index.variants, &header, &options, 0) {
                Ok(built) => catalog = Some(built),
                Err(error)
                    if options.bgi_path.is_none()
                        && options.stale_bgi_policy == StaleBgiPolicy::Ignore =>
                {
                    debug!(
                        "BGEN {}: ignoring the discovered index and walking the object: {error}",
                        sanitize_location(&path)
                    );
                    bgi = None;
                }
                Err(error) => return Err(error),
            }
        }
        let bgi = bgi;
        let catalog = match catalog {
            Some(catalog) => catalog,
            None => build_transient_catalog(&path, &source, &header, &options).await?,
        };
        // Opting into full verification walks the object once and checks every
        // row against its record. The walked catalog is kept afterwards: it is
        // already resolved, so nothing downstream has to read records again.
        let catalog = if bgi.is_some() && options.verify_index_records {
            let walked = build_transient_catalog(&path, &source, &header, &options).await?;
            verify_index_against_records(&path, &catalog, &walked)?;
            walked
        } else {
            catalog
        };
        // The fixed layout puts the state count in the schema, so it has to be
        // known before any batch is produced. One variant's block header carries
        // it; every other variant is checked against it while scanning.
        let (probability_shape, probability_probe_bytes, probability_probe_decompressed) =
            if options.output_mode == BgenOutputMode::Probability
                && options.probability_layout == BgenProbabilityLayout::Fixed
            {
                let (mut shape, bytes, decompressed) =
                    probe_probability_width(&path, &source, &header, &catalog, &options).await?;
                // One width is in play from here on: the schema and the scan
                // buffers both read `shape.width`, so neither has to know
                // whether it came from the probe or from the catalog.
                shape.width = derive_fixed_width(&path, &catalog, shape, &options)?;
                (Some(shape), bytes, decompressed)
            } else {
                (None, 0, 0)
            };
        let fileset = Arc::new(BgenFileset {
            path,
            source,
            header,
            catalog,
            bgi,
            selected_samples,
            options,
            probability_shape,
            probability_probe_bytes,
            probability_probe_decompressed,
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
        let candidate_count = candidates.len();
        // A variant identifier lives only in the record, never in the index, so
        // a predicate naming it needs those records parsed before the filter can
        // be applied exactly. Only their metadata is read, not their payloads.
        let mut metadata_cost = MetadataReadCost::default();
        let mut resolved = if exact_filters.iter().any(|filter| references_id(filter)) {
            let (resolved, cost) = resolve_variant_metadata(
                &self.fileset.path,
                &self.fileset.source,
                &self.fileset.header,
                &self.fileset.options,
                &self.fileset.catalog,
                &candidates,
                &HashMap::new(),
            )
            .await?;
            metadata_cost = cost;
            resolved
        } else {
            HashMap::new()
        };
        let mut selected: Vec<usize> = candidates
            .into_iter()
            .filter(|&index| {
                let variant = resolved
                    .get(&index)
                    .unwrap_or(&self.fileset.catalog.variants[index]);
                exact_filters
                    .iter()
                    .all(|filter| evaluate_exact_filter(variant, filter))
            })
            .collect();
        let filter_rejections = (candidate_count - selected.len()) as u64;
        if can_push_limit_below_filters(&guarantees)
            && let Some(limit) = limit
        {
            selected.truncate(limit);
        }
        // An all-rejected scan still reports what the filters removed, so it
        // yields an empty BGEN partition rather than an EmptyExec that would
        // drop the metrics.
        let all_rejected = selected.is_empty();

        let projected_names: std::collections::HashSet<_> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let genotype_projected = projected_names.contains("genotypes");
        let needs_payload = projected_names.contains("phased")
            || projected_names.contains("bits")
            || (genotype_projected && !self.fileset.selected_samples.is_empty());
        // Every variant column is served from the record, never from the index.
        //
        // `id` and a multiallelic variant's full allele list are only in the
        // record to begin with. The rest — chromosome, coordinates, RS
        // identifier — the index does carry, but emitting its copy would hand
        // back an index's values as though they were the object's, and a row
        // whose record is never read is a row whose index entry is never
        // checked. A scan that reads payloads picks all of this up on the way;
        // one that does not reads the records' metadata for it.
        let needs_record_metadata = !needs_payload
            && projected_names
                .iter()
                .any(|name| matches!(*name, "chrom" | "start" | "end" | "id" | "rsid" | "alleles"));
        if needs_record_metadata {
            let (records, cost) = resolve_variant_metadata(
                &self.fileset.path,
                &self.fileset.source,
                &self.fileset.header,
                &self.fileset.options,
                &self.fileset.catalog,
                &selected,
                &resolved,
            )
            .await?;
            resolved.extend(records);
            metadata_cost.bytes += cost.bytes;
            metadata_cost.requests += cost.requests;
        }

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
        // Planning yields nothing when every candidate was filtered out, so one
        // empty partition keeps the plan valid and its counters reportable.
        let partitions = if all_rejected {
            vec![BgenPartition {
                variants: Vec::new(),
                ranges: Vec::new(),
            }]
        } else {
            partitions
        };
        let metrics = Arc::new(GenotypeScanMetrics::default());
        metrics.add(
            GenotypeMetric::PrimaryBytesRead,
            self.fileset.header.header_bytes_read
                + self.fileset.catalog.bytes_read
                + self.fileset.probability_probe_bytes
                + self
                    .fileset
                    .bgi
                    .as_ref()
                    .map_or(0, |index| index.primary_bytes_read)
                + metadata_cost.bytes,
        );
        metrics.add(GenotypeMetric::RangeRequests, metadata_cost.requests);
        metrics.add(
            GenotypeMetric::CompanionBytesRead,
            self.fileset
                .bgi
                .as_ref()
                .map_or(0, |index| index.bytes_read)
                + self.fileset.header.companion_sample_bytes,
        );
        // Coalescing is a planning outcome, so it is counted once from the plan.
        // Counting it per read during execution would only restate
        // RangeRequests, which is incremented in the same loop.
        metrics.add(
            GenotypeMetric::CoalescedRanges,
            partitions
                .iter()
                .map(|partition| partition.ranges.len() as u64)
                .sum(),
        );
        metrics.add(
            GenotypeMetric::MetadataCandidates,
            self.fileset.catalog.variants.len() as u64,
        );
        metrics.add(GenotypeMetric::SelectedVariants, selected.len() as u64);
        metrics.add(GenotypeMetric::ExactFilterRejections, filter_rejections);
        // The width probe decompresses one block, so it is counted like any
        // other payload the scan reads.
        metrics.add(
            GenotypeMetric::CompressedBytes,
            self.fileset.probability_probe_bytes,
        );
        metrics.add(
            GenotypeMetric::DecompressedBytes,
            self.fileset.probability_probe_decompressed,
        );
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
            resolved: Arc::new(resolved),
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

/// Reads the probability state count from the first variant's block.
///
/// Decoding with an empty sample selection reads the block header without
/// reconstructing any sample, so this costs one block, not one scan.
/// What variant 0 says about the file's probability shape.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProbeShape {
    /// States per sample the fixed layout emits.
    ///
    /// This starts as variant 0's own width and is widened by
    /// [`derive_fixed_width`] to cover every catalog variant.
    pub(crate) width: usize,
    ploidy: u8,
    phased: bool,
}

/// Widest sample any catalog variant can store, given the shape variant 0
/// declares.
///
/// A Layout 2 block header lives inside the compressed payload, so learning
/// every variant's exact width would mean decompressing the whole file at plan
/// time. Allele counts are already in the catalog, and ploidy and phasing are
/// constant across a file in practice, so the widest state count follows from
/// the probe plus the catalog at no I/O cost. A sample that turns out to store
/// more than this is rejected during the scan rather than silently truncated.
///
/// # Assumption
///
/// Ploidy and phasing are taken from variant 0 and applied to every variant.
/// The format permits both to vary per variant, and even per sample within a
/// variant, so a file whose later variants are more ploid than its first — a
/// diploid autosome followed by a triploid call, say — derives a width too
/// narrow for them. That does not corrupt output: the scan rejects a sample
/// storing more states than the schema declares, naming the variant. Only the
/// allele count, which the catalog holds for every variant, widens the estimate
/// here; ploidy cannot, because it is inside the payload.
///
/// The remedy for such a file is [`BgenProbabilityLayout::Nested`], which needs
/// no single width.
///
/// The derived width is checked against `max_states_per_sample` here rather than
/// only per decoded variant. Every emitted sample is padded to this width, so a
/// query filtered to the narrow variants of a file that also holds a very wide
/// one would allocate the padding without the widest variant ever being decoded
/// — the per-variant check would never run.
fn derive_fixed_width(
    path: &str,
    catalog: &BgenCatalog,
    shape: ProbeShape,
    options: &BgenReadOptions,
) -> Result<usize> {
    let mut width = shape.width as u64;
    for variant in catalog.variants.iter() {
        width = width.max(complete_probability_count(
            shape.ploidy,
            variant.allele_count,
            shape.phased,
        )?);
    }
    if width > options.max_states_per_sample as u64 {
        return Err(DataFusionError::Plan(format!(
            "BGEN {} needs a fixed probability width of {width} to cover its widest variant, \
             exceeding max_states_per_sample {}; use the nested probability layout",
            sanitize_location(path),
            options.max_states_per_sample
        )));
    }
    usize::try_from(width)
        .map_err(|_| DataFusionError::Plan("BGEN probability width does not fit usize".to_string()))
}

async fn probe_probability_width(
    path: &str,
    source: &ObjectAccess,
    header: &BgenHeader,
    catalog: &BgenCatalog,
    options: &BgenReadOptions,
) -> Result<(ProbeShape, u64, u64)> {
    let variant = catalog.variants.first().ok_or_else(|| {
        DataFusionError::Plan(
            "BGEN fixed probability layout needs at least one variant to determine its width"
                .to_string(),
        )
    })?;
    // An indexed variant knows where its record starts but not where the
    // payload does, so the record is read and parsed first. It is one record.
    let (variant, payload, bytes_fetched) = match variant.payload_span() {
        Some(payload) => {
            let payload = source.read_range(path, payload).await?;
            let fetched = payload.len() as u64;
            (variant.as_ref().clone(), payload, fetched)
        }
        None => {
            let record = source.read_range(path, variant.record_span()).await?;
            let resolved = resolve_variant(path, variant, &record, header, options)?;
            let payload = resolved.payload_span().ok_or_else(|| {
                DataFusionError::Internal(
                    "BGEN variant stayed unresolved after being parsed".to_string(),
                )
            })?;
            let start = usize::try_from(payload.start.saturating_sub(resolved.record_offset))
                .map_err(|_| {
                    DataFusionError::Internal("BGEN payload offset does not fit usize".to_string())
                })?;
            let end = usize::try_from(payload.end.saturating_sub(resolved.record_offset)).map_err(
                |_| DataFusionError::Internal("BGEN payload end does not fit usize".to_string()),
            )?;
            let fetched = record.len() as u64;
            (resolved, record.slice(start..end), fetched)
        }
    };
    let variant = &variant;
    let mut scratch = DecodeScratch::new();
    // The probe selects no sample, so nothing is written into these.
    let mut buffers = GenotypeBuffers::new(BufferLayout::NestedProbability);
    let decoded = decode_variant(
        path,
        variant,
        header,
        &payload,
        &[],
        options,
        &mut scratch,
        &mut buffers,
    )?;
    if let Some(states) = decoded.state_width
        && states > options.max_states_per_sample
    {
        return Err(DataFusionError::Plan(format!(
            "BGEN {} variant 0 has {states} probability states, exceeding \
             max_states_per_sample {}",
            sanitize_location(path),
            options.max_states_per_sample
        )));
    }
    let variable_ploidy = || {
        DataFusionError::Plan(format!(
            "BGEN {} variant 0 declares a variable ploidy, which has no single probability width; \
             use the nested probability layout",
            sanitize_location(path)
        ))
    };
    let shape = ProbeShape {
        width: decoded.state_width.ok_or_else(variable_ploidy)?,
        ploidy: decoded.declared_ploidy.ok_or_else(variable_ploidy)?,
        phased: decoded.phased,
    };
    Ok((shape, bytes_fetched, decoded.decompressed_bytes as u64))
}

/// Converts a state count to Arrow's fixed-size list width.
///
/// Arrow stores that width as an `i32`, so a state count beyond its range would
/// wrap to a negative width rather than fail.
fn fixed_list_width(width: usize) -> Result<i32> {
    i32::try_from(width).map_err(|_| {
        DataFusionError::Plan(format!(
            "BGEN probability width {width} exceeds the fixed-size list limit; \
             use the nested probability layout"
        ))
    })
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
                match fileset.probability_shape.map(|shape| shape.width) {
                    // A fixed-width sample list needs no per-sample offsets.
                    Some(width) => DataType::FixedSizeList(
                        Arc::new(Field::new("state", DataType::Float32, false)),
                        fixed_list_width(width)?,
                    ),
                    None => {
                        DataType::List(Arc::new(Field::new("state", DataType::Float32, false)))
                    }
                },
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

/// Bytes read per record when only its metadata is wanted.
///
/// Variant metadata is a few dozen bytes for a typical biallelic record, so this
/// covers it many times over while staying far below any real genotype payload.
/// A record whose metadata runs past it is re-read in full.
const METADATA_PROBE_BYTES: u64 = 4 * 1024;

/// Checks every index-derived variant against the record walked from the object.
///
/// Used only when a caller asks for it: this is the check that costs a walk of
/// the whole object, and the reason the index is otherwise trusted for the
/// fields it records.
fn verify_index_against_records(
    path: &str,
    indexed: &BgenCatalog,
    walked: &BgenCatalog,
) -> Result<()> {
    if indexed.variants.len() != walked.variants.len() {
        return Err(DataFusionError::Plan(format!(
            "BGEN {}: the index describes {} variants, the object has {}",
            sanitize_location(path),
            indexed.variants.len(),
            walked.variants.len()
        )));
    }
    for (row, record) in indexed.variants.iter().zip(walked.variants.iter()) {
        let mismatch = |field: &str| {
            Err(DataFusionError::Plan(format!(
                "BGEN {} variant {} at byte {}: the index's {field} does not match the record; \
                 the index does not describe this object",
                sanitize_location(path),
                record.index,
                record.record_offset
            )))
        };
        if row.record_offset != record.record_offset || row.record_size != record.record_size {
            return mismatch("record range");
        }
        if row.chrom != record.chrom {
            return mismatch("chromosome");
        }
        if row.position != record.position {
            return mismatch("position");
        }
        if row.rsid != record.rsid {
            return mismatch("RS identifier");
        }
        if row.allele_count != record.allele_count {
            return mismatch("allele count");
        }
        if record.alleles[..row.alleles.len()] != row.alleles[..] {
            return mismatch("alleles");
        }
    }
    Ok(())
}

/// Object reads spent resolving variant metadata during planning.
///
/// These happen before the scan's own counters exist, so they are carried back
/// and folded in; otherwise a query that filters or projects a field the index
/// lacks would report reads it never made.
#[derive(Debug, Default, Clone, Copy)]
struct MetadataReadCost {
    bytes: u64,
    requests: u64,
}

/// Re-reads one variant's metadata with a growing prefix until it parses.
///
/// The prefix doubles rather than jumping to the whole record: a record's
/// payload can be hundreds of megabytes, and reading it to recover a long
/// identifier would give up exactly the bound this path exists to keep. The
/// configured metadata ceiling stops the growth, so a record that never parses
/// fails on its own limit instead of reading without end.
async fn widen_metadata_probe(
    path: &str,
    source: &ObjectAccess,
    variant: &BgenVariant,
    header: &BgenHeader,
    options: &BgenReadOptions,
    from: u64,
    cost: &mut MetadataReadCost,
) -> Result<BgenVariant> {
    let record = variant.record_span();
    let ceiling = (options.max_variant_metadata_bytes as u64)
        .saturating_add(PAYLOAD_FRAMING_BYTES)
        .min(record.end.saturating_sub(record.start));
    let mut probe = from;
    loop {
        probe = probe.saturating_mul(2).min(ceiling);
        let bytes = source
            .read_range(path, record.start..record.start.saturating_add(probe))
            .await?;
        cost.requests += 1;
        cost.bytes += bytes.len() as u64;
        match try_resolve_variant(path, variant, &bytes, header, options)? {
            ResolveOutcome::Resolved(variant) => return Ok(variant),
            ResolveOutcome::NeedMore if probe < ceiling => continue,
            // At the ceiling the record itself is the problem, and
            // `resolve_variant` reports it against the configured limit.
            ResolveOutcome::NeedMore => {
                return resolve_variant(path, variant, &bytes, header, options);
            }
        }
    }
}

/// Parses the metadata of indexed variants without reading their payloads.
///
/// A BGI records neither the variant identifier nor the alleles past the
/// second, so a query that filters or projects those has to read the records
/// themselves. Only the front of each record is fetched, and neighbouring reads
/// are coalesced under the configured gap budget, so this costs the metadata
/// rather than the payloads sitting behind it.
///
/// Returns the variants this call resolved, by catalog index, and what reading
/// them cost, so the scan's counters include the object reads planning made.
/// Variants the catalog already knows in full, and those already in `known`,
/// are absent: the caller has them either way.
///
/// # Precondition
///
/// `indices` must be ascending by `catalog.variants[index].record_offset`. Both
/// call sites satisfy it — index candidates arrive ordered by
/// `file_start_position`, a walked catalog is built front to back, and filtering
/// preserves order — and one cursor walks the variants alongside the coalesced
/// ranges on that basis. Out-of-order input would leave variants unvisited,
/// which the count check at the end reports rather than passing over.
async fn resolve_variant_metadata(
    path: &str,
    source: &ObjectAccess,
    header: &BgenHeader,
    options: &BgenReadOptions,
    catalog: &BgenCatalog,
    indices: &[usize],
    known: &HashMap<usize, Arc<BgenVariant>>,
) -> Result<(HashMap<usize, Arc<BgenVariant>>, MetadataReadCost)> {
    let mut resolved = HashMap::new();
    let mut cost = MetadataReadCost::default();
    // Records another pass already read are not read again: a query that both
    // filters on a field the index lacks and projects one resolves the same
    // candidates twice, and the second pass should cost nothing.
    let pending: Vec<usize> = indices
        .iter()
        .copied()
        .filter(|&index| !catalog.variants[index].is_resolved() && !known.contains_key(&index))
        .collect();
    if pending.is_empty() {
        return Ok((resolved, cost));
    }

    debug_assert!(
        pending
            .windows(2)
            .all(|pair| catalog.variants[pair[0]].record_offset
                <= catalog.variants[pair[1]].record_offset),
        "resolve_variant_metadata needs indices in record order"
    );
    let probe = METADATA_PROBE_BYTES
        .min(options.max_variant_metadata_bytes as u64)
        .max(1);
    let ranges = pending
        .iter()
        .map(|&index| {
            let record = catalog.variants[index].record_span();
            ByteRange::new(
                record.start,
                record.start.saturating_add(probe).min(record.end),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    // Only touching prefixes are merged, never ones with a gap between them.
    //
    // `max_range_gap` is a budget for bridging the metadata that separates two
    // genotype payloads, which is a few dozen bytes. Here the gaps are the
    // payloads themselves, so spending that budget would merge straight across
    // them and pull a dense file down almost whole to answer a metadata query —
    // the same thing that made opening a table read the object. Records packed
    // closer than the probe still coalesce, because then the bytes in between
    // are ones the probe would have read anyway.
    let coalesced = coalesce_byte_ranges(ranges, 0, options.max_range_bytes)?;

    // Both the coalesced ranges and the pending variants are in offset order, so
    // one cursor walks them together instead of rescanning the variants for
    // every range.
    let mut next = 0;
    for range in coalesced {
        let bytes = source.read_range(path, range.start..range.end).await?;
        cost.requests += 1;
        cost.bytes += bytes.len() as u64;
        while let Some(&index) = pending.get(next) {
            let variant = &catalog.variants[index];
            let start = variant.record_offset;
            if start >= range.end {
                break;
            }
            let offset = usize::try_from(start.saturating_sub(range.start)).map_err(|_| {
                DataFusionError::Internal("BGEN record offset does not fit usize".to_string())
            })?;
            let slice = bytes.get(offset..).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "BGEN variant {index} is outside the range read for its metadata"
                ))
            })?;
            let variant = match try_resolve_variant(path, variant, slice, header, options)? {
                ResolveOutcome::Resolved(variant) => variant,
                // Metadata longer than the probe covers. Widen it rather than
                // reading the record, whose payload is the thing this whole
                // path exists to avoid downloading.
                ResolveOutcome::NeedMore => {
                    widen_metadata_probe(path, source, variant, header, options, probe, &mut cost)
                        .await?
                }
            };
            resolved.insert(index, Arc::new(variant));
            next += 1;
        }
    }
    if next != pending.len() {
        return Err(DataFusionError::Internal(format!(
            "BGEN resolved {next} of {} variant records",
            pending.len()
        )));
    }
    Ok((resolved, cost))
}

fn plan_payload_partitions(
    selected: &[usize],
    catalog: &BgenCatalog,
    target_partitions: usize,
    max_gap: u64,
    max_range_bytes: u64,
) -> Result<Vec<BgenPartition>> {
    // A variant parsed from the object contributes just its payload. One known
    // only from the index contributes its whole record, because the metadata in
    // front of the payload is what says where the payload starts — a few dozen
    // bytes immediately before bytes the scan is fetching anyway.
    let ranges = selected
        .iter()
        .map(|&index| {
            let span = catalog.variants[index].scan_span();
            ByteRange::new(span.start, span.end)
        })
        .collect::<Result<Vec<_>>>()?;
    // Bridging the metadata gaps between payloads would otherwise merge a whole
    // small file into one range and leave every partition but the first empty,
    // so cap the coalesced size by a split of the selected payload bytes.
    //
    // The cap aims for several ranges per partition rather than one. A payload
    // is indivisible, so a cap of exactly one partition's share hands the scan
    // `target_partitions + 1` chunks, and `target + 1` chunks never divide
    // evenly into `target` partitions: one partition always takes two and
    // becomes the bottleneck. Measured on a 4.9 MB slice, that capped the
    // eight-partition speedup at 3.64x with the busiest partition holding 21.8%
    // of the bytes against an ideal 12.5%.
    //
    // The split uses the requested partition count, while the plan ends up with
    // `min(target_partitions, coalesced.len())` partitions. Asking for many more
    // partitions than there are payload ranges therefore caps more tightly than
    // strictly necessary, which costs a few extra object reads and never
    // correctness; sizing it from the final count is not possible before
    // coalescing has decided that count.
    let payload_bytes: u64 = ranges
        .iter()
        .map(|range| range.len())
        .fold(0, u64::saturating_add);
    let partitions = target_partitions.max(1) as u64;
    // A single-partition scan has no imbalance to correct, so splitting it finer
    // would only add object-store round trips to a path that reads the whole
    // selection sequentially anyway.
    let chunks = if partitions > 1 {
        partitions.saturating_mul(PAYLOAD_RANGES_PER_PARTITION)
    } else {
        1
    };
    // The floor must never cost a partition its work: a file smaller than the
    // floor would otherwise coalesce into one range and leave every partition
    // but the first empty, which is the collapse the cap exists to prevent. So
    // the floor applies only up to one partition's share.
    let partition_cap = payload_bytes
        .div_ceil(chunks)
        .max(MIN_PAYLOAD_RANGE_BYTES)
        .min(payload_bytes.div_ceil(partitions))
        .max(1);
    let coalesced = coalesce_byte_ranges(ranges, max_gap, max_range_bytes.min(partition_cap))?;

    // Coalesced ranges are sorted and disjoint, so each payload can only fall in
    // the last range that starts at or before it. Locating that range by binary
    // search assigns every variant in one pass; rescanning all variants for each
    // range is quadratic and dominates planning on whole-chromosome files.
    let mut range_variants: Vec<Vec<usize>> = vec![Vec::new(); coalesced.len()];
    for &index in selected {
        let span = catalog.variants[index].scan_span();
        // Coalescing only merges ranges built from these same bounds, so every
        // span is contained in exactly one coalesced range. Failing loudly keeps
        // a future coalescing bug from silently dropping variants from the scan.
        let position = coalesced
            .partition_point(|range| range.start <= span.start)
            .checked_sub(1)
            .filter(|&position| span.end <= coalesced[position].end)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "BGEN variant {index} span {}..{} is not inside any coalesced range",
                    span.start, span.end
                ))
            })?;
        range_variants[position].push(index);
    }

    // Partitions must stay contiguous so a scan reproduces source variant order
    // no matter how many partitions the session requests.
    Ok(
        partition_byte_ranges_in_order(coalesced.iter().copied(), target_partitions)
            .into_iter()
            .map(|partition| BgenPartition {
                variants: Vec::new(),
                ranges: partition
                    .ranges
                    .into_iter()
                    .map(|range| {
                        // Partitioning only groups the coalesced ranges, and their
                        // starts are unique, so each range maps back to one entry.
                        let variants = coalesced
                            .binary_search_by_key(&range.start, |candidate| candidate.start)
                            .ok()
                            .map(|position| std::mem::take(&mut range_variants[position]))
                            .unwrap_or_default();
                        BgenReadRange { range, variants }
                    })
                    .collect(),
            })
            .collect(),
    )
}

fn plan_metadata_partitions(selected: &[usize], target_partitions: usize) -> Vec<BgenPartition> {
    if selected.is_empty() {
        return Vec::new();
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{BgenVariant, VariantDetail};

    /// A catalog of contiguous payloads whose sizes vary the way real ones do.
    ///
    /// Uniform sizes are degenerate here: they divide exactly into any partition
    /// count, so every split looks perfect and the imbalance never appears.
    fn contiguous_catalog(count: usize, payload_size: u64) -> BgenCatalog {
        let mut offset = 0;
        let variants = (0..count)
            .map(|index| {
                let payload_size = payload_size + (index as u64 * 37) % 100;
                let payload_offset = offset;
                offset += payload_size;
                Arc::new(BgenVariant {
                    index,
                    rsid: None,
                    chrom: "1".to_string(),
                    start: index as u64,
                    end: index as u64 + 1,
                    position: index as u32 + 1,
                    alleles: vec!["A".to_string(), "C".to_string()],
                    allele_count: 2,
                    record_offset: payload_offset,
                    record_size: payload_size,
                    detail: VariantDetail::Parsed {
                        id: None,
                        payload_offset,
                        payload_size,
                    },
                })
            })
            .collect::<Vec<_>>();
        BgenCatalog {
            variants: Arc::new(variants),
            bytes_read: 0,
        }
    }

    /// Share of the payload bytes held by the busiest partition.
    fn heaviest_share(partitions: &[BgenPartition]) -> f64 {
        let bytes: Vec<u64> = partitions
            .iter()
            .map(|partition| {
                partition
                    .ranges
                    .iter()
                    .map(|planned| planned.range.end - planned.range.start)
                    .sum()
            })
            .collect();
        let total: u64 = bytes.iter().sum();
        bytes.iter().max().copied().unwrap_or(0) as f64 / total as f64
    }

    #[test]
    fn payload_partitions_are_balanced() {
        // A payload cannot be split across ranges, so capping a range at exactly
        // one partition's byte share hands the scan `target + 1` indivisible
        // chunks — and one partition always takes two of them. At two
        // partitions that is an 87/13 split, which caps the speedup at 1.15x no
        // matter how fast the decoder is.
        // Aiming for `PAYLOAD_RANGES_PER_PARTITION` ranges per partition leaves
        // the busiest one at most a single extra range, so its share stays near
        // `(k + 1) / k` of a fair share. The bound below is that ratio with a
        // little room; the failure it guards against is the old behaviour, where
        // the busiest partition held 1.75x its share and up to 87% of the bytes.
        let catalog = contiguous_catalog(1_000, 5_000);
        let selected: Vec<usize> = (0..1_000).collect();
        let tolerance =
            (PAYLOAD_RANGES_PER_PARTITION as f64 + 1.0) / PAYLOAD_RANGES_PER_PARTITION as f64;
        for target in [2_usize, 4, 8] {
            let partitions =
                plan_payload_partitions(&selected, &catalog, target, 64 * 1024, 16 * 1024 * 1024)
                    .unwrap();
            let share = heaviest_share(&partitions);
            let fair = 1.0 / target as f64;
            assert!(
                share <= fair * tolerance + f64::EPSILON,
                "target {target}: busiest partition holds {:.1}% of the payload bytes, \
                 more than {:.1}% — {tolerance:.2}x its {:.1}% fair share",
                share * 100.0,
                fair * tolerance * 100.0,
                fair * 100.0
            );
        }
    }

    #[test]
    fn a_file_below_the_range_floor_still_fills_its_partitions() {
        // The floor stops the split asking for object reads far under a sensible
        // size, but it must not do that by starving partitions: a file smaller
        // than the floor would coalesce into a single range and leave every
        // partition but the first empty, which is exactly the collapse the cap
        // exists to prevent.
        let payload_size = 200;
        let count = 64;
        let catalog = contiguous_catalog(count, payload_size);
        assert!(
            (count as u64) * payload_size < MIN_PAYLOAD_RANGE_BYTES,
            "this fixture only tests the floor while it stays below it"
        );
        let selected: Vec<usize> = (0..count).collect();
        for target in [2_usize, 4, 8] {
            let partitions =
                plan_payload_partitions(&selected, &catalog, target, 64 * 1024, 16 * 1024 * 1024)
                    .unwrap();
            assert_eq!(
                partitions.len(),
                target,
                "a {} byte payload lost partitions to the floor at target {target}",
                (count as u64) * payload_size
            );
        }
    }

    #[test]
    fn the_range_floor_bounds_reads_once_partitions_are_fed() {
        // Above the floor the split aims for PAYLOAD_RANGES_PER_PARTITION ranges
        // per partition rather than splitting without bound, so a scan issues a
        // handful of object reads per partition instead of one per payload.
        let catalog = contiguous_catalog(25_000, 195);
        let selected: Vec<usize> = (0..25_000).collect();
        let partitions =
            plan_payload_partitions(&selected, &catalog, 8, 64 * 1024, 16 * 1024 * 1024).unwrap();
        let ranges: usize = partitions
            .iter()
            .map(|partition| partition.ranges.len())
            .sum();
        // 4.9 MB over eight partitions puts the 256 KiB floor in charge, so the
        // plan lands near 19 ranges. The window allows for payload-size variance
        // without being so wide that a regression could hide inside it.
        assert!(
            (10..=28).contains(&ranges),
            "expected roughly 19 ranges for 8 partitions, got {ranges}"
        );
    }

    #[test]
    fn a_single_partition_scan_is_not_split_finer() {
        // With one partition there is no imbalance to correct, so aiming for
        // several ranges would only add object-store round trips to a path that
        // reads its whole selection sequentially.
        let catalog = contiguous_catalog(25_000, 195);
        let selected: Vec<usize> = (0..25_000).collect();
        let partitions =
            plan_payload_partitions(&selected, &catalog, 1, 64 * 1024, 16 * 1024 * 1024).unwrap();
        let ranges: usize = partitions
            .iter()
            .map(|partition| partition.ranges.len())
            .sum();
        assert_eq!(
            ranges, 1,
            "a 4.9 MB contiguous selection fits one coalesced range under the \
             16 MiB limit, so a single-partition scan should issue one read"
        );
    }
}
