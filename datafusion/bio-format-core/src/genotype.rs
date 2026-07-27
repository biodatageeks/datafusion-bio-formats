//! Shared logical contracts for genotype table providers.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::TableProviderFilterPushDown;

pub use crate::metadata::{
    GENOTYPE_ALLELE_ORDER_KEY, GENOTYPE_COUNTED_ALLELE_KEY, GENOTYPE_OUTPUT_MODE_KEY,
    GENOTYPE_SAMPLE_NAMES_KEY, GENOTYPE_SOURCE_BIT_PRECISION_KEY, GENOTYPE_STATE_ORDER_KEY,
};

/// Coordinate presentation used by a genotype table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinateSystem {
    /// One-based, fully closed coordinates.
    OneBasedClosed,
    /// Zero-based, half-open coordinates.
    ZeroBasedHalfOpen,
}

impl CoordinateSystem {
    /// Creates a coordinate system from the existing boolean read option.
    pub fn from_zero_based(zero_based: bool) -> Self {
        if zero_based {
            Self::ZeroBasedHalfOpen
        } else {
            Self::OneBasedClosed
        }
    }

    /// Returns the value used by the shared Arrow coordinate metadata key.
    pub fn metadata_value(self) -> &'static str {
        match self {
            Self::OneBasedClosed => "false",
            Self::ZeroBasedHalfOpen => "true",
        }
    }

    /// Converts a one-based site position into output start and end coordinates.
    pub fn site(self, one_based_position: u64) -> Result<SiteCoordinates> {
        if one_based_position == 0 {
            return Err(DataFusionError::Plan(
                "genotype positions must be one-based positive integers".to_string(),
            ));
        }

        Ok(match self {
            Self::OneBasedClosed => SiteCoordinates {
                start: one_based_position,
                end: one_based_position,
            },
            Self::ZeroBasedHalfOpen => SiteCoordinates {
                start: one_based_position - 1,
                end: one_based_position,
            },
        })
    }
}

/// Start and end coordinates for a single genomic site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SiteCoordinates {
    /// Site start in the requested coordinate system.
    pub start: u64,
    /// Site end in the requested coordinate system.
    pub end: u64,
}

/// Behavior when a requested sample name is absent from the source.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MissingSamplePolicy {
    /// Return a planning error for the first absent requested sample.
    #[default]
    Error,
    /// Omit absent requested samples from the resolved selection.
    Ignore,
}

/// A resolved ordered selection of source samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampleSelection {
    source_indices: Vec<usize>,
    names: Vec<String>,
}

impl SampleSelection {
    /// Returns physical source indices in output order.
    pub fn source_indices(&self) -> &[usize] {
        &self.source_indices
    }

    /// Returns final selected sample names in output order.
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Returns true when the explicit or resolved selection contains no samples.
    pub fn is_empty(&self) -> bool {
        self.source_indices.is_empty()
    }

    /// Serializes selected sample names into Arrow field metadata.
    pub fn field_metadata(&self) -> HashMap<String, String> {
        HashMap::from([(
            GENOTYPE_SAMPLE_NAMES_KEY.to_string(),
            serde_json::to_string(&self.names)
                .expect("serializing a string vector to JSON cannot fail"),
        )])
    }
}

/// Resolves requested sample names to physical source indices.
///
/// A missing `requested` value selects every source sample. Repeated requested
/// names are de-duplicated at first occurrence, and request order is preserved.
pub fn resolve_samples(
    source_samples: &[String],
    requested: Option<&[String]>,
    missing_policy: MissingSamplePolicy,
) -> Result<SampleSelection> {
    let mut source_by_name = HashMap::with_capacity(source_samples.len());
    for (index, name) in source_samples.iter().enumerate() {
        if source_by_name.insert(name.as_str(), index).is_some() {
            return Err(DataFusionError::Plan(format!(
                "source sample name is ambiguous: {name}"
            )));
        }
    }

    let Some(requested) = requested else {
        return Ok(SampleSelection {
            source_indices: (0..source_samples.len()).collect(),
            names: source_samples.to_vec(),
        });
    };

    let mut seen = HashSet::with_capacity(requested.len());
    let mut source_indices = Vec::with_capacity(requested.len());
    let mut names = Vec::with_capacity(requested.len());

    for name in requested {
        if !seen.insert(name.as_str()) {
            continue;
        }

        match source_by_name.get(name.as_str()) {
            Some(&index) => {
                source_indices.push(index);
                names.push(name.clone());
            }
            None if missing_policy == MissingSamplePolicy::Ignore => {}
            None => {
                return Err(DataFusionError::Plan(format!(
                    "requested sample is absent from the source: {name}"
                )));
            }
        }
    }

    Ok(SampleSelection {
        source_indices,
        names,
    })
}

/// A resolved ordered selection of format-specific genotype fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenotypeFieldSelection {
    source_indices: Vec<usize>,
    names: Vec<String>,
}

impl GenotypeFieldSelection {
    /// Returns physical field indices in output order.
    pub fn source_indices(&self) -> &[usize] {
        &self.source_indices
    }

    /// Returns selected field names in output order.
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Returns true when no genotype fields were selected.
    pub fn is_empty(&self) -> bool {
        self.source_indices.is_empty()
    }
}

/// Resolves requested genotype fields against the format-supported fields.
///
/// A missing `requested` value selects every available field. Unlike samples,
/// an unknown genotype field is always a planning error.
pub fn resolve_genotype_fields(
    available_fields: &[String],
    requested: Option<&[String]>,
) -> Result<GenotypeFieldSelection> {
    let mut available_by_name = HashMap::with_capacity(available_fields.len());
    for (index, name) in available_fields.iter().enumerate() {
        if available_by_name.insert(name.as_str(), index).is_some() {
            return Err(DataFusionError::Plan(format!(
                "available genotype field name is ambiguous: {name}"
            )));
        }
    }

    let Some(requested) = requested else {
        return Ok(GenotypeFieldSelection {
            source_indices: (0..available_fields.len()).collect(),
            names: available_fields.to_vec(),
        });
    };

    let mut seen = HashSet::with_capacity(requested.len());
    let mut source_indices = Vec::with_capacity(requested.len());
    let mut names = Vec::with_capacity(requested.len());

    for name in requested {
        if !seen.insert(name.as_str()) {
            continue;
        }

        let Some(&index) = available_by_name.get(name.as_str()) else {
            let available = available_fields.join(", ");
            return Err(DataFusionError::Plan(format!(
                "unsupported genotype field {name}; available fields: {available}"
            )));
        };
        source_indices.push(index);
        names.push(name.clone());
    }

    Ok(GenotypeFieldSelection {
        source_indices,
        names,
    })
}

/// Provider guarantee for a pushed filter expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateGuarantee {
    /// The provider evaluates the complete expression.
    Exact,
    /// Provider pruning returns a candidate superset requiring validation.
    Inexact,
    /// The provider does not evaluate the expression.
    Unsupported,
}

impl PredicateGuarantee {
    /// Converts the guarantee to DataFusion's table-provider declaration.
    pub fn as_datafusion(self) -> TableProviderFilterPushDown {
        match self {
            Self::Exact => TableProviderFilterPushDown::Exact,
            Self::Inexact => TableProviderFilterPushDown::Inexact,
            Self::Unsupported => TableProviderFilterPushDown::Unsupported,
        }
    }
}

/// Returns true when a scan limit can be placed below all supplied predicates.
///
/// Only exact provider-owned predicates permit this optimization. Inexact and
/// unsupported expressions require candidate validation or a residual filter.
pub fn can_push_limit_below_filters(guarantees: &[PredicateGuarantee]) -> bool {
    guarantees
        .iter()
        .all(|guarantee| *guarantee == PredicateGuarantee::Exact)
}

/// Counter exposed by genotype scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum GenotypeMetric {
    /// Bytes read from the primary genotype object.
    PrimaryBytesRead,
    /// Bytes read from companion metadata or indexes.
    CompanionBytesRead,
    /// Physical range requests issued.
    RangeRequests,
    /// Selected ranges after coalescing.
    CoalescedRanges,
    /// Compressed bytes processed.
    CompressedBytes,
    /// Decompressed bytes produced.
    DecompressedBytes,
    /// Metadata rows considered before pruning.
    MetadataCandidates,
    /// Variants selected for payload processing.
    SelectedVariants,
    /// Variants emitted to Arrow batches.
    EmittedVariants,
    /// Genotype payloads skipped by projection.
    PayloadsSkipped,
    /// Samples requested by the scan.
    SamplesRequested,
    /// Sample values decoded by the scan.
    SamplesDecoded,
    /// Sample values skipped during decoding.
    SampleValuesSkipped,
    /// Dependency records decoded without being emitted.
    DependencyRecords,
    /// Candidate rows rejected by exact record-level filtering.
    ExactFilterRejections,
    /// RecordBatches emitted.
    Batches,
    /// Rows emitted across all batches.
    BatchRows,
    /// Estimated genotype bytes appended to output batches.
    GenotypeBytes,
}

impl GenotypeMetric {
    const COUNT: usize = 18;

    /// All counters in stable declaration order.
    pub const ALL: [Self; Self::COUNT] = [
        Self::PrimaryBytesRead,
        Self::CompanionBytesRead,
        Self::RangeRequests,
        Self::CoalescedRanges,
        Self::CompressedBytes,
        Self::DecompressedBytes,
        Self::MetadataCandidates,
        Self::SelectedVariants,
        Self::EmittedVariants,
        Self::PayloadsSkipped,
        Self::SamplesRequested,
        Self::SamplesDecoded,
        Self::SampleValuesSkipped,
        Self::DependencyRecords,
        Self::ExactFilterRejections,
        Self::Batches,
        Self::BatchRows,
        Self::GenotypeBytes,
    ];
}

/// Thread-safe counters shared by genotype scan partitions.
#[derive(Debug)]
pub struct GenotypeScanMetrics {
    counters: [AtomicU64; GenotypeMetric::COUNT],
}

impl Default for GenotypeScanMetrics {
    fn default() -> Self {
        Self {
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl GenotypeScanMetrics {
    /// Adds `value` to a counter using relaxed atomic ordering.
    pub fn add(&self, metric: GenotypeMetric, value: u64) {
        self.counters[metric as usize].fetch_add(value, Ordering::Relaxed);
    }

    /// Returns the current value of a counter.
    pub fn value(&self, metric: GenotypeMetric) -> u64 {
        self.counters[metric as usize].load(Ordering::Relaxed)
    }

    /// Returns a stable snapshot of every counter.
    pub fn snapshot(&self) -> [(GenotypeMetric, u64); GenotypeMetric::COUNT] {
        std::array::from_fn(|index| {
            let metric = GenotypeMetric::ALL[index];
            (metric, self.value(metric))
        })
    }
}

/// Tracks output batch limits for wide genotype rows.
#[derive(Debug, Clone)]
pub struct GenotypeBatchSizer {
    max_rows: usize,
    soft_byte_limit: usize,
    rows: usize,
    estimated_bytes: usize,
}

impl GenotypeBatchSizer {
    /// Creates a sizer with nonzero row and byte limits.
    pub fn new(max_rows: usize, soft_byte_limit: usize) -> Result<Self> {
        if max_rows == 0 {
            return Err(DataFusionError::Plan(
                "genotype batch max_rows must be greater than zero".to_string(),
            ));
        }
        if soft_byte_limit == 0 {
            return Err(DataFusionError::Plan(
                "genotype batch soft_byte_limit must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            max_rows,
            soft_byte_limit,
            rows: 0,
            estimated_bytes: 0,
        })
    }

    /// Returns true when the current nonempty batch should be emitted before
    /// appending a row with `estimated_row_bytes`.
    pub fn should_flush_before(&self, estimated_row_bytes: usize) -> bool {
        self.rows > 0
            && (self.rows >= self.max_rows
                || self.estimated_bytes.saturating_add(estimated_row_bytes) > self.soft_byte_limit)
    }

    /// Records an appended row and its estimated genotype bytes.
    pub fn push_row(&mut self, estimated_row_bytes: usize) {
        self.rows = self.rows.saturating_add(1);
        self.estimated_bytes = self.estimated_bytes.saturating_add(estimated_row_bytes);
    }

    /// Returns the number of rows currently tracked.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Returns estimated genotype bytes currently tracked.
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    /// Resets counters after a batch is emitted.
    pub fn reset(&mut self) {
        self.rows = 0;
        self.estimated_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn converts_site_coordinates() {
        assert_eq!(
            CoordinateSystem::OneBasedClosed.site(10).unwrap(),
            SiteCoordinates { start: 10, end: 10 }
        );
        assert_eq!(
            CoordinateSystem::ZeroBasedHalfOpen.site(10).unwrap(),
            SiteCoordinates { start: 9, end: 10 }
        );
        assert!(CoordinateSystem::ZeroBasedHalfOpen.site(0).is_err());
    }

    #[test]
    fn resolves_samples_in_request_order_and_deduplicates() {
        let source = strings(&["a", "b", "c"]);
        let requested = strings(&["c", "a", "c"]);
        let selection =
            resolve_samples(&source, Some(&requested), MissingSamplePolicy::Error).unwrap();
        assert_eq!(selection.source_indices(), &[2, 0]);
        assert_eq!(selection.names(), &strings(&["c", "a"]));
    }

    #[test]
    fn applies_missing_sample_policy() {
        let source = strings(&["a", "b"]);
        let requested = strings(&["missing", "b"]);
        assert!(resolve_samples(&source, Some(&requested), MissingSamplePolicy::Error).is_err());
        let selection =
            resolve_samples(&source, Some(&requested), MissingSamplePolicy::Ignore).unwrap();
        assert_eq!(selection.source_indices(), &[1]);
        assert_eq!(selection.names(), &strings(&["b"]));
    }

    #[test]
    fn rejects_ambiguous_source_samples() {
        let source = strings(&["same", "same"]);
        assert!(resolve_samples(&source, None, MissingSamplePolicy::Error).is_err());
    }

    #[test]
    fn serializes_sample_metadata() {
        let source = strings(&["a", "b"]);
        let selection = resolve_samples(&source, None, MissingSamplePolicy::Error).unwrap();
        assert_eq!(
            selection
                .field_metadata()
                .get(GENOTYPE_SAMPLE_NAMES_KEY)
                .unwrap(),
            r#"["a","b"]"#
        );
    }

    #[test]
    fn resolves_and_validates_genotype_fields() {
        let available = strings(&["GT", "DS"]);
        let requested = strings(&["DS", "DS"]);
        let selection = resolve_genotype_fields(&available, Some(&requested)).unwrap();
        assert_eq!(selection.source_indices(), &[1]);
        assert_eq!(selection.names(), &strings(&["DS"]));

        let unknown = strings(&["GP"]);
        assert!(resolve_genotype_fields(&available, Some(&unknown)).is_err());
    }

    #[test]
    fn only_exact_filters_allow_limit_pushdown() {
        assert!(can_push_limit_below_filters(&[]));
        assert!(can_push_limit_below_filters(&[
            PredicateGuarantee::Exact,
            PredicateGuarantee::Exact,
        ]));
        assert!(!can_push_limit_below_filters(&[
            PredicateGuarantee::Exact,
            PredicateGuarantee::Inexact,
        ]));
        assert!(!can_push_limit_below_filters(&[
            PredicateGuarantee::Unsupported,
        ]));
    }

    #[test]
    fn metrics_accumulate_and_snapshot() {
        let metrics = GenotypeScanMetrics::default();
        metrics.add(GenotypeMetric::EmittedVariants, 2);
        metrics.add(GenotypeMetric::EmittedVariants, 3);
        assert_eq!(metrics.value(GenotypeMetric::EmittedVariants), 5);
        assert_eq!(metrics.snapshot().len(), GenotypeMetric::COUNT);
    }

    #[test]
    fn batch_sizer_flushes_without_blocking_one_large_row() {
        let mut sizer = GenotypeBatchSizer::new(2, 100).unwrap();
        assert!(!sizer.should_flush_before(120));
        sizer.push_row(120);
        assert!(sizer.should_flush_before(1));
        sizer.reset();
        sizer.push_row(40);
        assert!(!sizer.should_flush_before(60));
        sizer.push_row(60);
        assert!(sizer.should_flush_before(1));
    }
}
