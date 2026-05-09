use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{Expr, Operator};
use datafusion_bio_format_core::metadata::{VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY};
use zarrs::array::CodecOptions;

use super::schema::VCF_ZARR_RAW_ARRAY_METADATA_KEY;

/// Logical projection converted into the raw VCF Zarr arrays needed to satisfy it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProjectionPlan {
    /// Projected schema indices supplied by DataFusion.
    pub projected_indices: Option<Vec<usize>>,
    /// Logical columns requested by the query.
    pub projected_columns: BTreeSet<String>,
    /// Logical columns needed by pushed filters.
    pub predicate_columns: BTreeSet<String>,
    /// Raw VCF Zarr array names required by the logical projection.
    pub raw_arrays: BTreeSet<String>,
}

impl ProjectionPlan {
    /// Builds a projection plan from a DataFusion projection.
    pub fn from_projection(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Self {
        let projected_indices = projection.cloned();
        let field_indices: Vec<usize> = match projection {
            Some(indices) => indices.clone(),
            None => (0..schema.fields().len()).collect(),
        };

        let mut projected_columns = BTreeSet::new();
        let mut raw_arrays = BTreeSet::new();

        for index in field_indices {
            let field = schema.field(index);
            projected_columns.insert(field.name().clone());
            add_field_dependencies(
                field.name(),
                field.data_type(),
                field.metadata(),
                &mut raw_arrays,
            );
        }

        Self {
            projected_indices,
            projected_columns,
            predicate_columns: BTreeSet::new(),
            raw_arrays,
        }
    }

    /// Builds a projection plan from DataFusion projection and filter expressions.
    pub fn from_projection_and_filters(
        schema: &SchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Self {
        let mut plan = Self::from_projection(schema, projection);

        for filter in filters {
            collect_columns(filter, &mut plan.predicate_columns);
        }

        for column in &plan.predicate_columns {
            if let Ok(index) = schema.index_of(column) {
                let field = schema.field(index);
                add_field_dependencies(
                    field.name(),
                    field.data_type(),
                    field.metadata(),
                    &mut plan.raw_arrays,
                );
            }
        }

        plan
    }
}

/// Contiguous row ranges selected for a VCF Zarr scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowSelection {
    /// Store-relative half-open variant row ranges to read.
    pub ranges: Vec<Range<usize>>,
}

/// Partitioning mode attached to a VCF Zarr physical partition.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PartitioningMode {
    /// Partition ranges are exact rows to return.
    ExactRows,
    /// Partition ranges are chunk candidates that need partition-local exact pruning.
    ChunkCandidates,
}

/// Row selection assigned to one DataFusion physical partition.
#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PartitionRowSelection {
    /// Store-relative half-open variant row ranges owned by this partition.
    pub selection: RowSelection,
    /// Whether the selected ranges are exact rows or pruning candidates.
    pub mode: PartitioningMode,
}

impl RowSelection {
    /// Selects every row from `0..row_count`.
    pub fn all(row_count: usize) -> Self {
        if row_count == 0 {
            Self { ranges: Vec::new() }
        } else {
            Self {
                ranges: std::iter::once(0..row_count).collect(),
            }
        }
    }

    /// Builds contiguous selected ranges from a boolean row mask.
    pub fn from_mask(mask: &[bool]) -> Self {
        let mut ranges = Vec::new();
        let mut start = None;

        for (index, selected) in mask.iter().copied().enumerate() {
            match (start, selected) {
                (None, true) => start = Some(index),
                (Some(range_start), false) => {
                    ranges.push(range_start..index);
                    start = None;
                }
                _ => {}
            }
        }

        if let Some(range_start) = start {
            ranges.push(range_start..mask.len());
        }

        Self { ranges }
    }

    /// Filters this selection with a mask ordered over the currently selected rows.
    pub fn filter_mask(&self, mask: &[bool]) -> Self {
        debug_assert_eq!(mask.len(), self.row_count());

        let mut ranges = Vec::new();
        let mut current_start = None;
        let mut mask_index = 0;

        for range in &self.ranges {
            for row in range.clone() {
                let selected = mask.get(mask_index).copied().unwrap_or(false);
                match (current_start, selected) {
                    (None, true) => current_start = Some(row),
                    (Some(start), false) => {
                        ranges.push(start..row);
                        current_start = None;
                    }
                    _ => {}
                }
                mask_index += 1;
            }
            if let Some(start) = current_start.take() {
                ranges.push(start..range.end);
            }
        }

        Self { ranges }
    }

    /// Returns the total number of selected rows.
    pub fn row_count(&self) -> usize {
        self.ranges
            .iter()
            .map(|range| range.end.saturating_sub(range.start))
            .sum()
    }

    /// Applies a row limit while preserving range order.
    pub fn limit(&self, limit: Option<usize>) -> Self {
        let Some(mut remaining) = limit else {
            return self.clone();
        };

        let mut ranges = Vec::new();
        for range in &self.ranges {
            if remaining == 0 {
                break;
            }

            let len = range.end.saturating_sub(range.start);
            let take = len.min(remaining);
            if take > 0 {
                ranges.push(range.start..range.start + take);
                remaining -= take;
            }
        }

        Self { ranges }
    }

    /// Formats ranges for execution-plan display.
    pub fn display_ranges(&self) -> String {
        self.ranges
            .iter()
            .map(|range| format!("{}..{}", range.start, range.end))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Splits selected rows into chunk-aligned DataFusion partition selections.
    #[allow(dead_code)]
    pub(crate) fn chunk_aligned_partitions(
        &self,
        chunk_size: usize,
        target_partitions: usize,
        mode: PartitioningMode,
    ) -> Result<Vec<PartitionRowSelection>> {
        if chunk_size == 0 {
            return Err(DataFusionError::Execution(
                "variant_position chunk size must be greater than zero".to_string(),
            ));
        }

        let mut chunk_ranges: BTreeMap<usize, Vec<Range<usize>>> = BTreeMap::new();
        for range in &self.ranges {
            if range.start >= range.end {
                continue;
            }

            let start_chunk = range.start / chunk_size;
            let end_chunk = (range.end - 1) / chunk_size;
            for chunk in start_chunk..=end_chunk {
                let chunk_start = chunk.saturating_mul(chunk_size);
                let chunk_end = chunk_start.saturating_add(chunk_size);
                let start = range.start.max(chunk_start);
                let end = range.end.min(chunk_end);
                if start < end {
                    push_merged_range(chunk_ranges.entry(chunk).or_default(), start..end);
                }
            }
        }

        if chunk_ranges.is_empty() {
            return Ok(vec![PartitionRowSelection {
                selection: RowSelection { ranges: Vec::new() },
                mode,
            }]);
        }

        let chunks = chunk_ranges.into_iter().collect::<Vec<_>>();
        let partition_count = target_partitions.max(1).min(chunks.len());
        let base_chunks_per_partition = chunks.len() / partition_count;
        let remainder = chunks.len() % partition_count;

        let mut partitions = Vec::with_capacity(partition_count);
        let mut chunk_offset = 0;
        for partition_index in 0..partition_count {
            let take = base_chunks_per_partition + usize::from(partition_index < remainder);
            let mut ranges = Vec::new();
            for (_, chunk_selection) in &chunks[chunk_offset..chunk_offset + take] {
                for range in chunk_selection {
                    push_merged_range(&mut ranges, range.clone());
                }
            }
            partitions.push(PartitionRowSelection {
                selection: RowSelection { ranges },
                mode,
            });
            chunk_offset += take;
        }

        Ok(partitions)
    }
}

/// zarrs read options for work already scheduled as a DataFusion partition.
#[allow(dead_code)]
pub(crate) fn zarr_read_options() -> CodecOptions {
    CodecOptions::default()
        .with_concurrent_target(1)
        .with_chunk_concurrent_minimum(1)
}

#[allow(dead_code)]
fn push_merged_range(ranges: &mut Vec<Range<usize>>, range: Range<usize>) {
    if range.start >= range.end {
        return;
    }

    if let Some(last) = ranges.last_mut()
        && last.end == range.start
    {
        last.end = range.end;
        return;
    }

    ranges.push(range);
}

/// Source of row pruning for execution-plan display and diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PruningMethod {
    /// No pushed genomic predicate was available.
    None,
    /// Candidate chunks were selected from the VCF Zarr `region_index` array.
    RegionIndex,
    /// Candidate rows were selected directly from lightweight variant arrays.
    PositionArrays,
}

impl PruningMethod {
    /// Returns the stable execution-plan label for this pruning method.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::RegionIndex => "region_index",
            Self::PositionArrays => "position_arrays",
        }
    }
}

/// Result of pruning a scan to store-relative rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RowPruning {
    /// Store-relative rows selected by pruning.
    pub selection: RowSelection,
    /// Pruning strategy used to produce the selection.
    pub method: PruningMethod,
    /// Exact fallback pruning to run during physical execution.
    pub deferred_pruning: Option<DeferredPositionPruning>,
}

/// Predicate context needed to apply fallback position-array pruning during execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeferredPositionPruning {
    pub constraints: PredicateConstraints,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PredicateConstraints {
    pub chrom_values: Option<BTreeSet<String>>,
    pub start: NumericBounds,
    pub end: NumericBounds,
}

impl PredicateConstraints {
    pub fn is_empty(&self) -> bool {
        self.chrom_values.is_none() && self.start.is_empty() && self.end.is_empty()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct NumericBounds {
    min: Option<i64>,
    max: Option<i64>,
}

impl NumericBounds {
    pub(crate) fn is_empty(&self) -> bool {
        self.min.is_none() && self.max.is_none()
    }

    pub(crate) fn min(&self) -> Option<i64> {
        self.min
    }

    pub(crate) fn max(&self) -> Option<i64> {
        self.max
    }

    pub fn contains(&self, value: i64) -> bool {
        self.min.is_none_or(|min| value >= min) && self.max.is_none_or(|max| value <= max)
    }

    fn constrain_min(&mut self, value: i64) {
        self.min = Some(self.min.map_or(value, |current| current.max(value)));
    }

    fn constrain_max(&mut self, value: i64) {
        self.max = Some(self.max.map_or(value, |current| current.min(value)));
    }

    fn constrain_eq(&mut self, value: i64) {
        self.constrain_min(value);
        self.constrain_max(value);
    }
}

pub(crate) fn predicate_constraints(filters: &[Expr]) -> Option<PredicateConstraints> {
    let mut constraints = PredicateConstraints::default();

    for filter in filters {
        collect_constraints(filter, &mut constraints);
    }

    (!constraints.is_empty()).then_some(constraints)
}

fn add_field_dependencies(
    column: &str,
    data_type: &DataType,
    metadata: &std::collections::HashMap<String, String>,
    raw_arrays: &mut BTreeSet<String>,
) {
    match column {
        "chrom" => {
            raw_arrays.insert("variant_contig".to_string());
            raw_arrays.insert("contig_id".to_string());
        }
        "start" => {
            raw_arrays.insert("variant_position".to_string());
        }
        "end" => {
            raw_arrays.insert("variant_position".to_string());
            raw_arrays.insert("variant_length".to_string());
            raw_arrays.insert("variant_allele".to_string());
        }
        "id" => {
            raw_arrays.insert("variant_id".to_string());
        }
        "ref" | "alt" => {
            raw_arrays.insert("variant_allele".to_string());
        }
        "qual" => {
            raw_arrays.insert("variant_quality".to_string());
        }
        "filter" => {
            raw_arrays.insert("variant_filter".to_string());
            raw_arrays.insert("filter_id".to_string());
        }
        "genotypes" => {
            if let DataType::Struct(children) = data_type {
                for child in children {
                    if let Some(raw_array) = child.metadata().get(VCF_ZARR_RAW_ARRAY_METADATA_KEY) {
                        raw_arrays.insert(raw_array.clone());
                    } else {
                        let id = child
                            .metadata()
                            .get(VCF_FIELD_FORMAT_ID_KEY)
                            .map(String::as_str)
                            .unwrap_or_else(|| child.name().as_str());
                        raw_arrays.insert(format!("call_{id}"));
                    }
                }
            }
        }
        other => {
            if metadata
                .get(VCF_FIELD_FIELD_TYPE_KEY)
                .is_some_and(|field_type| field_type == "INFO")
            {
                let raw_array = metadata
                    .get(VCF_ZARR_RAW_ARRAY_METADATA_KEY)
                    .cloned()
                    .unwrap_or_else(|| format!("variant_{other}"));
                raw_arrays.insert(raw_array);
            }
        }
    }
}

fn collect_columns(expr: &Expr, columns: &mut BTreeSet<String>) {
    match expr {
        Expr::Column(column) => {
            columns.insert(column.name.clone());
        }
        Expr::BinaryExpr(binary) => {
            collect_columns(&binary.left, columns);
            collect_columns(&binary.right, columns);
        }
        Expr::Between(between) => {
            collect_columns(&between.expr, columns);
            collect_columns(&between.low, columns);
            collect_columns(&between.high, columns);
        }
        Expr::InList(in_list) => {
            collect_columns(&in_list.expr, columns);
            for item in &in_list.list {
                collect_columns(item, columns);
            }
        }
        Expr::Cast(cast) => collect_columns(&cast.expr, columns),
        Expr::TryCast(cast) => collect_columns(&cast.expr, columns),
        Expr::Not(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Negative(expr) => collect_columns(expr, columns),
        _ => {}
    }
}

fn collect_constraints(expr: &Expr, constraints: &mut PredicateConstraints) -> bool {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left = collect_constraints(&binary.left, constraints);
            let right = collect_constraints(&binary.right, constraints);
            left || right
        }
        Expr::BinaryExpr(binary) => {
            collect_comparison(&binary.left, binary.op, &binary.right, constraints)
        }
        Expr::Between(between) if !between.negated => {
            let Some(column) = column_name(&between.expr) else {
                return false;
            };
            let Some(low) = scalar_i64(&between.low) else {
                return false;
            };
            let Some(high) = scalar_i64(&between.high) else {
                return false;
            };
            constrain_numeric_column(column, Operator::GtEq, low, constraints)
                | constrain_numeric_column(column, Operator::LtEq, high, constraints)
        }
        Expr::InList(in_list) if !in_list.negated => collect_in_list(in_list, constraints),
        _ => false,
    }
}

fn collect_comparison(
    left: &Expr,
    op: Operator,
    right: &Expr,
    constraints: &mut PredicateConstraints,
) -> bool {
    if let (Some(column), Some(value)) = (column_name(left), scalar_i64(right)) {
        return constrain_numeric_column(column, op, value, constraints);
    }

    if let (Some(value), Some(column)) = (scalar_i64(left), column_name(right)) {
        return constrain_numeric_column(column, reverse_operator(op), value, constraints);
    }

    if let (Some(column), Some(value)) = (column_name(left), scalar_string(right)) {
        return constrain_string_column(column, op, value, constraints);
    }

    if let (Some(value), Some(column)) = (scalar_string(left), column_name(right)) {
        return constrain_string_column(column, reverse_operator(op), value, constraints);
    }

    false
}

fn collect_in_list(
    in_list: &datafusion::logical_expr::expr::InList,
    constraints: &mut PredicateConstraints,
) -> bool {
    let Some("chrom") = column_name(&in_list.expr) else {
        return false;
    };

    let values: BTreeSet<String> = in_list.list.iter().filter_map(scalar_string).collect();
    if values.is_empty() {
        return false;
    }

    constrain_chrom_values(values, constraints);
    true
}

fn constrain_string_column(
    column: &str,
    op: Operator,
    value: String,
    constraints: &mut PredicateConstraints,
) -> bool {
    if column != "chrom" || op != Operator::Eq {
        return false;
    }

    constrain_chrom_values([value].into_iter().collect(), constraints);
    true
}

fn constrain_chrom_values(values: BTreeSet<String>, constraints: &mut PredicateConstraints) {
    constraints.chrom_values = Some(match constraints.chrom_values.take() {
        Some(existing) => existing.intersection(&values).cloned().collect(),
        None => values,
    });
}

fn constrain_numeric_column(
    column: &str,
    op: Operator,
    value: i64,
    constraints: &mut PredicateConstraints,
) -> bool {
    let bounds = match column {
        "start" => &mut constraints.start,
        "end" => &mut constraints.end,
        _ => return false,
    };

    match op {
        Operator::Eq => bounds.constrain_eq(value),
        Operator::Gt => bounds.constrain_min(value.saturating_add(1)),
        Operator::GtEq => bounds.constrain_min(value),
        Operator::Lt => bounds.constrain_max(value.saturating_sub(1)),
        Operator::LtEq => bounds.constrain_max(value),
        _ => return false,
    }

    true
}

fn reverse_operator(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        Expr::Cast(cast) => column_name(&cast.expr),
        Expr::TryCast(cast) => column_name(&cast.expr),
        _ => None,
    }
}

fn scalar_i64(expr: &Expr) -> Option<i64> {
    let scalar = scalar_value(expr)?;
    match scalar {
        ScalarValue::Int8(Some(value)) => Some(i64::from(value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(value)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(value)),
        ScalarValue::Int64(Some(value)) => Some(value),
        ScalarValue::UInt8(Some(value)) => Some(i64::from(value)),
        ScalarValue::UInt16(Some(value)) => Some(i64::from(value)),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(value).ok(),
        _ => None,
    }
}

fn scalar_string(expr: &Expr) -> Option<String> {
    let scalar = scalar_value(expr)?;
    match scalar {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value),
        _ => None,
    }
}

fn scalar_value(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Cast(cast) => scalar_value(&cast.expr),
        Expr::TryCast(cast) => scalar_value(&cast.expr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{PartitioningMode, RowSelection, zarr_read_options};

    fn partition_ranges(
        selection: RowSelection,
        chunk_size: usize,
        target_partitions: usize,
    ) -> Vec<Vec<std::ops::Range<usize>>> {
        selection
            .chunk_aligned_partitions(chunk_size, target_partitions, PartitioningMode::ExactRows)
            .expect("partitioning should succeed")
            .into_iter()
            .map(|partition| partition.selection.ranges)
            .collect()
    }

    fn ranges(
        ranges: impl IntoIterator<Item = std::ops::Range<usize>>,
    ) -> Vec<std::ops::Range<usize>> {
        ranges.into_iter().collect()
    }

    fn range(start: usize, end: usize) -> Vec<std::ops::Range<usize>> {
        std::iter::once(start..end).collect()
    }

    #[test]
    fn partitions_preserve_chunks_and_cap_at_selected_chunk_count() {
        let partitions = partition_ranges(
            RowSelection {
                ranges: range(0, 30),
            },
            10,
            8,
        );

        assert_eq!(partitions, vec![vec![0..10], vec![10..20], vec![20..30]]);
    }

    #[test]
    fn partitions_group_adjacent_chunks_when_selected_chunks_exceed_target() {
        let partitions = partition_ranges(
            RowSelection {
                ranges: range(0, 50),
            },
            10,
            2,
        );

        assert_eq!(partitions, vec![vec![0..30], vec![30..50]]);
    }

    #[test]
    fn partitions_keep_sparse_rows_inside_their_chunk() {
        let partitions = partition_ranges(
            RowSelection {
                ranges: ranges([2..4, 7..9, 17..19]),
            },
            10,
            8,
        );

        assert_eq!(partitions, vec![vec![2..4, 7..9], vec![17..19]]);
    }

    #[test]
    fn partitions_return_one_empty_partition_for_empty_selection() {
        let partitions = partition_ranges(RowSelection { ranges: Vec::new() }, 10, 8);

        assert_eq!(partitions, vec![Vec::<std::ops::Range<usize>>::new()]);
    }

    #[test]
    fn partitions_treat_zero_target_as_one_partition() {
        let partitions = partition_ranges(
            RowSelection {
                ranges: range(0, 30),
            },
            10,
            0,
        );

        assert_eq!(partitions, vec![vec![0..30]]);
    }

    #[test]
    fn partitions_reject_zero_chunk_size() {
        let message = RowSelection {
            ranges: range(0, 30),
        }
        .chunk_aligned_partitions(0, 8, PartitioningMode::ExactRows)
        .expect_err("zero chunk size must fail")
        .to_string();

        assert!(
            message.contains("variant_position") && message.contains("chunk size"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn partition_keeps_mode_on_each_partition() {
        let partitions = RowSelection {
            ranges: range(0, 20),
        }
        .chunk_aligned_partitions(10, 4, PartitioningMode::ChunkCandidates)
        .expect("partitioning should succeed");

        assert_eq!(partitions.len(), 2);
        assert!(
            partitions
                .iter()
                .all(|partition| partition.mode == PartitioningMode::ChunkCandidates)
        );
    }

    #[test]
    fn zarr_read_options_disable_inner_chunk_and_codec_parallelism() {
        let options = zarr_read_options();

        assert_eq!(options.concurrent_target(), 1);
        assert_eq!(options.chunk_concurrent_minimum(), 1);
    }
}
