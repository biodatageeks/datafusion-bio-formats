//! Shared record-level filter evaluation for bioinformatics formats.
//!
//! Provides a generic mechanism for evaluating DataFusion filter expressions
//! against individual records from any file format (BAM, CRAM, VCF, GFF).
//! Each format implements the [`RecordFieldAccessor`](crate::record_filter::RecordFieldAccessor) trait for its record type.

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Between, Expr, Operator, expr::InList};
use std::sync::Arc;

/// Trait for accessing record field values by column name.
///
/// Each bioinformatics format implements this trait for its record type,
/// enabling generic filter evaluation across formats.
pub trait RecordFieldAccessor {
    /// Returns whether a known field is null for this record.
    ///
    /// The default preserves the behavior of accessors whose supported fields
    /// are non-null. Accessors for nullable fields should override this so a
    /// SQL predicate on null is treated as not passing a `WHERE` clause rather
    /// than as an unsupported field.
    fn is_null_field(&self, _name: &str) -> bool {
        false
    }
    /// Get a string field value by column name.
    fn get_string_field(&self, name: &str) -> Option<String>;
    /// Get a u32 field value by column name.
    fn get_u32_field(&self, name: &str) -> Option<u32>;
    /// Get an f32 field value by column name.
    fn get_f32_field(&self, name: &str) -> Option<f32>;
    /// Get an f64 field value by column name.
    fn get_f64_field(&self, name: &str) -> Option<f64>;
}

/// Check if a filter expression can be evaluated at the record level.
///
/// Supports: binary comparisons (=, !=, <, <=, >, >=), BETWEEN, IN LIST, AND.
/// Only filters referencing columns present in the schema are considered pushable.
pub fn can_push_down_record_filter(expr: &Expr, schema: &Arc<Schema>) -> bool {
    match expr {
        Expr::BinaryExpr(binary_expr) if matches!(binary_expr.op, Operator::And) => {
            can_push_down_record_filter(&binary_expr.left, schema)
                && can_push_down_record_filter(&binary_expr.right, schema)
        }
        Expr::BinaryExpr(binary_expr) => can_push_down_binary_expr(binary_expr, schema),
        Expr::Between(between_expr) => can_push_down_between_expr(between_expr, schema),
        Expr::InList(in_list_expr) => can_push_down_in_list_expr(in_list_expr, schema),
        _ => false,
    }
}

/// Evaluate filter expressions against a record using the RecordFieldAccessor trait.
///
/// Returns `true` if the record passes all filters, `false` otherwise.
/// Empty filter list always returns `true`.
pub fn evaluate_record_filters(record: &dyn RecordFieldAccessor, filters: &[Expr]) -> bool {
    if filters.is_empty() {
        return true;
    }
    filters
        .iter()
        .all(|filter| evaluate_single_filter(record, filter))
}

fn evaluate_single_filter(record: &dyn RecordFieldAccessor, filter: &Expr) -> bool {
    match filter {
        Expr::BinaryExpr(binary_expr) if matches!(binary_expr.op, Operator::And) => {
            evaluate_single_filter(record, &binary_expr.left)
                && evaluate_single_filter(record, &binary_expr.right)
        }
        Expr::BinaryExpr(binary_expr) => evaluate_binary_filter(record, binary_expr),
        Expr::Between(between_expr) => evaluate_between_filter(record, between_expr),
        Expr::InList(in_list_expr) => evaluate_in_list_filter(record, in_list_expr),
        _ => true, // Unsupported expressions pass through
    }
}

fn evaluate_binary_filter(
    record: &dyn RecordFieldAccessor,
    binary_expr: &datafusion::logical_expr::BinaryExpr,
) -> bool {
    if let Expr::Column(column) = &*binary_expr.left
        && let Expr::Literal(literal, _) = &*binary_expr.right
    {
        let field_name = &column.name;
        if record.is_null_field(field_name) || literal.is_null() {
            return false;
        }

        // Try string comparison first
        if let Some(record_value) = record.get_string_field(field_name) {
            return evaluate_string_comparison(&record_value, literal, &binary_expr.op);
        }

        // Try numeric comparisons
        if let Some(record_value) = record.get_f64_field(field_name) {
            return evaluate_numeric_comparison(record_value, literal, &binary_expr.op);
        }
        if let Some(record_value) = record.get_f32_field(field_name) {
            return evaluate_numeric_comparison(record_value as f64, literal, &binary_expr.op);
        }
        if let Some(record_value) = record.get_u32_field(field_name) {
            return evaluate_numeric_comparison(record_value as f64, literal, &binary_expr.op);
        }

        // Field not found on record — pass through
        return true;
    }
    true
}

fn evaluate_between_filter(record: &dyn RecordFieldAccessor, between_expr: &Between) -> bool {
    if let Expr::Column(column) = &*between_expr.expr
        && let (Expr::Literal(low_literal, _), Expr::Literal(high_literal, _)) =
            (&*between_expr.low, &*between_expr.high)
    {
        let field_name = &column.name;
        let negated = between_expr.negated;
        if record.is_null_field(field_name) || low_literal.is_null() || high_literal.is_null() {
            return false;
        }

        // Try u32
        if let Some(record_value) = record.get_u32_field(field_name) {
            return evaluate_between_comparison(
                record_value as f64,
                low_literal,
                high_literal,
                negated,
            );
        }
        // Try f32
        if let Some(record_value) = record.get_f32_field(field_name) {
            return evaluate_between_comparison(
                record_value as f64,
                low_literal,
                high_literal,
                negated,
            );
        }
        // Try f64
        if let Some(record_value) = record.get_f64_field(field_name) {
            return evaluate_between_comparison(record_value, low_literal, high_literal, negated);
        }
    }
    true
}

fn evaluate_in_list_filter(record: &dyn RecordFieldAccessor, in_list_expr: &InList) -> bool {
    if let Expr::Column(column) = &*in_list_expr.expr {
        let field_name = &column.name;
        if record.is_null_field(field_name) {
            return false;
        }

        let literals = in_list_expr.list.iter().filter_map(|expr| match expr {
            Expr::Literal(value, _) => Some(value),
            _ => None,
        });

        // Try string match
        if let Some(record_value) = record.get_string_field(field_name) {
            return evaluate_in_list(literals, in_list_expr.negated, |literal| match literal {
                ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                    Some(record_value == *value)
                }
                value if value.is_null() => None,
                _ => Some(false),
            });
        }

        // Try numeric matches using the same widening as binary comparisons.
        if let Some(record_value) = record.get_u32_field(field_name) {
            return evaluate_numeric_in_list(record_value as f64, literals, in_list_expr.negated);
        }
        if let Some(record_value) = record.get_f32_field(field_name) {
            return evaluate_numeric_in_list(record_value as f64, literals, in_list_expr.negated);
        }
        if let Some(record_value) = record.get_f64_field(field_name) {
            return evaluate_numeric_in_list(record_value, literals, in_list_expr.negated);
        }
    }
    true
}

fn evaluate_in_list<'a>(
    literals: impl Iterator<Item = &'a ScalarValue>,
    negated: bool,
    mut matches: impl FnMut(&ScalarValue) -> Option<bool>,
) -> bool {
    let mut saw_null = false;
    for literal in literals {
        match matches(literal) {
            Some(true) => return !negated,
            Some(false) => {}
            None => saw_null = true,
        }
    }

    // A non-match in a list containing NULL evaluates to UNKNOWN for both IN
    // and NOT IN, which does not pass a WHERE clause.
    !saw_null && negated
}

fn evaluate_numeric_in_list<'a>(
    record_value: f64,
    literals: impl Iterator<Item = &'a ScalarValue>,
    negated: bool,
) -> bool {
    evaluate_in_list(literals, negated, |literal| {
        scalar_numeric_value(literal).map(|literal_value| record_value == literal_value)
    })
}

fn scalar_numeric_value(literal: &ScalarValue) -> Option<f64> {
    match literal {
        ScalarValue::UInt32(Some(value)) => Some(*value as f64),
        ScalarValue::Float32(Some(value)) => Some(*value as f64),
        ScalarValue::Float64(Some(value)) => Some(*value),
        ScalarValue::Int32(Some(value)) => Some(*value as f64),
        ScalarValue::Int64(Some(value)) => Some(*value as f64),
        _ => None,
    }
}

fn evaluate_string_comparison(record_value: &str, literal: &ScalarValue, op: &Operator) -> bool {
    let literal_value = match literal {
        ScalarValue::Utf8(Some(s)) => s.as_str(),
        ScalarValue::LargeUtf8(Some(s)) => s.as_str(),
        _ => return true,
    };

    match op {
        Operator::Eq => record_value == literal_value,
        Operator::NotEq => record_value != literal_value,
        _ => true,
    }
}

fn evaluate_numeric_comparison(record_value: f64, literal: &ScalarValue, op: &Operator) -> bool {
    let Some(literal_value) = scalar_numeric_value(literal) else {
        return true;
    };

    match op {
        Operator::Eq => record_value == literal_value,
        Operator::NotEq => record_value != literal_value,
        Operator::Lt => record_value < literal_value,
        Operator::LtEq => record_value <= literal_value,
        Operator::Gt => record_value > literal_value,
        Operator::GtEq => record_value >= literal_value,
        _ => true,
    }
}

fn evaluate_between_comparison(
    record_value: f64,
    low_literal: &ScalarValue,
    high_literal: &ScalarValue,
    negated: bool,
) -> bool {
    let low_value = match low_literal {
        ScalarValue::UInt32(Some(val)) => *val as f64,
        ScalarValue::Float32(Some(val)) => *val as f64,
        ScalarValue::Float64(Some(val)) => *val,
        ScalarValue::Int32(Some(val)) => *val as f64,
        ScalarValue::Int64(Some(val)) => *val as f64,
        _ => return true,
    };

    let high_value = match high_literal {
        ScalarValue::UInt32(Some(val)) => *val as f64,
        ScalarValue::Float32(Some(val)) => *val as f64,
        ScalarValue::Float64(Some(val)) => *val,
        ScalarValue::Int32(Some(val)) => *val as f64,
        ScalarValue::Int64(Some(val)) => *val as f64,
        _ => return true,
    };

    let between = record_value >= low_value && record_value <= high_value;
    if negated { !between } else { between }
}

fn can_push_down_binary_expr(
    binary_expr: &datafusion::logical_expr::BinaryExpr,
    schema: &Arc<Schema>,
) -> bool {
    if let Expr::Column(column) = &*binary_expr.left {
        let field_name = &column.name;

        if let Ok(field) = schema.field_with_name(field_name)
            && matches!(&*binary_expr.right, Expr::Literal(_, _))
        {
            return match field.data_type() {
                DataType::Utf8 => {
                    matches!(binary_expr.op, Operator::Eq | Operator::NotEq)
                }
                DataType::UInt32 | DataType::Int32 | DataType::Float32 | DataType::Float64 => {
                    matches!(
                        binary_expr.op,
                        Operator::Eq
                            | Operator::NotEq
                            | Operator::Lt
                            | Operator::LtEq
                            | Operator::Gt
                            | Operator::GtEq
                    )
                }
                _ => false,
            };
        }
    }
    false
}

fn can_push_down_between_expr(between_expr: &Between, schema: &Arc<Schema>) -> bool {
    if let Expr::Column(column) = &*between_expr.expr {
        let field_name = &column.name;

        if let Ok(field) = schema.field_with_name(field_name)
            && matches!(
                field.data_type(),
                DataType::UInt32 | DataType::Int32 | DataType::Float32 | DataType::Float64
            )
        {
            return matches!(&*between_expr.low, Expr::Literal(_, _))
                && matches!(&*between_expr.high, Expr::Literal(_, _));
        }
    }
    false
}

fn can_push_down_in_list_expr(in_list_expr: &InList, schema: &Arc<Schema>) -> bool {
    if let Expr::Column(column) = &*in_list_expr.expr {
        let field_name = &column.name;

        if let Ok(field) = schema.field_with_name(field_name)
            && matches!(
                field.data_type(),
                DataType::Utf8
                    | DataType::UInt32
                    | DataType::Int32
                    | DataType::Float32
                    | DataType::Float64
            )
        {
            return in_list_expr
                .list
                .iter()
                .all(|expr| matches!(expr, Expr::Literal(_, _)));
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::{col, lit};

    struct TestRecord {
        chrom: String,
        start: u32,
        mapping_quality: u32,
    }

    impl RecordFieldAccessor for TestRecord {
        fn get_string_field(&self, name: &str) -> Option<String> {
            match name {
                "chrom" => Some(self.chrom.clone()),
                _ => None,
            }
        }
        fn get_u32_field(&self, name: &str) -> Option<u32> {
            match name {
                "start" => Some(self.start),
                "mapping_quality" => Some(self.mapping_quality),
                _ => None,
            }
        }
        fn get_f32_field(&self, _name: &str) -> Option<f32> {
            None
        }
        fn get_f64_field(&self, _name: &str) -> Option<f64> {
            None
        }
    }

    struct NullableNumericRecord {
        score: Option<f64>,
    }

    impl RecordFieldAccessor for NullableNumericRecord {
        fn is_null_field(&self, name: &str) -> bool {
            name == "score" && self.score.is_none()
        }

        fn get_string_field(&self, _name: &str) -> Option<String> {
            None
        }

        fn get_u32_field(&self, _name: &str) -> Option<u32> {
            None
        }

        fn get_f32_field(&self, _name: &str) -> Option<f32> {
            None
        }

        fn get_f64_field(&self, name: &str) -> Option<f64> {
            (name == "score").then_some(self.score).flatten()
        }
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, true),
            Field::new("start", DataType::UInt32, true),
            Field::new("mapping_quality", DataType::UInt32, true),
        ]))
    }

    #[test]
    fn test_evaluate_chrom_eq() {
        let record = TestRecord {
            chrom: "chr1".to_string(),
            start: 1000,
            mapping_quality: 30,
        };
        let filters = vec![col("chrom").eq(lit("chr1"))];
        assert!(evaluate_record_filters(&record, &filters));

        let filters_miss = vec![col("chrom").eq(lit("chr2"))];
        assert!(!evaluate_record_filters(&record, &filters_miss));
    }

    #[test]
    fn test_evaluate_numeric_gte() {
        let record = TestRecord {
            chrom: "chr1".to_string(),
            start: 1000,
            mapping_quality: 30,
        };
        let filters = vec![col("mapping_quality").gt_eq(lit(30u32))];
        assert!(evaluate_record_filters(&record, &filters));

        let filters_fail = vec![col("mapping_quality").gt_eq(lit(31u32))];
        assert!(!evaluate_record_filters(&record, &filters_fail));
    }

    #[test]
    fn test_can_push_down() {
        let schema = test_schema();
        assert!(can_push_down_record_filter(
            &col("chrom").eq(lit("chr1")),
            &schema
        ));
        assert!(can_push_down_record_filter(
            &col("start").gt_eq(lit(1000u32)),
            &schema
        ));
        // Unsupported: LIKE
        assert!(!can_push_down_record_filter(
            &col("chrom").like(lit("chr%")),
            &schema
        ));
    }

    #[test]
    fn test_empty_filters() {
        let record = TestRecord {
            chrom: "chr1".to_string(),
            start: 1000,
            mapping_quality: 30,
        };
        assert!(evaluate_record_filters(&record, &[]));
    }

    #[test]
    fn test_in_list_filter() {
        let record = TestRecord {
            chrom: "chr1".to_string(),
            start: 1000,
            mapping_quality: 30,
        };
        let filters = vec![col("chrom").in_list(vec![lit("chr1"), lit("chr2")], false)];
        assert!(evaluate_record_filters(&record, &filters));

        let filters_miss = vec![col("chrom").in_list(vec![lit("chr2"), lit("chr3")], false)];
        assert!(!evaluate_record_filters(&record, &filters_miss));
    }

    #[test]
    fn nullable_fields_do_not_pass_comparisons_or_negated_lists() {
        let record = NullableNumericRecord { score: None };
        assert!(!evaluate_record_filters(
            &record,
            &[col("score").not_eq(lit(10.0_f64))]
        ));
        assert!(!evaluate_record_filters(
            &record,
            &[col("score").in_list(vec![lit(10.0_f64)], true)]
        ));
    }

    #[test]
    fn numeric_in_list_supports_f64_accessors() {
        let record = NullableNumericRecord { score: Some(3.5) };
        assert!(evaluate_record_filters(
            &record,
            &[col("score").in_list(vec![lit(1.5_f64), lit(3.5_f64)], false)]
        ));
        assert!(!evaluate_record_filters(
            &record,
            &[col("score").in_list(vec![lit(1.5_f64), lit(2.5_f64)], false)]
        ));
    }
}
