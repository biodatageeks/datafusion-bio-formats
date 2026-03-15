use crate::storage::{GtfRecordTrait, parse_gtf_pair};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Between, Expr, Operator, expr::InList};
use datafusion_bio_format_core::record_filter::can_push_down_record_filter;
use std::sync::Arc;

/// Check if a filter expression can be pushed down for GTF records.
pub fn can_push_down_filter(expr: &Expr, schema: &Arc<Schema>) -> bool {
    if can_push_down_record_filter(expr, schema) {
        return true;
    }

    match expr {
        Expr::BinaryExpr(binary_expr) if matches!(binary_expr.op, Operator::And) => {
            can_push_down_filter(&binary_expr.left, schema)
                && can_push_down_filter(&binary_expr.right, schema)
        }
        Expr::BinaryExpr(binary_expr) => can_push_down_gtf_attribute_expr(binary_expr),
        Expr::InList(in_list_expr) => can_push_down_gtf_attribute_in_list(in_list_expr),
        _ => false,
    }
}

fn can_push_down_gtf_attribute_expr(binary_expr: &datafusion::logical_expr::BinaryExpr) -> bool {
    if let Expr::Column(column) = &*binary_expr.left {
        let field_name = &column.name;
        let is_standard = matches!(
            field_name.as_str(),
            "chrom" | "start" | "end" | "type" | "source" | "score" | "strand" | "phase"
        );
        if !is_standard && matches!(&*binary_expr.right, Expr::Literal(_, _)) {
            return matches!(binary_expr.op, Operator::Eq | Operator::NotEq);
        }
    }
    false
}

fn can_push_down_gtf_attribute_in_list(in_list_expr: &InList) -> bool {
    if let Expr::Column(column) = &*in_list_expr.expr {
        let field_name = &column.name;
        let is_standard = matches!(
            field_name.as_str(),
            "chrom" | "start" | "end" | "type" | "source" | "score" | "strand" | "phase"
        );
        if !is_standard {
            return in_list_expr
                .list
                .iter()
                .all(|expr| matches!(expr, Expr::Literal(_, _)));
        }
    }
    false
}

/// Evaluates filter expressions against a GTF record.
///
/// The `coordinate_system_zero_based` flag is needed because records store 1-based
/// positions from the file, but user SQL predicates are in the output coordinate system.
/// When true, record positions are converted to 0-based before comparison.
pub fn evaluate_filters_against_record<T: GtfRecordTrait>(
    record: &T,
    filters: &[Expr],
    attributes_str: &str,
    coordinate_system_zero_based: bool,
) -> bool {
    if filters.is_empty() {
        return true;
    }

    filters.iter().all(|filter| {
        evaluate_single_filter(record, filter, attributes_str, coordinate_system_zero_based)
    })
}

fn evaluate_single_filter<T: GtfRecordTrait>(
    record: &T,
    filter: &Expr,
    attributes_str: &str,
    coordinate_system_zero_based: bool,
) -> bool {
    match filter {
        Expr::BinaryExpr(binary_expr) if matches!(binary_expr.op, Operator::And) => {
            evaluate_single_filter(
                record,
                &binary_expr.left,
                attributes_str,
                coordinate_system_zero_based,
            ) && evaluate_single_filter(
                record,
                &binary_expr.right,
                attributes_str,
                coordinate_system_zero_based,
            )
        }
        Expr::BinaryExpr(binary_expr) => evaluate_binary_filter(
            record,
            binary_expr,
            attributes_str,
            coordinate_system_zero_based,
        ),
        Expr::Between(between_expr) => {
            evaluate_between_filter(record, between_expr, coordinate_system_zero_based)
        }
        Expr::InList(in_list_expr) => evaluate_in_list_filter(
            record,
            in_list_expr,
            attributes_str,
            coordinate_system_zero_based,
        ),
        _ => true,
    }
}

fn evaluate_binary_filter<T: GtfRecordTrait>(
    record: &T,
    binary_expr: &datafusion::logical_expr::BinaryExpr,
    attributes_str: &str,
    coordinate_system_zero_based: bool,
) -> bool {
    if let Expr::Column(column) = &*binary_expr.left
        && let Expr::Literal(literal, _) = &*binary_expr.right
    {
        let field_name = &column.name;

        return match field_name.as_str() {
            "chrom" => evaluate_string_comparison(
                record.reference_sequence_name(),
                literal,
                &binary_expr.op,
            ),
            "start" => {
                let start_1based = record.start();
                let start_output = if coordinate_system_zero_based {
                    start_1based.saturating_sub(1)
                } else {
                    start_1based
                };
                evaluate_numeric_comparison(start_output as f64, literal, &binary_expr.op)
            }
            "end" => {
                let record_value = record.end();
                evaluate_numeric_comparison(record_value as f64, literal, &binary_expr.op)
            }
            "type" => evaluate_string_comparison(record.ty(), literal, &binary_expr.op),
            "source" => evaluate_string_comparison(record.source(), literal, &binary_expr.op),
            "score" => {
                if let Some(score) = record.score() {
                    evaluate_numeric_comparison(score as f64, literal, &binary_expr.op)
                } else {
                    false
                }
            }
            "strand" => evaluate_string_comparison(record.strand(), literal, &binary_expr.op),
            "phase" => {
                if let Some(phase) = record.phase() {
                    evaluate_numeric_comparison(phase as f64, literal, &binary_expr.op)
                } else {
                    false
                }
            }
            _ => evaluate_attribute_filter(field_name, attributes_str, literal, &binary_expr.op),
        };
    }
    true
}

fn evaluate_between_filter<T: GtfRecordTrait>(
    record: &T,
    between_expr: &Between,
    coordinate_system_zero_based: bool,
) -> bool {
    if let Expr::Column(column) = &*between_expr.expr
        && let (Expr::Literal(low_literal, _), Expr::Literal(high_literal, _)) =
            (&*between_expr.low, &*between_expr.high)
    {
        let field_name = &column.name;
        let negated = between_expr.negated;

        return match field_name.as_str() {
            "start" => {
                let start_1based = record.start();
                let start_output = if coordinate_system_zero_based {
                    start_1based.saturating_sub(1)
                } else {
                    start_1based
                };
                evaluate_between_comparison(start_output as f64, low_literal, high_literal, negated)
            }
            "end" => {
                evaluate_between_comparison(record.end() as f64, low_literal, high_literal, negated)
            }
            _ => true,
        };
    }
    true
}

fn evaluate_in_list_filter<T: GtfRecordTrait>(
    record: &T,
    in_list_expr: &InList,
    attributes_str: &str,
    coordinate_system_zero_based: bool,
) -> bool {
    if let Expr::Column(column) = &*in_list_expr.expr {
        let field_name = &column.name;
        let values: Vec<String> = in_list_expr
            .list
            .iter()
            .filter_map(|e| match e {
                Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s.clone()),
                Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Some(s.clone()),
                Expr::Literal(ScalarValue::UInt32(Some(v)), _) => Some(v.to_string()),
                Expr::Literal(ScalarValue::Int32(Some(v)), _) => Some(v.to_string()),
                Expr::Literal(ScalarValue::Int64(Some(v)), _) => Some(v.to_string()),
                Expr::Literal(ScalarValue::Float32(Some(v)), _) => Some(v.to_string()),
                Expr::Literal(ScalarValue::Float64(Some(v)), _) => Some(v.to_string()),
                _ => None,
            })
            .collect();

        // Core columns are handled here because can_push_down_record_filter (from core)
        // allows IN pushdown for any schema column. Without these arms, numeric columns
        // would fall through to attribute lookup and silently drop valid rows.
        let contains = match field_name.as_str() {
            "chrom" => values.iter().any(|v| v == record.reference_sequence_name()),
            "start" => {
                let start_1based = record.start();
                let start_output = if coordinate_system_zero_based {
                    start_1based.saturating_sub(1)
                } else {
                    start_1based
                };
                values.contains(&start_output.to_string())
            }
            "end" => values.contains(&record.end().to_string()),
            "type" => values.iter().any(|v| v == record.ty()),
            "source" => values.iter().any(|v| v == record.source()),
            "score" => record
                .score()
                .map(|s| values.contains(&s.to_string()))
                .unwrap_or(false),
            "strand" => values.iter().any(|v| v == record.strand()),
            "phase" => record
                .phase()
                .map(|p| values.contains(&p.to_string()))
                .unwrap_or(false),
            other => extract_gtf_attribute_value(other, attributes_str)
                .map(|v| values.contains(&v))
                .unwrap_or(false),
        };

        return if in_list_expr.negated {
            !contains
        } else {
            contains
        };
    }
    true
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
    let literal_value = match literal {
        ScalarValue::UInt32(Some(val)) => *val as f64,
        ScalarValue::Float32(Some(val)) => *val as f64,
        ScalarValue::Float64(Some(val)) => *val,
        ScalarValue::Int32(Some(val)) => *val as f64,
        ScalarValue::Int64(Some(val)) => *val as f64,
        _ => return true,
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

fn evaluate_attribute_filter(
    field_name: &str,
    attributes_str: &str,
    literal: &ScalarValue,
    op: &Operator,
) -> bool {
    if let Some(attr_value) = extract_gtf_attribute_value(field_name, attributes_str) {
        return evaluate_string_comparison(&attr_value, literal, op);
    }
    false
}

/// Extract a GTF attribute value by key from the raw attributes string.
///
/// GTF attributes use the format: `key "value"; key "value";`
/// Some values may be unquoted (e.g., `level 2`).
pub fn extract_gtf_attribute_value(field_name: &str, attributes_str: &str) -> Option<String> {
    if attributes_str.is_empty() || attributes_str == "." {
        return None;
    }

    for pair in attributes_str.split(';') {
        if let Some((key, value)) = parse_gtf_pair(pair)
            && key == field_name
        {
            return Some(value.to_string());
        }
    }
    None
}
