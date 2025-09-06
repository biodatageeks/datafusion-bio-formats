use crate::storage::GffRecordTrait;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Between, Expr, Operator, expr::InList};
use std::sync::Arc;

pub fn can_push_down_filter(expr: &Expr, schema: &Arc<Schema>) -> bool {
    match expr {
        // AND expressions: expr1 AND expr2 (handle this first to avoid pattern conflict)
        Expr::BinaryExpr(binary_expr) if matches!(binary_expr.op, Operator::And) => {
            can_push_down_filter(&binary_expr.left, schema)
                && can_push_down_filter(&binary_expr.right, schema)
        }
        // Other binary expressions: column op literal
        Expr::BinaryExpr(binary_expr) => can_push_down_binary_expr(binary_expr, schema),
        Expr::Between(between_expr) => can_push_down_between_expr(between_expr, schema),
        Expr::InList(in_list_expr) => can_push_down_in_list_expr(in_list_expr, schema),
        _ => false,
    }
}

/// Evaluates filter expressions against a GFF record.
/// Returns true if the record passes all filters, false otherwise.
pub fn evaluate_filters_against_record<T: GffRecordTrait>(
    record: &T,
    filters: &[Expr],
    attributes_str: &str,
) -> bool {
    if filters.is_empty() {
        return true;
    }

    filters
        .iter()
        .all(|filter| evaluate_single_filter(record, filter, attributes_str))
}

fn evaluate_single_filter<T: GffRecordTrait>(
    record: &T,
    filter: &Expr,
    attributes_str: &str,
) -> bool {
    match filter {
        // AND expressions
        Expr::BinaryExpr(binary_expr) if matches!(binary_expr.op, Operator::And) => {
            evaluate_single_filter(record, &binary_expr.left, attributes_str)
                && evaluate_single_filter(record, &binary_expr.right, attributes_str)
        }
        // Other binary expressions: column op literal
        Expr::BinaryExpr(binary_expr) => {
            evaluate_binary_filter(record, binary_expr, attributes_str)
        }
        Expr::Between(between_expr) => {
            evaluate_between_filter(record, between_expr, attributes_str)
        }
        Expr::InList(in_list_expr) => evaluate_in_list_filter(record, in_list_expr, attributes_str),
        _ => true, // Unsupported expressions are treated as pass-through
    }
}

fn evaluate_binary_filter<T: GffRecordTrait>(
    record: &T,
    binary_expr: &datafusion::logical_expr::BinaryExpr,
    attributes_str: &str,
) -> bool {
    if let Expr::Column(column) = &*binary_expr.left {
        if let Expr::Literal(literal, _) = &*binary_expr.right {
            let field_name = &column.name;

            return match field_name.as_str() {
                "chrom" => {
                    let record_value = record.reference_sequence_name();
                    evaluate_string_comparison(&record_value, literal, &binary_expr.op)
                }
                "start" => {
                    let record_value = record.start();
                    evaluate_numeric_comparison(record_value as f64, literal, &binary_expr.op)
                }
                "end" => {
                    let record_value = record.end();
                    evaluate_numeric_comparison(record_value as f64, literal, &binary_expr.op)
                }
                "type" => {
                    let record_value = record.ty();
                    evaluate_string_comparison(&record_value, literal, &binary_expr.op)
                }
                "source" => {
                    let record_value = record.source();
                    evaluate_string_comparison(&record_value, literal, &binary_expr.op)
                }
                "score" => {
                    if let Some(score) = record.score() {
                        evaluate_numeric_comparison(score as f64, literal, &binary_expr.op)
                    } else {
                        false // no score means it doesn't match numeric filters
                    }
                }
                "strand" => {
                    let record_value = record.strand();
                    evaluate_string_comparison(&record_value, literal, &binary_expr.op)
                }
                _ => {
                    // Attribute-based filter
                    evaluate_attribute_filter(field_name, attributes_str, literal, &binary_expr.op)
                }
            };
        }
    }
    true
}

fn evaluate_between_filter<T: GffRecordTrait>(
    record: &T,
    between_expr: &Between,
    _attributes_str: &str,
) -> bool {
    if let Expr::Column(column) = &*between_expr.expr {
        if let (Expr::Literal(low_literal, _), Expr::Literal(high_literal, _)) =
            (&*between_expr.low, &*between_expr.high)
        {
            let field_name = &column.name;
            let negated = between_expr.negated;

            return match field_name.as_str() {
                "start" => evaluate_between_comparison(
                    record.start() as f64,
                    low_literal,
                    high_literal,
                    negated,
                ),
                "end" => evaluate_between_comparison(
                    record.end() as f64,
                    low_literal,
                    high_literal,
                    negated,
                ),
                _ => true,
            };
        }
    }
    true
}

fn evaluate_in_list_filter<T: GffRecordTrait>(
    record: &T,
    in_list_expr: &InList,
    attributes_str: &str,
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

        let contains = match field_name.as_str() {
            "chrom" => values.contains(&record.reference_sequence_name()),
            "type" => values.contains(&record.ty()),
            "source" => values.contains(&record.source()),
            "strand" => values.contains(&record.strand()),
            // Attribute fields
            other => extract_attribute_value(other, attributes_str)
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

fn evaluate_string_comparison(
    record_value: &str,
    literal: &ScalarValue,
    op: &datafusion::logical_expr::Operator,
) -> bool {
    let literal_value = match literal {
        ScalarValue::Utf8(Some(s)) => s.as_str(),
        ScalarValue::LargeUtf8(Some(s)) => s.as_str(),
        _ => return true,
    };

    match op {
        datafusion::logical_expr::Operator::Eq => record_value == literal_value,
        datafusion::logical_expr::Operator::NotEq => record_value != literal_value,
        _ => true,
    }
}

fn evaluate_numeric_comparison(
    record_value: f64,
    literal: &ScalarValue,
    op: &datafusion::logical_expr::Operator,
) -> bool {
    let literal_value = match literal {
        ScalarValue::UInt32(Some(val)) => *val as f64,
        ScalarValue::Float32(Some(val)) => *val as f64,
        ScalarValue::Float64(Some(val)) => *val,
        ScalarValue::Int32(Some(val)) => *val as f64,
        ScalarValue::Int64(Some(val)) => *val as f64,
        _ => return true,
    };

    match op {
        datafusion::logical_expr::Operator::Eq => record_value == literal_value,
        datafusion::logical_expr::Operator::NotEq => record_value != literal_value,
        datafusion::logical_expr::Operator::Lt => record_value < literal_value,
        datafusion::logical_expr::Operator::LtEq => record_value <= literal_value,
        datafusion::logical_expr::Operator::Gt => record_value > literal_value,
        datafusion::logical_expr::Operator::GtEq => record_value >= literal_value,
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
    op: &datafusion::logical_expr::Operator,
) -> bool {
    if let Some(attr_value) = extract_attribute_value(field_name, attributes_str) {
        return evaluate_string_comparison(&attr_value, literal, op);
    }
    false
}

fn extract_attribute_value(field_name: &str, attributes_str: &str) -> Option<String> {
    if attributes_str.is_empty() || attributes_str == "." {
        return None;
    }

    for pair in attributes_str.split(';') {
        if pair.is_empty() {
            continue;
        }
        if let Some(eq_pos) = pair.find('=') {
            let key = &pair[..eq_pos];
            if key == field_name {
                let value = &pair[eq_pos + 1..];

                // Handle quoted and URL-encoded values
                let decoded_value = if value.starts_with('"') && value.ends_with('"') {
                    value[1..value.len() - 1].to_string()
                } else if value.contains('%') {
                    value
                        .replace("%3B", ";")
                        .replace("%3D", "=")
                        .replace("%26", "&")
                        .replace("%2C", ",")
                        .replace("%09", "\t")
                } else {
                    value.to_string()
                };

                return Some(decoded_value);
            }
        }
    }
    None
}

fn can_push_down_binary_expr(
    binary_expr: &datafusion::logical_expr::BinaryExpr,
    schema: &Arc<Schema>,
) -> bool {
    use datafusion::logical_expr::Operator;

    // Check if left side is a column reference
    if let Expr::Column(column) = &*binary_expr.left {
        let field_name = &column.name;

        // Check if the column exists in schema
        if let Ok(field) = schema.field_with_name(field_name) {
            // Check if right side is a literal
            if matches!(&*binary_expr.right, Expr::Literal(_, _)) {
                return match field.data_type() {
                    // String columns: support =, !=
                    DataType::Utf8 => matches!(binary_expr.op, Operator::Eq | Operator::NotEq),
                    // Numeric columns: support =, !=, <, <=, >, >=
                    DataType::UInt32 | DataType::Float32 => matches!(
                        binary_expr.op,
                        Operator::Eq
                            | Operator::NotEq
                            | Operator::Lt
                            | Operator::LtEq
                            | Operator::Gt
                            | Operator::GtEq
                    ),
                    _ => false,
                };
            }
        }
    }
    false
}

fn can_push_down_between_expr(between_expr: &Between, schema: &Arc<Schema>) -> bool {
    if let Expr::Column(column) = &*between_expr.expr {
        let field_name = &column.name;

        if let Ok(field) = schema.field_with_name(field_name) {
            // BETWEEN only makes sense for numeric columns
            if matches!(field.data_type(), DataType::UInt32 | DataType::Float32) {
                // Check if both bounds are literals
                return matches!(&*between_expr.low, Expr::Literal(_, _))
                    && matches!(&*between_expr.high, Expr::Literal(_, _));
            }
        }
    }
    false
}

fn can_push_down_in_list_expr(in_list_expr: &InList, schema: &Arc<Schema>) -> bool {
    if let Expr::Column(column) = &*in_list_expr.expr {
        let field_name = &column.name;

        if let Ok(field) = schema.field_with_name(field_name) {
            // IN lists work for string and numeric columns
            if matches!(
                field.data_type(),
                DataType::Utf8 | DataType::UInt32 | DataType::Float32
            ) {
                // Check if all list items are literals
                return in_list_expr
                    .list
                    .iter()
                    .all(|expr| matches!(expr, Expr::Literal(_, _)));
            }
        }
    }
    false
}
