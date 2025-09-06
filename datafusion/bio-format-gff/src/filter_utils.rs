use datafusion::arrow::datatypes::{DataType, Schema};
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
