use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Between, Expr, Operator, expr::InList};

use crate::pvar::PvarRow;

pub(crate) fn supports_exact_filter(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            supports_exact_filter(&binary.left) && supports_exact_filter(&binary.right)
        }
        Expr::BinaryExpr(binary) => {
            let Expr::Column(column) = &*binary.left else {
                return false;
            };
            let Expr::Literal(literal, _) = &*binary.right else {
                return false;
            };
            supports_comparison(&column.name, literal, &binary.op)
        }
        Expr::Between(between) => supports_between(between),
        Expr::InList(in_list) => supports_in_list(in_list),
        _ => false,
    }
}

pub(crate) fn evaluate_exact_filter(variant: PvarRow<'_>, expr: &Expr) -> bool {
    debug_assert!(supports_exact_filter(expr));
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            evaluate_exact_filter(variant, &binary.left)
                && evaluate_exact_filter(variant, &binary.right)
        }
        Expr::BinaryExpr(binary) => {
            let (Expr::Column(column), Expr::Literal(literal, _)) = (&*binary.left, &*binary.right)
            else {
                return true;
            };
            evaluate_comparison(variant, &column.name, literal, &binary.op)
        }
        Expr::Between(between) => evaluate_between(variant, between),
        Expr::InList(in_list) => evaluate_in_list(variant, in_list),
        _ => true,
    }
}

fn supports_comparison(name: &str, literal: &ScalarValue, operator: &Operator) -> bool {
    match name {
        "chrom" | "id" => is_string(literal) && matches!(operator, Operator::Eq | Operator::NotEq),
        "start" | "end" => {
            is_integer(literal)
                && matches!(
                    operator,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                )
        }
        _ => false,
    }
}

fn supports_between(between: &Between) -> bool {
    let Expr::Column(column) = &*between.expr else {
        return false;
    };
    matches!(column.name.as_str(), "start" | "end")
        && matches!(&*between.low, Expr::Literal(value, _) if is_integer(value))
        && matches!(&*between.high, Expr::Literal(value, _) if is_integer(value))
}

fn supports_in_list(in_list: &InList) -> bool {
    let Expr::Column(column) = &*in_list.expr else {
        return false;
    };
    match column.name.as_str() {
        "chrom" | "id" => in_list
            .list
            .iter()
            .all(|expr| matches!(expr, Expr::Literal(value, _) if is_string(value))),
        "start" | "end" => in_list
            .list
            .iter()
            .all(|expr| matches!(expr, Expr::Literal(value, _) if is_integer(value))),
        _ => false,
    }
}

fn evaluate_comparison(
    variant: PvarRow<'_>,
    name: &str,
    literal: &ScalarValue,
    operator: &Operator,
) -> bool {
    match name {
        "chrom" => compare_string(Some(variant.chrom), literal, operator),
        "id" => compare_string(variant.id, literal, operator),
        "start" => compare_integer(variant.start, literal, operator),
        "end" => compare_integer(variant.end, literal, operator),
        _ => true,
    }
}

fn evaluate_between(variant: PvarRow<'_>, between: &Between) -> bool {
    let (Expr::Column(column), Expr::Literal(low, _), Expr::Literal(high, _)) =
        (&*between.expr, &*between.low, &*between.high)
    else {
        return true;
    };
    let value = match column.name.as_str() {
        "start" => variant.start,
        "end" => variant.end,
        _ => return true,
    };
    let Some(low) = integer(low) else {
        return true;
    };
    let Some(high) = integer(high) else {
        return true;
    };
    let matches = value >= low && value <= high;
    if between.negated { !matches } else { matches }
}

fn evaluate_in_list(variant: PvarRow<'_>, in_list: &InList) -> bool {
    let Expr::Column(column) = &*in_list.expr else {
        return true;
    };
    let matches = match column.name.as_str() {
        "chrom" => in_list.list.iter().any(
            |expr| matches!(expr, Expr::Literal(value, _) if string(value) == Some(variant.chrom)),
        ),
        "id" => {
            let Some(id) = variant.id else {
                return false;
            };
            in_list
                .list
                .iter()
                .any(|expr| matches!(expr, Expr::Literal(value, _) if string(value) == Some(id)))
        }
        "start" => in_list.list.iter().any(
            |expr| matches!(expr, Expr::Literal(value, _) if integer(value) == Some(variant.start)),
        ),
        "end" => in_list.list.iter().any(
            |expr| matches!(expr, Expr::Literal(value, _) if integer(value) == Some(variant.end)),
        ),
        _ => return true,
    };
    if in_list.negated { !matches } else { matches }
}

fn compare_string(value: Option<&str>, literal: &ScalarValue, operator: &Operator) -> bool {
    let Some(value) = value else {
        return false;
    };
    let Some(literal) = string(literal) else {
        return true;
    };
    match operator {
        Operator::Eq => value == literal,
        Operator::NotEq => value != literal,
        _ => true,
    }
}

fn compare_integer(value: u64, literal: &ScalarValue, operator: &Operator) -> bool {
    let Some(literal) = integer(literal) else {
        return true;
    };
    match operator {
        Operator::Eq => value == literal,
        Operator::NotEq => value != literal,
        Operator::Lt => value < literal,
        Operator::LtEq => value <= literal,
        Operator::Gt => value > literal,
        Operator::GtEq => value >= literal,
        _ => true,
    }
}

fn is_string(value: &ScalarValue) -> bool {
    string(value).is_some()
}

fn string(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Some(value),
        _ => None,
    }
}

fn is_integer(value: &ScalarValue) -> bool {
    integer(value).is_some()
}

fn integer(value: &ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::UInt8(Some(value)) => Some((*value).into()),
        ScalarValue::UInt16(Some(value)) => Some((*value).into()),
        ScalarValue::UInt32(Some(value)) => Some((*value).into()),
        ScalarValue::UInt64(Some(value)) => Some(*value),
        ScalarValue::Int8(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int16(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int32(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int64(Some(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::{col, lit};

    use super::*;

    fn variant() -> PvarRow<'static> {
        PvarRow {
            chrom: "1",
            start: 9,
            end: 10,
            id: Some("rs1"),
        }
    }

    #[test]
    fn supports_and_evaluates_catalog_filters() {
        let filter = col("chrom")
            .eq(lit("1"))
            .and(col("start").between(lit(5_u64), lit(10_u64)));
        assert!(supports_exact_filter(&filter));
        assert!(evaluate_exact_filter(variant(), &filter));
        assert!(supports_exact_filter(
            &col("id").in_list(vec![lit("rs1"), lit("rs2")], false)
        ));
        assert!(!supports_exact_filter(&col("ref").eq(lit("A"))));
    }

    #[test]
    fn preserves_sql_null_semantics_for_negated_id_lists() {
        let mut without_id = variant();
        without_id.id = None;
        let filter = col("id").in_list(vec![lit("rs2")], true);
        assert!(supports_exact_filter(&filter));
        assert!(!evaluate_exact_filter(without_id, &filter));
    }
}
