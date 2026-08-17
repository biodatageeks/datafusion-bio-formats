use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Between, Expr, Operator, expr::InList};

use crate::catalog::BgenVariant;

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
            match column.name.as_str() {
                "chrom" | "id" | "rsid" => {
                    string(literal).is_some() && matches!(binary.op, Operator::Eq | Operator::NotEq)
                }
                "start" | "end" => {
                    integer(literal).is_some()
                        && matches!(
                            binary.op,
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
        Expr::Between(between) => supports_between(between),
        Expr::InList(in_list) => supports_in_list(in_list),
        _ => false,
    }
}

/// Whether a predicate reads the variant identifier.
///
/// The identifier is the one filterable field a BGI does not record, so a
/// predicate naming it decides whether planning has to parse variant records
/// before it can filter exactly.
pub(crate) fn references_id(expr: &Expr) -> bool {
    let mut found = false;
    expr.apply(|node| {
        if matches!(node, Expr::Column(column) if column.name == "id") {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking an expression cannot fail");
    found
}

pub(crate) fn evaluate_exact_filter(variant: &BgenVariant, expr: &Expr) -> bool {
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
            match column.name.as_str() {
                "chrom" => compare_string(Some(&variant.chrom), literal, &binary.op),
                // A variant identifier is only known once its record has been
                // parsed. `scan` resolves every candidate before filtering when
                // a predicate names `id`, so an unresolved variant does not
                // reach here; if one did, keeping it is the direction that lets
                // a later filter still reject it.
                "id" => variant
                    .id()
                    .is_none_or(|id| compare_string(id, literal, &binary.op)),
                "rsid" => compare_string(variant.rsid.as_deref(), literal, &binary.op),
                "start" => compare_integer(variant.start, literal, &binary.op),
                "end" => compare_integer(variant.end, literal, &binary.op),
                _ => true,
            }
        }
        Expr::Between(between) => evaluate_between(variant, between),
        Expr::InList(in_list) => evaluate_in_list(variant, in_list),
        _ => true,
    }
}

fn supports_between(between: &Between) -> bool {
    matches!(&*between.expr, Expr::Column(column) if matches!(column.name.as_str(), "start" | "end"))
        && matches!(&*between.low, Expr::Literal(value, _) if integer(value).is_some())
        && matches!(&*between.high, Expr::Literal(value, _) if integer(value).is_some())
}

fn supports_in_list(in_list: &InList) -> bool {
    let Expr::Column(column) = &*in_list.expr else {
        return false;
    };
    match column.name.as_str() {
        "chrom" | "id" | "rsid" => in_list
            .list
            .iter()
            .all(|expr| matches!(expr, Expr::Literal(value, _) if string(value).is_some())),
        "start" | "end" => in_list
            .list
            .iter()
            .all(|expr| matches!(expr, Expr::Literal(value, _) if integer(value).is_some())),
        _ => false,
    }
}

fn evaluate_between(variant: &BgenVariant, between: &Between) -> bool {
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
    let (Some(low), Some(high)) = (integer(low), integer(high)) else {
        return true;
    };
    let matches = value >= low && value <= high;
    if between.negated { !matches } else { matches }
}

fn evaluate_in_list(variant: &BgenVariant, in_list: &InList) -> bool {
    let Expr::Column(column) = &*in_list.expr else {
        return true;
    };
    let matches = match column.name.as_str() {
        "chrom" => contains_string(in_list, &variant.chrom),
        "id" => match variant.id() {
            // Unresolved: see the note in `evaluate_exact_filter`.
            None => return true,
            Some(None) => return false,
            Some(Some(value)) => contains_string(in_list, value),
        },
        "rsid" => {
            let Some(value) = variant.rsid.as_deref() else {
                return false;
            };
            contains_string(in_list, value)
        }
        "start" => contains_integer(in_list, variant.start),
        "end" => contains_integer(in_list, variant.end),
        _ => return true,
    };
    if in_list.negated { !matches } else { matches }
}

fn contains_string(in_list: &InList, value: &str) -> bool {
    in_list
        .list
        .iter()
        .any(|expr| matches!(expr, Expr::Literal(literal, _) if string(literal) == Some(value)))
}

fn contains_integer(in_list: &InList, value: u64) -> bool {
    in_list
        .list
        .iter()
        .any(|expr| matches!(expr, Expr::Literal(literal, _) if integer(literal) == Some(value)))
}

fn compare_string(value: Option<&str>, literal: &ScalarValue, operator: &Operator) -> bool {
    let (Some(value), Some(literal)) = (value, string(literal)) else {
        return false;
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

fn string(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Some(value),
        _ => None,
    }
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
    use crate::catalog::VariantDetail;
    use datafusion::logical_expr::{col, lit};

    use super::*;

    fn variant(id: Option<&str>) -> BgenVariant {
        BgenVariant {
            index: 0,
            rsid: None,
            chrom: "1".to_string(),
            start: 9,
            end: 10,
            position: 10,
            alleles: vec!["A".to_string(), "C".to_string()],
            allele_count: 2,
            record_offset: 0,
            record_size: 0,
            detail: VariantDetail::Parsed {
                id: id.map(str::to_string),
                payload_offset: 0,
                payload_size: 0,
            },
        }
    }

    #[test]
    fn nullable_not_in_preserves_sql_filter_semantics() {
        let filter = col("id").in_list(vec![lit("v1")], true);
        assert!(supports_exact_filter(&filter));
        assert!(!evaluate_exact_filter(&variant(None), &filter));
        assert!(!evaluate_exact_filter(&variant(Some("v1")), &filter));
        assert!(evaluate_exact_filter(&variant(Some("v2")), &filter));
    }
}
