use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};

#[derive(Debug, Clone, Default)]
pub(crate) struct SimplePredicate {
    pub chrom: Option<String>,
    pub start_min: Option<i64>,
    pub start_max: Option<i64>,
    pub end_min: Option<i64>,
    pub end_max: Option<i64>,
    pub always_false: bool,
}

impl SimplePredicate {
    pub fn matches(&self, chrom: &str, start: i64, end: i64) -> bool {
        if self.always_false {
            return false;
        }
        if let Some(expected_chrom) = &self.chrom {
            if expected_chrom != chrom {
                return false;
            }
        }
        if let Some(min_start) = self.start_min {
            if start < min_start {
                return false;
            }
        }
        if let Some(max_start) = self.start_max {
            if start > max_start {
                return false;
            }
        }
        if let Some(min_end) = self.end_min {
            if end < min_end {
                return false;
            }
        }
        if let Some(max_end) = self.end_max {
            if end > max_end {
                return false;
            }
        }
        true
    }
}

pub(crate) fn extract_simple_predicate(filters: &[Expr]) -> SimplePredicate {
    let mut predicate = SimplePredicate::default();
    for expr in filters {
        apply_expr(expr, &mut predicate);
    }

    if let (Some(min), Some(max)) = (predicate.start_min, predicate.start_max) {
        if min > max {
            predicate.always_false = true;
        }
    }
    if let (Some(min), Some(max)) = (predicate.end_min, predicate.end_max) {
        if min > max {
            predicate.always_false = true;
        }
    }

    predicate
}

pub(crate) fn is_pushdown_supported(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            if binary_expr.op == Operator::And {
                return is_pushdown_supported(&binary_expr.left)
                    || is_pushdown_supported(&binary_expr.right);
            }

            let Expr::Column(col) = &*binary_expr.left else {
                return false;
            };
            let Expr::Literal(_, _) = &*binary_expr.right else {
                return false;
            };

            match col.name.as_str() {
                "chrom" | "chr" => matches!(binary_expr.op, Operator::Eq),
                "start" | "end" => matches!(
                    binary_expr.op,
                    Operator::Eq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
                ),
                _ => false,
            }
        }
        _ => false,
    }
}

fn apply_expr(expr: &Expr, predicate: &mut SimplePredicate) {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            apply_expr(&binary_expr.left, predicate);
            apply_expr(&binary_expr.right, predicate);
        }
        Expr::BinaryExpr(binary_expr) => {
            let Expr::Column(column) = &*binary_expr.left else {
                return;
            };
            let Expr::Literal(literal, _) = &*binary_expr.right else {
                return;
            };

            match column.name.as_str() {
                "chrom" | "chr" => {
                    if binary_expr.op == Operator::Eq {
                        if let Some(chrom) = scalar_to_string(literal) {
                            if let Some(existing) = &predicate.chrom {
                                if existing != &chrom {
                                    predicate.always_false = true;
                                }
                            }
                            predicate.chrom = Some(chrom);
                        }
                    }
                }
                "start" => {
                    if let Some(value) = scalar_to_i64(literal) {
                        match binary_expr.op {
                            Operator::Eq => {
                                update_min(&mut predicate.start_min, value);
                                update_max(&mut predicate.start_max, value);
                            }
                            Operator::Gt => update_min(&mut predicate.start_min, value + 1),
                            Operator::GtEq => update_min(&mut predicate.start_min, value),
                            Operator::Lt => update_max(&mut predicate.start_max, value - 1),
                            Operator::LtEq => update_max(&mut predicate.start_max, value),
                            _ => {}
                        }
                    }
                }
                "end" => {
                    if let Some(value) = scalar_to_i64(literal) {
                        match binary_expr.op {
                            Operator::Eq => {
                                update_min(&mut predicate.end_min, value);
                                update_max(&mut predicate.end_max, value);
                            }
                            Operator::Gt => update_min(&mut predicate.end_min, value + 1),
                            Operator::GtEq => update_min(&mut predicate.end_min, value),
                            Operator::Lt => update_max(&mut predicate.end_max, value - 1),
                            Operator::LtEq => update_max(&mut predicate.end_max, value),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
}

fn update_min(target: &mut Option<i64>, value: i64) {
    *target = Some(target.map_or(value, |existing| existing.max(value)));
}

fn update_max(target: &mut Option<i64>, value: i64) {
    *target = Some(target.map_or(value, |existing| existing.min(value)));
}

fn scalar_to_string(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
        _ => None,
    }
}

fn scalar_to_i64(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        _ => None,
    }
}
