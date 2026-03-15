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
        if let Some(expected_chrom) = &self.chrom
            && expected_chrom != chrom
        {
            return false;
        }
        if let Some(min_start) = self.start_min
            && start < min_start
        {
            return false;
        }
        if let Some(max_start) = self.start_max
            && start > max_start
        {
            return false;
        }
        if let Some(min_end) = self.end_min
            && end < min_end
        {
            return false;
        }
        if let Some(max_end) = self.end_max
            && end > max_end
        {
            return false;
        }
        true
    }
}

pub(crate) fn extract_simple_predicate(filters: &[Expr]) -> SimplePredicate {
    let mut predicate = SimplePredicate::default();
    for expr in filters {
        apply_expr(expr, &mut predicate);
    }

    if let (Some(min), Some(max)) = (predicate.start_min, predicate.start_max)
        && min > max
    {
        predicate.always_false = true;
    }
    if let (Some(min), Some(max)) = (predicate.end_min, predicate.end_max)
        && min > max
    {
        predicate.always_false = true;
    }

    predicate
}

pub(crate) fn is_pushdown_supported(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            // If either side of an AND has a pushable predicate, signal Inexact
            // so DataFusion extracts what we can push. DataFusion always re-applies
            // the full expression post-scan for Inexact filters.
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
                    if binary_expr.op == Operator::Eq
                        && let Some(chrom) = scalar_to_string(literal)
                    {
                        if let Some(existing) = &predicate.chrom
                            && existing != &chrom
                        {
                            predicate.always_false = true;
                        }
                        predicate.chrom = Some(chrom);
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::BinaryExpr;

    fn col_eq_str(col_name: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name(col_name))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(value.to_string())),
                None,
            )),
        })
    }

    fn col_cmp_i64(col_name: &str, op: Operator, value: i64) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name(col_name))),
            op,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(value)), None)),
        })
    }

    fn and(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::And,
            right: Box::new(right),
        })
    }

    // -----------------------------------------------------------------------
    // SimplePredicate::matches
    // -----------------------------------------------------------------------

    #[test]
    fn matches_empty_predicate_matches_all() {
        let pred = SimplePredicate::default();
        assert!(pred.matches("chr1", 100, 200));
    }

    #[test]
    fn matches_always_false() {
        let pred = SimplePredicate {
            always_false: true,
            ..Default::default()
        };
        assert!(!pred.matches("chr1", 100, 200));
    }

    #[test]
    fn matches_chrom_filter() {
        let pred = SimplePredicate {
            chrom: Some("chr1".to_string()),
            ..Default::default()
        };
        assert!(pred.matches("chr1", 100, 200));
        assert!(!pred.matches("chr2", 100, 200));
    }

    #[test]
    fn matches_start_range() {
        let pred = SimplePredicate {
            start_min: Some(50),
            start_max: Some(150),
            ..Default::default()
        };
        assert!(pred.matches("chr1", 50, 200));
        assert!(pred.matches("chr1", 150, 200));
        assert!(!pred.matches("chr1", 49, 200));
        assert!(!pred.matches("chr1", 151, 200));
    }

    #[test]
    fn matches_end_range() {
        let pred = SimplePredicate {
            end_min: Some(100),
            end_max: Some(300),
            ..Default::default()
        };
        assert!(pred.matches("chr1", 50, 100));
        assert!(pred.matches("chr1", 50, 300));
        assert!(!pred.matches("chr1", 50, 99));
        assert!(!pred.matches("chr1", 50, 301));
    }

    #[test]
    fn matches_combined() {
        let pred = SimplePredicate {
            chrom: Some("chr1".to_string()),
            start_min: Some(100),
            end_max: Some(500),
            ..Default::default()
        };
        assert!(pred.matches("chr1", 100, 500));
        assert!(!pred.matches("chr2", 100, 500));
        assert!(!pred.matches("chr1", 99, 500));
        assert!(!pred.matches("chr1", 100, 501));
    }

    // -----------------------------------------------------------------------
    // extract_simple_predicate
    // -----------------------------------------------------------------------

    #[test]
    fn extract_chrom_eq() {
        let filters = vec![col_eq_str("chrom", "chr1")];
        let pred = extract_simple_predicate(&filters);
        assert_eq!(pred.chrom.as_deref(), Some("chr1"));
        assert!(!pred.always_false);
    }

    #[test]
    fn extract_chr_alias() {
        let filters = vec![col_eq_str("chr", "22")];
        let pred = extract_simple_predicate(&filters);
        assert_eq!(pred.chrom.as_deref(), Some("22"));
    }

    #[test]
    fn extract_start_gt() {
        let filters = vec![col_cmp_i64("start", Operator::Gt, 100)];
        let pred = extract_simple_predicate(&filters);
        assert_eq!(pred.start_min, Some(101));
    }

    #[test]
    fn extract_start_gte() {
        let filters = vec![col_cmp_i64("start", Operator::GtEq, 100)];
        let pred = extract_simple_predicate(&filters);
        assert_eq!(pred.start_min, Some(100));
    }

    #[test]
    fn extract_start_lt() {
        let filters = vec![col_cmp_i64("start", Operator::Lt, 200)];
        let pred = extract_simple_predicate(&filters);
        assert_eq!(pred.start_max, Some(199));
    }

    #[test]
    fn extract_start_lte() {
        let filters = vec![col_cmp_i64("start", Operator::LtEq, 200)];
        let pred = extract_simple_predicate(&filters);
        assert_eq!(pred.start_max, Some(200));
    }

    #[test]
    fn extract_start_eq() {
        let filters = vec![col_cmp_i64("start", Operator::Eq, 150)];
        let pred = extract_simple_predicate(&filters);
        assert_eq!(pred.start_min, Some(150));
        assert_eq!(pred.start_max, Some(150));
    }

    #[test]
    fn extract_end_range() {
        let filters = vec![
            col_cmp_i64("end", Operator::GtEq, 100),
            col_cmp_i64("end", Operator::LtEq, 500),
        ];
        let pred = extract_simple_predicate(&filters);
        assert_eq!(pred.end_min, Some(100));
        assert_eq!(pred.end_max, Some(500));
    }

    #[test]
    fn extract_contradictory_start_always_false() {
        let filters = vec![
            col_cmp_i64("start", Operator::GtEq, 200),
            col_cmp_i64("start", Operator::LtEq, 100),
        ];
        let pred = extract_simple_predicate(&filters);
        assert!(pred.always_false);
    }

    #[test]
    fn extract_contradictory_end_always_false() {
        let filters = vec![
            col_cmp_i64("end", Operator::GtEq, 500),
            col_cmp_i64("end", Operator::LtEq, 100),
        ];
        let pred = extract_simple_predicate(&filters);
        assert!(pred.always_false);
    }

    #[test]
    fn extract_contradictory_chrom_always_false() {
        let filters = vec![col_eq_str("chrom", "chr1"), col_eq_str("chrom", "chr2")];
        let pred = extract_simple_predicate(&filters);
        assert!(pred.always_false);
    }

    #[test]
    fn extract_and_expression() {
        let expr = and(
            col_eq_str("chrom", "chr1"),
            col_cmp_i64("start", Operator::GtEq, 100),
        );
        let pred = extract_simple_predicate(&[expr]);
        assert_eq!(pred.chrom.as_deref(), Some("chr1"));
        assert_eq!(pred.start_min, Some(100));
    }

    #[test]
    fn extract_ignores_unknown_columns() {
        let filters = vec![col_eq_str("gene", "BRCA1")];
        let pred = extract_simple_predicate(&filters);
        assert!(pred.chrom.is_none());
        assert!(!pred.always_false);
    }

    #[test]
    fn extract_empty_filters() {
        let pred = extract_simple_predicate(&[]);
        assert!(pred.chrom.is_none());
        assert!(pred.start_min.is_none());
        assert!(!pred.always_false);
    }

    #[test]
    fn extract_tightest_min_wins() {
        let filters = vec![
            col_cmp_i64("start", Operator::GtEq, 50),
            col_cmp_i64("start", Operator::GtEq, 100),
        ];
        let pred = extract_simple_predicate(&filters);
        assert_eq!(pred.start_min, Some(100));
    }

    #[test]
    fn extract_tightest_max_wins() {
        let filters = vec![
            col_cmp_i64("start", Operator::LtEq, 500),
            col_cmp_i64("start", Operator::LtEq, 300),
        ];
        let pred = extract_simple_predicate(&filters);
        assert_eq!(pred.start_max, Some(300));
    }

    // -----------------------------------------------------------------------
    // is_pushdown_supported
    // -----------------------------------------------------------------------

    #[test]
    fn pushdown_chrom_eq() {
        assert!(is_pushdown_supported(&col_eq_str("chrom", "chr1")));
    }

    #[test]
    fn pushdown_chr_alias() {
        assert!(is_pushdown_supported(&col_eq_str("chr", "22")));
    }

    #[test]
    fn pushdown_start_gte() {
        assert!(is_pushdown_supported(&col_cmp_i64(
            "start",
            Operator::GtEq,
            100
        )));
    }

    #[test]
    fn pushdown_end_lt() {
        assert!(is_pushdown_supported(&col_cmp_i64(
            "end",
            Operator::Lt,
            200
        )));
    }

    #[test]
    fn pushdown_unsupported_column() {
        assert!(!is_pushdown_supported(&col_eq_str("gene", "BRCA1")));
    }

    #[test]
    fn pushdown_chrom_gt_unsupported() {
        assert!(!is_pushdown_supported(&col_cmp_i64(
            "chrom",
            Operator::Gt,
            100
        )));
    }

    #[test]
    fn pushdown_and_with_one_supported() {
        let expr = and(col_eq_str("chrom", "chr1"), col_eq_str("gene", "BRCA1"));
        assert!(is_pushdown_supported(&expr));
    }

    #[test]
    fn pushdown_and_with_none_supported() {
        let expr = and(
            col_eq_str("gene", "BRCA1"),
            col_eq_str("biotype", "protein_coding"),
        );
        assert!(!is_pushdown_supported(&expr));
    }

    // -----------------------------------------------------------------------
    // scalar_to_string / scalar_to_i64
    // -----------------------------------------------------------------------

    #[test]
    fn scalar_utf8_to_string() {
        assert_eq!(
            scalar_to_string(&ScalarValue::Utf8(Some("hello".to_string()))),
            Some("hello".to_string())
        );
    }

    #[test]
    fn scalar_large_utf8_to_string() {
        assert_eq!(
            scalar_to_string(&ScalarValue::LargeUtf8(Some("hello".to_string()))),
            Some("hello".to_string())
        );
    }

    #[test]
    fn scalar_null_utf8_none() {
        assert_eq!(scalar_to_string(&ScalarValue::Utf8(None)), None);
    }

    #[test]
    fn scalar_int64_to_i64() {
        assert_eq!(scalar_to_i64(&ScalarValue::Int64(Some(42))), Some(42));
    }

    #[test]
    fn scalar_int32_to_i64() {
        assert_eq!(scalar_to_i64(&ScalarValue::Int32(Some(42))), Some(42));
    }

    #[test]
    fn scalar_uint64_to_i64() {
        assert_eq!(scalar_to_i64(&ScalarValue::UInt64(Some(42))), Some(42));
    }

    #[test]
    fn scalar_uint32_to_i64() {
        assert_eq!(scalar_to_i64(&ScalarValue::UInt32(Some(42))), Some(42));
    }

    #[test]
    fn scalar_null_i64_none() {
        assert_eq!(scalar_to_i64(&ScalarValue::Int64(None)), None);
    }

    #[test]
    fn scalar_bool_not_i64() {
        assert_eq!(scalar_to_i64(&ScalarValue::Boolean(Some(true))), None);
    }
}
