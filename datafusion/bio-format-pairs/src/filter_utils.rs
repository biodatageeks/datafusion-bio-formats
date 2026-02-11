//! Pairs-specific filter analysis for index and residual pushdown.
//!
//! Pairs files have two coordinate dimensions (chr1/pos1 and chr2/pos2).
//! Tabix can only index on chr1/pos1, so:
//! - `chr1`/`pos1` filters → index pushdown (via tabix)
//! - `chr2`/`pos2`/other column filters → residual (record-level evaluation)

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};
use datafusion_bio_format_core::genomic_filter::GenomicRegion;

/// Result of analyzing Pairs filter expressions.
#[derive(Debug, Clone)]
pub struct PairsFilterAnalysis {
    /// Regions queryable via tabix index (derived from chr1/pos1 filters)
    pub regions: Vec<GenomicRegion>,
    /// All filters that should be applied at the record level (includes chr2/pos2)
    pub residual_filters: Vec<Expr>,
}

/// Analyze filter expressions and extract chr1/pos1 regions for tabix queries.
///
/// Filters on `chr1` and `pos1` are mapped to genomic regions for index lookups.
/// All other filters (including chr2, pos2, strand, etc.) become residual filters.
pub fn extract_pairs_genomic_regions(
    filters: &[Expr],
    coordinate_system_zero_based: bool,
) -> PairsFilterAnalysis {
    let mut chroms: Vec<String> = Vec::new();
    let mut start_lower: Option<u64> = None;
    let mut end_upper: Option<u64> = None;
    let mut residual_filters: Vec<Expr> = Vec::new();

    for filter in filters {
        collect_pairs_constraints(
            filter,
            &mut chroms,
            &mut start_lower,
            &mut end_upper,
            &mut residual_filters,
            coordinate_system_zero_based,
        );
    }

    chroms.sort();
    chroms.dedup();

    let regions = if chroms.is_empty() {
        Vec::new()
    } else {
        chroms
            .into_iter()
            .map(|chrom| GenomicRegion {
                chrom,
                start: start_lower,
                end: end_upper,
                unmapped_tail: false,
            })
            .collect()
    };

    PairsFilterAnalysis {
        regions,
        residual_filters,
    }
}

/// Check if a filter expression involves pairs index columns (chr1, pos1).
pub fn is_pairs_index_filter(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            if matches!(binary_expr.op, Operator::And) {
                is_pairs_index_filter(&binary_expr.left)
                    || is_pairs_index_filter(&binary_expr.right)
            } else if let Expr::Column(col) = &*binary_expr.left {
                matches!(col.name.as_str(), "chr1" | "pos1")
            } else {
                false
            }
        }
        Expr::Between(between) => {
            if let Expr::Column(col) = &*between.expr {
                matches!(col.name.as_str(), "pos1")
            } else {
                false
            }
        }
        Expr::InList(in_list) => {
            if let Expr::Column(col) = &*in_list.expr {
                matches!(col.name.as_str(), "chr1")
            } else {
                false
            }
        }
        _ => false,
    }
}

fn collect_pairs_constraints(
    expr: &Expr,
    chroms: &mut Vec<String>,
    start_lower: &mut Option<u64>,
    end_upper: &mut Option<u64>,
    residual_filters: &mut Vec<Expr>,
    coordinate_system_zero_based: bool,
) {
    match expr {
        Expr::BinaryExpr(binary_expr) if matches!(binary_expr.op, Operator::And) => {
            collect_pairs_constraints(
                &binary_expr.left,
                chroms,
                start_lower,
                end_upper,
                residual_filters,
                coordinate_system_zero_based,
            );
            collect_pairs_constraints(
                &binary_expr.right,
                chroms,
                start_lower,
                end_upper,
                residual_filters,
                coordinate_system_zero_based,
            );
        }
        Expr::BinaryExpr(binary_expr) => {
            if let Expr::Column(col) = &*binary_expr.left {
                if let Expr::Literal(scalar, _) = &*binary_expr.right {
                    match col.name.as_str() {
                        "chr1" => {
                            if binary_expr.op == Operator::Eq {
                                if let Some(s) = scalar_to_string(scalar) {
                                    chroms.push(s);
                                    return;
                                }
                            }
                            residual_filters.push(expr.clone());
                        }
                        "pos1" => {
                            if let Some(val) = scalar_to_u64(scalar) {
                                let val_1based = if coordinate_system_zero_based {
                                    val + 1
                                } else {
                                    val
                                };
                                match binary_expr.op {
                                    Operator::Eq => {
                                        *start_lower = Some(
                                            start_lower.map_or(val_1based, |v| v.max(val_1based)),
                                        );
                                    }
                                    Operator::Gt => {
                                        *start_lower = Some(
                                            start_lower
                                                .map_or(val_1based + 1, |v| v.max(val_1based + 1)),
                                        );
                                    }
                                    Operator::GtEq => {
                                        *start_lower = Some(
                                            start_lower.map_or(val_1based, |v| v.max(val_1based)),
                                        );
                                    }
                                    Operator::Lt => {
                                        let upper = val_1based.saturating_sub(1);
                                        *end_upper =
                                            Some(end_upper.map_or(upper, |v| v.min(upper)));
                                    }
                                    Operator::LtEq => {
                                        *end_upper = Some(
                                            end_upper.map_or(val_1based, |v| v.min(val_1based)),
                                        );
                                    }
                                    _ => {
                                        residual_filters.push(expr.clone());
                                    }
                                }
                                return;
                            }
                            residual_filters.push(expr.clone());
                        }
                        // chr2, pos2, strand1, strand2, etc. → residual
                        _ => {
                            residual_filters.push(expr.clone());
                        }
                    }
                } else {
                    residual_filters.push(expr.clone());
                }
            } else {
                residual_filters.push(expr.clone());
            }
        }
        Expr::Between(between) => {
            if let Expr::Column(col) = &*between.expr {
                if col.name == "pos1" && !between.negated {
                    if let (Expr::Literal(low, _), Expr::Literal(high, _)) =
                        (&*between.low, &*between.high)
                    {
                        if let (Some(low_val), Some(high_val)) =
                            (scalar_to_u64(low), scalar_to_u64(high))
                        {
                            let low_1based = if coordinate_system_zero_based {
                                low_val + 1
                            } else {
                                low_val
                            };
                            let high_1based = if coordinate_system_zero_based {
                                high_val + 1
                            } else {
                                high_val
                            };
                            *start_lower =
                                Some(start_lower.map_or(low_1based, |v| v.max(low_1based)));
                            *end_upper =
                                Some(end_upper.map_or(high_1based, |v| v.min(high_1based)));
                            return;
                        }
                    }
                }
            }
            residual_filters.push(expr.clone());
        }
        Expr::InList(in_list) => {
            if let Expr::Column(col) = &*in_list.expr {
                if col.name == "chr1" && !in_list.negated {
                    let extracted: Vec<String> = in_list
                        .list
                        .iter()
                        .filter_map(|e| {
                            if let Expr::Literal(scalar, _) = e {
                                scalar_to_string(scalar)
                            } else {
                                None
                            }
                        })
                        .collect();
                    if !extracted.is_empty() {
                        chroms.extend(extracted);
                        return;
                    }
                }
            }
            residual_filters.push(expr.clone());
        }
        _ => {
            residual_filters.push(expr.clone());
        }
    }
}

fn scalar_to_string(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => Some(s.clone()),
        ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

fn scalar_to_u64(scalar: &ScalarValue) -> Option<u64> {
    match scalar {
        ScalarValue::UInt32(Some(v)) => Some(*v as u64),
        ScalarValue::Int32(Some(v)) if *v >= 0 => Some(*v as u64),
        ScalarValue::Int64(Some(v)) if *v >= 0 => Some(*v as u64),
        ScalarValue::UInt64(Some(v)) => Some(*v),
        _ => None,
    }
}
