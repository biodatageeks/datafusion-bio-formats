//! Scalar UDFs for analytical queries on VCF columnar genotype lists.
//!
//! These UDFs operate on `List<T>` columns produced by the multi-sample columnar
//! genotypes schema, enabling bcftools-style filtering in SQL.

use datafusion::arrow::array::{
    Array, AsArray, BooleanArray, BooleanBuilder, Float32Array, Float64Builder, Int32Array,
    ListArray, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::TypeSignature::Exact;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

// ============================================================================
// list_avg — average of non-null elements in a list
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListAvgUdf {
    signature: Signature,
}

impl ListAvgUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Int32,
                        true,
                    )))]),
                    Exact(vec![DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Float32,
                        true,
                    )))]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ListAvgUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let array = args.args[0].clone().into_array(1)?;
        let list = array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("list_avg expects List input".to_string()))?;
        let mut builder = Float64Builder::with_capacity(list.len());
        for i in 0..list.len() {
            if list.is_null(i) {
                builder.append_null();
                continue;
            }
            let inner = list.value(i);
            let (sum, count) = compute_sum_count(&inner);
            if count == 0 {
                builder.append_null();
            } else {
                builder.append_value(sum / count as f64);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

fn compute_sum_count(array: &dyn Array) -> (f64, usize) {
    let mut sum = 0.0_f64;
    let mut count = 0_usize;
    if let Some(int_arr) = array.as_any().downcast_ref::<Int32Array>() {
        for i in 0..int_arr.len() {
            if !int_arr.is_null(i) {
                sum += int_arr.value(i) as f64;
                count += 1;
            }
        }
    } else if let Some(float_arr) = array.as_any().downcast_ref::<Float32Array>() {
        for i in 0..float_arr.len() {
            if !float_arr.is_null(i) {
                sum += float_arr.value(i) as f64;
                count += 1;
            }
        }
    }
    (sum, count)
}

/// Creates the `list_avg` scalar UDF.
pub fn list_avg_udf() -> ScalarUDF {
    ScalarUDF::from(ListAvgUdf::new())
}

// ============================================================================
// list_gte — element-wise >= comparison: List<Int32>, Int32 → List<Boolean>
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListGteUdf {
    signature: Signature,
}

impl ListGteUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                        DataType::Int32,
                    ]),
                    Exact(vec![
                        DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                        DataType::Float32,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ListGteUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_gte"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Boolean,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let list_arr = args.args[0].clone().into_array(1)?;
        let threshold_arr = args.args[1].clone().into_array(1)?;
        let list = list_arr.as_list::<i32>();
        let mut builder = ListBuilder::new(BooleanBuilder::new());

        for i in 0..list.len() {
            if list.is_null(i) {
                builder.append(false);
                continue;
            }
            let inner = list.value(i);
            if let Some(int_arr) = inner.as_any().downcast_ref::<Int32Array>() {
                let threshold = threshold_arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .map(|a| a.value(if a.len() == 1 { 0 } else { i }))
                    .unwrap_or(0);
                for j in 0..int_arr.len() {
                    if int_arr.is_null(j) {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(int_arr.value(j) >= threshold);
                    }
                }
            } else if let Some(float_arr) = inner.as_any().downcast_ref::<Float32Array>() {
                let threshold = threshold_arr
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .map(|a| a.value(if a.len() == 1 { 0 } else { i }))
                    .unwrap_or(0.0);
                for j in 0..float_arr.len() {
                    if float_arr.is_null(j) {
                        builder.values().append_null();
                    } else {
                        builder
                            .values()
                            .append_value(float_arr.value(j) >= threshold);
                    }
                }
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `list_gte` scalar UDF.
pub fn list_gte_udf() -> ScalarUDF {
    ScalarUDF::from(ListGteUdf::new())
}

// ============================================================================
// list_lte — element-wise <= comparison: List<Int32>, Int32 → List<Boolean>
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListLteUdf {
    signature: Signature,
}

impl ListLteUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                        DataType::Int32,
                    ]),
                    Exact(vec![
                        DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                        DataType::Float32,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ListLteUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_lte"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Boolean,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let list_arr = args.args[0].clone().into_array(1)?;
        let threshold_arr = args.args[1].clone().into_array(1)?;
        let list = list_arr.as_list::<i32>();
        let mut builder = ListBuilder::new(BooleanBuilder::new());

        for i in 0..list.len() {
            if list.is_null(i) {
                builder.append(false);
                continue;
            }
            let inner = list.value(i);
            if let Some(int_arr) = inner.as_any().downcast_ref::<Int32Array>() {
                let threshold = threshold_arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .map(|a| a.value(if a.len() == 1 { 0 } else { i }))
                    .unwrap_or(0);
                for j in 0..int_arr.len() {
                    if int_arr.is_null(j) {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(int_arr.value(j) <= threshold);
                    }
                }
            } else if let Some(float_arr) = inner.as_any().downcast_ref::<Float32Array>() {
                let threshold = threshold_arr
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .map(|a| a.value(if a.len() == 1 { 0 } else { i }))
                    .unwrap_or(0.0);
                for j in 0..float_arr.len() {
                    if float_arr.is_null(j) {
                        builder.values().append_null();
                    } else {
                        builder
                            .values()
                            .append_value(float_arr.value(j) <= threshold);
                    }
                }
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `list_lte` scalar UDF.
pub fn list_lte_udf() -> ScalarUDF {
    ScalarUDF::from(ListLteUdf::new())
}

// ============================================================================
// list_and — element-wise AND: List<Boolean>, List<Boolean> → List<Boolean>
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct ListAndUdf {
    signature: Signature,
}

impl ListAndUdf {
    fn new() -> Self {
        let bool_list = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));
        Self {
            signature: Signature::exact(vec![bool_list.clone(), bool_list], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ListAndUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_and"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Boolean,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let left_arr = args.args[0].clone().into_array(1)?;
        let right_arr = args.args[1].clone().into_array(1)?;
        let left = left_arr.as_list::<i32>();
        let right = right_arr.as_list::<i32>();
        let mut builder = ListBuilder::new(BooleanBuilder::new());

        for i in 0..left.len() {
            if left.is_null(i) || right.is_null(i) {
                builder.append(false);
                continue;
            }
            let left_inner = left.value(i);
            let right_inner = right.value(i);
            let left_bools = left_inner.as_any().downcast_ref::<BooleanArray>().unwrap();
            let right_bools = right_inner.as_any().downcast_ref::<BooleanArray>().unwrap();
            let len = left_bools.len().min(right_bools.len());
            for j in 0..len {
                if left_bools.is_null(j) || right_bools.is_null(j) {
                    builder.values().append_null();
                } else {
                    builder
                        .values()
                        .append_value(left_bools.value(j) && right_bools.value(j));
                }
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `list_and` scalar UDF.
pub fn list_and_udf() -> ScalarUDF {
    ScalarUDF::from(ListAndUdf::new())
}

// ============================================================================
// mask_gt — set GT to "." where mask is false
// ============================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
struct MaskGtUdf {
    signature: Signature,
}

impl MaskGtUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MaskGtUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mask_gt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let gt_arr = args.args[0].clone().into_array(1)?;
        let mask_arr = args.args[1].clone().into_array(1)?;
        let gt_list = gt_arr.as_list::<i32>();
        let mask_list = mask_arr.as_list::<i32>();
        let mut builder = ListBuilder::new(StringBuilder::new());

        for i in 0..gt_list.len() {
            if gt_list.is_null(i) {
                builder.append(false);
                continue;
            }
            let gt_inner = gt_list.value(i);
            let gt_strings = gt_inner.as_any().downcast_ref::<StringArray>().unwrap();

            let mask_bools = if !mask_list.is_null(i) {
                let mask_inner = mask_list.value(i);
                Some(
                    mask_inner
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .clone(),
                )
            } else {
                None
            };

            for j in 0..gt_strings.len() {
                let keep = mask_bools
                    .as_ref()
                    .is_some_and(|m| j < m.len() && !m.is_null(j) && m.value(j));
                if keep {
                    if gt_strings.is_null(j) {
                        builder.values().append_value(".");
                    } else {
                        builder.values().append_value(gt_strings.value(j));
                    }
                } else {
                    builder.values().append_value(".");
                }
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Creates the `mask_gt` scalar UDF.
pub fn mask_gt_udf() -> ScalarUDF {
    ScalarUDF::from(MaskGtUdf::new())
}

// ============================================================================
// Registration helper
// ============================================================================

/// Registers all VCF analytical UDFs with the given SessionContext.
///
/// Available UDFs:
/// - `list_avg(List<Int32|Float32>) -> Float64` — average of non-null list elements
/// - `list_gte(List<Int32|Float32>, T) -> List<Boolean>` — element-wise >=
/// - `list_lte(List<Int32|Float32>, T) -> List<Boolean>` — element-wise <=
/// - `list_and(List<Boolean>, List<Boolean>) -> List<Boolean>` — element-wise AND
/// - `mask_gt(List<Utf8>, List<Boolean>) -> List<Utf8>` — set GT to "." where mask is false
pub fn register_vcf_udfs(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udf(list_avg_udf());
    ctx.register_udf(list_gte_udf());
    ctx.register_udf(list_lte_udf());
    ctx.register_udf(list_and_udf());
    ctx.register_udf(mask_gt_udf());
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Builder, ListBuilder};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;

    async fn make_test_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        register_vcf_udfs(&ctx);

        // Create a test batch with List<Int32> columns
        let mut gq_builder = ListBuilder::new(Int32Builder::new());
        gq_builder.values().append_value(30);
        gq_builder.values().append_value(20);
        gq_builder.values().append_value(10);
        gq_builder.append(true);
        gq_builder.values().append_value(5);
        gq_builder.values().append_null();
        gq_builder.values().append_value(15);
        gq_builder.append(true);

        let mut dp_builder = ListBuilder::new(Int32Builder::new());
        dp_builder.values().append_value(50);
        dp_builder.values().append_value(30);
        dp_builder.values().append_value(20);
        dp_builder.append(true);
        dp_builder.values().append_value(10);
        dp_builder.values().append_value(200);
        dp_builder.values().append_value(100);
        dp_builder.append(true);

        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("0/1");
        gt_builder.values().append_value("1/1");
        gt_builder.values().append_value("0/0");
        gt_builder.append(true);
        gt_builder.values().append_value("./.");
        gt_builder.values().append_value("0/1");
        gt_builder.values().append_value("1/1");
        gt_builder.append(true);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "GQ",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new(
                "DP",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new(
                "GT",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(gq_builder.finish()),
                Arc::new(dp_builder.finish()),
                Arc::new(gt_builder.finish()),
            ],
        )
        .unwrap();

        ctx.register_batch("test_data", batch).unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_list_avg_int32() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT list_avg("GQ") as avg_gq FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let result = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        // Row 0: avg(30, 20, 10) = 20.0
        assert!((result.value(0) - 20.0).abs() < 0.001);
        // Row 1: avg(5, 15) = 10.0 (null skipped)
        assert!((result.value(1) - 10.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_list_gte() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT list_gte("GQ", 15) as gq_gte FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        // Row 0: [30>=15, 20>=15, 10>=15] = [true, true, false]
        let inner0 = list.value(0);
        let bools0 = inner0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools0.value(0));
        assert!(bools0.value(1));
        assert!(!bools0.value(2));
    }

    #[tokio::test]
    async fn test_mask_gt() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT mask_gt("GT", list_gte("GQ", 15)) as masked FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        // Row 0: GQ=[30,20,10], gte(15)=[T,T,F] → GT=["0/1","1/1","."]
        let inner0 = list.value(0);
        let strings0 = inner0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings0.value(0), "0/1");
        assert_eq!(strings0.value(1), "1/1");
        assert_eq!(strings0.value(2), ".");
    }

    #[tokio::test]
    async fn test_list_and() {
        let ctx = make_test_ctx().await;
        let df = ctx
            .sql(r#"SELECT list_and(list_gte("GQ", 10), list_lte("DP", 100)) as combined FROM test_data"#)
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        // Row 0: GQ=[30,20,10], DP=[50,30,20]
        // gte(GQ,10)=[T,T,T], lte(DP,100)=[T,T,T] → and=[T,T,T]
        let inner0 = list.value(0);
        let bools0 = inner0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools0.value(0));
        assert!(bools0.value(1));
        assert!(bools0.value(2));
        // Row 1: GQ=[5,null,15], DP=[10,200,100]
        // gte(GQ,10)=[F,null,T], lte(DP,100)=[T,F,T] → and=[F,null,T]
        let inner1 = list.value(1);
        let bools1 = inner1.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bools1.value(0));
        assert!(bools1.is_null(1));
        assert!(bools1.value(2));
    }
}
