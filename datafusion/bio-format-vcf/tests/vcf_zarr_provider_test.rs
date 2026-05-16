use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Int8Array, Int32Array, Int64Array, ListArray, StringArray,
    StructArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::{ExecutionPlanProperties, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::metadata::{
    VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY, VCF_FIELD_NUMBER_KEY, VCF_FIELD_TYPE_KEY,
    VCF_FILE_FORMAT_KEY, VCF_GENOTYPES_SAMPLE_NAMES_KEY, VCF_SAMPLE_NAMES_KEY, from_json_string,
};
use datafusion_bio_format_vcf::zarr::{
    SUPPORTED_VCF_ZARR_VERSION, VcfZarrReadOptions, VcfZarrTableProvider, describe_fields,
};
use futures::TryStreamExt;
use zarrs::array::Array as ZarrArray;
use zarrs::config::MetadataRetrieveVersion;
use zarrs::filesystem::FilesystemStore;

fn write_v2_root_metadata(root: &std::path::Path, attributes: &str) {
    std::fs::create_dir_all(root).expect("store root should be created");
    std::fs::write(root.join(".zgroup"), "{\"zarr_format\":2}")
        .expect("root group metadata should be written");
    std::fs::write(root.join(".zattrs"), attributes).expect("root attributes should be written");
}

fn write_zarray_metadata(root: &std::path::Path, array_name: &str) {
    let array_path = root.join(array_name);
    std::fs::create_dir_all(&array_path).expect("array directory should be created");
    std::fs::write(
        array_path.join(".zarray"),
        r#"{
  "shape": [0],
  "chunks": [1],
  "dtype": "<i4",
  "fill_value": 0,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}"#,
    )
    .expect("array metadata should be written");
}

fn write_required_zarray_metadata(root: &std::path::Path) {
    for array_name in [
        "variant_contig",
        "variant_position",
        "contig_id",
        "variant_allele",
    ] {
        write_zarray_metadata(root, array_name);
    }
}

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).expect("destination directory should be created");
    for entry in std::fs::read_dir(src).expect("source directory should be readable") {
        let entry = entry.expect("directory entry should be readable");
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir(&src_path, &dst_path);
        } else {
            std::fs::copy(&src_path, &dst_path).expect("fixture file should be copied");
        }
    }
}

fn copy_multi_chrom_with_samples(root: &std::path::Path) {
    copy_dir(
        std::path::Path::new("tests/data/vcf_zarr/multi_chrom.vcz"),
        root,
    );
    let sample_id = root.join("sample_id");
    if sample_id.exists() {
        std::fs::remove_dir_all(&sample_id).expect("sample_id fixture directory should be removed");
    }
    copy_dir(&root.join("contig_id"), &sample_id);
    std::fs::write(
        sample_id.join(".zattrs"),
        r#"{"_ARRAY_DIMENSIONS":["samples"]}"#,
    )
    .expect("sample_id dimensions should be written");
}

fn write_i32_2d_array<F>(
    root: &std::path::Path,
    array_name: &str,
    rows: usize,
    samples: usize,
    value: F,
) where
    F: Fn(usize, usize) -> i32,
{
    let array_path = root.join(array_name);
    std::fs::create_dir_all(&array_path).expect("array directory should be created");
    std::fs::write(
        array_path.join(".zarray"),
        format!(
            r#"{{
  "shape": [{rows}, {samples}],
  "chunks": [{rows}, {samples}],
  "dtype": "<i4",
  "fill_value": -1,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}}"#
        ),
    )
    .expect("array metadata should be written");
    std::fs::write(
        array_path.join(".zattrs"),
        r#"{"_ARRAY_DIMENSIONS":["variants","samples"]}"#,
    )
    .expect("array attrs should be written");

    let mut bytes = Vec::with_capacity(rows * samples * std::mem::size_of::<i32>());
    for row in 0..rows {
        for sample in 0..samples {
            bytes.extend_from_slice(&value(row, sample).to_le_bytes());
        }
    }
    std::fs::write(array_path.join("0.0"), bytes).expect("array chunk should be written");
}

fn write_u64_2d_array<F>(
    root: &std::path::Path,
    array_name: &str,
    rows: usize,
    width: usize,
    value: F,
) where
    F: Fn(usize, usize) -> u64,
{
    let array_path = root.join(array_name);
    std::fs::create_dir_all(&array_path).expect("array directory should be created");
    std::fs::write(
        array_path.join(".zarray"),
        format!(
            r#"{{
  "shape": [{rows}, {width}],
  "chunks": [{rows}, {width}],
  "dtype": "<u8",
  "fill_value": 0,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}}"#
        ),
    )
    .expect("array metadata should be written");
    std::fs::write(
        array_path.join(".zattrs"),
        r#"{"_ARRAY_DIMENSIONS":["variants","values"]}"#,
    )
    .expect("array attrs should be written");

    let mut bytes = Vec::with_capacity(rows * width * std::mem::size_of::<u64>());
    for row in 0..rows {
        for item in 0..width {
            bytes.extend_from_slice(&value(row, item).to_le_bytes());
        }
    }
    std::fs::write(array_path.join("0.0"), bytes).expect("array chunk should be written");
}

fn write_i32_3d_array<F>(
    root: &std::path::Path,
    array_name: &str,
    rows: usize,
    samples: usize,
    width: usize,
    value: F,
) where
    F: Fn(usize, usize, usize) -> i32,
{
    let array_path = root.join(array_name);
    std::fs::create_dir_all(&array_path).expect("array directory should be created");
    std::fs::write(
        array_path.join(".zarray"),
        format!(
            r#"{{
  "shape": [{rows}, {samples}, {width}],
  "chunks": [{rows}, {samples}, {width}],
  "dtype": "<i4",
  "fill_value": -1,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}}"#
        ),
    )
    .expect("array metadata should be written");
    std::fs::write(
        array_path.join(".zattrs"),
        r#"{"_ARRAY_DIMENSIONS":["variants","samples","ploidy"]}"#,
    )
    .expect("array attrs should be written");

    let mut bytes = Vec::with_capacity(rows * samples * width * std::mem::size_of::<i32>());
    for row in 0..rows {
        for sample in 0..samples {
            for item in 0..width {
                bytes.extend_from_slice(&value(row, sample, item).to_le_bytes());
            }
        }
    }
    std::fs::write(array_path.join("0.0.0"), bytes).expect("array chunk should be written");
}

fn write_bool_2d_array<F>(
    root: &std::path::Path,
    array_name: &str,
    rows: usize,
    samples: usize,
    value: F,
) where
    F: Fn(usize, usize) -> bool,
{
    let array_path = root.join(array_name);
    std::fs::create_dir_all(&array_path).expect("array directory should be created");
    std::fs::write(
        array_path.join(".zarray"),
        format!(
            r#"{{
  "shape": [{rows}, {samples}],
  "chunks": [{rows}, {samples}],
  "dtype": "|b1",
  "fill_value": false,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}}"#
        ),
    )
    .expect("array metadata should be written");
    std::fs::write(
        array_path.join(".zattrs"),
        r#"{"_ARRAY_DIMENSIONS":["variants","samples"]}"#,
    )
    .expect("array attrs should be written");

    let mut bytes = Vec::with_capacity(rows * samples);
    for row in 0..rows {
        for sample in 0..samples {
            bytes.push(u8::from(value(row, sample)));
        }
    }
    std::fs::write(array_path.join("0.0"), bytes).expect("array chunk should be written");
}

fn write_malformed_region_index_metadata(root: &std::path::Path) {
    let array_path = root.join("region_index");
    if array_path.exists() {
        std::fs::remove_dir_all(&array_path)
            .expect("existing region_index directory should be removed");
    }
    std::fs::create_dir_all(&array_path).expect("region_index directory should be created");
    std::fs::write(
        array_path.join(".zarray"),
        r#"{
  "shape": [1, 5],
  "chunks": [1, 5],
  "dtype": "<i4",
  "fill_value": 0,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}"#,
    )
    .expect("malformed region_index metadata should be written");
    std::fs::write(
        array_path.join(".zattrs"),
        r#"{"_ARRAY_DIMENSIONS":["region_index_values","region_index_fields"]}"#,
    )
    .expect("region_index attrs should be written");
}

fn write_malformed_sample_id_metadata(root: &std::path::Path) {
    let sample_id = root.join("sample_id");
    if sample_id.exists() {
        std::fs::remove_dir_all(&sample_id).expect("sample_id directory should be removed");
    }
    std::fs::create_dir_all(&sample_id).expect("sample_id directory should be created");
    std::fs::write(
        sample_id.join(".zarray"),
        r#"{
  "shape": [1, 1],
  "chunks": [1, 1],
  "dtype": "<i4",
  "fill_value": 0,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}"#,
    )
    .expect("sample_id metadata should be written");
}

fn write_malformed_array_metadata(root: &std::path::Path, array_name: &str) {
    let array_path = root.join(array_name);
    if array_path.exists() {
        std::fs::remove_dir_all(&array_path).expect("array directory should be removed");
    }
    std::fs::create_dir_all(&array_path).expect("array directory should be created");
    std::fs::write(array_path.join(".zarray"), "{not valid json")
        .expect("malformed array metadata should be written");
}

fn write_corrupt_chunk(root: &std::path::Path, array_name: &str, chunk_name: &str) {
    std::fs::write(
        root.join(array_name).join(chunk_name),
        b"not a valid zarr chunk",
    )
    .expect("corrupt chunk should be written");
}

fn sampled_format_store() -> tempfile::TempDir {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("sampled.vcz");
    copy_multi_chrom_with_samples(&root);
    write_i32_2d_array(&root, "call_GT", 1000, 2, |row, sample| {
        ((sample + 1) * 10_000 + row) as i32
    });
    write_i32_2d_array(&root, "call_DP", 1000, 2, |row, sample| {
        ((sample + 1) * 1_000 + row) as i32
    });
    temp_dir
}

#[test]
fn vcf_zarr_describe_fields_matches_vcf_describe_shape() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");
    let batch =
        describe_fields(root.to_string_lossy().into_owned()).expect("fixture should describe");

    assert_eq!(batch.schema().field(0).name(), "name");
    assert_eq!(batch.schema().field(1).name(), "field_type");
    assert_eq!(batch.schema().field(2).name(), "data_type");
    assert_eq!(batch.schema().field(3).name(), "description");
    assert_eq!(batch.num_columns(), 4);
    assert!(batch.num_rows() >= 3);

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name should be Utf8");
    let field_types = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("field_type should be Utf8");
    let data_types = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("data_type should be Utf8");
    let rows = (0..batch.num_rows())
        .map(|index| {
            (
                field_types.value(index),
                names.value(index),
                data_types.value(index),
            )
        })
        .collect::<Vec<_>>();

    assert!(rows.contains(&("INFO", "DP", "Integer")));
    assert!(rows.contains(&("FORMAT", "genotypes", "Struct")));
    assert!(!rows.iter().any(|row| row.0 == "FORMAT" && row.1 == "DP"));
    assert!(!rows.iter().any(|row| row.0 == "FORMAT" && row.1 == "GT"));
}

fn assert_i32_list_values(list: &ListArray, row: usize, expected: &[i32]) {
    let row_values = list.value(row);
    let values = row_values
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("list values should be Int32");
    assert_eq!(values.len(), expected.len());
    for (index, expected_value) in expected.iter().enumerate() {
        assert_eq!(values.value(index), *expected_value);
    }
}

fn assert_bool_list_values(list: &ListArray, row: usize, expected: &[bool]) {
    let row_values = list.value(row);
    let values = row_values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("list values should be Boolean");
    assert_eq!(values.len(), expected.len());
    for (index, expected_value) in expected.iter().enumerate() {
        assert_eq!(values.value(index), *expected_value);
    }
}

fn assert_nested_i32_list_values(list: &ListArray, row: usize, expected: &[&[i32]]) {
    let row_values = list.value(row);
    let sample_lists = row_values
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("row values should be nested List<Int32>");
    assert_eq!(sample_lists.len(), expected.len());
    for (sample, expected_values) in expected.iter().enumerate() {
        assert_i32_list_values(sample_lists, sample, expected_values);
    }
}

fn open_multi_chrom_provider(options: VcfZarrReadOptions) -> VcfZarrTableProvider {
    VcfZarrTableProvider::new("tests/data/vcf_zarr/multi_chrom.vcz".to_string(), options)
        .expect("fixture should open")
}

fn copy_multi_chrom_without_region_index() -> tempfile::TempDir {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("no_region_index.vcz");
    copy_dir(
        std::path::Path::new("tests/data/vcf_zarr/multi_chrom.vcz"),
        &root,
    );
    std::fs::remove_dir_all(root.join("region_index"))
        .expect("region_index directory should be removed");
    temp_dir
}

fn copy_multi_chrom_with_variant_position_chunk_size(chunk_size: usize) -> tempfile::TempDir {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("chunked.vcz");
    copy_dir(
        std::path::Path::new("tests/data/vcf_zarr/multi_chrom.vcz"),
        &root,
    );
    rewrite_variant_position_chunks(&root, chunk_size);

    temp_dir
}

fn copy_multi_chrom_without_region_index_with_variant_position_chunk_size(
    chunk_size: usize,
) -> tempfile::TempDir {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("no_region_index_chunked.vcz");
    copy_dir(
        std::path::Path::new("tests/data/vcf_zarr/multi_chrom.vcz"),
        &root,
    );
    std::fs::remove_dir_all(root.join("region_index"))
        .expect("region_index directory should be removed");
    rewrite_variant_position_chunks(&root, chunk_size);

    temp_dir
}

fn rewrite_variant_position_chunks(root: &std::path::Path, chunk_size: usize) {
    assert!(chunk_size > 0, "test chunk size must be nonzero");
    let store = Arc::new(FilesystemStore::new(root).expect("copied fixture store should open"));
    let variant_position =
        ZarrArray::open_opt(store, "/variant_position", &MetadataRetrieveVersion::V2)
            .expect("variant_position should open before rechunking");
    let row_count = variant_position.shape()[0];
    let row_range = std::iter::once(0..row_count).collect::<Vec<_>>();
    let values: Vec<i32> = variant_position
        .retrieve_array_subset(&zarrs::array::ArraySubset::new_with_ranges(&row_range))
        .expect("variant_position values should be readable before rechunking");

    let variant_position_path = root.join("variant_position");
    let zarray_path = root.join("variant_position/.zarray");
    std::fs::write(
        zarray_path,
        format!(
            r#"{{
  "shape": [
    {}
  ],
  "chunks": [
    {chunk_size}
  ],
  "dtype": "<i4",
  "fill_value": 0,
  "order": "C",
  "filters": null,
  "dimension_separator": ".",
  "compressor": null,
  "zarr_format": 2
}}"#,
            values.len()
        ),
    )
    .expect("variant_position chunk metadata should be updated");

    for (chunk_index, chunk) in values.chunks(chunk_size).enumerate() {
        let mut bytes = Vec::with_capacity(std::mem::size_of_val(chunk));
        for value in chunk {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        std::fs::write(variant_position_path.join(chunk_index.to_string()), bytes)
            .expect("variant_position raw chunk should be written");
    }
}

fn assert_field(field: &Field, name: &str, data_type: &DataType, nullable: bool) {
    assert_eq!(field.name(), name);
    assert_eq!(field.data_type(), data_type);
    assert_eq!(
        field.is_nullable(),
        nullable,
        "{name} nullability should be {nullable}"
    );
}

async fn collect_chrom_start_rows(
    root: &std::path::Path,
    target_partitions: usize,
    sql: &str,
) -> Vec<(String, u32)> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(target_partitions),
    );
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("fixture should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();

    let mut rows = Vec::new();
    for batch in batches {
        let chrom = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("chrom column should be a StringArray");
        let start = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("start column should be a UInt32Array");
        for row in 0..batch.num_rows() {
            rows.push((chrom.value(row).to_string(), start.value(row)));
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn vcf_zarr_parallel_partitions_match_single_partition_projected_rows() {
    let temp_dir = copy_multi_chrom_with_variant_position_chunk_size(100);
    let root = temp_dir.path().join("chunked.vcz");
    let sql = "SELECT chrom, start FROM vcz";

    let single_partition = collect_chrom_start_rows(&root, 1, sql).await;
    let parallel_partitions = collect_chrom_start_rows(&root, 4, sql).await;

    assert_eq!(single_partition.len(), 1000);
    assert_eq!(parallel_partitions, single_partition);
}

#[tokio::test]
async fn vcf_zarr_parallel_fallback_pruning_matches_single_partition_without_region_index() {
    let temp_dir = copy_multi_chrom_without_region_index_with_variant_position_chunk_size(100);
    let root = temp_dir.path().join("no_region_index_chunked.vcz");
    let sql = "SELECT chrom, start FROM vcz WHERE start >= 5000200 AND start <= 5000800";

    let single_partition = collect_chrom_start_rows(&root, 1, sql).await;
    let parallel_partitions = collect_chrom_start_rows(&root, 4, sql).await;

    assert!(!single_partition.is_empty());
    assert_eq!(parallel_partitions, single_partition);
}

#[tokio::test]
async fn vcf_zarr_scan_displays_single_zarr_concurrency_per_partition() {
    let temp_dir = copy_multi_chrom_with_variant_position_chunk_size(100);
    let root = temp_dir.path().join("chunked.vcz");
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let state = ctx.state();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("chunked fixture should open");

    let exec = provider
        .scan(&state, None, &[], None)
        .await
        .expect("scan should build execution plan");
    let plan = displayable(exec.as_ref()).indent(false).to_string();

    assert!(
        plan.contains("partition_count=4") && plan.contains("zarr_concurrency=1"),
        "execution plan should document that zarrs uses one worker per DataFusion partition: {plan}"
    );
}

#[tokio::test]
async fn vcf_zarr_scan_uses_session_batch_size_for_output_batches() {
    let temp_dir = copy_multi_chrom_with_variant_position_chunk_size(100);
    let root = temp_dir.path().join("chunked.vcz");
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(128),
    );
    let state = ctx.state();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("chunked fixture should open");
    let schema = provider.schema();
    let chrom = schema.index_of("chrom").unwrap();
    let start = schema.index_of("start").unwrap();

    let exec = provider
        .scan(&state, Some(&vec![chrom, start]), &[], None)
        .await
        .expect("scan should build execution plan");
    let batches = exec
        .execute(0, ctx.task_ctx())
        .expect("partition should execute")
        .try_collect::<Vec<_>>()
        .await
        .expect("stream should collect");

    assert!(
        batches.len() > 1,
        "batch size should split one physical partition into multiple output batches"
    );
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1000
    );
    assert!(
        batches.iter().all(|batch| batch.num_rows() <= 128),
        "all output batches should honor DataFusion session batch size: {:?}",
        batches
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn vcf_zarr_scan_target_partitions_are_capped_by_selected_chunks() {
    let temp_dir = copy_multi_chrom_with_variant_position_chunk_size(100);
    let root = temp_dir.path().join("chunked.vcz");
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    let state = ctx.state();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("chunked fixture should open");

    let exec = provider
        .scan(&state, None, &[], None)
        .await
        .expect("scan should build execution plan");
    let plan = displayable(exec.as_ref()).indent(false).to_string();

    assert_eq!(exec.output_partitioning().partition_count(), 8);
    assert!(
        plan.contains("partition_count=8")
            && plan.contains("partition_ranges=[0:[0..200], 1:[200..400]"),
        "execution plan should expose eight non-empty chunk-bounded partitions: {plan}"
    );
}

#[tokio::test]
async fn vcf_zarr_scan_avoids_empty_partitions_when_selected_chunks_are_fewer_than_target() {
    let temp_dir = copy_multi_chrom_with_variant_position_chunk_size(100);
    let root = temp_dir.path().join("chunked.vcz");
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
    let state = ctx.state();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("chunked fixture should open");

    let exec = provider
        .scan(&state, None, &[], Some(500))
        .await
        .expect("scan should build execution plan");
    let plan = displayable(exec.as_ref()).indent(false).to_string();

    assert_eq!(exec.output_partitioning().partition_count(), 5);
    assert!(
        plan.contains("partition_count=5")
            && plan.contains("partition_ranges=[0:[0..100], 1:[100..200]")
            && !plan.contains("5:["),
        "execution plan should avoid empty partitions when target slots exceed selected chunks: {plan}"
    );
}

#[test]
fn vcf_zarr_rejects_missing_store() {
    let result = VcfZarrTableProvider::new(
        "/definitely/not/a/store.vcz".to_string(),
        VcfZarrReadOptions::default(),
    );

    let message = result.expect_err("missing store must fail").to_string();
    assert!(
        message.contains("VCF Zarr") && message.contains("not found"),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_requires_version_0_4() {
    let fixture = "tests/data/vcf_zarr/unsupported_version.vcz";
    let result = VcfZarrTableProvider::new(fixture.to_string(), VcfZarrReadOptions::default());

    let message = result
        .expect_err("unsupported vcf_zarr_version must fail")
        .to_string();
    assert!(
        message.contains("unsupported vcf_zarr_version")
            && message.contains("999.0")
            && message.contains("0.4"),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_accepts_version_0_4_fixture() {
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());

    assert_eq!(
        provider
            .schema()
            .metadata()
            .get("bio.vcf.zarr.version")
            .map(String::as_str),
        Some(SUPPORTED_VCF_ZARR_VERSION)
    );
}

#[test]
fn vcf_zarr_reports_malformed_root_metadata() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("malformed.vcz");

    write_v2_root_metadata(&root, "{}");
    std::fs::write(root.join(".zattrs"), "{not json")
        .expect("malformed root attributes should be written");

    let result = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    );
    let message = result
        .expect_err("malformed root metadata must fail")
        .to_string();

    assert!(
        message.contains("Failed to read VCF Zarr root metadata")
            && message.contains(&root.display().to_string()),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_v2_root_attributes_are_authoritative() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("mixed_metadata.vcz");

    write_v2_root_metadata(
        &root,
        &format!("{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\"}}"),
    );
    std::fs::write(
        root.join("zarr.json"),
        r#"{"zarr_format":3,"node_type":"group","attributes":{"vcf_zarr_version":"999.0"}}"#,
    )
    .expect("conflicting v3 group metadata should be written");
    write_required_zarray_metadata(&root);

    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("temp store should open from v2 metadata");

    assert_eq!(
        provider
            .schema()
            .metadata()
            .get("bio.vcf.zarr.version")
            .map(String::as_str),
        Some(SUPPORTED_VCF_ZARR_VERSION)
    );
}

#[test]
fn vcf_zarr_requires_string_vcf_zarr_version_attribute() {
    for attributes in ["{}", "{\"vcf_zarr_version\":0.4}"] {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let root = temp_dir.path().join("bad_version.vcz");
        write_v2_root_metadata(&root, attributes);

        let message = VcfZarrTableProvider::new(
            root.to_str()
                .expect("temp path should be UTF-8")
                .to_string(),
            VcfZarrReadOptions::default(),
        )
        .expect_err("missing or non-string version must fail")
        .to_string();

        assert!(
            message.contains("vcf_zarr_version") && message.contains("missing or not a string"),
            "unexpected error: {message}"
        );
    }
}

#[test]
fn vcf_zarr_requires_core_raw_arrays_for_logical_schema() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("missing_array.vcz");
    write_v2_root_metadata(
        &root,
        &format!("{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\"}}"),
    );
    write_zarray_metadata(&root, "variant_contig");
    write_zarray_metadata(&root, "variant_position");
    write_zarray_metadata(&root, "contig_id");

    let message = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect_err("missing variant_allele array must fail")
    .to_string();

    assert!(
        message.contains("missing required array") && message.contains("variant_allele"),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_schema_exposes_logical_vcf_columns() {
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());

    let schema = provider.schema();

    let expected = [
        ("chrom", DataType::Utf8, false),
        ("start", DataType::UInt32, false),
        ("end", DataType::UInt32, false),
        ("id", DataType::Utf8, true),
        ("ref", DataType::Utf8, false),
        ("alt", DataType::Utf8, false),
        ("qual", DataType::Float64, true),
        ("filter", DataType::Utf8, true),
    ];

    for (index, (name, data_type, nullable)) in expected.iter().enumerate() {
        assert_field(schema.field(index), name, data_type, *nullable);
    }
    assert_field(
        schema.field(schema.index_of("AF").expect("AF should be auto-discovered")),
        "AF",
        &DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
        true,
    );
    assert_field(
        schema.field(schema.index_of("DB").expect("DB should be auto-discovered")),
        "DB",
        &DataType::Boolean,
        true,
    );
    assert_field(
        schema.field(schema.index_of("DP").expect("DP should be auto-discovered")),
        "DP",
        &DataType::Int8,
        true,
    );
    assert!(
        schema.index_of("id_mask").is_err(),
        "auxiliary mask arrays should not be exposed as INFO fields"
    );
    assert_eq!(schema.fields().len(), expected.len() + 3);
}

#[test]
fn vcf_zarr_schema_exposes_requested_info_fields() {
    let provider = open_multi_chrom_provider(VcfZarrReadOptions {
        info_fields: Some(vec!["DP".to_string()]),
        ..Default::default()
    });

    let schema = provider.schema();
    let dp = schema.field(schema.index_of("DP").expect("DP field should exist"));

    assert_field(dp, "DP", &DataType::Int8, true);
    assert_eq!(
        dp.metadata()
            .get(VCF_FIELD_FIELD_TYPE_KEY)
            .map(String::as_str),
        Some("INFO")
    );
    assert_eq!(
        dp.metadata().get(VCF_FIELD_TYPE_KEY).map(String::as_str),
        Some("Integer")
    );
    assert_eq!(
        dp.metadata().get(VCF_FIELD_NUMBER_KEY).map(String::as_str),
        Some(".")
    );
}

#[test]
fn vcf_zarr_schema_auto_discovers_format_fields() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");

    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("sampled FORMAT store should open");

    let schema = provider.schema();
    let genotypes = schema.field(
        schema
            .index_of("genotypes")
            .expect("genotypes should be auto-discovered"),
    );
    let DataType::Struct(children) = genotypes.data_type() else {
        panic!(
            "genotypes should be Struct, got {:?}",
            genotypes.data_type()
        );
    };

    let child_names = children
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(child_names, vec!["GT", "DP"]);
}

#[test]
fn vcf_zarr_schema_rejects_missing_requested_info_field() {
    let result = VcfZarrTableProvider::new(
        "tests/data/vcf_zarr/multi_chrom.vcz".to_string(),
        VcfZarrReadOptions {
            info_fields: Some(vec!["MISSING".to_string()]),
            ..Default::default()
        },
    );

    let message = result
        .expect_err("missing requested INFO array should fail")
        .to_string();
    assert!(
        message.contains("INFO field 'MISSING'") && message.contains("variant_MISSING"),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_schema_rejects_missing_requested_format_fields() {
    let result = VcfZarrTableProvider::new(
        "tests/data/vcf_zarr/multi_chrom.vcz".to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["GT".to_string(), "DP".to_string()]),
            ..Default::default()
        },
    );

    let message = result
        .expect_err("missing requested FORMAT arrays should fail")
        .to_string();
    assert!(
        message.contains("FORMAT field 'GT'") && message.contains("call_GT"),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_schema_exposes_requested_format_fields_when_arrays_exist() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");

    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["GT".to_string(), "DP".to_string()]),
            ..Default::default()
        },
    )
    .expect("store with requested FORMAT arrays should open");

    let schema = provider.schema();
    let genotypes = schema.field(
        schema
            .index_of("genotypes")
            .expect("genotypes field should exist"),
    );
    assert!(genotypes.is_nullable(), "genotypes should be nullable");

    let DataType::Struct(children) = genotypes.data_type() else {
        panic!(
            "genotypes should be Struct, got {:?}",
            genotypes.data_type()
        );
    };
    assert_eq!(children.len(), 2);

    for (index, expected_name) in ["GT", "DP"].iter().enumerate() {
        let child = &children[index];
        assert_eq!(child.name(), expected_name);
        assert!(child.is_nullable(), "{expected_name} should be nullable");

        let DataType::List(item) = child.data_type() else {
            panic!(
                "{expected_name} should be List<Int32>, got {:?}",
                child.data_type()
            );
        };
        assert_eq!(item.data_type(), &DataType::Int32);
        assert!(
            item.is_nullable(),
            "{expected_name} item should be nullable"
        );

        assert_eq!(
            child
                .metadata()
                .get(VCF_FIELD_FIELD_TYPE_KEY)
                .map(String::as_str),
            Some("FORMAT")
        );
        assert_eq!(
            child
                .metadata()
                .get(VCF_FIELD_FORMAT_ID_KEY)
                .map(String::as_str),
            Some(*expected_name)
        );
        assert_eq!(
            child.metadata().get(VCF_FIELD_TYPE_KEY).map(String::as_str),
            Some("Integer")
        );
        assert_eq!(
            child
                .metadata()
                .get(VCF_FIELD_NUMBER_KEY)
                .map(String::as_str),
            Some(".")
        );
    }
}

#[test]
fn vcf_zarr_schema_defaults_spec_gt_to_raw_nested_lists_with_phasing() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("raw_gt_schema.vcz");
    copy_multi_chrom_with_samples(&root);
    write_i32_3d_array(&root, "call_genotype", 1000, 2, 2, |_, _, _| 0);
    write_bool_2d_array(&root, "call_genotype_phased", 1000, 2, |_, sample| {
        sample == 1
    });

    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["GT".to_string()]),
            samples: Some(vec!["22".to_string()]),
            ..Default::default()
        },
    )
    .expect("spec GT store should open");

    let schema = provider.schema();
    let genotypes = schema.field(
        schema
            .index_of("genotypes")
            .expect("genotypes field should exist"),
    );
    let DataType::Struct(children) = genotypes.data_type() else {
        panic!(
            "genotypes should be Struct, got {:?}",
            genotypes.data_type()
        );
    };

    assert_eq!(
        children
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["GT", "GT_phased"]
    );

    let DataType::List(samples) = children[0].data_type() else {
        panic!("GT should be List<List<Int32>>");
    };
    let DataType::List(ploidy) = samples.data_type() else {
        panic!("GT sample item should be List<Int32>");
    };
    assert_eq!(ploidy.data_type(), &DataType::Int32);

    let DataType::List(phased_item) = children[1].data_type() else {
        panic!("GT_phased should be List<Boolean>");
    };
    assert_eq!(phased_item.data_type(), &DataType::Boolean);
}

#[test]
fn vcf_zarr_schema_can_request_string_gt_encoding() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("string_gt_schema.vcz");
    copy_multi_chrom_with_samples(&root);
    write_i32_3d_array(&root, "call_genotype", 1000, 2, 2, |_, _, _| 0);
    write_bool_2d_array(&root, "call_genotype_phased", 1000, 2, |_, sample| {
        sample == 1
    });

    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["GT".to_string()]),
            samples: Some(vec!["22".to_string()]),
            genotype_encoding_raw: false,
            ..Default::default()
        },
    )
    .expect("spec GT store should open");

    let schema = provider.schema();
    let genotypes = schema.field(
        schema
            .index_of("genotypes")
            .expect("genotypes field should exist"),
    );
    let DataType::Struct(children) = genotypes.data_type() else {
        panic!(
            "genotypes should be Struct, got {:?}",
            genotypes.data_type()
        );
    };
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].name(), "GT");

    let DataType::List(item) = children[0].data_type() else {
        panic!("GT should be List<Utf8>");
    };
    assert_eq!(item.data_type(), &DataType::Utf8);
}

#[test]
fn vcf_zarr_schema_resolves_requested_sample_subset() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");

    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["GT".to_string(), "DP".to_string()]),
            samples: Some(vec![
                "22".to_string(),
                "missing".to_string(),
                "21".to_string(),
            ]),
            ..Default::default()
        },
    )
    .expect("sampled FORMAT store should open");

    let schema = provider.schema();
    let sample_names_json = schema
        .metadata()
        .get(VCF_SAMPLE_NAMES_KEY)
        .expect("schema sample metadata should exist");
    let sample_names: Vec<String> =
        from_json_string(sample_names_json).expect("sample metadata should be JSON");
    assert_eq!(sample_names, vec!["22".to_string(), "21".to_string()]);

    let genotypes = schema.field(
        schema
            .index_of("genotypes")
            .expect("genotypes field should exist"),
    );
    let genotype_sample_names_json = genotypes
        .metadata()
        .get(VCF_GENOTYPES_SAMPLE_NAMES_KEY)
        .expect("genotypes sample metadata should exist");
    let genotype_sample_names: Vec<String> = from_json_string(genotype_sample_names_json)
        .expect("genotypes sample metadata should be JSON");
    assert_eq!(genotype_sample_names, sample_names);
}

#[test]
fn vcf_zarr_schema_rejects_malformed_sample_id_shape() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("bad_sample_id.vcz");
    copy_dir(
        std::path::Path::new("tests/data/vcf_zarr/multi_chrom.vcz"),
        &root,
    );
    if root.join("sample_id").exists() {
        std::fs::remove_dir_all(root.join("sample_id"))
            .expect("sample_id fixture directory should be removed");
    }
    write_i32_2d_array(&root, "call_DP", 1000, 2, |row, sample| {
        ((sample + 1) * 1_000 + row) as i32
    });
    write_malformed_sample_id_metadata(&root);

    let message = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["DP".to_string()]),
            ..Default::default()
        },
    )
    .expect_err("malformed sample_id must fail during schema construction")
    .to_string();

    assert!(
        message.contains("sample_id") && message.contains("1-dimensional"),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_schema_rejects_requested_samples_without_sample_id() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("missing_sample_id.vcz");
    copy_dir(
        std::path::Path::new("tests/data/vcf_zarr/multi_chrom.vcz"),
        &root,
    );
    if root.join("sample_id").exists() {
        std::fs::remove_dir_all(root.join("sample_id"))
            .expect("sample_id fixture directory should be removed");
    }
    write_i32_2d_array(&root, "call_DP", 1000, 2, |row, sample| {
        ((sample + 1) * 1_000 + row) as i32
    });

    let message = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["DP".to_string()]),
            samples: Some(vec!["sample_0".to_string()]),
            ..Default::default()
        },
    )
    .expect_err("requested sample names require sample_id")
    .to_string();

    assert!(
        message.contains("sample_id") && message.contains("requested sample"),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_schema_omits_genotypes_for_empty_format_fields() {
    let provider = open_multi_chrom_provider(VcfZarrReadOptions {
        format_fields: Some(vec![]),
        ..Default::default()
    });

    assert!(provider.schema().index_of("genotypes").is_err());
}

#[test]
fn vcf_zarr_schema_metadata_uses_zarr_and_vcf_metadata() {
    let options = VcfZarrReadOptions::default();
    let expected_coordinate_system = options.coordinate_system_zero_based.to_string();
    let provider = open_multi_chrom_provider(options);
    let schema = provider.schema();
    let metadata = schema.metadata();

    assert_eq!(
        metadata.get("bio.vcf.zarr.version").map(String::as_str),
        Some(SUPPORTED_VCF_ZARR_VERSION)
    );
    assert_eq!(
        metadata
            .get(COORDINATE_SYSTEM_METADATA_KEY)
            .map(String::as_str),
        Some(expected_coordinate_system.as_str())
    );
    assert_eq!(
        metadata.get(VCF_FILE_FORMAT_KEY).map(String::as_str),
        Some("VCFv4.3")
    );

    let sample_names_json = metadata
        .get(VCF_SAMPLE_NAMES_KEY)
        .expect("sample names metadata should exist");
    let sample_names: Vec<String> =
        from_json_string(sample_names_json).expect("sample names metadata should be JSON");
    assert!(sample_names.is_empty());
}

#[test]
fn vcf_zarr_schema_metadata_uses_explicit_coordinate_option() {
    let options = VcfZarrReadOptions {
        coordinate_system_zero_based: true,
        ..Default::default()
    };
    let provider = open_multi_chrom_provider(options);
    assert_eq!(
        provider
            .schema()
            .metadata()
            .get(COORDINATE_SYSTEM_METADATA_KEY)
            .map(String::as_str),
        Some("true")
    );
}

#[test]
fn vcf_zarr_schema_metadata_prefers_root_fileformat_over_fallback() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("fileformat_4_2.vcz");
    write_v2_root_metadata(
        &root,
        &format!(
            "{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\",\"vcf_meta_information\":[[\"fileformat\",\"VCFv4.2\"]]}}"
        ),
    );
    write_required_zarray_metadata(&root);

    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("store with required arrays should open");

    assert_eq!(
        provider
            .schema()
            .metadata()
            .get(VCF_FILE_FORMAT_KEY)
            .map(String::as_str),
        Some("VCFv4.2")
    );
}

#[tokio::test]
async fn vcf_zarr_scan_plan_includes_filter_column_raw_arrays() {
    let ctx = SessionContext::new();
    let state = ctx.state();
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());
    let schema = provider.schema();
    let chrom = schema.index_of("chrom").unwrap();
    let filter = col("start").gt_eq(lit(5_000_200u32));

    let exec = provider
        .scan(&state, Some(&vec![chrom]), &[filter], None)
        .await
        .expect("scan should build execution plan");
    let plan = displayable(exec.as_ref()).indent(false).to_string();

    assert!(
        plan.contains("raw_arrays=[contig_id, region_index, variant_contig, variant_position]")
            && plan.contains("pruning=region_index"),
        "execution plan should expose raw arrays needed by projection and filter: {plan}"
    );
}

#[tokio::test]
async fn vcf_zarr_scan_without_region_index_uses_position_array_pruning() {
    let temp_dir = copy_multi_chrom_without_region_index();
    let root = temp_dir.path().join("no_region_index.vcz");

    let ctx = SessionContext::new();
    let state = ctx.state();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("fixture without region_index should open");
    let schema = provider.schema();
    let chrom = schema.index_of("chrom").unwrap();
    let start = schema.index_of("start").unwrap();
    let filter = col("start")
        .gt_eq(lit(5_000_200u32))
        .and(col("start").lt_eq(lit(5_000_300u32)));

    let exec = provider
        .scan(&state, Some(&vec![chrom, start]), &[filter], None)
        .await
        .expect("scan should build execution plan");
    let plan = displayable(exec.as_ref()).indent(false).to_string();

    assert!(
        plan.contains("pruning=position_arrays")
            && plan.contains("row_ranges=[0..1000]")
            && !plan.contains("region_index"),
        "execution plan should defer exact fallback pruning to execution: {plan}"
    );
}

#[tokio::test]
async fn vcf_zarr_collects_fallback_pruned_limit_without_region_index() {
    let temp_dir = copy_multi_chrom_without_region_index();
    let root = temp_dir.path().join("no_region_index.vcz");
    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("fixture without region_index should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql(
            "SELECT chrom, start FROM vcz \
             WHERE start >= 5000200 AND start <= 5000400 \
             LIMIT 1",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    assert_eq!(batches[0].schema().field(0).name(), "chrom");
    assert_eq!(batches[0].schema().field(1).name(), "start");
}

#[tokio::test]
async fn vcf_zarr_scan_rejects_malformed_region_index_shape() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("bad_region_index.vcz");
    copy_dir(
        std::path::Path::new("tests/data/vcf_zarr/multi_chrom.vcz"),
        &root,
    );
    write_malformed_region_index_metadata(&root);

    let ctx = SessionContext::new();
    let state = ctx.state();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("fixture with malformed region_index metadata should open");
    let filter = col("start").eq(lit(5_000_100u32));

    let message = provider
        .scan(&state, None, &[filter], None)
        .await
        .expect_err("malformed region_index must fail during pruning")
        .to_string();

    assert!(
        message.contains("region_index") && message.contains("shape"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn vcf_zarr_scan_prunes_start_range_to_candidate_rows() {
    let ctx = SessionContext::new();
    let state = ctx.state();
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());
    let schema = provider.schema();
    let chrom = schema.index_of("chrom").unwrap();
    let start = schema.index_of("start").unwrap();
    let filter = col("start")
        .gt_eq(lit(5_000_200u32))
        .and(col("start").lt_eq(lit(5_000_300u32)));

    let exec = provider
        .scan(&state, Some(&vec![chrom, start]), &[filter], None)
        .await
        .expect("scan should build execution plan");
    let plan = displayable(exec.as_ref()).indent(false).to_string();

    assert!(
        plan.contains("row_ranges=[1..3]") && plan.contains("rows=2"),
        "execution plan should expose predicate-pruned candidate rows: {plan}"
    );
}

#[tokio::test]
async fn vcf_zarr_scan_applies_limit_after_region_pruning() {
    let ctx = SessionContext::new();
    let state = ctx.state();
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());
    let schema = provider.schema();
    let chrom = schema.index_of("chrom").unwrap();
    let start = schema.index_of("start").unwrap();
    let filter = col("start")
        .gt_eq(lit(5_000_200u32))
        .and(col("start").lt_eq(lit(5_000_400u32)));

    let exec = provider
        .scan(&state, Some(&vec![chrom, start]), &[filter], Some(1))
        .await
        .expect("scan should build execution plan");
    let plan = displayable(exec.as_ref()).indent(false).to_string();

    assert!(
        plan.contains("pruning=region_index")
            && plan.contains("row_ranges=[1..2]")
            && plan.contains("rows=1"),
        "execution plan should apply limit to pruned rows: {plan}"
    );
}

#[tokio::test]
async fn vcf_zarr_collects_projected_core_columns() {
    let ctx = SessionContext::new();
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT chrom, start FROM vcz LIMIT 5")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 5);
    assert_eq!(batches[0].schema().field(0).name(), "chrom");
    assert_eq!(batches[0].schema().field(1).name(), "start");
}

#[tokio::test]
async fn vcf_zarr_collects_filtered_projection_without_filter_column() {
    let ctx = SessionContext::new();
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT chrom FROM vcz WHERE start >= 5000200 AND start <= 5000300")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "chrom");
}

#[tokio::test]
async fn vcf_zarr_collects_all_core_columns() {
    let ctx = SessionContext::new();
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT chrom, start, \"end\", id, ref, alt, qual, filter FROM vcz LIMIT 3")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
    assert_eq!(batches[0].num_columns(), 8);
}

#[tokio::test]
async fn vcf_zarr_collects_requested_info_field() {
    let ctx = SessionContext::new();
    let provider = open_multi_chrom_provider(VcfZarrReadOptions {
        info_fields: Some(vec!["DP".to_string()]),
        ..Default::default()
    });

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT chrom, start, \"DP\" FROM vcz LIMIT 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(batches[0].schema().field(2).name(), "DP");
    assert_eq!(batches[0].schema().field(2).data_type(), &DataType::Int8);
    let dp = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int8Array>()
        .expect("DP should be an Int8 array");
    assert_eq!(dp.len(), 2);
}

#[tokio::test]
async fn vcf_zarr_collects_auto_discovered_info_field() {
    let ctx = SessionContext::new();
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT chrom, start, \"DP\" FROM vcz LIMIT 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(batches[0].schema().field(2).name(), "DP");
    assert_eq!(batches[0].schema().field(2).data_type(), &DataType::Int8);
}

#[tokio::test]
async fn vcf_zarr_collects_list_valued_float_info_as_typed_lists() {
    let ctx = SessionContext::new();
    let provider = open_multi_chrom_provider(VcfZarrReadOptions {
        info_fields: Some(vec!["AF".to_string()]),
        ..Default::default()
    });

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT \"AF\" FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
    );
    let af = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("AF should be a List<Float32>");
    let row = af.value(0);
    let values = row
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("AF list values should be Float32");
    assert_eq!(values.len(), 1);
}

#[tokio::test]
async fn vcf_zarr_info_projection_does_not_read_unprojected_format_chunks() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            info_fields: Some(vec!["DP".to_string()]),
            format_fields: Some(vec!["DP".to_string()]),
            samples: Some(vec!["22".to_string()]),
            ..Default::default()
        },
    )
    .expect("schema should open before corrupting unprojected FORMAT chunks");

    write_corrupt_chunk(&root, "call_DP", "0.0");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT \"DP\" FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "DP");
}

#[tokio::test]
async fn vcf_zarr_format_projection_does_not_read_unprojected_info_chunks() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            info_fields: Some(vec!["DP".to_string()]),
            format_fields: Some(vec!["DP".to_string()]),
            samples: Some(vec!["22".to_string()]),
            ..Default::default()
        },
    )
    .expect("schema should open before corrupting unprojected INFO chunks");

    write_corrupt_chunk(&root, "variant_DP", "0");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT genotypes FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct array");
    assert!(genotypes.column_by_name("DP").is_some());
}

#[tokio::test]
async fn vcf_zarr_core_projection_does_not_read_info_or_format_chunks() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            info_fields: Some(vec!["DP".to_string()]),
            format_fields: Some(vec!["DP".to_string()]),
            samples: Some(vec!["22".to_string()]),
            ..Default::default()
        },
    )
    .expect("schema should open before corrupting unprojected chunks");

    write_corrupt_chunk(&root, "variant_DP", "0");
    write_corrupt_chunk(&root, "call_DP", "0.0");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT chrom, start FROM vcz LIMIT 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(batches[0].num_columns(), 2);
    assert_eq!(batches[0].schema().field(0).name(), "chrom");
    assert_eq!(batches[0].schema().field(1).name(), "start");
}

#[tokio::test]
async fn vcf_zarr_count_star_uses_empty_projection_with_row_count() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            info_fields: Some(vec!["DP".to_string()]),
            format_fields: Some(vec!["DP".to_string()]),
            samples: Some(vec!["22".to_string()]),
            ..Default::default()
        },
    )
    .expect("schema should open before corrupting chunks unused by COUNT(*)");

    write_corrupt_chunk(&root, "variant_DP", "0");
    write_corrupt_chunk(&root, "call_DP", "0.0");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT COUNT(*) AS n FROM vcz")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) should be Int64");
    assert_eq!(count.value(0), 1000);
}

#[tokio::test]
async fn vcf_zarr_collects_requested_info_field_without_opening_unrequested_info_arrays() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("info_pruned.vcz");
    copy_dir(
        std::path::Path::new("tests/data/vcf_zarr/multi_chrom.vcz"),
        &root,
    );
    write_malformed_array_metadata(&root, "variant_UNREADABLE");

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            info_fields: Some(vec!["DP".to_string()]),
            ..Default::default()
        },
    )
    .expect("explicit INFO schema should ignore unrequested arrays");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT \"DP\" FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "DP");
}

#[tokio::test]
async fn vcf_zarr_collects_uint64_2d_info_field() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("uint64_info.vcz");
    copy_dir(
        std::path::Path::new("tests/data/vcf_zarr/multi_chrom.vcz"),
        &root,
    );
    write_u64_2d_array(&root, "variant_BIG", 1000, 2, |row, item| {
        if row == 0 {
            u64::MAX - 1 + item as u64
        } else {
            (row * 10 + item) as u64
        }
    });

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            info_fields: Some(vec!["BIG".to_string()]),
            ..Default::default()
        },
    )
    .expect("store with uint64 INFO should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT \"BIG\" FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("BIG should be a List<UInt64> array");
    let row = values.value(0);
    let row_values = row
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("BIG values should be UInt64");
    assert_eq!(row_values.len(), 2);
    assert_eq!(row_values.value(0), u64::MAX - 1);
    assert_eq!(row_values.value(1), u64::MAX);
}

#[tokio::test]
async fn vcf_zarr_collects_zero_based_coordinates_with_region_pruning() {
    let ctx = SessionContext::new();
    let provider = open_multi_chrom_provider(VcfZarrReadOptions {
        coordinate_system_zero_based: true,
        ..Default::default()
    });

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT start FROM vcz WHERE start = 5000099")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
}

#[tokio::test]
async fn vcf_zarr_collects_format_genotypes_with_sample_pruning() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");
    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["GT".to_string(), "DP".to_string()]),
            samples: Some(vec![
                "22".to_string(),
                "missing".to_string(),
                "21".to_string(),
            ]),
            ..Default::default()
        },
    )
    .expect("sampled FORMAT store should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT genotypes FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct array");
    let gt_list = genotypes
        .column_by_name("GT")
        .expect("GT list should exist")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("GT should be a list array");
    let dp_list = genotypes
        .column_by_name("DP")
        .expect("DP list should exist")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("DP should be a list array");

    let gt_row = gt_list.value(0);
    let gt_values = gt_row
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("GT values should be Int32");
    assert_eq!(gt_values.len(), 2);
    assert_eq!(gt_values.value(0), 20000);
    assert_eq!(gt_values.value(1), 10000);

    let dp_row = dp_list.value(0);
    let dp_values = dp_row
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("DP values should be Int32");
    assert_eq!(dp_values.len(), 2);
    assert_eq!(dp_values.value(0), 2000);
    assert_eq!(dp_values.value(1), 1000);
}

#[tokio::test]
async fn vcf_zarr_collects_auto_discovered_format_genotypes() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");
    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            samples: Some(vec!["22".to_string()]),
            ..Default::default()
        },
    )
    .expect("sampled FORMAT store should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT genotypes FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct array");
    assert!(genotypes.column_by_name("GT").is_some());
    assert!(genotypes.column_by_name("DP").is_some());
    assert_eq!(genotypes.columns().len(), 2);

    let gt_list = genotypes
        .column_by_name("GT")
        .expect("GT list should exist")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("GT should be a list array");
    let gt_row = gt_list.value(0);
    let gt_values = gt_row
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("GT values should be Int32");
    assert_eq!(gt_values.len(), 1);
    assert_eq!(gt_values.value(0), 20000);
}

#[tokio::test]
async fn vcf_zarr_collects_requested_format_field_without_opening_unrequested_format_arrays() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");
    write_malformed_array_metadata(&root, "call_GT");

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["DP".to_string()]),
            samples: Some(vec!["22".to_string()]),
            ..Default::default()
        },
    )
    .expect("explicit FORMAT schema should ignore unrequested arrays");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT genotypes FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct array");
    assert!(genotypes.column_by_name("GT").is_none());
    assert!(genotypes.column_by_name("DP").is_some());
}

#[tokio::test]
async fn vcf_zarr_collects_all_samples_when_samples_are_not_requested() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");
    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["DP".to_string()]),
            ..Default::default()
        },
    )
    .expect("sampled FORMAT store should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT genotypes FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct array");
    let dp_list = genotypes
        .column_by_name("DP")
        .expect("DP list should exist")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("DP should be a list array");
    let dp_row = dp_list.value(0);
    let dp_values = dp_row
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("DP values should be Int32");

    assert_eq!(dp_values.len(), 2);
    assert_eq!(dp_values.value(0), 1000);
    assert_eq!(dp_values.value(1), 2000);
}

#[tokio::test]
async fn vcf_zarr_collects_empty_format_lists_when_all_requested_samples_are_missing() {
    let temp_dir = sampled_format_store();
    let root = temp_dir.path().join("sampled.vcz");
    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["DP".to_string()]),
            samples: Some(vec!["missing".to_string()]),
            ..Default::default()
        },
    )
    .expect("sampled FORMAT store should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT genotypes FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct array");
    let dp_list = genotypes
        .column_by_name("DP")
        .expect("DP list should exist")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("DP should be a list array");
    let dp_row = dp_list.value(0);
    let dp_values = dp_row
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("DP values should be Int32");

    assert_eq!(dp_values.len(), 0);
}

#[tokio::test]
async fn vcf_zarr_preserves_negative_scalar_format_values_for_non_gt_fields() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("negative_format.vcz");
    copy_multi_chrom_with_samples(&root);
    write_i32_2d_array(&root, "call_DP", 1000, 2, |row, sample| {
        if row == 0 {
            [-1, -2][sample]
        } else {
            ((sample + 1) * 1_000 + row) as i32
        }
    });

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["DP".to_string()]),
            samples: Some(vec!["21".to_string(), "22".to_string()]),
            ..Default::default()
        },
    )
    .expect("sampled FORMAT store should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT genotypes FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct array");
    let dp_list = genotypes
        .column_by_name("DP")
        .expect("DP list should exist")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("DP should be a list array");
    let dp_row = dp_list.value(0);
    let dp_values = dp_row
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("DP values should be Int32");

    assert_eq!(dp_values.len(), 2);
    assert_eq!(dp_values.value(0), -1);
    assert_eq!(dp_values.value(1), -2);
}

#[tokio::test]
async fn vcf_zarr_collects_spec_genotype_array_as_raw_gt_by_default() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("genotype.vcz");
    copy_multi_chrom_with_samples(&root);
    write_i32_3d_array(
        &root,
        "call_genotype",
        1000,
        2,
        2,
        |_, sample, ploidy| match (sample, ploidy) {
            (0, 0) => 0,
            (0, 1) => 1,
            (1, 0) => 1,
            (1, 1) => 0,
            _ => -2,
        },
    );
    write_bool_2d_array(&root, "call_genotype_phased", 1000, 2, |_, sample| {
        sample == 1
    });

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["GT".to_string()]),
            samples: Some(vec!["22".to_string()]),
            ..Default::default()
        },
    )
    .expect("genotype FORMAT store should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT genotypes FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct array");
    let gt_list = genotypes
        .column_by_name("GT")
        .expect("GT list should exist")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("GT should be a list array");
    assert_nested_i32_list_values(gt_list, 0, &[&[1, 0]]);

    let phased_list = genotypes
        .column_by_name("GT_phased")
        .expect("GT_phased should exist in raw GT mode")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("GT_phased should be a list array");
    assert_bool_list_values(phased_list, 0, &[true]);
}

#[tokio::test]
async fn vcf_zarr_collects_spec_genotype_array_as_strings_when_requested() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("genotype_string.vcz");
    copy_multi_chrom_with_samples(&root);
    write_i32_3d_array(
        &root,
        "call_genotype",
        1000,
        2,
        2,
        |_, sample, ploidy| match (sample, ploidy) {
            (0, 0) => 0,
            (0, 1) => 1,
            (1, 0) => 1,
            (1, 1) => 0,
            _ => -2,
        },
    );
    write_bool_2d_array(&root, "call_genotype_phased", 1000, 2, |_, sample| {
        sample == 1
    });

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["GT".to_string()]),
            samples: Some(vec!["22".to_string()]),
            genotype_encoding_raw: false,
            ..Default::default()
        },
    )
    .expect("genotype FORMAT store should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT genotypes FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct array");
    assert!(genotypes.column_by_name("GT_phased").is_none());
    let gt_list = genotypes
        .column_by_name("GT")
        .expect("GT list should exist")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("GT should be a list array");
    let gt_row = gt_list.value(0);
    let gt_values = gt_row
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("GT values should be strings");
    assert_eq!(gt_values.len(), 1);
    assert_eq!(gt_values.value(0), "1|0");
}

#[tokio::test]
async fn vcf_zarr_collects_3d_non_gt_format_as_typed_nested_lists() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("format_3d.vcz");
    copy_multi_chrom_with_samples(&root);
    write_i32_3d_array(&root, "call_PL", 1000, 2, 3, |_, sample, value| {
        (sample * 10 + value + 1) as i32
    });

    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions {
            format_fields: Some(vec!["PL".to_string()]),
            samples: Some(vec!["22".to_string()]),
            ..Default::default()
        },
    )
    .expect("3D FORMAT store should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT genotypes FROM vcz WHERE start = 5000100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct array");
    let pl_list = genotypes
        .column_by_name("PL")
        .expect("PL list should exist")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("PL should be a list array");
    let pl_row = pl_list.value(0);
    let sample_lists = pl_row
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("PL should contain per-sample value lists");

    assert_eq!(sample_lists.len(), 1);
    assert_i32_list_values(sample_lists, 0, &[11, 12, 13]);
}

#[test]
fn vcf_zarr_schema_metadata_uses_complete_vcf_fileformat_fallback() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let cases = [
        (
            "no_fileformat",
            format!("{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\"}}"),
        ),
        (
            "malformed_fileformat",
            format!(
                "{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\",\"vcf_meta_information\":\"not an array\"}}"
            ),
        ),
    ];

    for (case_name, attributes) in cases {
        let root = temp_dir.path().join(format!("{case_name}.vcz"));
        write_v2_root_metadata(&root, &attributes);
        write_required_zarray_metadata(&root);

        let provider = VcfZarrTableProvider::new(
            root.to_str()
                .expect("temp path should be UTF-8")
                .to_string(),
            VcfZarrReadOptions::default(),
        )
        .expect("store with required arrays should open");

        assert_eq!(
            provider
                .schema()
                .metadata()
                .get(VCF_FILE_FORMAT_KEY)
                .map(String::as_str),
            Some("VCFv4.3"),
            "{case_name} should use complete fileformat fallback"
        );
    }
}
