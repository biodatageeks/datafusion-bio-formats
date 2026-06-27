use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use bigtools::bed::bedparser::BedFileStream;
use bigtools::beddata::BedParserStreamingIterator;
use bigtools::{BigBedWrite, BigWigWrite};
use datafusion::arrow::array::{Float32Array, StringArray, UInt32Array, UInt64Array};
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::*;
use datafusion_bio_format_bbi::bigbed::{BigBedSchemaMode, BigBedTableProvider};
use datafusion_bio_format_bbi::bigwig::BigWigTableProvider;
use datafusion_bio_format_core::test_utils::assert_plan_projection;
use tempfile::NamedTempFile;
use tokio::runtime;

fn runtime() -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .expect("failed to build test runtime")
}

fn chrom_sizes() -> HashMap<String, u32> {
    HashMap::from([("chr1".to_string(), 100), ("chr2".to_string(), 100)])
}

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn write_bigwig_fixture() -> TestResult<NamedTempFile> {
    std::thread::spawn(write_bigwig_fixture_inner)
        .join()
        .unwrap()
}

fn write_bigwig_fixture_inner() -> TestResult<NamedTempFile> {
    let mut bedgraph = NamedTempFile::new()?;
    writeln!(bedgraph, "chr1\t0\t10\t1.5")?;
    writeln!(bedgraph, "chr1\t20\t30\t2.5")?;
    writeln!(bedgraph, "chr2\t5\t12\t3.5")?;
    bedgraph.flush()?;

    let bigwig = NamedTempFile::new()?;
    let out = BigWigWrite::create_file(bigwig.path(), chrom_sizes())?;
    let input = File::open(bedgraph.path())?;
    let data = BedParserStreamingIterator::from_bedgraph_file(input, false);
    out.write(data, runtime())?;
    Ok(bigwig)
}

fn write_bigbed_fixture() -> TestResult<NamedTempFile> {
    std::thread::spawn(write_bigbed_fixture_inner)
        .join()
        .unwrap()
}

fn write_bigbed_fixture_inner() -> TestResult<NamedTempFile> {
    let mut bed = NamedTempFile::new()?;
    writeln!(bed, "chr1\t0\t10\tgene1\t42")?;
    writeln!(bed, "chr1\t20\t30\tgene2\t84")?;
    writeln!(bed, "chr2\t5\t12\tgene3\t126")?;
    bed.flush()?;

    let bigbed = NamedTempFile::new()?;
    let mut out = BigBedWrite::create_file(bigbed.path(), chrom_sizes())?;
    let first_rest = {
        use bigtools::bed::bedparser::StreamingBedValues;
        let input = File::open(bed.path())?;
        let mut vals = BedFileStream::from_bed_file(input);
        vals.next().unwrap()?.1.rest
    };
    out.autosql = Some(bigtools::bed::autosql::bed_autosql(&first_rest));
    out.options.compress = false;
    let input = File::open(bed.path())?;
    let data = BedParserStreamingIterator::from_bed_file(input, false);
    out.write(data, runtime())?;
    Ok(bigbed)
}

#[tokio::test]
async fn scans_bigwig_as_interval_signal_rows() -> TestResult<()> {
    let fixture = write_bigwig_fixture()?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("bw", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, start, \"end\", value FROM bw ORDER BY chrom, start")
        .await?;
    let batches = df.collect().await?;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>(),
        vec!["chrom", "start", "end", "value"]
    );

    let chrom = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let start = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let end = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let value = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();

    assert_eq!(chrom.value(0), "chr1");
    assert_eq!(start.value(0), 0);
    assert_eq!(end.value(0), 10);
    assert_eq!(value.value(0), 1.5);
    assert_eq!(chrom.value(2), "chr2");
    assert_eq!(start.value(2), 5);
    assert_eq!(value.value(2), 3.5);

    Ok(())
}

#[tokio::test]
async fn pushes_bigwig_projection_into_exec() -> TestResult<()> {
    let fixture = write_bigwig_fixture()?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("bw", Arc::new(table))?;

    let df = ctx.sql("SELECT chrom, start FROM bw").await?;
    let plan = df.create_physical_plan().await?;
    assert_plan_projection(&plan, "BigWigExec", &["chrom", "start"]);

    Ok(())
}

#[tokio::test]
async fn pushes_bigwig_genomic_filter_into_scan_regions() -> TestResult<()> {
    let fixture = write_bigwig_fixture()?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("bw", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, start FROM bw WHERE chrom = 'chr2' AND start < 10")
        .await?;
    let plan = df.create_physical_plan().await?;
    let plan_text = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(false)
        .to_string();

    assert!(
        plan_text.contains("BigWigExec"),
        "expected BigWigExec in plan:\n{plan_text}"
    );
    assert!(
        plan_text.contains("regions=[chr2:0-10]"),
        "expected chr2 interval pruning in plan:\n{plan_text}"
    );

    Ok(())
}

#[tokio::test]
async fn scans_bigbed_autosql_columns() -> TestResult<()> {
    let fixture = write_bigbed_fixture()?;
    let table = BigBedTableProvider::new(
        fixture.path().to_string_lossy().to_string(),
        true,
        BigBedSchemaMode::Auto,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("bb", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, start, \"end\", name, score FROM bb ORDER BY chrom, start")
        .await?;
    let batches = df.collect().await?;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>(),
        vec!["chrom", "start", "end", "name", "score"]
    );

    let chrom = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let start = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let name = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let score = batch
        .column(4)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    assert_eq!(chrom.value(0), "chr1");
    assert_eq!(start.value(0), 0);
    assert_eq!(name.value(0), "gene1");
    assert_eq!(score.value(2), 126);

    Ok(())
}

#[tokio::test]
async fn filters_bigbed_by_genomic_region() -> TestResult<()> {
    let fixture = write_bigbed_fixture()?;
    let table = BigBedTableProvider::new(
        fixture.path().to_string_lossy().to_string(),
        true,
        BigBedSchemaMode::Auto,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("bb", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, start, \"end\", name FROM bb WHERE chrom = 'chr2' AND start < 10")
        .await?;
    let batches = df.collect().await?;

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let batch = &batches[0];
    let chrom = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let name = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(chrom.value(0), "chr2");
    assert_eq!(name.value(0), "gene3");

    Ok(())
}

#[tokio::test]
async fn rest_mode_exposes_single_rest_column() -> TestResult<()> {
    let fixture = write_bigbed_fixture()?;
    let table = BigBedTableProvider::new(
        fixture.path().to_string_lossy().to_string(),
        true,
        BigBedSchemaMode::Rest,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("bb", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, start, \"end\", rest FROM bb ORDER BY chrom, start")
        .await?;
    let batches = df.collect().await?;

    let columns = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(columns, vec!["chrom", "start", "end", "rest"]);

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);

    let rest = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(rest.value(0), "gene1\t42");

    Ok(())
}

#[tokio::test]
async fn accepts_file_uri_paths() -> TestResult<()> {
    let fixture = write_bigwig_fixture()?;
    let uri = format!("file://{}", fixture.path().to_string_lossy());
    let table = BigWigTableProvider::new(uri, true)?;

    let ctx = SessionContext::new();
    ctx.register_table("bw", Arc::new(table))?;

    let df = ctx.sql("SELECT chrom FROM bw").await?;
    let batches = df.collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);

    Ok(())
}

fn write_large_bigwig_fixture(intervals: u32) -> TestResult<NamedTempFile> {
    std::thread::spawn(move || write_large_bigwig_fixture_inner(intervals))
        .join()
        .unwrap()
}

fn write_large_bigwig_fixture_inner(intervals: u32) -> TestResult<NamedTempFile> {
    let mut bedgraph = NamedTempFile::new()?;
    for i in 0..intervals {
        // Non-overlapping 1bp intervals at positions 0, 2, 4, ...
        writeln!(bedgraph, "chr1\t{}\t{}\t{}", i * 2, i * 2 + 1, i as f32)?;
    }
    bedgraph.flush()?;

    let sizes = HashMap::from([("chr1".to_string(), intervals * 2 + 10)]);
    let bigwig = NamedTempFile::new()?;
    let out = BigWigWrite::create_file(bigwig.path(), sizes)?;
    let input = File::open(bedgraph.path())?;
    let data = BedParserStreamingIterator::from_bedgraph_file(input, false);
    out.write(data, runtime())?;
    Ok(bigwig)
}

#[tokio::test]
async fn streams_large_region_in_fixed_size_batches() -> TestResult<()> {
    // More intervals than one batch holds, so a single (unfiltered) chromosome
    // region must be emitted as several fixed-size batches rather than buffered
    // whole.
    let intervals = 20_000u32;
    let fixture = write_large_bigwig_fixture(intervals)?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;

    let ctx = SessionContext::new();
    let state = ctx.state();
    let plan = table.scan(&state, None, &[], None).await?;
    let stream = plan.execute(0, ctx.task_ctx())?;
    let batches = collect(stream).await?;

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, intervals as usize);
    assert!(
        batches.len() >= 2,
        "expected multiple fixed-size batches, got {}",
        batches.len()
    );
    assert!(
        batches.iter().all(|b| b.num_rows() <= 8192),
        "no batch should exceed the chunk size"
    );

    Ok(())
}

#[tokio::test]
async fn accepts_file_localhost_uri_paths() -> TestResult<()> {
    let fixture = write_bigwig_fixture()?;
    let uri = format!("file://localhost{}", fixture.path().to_string_lossy());
    let table = BigWigTableProvider::new(uri, true)?;

    let ctx = SessionContext::new();
    ctx.register_table("bw", Arc::new(table))?;

    let df = ctx.sql("SELECT chrom FROM bw").await?;
    let batches = df.collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);

    Ok(())
}

#[tokio::test]
async fn rejects_remote_host_file_uri() {
    let result = BigWigTableProvider::new("file://example.com/data/file.bw".to_string(), true);
    let error = result.expect_err("remote file:// host authority must be rejected");
    assert!(
        error.to_string().contains("remote file:// URIs"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn pushes_bigbed_projection_into_exec() -> TestResult<()> {
    let fixture = write_bigbed_fixture()?;
    let table = BigBedTableProvider::new(
        fixture.path().to_string_lossy().to_string(),
        true,
        BigBedSchemaMode::Auto,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("bb", Arc::new(table))?;

    let df = ctx.sql("SELECT chrom, start FROM bb").await?;
    let plan = df.create_physical_plan().await?;
    assert_plan_projection(&plan, "BigBedExec", &["chrom", "start"]);

    Ok(())
}

#[tokio::test]
async fn pushes_bigbed_genomic_filter_into_scan_regions() -> TestResult<()> {
    let fixture = write_bigbed_fixture()?;
    let table = BigBedTableProvider::new(
        fixture.path().to_string_lossy().to_string(),
        true,
        BigBedSchemaMode::Auto,
    )?;

    let ctx = SessionContext::new();
    ctx.register_table("bb", Arc::new(table))?;

    let df = ctx
        .sql(
            "SELECT chrom, start, name FROM bb WHERE chrom = 'chr1' AND start >= 20 AND start < 30",
        )
        .await?;
    let plan = df.create_physical_plan().await?;
    let plan_text = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(false)
        .to_string();

    assert!(
        plan_text.contains("BigBedExec"),
        "expected BigBedExec in plan:\n{plan_text}"
    );
    assert!(
        plan_text.contains("regions=[chr1:20-30]"),
        "expected chr1 interval pruning in plan:\n{plan_text}"
    );

    Ok(())
}
