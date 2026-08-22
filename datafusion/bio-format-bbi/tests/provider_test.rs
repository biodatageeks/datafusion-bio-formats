use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use bigtools::bed::bedparser::BedFileStream;
use bigtools::beddata::BedParserStreamingIterator;
use bigtools::{BigBedWrite, BigWigWrite};
use datafusion::arrow::array::{Float32Array, Int64Array, StringArray, UInt32Array, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::TableProvider;
use datafusion::execution::context::SessionConfig;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::*;
use datafusion_bio_format_bbi::bigbed::{BigBedSchemaMode, BigBedTableProvider};
use datafusion_bio_format_bbi::bigwig::BigWigTableProvider;
use datafusion_bio_format_core::test_utils::{assert_plan_projection, find_leaf_exec};
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

fn skewed_chrom_sizes() -> HashMap<String, u32> {
    HashMap::from([("chr1".to_string(), 900), ("chr2".to_string(), 100)])
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

fn write_partition_bigwig_fixture() -> TestResult<NamedTempFile> {
    std::thread::spawn(|| -> TestResult<NamedTempFile> {
        let mut bedgraph = NamedTempFile::new()?;
        writeln!(bedgraph, "chr1\t240\t260\t1.0")?;
        writeln!(bedgraph, "chr1\t490\t510\t2.0")?;
        writeln!(bedgraph, "chr1\t740\t760\t3.0")?;
        writeln!(bedgraph, "chr2\t10\t20\t4.0")?;
        bedgraph.flush()?;

        let bigwig = NamedTempFile::new()?;
        let mut out = BigWigWrite::create_file(bigwig.path(), skewed_chrom_sizes())?;
        // Force distinct same-chromosome primary blocks so tests exercise real
        // index-derived cuts rather than inferred coordinate quartiles.
        out.options.items_per_slot = 1;
        let input = File::open(bedgraph.path())?;
        let data = BedParserStreamingIterator::from_bedgraph_file(input, false);
        out.write(data, runtime())?;
        Ok(bigwig)
    })
    .join()
    .unwrap()
}

fn write_partition_bigbed_fixture() -> TestResult<NamedTempFile> {
    std::thread::spawn(|| -> TestResult<NamedTempFile> {
        let mut bed = NamedTempFile::new()?;
        writeln!(bed, "chr1\t240\t260\tfeature1\t1")?;
        writeln!(bed, "chr1\t490\t490\tboundary\t0")?;
        writeln!(bed, "chr1\t490\t510\tfeature2\t2")?;
        writeln!(bed, "chr1\t740\t760\tfeature3\t3")?;
        writeln!(bed, "chr2\t10\t20\tfeature4\t4")?;
        bed.flush()?;

        let bigbed = NamedTempFile::new()?;
        let mut out = BigBedWrite::create_file(bigbed.path(), skewed_chrom_sizes())?;
        let first_rest = {
            use bigtools::bed::bedparser::StreamingBedValues;
            let input = File::open(bed.path())?;
            let mut vals = BedFileStream::from_bed_file(input);
            vals.next().unwrap()?.1.rest
        };
        out.autosql = Some(bigtools::bed::autosql::bed_autosql(&first_rest));
        out.options.compress = false;
        out.options.items_per_slot = 1;
        let input = File::open(bed.path())?;
        let data = BedParserStreamingIterator::from_bed_file(input, false);
        out.write(data, runtime())?;
        Ok(bigbed)
    })
    .join()
    .unwrap()
}

fn context_with_partitions(partitions: usize) -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_target_partitions(partitions))
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
    // The bounded BigTools query preserves original coordinates for intervals
    // overlapping the query edge, so BigWig can prune positionally as well as by
    // chromosome without clipping emitted values.
    assert!(
        plan_text.contains("regions=[chr2:0-10]"),
        "expected chr2 to use the extracted upper bound:\n{plan_text}"
    );

    Ok(())
}

#[tokio::test]
async fn bigwig_genomic_filter_returns_unclipped_intervals() -> TestResult<()> {
    // Regression: a `start < N` filter must not clip the emitted interval `end`.
    // The chr2 interval is [5, 12); filtering `start < 10` must still return
    // end = 12, not 10 (the filter bound / clipped window edge).
    let fixture = write_bigwig_fixture()?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;

    let ctx = SessionContext::new();
    ctx.register_table("bw", Arc::new(table))?;

    let df = ctx
        .sql("SELECT chrom, start, \"end\", value FROM bw WHERE chrom = 'chr2' AND start < 10")
        .await?;
    let batches = df.collect().await?;

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let batch = &batches[0];
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
    assert_eq!(chrom.value(0), "chr2");
    assert_eq!(start.value(0), 5);
    assert_eq!(
        end.value(0),
        12,
        "interval end must be the true 12, not clipped to the filter bound"
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

    let ctx = context_with_partitions(1);
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
async fn empty_projection_uses_large_logical_batches_without_arrays() -> TestResult<()> {
    let intervals = 20_000u32;
    let fixture = write_large_bigwig_fixture(intervals)?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;

    let ctx = context_with_partitions(1);
    let projection = Vec::new();
    let plan = table
        .scan(&ctx.state(), Some(&projection), &[], None)
        .await?;
    let batches = collect(plan.execute(0, ctx.task_ctx())?).await?;

    assert_eq!(batches.len(), 1, "empty projections use a larger batch cap");
    assert_eq!(batches[0].num_rows(), intervals as usize);
    assert_eq!(batches[0].num_columns(), 0);
    Ok(())
}

#[tokio::test]
async fn unfiltered_single_chromosome_scan_can_split_but_filtered_scan_stays_serial()
-> TestResult<()> {
    let fixture = write_large_bigwig_fixture(20_000)?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;
    let ctx = context_with_partitions(4);

    let full_plan = table.scan(&ctx.state(), None, &[], None).await?;
    assert_eq!(
        full_plan
            .properties()
            .output_partitioning()
            .partition_count(),
        4,
        "an unfiltered one-chromosome file should use target_partitions"
    );
    let mut total = 0;
    for partition in 0..4 {
        let batches = collect(full_plan.execute(partition, ctx.task_ctx())?).await?;
        total += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    }
    assert_eq!(total, 20_000);

    let filtered_plan = table
        .scan(&ctx.state(), None, &[col("chrom").eq(lit("chr1"))], None)
        .await?;
    assert_eq!(
        filtered_plan
            .properties()
            .output_partitioning()
            .partition_count(),
        1,
        "an explicitly selected chromosome should not fan out"
    );

    let residual_plan = table
        .scan(&ctx.state(), None, &[col("end").gt(lit(0u32))], None)
        .await?;
    assert_eq!(
        residual_plan
            .properties()
            .output_partitioning()
            .partition_count(),
        4,
        "a residual coordinate predicate should not disable full-file parallelism"
    );
    Ok(())
}

#[tokio::test]
async fn bigwig_early_terminates_at_upper_bound() -> TestResult<()> {
    // 20k intervals at starts 0, 2, 4, ...; a `start < 100` upper bound must stop
    // the scan after the 50 matching rows instead of streaming the whole
    // chromosome. The exec applies only the early-stop cursor (DataFusion
    // re-applies the predicate above it), so observing 50 rows here — not
    // 20_000 — proves the scan terminated early rather than read-then-filtered.
    let intervals = 20_000u32;
    let fixture = write_large_bigwig_fixture(intervals)?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;

    let ctx = SessionContext::new();
    let state = ctx.state();
    // A chrom predicate is required for the region's upper bound to be extracted.
    let filter = col("chrom")
        .eq(lit("chr1"))
        .and(col("start").lt(lit(100u32)));
    let plan = table.scan(&state, None, &[filter], None).await?;
    let stream = plan.execute(0, ctx.task_ctx())?;
    let batches = collect(stream).await?;

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 50,
        "early termination should emit only the matching prefix, not the whole chromosome"
    );
    assert_eq!(
        batches.len(),
        1,
        "50 rows fit in a single batch; a full scan would emit many"
    );

    Ok(())
}

#[tokio::test]
async fn bigwig_early_termination_spans_multiple_batches() -> TestResult<()> {
    // `start < 20_000` matches the first 10_000 intervals (starts 0..19_998),
    // which is more than one 8_192-row batch — verifying the stop cursor fires
    // mid-stream across batch boundaries, not only within the first batch.
    let intervals = 20_000u32;
    let fixture = write_large_bigwig_fixture(intervals)?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;

    let ctx = SessionContext::new();
    let state = ctx.state();
    let filter = col("chrom")
        .eq(lit("chr1"))
        .and(col("start").lt(lit(20_000u32)));
    let plan = table.scan(&state, None, &[filter], None).await?;
    let stream = plan.execute(0, ctx.task_ctx())?;
    let batches = collect(stream).await?;

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 10_000,
        "should stop after the 10_000 matching intervals, not read all 20_000"
    );
    assert!(
        batches.len() >= 2,
        "10_000 rows must span multiple fixed-size batches, got {}",
        batches.len()
    );

    Ok(())
}

#[tokio::test]
async fn bigwig_lower_bound_uses_bounded_unclipped_query() -> TestResult<()> {
    // The unclipped query can seek to a lower bound without changing a
    // boundary-overlapping interval's original coordinates.
    let intervals = 20_000u32;
    let fixture = write_large_bigwig_fixture(intervals)?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;

    let ctx = SessionContext::new();
    let state = ctx.state();
    let filter = col("chrom")
        .eq(lit("chr1"))
        .and(col("start").gt(lit(100u32)));
    let plan = table.scan(&state, None, &[filter], None).await?;
    let stream = plan.execute(0, ctx.task_ctx())?;
    let batches = collect(stream).await?;

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 19_949,
        "the source should seek to the lower bound without scanning earlier rows"
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

// COUNT(*) / empty-projection smoke tests — verify the BBI providers handle
// the zero-column projection consistently (audit follow-up to #208).

fn count_star_value(batch: &RecordBatch) -> i64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn count_star_bigwig() -> TestResult<()> {
    let fixture = write_bigwig_fixture()?;
    let table = BigWigTableProvider::new(fixture.path().to_string_lossy().to_string(), true)?;
    let ctx = SessionContext::new();
    ctx.register_table("bw", Arc::new(table))?;

    let batches = ctx
        .sql("SELECT count(*) AS c FROM bw")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    assert_eq!(count_star_value(&batches[0]), 3);
    Ok(())
}

#[tokio::test]
async fn count_star_bigbed() -> TestResult<()> {
    let fixture = write_bigbed_fixture()?;
    let table = BigBedTableProvider::new(
        fixture.path().to_string_lossy().to_string(),
        true,
        BigBedSchemaMode::Auto,
    )?;
    let ctx = SessionContext::new();
    ctx.register_table("bb", Arc::new(table))?;

    let batches = ctx
        .sql("SELECT count(*) AS c FROM bb")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    assert_eq!(count_star_value(&batches[0]), 3);
    Ok(())
}

#[tokio::test]
async fn bigwig_full_scan_uses_block_bounded_partitions_without_boundary_corruption()
-> TestResult<()> {
    let fixture = write_partition_bigwig_fixture()?;
    let path = fixture.path().to_string_lossy().to_string();

    let serial = context_with_partitions(1);
    serial.register_table(
        "bw",
        Arc::new(BigWigTableProvider::new(path.clone(), true)?),
    )?;
    let expected = serial
        .sql("SELECT chrom, start, \"end\", value FROM bw ORDER BY chrom, start")
        .await?
        .collect()
        .await?;

    let parallel = context_with_partitions(4);
    parallel.register_table("bw", Arc::new(BigWigTableProvider::new(path, true)?))?;
    let count = parallel.sql("SELECT count(*) FROM bw").await?;
    let count_plan = count.create_physical_plan().await?;
    let leaf = find_leaf_exec(&count_plan);
    assert_eq!(leaf.name(), "BigWigExec");
    assert_eq!(leaf.properties().output_partitioning().partition_count(), 4);
    let count_plan_text = DisplayableExecutionPlan::new(count_plan.as_ref())
        .indent(false)
        .to_string();
    assert!(
        count_plan_text.contains("estimated_data_bytes=["),
        "BigWig plan should expose index-derived partition work:\n{count_plan_text}"
    );
    assert!(
        count_plan_text.contains("chr1:0-490") && count_plan_text.contains("chr1:490-740"),
        "BigWig plan should use observed same-chromosome block cuts:\n{count_plan_text}"
    );

    let actual = parallel
        .sql("SELECT chrom, start, \"end\", value FROM bw ORDER BY chrom, start")
        .await?
        .collect()
        .await?;
    assert_eq!(
        pretty_format_batches(&actual)?.to_string(),
        pretty_format_batches(&expected)?.to_string()
    );
    assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);

    let filtered_plan = parallel
        .sql("SELECT count(*) FROM bw WHERE chrom = 'chr1'")
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(
        find_leaf_exec(&filtered_plan)
            .properties()
            .output_partitioning()
            .partition_count(),
        1,
        "a single selected chromosome should not fan out"
    );
    Ok(())
}

#[tokio::test]
async fn bigbed_full_scan_uses_block_bounded_partitions_without_duplicates() -> TestResult<()> {
    let fixture = write_partition_bigbed_fixture()?;
    let path = fixture.path().to_string_lossy().to_string();

    let serial = context_with_partitions(1);
    serial.register_table(
        "bb",
        Arc::new(BigBedTableProvider::new(
            path.clone(),
            true,
            BigBedSchemaMode::Auto,
        )?),
    )?;
    let expected = serial
        .sql(
            "SELECT chrom, start, \"end\", name, score FROM bb \
             ORDER BY chrom, start, \"end\", name",
        )
        .await?
        .collect()
        .await?;

    let parallel = context_with_partitions(4);
    parallel.register_table(
        "bb",
        Arc::new(BigBedTableProvider::new(
            path,
            true,
            BigBedSchemaMode::Auto,
        )?),
    )?;
    let count = parallel.sql("SELECT count(*) FROM bb").await?;
    let count_plan = count.create_physical_plan().await?;
    let leaf = find_leaf_exec(&count_plan);
    assert_eq!(leaf.name(), "BigBedExec");
    assert_eq!(leaf.properties().output_partitioning().partition_count(), 4);
    let count_plan_text = DisplayableExecutionPlan::new(count_plan.as_ref())
        .indent(false)
        .to_string();
    assert!(
        count_plan_text.contains("estimated_data_bytes=["),
        "BigBed plan should expose index-derived partition work:\n{count_plan_text}"
    );
    assert!(
        count_plan_text.contains("chr1:0-490") && count_plan_text.contains("chr1:490-740"),
        "BigBed plan should use observed same-chromosome block cuts:\n{count_plan_text}"
    );

    let actual = parallel
        .sql(
            "SELECT chrom, start, \"end\", name, score FROM bb \
             ORDER BY chrom, start, \"end\", name",
        )
        .await?
        .collect()
        .await?;
    assert_eq!(
        pretty_format_batches(&actual)?.to_string(),
        pretty_format_batches(&expected)?.to_string()
    );
    assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);

    let filtered_plan = parallel
        .sql("SELECT count(*) FROM bb WHERE chrom = 'chr1'")
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(
        find_leaf_exec(&filtered_plan)
            .properties()
            .output_partitioning()
            .partition_count(),
        1,
        "a single selected chromosome should not fan out"
    );
    Ok(())
}

#[tokio::test]
async fn bbi_full_scan_partition_count_tracks_every_target_from_one_to_eight() -> TestResult<()> {
    let bigwig = write_partition_bigwig_fixture()?;
    let bigbed = write_partition_bigbed_fixture()?;

    for target in 1..=8 {
        let context = context_with_partitions(target);
        context.register_table(
            "bw",
            Arc::new(BigWigTableProvider::new(
                bigwig.path().to_string_lossy().to_string(),
                true,
            )?),
        )?;
        context.register_table(
            "bb",
            Arc::new(BigBedTableProvider::new(
                bigbed.path().to_string_lossy().to_string(),
                true,
                BigBedSchemaMode::Auto,
            )?),
        )?;

        for table in ["bw", "bb"] {
            let plan = context
                .sql(&format!("SELECT count(*) FROM {table}"))
                .await?
                .create_physical_plan()
                .await?;
            let leaf = find_leaf_exec(&plan);
            let source_partitions = leaf.properties().output_partitioning().partition_count();
            assert_eq!(source_partitions, target.min(4));
            let plan_text = DisplayableExecutionPlan::new(plan.as_ref())
                .indent(false)
                .to_string();
            if source_partitions == target {
                assert!(
                    !plan_text.contains("RepartitionExec"),
                    "{table} target_partitions={target} unexpectedly repartitioned:\n{plan_text}"
                );
            }

            let batches = context
                .sql(&format!("SELECT count(*) AS c FROM {table}"))
                .await?
                .collect()
                .await?;
            let expected_rows = if table == "bb" { 5 } else { 4 };
            assert_eq!(count_star_value(&batches[0]), expected_rows);
        }
    }
    Ok(())
}
