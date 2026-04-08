/// Convert Ensembl VEP cache storable entities to optimized Parquet files.
///
/// Usage: cargo run --release --example storable_to_parquet -- <cache_root> <output_dir> <entity> [partitions] [--chrom CHROM]
///
/// entity: transcript | exon | translation | regulatory | motif | variation
///
/// Output files:
///   <output_dir>/<version_assembly>_<entity>[_<chrom>].parquet
///   e.g. 115_GRCh38_transcript_22.parquet
///
/// Translation is split into two files for optimal access patterns:
///   <output_dir>/<version_assembly>_translation_core[_<chrom>].parquet  (sorted by transcript_id)
///   <output_dir>/<version_assembly>_translation_sift[_<chrom>].parquet  (sorted by chrom, start)
///
/// Layout optimizations (see https://github.com/biodatageeks/datafusion-bio-formats/issues/131):
///   - Entity-specific row group sizing for effective RG pruning
///   - Sort order matched to downstream query access patterns
///   - sorting_columns declared in parquet footer for DataFusion sort-aware optimizations
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::format::SortingColumn;
use datafusion::parquet::schema::types::ColumnPath;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_ensembl_cache::{
    EnsemblCacheOptions, EnsemblCacheTableProvider, EnsemblEntityKind,
};
use futures::StreamExt;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

fn rss_mb() -> f64 {
    #[cfg(target_os = "macos")]
    {
        use std::mem::MaybeUninit;
        unsafe extern "C" {
            fn mach_task_self() -> u32;
            fn task_info(
                target_task: u32,
                flavor: u32,
                task_info_out: *mut LibcTaskBasicInfo,
                task_info_cnt: *mut u32,
            ) -> i32;
        }
        #[repr(C)]
        struct LibcTaskBasicInfo {
            suspend_count: i32,
            virtual_size: u64,
            resident_size: u64,
            user_time: [u32; 2],
            system_time: [u32; 2],
            policy: i32,
        }
        const MACH_TASK_BASIC_INFO: u32 = 20;
        const MACH_TASK_BASIC_INFO_COUNT: u32 =
            (std::mem::size_of::<LibcTaskBasicInfo>() / std::mem::size_of::<u32>()) as u32;
        unsafe {
            let mut info = MaybeUninit::<LibcTaskBasicInfo>::uninit();
            let mut count = MACH_TASK_BASIC_INFO_COUNT;
            let kr = task_info(
                mach_task_self(),
                MACH_TASK_BASIC_INFO,
                info.as_mut_ptr(),
                &mut count,
            );
            if kr == 0 {
                return info.assume_init().resident_size as f64 / (1024.0 * 1024.0);
            }
        }
    }
    0.0
}

/// Build the dedup + sort query for each entity type.
/// Sort order is chosen to match downstream access patterns (see issue #131).
///
/// For transcripts, this also applies region-local HGNC propagation to keep the
/// promoted `gene_hgnc_id` column aligned with VEP cache locality. VEP merges
/// features inside 1 Mb cache regions, not across every same-symbol transcript
/// on a chromosome.
const VEP_CACHE_REGION_SIZE_BP: i64 = 1_000_000;

fn build_dedup_query(
    kind: EnsemblEntityKind,
    table_name: &str,
    chrom_filter: &Option<String>,
    schema: Option<&SchemaRef>,
) -> String {
    let where_clause = if let Some(chrom) = chrom_filter {
        format!(" WHERE chrom = '{chrom}'")
    } else {
        String::new()
    };

    match kind {
        // Transcript: dedup by stable_id, sort by (chrom, start) for interval queries.
        // Propagate gene_hgnc_id only within a VEP-sized cache region. This
        // avoids copying HGNC IDs across distant loci that reuse the same symbol.
        EnsemblEntityKind::Transcript => {
            let schema = schema.expect("Transcript entity requires schema for HGNC propagation");
            let region_expr =
                format!("CAST(FLOOR((start - 1) / {VEP_CACHE_REGION_SIZE_BP}.0) AS BIGINT)");
            let columns: Vec<String> = schema
                .fields()
                .iter()
                .map(|f| {
                    if f.name() == "gene_hgnc_id" {
                        format!(
                            "COALESCE(gene_hgnc_id, \
                                 CASE WHEN gene_symbol IS NOT NULL \
                                      THEN FIRST_VALUE(gene_hgnc_id) IGNORE NULLS \
                                           OVER (PARTITION BY chrom, gene_symbol, {region_expr} \
                                                 ORDER BY gene_hgnc_id NULLS LAST) \
                                      ELSE NULL END) AS gene_hgnc_id"
                        )
                    } else {
                        format!("\"{}\"", f.name())
                    }
                })
                .collect();
            let select_list = columns.join(", ");

            format!(
                "SELECT {select_list} FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY stable_id \
                        ORDER BY cds_start NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1 \
                ORDER BY chrom, start"
            )
        }
        // Translation is handled separately by write_translation_split() — not via this path.
        EnsemblEntityKind::Translation => unreachable!("use write_translation_split() instead"),
        // Exon: dedup by (transcript_id, exon_number), sort by (transcript_id, start)
        // to enable RG pruning for WHERE transcript_id IN (...) queries
        EnsemblEntityKind::Exon => {
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY transcript_id, exon_number \
                        ORDER BY stable_id NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1 \
                ORDER BY transcript_id, start"
            )
        }
        // Other entities (variation, regulatory, motif): sort by (chrom, start)
        _ => {
            format!(
                "SELECT * FROM {table_name}{where_clause} \
                ORDER BY chrom, start"
            )
        }
    }
}

/// Row group sizing per entity type.
/// Chosen to balance RG pruning effectiveness vs per-RG alignment padding overhead.
fn row_group_size(kind: EnsemblEntityKind) -> usize {
    match kind {
        // Variation: 100K rows/RG — already optimal, large table
        EnsemblEntityKind::Variation => 100_000,
        // Transcript: 6-12K rows/RG for interval predicate pruning
        EnsemblEntityKind::Transcript => 8_000,
        // Exon: ~45K rows/RG — fewer RGs reduces inter-RG alignment padding
        EnsemblEntityKind::Exon => 45_000,
        // Translation splits: 6K rows/RG for effective pruning on ~22K rows
        EnsemblEntityKind::Translation => 6_000,
        // Regulatory: ~9K rows/RG for interval predicate pruning
        EnsemblEntityKind::RegulatoryFeature => 9_000,
        // Motif: typically empty or very small, use moderate size
        EnsemblEntityKind::MotifFeature => 10_000,
    }
}

/// Build sorting_columns metadata for the parquet footer based on sort key.
fn sorting_columns_for(schema: &SchemaRef, sort_columns: &[&str]) -> Option<Vec<SortingColumn>> {
    let cols: Vec<SortingColumn> = sort_columns
        .iter()
        .filter_map(|name| {
            schema
                .column_with_name(name)
                .map(|(idx, _)| SortingColumn::new(idx as i32, false, false))
        })
        .collect();
    if cols.len() == sort_columns.len() {
        Some(cols)
    } else {
        None
    }
}

/// Sort key for each entity type (must match the ORDER BY in build_dedup_query).
fn sort_key(kind: EnsemblEntityKind) -> &'static [&'static str] {
    match kind {
        EnsemblEntityKind::Exon => &["transcript_id", "start"],
        _ => &["chrom", "start"],
    }
}

/// Build WriterProperties with compression, row group size, sorting metadata, and bloom filters.
/// If `rg_size_override` is provided, it takes precedence over the entity-level default.
fn writer_properties(
    kind: EnsemblEntityKind,
    schema: &SchemaRef,
    sort_columns: &[&str],
    rg_size_override: Option<usize>,
) -> WriterProperties {
    let rg_size = rg_size_override.unwrap_or_else(|| row_group_size(kind));
    let sorting = sorting_columns_for(schema, sort_columns);

    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(rg_size)
        .set_sorting_columns(sorting);

    // Bloom filter on transcript_id for translation and exon tables
    if matches!(
        kind,
        EnsemblEntityKind::Translation | EnsemblEntityKind::Exon
    ) {
        builder = builder.set_column_bloom_filter_enabled(ColumnPath::from("transcript_id"), true);
    }

    builder.build()
}

/// Project a RecordBatch to a target schema (select columns by name).
fn project_batch(
    batch: &datafusion::arrow::array::RecordBatch,
    target_schema: &SchemaRef,
) -> datafusion::common::Result<datafusion::arrow::array::RecordBatch> {
    let source_schema = batch.schema();
    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        let (idx, _) = source_schema
            .column_with_name(field.name())
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Column '{}' not found in source batch",
                    field.name()
                ))
            })?;
        columns.push(batch.column(idx).clone());
    }
    Ok(datafusion::arrow::array::RecordBatch::try_new(
        target_schema.clone(),
        columns,
    )?)
}

/// Write translation entity as two split files: translation_core and translation_sift.
/// Coordinates are 1-based (coordinate_system_zero_based=false) to match VEP conventions.
async fn write_translation_split(
    ctx: &SessionContext,
    table_name: &str,
    chrom_filter: &Option<String>,
    output_dir: &str,
    cache_dir_name: &str,
    coordinate_system_zero_based: bool,
) -> datafusion::common::Result<Vec<(String, usize)>> {
    let where_clause = if let Some(chrom) = chrom_filter {
        format!(" WHERE chrom = '{chrom}'")
    } else {
        String::new()
    };

    let chrom_suffix = if let Some(chrom) = &chrom_filter {
        format!("_{chrom}")
    } else {
        String::new()
    };

    // Dedup query — we'll re-sort per split
    let dedup_query = format!(
        "SELECT * FROM (\
            SELECT *, ROW_NUMBER() OVER (\
                PARTITION BY transcript_id \
                ORDER BY cdna_coding_start NULLS LAST\
            ) AS _rn \
            FROM {table_name}{where_clause}\
        ) WHERE _rn = 1"
    );

    // Register deduped data as a temp table so we can query it twice with different sorts
    let df = ctx.sql(&dedup_query).await?;
    // Drop _rn column
    let schema = df.schema().clone();
    let cols: Vec<_> = schema
        .columns()
        .into_iter()
        .filter(|c| c.name() != "_rn")
        .collect();
    let df = df.select_columns(&cols.iter().map(|c| c.name()).collect::<Vec<_>>())?;
    let deduped = df.collect().await?;

    // Empty result (e.g. --chrom Y on a cache without Y data)
    if deduped.is_empty() || deduped.iter().all(|b| b.num_rows() == 0) {
        return Ok(vec![]);
    }

    // Register as temp table
    let mem_table = datafusion::datasource::MemTable::try_new(deduped[0].schema(), vec![deduped])?;
    ctx.register_table("_tl_deduped", Arc::new(mem_table))?;

    let mut results = Vec::new();

    // --- translation_core: sorted by transcript_id ---
    let core_schema =
        datafusion_bio_format_ensembl_cache::translation_core_schema(coordinate_system_zero_based);
    let core_select = core_schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let core_query = format!("SELECT {core_select} FROM _tl_deduped ORDER BY transcript_id");

    let core_file = format!("{output_dir}/{cache_dir_name}_translation_core{chrom_suffix}.parquet");
    let core_sort = &["transcript_id"];
    let core_props = writer_properties(
        EnsemblEntityKind::Translation,
        &core_schema,
        core_sort,
        None,
    );

    let core_df = ctx.sql(&core_query).await?;
    let mut core_stream = core_df.execute_stream().await?;

    let file = File::create(&core_file)
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
    let mut writer = ArrowWriter::try_new(file, core_schema.clone(), Some(core_props))?;
    let mut core_rows = 0usize;

    while let Some(batch_result) = core_stream.next().await {
        let batch = batch_result?;
        if batch.num_rows() == 0 {
            continue;
        }
        let batch = project_batch(&batch, &core_schema)?;
        core_rows += batch.num_rows();
        writer.write(&batch)?;
    }
    writer.close()?;
    println!("  translation_core: {core_rows} rows -> {core_file}");
    results.push((core_file, core_rows));

    // --- translation_sift: sorted by (chrom, start) ---
    let sift_schema =
        datafusion_bio_format_ensembl_cache::translation_sift_schema(coordinate_system_zero_based);
    let sift_select = sift_schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let sift_query = format!("SELECT {sift_select} FROM _tl_deduped ORDER BY chrom, start");

    let sift_file = format!("{output_dir}/{cache_dir_name}_translation_sift{chrom_suffix}.parquet");
    let sift_sort = &["chrom", "start"];
    // Small RGs (~256 rows) for sift: windowed 5 Mb queries need narrow RGs
    // so position predicates prune effectively on the nested list<struct> data.
    let sift_props = writer_properties(
        EnsemblEntityKind::Translation,
        &sift_schema,
        sift_sort,
        Some(256),
    );

    let sift_df = ctx.sql(&sift_query).await?;
    let mut sift_stream = sift_df.execute_stream().await?;

    let file = File::create(&sift_file)
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
    let mut writer = ArrowWriter::try_new(file, sift_schema.clone(), Some(sift_props))?;
    let mut sift_rows = 0usize;

    while let Some(batch_result) = sift_stream.next().await {
        let batch = batch_result?;
        if batch.num_rows() == 0 {
            continue;
        }
        let batch = project_batch(&batch, &sift_schema)?;
        sift_rows += batch.num_rows();
        writer.write(&batch)?;
    }
    writer.close()?;
    println!("  translation_sift: {sift_rows} rows -> {sift_file}");
    results.push((sift_file, sift_rows));

    // Deregister temp table
    ctx.deregister_table("_tl_deduped")?;

    Ok(results)
}

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let cache_root = std::env::args().nth(1).expect(
        "Usage: storable_to_parquet <cache_root> <output_dir> <entity> [partitions] [--chrom CHROM]\n\
         entity: transcript | exon | translation | regulatory | motif | variation",
    );
    let output_dir = std::env::args().nth(2).expect("Missing output_dir");
    let entity_str = std::env::args().nth(3).expect("Missing entity type");
    // Parse remaining args: [partitions] [--chrom CHROM]
    let mut partitions: usize = 8;
    let mut chrom_filter: Option<String> = None;
    let remaining: Vec<String> = std::env::args().skip(4).collect();
    let mut i = 0;
    while i < remaining.len() {
        if remaining[i] == "--chrom" {
            chrom_filter = remaining.get(i + 1).cloned();
            i += 2;
        } else if let Ok(p) = remaining[i].parse::<usize>() {
            partitions = p;
            i += 1;
        } else {
            i += 1;
        }
    }

    let (kind, table_name, entity_label) = match entity_str.as_str() {
        "transcript" | "tx" => (EnsemblEntityKind::Transcript, "tx", "transcript"),
        "exon" => (EnsemblEntityKind::Exon, "exon", "exon"),
        "translation" | "tl" => (EnsemblEntityKind::Translation, "tl", "translation"),
        "regulatory" | "reg" => (EnsemblEntityKind::RegulatoryFeature, "reg", "regulatory"),
        "motif" => (EnsemblEntityKind::MotifFeature, "motif", "motif"),
        "variation" | "var" => (EnsemblEntityKind::Variation, "var", "variation"),
        other => {
            eprintln!(
                "Unknown entity: {other}. Use: transcript, exon, translation, regulatory, motif, variation"
            );
            std::process::exit(1);
        }
    };

    // Derive version and assembly from cache_root path (e.g. .../115_GRCh38)
    let cache_dir_name = std::path::Path::new(&cache_root)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("unknown");

    println!("Cache root:  {cache_root}");
    println!("Entity:      {entity_str} ({kind:?})");
    println!("Partitions:  {partitions}");
    if let Some(ref chrom) = chrom_filter {
        println!("Chrom filter: {chrom}");
    }
    println!("Initial RSS: {:.1} MB", rss_mb());

    let config = SessionConfig::new().with_target_partitions(partitions);
    let ctx = SessionContext::new_with_config(config);

    let mut options = EnsemblCacheOptions::new(&cache_root);
    options.target_partitions = Some(partitions);
    let provider = EnsemblCacheTableProvider::for_entity(kind, options)?;
    ctx.register_table(table_name, provider)?;

    println!("After provider init RSS: {:.1} MB", rss_mb());

    // Ensure output directory exists
    std::fs::create_dir_all(&output_dir)
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;

    // Memory monitor
    let monitor_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let monitor_flag_clone = monitor_flag.clone();
    let monitor_handle = std::thread::spawn(move || {
        let mut peak = 0.0f64;
        while monitor_flag_clone.load(std::sync::atomic::Ordering::Relaxed) {
            let rss = rss_mb();
            if rss > peak {
                peak = rss;
                println!("  [monitor] RSS: {rss:.1} MB (new peak)");
            }
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        peak
    });

    let start = Instant::now();

    // --- Translation: split into two files ---
    if kind == EnsemblEntityKind::Translation {
        let results = write_translation_split(
            &ctx,
            table_name,
            &chrom_filter,
            &output_dir,
            cache_dir_name,
            false, // coordinate_system_zero_based
        )
        .await?;

        let elapsed = start.elapsed();
        monitor_flag.store(false, std::sync::atomic::Ordering::Relaxed);
        let peak_rss = monitor_handle.join().unwrap_or(0.0);

        println!();
        println!("=== Summary (translation split) ===");
        for (file, rows) in &results {
            let size = std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
            println!(
                "  {file}: {rows} rows ({:.1} MB)",
                size as f64 / 1024.0 / 1024.0
            );
        }
        println!("Elapsed:   {elapsed:.2?}");
        println!("Final RSS: {:.1} MB", rss_mb());
        println!("Peak RSS:  {peak_rss:.1} MB");
        return Ok(());
    }

    // --- All other entities: single file ---
    let chrom_suffix = if let Some(chrom) = &chrom_filter {
        format!("_{chrom}")
    } else {
        String::new()
    };
    let output_file = format!("{output_dir}/{cache_dir_name}_{entity_label}{chrom_suffix}.parquet");
    println!("Output file: {output_file}");

    // Fetch the provider schema for transcript HGNC propagation (explicit column list).
    let provider_schema: Option<SchemaRef> = if kind == EnsemblEntityKind::Transcript {
        Some(Arc::new(
            ctx.table(table_name).await?.schema().as_arrow().clone(),
        ))
    } else {
        None
    };

    let query = build_dedup_query(kind, table_name, &chrom_filter, provider_schema.as_ref());
    println!("Query: {query}");

    let df = ctx.sql(&query).await?;

    // Drop the _rn column added by ROW_NUMBER() before writing to parquet.
    // Transcript uses an explicit column list that already excludes _rn.
    let df = if kind == EnsemblEntityKind::Exon {
        let schema = df.schema().clone();
        let cols: Vec<_> = schema
            .columns()
            .into_iter()
            .filter(|c| c.name() != "_rn")
            .collect();
        df.select_columns(&cols.iter().map(|c| c.name()).collect::<Vec<_>>())?
    } else {
        df
    };

    let mut stream = df.execute_stream().await?;
    let schema = stream.schema();

    let sk = sort_key(kind);
    let props = writer_properties(kind, &schema, sk, None);

    let file = File::create(&output_file)
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut total_rows: usize = 0;
    let mut total_batches: usize = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        if batch.num_rows() == 0 {
            continue;
        }
        total_rows += batch.num_rows();
        total_batches += 1;
        writer.write(&batch)?;

        if total_batches.is_multiple_of(50) {
            println!(
                "  Progress: {total_rows} rows, {total_batches} batches | RSS: {:.1} MB",
                rss_mb()
            );
        }
    }

    writer.close()?;

    let elapsed = start.elapsed();

    monitor_flag.store(false, std::sync::atomic::Ordering::Relaxed);
    let peak_rss = monitor_handle.join().unwrap_or(0.0);

    let file_size = std::fs::metadata(&output_file)
        .map(|m| m.len())
        .unwrap_or(0);

    println!();
    println!("=== Summary ({entity_label}) ===");
    println!("Rows written:   {total_rows}");
    println!(
        "Output:         {output_file} ({:.1} MB)",
        file_size as f64 / 1024.0 / 1024.0
    );
    println!("Row group size: {}", row_group_size(kind));
    println!("Sort key:       {}", sk.join(", "));
    println!("Batches:        {total_batches}");
    println!("Elapsed:        {elapsed:.2?}");
    println!("Final RSS:      {:.1} MB", rss_mb());
    println!("Peak RSS:       {peak_rss:.1} MB");
    if elapsed.as_secs_f64() > 0.0 {
        println!(
            "Throughput:     {:.0} rows/sec",
            total_rows as f64 / elapsed.as_secs_f64()
        );
    }

    Ok(())
}
