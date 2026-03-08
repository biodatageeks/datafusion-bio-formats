/// Convert Ensembl VEP cache storable entities to a single Parquet file.
///
/// Usage: cargo run --release --example storable_to_parquet -- <cache_root> <output_dir> <entity> [partitions] [--chrom CHROM]
///
/// entity: transcript | exon | translation | regulatory | motif | variation
///
/// Output file: <output_dir>/<version>_<assembly>_<entity>[_<chrom>].parquet
/// e.g. 115_GRCh38_transcript_22.parquet
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
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

/// Build the dedup query for entities that can appear in multiple region bins.
/// Uses ROW_NUMBER() to prefer entries with non-null CDS boundaries.
fn build_dedup_query(
    kind: EnsemblEntityKind,
    table_name: &str,
    chrom_filter: &Option<String>,
) -> String {
    let where_clause = if let Some(chrom) = chrom_filter {
        format!(" WHERE chrom = '{chrom}'")
    } else {
        String::new()
    };

    match kind {
        // Transcript: dedup by stable_id, prefer entries with non-null cds_start
        EnsemblEntityKind::Transcript => {
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY stable_id \
                        ORDER BY cds_start NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1"
            )
        }
        // Translation: dedup by transcript_id, prefer entries with non-null cdna_coding_start
        EnsemblEntityKind::Translation => {
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY transcript_id \
                        ORDER BY cdna_coding_start NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1"
            )
        }
        // Exon: dedup by (transcript_id, exon_number), prefer entries with non-null stable_id
        EnsemblEntityKind::Exon => {
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY transcript_id, exon_number \
                        ORDER BY stable_id NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1"
            )
        }
        // Other entities: no dedup needed
        _ => {
            if let Some(chrom) = chrom_filter {
                format!("SELECT * FROM {table_name} WHERE chrom = '{chrom}'")
            } else {
                format!("SELECT * FROM {table_name}")
            }
        }
    }
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

    // Build output filename: <version_assembly>_<entity>[_<chrom>].parquet
    let output_file = if let Some(ref chrom) = chrom_filter {
        format!("{output_dir}/{cache_dir_name}_{entity_label}_{chrom}.parquet")
    } else {
        format!("{output_dir}/{cache_dir_name}_{entity_label}.parquet")
    };

    println!("Cache root:  {cache_root}");
    println!("Entity:      {entity_str} ({kind:?})");
    println!("Output file: {output_file}");
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
    let output_path = std::path::Path::new(&output_file);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
    }

    // Memory monitor
    let monitor_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let monitor_flag_clone = monitor_flag.clone();
    let monitor_handle = std::thread::spawn(move || {
        let mut peak = 0.0f64;
        while monitor_flag_clone.load(std::sync::atomic::Ordering::Relaxed) {
            let rss = rss_mb();
            if rss > peak {
                peak = rss;
                println!("  [monitor] RSS: {:.1} MB (new peak)", rss);
            }
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
        peak
    });

    let start = Instant::now();

    // Build query with SQL-level dedup for transcript/exon/translation.
    // ROW_NUMBER() prefers entries with non-null CDS boundaries over nulls.
    let query = build_dedup_query(kind, table_name, &chrom_filter);
    println!("Query: {query}");

    let df = ctx.sql(&query).await?;

    // Drop the _rn column added by ROW_NUMBER() before writing to parquet
    let needs_rn_drop = matches!(
        kind,
        EnsemblEntityKind::Transcript | EnsemblEntityKind::Translation | EnsemblEntityKind::Exon
    );
    let df = if needs_rn_drop {
        let schema = df.schema().clone();
        let cols: Vec<_> = schema
            .columns()
            .into_iter()
            .filter(|c| c.name() != "_rn")
            .collect();
        df.select_columns(&cols.iter().map(|c| c.name().as_ref()).collect::<Vec<_>>())?
    } else {
        df
    };

    let mut stream = df.execute_stream().await?;
    let schema = stream.schema();

    let writer_props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(100_000)
        .build();

    // Single output file
    let file = File::create(&output_file)
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
    let mut writer = ArrowWriter::try_new(file, schema.clone().into(), Some(writer_props))?;

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

        if total_batches % 50 == 0 {
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
    println!("Batches:        {total_batches}");
    println!("Elapsed:        {elapsed:.2?}");
    println!("Final RSS:      {:.1} MB", rss_mb());
    println!("Peak RSS:       {:.1} MB", peak_rss);
    if elapsed.as_secs_f64() > 0.0 {
        println!(
            "Throughput:     {:.0} rows/sec",
            total_rows as f64 / elapsed.as_secs_f64()
        );
    }

    Ok(())
}
