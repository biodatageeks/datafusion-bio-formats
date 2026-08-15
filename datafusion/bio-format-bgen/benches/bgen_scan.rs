use std::fs;
use std::hint::black_box;
use std::io::Write;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_bgen::{
    BgenOutputMode, BgenProbabilityLayout, BgenReadOptions, BgenTableProvider,
};
use flate2::Compression as FlateCompression;
use flate2::write::ZlibEncoder;
use rusqlite::{Connection, params};
use tempfile::TempDir;

const VARIANT_COUNT: usize = 2_048;
const SAMPLE_COUNT: usize = 256;

#[derive(Clone, Copy)]
enum Codec {
    None,
    Zlib,
    Zstd,
}

struct Fixture {
    _directory: TempDir,
    layout2_none: String,
    layout2_zlib: String,
    layout2_zstd: String,
    layout1: String,
}

struct IndexRow {
    chrom: String,
    position: usize,
    rsid: String,
    offset: usize,
    size: usize,
}

fn create_fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let layout2_none = directory.path().join("none.bgen");
    let layout2_zlib = directory.path().join("zlib.bgen");
    let layout2_zstd = directory.path().join("zstd.bgen");
    let layout1 = directory.path().join("layout1.bgen");
    for (path, codec, indexed) in [
        (&layout2_none, Codec::None, true),
        (&layout2_zlib, Codec::Zlib, false),
        (&layout2_zstd, Codec::Zstd, false),
    ] {
        let (bytes, rows) = encode_layout2(codec);
        fs::write(path, &bytes).unwrap();
        if indexed {
            create_bgi(&format!("{}.bgi", path.to_string_lossy()), &bytes, &rows);
        }
    }
    fs::write(&layout1, encode_layout1()).unwrap();
    Fixture {
        _directory: directory,
        layout2_none: layout2_none.to_string_lossy().into_owned(),
        layout2_zlib: layout2_zlib.to_string_lossy().into_owned(),
        layout2_zstd: layout2_zstd.to_string_lossy().into_owned(),
        layout1: layout1.to_string_lossy().into_owned(),
    }
}

fn encode_layout2(codec: Codec) -> (Vec<u8>, Vec<IndexRow>) {
    let mut sample_block = Vec::new();
    sample_block.extend_from_slice(&0_u32.to_le_bytes());
    sample_block.extend_from_slice(&(SAMPLE_COUNT as u32).to_le_bytes());
    for sample in 0..SAMPLE_COUNT {
        put_u16_string(&mut sample_block, &format!("sample{sample}"));
    }
    let sample_length = sample_block.len() as u32;
    sample_block[..4].copy_from_slice(&sample_length.to_le_bytes());

    let first_variant = 24 + sample_block.len();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&((first_variant - 4) as u32).to_le_bytes());
    bytes.extend_from_slice(&20_u32.to_le_bytes());
    bytes.extend_from_slice(&(VARIANT_COUNT as u32).to_le_bytes());
    bytes.extend_from_slice(&(SAMPLE_COUNT as u32).to_le_bytes());
    bytes.extend_from_slice(b"bgen");
    let compression = match codec {
        Codec::None => 0_u32,
        Codec::Zlib => 1,
        Codec::Zstd => 2,
    };
    bytes.extend_from_slice(&(compression | (2 << 2) | (1 << 31)).to_le_bytes());
    bytes.extend_from_slice(&sample_block);

    let mut rows = Vec::with_capacity(VARIANT_COUNT);
    for variant in 0..VARIANT_COUNT {
        let offset = bytes.len();
        put_u16_string(&mut bytes, &format!("v{variant}"));
        put_u16_string(&mut bytes, &format!("rs{variant}"));
        let chrom = if variant < VARIANT_COUNT / 2 {
            "1"
        } else {
            "2"
        };
        put_u16_string(&mut bytes, chrom);
        bytes.extend_from_slice(&((variant + 1) as u32).to_le_bytes());
        bytes.extend_from_slice(&2_u16.to_le_bytes());
        put_u32_string(&mut bytes, "A");
        put_u32_string(&mut bytes, "C");

        let mut block = Vec::with_capacity(10 + SAMPLE_COUNT * 3);
        block.extend_from_slice(&(SAMPLE_COUNT as u32).to_le_bytes());
        block.extend_from_slice(&2_u16.to_le_bytes());
        block.extend_from_slice(&[2, 2]);
        block.extend(std::iter::repeat_n(2_u8, SAMPLE_COUNT));
        block.extend_from_slice(&[0, 8]);
        for sample in 0..SAMPLE_COUNT {
            match (variant + sample) % 3 {
                0 => block.extend_from_slice(&[255, 0]),
                1 => block.extend_from_slice(&[0, 255]),
                _ => block.extend_from_slice(&[0, 0]),
            }
        }
        match codec {
            Codec::None => {
                bytes.extend_from_slice(&(block.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&block);
            }
            Codec::Zlib => {
                let mut encoder = ZlibEncoder::new(Vec::new(), FlateCompression::fast());
                encoder.write_all(&block).unwrap();
                let compressed = encoder.finish().unwrap();
                bytes.extend_from_slice(&((compressed.len() + 4) as u32).to_le_bytes());
                bytes.extend_from_slice(&(block.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&compressed);
            }
            Codec::Zstd => {
                let compressed = zstd::stream::encode_all(block.as_slice(), 1).unwrap();
                bytes.extend_from_slice(&((compressed.len() + 4) as u32).to_le_bytes());
                bytes.extend_from_slice(&(block.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&compressed);
            }
        }
        rows.push(IndexRow {
            chrom: chrom.to_string(),
            position: variant + 1,
            rsid: format!("rs{variant}"),
            offset,
            size: bytes.len() - offset,
        });
    }
    (bytes, rows)
}

fn encode_layout1() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&20_u32.to_le_bytes());
    bytes.extend_from_slice(&20_u32.to_le_bytes());
    bytes.extend_from_slice(&(VARIANT_COUNT as u32).to_le_bytes());
    bytes.extend_from_slice(&(SAMPLE_COUNT as u32).to_le_bytes());
    bytes.extend_from_slice(b"bgen");
    bytes.extend_from_slice(&(1_u32 << 2).to_le_bytes());
    for variant in 0..VARIANT_COUNT {
        bytes.extend_from_slice(&(SAMPLE_COUNT as u32).to_le_bytes());
        put_u16_string(&mut bytes, &format!("v{variant}"));
        put_u16_string(&mut bytes, &format!("rs{variant}"));
        put_u16_string(&mut bytes, "1");
        bytes.extend_from_slice(&((variant + 1) as u32).to_le_bytes());
        put_u32_string(&mut bytes, "A");
        put_u32_string(&mut bytes, "C");
        for sample in 0..SAMPLE_COUNT {
            let probabilities = match (variant + sample) % 3 {
                0 => [32_768_u16, 0, 0],
                1 => [0, 32_768, 0],
                _ => [0, 0, 32_768],
            };
            for probability in probabilities {
                bytes.extend_from_slice(&probability.to_le_bytes());
            }
        }
    }
    bytes
}

fn create_bgi(path: &str, bgen: &[u8], rows: &[IndexRow]) {
    let connection = Connection::open(path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE Metadata(
                 filename TEXT, file_size INTEGER, last_write_time INTEGER,
                 first_1000_bytes BLOB, index_creation_time INTEGER
             );
             CREATE TABLE Variant(
                 chromosome TEXT NOT NULL, position INTEGER NOT NULL,
                 rsid TEXT NOT NULL, number_of_alleles INTEGER NOT NULL,
                 allele1 TEXT, allele2 TEXT, file_start_position INTEGER NOT NULL,
                 size_in_bytes INTEGER NOT NULL
             );",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO Metadata VALUES ('benchmark.bgen', ?1, 0, ?2, 0)",
            params![bgen.len(), &bgen[..bgen.len().min(1000)]],
        )
        .unwrap();
    for row in rows {
        connection
            .execute(
                "INSERT INTO Variant VALUES (?1, ?2, ?3, 2, 'A', 'C', ?4, ?5)",
                params![row.chrom, row.position, row.rsid, row.offset, row.size],
            )
            .unwrap();
    }
}

fn put_u16_string(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u16).to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

fn put_u32_string(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

async fn context(
    path: &str,
    name: &str,
    options: BgenReadOptions,
    partitions: usize,
) -> SessionContext {
    let provider = BgenTableProvider::try_new(path, options).await.unwrap();
    let context =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(partitions));
    context.register_table(name, Arc::new(provider)).unwrap();
    context
}

/// Builds a context and proves its scan works.
///
/// Returns `None` for a combination the file does not support, so one
/// unsupported case is skipped rather than panicking the whole run. A
/// mixed-width file cannot use the fixed probability layout today; that is the
/// case this exists for, and its disappearance is a deliverable.
async fn try_context(
    path: &str,
    name: &str,
    options: BgenReadOptions,
    partitions: usize,
) -> Option<SessionContext> {
    let provider = BgenTableProvider::try_new(path, options).await.ok()?;
    let context =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(partitions));
    context.register_table(name, Arc::new(provider)).ok()?;
    // A whole scan, not a LIMIT: the case this guards against is a variant
    // whose width differs from variant 0's, and a limited scan reads variant 0.
    // The cost is one extra scan per benched combination at setup.
    context
        .sql(&format!("SELECT genotypes FROM {name}"))
        .await
        .ok()?
        .collect()
        .await
        .ok()?;
    Some(context)
}

async fn execute(context: &SessionContext, sql: &str) -> usize {
    context
        .sql(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

fn benchmarks(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fixture = create_fixture();
    let none = runtime.block_on(context(
        &fixture.layout2_none,
        "bgen_none",
        Default::default(),
        1,
    ));
    let zlib = runtime.block_on(context(
        &fixture.layout2_zlib,
        "bgen_zlib",
        Default::default(),
        1,
    ));
    let zstd = runtime.block_on(context(
        &fixture.layout2_zstd,
        "bgen_zstd",
        Default::default(),
        1,
    ));
    let layout1 = runtime.block_on(context(
        &fixture.layout1,
        "bgen_layout1",
        Default::default(),
        1,
    ));
    let dosage = runtime.block_on(context(
        &fixture.layout2_zstd,
        "bgen_dosage",
        BgenReadOptions {
            output_mode: BgenOutputMode::Dosage,
            ..Default::default()
        },
        1,
    ));
    let sparse_samples = runtime.block_on(context(
        &fixture.layout2_zlib,
        "bgen_sparse_samples",
        BgenReadOptions {
            samples: Some(vec![
                "sample1".to_string(),
                "sample127".to_string(),
                "sample255".to_string(),
            ]),
            ..Default::default()
        },
        1,
    ));
    let parallel = runtime.block_on(context(
        &fixture.layout2_zstd,
        "bgen_parallel",
        Default::default(),
        4,
    ));
    let sparse_ids = (0..VARIANT_COUNT)
        .step_by(16)
        .map(|variant| format!("'rs{variant}'"))
        .collect::<Vec<_>>()
        .join(",");
    let sparse_sql = format!("SELECT genotypes FROM bgen_none WHERE rsid IN ({sparse_ids})");

    let mut group = criterion.benchmark_group("bgen_scan");
    for (name, context, table) in [
        ("layout2_uncompressed", &none, "bgen_none"),
        ("layout2_zlib", &zlib, "bgen_zlib"),
        ("layout2_zstd", &zstd, "bgen_zstd"),
        ("layout1_uncompressed", &layout1, "bgen_layout1"),
    ] {
        group.bench_function(name, |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                black_box(execute(context, &format!("SELECT genotypes FROM {table}")).await)
            });
        });
    }
    group.bench_function("metadata_only", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(execute(&none, "SELECT chrom, rsid FROM bgen_none").await)
        });
    });
    group.bench_function("sparse_bgi_variants", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| async { black_box(execute(&none, &sparse_sql).await) });
    });
    group.bench_function("sparse_samples_zlib", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(execute(&sparse_samples, "SELECT genotypes FROM bgen_sparse_samples").await)
        });
    });
    group.bench_function("dosage_zstd", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(execute(&dosage, "SELECT genotypes FROM bgen_dosage").await)
        });
    });
    group.bench_function("parallel_zstd_4", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(execute(&parallel, "SELECT genotypes FROM bgen_parallel").await)
        });
    });

    // A real cohort file is the only fixture that can guide probability-path
    // work; the synthetic one above is 2,048 x 256 and dominated by fixed
    // costs. Opt in with BGEN_BENCH_PATH so CI, which has no such file, keeps
    // running the synthetic benches alone.
    if let Ok(real_path) = std::env::var("BGEN_BENCH_PATH") {
        // Criterion saves a baseline per benchmark id, so two fixtures sharing
        // an id would compare against each other and report the difference
        // between the files as a change in the code.
        let fixture = std::path::Path::new(&real_path)
            .file_stem()
            .map(|stem| {
                stem.to_string_lossy()
                    .chars()
                    .map(|character| {
                        if character.is_alphanumeric() {
                            character
                        } else {
                            '_'
                        }
                    })
                    .collect::<String>()
            })
            .unwrap_or_else(|| "fixture".to_string());
        let mut contexts = Vec::new();
        for (mode_name, output_mode) in [
            ("probability", BgenOutputMode::Probability),
            ("dosage", BgenOutputMode::Dosage),
        ] {
            for (layout_name, layout) in [
                ("nested", BgenProbabilityLayout::Nested),
                ("fixed", BgenProbabilityLayout::Fixed),
            ] {
                // The layout only applies to probability output.
                if output_mode == BgenOutputMode::Dosage && layout == BgenProbabilityLayout::Fixed {
                    continue;
                }
                for partitions in [1_usize, 8] {
                    let table = format!("real_{mode_name}_{layout_name}_p{partitions}");
                    let context = runtime.block_on(try_context(
                        &real_path,
                        &table,
                        BgenReadOptions {
                            output_mode,
                            probability_layout: layout,
                            ..Default::default()
                        },
                        partitions,
                    ));
                    match context {
                        Some(context) => {
                            let id =
                                format!("real_{fixture}_{mode_name}_{layout_name}_p{partitions}");
                            contexts.push((id, table, context));
                        }
                        None => eprintln!("skipping {table} on {fixture}: unsupported"),
                    }
                }
            }
        }
        for (id, table, context) in &contexts {
            let sql = format!("SELECT genotypes FROM {table}");
            group.bench_function(id, |bencher| {
                bencher
                    .to_async(&runtime)
                    .iter(|| async { black_box(execute(context, &sql).await) });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
