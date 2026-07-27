use std::fs;
use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_plink1::{PlinkReadOptions, PlinkTableProvider};
use tempfile::TempDir;

const VARIANT_COUNT: usize = 4_096;
const SAMPLE_COUNT: usize = 256;

struct Fixture {
    _directory: TempDir,
    bed_path: String,
}

fn create_fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("benchmark");
    let bed = prefix.with_extension("bed");
    let mut bim = String::new();
    for variant in 0..VARIANT_COUNT {
        let chrom = if variant < VARIANT_COUNT / 2 {
            "1"
        } else {
            "2"
        };
        bim.push_str(&format!("{chrom} rs{variant} 0 {} A C\n", variant + 1));
    }
    let mut fam = String::new();
    for sample in 0..SAMPLE_COUNT {
        fam.push_str(&format!("f sample{sample} 0 0 0 -9\n"));
    }
    let bytes_per_variant = SAMPLE_COUNT.div_ceil(4);
    let mut payload = vec![0x6c, 0x1b, 0x01];
    payload.reserve(VARIANT_COUNT * bytes_per_variant);
    for variant in 0..VARIANT_COUNT {
        for byte_index in 0..bytes_per_variant {
            let mut byte = 0_u8;
            for slot in 0..4 {
                let sample = byte_index * 4 + slot;
                let code = [0b00, 0b10, 0b11, 0b01][(variant + sample) % 4];
                byte |= code << (slot * 2);
            }
            payload.push(byte);
        }
    }
    fs::write(prefix.with_extension("bim"), bim).unwrap();
    fs::write(prefix.with_extension("fam"), fam).unwrap();
    fs::write(&bed, payload).unwrap();
    Fixture {
        _directory: directory,
        bed_path: bed.to_string_lossy().into_owned(),
    }
}

async fn context(
    fixture: &Fixture,
    name: &str,
    options: PlinkReadOptions,
    target_partitions: usize,
) -> SessionContext {
    let provider = PlinkTableProvider::try_new(&fixture.bed_path, options)
        .await
        .unwrap();
    let context = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(target_partitions),
    );
    context.register_table(name, Arc::new(provider)).unwrap();
    context
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
    let dense = runtime.block_on(context(&fixture, "dense", PlinkReadOptions::default(), 1));
    let sparse_samples = runtime.block_on(context(
        &fixture,
        "sparse_samples",
        PlinkReadOptions {
            samples: Some(vec![
                "sample1".to_string(),
                "sample127".to_string(),
                "sample255".to_string(),
            ]),
            ..Default::default()
        },
        1,
    ));
    let coalesced = runtime.block_on(context(
        &fixture,
        "coalesced",
        PlinkReadOptions {
            max_range_gap: (SAMPLE_COUNT.div_ceil(4) * 7) as u64,
            ..Default::default()
        },
        1,
    ));
    let parallel = runtime.block_on(context(
        &fixture,
        "parallel",
        PlinkReadOptions::default(),
        4,
    ));
    let sparse_ids = (0..VARIANT_COUNT)
        .step_by(8)
        .map(|variant| format!("'rs{variant}'"))
        .collect::<Vec<_>>()
        .join(",");

    let mut group = criterion.benchmark_group("plink1_scan");
    group.bench_function("dense_genotypes", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| async { black_box(execute(&dense, "SELECT genotypes FROM dense").await) });
    });
    group.bench_function("metadata_only", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| async { black_box(execute(&dense, "SELECT chrom, id FROM dense").await) });
    });
    group.bench_function("sparse_samples", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(execute(&sparse_samples, "SELECT genotypes FROM sparse_samples").await)
        });
    });
    let sparse_sql = format!("SELECT genotypes FROM dense WHERE id IN ({sparse_ids})");
    group.bench_function("sparse_variants", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| async { black_box(execute(&dense, &sparse_sql).await) });
    });
    let coalesced_sql = format!("SELECT genotypes FROM coalesced WHERE id IN ({sparse_ids})");
    group.bench_function("range_coalescing", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| async { black_box(execute(&coalesced, &coalesced_sql).await) });
    });
    group.bench_function("parallel_scan_4", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(execute(&parallel, "SELECT genotypes FROM parallel").await)
        });
    });
    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
