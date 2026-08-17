use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_pgen::{PgenReadOptions, PgenTableProvider};
use tempfile::TempDir;

const VARIANT_COUNT: usize = 2_048;
const SAMPLE_COUNT: usize = 128;

struct Fixture {
    _directory: TempDir,
    dense: String,
    onebit: String,
    difflist: String,
    ld: String,
    dosage: String,
}

fn create_fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    write_metadata(directory.path());
    let dense = write_pgen(
        directory.path(),
        "dense",
        &(0..VARIANT_COUNT)
            .map(|variant| {
                (
                    0x00,
                    pack_codes(
                        &(0..SAMPLE_COUNT)
                            .map(|sample| ((variant + sample) % 4) as u8)
                            .collect::<Vec<_>>(),
                    ),
                )
            })
            .collect::<Vec<_>>(),
    );
    let onebit = write_pgen(
        directory.path(),
        "onebit",
        &(0..VARIANT_COUNT)
            .map(|variant| {
                let bit_byte = if variant % 2 == 0 { 0x55 } else { 0xaa };
                let mut record = vec![2];
                record.extend(std::iter::repeat_n(bit_byte, SAMPLE_COUNT.div_ceil(8)));
                record.push(0);
                (0x01, record)
            })
            .collect::<Vec<_>>(),
    );
    let difflist = write_pgen(
        directory.path(),
        "difflist",
        &(0..VARIANT_COUNT)
            .map(|variant| {
                let start = (variant % (SAMPLE_COUNT - 4)) as u8;
                (0x04, vec![4, start, 0b11_10_01_01, 1, 1, 1])
            })
            .collect::<Vec<_>>(),
    );
    let ld = write_pgen(
        directory.path(),
        "ld",
        &(0..VARIANT_COUNT)
            .map(|variant| {
                if variant % 16 == 0 {
                    (
                        0x00,
                        pack_codes(
                            &(0..SAMPLE_COUNT)
                                .map(|sample| ((variant + sample) % 3) as u8)
                                .collect::<Vec<_>>(),
                        ),
                    )
                } else {
                    (0x02, vec![0])
                }
            })
            .collect::<Vec<_>>(),
    );
    let dosage = write_pgen(
        directory.path(),
        "dosage",
        &(0..VARIANT_COUNT)
            .map(|variant| {
                let categories = (0..SAMPLE_COUNT)
                    .map(|sample| ((variant + sample) % 4) as u8)
                    .collect::<Vec<_>>();
                let mut record = pack_codes(&categories);
                for category in categories {
                    let dosage = match category {
                        0 => 0_u16,
                        1 => 16_384,
                        2 => 32_768,
                        3 => u16::MAX,
                        _ => unreachable!(),
                    };
                    record.extend(dosage.to_le_bytes());
                }
                (0x40, record)
            })
            .collect::<Vec<_>>(),
    );
    Fixture {
        _directory: directory,
        dense,
        onebit,
        difflist,
        ld,
        dosage,
    }
}

fn write_metadata(directory: &Path) {
    let mut pvar = "#CHROM\tPOS\tID\tREF\tALT\n".to_string();
    for variant in 0..VARIANT_COUNT {
        pvar.push_str(&format!(
            "{}\t{}\tv{variant}\tA\tC\n",
            if variant < VARIANT_COUNT / 2 {
                "1"
            } else {
                "2"
            },
            variant + 1
        ));
    }
    fs::write(directory.join("cohort.pvar"), pvar).unwrap();
    let mut psam = "#IID\n".to_string();
    for sample in 0..SAMPLE_COUNT {
        psam.push_str(&format!("sample{sample}\n"));
    }
    fs::write(directory.join("cohort.psam"), psam).unwrap();
}

fn write_pgen(directory: &Path, name: &str, records: &[(u8, Vec<u8>)]) -> String {
    let length_width = if records.iter().all(|(_, record)| record.len() < 256) {
        1
    } else {
        2
    };
    let control = 4 + length_width - 1;
    let header_len = 12 + 8 + records.len() + records.len() * length_width;
    let mut bytes = vec![0x6c, 0x1b, 0x10];
    bytes.extend((records.len() as u32).to_le_bytes());
    bytes.extend((SAMPLE_COUNT as u32).to_le_bytes());
    bytes.push(control as u8);
    bytes.extend((header_len as u64).to_le_bytes());
    bytes.extend(records.iter().map(|(record_type, _)| *record_type));
    for (_, record) in records {
        let encoded = (record.len() as u32).to_le_bytes();
        bytes.extend_from_slice(&encoded[..length_width]);
    }
    for (_, record) in records {
        bytes.extend(record);
    }
    let path = directory.join(format!("{name}.pgen"));
    fs::write(&path, bytes).unwrap();
    path.to_string_lossy().into_owned()
}

fn pack_codes(codes: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0; codes.len().div_ceil(4)];
    for (index, code) in codes.iter().copied().enumerate() {
        bytes[index / 4] |= code << ((index % 4) * 2);
    }
    bytes
}

async fn context(
    pgen: &str,
    name: &str,
    options: PgenReadOptions,
    partitions: usize,
) -> SessionContext {
    let directory = Path::new(pgen).parent().unwrap();
    let provider = PgenTableProvider::try_new(
        pgen,
        PgenReadOptions {
            pvar_path: Some(directory.join("cohort.pvar").to_string_lossy().into_owned()),
            psam_path: Some(directory.join("cohort.psam").to_string_lossy().into_owned()),
            ..options
        },
    )
    .await
    .unwrap();
    let context =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(partitions));
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
    let dense = runtime.block_on(context(&fixture.dense, "dense", Default::default(), 1));
    let onebit = runtime.block_on(context(&fixture.onebit, "onebit", Default::default(), 1));
    let difflist = runtime.block_on(context(
        &fixture.difflist,
        "difflist",
        Default::default(),
        1,
    ));
    let ld = runtime.block_on(context(&fixture.ld, "ld", Default::default(), 1));
    let dosage = runtime.block_on(context(
        &fixture.dosage,
        "dosage",
        PgenReadOptions {
            genotype_fields: Some(vec!["DS".to_string()]),
            ..Default::default()
        },
        1,
    ));
    let sparse_samples = runtime.block_on(context(
        &fixture.dense,
        "sparse_samples",
        PgenReadOptions {
            samples: Some(vec![
                "sample1".to_string(),
                "sample63".to_string(),
                "sample127".to_string(),
            ]),
            ..Default::default()
        },
        1,
    ));
    let parallel = runtime.block_on(context(&fixture.dense, "parallel", Default::default(), 4));
    let sparse_ids = (0..VARIANT_COUNT)
        .step_by(16)
        .map(|variant| format!("'v{variant}'"))
        .collect::<Vec<_>>()
        .join(",");
    let sparse_sql = format!("SELECT genotypes FROM dense WHERE id IN ({sparse_ids})");

    let mut group = criterion.benchmark_group("pgen_scan");
    for (name, context, table) in [
        ("dense", &dense, "dense"),
        ("onebit", &onebit, "onebit"),
        ("difflist", &difflist, "difflist"),
        ("ld_heavy", &ld, "ld"),
    ] {
        group.bench_function(name, |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                black_box(execute(context, &format!("SELECT genotypes FROM {table}")).await)
            });
        });
    }
    group.bench_function("metadata_only", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| async { black_box(execute(&dense, "SELECT chrom, id FROM dense").await) });
    });
    group.bench_function("sparse_variants", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| async { black_box(execute(&dense, &sparse_sql).await) });
    });
    group.bench_function("sparse_samples", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(execute(&sparse_samples, "SELECT genotypes FROM sparse_samples").await)
        });
    });
    group.bench_function("dosage", |bencher| {
        bencher
            .to_async(&runtime)
            .iter(|| async { black_box(execute(&dosage, "SELECT genotypes FROM dosage").await) });
    });
    group.bench_function("parallel_dense_4", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            black_box(execute(&parallel, "SELECT genotypes FROM parallel").await)
        });
    });
    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
