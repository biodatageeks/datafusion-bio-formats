//! Shared ignored-test benchmark harness; never compiled into a production API.
use async_trait::async_trait;
use datafusion::{common::Result, prelude::*};
use datafusion_bio_format_structure::{
    EntrySource, StructureLevel, StructureOptions, StructureTableProvider, batch_builder,
    model::NormalizedEntry, schema, table_provider::EntryStream,
};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

pub(super) type Decoder = fn(&[u8], &StructureOptions) -> Result<Vec<NormalizedEntry>>;
pub(super) type Raw = fn(&[u8]) -> Result<usize>;

#[derive(Debug)]
struct Source {
    data: Arc<Vec<u8>>,
    decoder: Decoder,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl EntrySource for Source {
    async fn load(&self, options: &StructureOptions) -> Result<EntryStream> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let mut entries = (self.decoder)(&self.data, options)?;
        if let Some(first) = entries.first_mut() {
            first.encoded_bytes = self.data.len();
        }
        Ok(Box::pin(futures::stream::iter(entries.into_iter().map(Ok))))
    }
}

pub(super) fn config() -> Value {
    assert!(!cfg!(debug_assertions), "benchmark requires --release");
    let path = std::env::var("BIO_CODEC_BENCH_CONFIG").expect("benchmark configuration path");
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

pub(super) fn run(config: Value, decoder: Decoder, raw: Option<Raw>) -> Result<()> {
    let stage = config["stage"].as_str().unwrap();
    let workers = config["workers"].as_u64().unwrap() as usize;
    let iterations = config["iterations"].as_u64().unwrap() as usize;
    let copies = config["copies"].as_u64().unwrap() as usize;
    let options = StructureOptions {
        level: if config["level"] == "residue" {
            StructureLevel::Residue
        } else {
            StructureLevel::Atom
        },
        ..Default::default()
    };
    let schema = schema::schema(&options);
    let projection = (0..schema.fields().len()).collect::<Vec<_>>();
    let inputs = config["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|input| {
            let data = std::fs::read(input["path"].as_str().unwrap()).unwrap();
            let start = input["offset"].as_u64().unwrap_or(0) as usize;
            let end = input["length"]
                .as_u64()
                .map_or(data.len(), |n| start + n as usize);
            Arc::new(data[start..end].to_vec())
        })
        .collect::<Vec<_>>();
    let bytes_per_iteration: usize = inputs.iter().map(|d| d.len()).sum::<usize>() * copies;
    let calls = Arc::new(AtomicUsize::new(0));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(workers));
        let sources = (0..copies)
            .flat_map(|_| {
                inputs.iter().map(|data| {
                    Arc::new(Source {
                        data: data.clone(),
                        decoder,
                        calls: calls.clone(),
                    }) as Arc<dyn EntrySource>
                })
            })
            .collect();
        let df = ctx.read_table(Arc::new(StructureTableProvider::from_sources(
            sources,
            options.clone(),
        )?))?;
        let decoded = if stage == "arrow" {
            inputs
                .iter()
                .map(|d| decoder(d, &options))
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![]
        };
        let mut seconds = 0.0;
        let mut first_batch_seconds = 0.0;
        let mut total_rows = 0;
        let mut total_calls = 0;
        // Two complete untimed rounds warm code and allocators in each process.
        for round in 0..iterations + 2 {
            calls.store(0, Ordering::Relaxed);
            let start = Instant::now();
            let mut rows = 0;
            let mut first = None;
            if stage == "query" {
                let mut stream = df.clone().execute_stream().await?;
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    first.get_or_insert_with(|| start.elapsed().as_secs_f64());
                    rows += black_box(batch).num_rows();
                }
                assert_eq!(calls.load(Ordering::Relaxed), copies * inputs.len());
            } else {
                for _ in 0..copies {
                    for (i, input) in inputs.iter().enumerate() {
                        if stage == "raw" {
                            rows += black_box(raw.expect("raw CIF adapter")(black_box(input))?);
                        } else if stage == "arrow" {
                            for entry in &decoded[i] {
                                rows += black_box(batch_builder::build(
                                    entry,
                                    &options,
                                    schema.clone(),
                                    &projection,
                                )?)
                                .num_rows();
                            }
                        } else {
                            let entries = decoder(black_box(input), &options)?;
                            calls.fetch_add(1, Ordering::Relaxed);
                            for entry in entries {
                                if stage == "pipeline" {
                                    rows += black_box(batch_builder::build(
                                        &entry,
                                        &options,
                                        schema.clone(),
                                        &projection,
                                    )?)
                                    .num_rows();
                                } else {
                                    assert_eq!(stage, "decode");
                                    rows += black_box(entry).atoms.len();
                                }
                            }
                        }
                    }
                }
            }
            let elapsed = start.elapsed().as_secs_f64();
            if round >= 2 {
                seconds += elapsed;
                first_batch_seconds += first.unwrap_or(0.0);
                total_rows += rows;
                total_calls += calls.load(Ordering::Relaxed);
            }
        }
        println!(
            "CODEC_BENCH {}",
            json!({
                "seconds": seconds, "first_batch_seconds": first_batch_seconds,
                "iterations": iterations, "rows": total_rows,
                "decode_calls": total_calls, "input_bytes": bytes_per_iteration * iterations,
            })
        );
        Ok(())
    })
}
