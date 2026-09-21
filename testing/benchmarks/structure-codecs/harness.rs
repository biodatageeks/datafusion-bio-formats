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
    collections::{BTreeMap, BTreeSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

pub(super) type Decoder = fn(&[u8], &StructureOptions) -> Result<Vec<NormalizedEntry>>;
pub(super) type Raw = fn(&[u8]) -> Result<usize>;

// Diagnostic only: expose allocation differences without changing either decoder.
fn layout(entries: &[Vec<NormalizedEntry>]) -> Value {
    let mut pages = BTreeSet::new();
    let mut capacities = BTreeMap::<&str, usize>::new();
    let mut atom_capacity = 0;
    for entry in entries.iter().flatten() {
        atom_capacity += entry.atoms.capacity();
        for atom in &entry.atoms {
            for (name, value) in [
                ("atom_id", atom.atom_id.as_ref()),
                ("atom_name", Some(&atom.atom_name)),
                ("residue_name", Some(&atom.residue_name)),
                ("record_type", Some(&atom.record_type)),
                ("auth_atom_id", atom.auth_atom_id.as_ref()),
                ("auth_comp_id", atom.auth_comp_id.as_ref()),
                ("auth_asym_id", atom.auth_asym_id.as_ref()),
                ("auth_seq_id", atom.auth_seq_id.as_ref()),
            ] {
                if let Some(value) = value {
                    *capacities.entry(name).or_default() += value.capacity();
                    if !value.is_empty() {
                        pages.insert(value.as_ptr() as usize / 4096);
                    }
                }
            }
        }
    }
    let mut hash = DefaultHasher::new();
    format!("{entries:?}").hash(&mut hash);
    json!({"atom_capacity": atom_capacity, "string_capacities": capacities,
        "string_start_pages_4k": pages.len(), "normalized_debug_hash": hash.finish()})
}

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
        let mut decoded = if matches!(stage, "arrow" | "arrow_clone" | "residue") {
            inputs
                .iter()
                .map(|d| decoder(d, &options))
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![]
        };
        if stage == "arrow_clone" {
            decoded = decoded.clone();
        }
        let allocation_layout = if config["inspect_layout"].as_bool().unwrap_or(false) {
            Some(layout(&decoded))
        } else {
            None
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
                        } else if stage == "residue" {
                            for entry in &decoded[i] {
                                rows +=
                                    black_box(datafusion_bio_format_structure::residue::residues(
                                        entry, &options,
                                    ))
                                    .len();
                            }
                        } else if matches!(stage, "arrow" | "arrow_clone") {
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
                "allocation_layout": allocation_layout,
            })
        );
        Ok(())
    })
}
