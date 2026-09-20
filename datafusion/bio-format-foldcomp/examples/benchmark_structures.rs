//! Small reproducible stage/scaling benchmark. Run with --release for performance conclusions.
use datafusion::{catalog::TableProvider, prelude::*};
use datafusion_bio_format_foldcomp::{FoldcompOptions, FoldcompTableProvider, codec};
use datafusion_bio_format_structure::{
    StructureLevel, StructureOptions, StructureTableProvider, batch_builder, mmcif, pdb, schema,
};
use futures::StreamExt;
use std::{hint::black_box, sync::Arc, time::Instant};
#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let root = format!(
        "{}/../../testing/data/structure",
        env!("CARGO_MANIFEST_DIR")
    );
    let opts = StructureOptions::default();
    println!("stage,format,level,workers,iteration,rows,seconds,first_batch_seconds");
    for format in ["pdb", "cif", "fcz"] {
        let path = format!("{root}/1ubq.{format}");
        let bytes = std::fs::read(&path)?;
        let parse = || -> datafusion::common::Result<_> {
            Ok(match format {
                "pdb" => pdb::parse(std::str::from_utf8(&bytes).unwrap(), &opts)?.remove(0),
                "cif" => mmcif::parse(&bytes, &opts)?.remove(0),
                _ => codec::decode(&bytes, &opts)?,
            })
        };
        let start = Instant::now();
        for _ in 0..32 {
            black_box(parse()?);
        }
        println!(
            "parse,{format},atom,1,0,{}, {},",
            parse()?.atoms.len() * 32,
            start.elapsed().as_secs_f64()
        );
        let entry = parse()?;
        for level in [StructureLevel::Atom, StructureLevel::Residue] {
            let options = StructureOptions {
                level,
                ..opts.clone()
            };
            let schema = schema::schema(&options);
            let projection = (0..schema.fields().len()).collect::<Vec<_>>();
            let start = Instant::now();
            let mut rows = 0;
            for _ in 0..32 {
                rows += black_box(batch_builder::build(
                    &entry,
                    &options,
                    schema.clone(),
                    &projection,
                )?)
                .num_rows();
            }
            println!(
                "arrow,{format},{level:?},1,0,{rows},{},",
                start.elapsed().as_secs_f64()
            );
        }
    }
    for workers in [1, 2, 4, 8] {
        for format in ["pdb", "cif", "foldcomp"] {
            for level in [StructureLevel::Atom, StructureLevel::Residue] {
                let options = StructureOptions {
                    level,
                    ..opts.clone()
                };
                let ctx = SessionContext::new_with_config(
                    SessionConfig::new().with_target_partitions(workers),
                );
                let table: Arc<dyn TableProvider> = if format == "foldcomp" {
                    Arc::new(FoldcompTableProvider::new(
                        format!("{root}/example_db"),
                        FoldcompOptions {
                            structure: options,
                            entry_keys: Some(vec![0, 7]),
                            ..Default::default()
                        },
                    )?)
                } else {
                    Arc::new(StructureTableProvider::new(
                        vec![format!("{root}/1ubq.{format}"); 32],
                        None,
                        options,
                        None,
                    )?)
                };
                let df = ctx.read_table(table)?;
                for iteration in 0..2 {
                    let start = Instant::now();
                    let mut stream = df.clone().execute_stream().await?;
                    let mut first = None;
                    let mut rows = 0;
                    while let Some(batch) = stream.next().await {
                        first.get_or_insert_with(|| start.elapsed().as_secs_f64());
                        rows += batch?.num_rows();
                    }
                    println!(
                        "scan,{format},{level:?},{workers},{iteration},{rows},{},{}",
                        start.elapsed().as_secs_f64(),
                        first.unwrap_or_default()
                    );
                }
            }
        }
    }
    Ok(())
}
