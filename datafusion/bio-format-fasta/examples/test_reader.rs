use datafusion::prelude::*;
use datafusion_bio_format_fasta::table_provider::FastaTableProvider;
use std::path::PathBuf;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../../sandbox/test.fasta");

    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    let table_provider = FastaTableProvider::new(path.to_str().unwrap().to_string(), None, None)?;
    ctx.register_table("fasta", std::sync::Arc::new(table_provider))?;

    let df = ctx.sql("SELECT * FROM fasta").await?;
    df.show().await?;

    Ok(())
}
