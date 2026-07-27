use std::fs::File;
use std::io::{Read, Write};
use std::process::Command;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, ListArray, StringArray, StructArray, UInt32Array};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::TableProvider;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_vcf::table_provider::{VcfInputFormat, VcfTableProvider};
use noodles_bcf as bcf;
use noodles_vcf as vcf;
use noodles_vcf::variant::io::Write as _;
use tempfile::TempDir;

const SOURCE_VCF: &str = r#"##fileformat=VCFv4.3
##contig=<ID=chr1,length=1000000>
##contig=<ID=chr2,length=1000000>
##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	S1	S2
chr1	10	rs1	A	C	50	PASS	AC=1;AF=0.25	GT:DP	0/1:12	0/0:9
chr1	20	rs2	G	A,T	60	PASS	AC=2,1;AF=0.5,0.25	GT:DP	1|2:20	./.:.
chr2	30	rs3	T	G	.	PASS	AC=2;AF=0.5	GT:DP	1/1:14	0/1:11
"#;

fn create_equivalent_vcf_and_bcf() -> Result<(TempDir, String, String), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let vcf_path = dir.path().join("equivalent.vcf");
    let bcf_path = dir.path().join("equivalent.bcf");
    std::fs::write(&vcf_path, SOURCE_VCF)?;

    let mut reader = vcf::io::reader::Builder::default().build_from_path(&vcf_path)?;
    let header = reader.read_header()?;
    let mut writer = bcf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_header(&header)?;
    for result in reader.records() {
        writer.write_variant_record(&header, &result?)?;
    }
    writer.try_finish()?;

    Ok((
        dir,
        vcf_path.to_string_lossy().into_owned(),
        bcf_path.to_string_lossy().into_owned(),
    ))
}

async fn query(
    table_name: &str,
    provider: VcfTableProvider,
    sql: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let context = SessionContext::new();
    context.register_table(table_name, Arc::new(provider))?;
    let batches = context.sql(sql).await?.collect().await?;
    Ok(pretty_format_batches(&batches)?.to_string())
}

#[tokio::test]
async fn bcf_matches_vcf_for_projected_multisample_values() -> Result<(), Box<dyn std::error::Error>>
{
    let (_dir, vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let samples = Some(vec!["S2".to_string(), "S1".to_string()]);
    let info = Some(vec!["AC".to_string(), "AF".to_string()]);
    let format = Some(vec!["GT".to_string(), "DP".to_string()]);

    let vcf_provider = VcfTableProvider::new_with_samples_and_format(
        vcf_path,
        info.clone(),
        format.clone(),
        samples.clone(),
        None,
        true,
        VcfInputFormat::Vcf,
        None,
    )?;
    let bcf_provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        info,
        format,
        samples,
        None,
        true,
        VcfInputFormat::Auto,
        None,
    )?;

    assert_eq!(vcf_provider.schema(), bcf_provider.schema());

    let vcf_rows = query(
        "variants",
        vcf_provider,
        "SELECT chrom, start, id, \"AC\", genotypes FROM variants ORDER BY chrom, start",
    )
    .await?;
    let bcf_rows = query(
        "variants",
        bcf_provider,
        "SELECT chrom, start, id, \"AC\", genotypes FROM variants ORDER BY chrom, start",
    )
    .await?;

    assert_eq!(bcf_rows, vcf_rows);
    assert!(bcf_rows.contains("rs2"));
    assert!(bcf_rows.contains("1|2"));
    assert!(bcf_rows.contains("./."));
    Ok(())
}

#[tokio::test]
async fn bcf_uses_csi_for_region_queries() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let index = bcf::fs::index(&bcf_path)?;
    let index_path = format!("{bcf_path}.csi");
    noodles_csi::fs::write(&index_path, &index)?;

    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(Vec::new()),
        Some(Vec::new()),
        None,
        true,
        VcfInputFormat::Bcf,
        Some(index_path),
    )?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    context.register_table("variants", Arc::new(provider))?;

    let batches = context
        .sql(
            "SELECT chrom, start, id FROM variants \
             WHERE chrom = 'chr2' AND start >= 29 AND start < 30",
        )
        .await?
        .collect()
        .await?;
    let rows = pretty_format_batches(&batches)?.to_string();

    assert!(rows.contains("rs3"));
    assert!(!rows.contains("rs1"));
    assert!(!rows.contains("rs2"));
    Ok(())
}

#[tokio::test]
async fn bcf_matches_bcftools_oracle() -> Result<(), Box<dyn std::error::Error>> {
    let available = Command::new("bcftools").arg("--version").output().is_ok();
    if !available {
        assert_ne!(
            std::env::var("REQUIRE_BCFTOOLS").as_deref(),
            Ok("1"),
            "bcftools is required when REQUIRE_BCFTOOLS=1"
        );
        eprintln!("bcftools not installed; skipping external BCF oracle");
        return Ok(());
    }

    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let oracle = Command::new("bcftools")
        .args([
            "query",
            "-f",
            "%CHROM\t%POS\t%ID\t%REF\t%ALT\t%INFO/AC[\t%GT]\n",
            &bcf_path,
        ])
        .output()?;
    assert!(
        oracle.status.success(),
        "bcftools failed: {}",
        String::from_utf8_lossy(&oracle.stderr)
    );

    let provider = VcfTableProvider::new(
        bcf_path,
        Some(vec!["AC".to_string()]),
        Some(vec!["GT".to_string()]),
        None,
        true,
    )?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql(
            "SELECT chrom, start, id, ref, alt, \"AC\", genotypes \
             FROM variants ORDER BY chrom, start",
        )
        .await?
        .collect()
        .await?;

    let mut actual = String::new();
    for batch in batches {
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
        let ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let reference = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let alternate = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ac = batch
            .column(5)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let ac_values = ac.values().as_any().downcast_ref::<Int32Array>().unwrap();
        let genotypes = batch
            .column(6)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let gt = genotypes
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let gt_values = gt.values().as_any().downcast_ref::<StringArray>().unwrap();

        for row in 0..batch.num_rows() {
            let ac_values = ac
                .value_offsets()
                .windows(2)
                .nth(row)
                .map(|offsets| {
                    (offsets[0]..offsets[1])
                        .map(|index| ac_values.value(index as usize).to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap();
            let gt_values = gt
                .value_offsets()
                .windows(2)
                .nth(row)
                .map(|offsets| {
                    (offsets[0]..offsets[1])
                        .map(|index| gt_values.value(index as usize))
                        .collect::<Vec<_>>()
                        .join("\t")
                })
                .unwrap();
            actual.push_str(&format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                chrom.value(row),
                start.value(row) + 1,
                ids.value(row),
                reference.value(row),
                alternate.value(row).replace('|', ","),
                ac_values,
                gt_values,
            ));
        }
    }

    assert_eq!(actual, String::from_utf8(oracle.stdout)?);
    Ok(())
}

#[test]
fn bcf_rejects_unknown_samples_by_default() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let error = VcfTableProvider::new_with_samples(
        bcf_path,
        None,
        Some(vec!["GT".to_string()]),
        Some(vec!["absent".to_string()]),
        None,
        true,
    )
    .unwrap_err();

    assert!(error.to_string().contains("absent"));
    Ok(())
}

#[tokio::test]
async fn truncated_bcf_fails_during_scan() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let mut decompressed = Vec::new();
    noodles_bgzf_vcf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    decompressed.truncate(decompressed.len() - 5);
    let mut writer = noodles_bgzf_vcf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let result = context
        .sql("SELECT chrom, start FROM variants")
        .await?
        .collect()
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("BCF"));
    Ok(())
}
