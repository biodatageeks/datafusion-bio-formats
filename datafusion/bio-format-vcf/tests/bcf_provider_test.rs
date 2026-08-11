use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Int32Array, ListArray, StringArray, StructArray, UInt32Array};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::TableProvider;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_core::genotype::MissingSamplePolicy;
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

#[derive(Clone, Debug)]
struct HttpRequest {
    path: String,
    range: Option<(usize, usize)>,
}

struct RangeServer {
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl RangeServer {
    fn start(bcf: Vec<u8>, csi: Vec<u8>) -> Self {
        Self::start_inner(bcf, csi, false)
    }

    /// Serves the BCF normally but answers every CSI companion request with
    /// 403 Forbidden, mimicking a pre-signed URL that does not authorize the
    /// derived companion path.
    fn start_denying_csi(bcf: Vec<u8>, csi: Vec<u8>) -> Self {
        Self::start_inner(bcf, csi, true)
    }

    fn start_inner(bcf: Vec<u8>, csi: Vec<u8>, deny_csi: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let server_requests = Arc::clone(&requests);
        let server_stop = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(30);
            while !server_stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("range server failed: {error}"),
                };

                let mut request = [0u8; 8192];
                let size = stream.read(&mut request).unwrap();
                let request = String::from_utf8_lossy(&request[..size]);
                let mut lines = request.lines();
                let first_line = lines.next().unwrap();
                let mut parts = first_line.split_whitespace();
                let method = parts.next().unwrap();
                let path = parts.next().unwrap().to_string();
                let range = lines.find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if !name.eq_ignore_ascii_case("range") {
                        return None;
                    }
                    let value = value.trim().strip_prefix("bytes=")?;
                    let (start, end) = value.split_once('-')?;
                    Some((
                        start.parse::<usize>().unwrap(),
                        end.parse::<usize>().unwrap(),
                    ))
                });
                server_requests.lock().unwrap().push(HttpRequest {
                    path: path.clone(),
                    range,
                });

                let body = if path.starts_with("/remote.bcf.csi") {
                    if deny_csi {
                        let _ = write!(
                            stream,
                            "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                        continue;
                    }
                    &csi
                } else if path.starts_with("/remote.bcf") {
                    &bcf
                } else {
                    let _ = write!(
                        stream,
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    continue;
                };

                if method == "HEAD" {
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                } else if let Some((start, end)) = range {
                    let end = end.min(body.len() - 1);
                    let bytes = &body[start..=end];
                    let _ = write!(
                        stream,
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nConnection: close\r\n\r\n",
                        bytes.len(),
                        start,
                        end,
                        body.len()
                    );
                    let _ = stream.write_all(bytes);
                } else {
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(body);
                }
            }
        });

        Self {
            address,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    fn url(&self) -> String {
        format!("http://{}/remote.bcf", self.address)
    }
}

impl Drop for RangeServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_bcf_uses_csi_range_requests() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let index = bcf::fs::index(&bcf_path)?;
    let index_path = format!("{bcf_path}.csi");
    noodles_csi::fs::write(&index_path, &index)?;
    let server = RangeServer::start(std::fs::read(&bcf_path)?, std::fs::read(index_path)?);

    let provider =
        VcfTableProvider::new(server.url(), Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT id FROM variants WHERE chrom = 'chr2'")
        .await?
        .collect()
        .await?;
    let rows = pretty_format_batches(&batches)?.to_string();
    assert!(rows.contains("rs3"));
    assert!(!rows.contains("rs1"));

    let requests = server.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|request| request.path.starts_with("/remote.bcf.csi")),
        "CSI companion was not requested: {requests:?}"
    );
    assert!(
        requests.iter().any(|request| {
            request.path.starts_with("/remote.bcf")
                && !request.path.starts_with("/remote.bcf.csi")
                && request.range.is_some()
        }),
        "BCF payload was not read with a byte range: {requests:?}"
    );
    let csi_requests = requests
        .iter()
        .filter(|request| request.path.starts_with("/remote.bcf.csi"))
        .count();
    assert!(
        csi_requests <= 3,
        "the CSI should be fetched once at planning time and shared across \
         partitions, got {csi_requests} index requests: {requests:?}"
    );
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
async fn text_vcf_honors_explicit_missing_sample_policy() -> Result<(), Box<dyn std::error::Error>>
{
    let (_dir, vcf_path, _bcf_path) = create_equivalent_vcf_and_bcf()?;

    let error = VcfTableProvider::new_with_samples_and_format_and_policy(
        vcf_path.clone(),
        None,
        Some(vec!["GT".to_string()]),
        Some(vec!["absent".to_string()]),
        None,
        true,
        VcfInputFormat::Vcf,
        None,
        Some(MissingSamplePolicy::Error),
    )
    .unwrap_err();
    assert!(error.to_string().contains("absent"));

    VcfTableProvider::new_with_samples_and_format(
        vcf_path,
        None,
        Some(vec!["GT".to_string()]),
        Some(vec!["absent".to_string()]),
        None,
        true,
        VcfInputFormat::Vcf,
        None,
    )?;

    Ok(())
}

#[tokio::test]
async fn bcf_rejects_record_sample_count_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let mut decompressed = Vec::new();
    noodles_bgzf_vcf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;

    // BCF starts with magic/version (5 bytes), followed by the little-endian
    // header-text length (4 bytes). Each record then starts with l_shared and
    // l_indiv (8 bytes), and n_sample occupies bytes 20..23 of the shared data.
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let first_record_shared_start = 9 + header_len + 8;
    let sample_count = first_record_shared_start + 20;
    assert_eq!(&decompressed[sample_count..sample_count + 3], &[2, 0, 0]);
    decompressed[sample_count..sample_count + 3].copy_from_slice(&[0, 0, 0]);

    let mut writer = noodles_bgzf_vcf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["GT".to_string()]),
        None,
        true,
    )?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT genotypes FROM variants")
        .await?
        .collect()
        .await
        .expect_err("a BCF record/header sample-count mismatch must fail the scan")
        .to_string();

    assert!(
        error.contains("BCF record sample count 0 does not match header sample count 2"),
        "unexpected error: {error}"
    );
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

#[tokio::test]
async fn bcf_partition_splits_do_not_duplicate_spanning_records()
-> Result<(), Box<dyn std::error::Error>> {
    // Enough data on a single contig that balanced partitioning splits chr1 into
    // adjacent coordinate sub-regions, with 2000 bp REF alleles every 500 bp so
    // records straddle any split boundary the balancer picks.
    let dir = tempfile::tempdir()?;
    let vcf_path = dir.path().join("span.vcf");
    let bcf_path_buf = dir.path().join("span.bcf");
    let bcf_path = bcf_path_buf.to_string_lossy().into_owned();

    let mut vcf_text = String::from(
        "##fileformat=VCFv4.3\n\
         ##contig=<ID=chr1,length=1000000>\n\
         ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
         #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n",
    );
    let long_ref = "A".repeat(2000);
    let total_records = 800usize;
    for record_index in 0..total_records {
        let pos = 1 + record_index * 500;
        vcf_text.push_str(&format!(
            "chr1\t{pos}\trs{record_index}\t{long_ref}\tC\t50\tPASS\t.\tGT\t0/1\n"
        ));
    }
    std::fs::write(&vcf_path, &vcf_text)?;

    let mut reader = vcf::io::reader::Builder::default().build_from_path(&vcf_path)?;
    let header = reader.read_header()?;
    let mut writer = bcf::io::Writer::new(File::create(&bcf_path_buf)?);
    writer.write_header(&header)?;
    for result in reader.records() {
        writer.write_variant_record(&header, &result?)?;
    }
    writer.try_finish()?;

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

    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let state = context.state();
    let plan = provider.scan(&state, None, &[], None).await?;
    let partition_count = plan.properties().output_partitioning().partition_count();
    assert!(
        partition_count > 1,
        "expected chr1 to be split across partitions, got {partition_count}"
    );

    let batches = datafusion::physical_plan::collect(plan, context.task_ctx()).await?;
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(
        total_rows, total_records,
        "records spanning a partition boundary must be emitted exactly once"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_unknown_contig_filter_returns_empty_locally() -> Result<(), Box<dyn std::error::Error>>
{
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
        .sql("SELECT chrom, start FROM variants WHERE chrom = 'chrMissing'")
        .await?
        .collect()
        .await?;
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_bcf_unknown_contig_filter_returns_empty() -> Result<(), Box<dyn std::error::Error>>
{
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let index = bcf::fs::index(&bcf_path)?;
    let index_path = format!("{bcf_path}.csi");
    noodles_csi::fs::write(&index_path, &index)?;
    let server = RangeServer::start(std::fs::read(&bcf_path)?, std::fs::read(index_path)?);

    let provider =
        VcfTableProvider::new(server.url(), Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    context.register_table("variants", Arc::new(provider))?;

    let batches = context
        .sql("SELECT chrom, start FROM variants WHERE chrom = 'chrMissing'")
        .await?
        .collect()
        .await?;
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 0);
    Ok(())
}

#[tokio::test]
async fn bcf_info_array_missing_elements_decode_as_nulls() -> Result<(), Box<dyn std::error::Error>>
{
    // BCF encodes missing array elements as a typed sentinel; checked error
    // propagation must still map them to nulls, not fail the scan.
    let dir = tempfile::tempdir()?;
    let vcf_path = dir.path().join("missing.vcf");
    let bcf_path_buf = dir.path().join("missing.bcf");
    let bcf_path = bcf_path_buf.to_string_lossy().into_owned();

    let vcf_text = "##fileformat=VCFv4.3\n\
        ##contig=<ID=chr1,length=1000>\n\
        ##INFO=<ID=AC,Number=A,Type=Integer,Description=\"Allele count\">\n\
        #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
        chr1\t10\trs1\tA\tC,T\t50\tPASS\tAC=5,.\n";
    std::fs::write(&vcf_path, vcf_text)?;

    let mut reader = vcf::io::reader::Builder::default().build_from_path(&vcf_path)?;
    let header = reader.read_header()?;
    let mut writer = bcf::io::Writer::new(File::create(&bcf_path_buf)?);
    writer.write_header(&header)?;
    for result in reader.records() {
        writer.write_variant_record(&header, &result?)?;
    }
    writer.try_finish()?;

    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(vec!["AC".to_string()]),
        Some(Vec::new()),
        Some(Vec::new()),
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;

    let batches = context
        .sql("SELECT \"AC\" FROM variants")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let list = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("AC should be a list array");
    let values = list.value(0);
    let ints = values
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("AC elements should be Int32");
    assert_eq!(ints.len(), 2);
    assert_eq!(ints.value(0), 5);
    assert!(
        datafusion::arrow::array::Array::is_null(ints, 1),
        "missing array element must decode as null"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_bcf_stale_csi_index_fails_loudly() -> Result<(), Box<dyn std::error::Error>> {
    // Build a BCF large enough to span many BGZF blocks, then serve an object
    // truncated to 60% with the full CSI, i.e. a stale index describing data
    // beyond the object's end.
    let dir = tempfile::tempdir()?;
    let vcf_path = dir.path().join("stale.vcf");
    let bcf_path_buf = dir.path().join("stale.bcf");
    let bcf_path = bcf_path_buf.to_string_lossy().into_owned();

    let mut vcf_text = String::from(
        "##fileformat=VCFv4.3\n\
         ##contig=<ID=chr1,length=1000000>\n\
         #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n",
    );
    let long_ref = "A".repeat(2000);
    for record_index in 0..200usize {
        let pos = 1 + record_index * 500;
        vcf_text.push_str(&format!(
            "chr1\t{pos}\trs{record_index}\t{long_ref}\tC\t50\tPASS\t.\n"
        ));
    }
    std::fs::write(&vcf_path, &vcf_text)?;

    let mut reader = vcf::io::reader::Builder::default().build_from_path(&vcf_path)?;
    let header = reader.read_header()?;
    let mut writer = bcf::io::Writer::new(File::create(&bcf_path_buf)?);
    writer.write_header(&header)?;
    for result in reader.records() {
        writer.write_variant_record(&header, &result?)?;
    }
    writer.try_finish()?;

    let index = bcf::fs::index(&bcf_path)?;
    let index_path = format!("{bcf_path}.csi");
    noodles_csi::fs::write(&index_path, &index)?;

    let full = std::fs::read(&bcf_path)?;
    let truncated = full[..full.len() * 6 / 10].to_vec();
    let server = RangeServer::start(truncated, std::fs::read(index_path)?);

    let provider =
        VcfTableProvider::new(server.url(), Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    context.register_table("variants", Arc::new(provider))?;

    let result = context
        .sql("SELECT id FROM variants WHERE chrom = 'chr1'")
        .await?
        .collect()
        .await;
    let error = result
        .expect_err("a stale CSI must fail the scan, not silently return incomplete results")
        .to_string();
    assert!(
        error.contains("does not match the file"),
        "unexpected error: {error}"
    );
    Ok(())
}

const FORMAT_ARRAY_HEADER: &str = "##fileformat=VCFv4.3\n\
##contig=<ID=chr1,length=1000>\n\
##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
##FORMAT=<ID=AD,Number=2,Type=Integer,Description=\"Allelic depths\">\n";

fn write_format_array_bcf(
    dir: &TempDir,
    name: &str,
    body: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let vcf_path = dir.path().join(format!("{name}.vcf"));
    let bcf_path_buf = dir.path().join(format!("{name}.bcf"));
    std::fs::write(&vcf_path, body)?;

    let mut reader = vcf::io::reader::Builder::default().build_from_path(&vcf_path)?;
    let header = reader.read_header()?;
    let mut writer = bcf::io::Writer::new(File::create(&bcf_path_buf)?);
    writer.write_header(&header)?;
    for result in reader.records() {
        writer.write_variant_record(&header, &result?)?;
    }
    writer.try_finish()?;
    Ok((
        vcf_path.to_string_lossy().into_owned(),
        bcf_path_buf.to_string_lossy().into_owned(),
    ))
}

#[tokio::test]
async fn bcf_format_array_missing_elements_decode_as_nulls()
-> Result<(), Box<dyn std::error::Error>> {
    // Single-sample path: FORMAT AD=5,. must decode as [5, null], not error.
    let dir = tempfile::tempdir()?;
    let body = format!(
        "{FORMAT_ARRAY_HEADER}#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
         chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT:AD\t0/1:5,.\n"
    );
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "single", &body)?;

    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["AD".to_string()]),
        None,
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;

    let batches = context
        .sql("SELECT \"AD\" FROM variants")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let list = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("AD should be a list array");
    let values = list.value(0);
    let ints = values
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("AD elements should be Int32");
    assert_eq!(ints.len(), 2);
    assert_eq!(ints.value(0), 5);
    assert!(
        datafusion::arrow::array::Array::is_null(ints, 1),
        "missing FORMAT array element must decode as null"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_multisample_format_array_missing_matches_vcf() -> Result<(), Box<dyn std::error::Error>>
{
    // Multisample pools: missing FORMAT array elements must round-trip through
    // BCF identically to the text-VCF reader.
    let dir = tempfile::tempdir()?;
    let body = format!(
        "{FORMAT_ARRAY_HEADER}#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n\
         chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT:AD\t0/1:5,.\t1/1:.,7\n"
    );
    let (vcf_path, bcf_path) = write_format_array_bcf(&dir, "multi", &body)?;

    let vcf_provider = VcfTableProvider::new_with_samples_and_format(
        vcf_path,
        Some(Vec::new()),
        Some(vec!["GT".to_string(), "AD".to_string()]),
        None,
        None,
        true,
        VcfInputFormat::Vcf,
        None,
    )?;
    let bcf_provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["GT".to_string(), "AD".to_string()]),
        None,
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?;

    let vcf_rows = query(
        "variants",
        vcf_provider,
        "SELECT chrom, start, genotypes FROM variants",
    )
    .await?;
    let bcf_rows = query(
        "variants",
        bcf_provider,
        "SELECT chrom, start, genotypes FROM variants",
    )
    .await?;

    assert_eq!(bcf_rows, vcf_rows);
    assert!(bcf_rows.contains('5'));
    assert!(bcf_rows.contains('7'));
    Ok(())
}

#[tokio::test]
async fn bcf_provider_rejects_insert_overwrite() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(Vec::new()),
        Some(Vec::new()),
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;

    let result = context
        .sql(
            "INSERT OVERWRITE variants \
             SELECT chrom, start, \"end\", id, \"ref\", alt, qual, filter FROM variants",
        )
        .await?
        .collect()
        .await;
    let error = result
        .expect_err("writing through a BCF provider must be rejected")
        .to_string();
    assert!(
        error.contains("BCF write is not supported"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_bcf_forbidden_csi_probe_falls_back_to_sequential_scan()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let index = bcf::fs::index(&bcf_path)?;
    let index_path = format!("{bcf_path}.csi");
    noodles_csi::fs::write(&index_path, &index)?;
    let server =
        RangeServer::start_denying_csi(std::fs::read(&bcf_path)?, std::fs::read(index_path)?);

    // Provider construction must survive the 403 on the companion probe and
    // fall back to the unindexed sequential scan.
    let provider =
        VcfTableProvider::new(server.url(), Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT id FROM variants WHERE chrom = 'chr2'")
        .await?
        .collect()
        .await?;
    let rows = pretty_format_batches(&batches)?.to_string();
    assert!(rows.contains("rs3"));
    assert!(!rows.contains("rs1"));
    Ok(())
}

#[tokio::test]
async fn write_provider_rejects_bcf_destination_path() -> Result<(), Box<dyn std::error::Error>> {
    // Write-mode constructors hard-code the input format to text VCF; the
    // insert_into guard must still refuse a .bcf destination path, which would
    // otherwise be overwritten with text VCF under a BCF filename.
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let source = VcfTableProvider::new_with_samples_and_format(
        bcf_path.clone(),
        Some(Vec::new()),
        Some(Vec::new()),
        Some(Vec::new()),
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?;
    let schema = source.schema();

    let dir = tempfile::tempdir()?;
    let dest_path = dir.path().join("out.bcf").to_string_lossy().into_owned();
    let dest = VcfTableProvider::new_for_write(
        dest_path,
        schema,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        true,
    );

    let context = SessionContext::new();
    context.register_table("source", Arc::new(source))?;
    context.register_table("dest", Arc::new(dest))?;

    let result = context
        .sql(
            "INSERT OVERWRITE dest \
             SELECT chrom, start, \"end\", id, \"ref\", alt, qual, filter FROM source",
        )
        .await?
        .collect()
        .await;
    let error = result
        .expect_err("a .bcf destination must be rejected by the write path")
        .to_string();
    assert!(
        error.contains("BCF write is not supported"),
        "unexpected error: {error}"
    );
    Ok(())
}
