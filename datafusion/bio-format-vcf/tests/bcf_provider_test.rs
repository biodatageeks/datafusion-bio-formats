use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{
    Int8Array, Int32Array, Int64Array, ListArray, StringArray, StructArray, UInt32Array,
};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use datafusion_bio_format_core::{
    GENOTYPE_COUNTED_ALLELE_KEY, GENOTYPE_OUTPUT_MODE_KEY, GENOTYPE_SAMPLE_NAMES_KEY,
    from_json_string, genotype::MissingSamplePolicy,
};
use datafusion_bio_format_vcf::table_provider::{
    GenotypeOutputMode, VcfInputFormat, VcfTableProvider, describe_fields,
};
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

fn create_partitioned_bcf() -> Result<(TempDir, String, String, usize), Box<dyn std::error::Error>>
{
    // Long, overlapping records make a single contig large enough for the
    // balancer to split while exercising exact record ownership at boundaries.
    let dir = tempfile::tempdir()?;
    let vcf_path = dir.path().join("partitioned.vcf");
    let bcf_path_buf = dir.path().join("partitioned.bcf");
    let bcf_path = bcf_path_buf.to_string_lossy().into_owned();

    let mut vcf_text = String::from(
        // Deliberately omit the contig length: CSI bin geometry must still make
        // this single-contig input splittable, as in common real-world headers.
        "##fileformat=VCFv4.3\n\
         ##contig=<ID=chr1>\n\
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

    Ok((dir, bcf_path, index_path, total_records))
}

#[derive(Clone, Debug)]
struct HttpRequest {
    method: String,
    path: String,
    range: Option<(usize, usize)>,
}

struct RangeServer {
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
    deny_bcf_head: Arc<AtomicBool>,
    shorten_bcf_ranges: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl RangeServer {
    fn start(bcf: Vec<u8>, csi: Vec<u8>) -> Self {
        Self::start_inner(bcf, csi, false, false, false)
    }

    /// Serves the BCF normally but answers every CSI companion request with
    /// 403 Forbidden, mimicking a pre-signed URL that does not authorize the
    /// derived companion path.
    fn start_denying_csi(bcf: Vec<u8>, csi: Vec<u8>) -> Self {
        Self::start_inner(bcf, csi, true, false, false)
    }

    /// Rejects CSI HEAD immediately. The BCF HEAD policy can be tightened after
    /// provider construction to isolate requests made by the indexed scan.
    fn start_denying_csi_head(bcf: Vec<u8>, csi: Vec<u8>) -> Self {
        Self::start_inner(bcf, csi, false, true, false)
    }

    fn start_inner(
        bcf: Vec<u8>,
        csi: Vec<u8>,
        deny_csi: bool,
        deny_csi_head: bool,
        deny_bcf_head: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let deny_bcf_head = Arc::new(AtomicBool::new(deny_bcf_head));
        let shorten_bcf_ranges = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let server_requests = Arc::clone(&requests);
        let server_deny_bcf_head = Arc::clone(&deny_bcf_head);
        let server_shorten_bcf_ranges = Arc::clone(&shorten_bcf_ranges);
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
                stream.set_nonblocking(false).unwrap();

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
                    method: method.to_string(),
                    path: path.clone(),
                    range,
                });

                let is_bcf_payload =
                    path.starts_with("/remote.bcf") && !path.starts_with("/remote.bcf.csi");
                let body = if path.starts_with("/remote.bcf.csi") {
                    if deny_csi || (deny_csi_head && method == "HEAD") {
                        let _ = write!(
                            stream,
                            "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                        continue;
                    }
                    &csi
                } else if path.starts_with("/remote.bcf") {
                    if server_deny_bcf_head.load(Ordering::Relaxed) && method == "HEAD" {
                        let _ = write!(
                            stream,
                            "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                        continue;
                    }
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
                    if start >= body.len() {
                        let _ = write!(
                            stream,
                            "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        continue;
                    }
                    let mut end = end.min(body.len() - 1);
                    if server_shorten_bcf_ranges.load(Ordering::Relaxed)
                        && is_bcf_payload
                        && end.saturating_sub(start) >= 32
                    {
                        end -= 1;
                    }
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
            deny_bcf_head,
            shorten_bcf_ranges,
            stop,
            thread: Some(thread),
        }
    }

    fn url(&self) -> String {
        format!("http://{}/remote.bcf", self.address)
    }

    fn deny_bcf_head(&self) {
        self.deny_bcf_head.store(true, Ordering::Relaxed);
    }

    /// Makes later large BCF range GETs return a successful response one byte
    /// short, as can happen when a stale CSI end lies beyond EOF.
    fn shorten_bcf_ranges(&self) {
        self.shorten_bcf_ranges.store(true, Ordering::Relaxed);
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
    let bcf_schema = bcf_provider.schema();
    let genotypes = bcf_schema.field_with_name("genotypes")?;
    let shared_sample_names_json = genotypes
        .metadata()
        .get(GENOTYPE_SAMPLE_NAMES_KEY)
        .expect("multi-sample BCF must expose the shared genotype sample metadata key");
    let shared_sample_names: Vec<String> = from_json_string(shared_sample_names_json)
        .expect("shared genotype sample metadata must be valid JSON");
    assert_eq!(shared_sample_names, ["S2", "S1"]);

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
async fn bcf_describe_fields_matches_vcf() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;

    let vcf_fields = describe_fields(&vcf_path, None).await?;
    let bcf_fields = describe_fields(&bcf_path, None).await?;

    assert_eq!(bcf_fields, vcf_fields);
    Ok(())
}

#[tokio::test]
async fn local_bcf_rejects_csi_with_different_reference_count()
-> Result<(), Box<dyn std::error::Error>> {
    let (_bcf_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let (_index_dir, _partitioned_bcf, foreign_index_path, _) = create_partitioned_bcf()?;

    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(Vec::new()),
        Some(Vec::new()),
        None,
        true,
        VcfInputFormat::Bcf,
        Some(foreign_index_path),
    )?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;

    let error = context
        .sql("SELECT id FROM variants WHERE chrom = 'chr1'")
        .await?
        .collect()
        .await
        .expect_err("a CSI for a different reference dictionary must fail the scan")
        .to_string();
    assert!(
        error.contains("BCF header has 2 contigs, but CSI has 1"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn bcf_reports_indexed_coordinate_pushdown_as_inexact() -> Result<(), Box<dyn std::error::Error>> {
    let (dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let index = bcf::fs::index(&bcf_path)?;
    let conventional_index_path = format!("{bcf_path}.csi");
    assert!(!std::path::Path::new(&conventional_index_path).exists());
    let index_path = dir
        .path()
        .join("coordinate-pushdown.explicit.csi")
        .to_string_lossy()
        .into_owned();
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
    let filter = col("chrom")
        .eq(lit("chr1"))
        .and(col("start").gt_eq(lit(9u32)));

    let support = provider.supports_filters_pushdown(&[&filter])?;
    assert_eq!(support, [TableProviderFilterPushDown::Inexact]);
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
    server.requests.lock().unwrap().clear();
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
        .collect::<Vec<_>>();
    assert_eq!(
        csi_requests.len(),
        1,
        "the CSI should be fetched once at planning time and shared across \
         partitions: {requests:?}"
    );
    assert_eq!(csi_requests[0].method, "GET");
    let bcf_non_range_requests = requests
        .iter()
        .filter(|request| {
            request.path.starts_with("/remote.bcf")
                && !request.path.starts_with("/remote.bcf.csi")
                && request.range.is_none()
        })
        .count();
    assert_eq!(
        bcf_non_range_requests, 0,
        "the header parsed at provider construction should be shared across \
         partitions: {requests:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_bcf_rejects_csi_with_different_reference_count()
-> Result<(), Box<dyn std::error::Error>> {
    let (_bcf_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let (_index_dir, _partitioned_bcf, foreign_index_path, _) = create_partitioned_bcf()?;
    let server = RangeServer::start(
        std::fs::read(&bcf_path)?,
        std::fs::read(foreign_index_path)?,
    );

    let provider =
        VcfTableProvider::new(server.url(), Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;

    let error = context
        .sql("SELECT id FROM variants WHERE chrom = 'chr1'")
        .await?
        .collect()
        .await
        .expect_err("a remote CSI for a different reference dictionary must fail the scan")
        .to_string();
    assert!(
        error.contains("BCF header has 2 contigs, but CSI has 1"),
        "unexpected error: {error}"
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
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;

    // BCF starts with magic/version (5 bytes), followed by the little-endian
    // header-text length (4 bytes). Each record then starts with l_shared and
    // l_indiv (8 bytes), and n_sample occupies bytes 20..23 of the shared data.
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let first_record_shared_start = 9 + header_len + 8;
    let sample_count = first_record_shared_start + 20;
    assert_eq!(&decompressed[sample_count..sample_count + 3], &[2, 0, 0]);
    decompressed[sample_count..sample_count + 3].copy_from_slice(&[0, 0, 0]);

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bcf_rejects_oversized_declared_header_before_allocation_locally_and_remotely()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    decompressed[5..9].copy_from_slice(&u32::MAX.to_le_bytes());

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;
    drop(writer);

    let local_error = VcfTableProvider::new(
        bcf_path.clone(),
        Some(Vec::new()),
        Some(Vec::new()),
        None,
        true,
    )
    .expect_err("an oversized local BCF header must fail before allocation")
    .to_string();
    assert!(
        local_error.contains("invalid BCF header length") && local_error.contains("safety limit"),
        "unexpected local error: {local_error}"
    );

    let server = RangeServer::start(std::fs::read(&bcf_path)?, Vec::new());
    let remote_error =
        VcfTableProvider::new(server.url(), Some(Vec::new()), Some(Vec::new()), None, true)
            .expect_err("an oversized remote BCF header must fail before allocation")
            .to_string();
    assert!(
        remote_error.contains("invalid BCF header length") && remote_error.contains("safety limit"),
        "unexpected remote error: {remote_error}"
    );
    Ok(())
}

#[tokio::test]
async fn truncated_bcf_fails_during_scan() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    decompressed.truncate(decompressed.len() - 5);
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
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
async fn bcf_partition_splits_without_contig_length_do_not_duplicate_spanning_records()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, bcf_path, index_path, total_records) = create_partitioned_bcf()?;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_bcf_without_contig_length_executes_multiple_csi_partitions_with_unique_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, bcf_path, index_path, total_records) = create_partitioned_bcf()?;
    let server = RangeServer::start(std::fs::read(bcf_path)?, std::fs::read(index_path)?);
    let provider = VcfTableProvider::new_with_samples_and_format(
        server.url(),
        Some(Vec::new()),
        Some(Vec::new()),
        Some(Vec::new()),
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?;
    server.requests.lock().unwrap().clear();

    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let plan = provider.scan(&context.state(), None, &[], None).await?;
    let partition_count = plan.properties().output_partitioning().partition_count();
    assert!(
        partition_count > 1,
        "expected a remote BCF full scan to expose multiple CSI partitions, got {partition_count}"
    );

    let batches = datafusion::physical_plan::collect(plan, context.task_ctx()).await?;
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, total_records);
    let mut ids = HashSet::with_capacity(total_records);
    for batch in &batches {
        let values = batch
            .column_by_name("id")
            .expect("the unprojected scan must include id")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("BCF id must be Utf8");
        for id in values.iter().flatten() {
            assert!(ids.insert(id.to_string()), "duplicate remote BCF id: {id}");
        }
    }
    assert_eq!(ids.len(), total_records);
    assert!((0..total_records).all(|record_index| ids.contains(&format!("rs{record_index}"))));

    let requests = server.requests.lock().unwrap();
    let bcf_ranges = requests
        .iter()
        .filter(|request| {
            request.method == "GET"
                && request.path.starts_with("/remote.bcf")
                && !request.path.starts_with("/remote.bcf.csi")
                && request.range.is_some()
        })
        .count();
    assert!(
        bcf_ranges >= partition_count,
        "each remote scan partition should issue indexed BCF range GETs: \
         partitions={partition_count}, range requests={bcf_ranges}, requests={requests:?}"
    );
    assert!(
        requests.iter().all(|request| {
            !request.path.starts_with("/remote.bcf")
                || request.path.starts_with("/remote.bcf.csi")
                || request.range.is_some()
        }),
        "remote partition execution must not use a full BCF GET: {requests:?}"
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_bcf_short_range_response_fails_loudly() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let index = bcf::fs::index(&bcf_path)?;
    let index_path = format!("{bcf_path}.csi");
    noodles_csi::fs::write(&index_path, &index)?;
    let server = RangeServer::start(std::fs::read(&bcf_path)?, std::fs::read(index_path)?);

    let provider =
        VcfTableProvider::new(server.url(), Some(Vec::new()), Some(Vec::new()), None, true)?;
    server.shorten_bcf_ranges();
    let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    context.register_table("variants", Arc::new(provider))?;

    let error = context
        .sql("SELECT id FROM variants WHERE chrom = 'chr2'")
        .await?
        .collect()
        .await
        .expect_err("a successful short CSI range response must fail the scan")
        .to_string();
    assert!(
        error.contains("the index does not match the file"),
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
async fn bcf_dosage_rejects_invalid_format_selection_when_samples_are_empty()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let invalid_selections = [
        None,
        Some(Vec::new()),
        Some(vec!["DP".to_string()]),
        Some(vec!["GT".to_string(), "DP".to_string()]),
    ];

    for format_fields in invalid_selections {
        let provider = VcfTableProvider::new_with_samples_and_format(
            bcf_path.clone(),
            Some(Vec::new()),
            format_fields,
            Some(Vec::new()),
            None,
            true,
            VcfInputFormat::Bcf,
            None,
        )?;
        let error = provider
            .with_genotype_output_mode(GenotypeOutputMode::Dosage)
            .expect_err("dosage must require GT even when no samples are materialized")
            .to_string();
        assert!(
            error.contains("requires GT as the only selected FORMAT field"),
            "unexpected error: {error}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn bcf_dosage_rejects_gt_missing_from_header_when_samples_are_empty()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=DP,Number=1,Type=Integer,Description=\"Read depth\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tDP\t12\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "dosage-no-gt", body)?;
    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["GT".to_string()]),
        Some(Vec::new()),
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?;
    let error = provider
        .with_genotype_output_mode(GenotypeOutputMode::Dosage)
        .expect_err("dosage must require GT to be declared in the BCF header")
        .to_string();
    assert!(
        error.contains("requires a GT FORMAT field declared in the BCF header"),
        "unexpected error: {error}"
    );

    Ok(())
}

#[tokio::test]
async fn bcf_dosage_allows_default_formats_when_header_declares_only_gt()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT\t0/1\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "dosage-default-gt", body)?;
    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        None,
        None,
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?
    .with_genotype_output_mode(GenotypeOutputMode::Dosage)?;

    assert_eq!(
        provider.schema().field_with_name("GT")?.data_type(),
        &datafusion::arrow::datatypes::DataType::Int8
    );
    Ok(())
}

#[tokio::test]
async fn bcf_dosage_decodes_phase_missingness_ploidy_and_sample_order()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT\t0|0\t0/1\n\
                chr1\t20\trs2\tG\tT\t50\tPASS\t.\tGT\t1/1\t./1\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "dosage", body)?;
    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["GT".to_string()]),
        Some(vec!["S2".to_string(), "S1".to_string()]),
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?
    .with_genotype_output_mode(GenotypeOutputMode::Dosage)?;

    let genotypes_field = provider.schema().field_with_name("genotypes")?.clone();
    let datafusion::arrow::datatypes::DataType::Struct(children) = genotypes_field.data_type()
    else {
        panic!("multisample dosage must retain the genotypes struct");
    };
    let gt_field = children.iter().find(|field| field.name() == "GT").unwrap();
    let datafusion::arrow::datatypes::DataType::List(item) = gt_field.data_type() else {
        panic!("multisample dosage GT must remain a list");
    };
    assert_eq!(
        item.data_type(),
        &datafusion::arrow::datatypes::DataType::Int8
    );

    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT genotypes FROM variants ORDER BY start")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct");
    let gt = genotypes
        .column_by_name("GT")
        .expect("GT should be projected")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("GT should contain one dosage list per row");

    let row0 = gt.value(0);
    let row0 = row0.as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(row0.values(), &[1, 0]);

    let row1 = gt.value(1);
    let row1 = row1.as_any().downcast_ref::<Int8Array>().unwrap();
    assert!(datafusion::arrow::array::Array::is_null(row1, 0));
    assert_eq!(row1.value(1), 2);

    Ok(())
}

#[tokio::test]
async fn bcf_dosage_decodes_single_sample_to_nullable_int8()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT\t0|1\n\
                chr1\t20\trs2\tG\tT\t50\tPASS\t.\tGT\t./.\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "dosage-single", body)?;
    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["GT".to_string()]),
        None,
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?
    .with_genotype_output_mode(GenotypeOutputMode::Dosage)?;
    assert_eq!(
        provider.schema().field_with_name("GT")?.data_type(),
        &datafusion::arrow::datatypes::DataType::Int8
    );

    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT \"GT\" FROM variants ORDER BY start")
        .await?
        .collect()
        .await?;
    let gt = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int8Array>()
        .expect("single-sample GT dosage should be Int8");
    assert_eq!(gt.value(0), 1);
    assert!(datafusion::arrow::array::Array::is_null(gt, 1));
    Ok(())
}

#[tokio::test]
async fn bcf_dosage_allows_an_explicit_empty_sample_selection()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT\t0/0\t0/1\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "dosage-no-samples", body)?;
    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["GT".to_string()]),
        Some(Vec::new()),
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?
    .with_genotype_output_mode(GenotypeOutputMode::Dosage)?;
    assert!(provider.schema().field_with_name("genotypes").is_err());
    assert_eq!(
        provider
            .schema()
            .metadata()
            .get("bio.vcf.genotype_output_mode")
            .map(String::as_str),
        Some("dosage")
    );
    assert_eq!(
        provider
            .schema()
            .metadata()
            .get(GENOTYPE_OUTPUT_MODE_KEY)
            .map(String::as_str),
        Some("dosage")
    );
    assert_eq!(
        provider
            .schema()
            .metadata()
            .get(GENOTYPE_COUNTED_ALLELE_KEY)
            .map(String::as_str),
        Some("1")
    );

    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    Ok(())
}

#[tokio::test]
async fn bcf_dosage_rejects_multiallelic_records_with_or_without_gt_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    for query in [
        "SELECT genotypes FROM variants",
        "SELECT COUNT(*) FROM variants",
    ] {
        let provider = VcfTableProvider::new_with_samples_and_format(
            bcf_path.clone(),
            Some(Vec::new()),
            Some(vec!["GT".to_string()]),
            None,
            None,
            true,
            VcfInputFormat::Bcf,
            None,
        )?
        .with_genotype_output_mode(GenotypeOutputMode::Dosage)?;
        let context = SessionContext::new();
        context.register_table("variants", Arc::new(provider))?;
        let error = context
            .sql(query)
            .await?
            .collect()
            .await
            .expect_err("dosage must reject multiple ALT alleles for every projection")
            .to_string();
        assert!(
            error.contains("supports exactly one ALT allele"),
            "unexpected error for {query}: {error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn bcf_dosage_filters_unselected_multiallelic_records_consistently()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let sql = "SELECT id, genotypes FROM variants WHERE chrom = 'chr2'";

    // Without a CSI companion, the sequential decoder sees the chr1
    // multiallelic record before reaching the selected chr2 record.
    let sequential_provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path.clone(),
        Some(Vec::new()),
        Some(vec!["GT".to_string()]),
        None,
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?
    .with_genotype_output_mode(GenotypeOutputMode::Dosage)?;
    let sequential_rows = query("variants", sequential_provider, sql).await?;

    // Adding CSI pushdown must change I/O planning, not query semantics.
    let index = bcf::fs::index(&bcf_path)?;
    let index_path = format!("{bcf_path}.csi");
    noodles_csi::fs::write(&index_path, &index)?;
    let indexed_provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["GT".to_string()]),
        None,
        None,
        true,
        VcfInputFormat::Bcf,
        Some(index_path),
    )?
    .with_genotype_output_mode(GenotypeOutputMode::Dosage)?;
    let indexed_rows = query("variants", indexed_provider, sql).await?;

    assert_eq!(sequential_rows, indexed_rows);
    assert!(sequential_rows.contains("rs3"));
    assert!(!sequential_rows.contains("rs1"));
    assert!(!sequential_rows.contains("rs2"));
    Ok(())
}

#[tokio::test]
async fn bcf_dosage_validates_gt_with_dependent_formats_on_filtered_fast_path()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##contig=<ID=chr2,length=1000>\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                ##FORMAT=<ID=XG,Number=G,Type=Integer,Description=\"Genotype values\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT:XG\t0/1:0,10,20\n\
                chr2\t20\trs2\tG\tT\t50\tPASS\t.\tGT:XG\t1/1:20,10,0\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "dosage-number-g", body)?;

    let dosage_provider = |path: String| {
        VcfTableProvider::new_with_samples_and_format(
            path,
            Some(Vec::new()),
            Some(vec!["GT".to_string()]),
            None,
            None,
            true,
            VcfInputFormat::Bcf,
            None,
        )?
        .with_genotype_output_mode(GenotypeOutputMode::Dosage)
    };
    let sql = "SELECT id, \"GT\" FROM variants WHERE chrom = 'chr2'";

    let rows = query("variants", dosage_provider(bcf_path.clone())?, sql).await?;
    assert!(rows.contains("rs2"));

    // Number=G makes the FORMAT validator validate the complete GT payload on
    // the fast dosage path. A corrupt GT in the filtered chr1 record must still
    // fail, even though dosage compatibility is checked only for selected rows.
    corrupt_bcf_gt_allele_value(&bcf_path, 1, 0x82)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(dosage_provider(bcf_path)?))?;
    let error = context
        .sql(sql)
        .await?
        .collect()
        .await
        .expect_err("filtered GT corruption must remain visible on the dependent FORMAT path")
        .to_string();
    assert!(
        error.contains("reserved or invalid value"),
        "unexpected error: {error}"
    );

    Ok(())
}

#[tokio::test]
async fn bcf_dosage_validates_unselected_gt_samples() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT\t0/0\t0/1\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "dosage-subset", body)?;
    // The third flat allele belongs to unselected S2. 0x82 is a reserved
    // negative Int8 encoding and must fail even though only S1 is materialized.
    corrupt_bcf_gt_allele_value(&bcf_path, 2, 0x82)?;

    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["GT".to_string()]),
        Some(vec!["S1".to_string()]),
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?
    .with_genotype_output_mode(GenotypeOutputMode::Dosage)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT genotypes FROM variants")
        .await?
        .collect()
        .await
        .expect_err("an invalid unselected GT sample must fail the dosage scan")
        .to_string();
    assert!(
        error.contains("reserved or invalid value"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_zero_width_gt_descriptor_without_panicking()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT\t0/1\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "zero-width-gt", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let samples_start = record_start + 8 + shared_len;
    let descriptor_offset = bcf_typed_value_end(&decompressed, samples_start);
    assert_eq!(decompressed[descriptor_offset], 0x21);
    decompressed[descriptor_offset] = 0x01;
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["GT".to_string()]),
        None,
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?
    .with_genotype_output_mode(GenotypeOutputMode::Dosage)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("zero-width GT must be rejected as data corruption")
        .to_string();
    assert!(
        error.contains("invalid zero-length encoding"),
        "unexpected error: {error}"
    );
    Ok(())
}

fn bcf_typed_value_end(data: &[u8], offset: usize) -> usize {
    let descriptor = data[offset];
    let value_count = usize::from(descriptor >> 4);
    assert!(value_count < 0x0f, "test fixture should use short lengths");
    let value_width = match descriptor & 0x0f {
        0 => 0,
        1 | 7 => 1,
        2 => 2,
        3 | 5 => 4,
        code => panic!("unexpected BCF type code in test fixture: {code}"),
    };
    offset + 1 + value_count * value_width
}

fn corrupt_bcf_record_dictionary_index(
    bcf_path: &str,
    target: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let replacement_index = if target == "filter_info" {
        let mut reader = bcf::io::Reader::new(File::open(bcf_path)?);
        let header = reader.read_header()?;
        header
            .string_maps()
            .strings()
            .get_index_of("AC")
            .expect("AC should have a BCF dictionary entry")
    } else {
        126
    };
    let replacement_index = u8::try_from(replacement_index)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let site_start = record_start + 8;
    let allele_count =
        u16::from_le_bytes(decompressed[site_start + 18..site_start + 20].try_into()?) as usize;

    let corrupt_scalar_i8 = |data: &mut [u8], offset: usize| {
        assert_eq!(data[offset], 0x11, "fixture dictionary ID should be i8");
        data[offset + 1] = replacement_index;
    };

    if target == "contig" {
        decompressed[site_start..site_start + 4].copy_from_slice(&126i32.to_le_bytes());
    } else {
        let mut cursor = site_start + 24;
        cursor = bcf_typed_value_end(&decompressed, cursor); // IDs
        for _ in 0..allele_count {
            cursor = bcf_typed_value_end(&decompressed, cursor); // REF and ALTs
        }

        if matches!(target, "filter" | "filter_info") {
            corrupt_scalar_i8(&mut decompressed, cursor);
        } else {
            cursor = bcf_typed_value_end(&decompressed, cursor); // FILTER
            if target == "info" {
                corrupt_scalar_i8(&mut decompressed, cursor);
            } else if target == "format" {
                let samples_start = site_start + shared_len;
                corrupt_scalar_i8(&mut decompressed, samples_start);
            } else {
                panic!("unknown dictionary target: {target}");
            }
        }
    }

    let mut writer = noodles_bgzf::io::Writer::new(File::create(bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;
    Ok(())
}

fn corrupt_bcf_first_value_descriptor_type(
    bcf_path: &str,
    target: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let site_start = record_start + 8;

    let descriptor_offset = if target == "info" {
        let allele_count =
            u16::from_le_bytes(decompressed[site_start + 18..site_start + 20].try_into()?) as usize;
        let mut cursor = site_start + 24;
        cursor = bcf_typed_value_end(&decompressed, cursor); // IDs
        for _ in 0..allele_count {
            cursor = bcf_typed_value_end(&decompressed, cursor); // REF and ALTs
        }
        cursor = bcf_typed_value_end(&decompressed, cursor); // FILTER
        bcf_typed_value_end(&decompressed, cursor) // INFO key
    } else if target == "format" {
        let samples_start = site_start + shared_len;
        bcf_typed_value_end(&decompressed, samples_start) // FORMAT key
    } else {
        panic!("unknown BCF value target: {target}");
    };

    assert_eq!(
        decompressed[descriptor_offset] & 0x0f,
        1,
        "fixture value should use an integer descriptor"
    );
    decompressed[descriptor_offset] = (decompressed[descriptor_offset] & 0xf0) | 7;

    let mut writer = noodles_bgzf::io::Writer::new(File::create(bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;
    Ok(())
}

fn corrupt_bcf_gt_allele_value(
    bcf_path: &str,
    allele_offset: usize,
    encoded_value: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let site_start = record_start + 8;
    let samples_start = site_start + shared_len;
    let descriptor_offset = bcf_typed_value_end(&decompressed, samples_start); // GT key
    assert_eq!(
        decompressed[descriptor_offset], 0x21,
        "fixture GT should encode two i8 alleles"
    );
    decompressed[descriptor_offset + 1 + allele_offset] = encoded_value;

    let mut writer = noodles_bgzf::io::Writer::new(File::create(bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;
    Ok(())
}

#[tokio::test]
async fn bcf_validates_dictionary_indices_when_columns_are_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FILTER=<ID=q10,Description=\"Low quality\">\n\
                ##INFO=<ID=AC,Number=A,Type=Integer,Description=\"Allele count\">\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tq10\tAC=1\tGT\t0/1\n";
    let cases: &[(&str, &str)] = &[
        ("contig", "contig dictionary"),
        ("filter", "FILTER dictionary"),
        ("filter_info", "no FILTER header definition"),
        ("info", "INFO dictionary"),
        ("format", "FORMAT dictionary"),
    ];

    for (name, expected_error) in cases {
        let dir = tempfile::tempdir()?;
        let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, name, body)?;
        corrupt_bcf_record_dictionary_index(&bcf_path, name)?;

        let provider =
            VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
        let context = SessionContext::new();
        context.register_table("variants", Arc::new(provider))?;
        let error = context
            .sql("SELECT COUNT(*) FROM variants")
            .await?
            .collect()
            .await
            .expect_err("an unprojected invalid BCF dictionary index must fail the scan")
            .to_string();
        assert!(
            error.contains(expected_error),
            "unexpected {name} error: {error}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn bcf_rejects_pass_combined_with_a_failing_filter_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FILTER=<ID=q10,Description=\"Low quality\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tq10\t.\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "pass-and-q10", body)?;

    // Expand the valid scalar q10 FILTER payload to [PASS, q10]. PASS has the
    // reserved dictionary index 0, and the record's shared section grows by
    // one byte.
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let site_start = record_start + 8;
    let allele_count =
        u16::from_le_bytes(decompressed[site_start + 18..site_start + 20].try_into()?) as usize;
    let mut filter_start = site_start + 24;
    filter_start = bcf_typed_value_end(&decompressed, filter_start); // IDs
    for _ in 0..allele_count {
        filter_start = bcf_typed_value_end(&decompressed, filter_start); // REF and ALTs
    }
    assert_eq!(
        decompressed[filter_start], 0x11,
        "q10 should use a scalar i8 FILTER dictionary index"
    );
    assert_ne!(
        decompressed[filter_start + 1],
        0,
        "q10 should not use the reserved PASS index"
    );
    decompressed[filter_start] = 0x21;
    decompressed.insert(filter_start + 1, 0);
    decompressed[record_start..record_start + 4]
        .copy_from_slice(&u32::try_from(shared_len + 1)?.to_le_bytes());

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("PASS combined with q10 must fail an unprojected scan")
        .to_string();
    assert!(
        error.contains("BCF FILTER contains PASS together with a failing filter"),
        "unexpected error: {error}"
    );

    Ok(())
}

#[tokio::test]
async fn bcf_validates_core_fields_when_columns_are_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\n";
    let cases = [
        ("id", "invalid BCF ID"),
        ("reference", "invalid BCF reference allele"),
        ("alternate", "invalid BCF alternate allele"),
        ("quality", "invalid BCF quality score"),
        ("allele-count", "invalid BCF allele count"),
    ];

    for (name, expected_error) in cases {
        let dir = tempfile::tempdir()?;
        let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, name, body)?;
        let mut decompressed = Vec::new();
        noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
        let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
        let site_start = 9 + header_len + 8;
        let ids_offset = site_start + 24;
        let reference_offset = bcf_typed_value_end(&decompressed, ids_offset);
        let alternate_offset = bcf_typed_value_end(&decompressed, reference_offset);

        match name {
            "id" => decompressed[ids_offset + 1] = 0xff,
            "reference" => decompressed[reference_offset + 1] = 0xff,
            "alternate" => decompressed[alternate_offset + 1] = 0xff,
            "quality" => decompressed[site_start + 12..site_start + 16]
                .copy_from_slice(&0x7f80_0002_u32.to_le_bytes()),
            "allele-count" => {
                decompressed[site_start + 18..site_start + 20].copy_from_slice(&0u16.to_le_bytes())
            }
            _ => unreachable!(),
        }

        let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
        writer.write_all(&decompressed)?;
        writer.try_finish()?;

        let provider =
            VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
        let context = SessionContext::new();
        context.register_table("variants", Arc::new(provider))?;
        let error = context
            .sql("SELECT COUNT(*) FROM variants")
            .await?
            .collect()
            .await
            .expect_err("an invalid unprojected BCF core field must fail the scan")
            .to_string();
        assert!(
            error.contains(expected_error),
            "unexpected {name} error: {error}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn bcf_validates_position_when_columns_are_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\n";
    let cases = [
        (-1i32, "BCF record has no position"),
        (-2, "invalid BCF position"),
    ];

    for (encoded_position, expected_error) in cases {
        let dir = tempfile::tempdir()?;
        let (_vcf_path, bcf_path) =
            write_format_array_bcf(&dir, &format!("position-{encoded_position}"), body)?;

        let mut decompressed = Vec::new();
        noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
        let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
        let site_start = 9 + header_len + 8;
        decompressed[site_start + 4..site_start + 8]
            .copy_from_slice(&encoded_position.to_le_bytes());
        let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
        writer.write_all(&decompressed)?;
        writer.try_finish()?;

        let provider =
            VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
        let context = SessionContext::new();
        context.register_table("variants", Arc::new(provider))?;
        let error = context
            .sql("SELECT COUNT(*) FROM variants")
            .await?
            .collect()
            .await
            .expect_err("an invalid unprojected BCF position must fail the scan")
            .to_string();
        assert!(
            error.contains(expected_error),
            "unexpected position {encoded_position} error: {error}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn bcf_rejects_oversized_declared_record_before_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    decompressed[record_start..record_start + 4].copy_from_slice(&u32::MAX.to_le_bytes());

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("an oversized declared BCF record must fail before allocation")
        .to_string();
    assert!(
        error.contains("invalid BCF record length") && error.contains("safety limit"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_validates_fixed_span_when_columns_are_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\n";

    for encoded_span in [0i32, -1] {
        let dir = tempfile::tempdir()?;
        let (_vcf_path, bcf_path) =
            write_format_array_bcf(&dir, &format!("span-{encoded_span}"), body)?;
        let mut decompressed = Vec::new();
        noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
        let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
        let site_start = 9 + header_len + 8;
        decompressed[site_start + 8..site_start + 12].copy_from_slice(&encoded_span.to_le_bytes());

        let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
        writer.write_all(&decompressed)?;
        writer.try_finish()?;

        let provider =
            VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
        let context = SessionContext::new();
        context.register_table("variants", Arc::new(provider))?;
        let error = context
            .sql("SELECT COUNT(*) FROM variants")
            .await?
            .collect()
            .await
            .expect_err("a nonpositive unprojected BCF span must fail the scan")
            .to_string();
        assert!(
            error.contains(&format!("invalid BCF record span: rlen is {encoded_span}")),
            "unexpected span {encoded_span} error: {error}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn bcf_logical_end_preserves_info_end_over_fixed_span()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=END,Number=1,Type=Integer,Description=\"End position\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\tdel1\tA\t<DEL>\t50\tPASS\tEND=100\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "info-end", body)?;
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let site_start = 9 + header_len + 8;
    // Keep rlen valid but make it disagree with INFO/END. The fixed span must
    // still be validated, while the logical end column follows VCF semantics.
    decompressed[site_start + 8..site_start + 12].copy_from_slice(&1i32.to_le_bytes());
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT \"end\" FROM variants")
        .await?
        .collect()
        .await?;
    let end = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("end should be UInt32");
    assert_eq!(end.value(0), 100);
    Ok(())
}

#[tokio::test]
async fn bcf_validates_info_end_when_columns_are_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    for encoded_end in [0i32, -1] {
        let dir = tempfile::tempdir()?;
        let body = "##fileformat=VCFv4.3\n\
                    ##contig=<ID=chr1,length=1000>\n\
                    ##INFO=<ID=END,Number=1,Type=Integer,Description=\"End position\">\n\
                    #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                    chr1\t10\tdel1\tA\t<DEL>\t50\tPASS\tEND=100\n";
        let (_vcf_path, bcf_path) =
            write_format_array_bcf(&dir, &format!("invalid-info-end-{encoded_end}"), body)?;
        let mut decompressed = Vec::new();
        noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
        let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
        let record_start = 9 + header_len;
        let shared_len =
            u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
        let shared_start = record_start + 8;
        let shared_end = shared_start + shared_len;
        let end_value_offset = decompressed[shared_start..shared_end]
            .windows(2)
            .rposition(|window| window == [0x11, 100])
            .map(|offset| shared_start + offset + 1)
            .expect("END=100 should use a one-value Int8 BCF encoding");
        decompressed[end_value_offset] = encoded_end as u8;
        let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
        writer.write_all(&decompressed)?;
        writer.try_finish()?;

        let provider =
            VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
        let context = SessionContext::new();
        context.register_table("variants", Arc::new(provider))?;
        let error = context
            .sql("SELECT COUNT(*) FROM variants")
            .await?
            .collect()
            .await
            .expect_err("an invalid INFO/END must fail an unprojected scan")
            .to_string();
        assert!(
            error.contains("invalid BCF variant span")
                && error.contains("invalid INFO END position"),
            "unexpected END={encoded_end} error: {error}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn bcf_rejects_wrong_fixed_info_cardinality_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=DP,Number=3,Type=Integer,Description=\"Read depths\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\tDP=5,7,9\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "fixed-info-extra", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let declaration = b"##INFO=<ID=DP,Number=3";
    let declaration_start = decompressed
        .windows(declaration.len())
        .position(|window| window == declaration)
        .expect("DP INFO declaration should be present in the BCF header");
    decompressed[declaration_start + declaration.len() - 1] = b'2';
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("a wrong fixed INFO cardinality must fail an unprojected scan")
        .to_string();
    assert!(
        error.contains("INFO field 'DP' declares 2 values") && error.contains("encodes 3 values"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_multicharacter_info_values_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=CH,Number=1,Type=Character,Description=\"Character value\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\tCH=A\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "multicharacter-info", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let site_start = record_start + 8;
    let allele_count =
        u16::from_le_bytes(decompressed[site_start + 18..site_start + 20].try_into()?) as usize;
    let mut value_start = site_start + 24;
    value_start = bcf_typed_value_end(&decompressed, value_start); // IDs
    for _ in 0..allele_count {
        value_start = bcf_typed_value_end(&decompressed, value_start); // REF and ALTs
    }
    value_start = bcf_typed_value_end(&decompressed, value_start); // FILTER
    value_start = bcf_typed_value_end(&decompressed, value_start); // CH key
    assert_eq!(
        decompressed[value_start], 0x17,
        "CH should contain one string byte"
    );
    decompressed[value_start] = 0x27;
    decompressed.insert(value_start + 2, b'B');
    decompressed[record_start..record_start + 4]
        .copy_from_slice(&u32::try_from(shared_len + 1)?.to_le_bytes());

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("a multi-character unprojected Character INFO value must fail the scan")
        .to_string();
    assert!(
        error.contains("invalid BCF INFO Character field 'CH'")
            && error.contains("contains 2 characters"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_duplicate_info_fields_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Read depth\">\n\
                ##INFO=<ID=ZZ,Number=1,Type=Integer,Description=\"Auxiliary value\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\tDP=7;ZZ=9\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "duplicate-info", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let site_start = record_start + 8;
    let allele_count =
        u16::from_le_bytes(decompressed[site_start + 18..site_start + 20].try_into()?) as usize;
    let mut dp_key_start = site_start + 24;
    dp_key_start = bcf_typed_value_end(&decompressed, dp_key_start); // IDs
    for _ in 0..allele_count {
        dp_key_start = bcf_typed_value_end(&decompressed, dp_key_start); // REF and ALTs
    }
    dp_key_start = bcf_typed_value_end(&decompressed, dp_key_start); // FILTER
    assert_eq!(
        decompressed[dp_key_start], 0x11,
        "DP key should use a scalar i8 dictionary index"
    );
    let dp_key_index = decompressed[dp_key_start + 1];
    let dp_value_start = bcf_typed_value_end(&decompressed, dp_key_start);
    let zz_key_start = bcf_typed_value_end(&decompressed, dp_value_start);
    assert_eq!(
        decompressed[zz_key_start], 0x11,
        "ZZ key should use a scalar i8 dictionary index"
    );
    decompressed[zz_key_start + 1] = dp_key_index;

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("a duplicate unprojected INFO field must fail the scan")
        .to_string();
    assert!(
        error.contains("BCF record contains duplicate INFO field 'DP'"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_counts_logical_info_values_before_vector_end() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=DP,Number=2,Type=Integer,Description=\"Read depths\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\tDP=5,7\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "info-vector-end", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let site_start = 9 + header_len + 8;
    let allele_count =
        u16::from_le_bytes(decompressed[site_start + 18..site_start + 20].try_into()?) as usize;
    let mut cursor = site_start + 24;
    cursor = bcf_typed_value_end(&decompressed, cursor); // IDs
    for _ in 0..allele_count {
        cursor = bcf_typed_value_end(&decompressed, cursor); // REF and ALTs
    }
    cursor = bcf_typed_value_end(&decompressed, cursor); // FILTER
    cursor = bcf_typed_value_end(&decompressed, cursor); // INFO key
    assert_eq!(decompressed[cursor], 0x21, "DP should contain two i8s");
    decompressed[cursor + 2] = 0x81; // i8 vector-end in the second storage slot

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("vector-end must not count as a biological INFO value")
        .to_string();
    assert!(
        error.contains("INFO field 'DP' declares 2 values") && error.contains("encodes 1 values"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_data_after_info_string_vector_end_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=TXT,Number=.,Type=String,Description=\"Text values\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\tTXT=ABC\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "info-string-after-end", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let site_start = 9 + header_len + 8;
    let allele_count =
        u16::from_le_bytes(decompressed[site_start + 18..site_start + 20].try_into()?) as usize;
    let mut value_start = site_start + 24;
    value_start = bcf_typed_value_end(&decompressed, value_start); // IDs
    for _ in 0..allele_count {
        value_start = bcf_typed_value_end(&decompressed, value_start); // REF and ALTs
    }
    value_start = bcf_typed_value_end(&decompressed, value_start); // FILTER
    value_start = bcf_typed_value_end(&decompressed, value_start); // TXT key
    assert_eq!(
        decompressed[value_start], 0x37,
        "TXT should contain three string bytes"
    );
    assert_eq!(&decompressed[value_start + 1..value_start + 4], b"ABC");
    decompressed[value_start + 2] = 0;

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("data after an unprojected INFO string vector-end must fail the scan")
        .to_string();
    assert!(
        error.contains("invalid BCF INFO string for field 'TXT'")
            && error.contains("value after vector-end at offset 2"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_accepts_fixed_string_info_cardinality() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=LABELS,Number=2,Type=String,Description=\"Labels\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\tLABELS=alpha,beta\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "fixed-string-info", body)?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    Ok(())
}

#[tokio::test]
async fn bcf_accepts_missing_fixed_info_array() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=DP,Number=2,Type=Integer,Description=\"Read depths\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\tDP=5,7\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "fixed-info-missing", body)?;

    // The pinned noodles writer cannot emit a whole-field missing INFO value,
    // so rewrite the two-i8 payload to the valid scalar i8 missing sentinel.
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let site_start = record_start + 8;
    let allele_count =
        u16::from_le_bytes(decompressed[site_start + 18..site_start + 20].try_into()?) as usize;
    let mut cursor = site_start + 24;
    cursor = bcf_typed_value_end(&decompressed, cursor); // IDs
    for _ in 0..allele_count {
        cursor = bcf_typed_value_end(&decompressed, cursor); // REF and ALTs
    }
    cursor = bcf_typed_value_end(&decompressed, cursor); // FILTER
    cursor = bcf_typed_value_end(&decompressed, cursor); // INFO key
    assert_eq!(decompressed[cursor], 0x21, "DP should contain two i8s");
    decompressed[cursor] = 0x11;
    decompressed[cursor + 1] = 0x80;
    decompressed.remove(cursor + 2);
    decompressed[record_start..record_start + 4]
        .copy_from_slice(&u32::try_from(shared_len - 1)?.to_le_bytes());
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_wrong_allele_dependent_info_cardinality_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        ("alternate", "AC", '2', 'A', "Number=A (1 expected)"),
        ("reference", "AD", '3', 'R', "Number=R (2 expected)"),
    ];

    for (name, field, original_number, replacement_number, expected_error) in cases {
        let dir = tempfile::tempdir()?;
        let body = format!(
            "##fileformat=VCFv4.3\n\
             ##contig=<ID=chr1,length=1000>\n\
             ##INFO=<ID={field},Number={original_number},Type=Integer,Description=\"Values\">\n\
             #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
             chr1\t10\trs1\tA\tC\t50\tPASS\t{field}={}\n",
            if original_number == '2' {
                "5,7"
            } else {
                "5,7,9"
            }
        );
        let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, name, &body)?;

        let mut decompressed = Vec::new();
        noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
        let declaration = format!("##INFO=<ID={field},Number={original_number}");
        let declaration_start = decompressed
            .windows(declaration.len())
            .position(|window| window == declaration.as_bytes())
            .expect("INFO declaration should be present in the BCF header");
        decompressed[declaration_start + declaration.len() - 1] = replacement_number as u8;
        let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
        writer.write_all(&decompressed)?;
        writer.try_finish()?;

        let provider =
            VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
        let context = SessionContext::new();
        context.register_table("variants", Arc::new(provider))?;
        let error = context
            .sql("SELECT COUNT(*) FROM variants")
            .await?
            .collect()
            .await
            .expect_err("a wrong allele-dependent INFO cardinality must fail the scan")
            .to_string();
        assert!(
            error.contains(expected_error),
            "unexpected {name} error: {error}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn bcf_rejects_wrong_fixed_format_cardinality_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=AD,Number=3,Type=Integer,Description=\"Allelic depths\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tAD\t5,7,9\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "fixed-format-extra", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let declaration = b"##FORMAT=<ID=AD,Number=3";
    let declaration_start = decompressed
        .windows(declaration.len())
        .position(|window| window == declaration)
        .expect("AD FORMAT declaration should be present in the BCF header");
    decompressed[declaration_start + declaration.len() - 1] = b'2';
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("a wrong fixed FORMAT cardinality must fail an unprojected scan")
        .to_string();
    assert!(
        error.contains("FORMAT field 'AD' declares 2 values")
            && error.contains("encodes 3 values per sample"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_invalid_utf8_in_unprojected_dynamic_format_string()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=TXT,Number=.,Type=String,Description=\"Text values\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tTXT\tvalid\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "dynamic-format-utf8", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let samples_start = record_start + 8 + shared_len;
    let descriptor_offset = bcf_typed_value_end(&decompressed, samples_start); // TXT key
    assert_eq!(
        decompressed[descriptor_offset] & 0x0f,
        7,
        "TXT should use a BCF string descriptor"
    );
    decompressed[descriptor_offset + 1] = 0xff;

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("invalid UTF-8 in an unprojected dynamic FORMAT string must fail the scan")
        .to_string();
    assert!(
        error.contains("invalid BCF FORMAT string for field 'TXT', sample 0"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_data_after_format_string_vector_end_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=TXT,Number=.,Type=String,Description=\"Text values\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tTXT\tA\tABC\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "format-string-after-end", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let samples_start = record_start + 8 + shared_len;
    let descriptor_offset = bcf_typed_value_end(&decompressed, samples_start); // TXT key
    assert_eq!(
        decompressed[descriptor_offset], 0x37,
        "TXT should contain three string bytes per sample"
    );
    assert_eq!(
        &decompressed[descriptor_offset + 1..descriptor_offset + 4],
        b"A\0\0"
    );
    decompressed[descriptor_offset + 3] = b'B';

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("data after an unprojected FORMAT string vector-end must fail the scan")
        .to_string();
    assert!(
        error.contains("invalid BCF FORMAT string for field 'TXT', sample 0")
            && error.contains("value after vector-end at offset 2"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_counts_logical_format_values_before_vector_end()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=AD,Number=2,Type=Integer,Description=\"Allelic depths\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tAD\t5,7\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "format-vector-end", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let samples_start = record_start + 8 + shared_len;
    let descriptor_offset = bcf_typed_value_end(&decompressed, samples_start); // AD key
    assert_eq!(
        decompressed[descriptor_offset], 0x21,
        "AD should contain two i8s per sample"
    );
    decompressed[descriptor_offset + 2] = 0x81;

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("vector-end must not count as a biological FORMAT value")
        .to_string();
    assert!(
        error.contains("FORMAT field 'AD' declares 2 values")
            && error.contains("encodes 1 values per sample"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_wrong_allele_dependent_format_cardinality_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        ("alternate-format", "AC", '2', 'A', "Number=A (1 expected)"),
        ("reference-format", "AD", '3', 'R', "Number=R (2 expected)"),
    ];

    for (name, field, original_number, replacement_number, expected_error) in cases {
        let dir = tempfile::tempdir()?;
        let body = format!(
            "##fileformat=VCFv4.3\n\
             ##contig=<ID=chr1,length=1000>\n\
             ##FORMAT=<ID={field},Number={original_number},Type=Integer,Description=\"Values\">\n\
             #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
             chr1\t10\trs1\tA\tC\t50\tPASS\t.\t{field}\t{}\n",
            if original_number == '2' {
                "5,7"
            } else {
                "5,7,9"
            }
        );
        let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, name, &body)?;

        let mut decompressed = Vec::new();
        noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
        let declaration = format!("##FORMAT=<ID={field},Number={original_number}");
        let declaration_start = decompressed
            .windows(declaration.len())
            .position(|window| window == declaration.as_bytes())
            .expect("FORMAT declaration should be present in the BCF header");
        decompressed[declaration_start + declaration.len() - 1] = replacement_number as u8;
        let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
        writer.write_all(&decompressed)?;
        writer.try_finish()?;

        let provider =
            VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
        let context = SessionContext::new();
        context.register_table("variants", Arc::new(provider))?;
        let error = context
            .sql("SELECT COUNT(*) FROM variants")
            .await?
            .collect()
            .await
            .expect_err("a wrong allele-dependent FORMAT cardinality must fail the scan")
            .to_string();
        assert!(
            error.contains(expected_error),
            "unexpected {name} error: {error}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn bcf_rejects_wrong_gt_dependent_format_cardinality_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                ##FORMAT=<ID=XG,Number=2,Type=Integer,Description=\"Values\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tXG:GT\t5,7:0/1\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "number-g", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let declaration = b"##FORMAT=<ID=XG,Number=2";
    let declaration_start = decompressed
        .windows(declaration.len())
        .position(|window| window == declaration)
        .expect("XG FORMAT declaration should be present in the BCF header");
    decompressed[declaration_start + declaration.len() - 1] = b'G';
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("a wrong Number=G FORMAT cardinality must fail the scan")
        .to_string();
    assert!(
        error.contains("Number=G (3 expected for sample 0 with ploidy 2)"),
        "unexpected error: {error}"
    );

    Ok(())
}

#[tokio::test]
async fn bcf_accepts_number_g_cardinality_for_mixed_ploidy_samples()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                ##FORMAT=<ID=XG,Number=G,Type=Integer,Description=\"Genotype values\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tXG:GT\t5,7,9:0/1\t5,7:1\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "number-g-mixed-ploidy", body)?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );

    Ok(())
}

#[tokio::test]
async fn bcf_accepts_compact_all_missing_fixed_format_array()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=AD,Number=2,Type=Integer,Description=\"Allelic depths\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tAD\t5,7\t8,9\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "fixed-format-missing", body)?;

    // Compact the two values per sample to one type-specific missing sentinel,
    // which is the valid BCF representation when the entire series is absent.
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let individual_len =
        u32::from_le_bytes(decompressed[record_start + 4..record_start + 8].try_into()?) as usize;
    let samples_start = record_start + 8 + shared_len;
    let descriptor_offset = bcf_typed_value_end(&decompressed, samples_start); // AD key
    assert_eq!(
        decompressed[descriptor_offset], 0x21,
        "AD should encode two i8 values per sample"
    );
    decompressed[descriptor_offset] = 0x11;
    let payload_start = descriptor_offset + 1;
    decompressed[payload_start] = 0x80;
    decompressed[payload_start + 1] = 0x80;
    decompressed.drain(payload_start + 2..payload_start + 4);
    decompressed[record_start + 4..record_start + 8]
        .copy_from_slice(&u32::try_from(individual_len - 2)?.to_le_bytes());
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_unprojected_invalid_gt_allele_encodings()
-> Result<(), Box<dyn std::error::Error>> {
    type GtMutationCase<'a> = (&'a str, &'a [(usize, u8)], &'a str);

    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                ##FORMAT=<ID=XG,Number=G,Type=Integer,Description=\"Genotype values\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT:XG\t0/1:.\n";
    let cases: &[GtMutationCase<'_>] = &[
        ("out-of-range", &[(1, 0x06)], "GT allele index 2"),
        ("reserved", &[(1, 0x82)], "reserved or invalid value -126"),
        ("after-vector-end", &[(0, 0x81)], "value after vector-end"),
        (
            "zero-ploidy-with-missing-number-g",
            &[(0, 0x81), (1, 0x81)],
            "genotype has zero ploidy",
        ),
    ];

    for &(name, mutations, expected_error) in cases {
        let dir = tempfile::tempdir()?;
        let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, name, body)?;
        for &(allele_offset, encoded_value) in mutations {
            corrupt_bcf_gt_allele_value(&bcf_path, allele_offset, encoded_value)?;
        }

        let provider =
            VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
        let context = SessionContext::new();
        context.register_table("variants", Arc::new(provider))?;
        let error = context
            .sql("SELECT COUNT(*) FROM variants")
            .await?
            .collect()
            .await
            .expect_err("an invalid unprojected GT allele encoding must fail the scan")
            .to_string();
        assert!(
            error.contains(expected_error),
            "unexpected {name} error: {error}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn bcf_validates_unprojected_info_and_format_descriptor_types()
-> Result<(), Box<dyn std::error::Error>> {
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Read depth\">\n\
                ##FORMAT=<ID=GQ,Number=1,Type=Integer,Description=\"Genotype quality\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\tDP=5\tGQ\t7\n";

    for (target, expected_field) in [("info", "INFO field 'DP'"), ("format", "FORMAT field 'GQ'")] {
        let dir = tempfile::tempdir()?;
        let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, target, body)?;
        corrupt_bcf_first_value_descriptor_type(&bcf_path, target)?;

        let provider =
            VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
        let context = SessionContext::new();
        context.register_table("variants", Arc::new(provider))?;
        let error = context
            .sql("SELECT COUNT(*) FROM variants")
            .await?
            .collect()
            .await
            .expect_err("an unprojected descriptor type mismatch must fail the scan")
            .to_string();
        assert!(
            error.contains(expected_field) && error.contains("incompatible String encoding"),
            "unexpected {target} error: {error}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn bcf_rejects_duplicate_non_gt_format_fields_when_unprojected()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=DP,Number=1,Type=Integer,Description=\"Read depth\">\n\
                ##FORMAT=<ID=GQ,Number=1,Type=Integer,Description=\"Genotype quality\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tDP:GQ\t7:9\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "duplicate-format", body)?;

    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let samples_start = record_start + 8 + shared_len;
    assert_eq!(
        decompressed[samples_start], 0x11,
        "DP key should use a scalar i8 dictionary index"
    );
    let dp_key_index = decompressed[samples_start + 1];
    let dp_value_start = bcf_typed_value_end(&decompressed, samples_start);
    let gq_key_start = bcf_typed_value_end(&decompressed, dp_value_start);
    assert_eq!(
        decompressed[gq_key_start], 0x11,
        "GQ key should use a scalar i8 dictionary index"
    );
    decompressed[gq_key_start + 1] = dp_key_index;

    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("a duplicate unprojected non-GT FORMAT field must fail the scan")
        .to_string();
    assert!(
        error.contains("BCF record contains duplicate FORMAT field 'DP'"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_excess_values_for_unprojected_scalar_info_field()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=DP,Number=2,Type=Integer,Description=\"Read depth\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\tDP=5,7\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "scalar-info-extra", body)?;

    // Keep the two-integer record payload intact and corrupt only the
    // same-width header declaration from an array into a scalar.
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let declaration = b"##INFO=<ID=DP,Number=2";
    let declaration_start = decompressed
        .windows(declaration.len())
        .position(|window| window == declaration)
        .expect("DP INFO declaration should be present in the BCF header");
    decompressed[declaration_start + declaration.len() - 1] = b'1';
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let error = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await
        .expect_err("excess values for an unprojected scalar INFO field must fail the scan")
        .to_string();
    assert!(
        error.contains("INFO field 'DP' is declared scalar") && error.contains("encodes 2 values"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_accepts_missing_unprojected_scalar_info_field()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Read depth\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\tDP=5\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "scalar-info-missing", body)?;

    // noodles cannot currently write an explicit null INFO value, so rewrite
    // the valid scalar payload from [Int8(5)] to the BCF null descriptor and
    // shrink the shared record section by the removed payload byte.
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let header_len = u32::from_le_bytes(decompressed[5..9].try_into()?) as usize;
    let record_start = 9 + header_len;
    let shared_len =
        u32::from_le_bytes(decompressed[record_start..record_start + 4].try_into()?) as usize;
    let site_start = record_start + 8;
    let allele_count =
        u16::from_le_bytes(decompressed[site_start + 18..site_start + 20].try_into()?) as usize;
    let mut cursor = site_start + 24;
    cursor = bcf_typed_value_end(&decompressed, cursor); // IDs
    for _ in 0..allele_count {
        cursor = bcf_typed_value_end(&decompressed, cursor); // REF and ALTs
    }
    cursor = bcf_typed_value_end(&decompressed, cursor); // FILTER
    cursor = bcf_typed_value_end(&decompressed, cursor); // INFO key
    assert_eq!(decompressed[cursor], 0x11, "DP should be a scalar i8");
    decompressed[cursor] = 0x00;
    decompressed.remove(cursor + 1);
    decompressed[record_start..record_start + 4]
        .copy_from_slice(&u32::try_from(shared_len - 1)?.to_le_bytes());
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new(bcf_path, Some(Vec::new()), Some(Vec::new()), None, true)?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;
    let batches = context
        .sql("SELECT COUNT(*) FROM variants")
        .await?
        .collect()
        .await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    Ok(())
}

#[tokio::test]
async fn bcf_accepts_missing_sample_in_fixed_format_array() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=AD,Number=2,Type=Integer,Description=\"Allelic depths\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tAD\t.\t5,7\n\
                chr1\t20\trs2\tG\tT\t50\tPASS\t.\tAD\t.,.\t8,9\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "missing-format-sample", body)?;

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
        .sql("SELECT genotypes FROM variants ORDER BY start")
        .await?
        .collect()
        .await?;
    let genotypes = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes should be a struct");
    let ad_rows = genotypes
        .column_by_name("AD")
        .expect("AD should be projected")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("AD should contain one list of samples per row");
    let samples = ad_rows.value(0);
    let samples = samples
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("each AD sample should be a nullable integer list");
    assert!(
        datafusion::arrow::array::Array::is_null(samples, 0),
        "a wholly missing sample must remain an outer null"
    );
    let s2 = samples.value(1);
    let s2 = s2
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("present AD values should be Int32");
    assert_eq!(s2.values(), &[5, 7]);

    let samples = ad_rows.value(1);
    let samples = samples
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("each AD sample should be a nullable integer list");
    assert!(
        !datafusion::arrow::array::Array::is_null(samples, 0),
        "an explicit two-element missing array must remain present"
    );
    let s1 = samples.value(0);
    let s1 = s1
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("explicit AD elements should be Int32");
    assert_eq!(s1.len(), 2);
    assert!(datafusion::arrow::array::Array::is_null(s1, 0));
    assert!(datafusion::arrow::array::Array::is_null(s1, 1));
    let s2 = samples.value(1);
    let s2 = s2
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("present AD values should be Int32");
    assert_eq!(s2.values(), &[8, 9]);
    Ok(())
}

#[tokio::test]
async fn bcf_rejects_excess_values_for_scalar_format_field()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let body = "##fileformat=VCFv4.3\n\
                ##contig=<ID=chr1,length=1000>\n\
                ##FORMAT=<ID=DP,Number=2,Type=Integer,Description=\"Read depth\">\n\
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                chr1\t10\trs1\tA\tC\t50\tPASS\t.\tDP\t5,7\n";
    let (_vcf_path, bcf_path) = write_format_array_bcf(&dir, "scalar-extra", body)?;

    // Write a valid two-value array first, then corrupt only the same-width
    // header declaration. The record remains encoded with two integers while
    // the provider builds a scalar DP column from Number=1.
    let mut decompressed = Vec::new();
    noodles_bgzf::io::Reader::new(File::open(&bcf_path)?).read_to_end(&mut decompressed)?;
    let declaration = b"##FORMAT=<ID=DP,Number=2";
    let declaration_start = decompressed
        .windows(declaration.len())
        .position(|window| window == declaration)
        .expect("DP FORMAT declaration should be present in the BCF header");
    decompressed[declaration_start + declaration.len() - 1] = b'1';
    let mut writer = noodles_bgzf::io::Writer::new(File::create(&bcf_path)?);
    writer.write_all(&decompressed)?;
    writer.try_finish()?;

    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_path,
        Some(Vec::new()),
        Some(vec!["DP".to_string()]),
        None,
        None,
        true,
        VcfInputFormat::Bcf,
        None,
    )?;
    let context = SessionContext::new();
    context.register_table("variants", Arc::new(provider))?;

    let error = context
        .sql("SELECT \"DP\" FROM variants")
        .await?
        .collect()
        .await
        .expect_err("excess values for a scalar FORMAT field must fail the scan")
        .to_string();
    assert!(
        error.contains("FORMAT field 'DP' is declared scalar")
            && error.contains("encodes 2 values per sample"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn bcf_format_array_missing_elements_decode_as_nulls()
-> Result<(), Box<dyn std::error::Error>> {
    // Single-sample path: missing elements remain inner nulls, including when
    // every explicitly present array element is missing.
    let dir = tempfile::tempdir()?;
    let body = format!(
        "{FORMAT_ARRAY_HEADER}#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
         chr1\t10\trs1\tA\tC\t50\tPASS\t.\tGT:AD\t0/1:5,.\n\
         chr1\t20\trs2\tG\tT\t50\tPASS\t.\tGT:AD\t0/0:.,.\n"
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
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
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

    let values = list.value(1);
    let ints = values
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("AD elements should be Int32");
    assert_eq!(ints.len(), 2);
    assert!(datafusion::arrow::array::Array::is_null(ints, 0));
    assert!(datafusion::arrow::array::Array::is_null(ints, 1));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_scan_avoids_head_for_get_only_remote_bcf() -> Result<(), Box<dyn std::error::Error>>
{
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let index = bcf::fs::index(&bcf_path)?;
    let index_path = format!("{bcf_path}.csi");
    noodles_csi::fs::write(&index_path, &index)?;
    let server =
        RangeServer::start_denying_csi_head(std::fs::read(&bcf_path)?, std::fs::read(index_path)?);
    let bcf_url = server.url();
    let csi_url = format!("{bcf_url}.csi");

    let provider = VcfTableProvider::new_with_samples_and_format(
        bcf_url,
        Some(Vec::new()),
        Some(Vec::new()),
        None,
        None,
        true,
        VcfInputFormat::Auto,
        Some(csi_url),
    )?;
    server.requests.lock().unwrap().clear();
    server.deny_bcf_head();
    let rows = query(
        "variants",
        provider,
        "SELECT id FROM variants WHERE chrom = 'chr2'",
    )
    .await?;
    assert!(rows.contains("rs3"));
    assert!(!rows.contains("rs1"));
    let requests = server.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .filter(|request| request.path.starts_with("/remote.bcf"))
            .all(|request| request.method != "HEAD"),
        "GET-only signed URLs must not be probed with HEAD: {requests:?}"
    );
    assert!(
        requests.iter().any(|request| {
            request.path.starts_with("/remote.bcf")
                && !request.path.starts_with("/remote.bcf.csi")
                && request.range.is_some()
        }),
        "the query should use indexed BCF range GETs: {requests:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_bcf_url_fragment_is_auto_detected() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, _vcf_path, bcf_path) = create_equivalent_vcf_and_bcf()?;
    let index = bcf::fs::index(&bcf_path)?;
    let index_path = format!("{bcf_path}.csi");
    noodles_csi::fs::write(&index_path, &index)?;
    let server = RangeServer::start(std::fs::read(&bcf_path)?, std::fs::read(index_path)?);

    let provider = VcfTableProvider::new(
        format!("{}#download", server.url()),
        Some(Vec::new()),
        Some(Vec::new()),
        None,
        true,
    )?;
    let rows = query("variants", provider, "SELECT id FROM variants").await?;
    assert!(rows.contains("rs1"));
    assert!(rows.contains("rs3"));
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
