use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Array, ListArray, StringArray, StructArray, UInt8Array};
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::{TableProviderFilterPushDown, col, lit};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_core::genotype::{GenotypeMetric, MissingSamplePolicy};
use datafusion_bio_format_core::metadata::{
    GENOTYPE_COUNTED_ALLELE_KEY, GENOTYPE_SAMPLE_NAMES_KEY,
};
use datafusion_bio_format_plink1::{
    PLINK_SAMPLE_IDENTITIES_KEY, PlinkExec, PlinkReadOptions, PlinkTableProvider, SampleIdMode,
};
use tempfile::TempDir;

const FAM: &str = "\
f1 s1 0 0 1 -9
f1 s2 0 0 2 -9
f2 s3 0 0 1 -9
f:2 s%4 0 0 2 -9
f3 s5 0 0 1 -9
";

const BIM: &str = "\
1 rs1 0 10 A C
1 rs2 0.25 20 G T
2 rs3 1.5 30 C A
2 . 2 40 T G
";

// Source-order A1 dosages:
// rs1: 2, 1, 0, missing, 2
// rs2: 0, missing, 1, 2, 0
// rs3: 1, 1, 2, 0, missing
// row4: missing, 0, 0, 1, 1
const CODES: [[u8; 5]; 4] = [
    [0b00, 0b10, 0b11, 0b01, 0b00],
    [0b11, 0b01, 0b10, 0b00, 0b11],
    [0b10, 0b10, 0b00, 0b11, 0b01],
    [0b01, 0b11, 0b11, 0b10, 0b10],
];

struct Fixture {
    _dir: TempDir,
    bed: PathBuf,
    bim: PathBuf,
    fam: PathBuf,
}

#[derive(Clone, Debug)]
struct HttpRequest {
    path: String,
    method: String,
    range: Option<(usize, usize)>,
}

struct RangeServer {
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl RangeServer {
    fn start(bed: Vec<u8>, bim: Vec<u8>, fam: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let server_requests = requests.clone();
        let server_stop = stop.clone();
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
                let mut request_bytes = [0_u8; 8192];
                let size = stream.read(&mut request_bytes).unwrap();
                let request = String::from_utf8_lossy(&request_bytes[..size]);
                let mut lines = request.lines();
                let mut request_line = lines.next().unwrap().split_whitespace();
                let method = request_line.next().unwrap().to_string();
                let path = request_line.next().unwrap().to_string();
                let range = lines.find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if !name.eq_ignore_ascii_case("range") {
                        return None;
                    }
                    let value = value.trim().strip_prefix("bytes=")?;
                    let (start, end) = value.split_once('-')?;
                    Some((start.parse().ok()?, end.parse().ok()?))
                });
                server_requests.lock().unwrap().push(HttpRequest {
                    path: path.clone(),
                    method: method.clone(),
                    range,
                });

                let body = match path.as_str() {
                    "/cohort.bed" => &bed,
                    "/cohort.bim" => &bim,
                    "/cohort.fam" => &fam,
                    _ => {
                        let _ = write!(
                            stream,
                            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                        continue;
                    }
                };
                if method == "HEAD" {
                    let _ = write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                } else if let Some((start, end)) = range {
                    let end = end.min(body.len() - 1);
                    let selected = &body[start..=end];
                    let _ = write!(
                        stream,
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        selected.len(),
                        start,
                        end,
                        body.len()
                    );
                    let _ = stream.write_all(selected);
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

    fn url(&self, name: &str) -> String {
        format!("http://{}/{name}", self.address)
    }

    fn bed_get_ranges(&self) -> Vec<Option<(usize, usize)>> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.path == "/cohort.bed" && request.method == "GET")
            .map(|request| request.range)
            .collect()
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

fn fixture() -> Fixture {
    let dir = TempDir::new().unwrap();
    let bed = dir.path().join("cohort.bed");
    let bim = dir.path().join("cohort.bim");
    let fam = dir.path().join("cohort.fam");
    fs::write(&bim, BIM).unwrap();
    fs::write(&fam, FAM).unwrap();
    fs::write(&bed, encode_bed(&CODES)).unwrap();
    Fixture {
        _dir: dir,
        bed,
        bim,
        fam,
    }
}

fn encode_bed<const V: usize, const S: usize>(codes: &[[u8; S]; V]) -> Vec<u8> {
    let mut bytes = vec![0x6c, 0x1b, 0x01];
    for row in codes {
        for chunk in row.chunks(4) {
            let mut byte = 0_u8;
            for (slot, code) in chunk.iter().enumerate() {
                byte |= code << (slot * 2);
            }
            bytes.push(byte);
        }
    }
    bytes
}

fn path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn one_partition_context() -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1))
}

fn gt_values(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column: usize,
) -> Vec<Option<u8>> {
    let genotypes = batch
        .column(column)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let gt = genotypes
        .column_by_name("GT")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = gt.value(0);
    let values = values.as_any().downcast_ref::<UInt8Array>().unwrap();
    (0..values.len())
        .map(|index| (!values.is_null(index)).then(|| values.value(index)))
        .collect()
}

#[tokio::test]
async fn resolves_conventional_fileset_and_decodes_reordered_samples() {
    let fixture = fixture();
    let options = PlinkReadOptions {
        samples: Some(vec!["s%4".to_string(), "s1".to_string(), "s3".to_string()]),
        ..Default::default()
    };
    let provider = PlinkTableProvider::try_new(path(&fixture.bed), options)
        .await
        .unwrap();
    assert_eq!(provider.bim_path(), path(&fixture.bim));
    assert_eq!(provider.fam_path(), path(&fixture.fam));
    assert_eq!(provider.sample_names(), &["s%4", "s1", "s3"]);

    let context = one_partition_context();
    context.register_table("p", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT id, genotypes FROM p WHERE id = 'rs2'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "rs2"
    );
    assert_eq!(gt_values(&batches[0], 1), vec![Some(2), Some(0), Some(1)]);
}

#[tokio::test]
async fn exposes_coordinate_allele_and_sample_metadata() {
    let fixture = fixture();
    let provider = PlinkTableProvider::try_new(path(&fixture.bed), PlinkReadOptions::default())
        .await
        .unwrap();
    let schema = provider.schema();
    assert_eq!(
        schema
            .metadata()
            .get("bio.coordinate_system_zero_based")
            .map(String::as_str),
        Some("true")
    );
    let genotype_field = schema.field_with_name("genotypes").unwrap();
    assert_eq!(
        genotype_field
            .metadata()
            .get(GENOTYPE_SAMPLE_NAMES_KEY)
            .map(String::as_str),
        Some(r#"["s1","s2","s3","s%4","s5"]"#)
    );
    assert!(
        genotype_field
            .metadata()
            .contains_key(PLINK_SAMPLE_IDENTITIES_KEY)
    );
    let datafusion::arrow::datatypes::DataType::Struct(children) = genotype_field.data_type()
    else {
        panic!("genotypes must be a struct");
    };
    assert_eq!(
        children[0]
            .metadata()
            .get(GENOTYPE_COUNTED_ALLELE_KEY)
            .map(String::as_str),
        Some("A1")
    );

    let context = one_partition_context();
    context.register_table("p", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT start, \"end\", a1, a2 FROM p WHERE id = 'rs1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let start = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .unwrap();
    let end = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .unwrap();
    assert_eq!((start.value(0), end.value(0)), (9, 10));
}

#[tokio::test]
async fn reports_exact_catalog_pushdown_and_reads_only_selected_payload() {
    let fixture = fixture();
    let provider = PlinkTableProvider::try_new(path(&fixture.bed), PlinkReadOptions::default())
        .await
        .unwrap();
    let filter = col("id").eq(lit("rs3"));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter]).unwrap(),
        vec![TableProviderFilterPushDown::Exact]
    );
    assert_eq!(
        provider
            .supports_filters_pushdown(&[&col("a1").eq(lit("A"))])
            .unwrap(),
        vec![TableProviderFilterPushDown::Unsupported]
    );

    let context = one_partition_context();
    let state = context.state();
    let plan = provider
        .scan(&state, Some(&vec![0, 7]), &[filter], None)
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<PlinkExec>().unwrap();
    let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let metrics = exec.metrics_snapshot();
    let value = |metric| metrics[metric as usize].1;
    assert_eq!(value(GenotypeMetric::PrimaryBytesRead), 2);
    assert_eq!(value(GenotypeMetric::RangeRequests), 1);
    assert_eq!(value(GenotypeMetric::SamplesDecoded), 5);
    assert_eq!(value(GenotypeMetric::SelectedVariants), 1);
}

#[tokio::test]
async fn exact_filtered_limit_restricts_payload_planning() {
    let fixture = fixture();
    let provider = PlinkTableProvider::try_new(path(&fixture.bed), PlinkReadOptions::default())
        .await
        .unwrap();
    let context = one_partition_context();
    let state = context.state();
    let plan = provider
        .scan(
            &state,
            Some(&vec![3, 7]),
            &[col("chrom").eq(lit("1"))],
            Some(1),
        )
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<PlinkExec>().unwrap();
    let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "rs1"
    );
    let metrics = exec.metrics_snapshot();
    assert_eq!(metrics[GenotypeMetric::SelectedVariants as usize].1, 1);
    assert_eq!(metrics[GenotypeMetric::PrimaryBytesRead as usize].1, 2);
}

#[tokio::test]
async fn metadata_only_and_count_scans_do_not_read_bed_payload() {
    let fixture = fixture();
    let provider = PlinkTableProvider::try_new(path(&fixture.bed), PlinkReadOptions::default())
        .await
        .unwrap();
    let context = one_partition_context();
    let state = context.state();
    for projection in [vec![0, 3], Vec::new()] {
        let plan = provider
            .scan(&state, Some(&projection), &[], None)
            .await
            .unwrap();
        let exec = plan.as_any().downcast_ref::<PlinkExec>().unwrap();
        let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            4
        );
        let metrics = exec.metrics_snapshot();
        assert_eq!(
            metrics[GenotypeMetric::PrimaryBytesRead as usize].1,
            0,
            "projection {projection:?}"
        );
        assert_eq!(metrics[GenotypeMetric::PayloadsSkipped as usize].1, 4);
    }
}

#[tokio::test]
async fn empty_sample_selection_emits_empty_lists_without_payload_reads() {
    let fixture = fixture();
    let provider = PlinkTableProvider::try_new(
        path(&fixture.bed),
        PlinkReadOptions {
            samples: Some(vec!["absent".to_string()]),
            missing_sample_policy: MissingSamplePolicy::Ignore,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = one_partition_context();
    let state = context.state();
    let plan = provider
        .scan(&state, Some(&vec![7]), &[], Some(1))
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<PlinkExec>().unwrap();
    let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
    assert_eq!(gt_values(&batches[0], 0), Vec::<Option<u8>>::new());
    assert_eq!(
        exec.metrics_snapshot()[GenotypeMetric::PrimaryBytesRead as usize].1,
        0
    );
}

#[tokio::test]
async fn remote_metadata_and_sparse_queries_use_bounded_bed_ranges() {
    let server = RangeServer::start(
        encode_bed(&CODES),
        BIM.as_bytes().to_vec(),
        FAM.as_bytes().to_vec(),
    );
    let provider =
        PlinkTableProvider::try_new(server.url("cohort.bed"), PlinkReadOptions::default())
            .await
            .unwrap();
    assert_eq!(server.bed_get_ranges(), vec![Some((0, 2))]);

    let context = one_partition_context();
    context
        .register_table("remote_plink", Arc::new(provider))
        .unwrap();
    let metadata = context
        .sql("SELECT id FROM remote_plink WHERE chrom = '2'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        metadata.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    assert_eq!(server.bed_get_ranges(), vec![Some((0, 2))]);

    let genotype = context
        .sql("SELECT genotypes FROM remote_plink WHERE id = 'rs3'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        gt_values(&genotype[0], 0),
        vec![Some(1), Some(1), Some(2), Some(0), None]
    );
    assert_eq!(server.bed_get_ranges(), vec![Some((0, 2)), Some((7, 8))]);
}

#[tokio::test]
async fn explicit_companions_override_basename_and_fid_iid_is_escaped() {
    let fixture = fixture();
    let provider = PlinkTableProvider::try_new(
        path(&fixture.bed),
        PlinkReadOptions {
            bim_path: Some(path(&fixture.bim)),
            fam_path: Some(path(&fixture.fam)),
            sample_id_mode: SampleIdMode::FidIid,
            samples: Some(vec!["f%3A2:s%254".to_string()]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(provider.sample_names(), &["f%3A2:s%254"]);
}

#[tokio::test]
async fn rejects_fileset_integrity_and_layout_errors_during_open() {
    for (header, length_delta, expected) in [
        ([0x6c, 0x1b, 0x00], 0_isize, "sample-major"),
        ([0x00, 0x00, 0x00], 0, "invalid or legacy magic"),
        ([0x6c, 0x1b, 0x01], -1, "length mismatch"),
        ([0x6c, 0x1b, 0x01], 1, "length mismatch"),
    ] {
        let fixture = fixture();
        let mut bytes = fs::read(&fixture.bed).unwrap();
        bytes[..3].copy_from_slice(&header);
        if length_delta < 0 {
            bytes.pop();
        } else if length_delta > 0 {
            bytes.push(0);
        }
        fs::write(&fixture.bed, bytes).unwrap();
        let error = PlinkTableProvider::try_new(path(&fixture.bed), Default::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
    }
}

#[tokio::test]
async fn missing_companions_fail_with_the_fileset_role() {
    let dir = TempDir::new().unwrap();
    let bed = dir.path().join("missing.bed");
    fs::write(&bed, [0x6c, 0x1b, 0x01]).unwrap();
    let error = PlinkTableProvider::try_new(path(&bed), Default::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("BIM"), "{error}");

    let explicit = dir.path().join("not-there.bim");
    let error = PlinkTableProvider::try_new(
        path(&bed),
        PlinkReadOptions {
            bim_path: Some(path(&explicit)),
            ..Default::default()
        },
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("explicit BIM companion"), "{error}");
}

#[tokio::test]
async fn rejects_malformed_companions_duplicate_iids_limits_and_padding() {
    let malformed_bim = fixture();
    fs::write(&malformed_bim.bim, "1 rs1 nan 10 A C\n").unwrap();
    let error = PlinkTableProvider::try_new(path(&malformed_bim.bed), Default::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("centimorgan value must be finite"));

    let duplicate_fam = fixture();
    fs::write(&duplicate_fam.fam, "f1 s1 0 0 1 -9\nf2 s1 0 0 2 -9\n").unwrap();
    fs::write(
        &duplicate_fam.bed,
        encode_bed(&[[0_u8, 0_u8], [0, 0], [0, 0], [0, 0]]),
    )
    .unwrap();
    let error = PlinkTableProvider::try_new(path(&duplicate_fam.bed), Default::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("duplicate IID"));

    let limited = fixture();
    let error = PlinkTableProvider::try_new(
        path(&limited.bed),
        PlinkReadOptions {
            max_variants: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("max_variants"));

    let bad_padding = fixture();
    let mut bytes = fs::read(&bad_padding.bed).unwrap();
    bytes[4] |= 0b01_00_00_00;
    fs::write(&bad_padding.bed, bytes).unwrap();
    let provider = PlinkTableProvider::try_new(path(&bad_padding.bed), PlinkReadOptions::default())
        .await
        .unwrap();
    let context = one_partition_context();
    context.register_table("p", Arc::new(provider)).unwrap();
    let error = context
        .sql("SELECT genotypes FROM p WHERE id = 'rs1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("variant 0"));
    assert!(error.contains("padding"));
}

#[tokio::test]
async fn differential_against_bed_reader_when_available() {
    let fixture = fixture();
    let available = Command::new("python3")
        .args(["-c", "import bed_reader"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if !available {
        assert!(
            std::env::var_os("REQUIRE_PLINK_ORACLE").is_none(),
            "bed-reader is required when REQUIRE_PLINK_ORACLE is set"
        );
        return;
    }

    let script = r#"
import numpy as np
from bed_reader import open_bed
values = open_bed(__import__("sys").argv[1]).read(dtype="float64")
expected = np.array([
    [2, 1, 0, np.nan, 2],
    [0, np.nan, 1, 2, 0],
    [1, 1, 2, 0, np.nan],
    [np.nan, 0, 0, 1, 1],
], dtype="float64")
assert values.shape == expected.shape, (values.shape, expected.shape)
assert np.array_equal(np.isnan(values), np.isnan(expected))
assert np.array_equal(np.nan_to_num(values), np.nan_to_num(expected))
"#;
    let status = Command::new("python3")
        .args(["-c", script, &path(&fixture.bed)])
        .status()
        .unwrap();
    assert!(status.success());

    let provider = PlinkTableProvider::try_new(path(&fixture.bed), PlinkReadOptions::default())
        .await
        .unwrap();
    let context = one_partition_context();
    context.register_table("p", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM p WHERE id = 'rs1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        gt_values(&batches[0], 0),
        vec![Some(2), Some(1), Some(0), None, Some(2)]
    );
}

#[test]
fn differential_against_plink_when_available() {
    let available = Command::new("plink")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if !available {
        assert!(
            std::env::var_os("REQUIRE_PLINK_CLI_ORACLE").is_none(),
            "PLINK is required when REQUIRE_PLINK_CLI_ORACLE is set"
        );
        return;
    }

    let fixture = fixture();
    let prefix = fixture.bed.with_extension("");
    let output = fixture._dir.path().join("plink-oracle");
    let status = Command::new("plink")
        .args([
            "--bfile",
            &path(&prefix),
            "--keep-allele-order",
            "--recode",
            "A",
            "--out",
            &path(&output),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());

    let raw = fs::read_to_string(output.with_extension("raw")).unwrap();
    let mut lines = raw.lines();
    let header: Vec<_> = lines.next().unwrap().split_whitespace().collect();
    let rs1 = header
        .iter()
        .position(|name| name.starts_with("rs1_"))
        .unwrap();
    let rs2 = header
        .iter()
        .position(|name| name.starts_with("rs2_"))
        .unwrap();
    let rs3 = header
        .iter()
        .position(|name| name.starts_with("rs3_"))
        .unwrap();
    let rows: Vec<Vec<_>> = lines
        .map(|line| line.split_whitespace().collect())
        .collect();
    let value = |row: usize, column: usize| match rows[row][column] {
        "NA" => None,
        value => Some(value.parse::<u8>().unwrap()),
    };
    assert_eq!(
        (0..5).map(|row| value(row, rs1)).collect::<Vec<_>>(),
        vec![Some(2), Some(1), Some(0), None, Some(2)]
    );
    assert_eq!(
        (0..5).map(|row| value(row, rs2)).collect::<Vec<_>>(),
        vec![Some(0), None, Some(1), Some(2), Some(0)]
    );
    assert_eq!(
        (0..5).map(|row| value(row, rs3)).collect::<Vec<_>>(),
        vec![Some(1), Some(1), Some(2), Some(0), None]
    );
}
