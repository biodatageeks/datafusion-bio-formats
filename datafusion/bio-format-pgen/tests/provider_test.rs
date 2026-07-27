use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, ListArray, StringArray, StructArray, UInt16Array,
};
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::{TableProviderFilterPushDown, col, lit};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_core::genotype::GenotypeMetric;
use datafusion_bio_format_pgen::{PgenExec, PgenReadOptions, PgenTableProvider};
use tempfile::TempDir;

struct Fixture {
    _temp: TempDir,
    pgen: PathBuf,
    pvar: PathBuf,
    psam: PathBuf,
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
    fn start(pgen: Vec<u8>, pvar: Vec<u8>, psam: Vec<u8>) -> Self {
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
                    "/cohort.pgen" => &pgen,
                    "/cohort.pvar" => &pvar,
                    "/cohort.psam" => &psam,
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

    fn get_requests(&self, name: &str) -> Vec<HttpRequest> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.path == format!("/{name}") && request.method == "GET")
            .cloned()
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

fn path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn context(partitions: usize) -> SessionContext {
    SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(1024)
            .with_target_partitions(partitions),
    )
}

fn pack_codes(codes: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0_u8; codes.len().div_ceil(4)];
    for (index, code) in codes.iter().copied().enumerate() {
        bytes[index / 4] |= code << ((index % 4) * 2);
    }
    bytes
}

fn write_metadata(root: &Path, allele_counts: &[usize], samples: usize) -> (PathBuf, PathBuf) {
    let pvar = root.join("cohort.pvar");
    let psam = root.join("cohort.psam");
    let mut pvar_text = "#CHROM\tPOS\tID\tREF\tALT\n".to_string();
    for (index, &allele_count) in allele_counts.iter().enumerate() {
        let alternate = (1..allele_count)
            .map(|allele| format!("A{allele}"))
            .collect::<Vec<_>>()
            .join(",");
        pvar_text.push_str(&format!(
            "{}\t{}\tv{}\tR\t{}\n",
            if index + 1 == allele_counts.len() {
                "2"
            } else {
                "1"
            },
            (index + 1) * 10,
            index + 1,
            alternate
        ));
    }
    fs::write(&pvar, pvar_text).unwrap();
    let mut psam_text = "#FID\tIID\tSID\n".to_string();
    for sample in 0..samples {
        psam_text.push_str(&format!(
            "f{}\ts{}\tsrc{}\n",
            sample % 2,
            sample + 1,
            sample
        ));
    }
    fs::write(&psam, psam_text).unwrap();
    (pvar, psam)
}

fn variable_fixture(
    mode: u8,
    record_types: &[u8],
    records: &[Vec<u8>],
    allele_counts: &[usize],
) -> Fixture {
    assert_eq!(record_types.len(), records.len());
    assert_eq!(records.len(), allele_counts.len());
    assert!(records.iter().all(|record| record.len() < 256));
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let pgen = root.join("cohort.pgen");
    let pgi = matches!(mode, 0x20 | 0x21).then(|| root.join("cohort.pgen.pgi"));
    let (pvar, psam) = write_metadata(root, allele_counts, 4);
    let extension = if matches!(mode, 0x11 | 0x21) {
        vec![0, 0]
    } else {
        Vec::new()
    };
    let header_len =
        12 + 8 + record_types.len() + records.len() + allele_counts.len() + extension.len();
    let block_offset = if pgi.is_some() {
        3_u64
    } else {
        header_len as u64
    };
    let index_magic = match mode {
        0x20 => 0x30,
        0x21 => 0x31,
        _ => mode,
    };
    let mut header = vec![0x6c, 0x1b, index_magic];
    header.extend((records.len() as u32).to_le_bytes());
    header.extend(4_u32.to_le_bytes());
    header.push(0x54);
    header.extend(block_offset.to_le_bytes());
    header.extend(record_types);
    header.extend(records.iter().map(|record| record.len() as u8));
    header.extend(allele_counts.iter().map(|&count| count as u8));
    header.extend(extension);

    if let Some(pgi) = &pgi {
        fs::write(pgi, header).unwrap();
        let mut primary = vec![0x6c, 0x1b, mode];
        for record in records {
            primary.extend(record);
        }
        fs::write(&pgen, primary).unwrap();
    } else {
        for record in records {
            header.extend(record);
        }
        fs::write(&pgen, header).unwrap();
    }
    Fixture {
        _temp: temp,
        pgen,
        pvar,
        psam,
    }
}

fn fixed_fixture(mode: u8) -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let pgen = root.join("cohort.pgen");
    let (pvar, psam) = write_metadata(root, &[2, 2], 4);
    let mut bytes = vec![0x6c, 0x1b, mode];
    bytes.extend(2_u32.to_le_bytes());
    bytes.extend(4_u32.to_le_bytes());
    bytes.push(0x40);
    for codes in [[0, 1, 2, 3], [2, 1, 0, 3]] {
        bytes.extend(pack_codes(&codes));
        if mode >= 3 {
            for code in codes {
                let value = match code {
                    0 => 0_u16,
                    1 => 16_384,
                    2 => 32_768,
                    3 => u16::MAX,
                    _ => unreachable!(),
                };
                bytes.extend(value.to_le_bytes());
            }
        }
        if mode == 4 {
            for value in [0_i16, 0, 0, i16::MIN] {
                bytes.extend(value.to_le_bytes());
            }
        }
    }
    fs::write(&pgen, bytes).unwrap();
    Fixture {
        _temp: temp,
        pgen,
        pvar,
        psam,
    }
}

fn plink1_mode_fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let pgen = root.join("cohort.pgen");
    let (pvar, psam) = write_metadata(root, &[2], 4);
    let mut bytes = vec![0x6c, 0x1b, 0x01];
    bytes.extend(pack_codes(&[3, 2, 0, 1]));
    fs::write(&pgen, bytes).unwrap();
    Fixture {
        _temp: temp,
        pgen,
        pvar,
        psam,
    }
}

fn variable_records() -> (Vec<u8>, Vec<Vec<u8>>, Vec<usize>) {
    let mut types = Vec::new();
    let mut records = Vec::new();
    let mut alleles = Vec::new();

    types.push(0x00);
    records.push(pack_codes(&[0, 1, 2, 3]));
    alleles.push(2);

    types.push(0x01);
    records.push(vec![2, 0b0000_1010, 1, 2, 1]);
    alleles.push(2);

    types.push(0x04);
    records.push(vec![1, 2, 2]);
    alleles.push(2);

    types.push(0x02);
    records.push(vec![1, 1, 3]);
    alleles.push(2);

    types.push(0x10);
    records.push(vec![0b11_01_00_01, 0b0000_0100]);
    alleles.push(2);

    types.push(0x60);
    records.push(vec![0b11_10_01_00, 0b0000_0101, 0, 0, 0, 128]);
    alleles.push(2);

    types.push(0xe0);
    records.push(vec![0xff, 0b0000_0001, 0, 64, 0b0000_0001, 0, 0]);
    alleles.push(2);

    types.push(0x08);
    records.push(vec![0b11_00_10_01, 0x00, 0x01, 0x01, 0x00]);
    alleles.push(3);

    types.push(0x10);
    records.push(vec![0b11_00_01_01, 0b0000_0011, 0b0000_0001]);
    alleles.push(2);

    (types, records, alleles)
}

fn genotype_struct(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column: usize,
) -> &StructArray {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
}

fn gt_values(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column: usize,
    row: usize,
) -> Vec<Option<Vec<u16>>> {
    let gt = genotype_struct(batch, column)
        .column_by_name("GT")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let samples = gt.value(row);
    let samples = samples.as_any().downcast_ref::<ListArray>().unwrap();
    (0..samples.len())
        .map(|sample| {
            if samples.is_null(sample) {
                return None;
            }
            let values = samples.value(sample);
            let values = values.as_any().downcast_ref::<UInt16Array>().unwrap();
            Some(values.values().to_vec())
        })
        .collect()
}

fn phased_values(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column: usize,
    row: usize,
) -> Vec<Option<bool>> {
    let values = genotype_struct(batch, column)
        .column_by_name("PHASED")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(row);
    let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
    (0..values.len())
        .map(|index| (!values.is_null(index)).then(|| values.value(index)))
        .collect()
}

fn ds_values(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column: usize,
    row: usize,
) -> Vec<Option<f32>> {
    let values = genotype_struct(batch, column)
        .column_by_name("DS")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(row);
    let values = values.as_any().downcast_ref::<Float32Array>().unwrap();
    (0..values.len())
        .map(|index| (!values.is_null(index)).then(|| values.value(index)))
        .collect()
}

fn hds_values(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column: usize,
    row: usize,
) -> Vec<Option<Vec<f32>>> {
    let values = genotype_struct(batch, column)
        .column_by_name("HDS")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(row);
    let values = values.as_any().downcast_ref::<ListArray>().unwrap();
    (0..values.len())
        .map(|index| {
            if values.is_null(index) {
                return None;
            }
            let haplotypes = values.value(index);
            let haplotypes = haplotypes.as_any().downcast_ref::<Float32Array>().unwrap();
            Some(haplotypes.values().to_vec())
        })
        .collect()
}

#[tokio::test]
async fn decodes_all_fixed_width_and_plink1_modes() {
    for mode in [2_u8, 3, 4] {
        let fixture = fixed_fixture(mode);
        let provider = PgenTableProvider::try_new(
            path(&fixture.pgen),
            PgenReadOptions {
                samples: Some(vec!["s3".to_string(), "s1".to_string(), "s4".to_string()]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let context = context(1);
        context.register_table("p", Arc::new(provider)).unwrap();
        let batches = context
            .sql("SELECT id, genotypes FROM p ORDER BY start")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            gt_values(&batches[0], 1, 0),
            vec![Some(vec![1, 1]), Some(vec![0, 0]), None]
        );
        if mode >= 3 {
            assert_eq!(
                ds_values(&batches[0], 1, 0),
                vec![Some(2.0), Some(0.0), None]
            );
        } else {
            assert_eq!(ds_values(&batches[0], 1, 0), vec![None, None, None]);
        }
        if mode == 4 {
            assert_eq!(
                hds_values(&batches[0], 1, 0),
                vec![Some(vec![1.0, 1.0]), Some(vec![0.0, 0.0]), None]
            );
        }
    }

    let fixture = plink1_mode_fixture();
    let provider = PgenTableProvider::try_new(path(&fixture.pgen), Default::default())
        .await
        .unwrap();
    let context = context(1);
    context.register_table("p", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM p")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        gt_values(&batches[0], 0, 0),
        vec![Some(vec![0, 0]), Some(vec![0, 1]), Some(vec![1, 1]), None]
    );
}

#[tokio::test]
async fn decodes_variable_representations_indexes_phase_dosage_and_patches() {
    let (types, records, alleles) = variable_records();
    for mode in [0x10_u8, 0x11, 0x20, 0x21] {
        let fixture = variable_fixture(mode, &types, &records, &alleles);
        let provider = PgenTableProvider::try_new(path(&fixture.pgen), Default::default())
            .await
            .unwrap();
        assert_eq!(provider.pgi_path().is_some(), mode >= 0x20);
        let context = context(3);
        context.register_table("p", Arc::new(provider)).unwrap();
        let batches = context
            .sql("SELECT id, genotypes FROM p ORDER BY start")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            9
        );
        let batch = &batches[0];
        assert_eq!(
            gt_values(batch, 1, 1),
            vec![
                Some(vec![0, 0]),
                Some(vec![1, 1]),
                Some(vec![0, 1]),
                Some(vec![1, 1])
            ]
        );
        assert_eq!(
            gt_values(batch, 1, 3),
            vec![Some(vec![0, 0]), None, Some(vec![1, 1]), Some(vec![0, 0])]
        );
        assert_eq!(
            phased_values(batch, 1, 4),
            vec![Some(true), Some(false), Some(true), None]
        );
        assert_eq!(
            ds_values(batch, 1, 5),
            vec![Some(0.0), Some(1.0), Some(2.0), None]
        );
        assert_eq!(
            hds_values(batch, 1, 6),
            vec![Some(vec![0.5, 0.5]), None, None, None]
        );
        assert_eq!(
            gt_values(batch, 1, 7),
            vec![Some(vec![0, 2]), Some(vec![1, 2]), Some(vec![0, 0]), None]
        );
        assert_eq!(
            gt_values(batch, 1, 8),
            vec![Some(vec![1, 0]), Some(vec![0, 1]), Some(vec![0, 0]), None]
        );
        assert_eq!(
            phased_values(batch, 1, 8),
            vec![Some(true), Some(false), Some(false), None]
        );
    }
}

#[tokio::test]
async fn applies_exact_pvar_pushdowns_limits_and_metadata_projection() {
    let (types, records, alleles) = variable_records();
    let fixture = variable_fixture(0x10, &types, &records, &alleles);
    let provider = PgenTableProvider::try_new(path(&fixture.pgen), Default::default())
        .await
        .unwrap();
    assert_eq!(
        provider
            .supports_filters_pushdown(&[&col("chrom").eq(lit("1"))])
            .unwrap(),
        vec![TableProviderFilterPushDown::Exact]
    );
    let context = context(4);
    let state = context.state();
    let plan = provider
        .scan(
            &state,
            Some(&vec![3]),
            &[col("chrom").eq(lit("1"))],
            Some(2),
        )
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<PgenExec>().unwrap();
    let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    assert_eq!(
        exec.metrics_snapshot()[GenotypeMetric::PayloadsSkipped as usize].1,
        2
    );
    assert_eq!(
        exec.metrics_snapshot()[GenotypeMetric::RangeRequests as usize].1,
        0
    );

    let count_plan = provider
        .scan(
            &state,
            Some(&Vec::new()),
            &[col("id").in_list(vec![lit("v2"), lit("v4")], false)],
            None,
        )
        .await
        .unwrap();
    let count_exec = count_plan.as_any().downcast_ref::<PgenExec>().unwrap();
    let count_batches = collect(count_plan.clone(), context.task_ctx())
        .await
        .unwrap();
    assert_eq!(
        count_batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        2
    );
    assert_eq!(
        count_exec.metrics_snapshot()[GenotypeMetric::PayloadsSkipped as usize].1,
        2
    );
    assert_eq!(
        count_exec.metrics_snapshot()[GenotypeMetric::RangeRequests as usize].1,
        0
    );
}

#[tokio::test]
async fn honors_empty_sample_and_genotype_field_selection() {
    let fixture = fixed_fixture(3);
    let provider = PgenTableProvider::try_new(
        path(&fixture.pgen),
        PgenReadOptions {
            samples: Some(Vec::new()),
            genotype_fields: Some(vec!["DS".to_string()]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(2);
    let state = context.state();
    let plan = provider
        .scan(&state, Some(&vec![6]), &[], None)
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<PgenExec>().unwrap();
    let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
    assert_eq!(ds_values(&batches[0], 0, 0), Vec::<Option<f32>>::new());
    assert_eq!(
        exec.metrics_snapshot()[GenotypeMetric::RangeRequests as usize].1,
        0
    );
}

#[tokio::test]
async fn reads_zstd_pvar_and_rejects_hybrid_or_inconsistent_filesets() {
    let fixture = fixed_fixture(2);
    let compressed =
        zstd::stream::encode_all(fs::read(&fixture.pvar).unwrap().as_slice(), 1).unwrap();
    let pvar_zst = fixture.pvar.with_extension("pvar.zst");
    fs::write(&pvar_zst, compressed).unwrap();
    fs::remove_file(&fixture.pvar).unwrap();
    PgenTableProvider::try_new(path(&fixture.pgen), Default::default())
        .await
        .unwrap();

    let fixture = fixed_fixture(2);
    fs::write(&fixture.psam, "#IID\ns1\n").unwrap();
    let error = PgenTableProvider::try_new(path(&fixture.pgen), Default::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("sample count"), "{error}");

    let temp = tempfile::tempdir().unwrap();
    let pgen = temp.path().join("hybrid.pgen");
    fs::write(&pgen, [0x6c, 0x1b, 0x01]).unwrap();
    fs::write(temp.path().join("hybrid.bim"), b"1 v 0 1 A C\n").unwrap();
    fs::write(temp.path().join("hybrid.fam"), b"f s 0 0 0 -9\n").unwrap();
    let error = PgenTableProvider::try_new(path(&pgen), Default::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("hybrid"), "{error}");
}

#[tokio::test]
async fn rejects_malformed_indexes_records_and_unsupported_multiallelic_dosage() {
    let (types, records, alleles) = variable_records();
    let fixture = variable_fixture(0x10, &types, &records, &alleles);
    let mut bytes = fs::read(&fixture.pgen).unwrap();
    bytes[12..20].copy_from_slice(&u64::MAX.to_le_bytes());
    fs::write(&fixture.pgen, bytes).unwrap();
    assert!(
        PgenTableProvider::try_new(path(&fixture.pgen), Default::default())
            .await
            .is_err()
    );

    let fixture = variable_fixture(0x10, &[0x00], &[vec![0]], &[2]);
    let mut bytes = fs::read(&fixture.pgen).unwrap();
    bytes.push(0);
    fs::write(&fixture.pgen, bytes).unwrap();
    let error = PgenTableProvider::try_new(path(&fixture.pgen), Default::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("object ends"), "{error}");

    let fixture = variable_fixture(0x10, &[0x02], &[vec![0]], &[2]);
    let error = PgenTableProvider::try_new(path(&fixture.pgen), Default::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("has no base"), "{error}");

    let fixture = variable_fixture(0x10, &[0x01], &[vec![0; 5]], &[2]);
    let error = PgenTableProvider::try_new(
        path(&fixture.pgen),
        PgenReadOptions {
            max_record_bytes: 4,
            ..Default::default()
        },
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("max_record_bytes"), "{error}");

    let fixture = variable_fixture(0x10, &[0x04], &[vec![2, 0, 0, 0]], &[2]);
    let provider = PgenTableProvider::try_new(path(&fixture.pgen), Default::default())
        .await
        .unwrap();
    let first_context = context(1);
    first_context
        .register_table("p", Arc::new(provider))
        .unwrap();
    let error = first_context
        .sql("SELECT genotypes FROM p")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("delta is zero"), "{error}");

    let fixture = variable_fixture(
        0x10,
        &[0x68],
        &[vec![0, 0xff, 0, 0, 0, 0, 0, 0, 0, 0]],
        &[3],
    );
    let provider = PgenTableProvider::try_new(path(&fixture.pgen), Default::default())
        .await
        .unwrap();
    let context = context(1);
    context.register_table("p", Arc::new(provider)).unwrap();
    let error = context
        .sql("SELECT genotypes FROM p")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("unsupported multiallelic"), "{error}");
}

#[tokio::test]
async fn remote_sparse_scan_uses_bounded_pgen_ranges() {
    let (types, records, alleles) = variable_records();
    let fixture = variable_fixture(0x10, &types, &records, &alleles);
    let pgen = fs::read(&fixture.pgen).unwrap();
    let pgen_len = pgen.len();
    let server = RangeServer::start(
        pgen,
        fs::read(&fixture.pvar).unwrap(),
        fs::read(&fixture.psam).unwrap(),
    );
    let provider = PgenTableProvider::try_new(server.url("cohort.pgen"), Default::default())
        .await
        .unwrap();
    let context = context(4);
    let state = context.state();
    let plan = provider
        .scan(&state, Some(&vec![3, 6]), &[col("id").eq(lit("v4"))], None)
        .await
        .unwrap();
    let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let requests = server.get_requests("cohort.pgen");
    assert!(!requests.is_empty());
    assert!(
        requests.iter().all(|request| request.range.is_some()),
        "PGEN object access must remain range-based: {requests:?}"
    );
    assert!(
        requests.iter().all(|request| {
            request
                .range
                .is_some_and(|(start, end)| end - start + 1 < pgen_len)
        }),
        "a sparse query must not GET the complete PGEN object: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.range.is_some_and(|(start, _)| start > 0)),
        "the selected payload must be fetched independently of the header"
    );
    let requested_bytes = requests
        .iter()
        .map(|request| {
            request
                .range
                .map(|(start, end)| (end - start + 1) as u64)
                .unwrap_or(0)
        })
        .sum::<u64>();
    let exec = plan.as_any().downcast_ref::<PgenExec>().unwrap();
    assert_eq!(
        exec.metrics_snapshot()[GenotypeMetric::PrimaryBytesRead as usize].1,
        requested_bytes
    );
}

#[tokio::test]
async fn differential_pgenlib_and_snputils_oracles_when_installed() {
    let python = std::env::var("PGEN_REFERENCE_PYTHON").unwrap_or_else(|_| "python3".to_string());
    let pythonpath = std::env::var("PGEN_REFERENCE_PYTHONPATH").ok();
    let require = std::env::var_os("PGEN_REQUIRE_REFERENCE_ORACLES").is_some();
    let mut probe = Command::new(&python);
    probe.args(["-c", "import pgenlib, snputils"]);
    if let Some(path) = &pythonpath {
        probe.env("PYTHONPATH", path);
    }
    if !probe.status().is_ok_and(|status| status.success()) {
        assert!(
            !require,
            "required pgenlib/snputils reference oracles are unavailable"
        );
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let prefix = temp.path().join("oracle");
    let script = r#"
import pathlib, sys
import numpy as np
import pgenlib
from snputils import PGENReader
p = pathlib.Path(sys.argv[1])
rows = np.array([
  [0, 1, 2, -9, 0, 0, 0, 0],
  [0, 0, 0, 0, 0, 0, 2, 0],
  [0, 1, 2, -9, 0, 2, 0, 0],
], dtype=np.int8)
with pgenlib.PgenWriter(bytes(p.with_suffix('.pgen')), 8, 3, False) as writer:
    for row in rows:
        writer.append_biallelic(row)
p.with_suffix('.pvar').write_text('#CHROM\tPOS\tID\tREF\tALT\n1\t10\tv1\tA\tC\n1\t20\tv2\tG\tT\n2\t30\tv3\tC\tG\n')
p.with_suffix('.psam').write_text('#IID\n' + ''.join(f's{i+1}\n' for i in range(8)))
reader = pgenlib.PgenReader(bytes(p.with_suffix('.pgen')))
observed = np.empty_like(rows)
reader.read_range(0, 3, observed)
reader.close()
np.testing.assert_array_equal(observed, rows)
snp = PGENReader(p).read(genotype_mode='dosage')
expected = rows.T
if snp.genotypes.shape == rows.shape:
    expected = rows
np.testing.assert_array_equal(snp.genotypes, expected)

phase = p.parent / 'phase'
alleles = np.array([0, 1, 1, 0, 0, 0, -9, -9], dtype=np.int32)
phase_present = np.array([1, 1, 0, 0], dtype=np.uint8)
with pgenlib.PgenWriter(bytes(phase.with_suffix('.pgen')), 4, 1, False,
                        hardcall_phase_present=True) as writer:
    writer.append_partially_phased(alleles, phase_present)
phase.with_suffix('.pvar').write_text(
    '#CHROM\tPOS\tID\tREF\tALT\n1\t10\tphase1\tA\tC\n')
phase.with_suffix('.psam').write_text('#IID\ns1\ns2\ns3\ns4\n')
reader = pgenlib.PgenReader(bytes(phase.with_suffix('.pgen')))
observed_alleles = np.empty(8, dtype=np.int32)
observed_phase = np.empty(4, dtype=np.uint8)
reader.read_alleles_and_phasepresent(0, observed_alleles, observed_phase)
reader.close()
np.testing.assert_array_equal(observed_alleles, alleles)
np.testing.assert_array_equal(
    PGENReader(phase).read(genotype_mode='phased').genotypes,
    np.array([[[0, 1], [1, 0], [0, 0], [-9, -9]]], dtype=np.int8))

dosage = p.parent / 'dosage'
dosages = np.array([0.125, 1.0, 1.875, -9.0], dtype=np.float32)
with pgenlib.PgenWriter(bytes(dosage.with_suffix('.pgen')), 4, 1, False,
                        dosage_present=True) as writer:
    writer.append_dosages(dosages)
dosage.with_suffix('.pvar').write_text(
    '#CHROM\tPOS\tID\tREF\tALT\n1\t10\tdosage1\tA\tC\n')
dosage.with_suffix('.psam').write_text('#IID\ns1\ns2\ns3\ns4\n')
reader = pgenlib.PgenReader(bytes(dosage.with_suffix('.pgen')))
observed_dosages = np.empty(4, dtype=np.float64)
reader.read_dosages(0, observed_dosages)
reader.close()
np.testing.assert_allclose(observed_dosages, dosages)
"#;
    let mut command = Command::new(&python);
    command.args(["-c", script, prefix.to_str().unwrap()]);
    if let Some(path) = &pythonpath {
        command.env("PYTHONPATH", path);
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "reference fixture generation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let plink2 = std::env::var("PGEN_REFERENCE_PLINK2").unwrap_or_else(|_| "plink2".to_string());
    let require_plink2 = std::env::var_os("PGEN_REQUIRE_PLINK2_ORACLE").is_some();
    let plink2_available = Command::new(&plink2)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success());
    if plink2_available {
        let export_prefix = temp.path().join("plink2-export");
        let output = Command::new(&plink2)
            .args([
                "--pfile",
                prefix.to_str().unwrap(),
                "--export",
                "vcf",
                "--out",
                export_prefix.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "PLINK 2 oracle failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let vcf = fs::read_to_string(export_prefix.with_extension("vcf")).unwrap();
        let first = vcf.lines().find(|line| !line.starts_with('#')).unwrap();
        let calls = first
            .split('\t')
            .skip(9)
            .map(|sample| sample.split(':').next().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            calls,
            vec!["0/0", "0/1", "1/1", "./.", "0/0", "0/0", "0/0", "0/0"]
        );
    } else {
        assert!(!require_plink2, "required PLINK 2 oracle is unavailable");
    }

    let provider = PgenTableProvider::try_new(
        path(&prefix.with_extension("pgen")),
        PgenReadOptions {
            samples: Some(vec!["s8".to_string(), "s2".to_string(), "s1".to_string()]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let oracle_context = context(1);
    oracle_context
        .register_table("p", Arc::new(provider))
        .unwrap();
    let batches = oracle_context
        .sql("SELECT id, genotypes FROM p ORDER BY start")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        ids.iter().collect::<Vec<_>>(),
        vec![Some("v1"), Some("v2"), Some("v3")]
    );
    assert_eq!(
        gt_values(&batches[0], 1, 0),
        vec![Some(vec![0, 0]), Some(vec![0, 1]), Some(vec![0, 0])]
    );
    assert_eq!(
        gt_values(&batches[0], 1, 1),
        vec![Some(vec![0, 0]), Some(vec![0, 0]), Some(vec![0, 0])]
    );

    let phase_provider =
        PgenTableProvider::try_new(path(&temp.path().join("phase.pgen")), Default::default())
            .await
            .unwrap();
    let phase_context = context(1);
    phase_context
        .register_table("phase", Arc::new(phase_provider))
        .unwrap();
    let phase_batches = phase_context
        .sql("SELECT genotypes FROM phase")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        gt_values(&phase_batches[0], 0, 0),
        vec![Some(vec![0, 1]), Some(vec![1, 0]), Some(vec![0, 0]), None]
    );
    assert_eq!(
        phased_values(&phase_batches[0], 0, 0),
        vec![Some(true), Some(true), Some(false), None]
    );

    let dosage_provider = PgenTableProvider::try_new(
        path(&temp.path().join("dosage.pgen")),
        PgenReadOptions {
            genotype_fields: Some(vec!["DS".to_string()]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let dosage_context = context(1);
    dosage_context
        .register_table("dosage", Arc::new(dosage_provider))
        .unwrap();
    let dosage_batches = dosage_context
        .sql("SELECT genotypes FROM dosage")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        ds_values(&dosage_batches[0], 0, 0),
        vec![Some(0.125), Some(1.0), Some(1.875), None]
    );
}
