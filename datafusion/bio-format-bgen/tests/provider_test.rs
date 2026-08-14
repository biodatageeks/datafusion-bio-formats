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
    Array, Float32Array, ListArray, StringArray, StructArray, UInt8Array,
};
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::{TableProviderFilterPushDown, col, lit};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_bgen::{
    BgenExec, BgenOutputMode, BgenReadOptions, BgenTableProvider, StaleBgiPolicy,
};
use datafusion_bio_format_core::genotype::{GenotypeMetric, MissingSamplePolicy};
use flate2::Compression as FlateCompression;
use flate2::write::ZlibEncoder;
use rusqlite::{Connection, params};
use tempfile::TempDir;

#[derive(Clone, Copy)]
enum Codec {
    None,
    Zlib,
    Zstd,
}

struct SampleProbabilities {
    ploidy: u8,
    missing: bool,
    stored: Vec<u32>,
}

struct Variant {
    id: &'static str,
    rsid: &'static str,
    chrom: &'static str,
    position: u32,
    alleles: Vec<&'static str>,
    phased: bool,
    bits: u8,
    samples: Vec<SampleProbabilities>,
}

#[derive(Clone)]
struct IndexRow {
    chrom: String,
    position: u32,
    rsid: String,
    allele_count: usize,
    allele1: String,
    allele2: Option<String>,
    offset: u64,
    size: u64,
    payload_offset: u64,
}

struct Fixture {
    _dir: TempDir,
    bgen: PathBuf,
    bgi: PathBuf,
    rows: Vec<IndexRow>,
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
    fn start(bgen: Vec<u8>, bgi: Vec<u8>) -> Self {
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
                // Accepted sockets can inherit the listener's non-blocking mode,
                // so reading the request would race the client's write.
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
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
                    "/cohort.bgen" => &bgen,
                    "/cohort.bgen.bgi" => &bgi,
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

fn variants() -> Vec<Variant> {
    vec![
        Variant {
            id: "v1",
            rsid: "rs1",
            chrom: "1",
            position: 10,
            alleles: vec!["A", "C"],
            phased: false,
            bits: 8,
            samples: vec![
                sample(2, false, &[255, 0]),
                sample(2, false, &[0, 255]),
                sample(2, true, &[0, 0]),
            ],
        },
        Variant {
            id: "v2",
            rsid: "rs2",
            chrom: "1",
            position: 20,
            alleles: vec!["G", "T"],
            phased: true,
            bits: 8,
            samples: vec![
                sample(2, false, &[255, 0]),
                sample(2, false, &[0, 0]),
                sample(2, true, &[0, 0]),
            ],
        },
        Variant {
            id: "v3",
            rsid: "rs3",
            chrom: "2",
            position: 30,
            alleles: vec!["A", "C", "G"],
            phased: false,
            bits: 4,
            samples: vec![
                sample(1, false, &[15, 0]),
                sample(2, false, &[0, 15, 0, 0, 0]),
                sample(1, true, &[0, 0]),
            ],
        },
    ]
}

fn sample(ploidy: u8, missing: bool, stored: &[u32]) -> SampleProbabilities {
    SampleProbabilities {
        ploidy,
        missing,
        stored: stored.to_vec(),
    }
}

fn fixture(codec: Codec, embedded_samples: bool) -> Fixture {
    let dir = TempDir::new().unwrap();
    let bgen = dir.path().join("cohort.bgen");
    let bgi = dir.path().join("cohort.bgen.bgi");
    let (bytes, rows) = encode_layout2(codec, embedded_samples, &variants());
    fs::write(&bgen, bytes).unwrap();
    Fixture {
        _dir: dir,
        bgen,
        bgi,
        rows,
    }
}

fn encode_layout2(
    codec: Codec,
    embedded_samples: bool,
    variants: &[Variant],
) -> (Vec<u8>, Vec<IndexRow>) {
    let names = ["s1", "s2", "s3"];
    let mut sample_block = Vec::new();
    if embedded_samples {
        sample_block.extend_from_slice(&0_u32.to_le_bytes());
        sample_block.extend_from_slice(&(names.len() as u32).to_le_bytes());
        for name in names {
            put_string_u16(&mut sample_block, name);
        }
        let length = sample_block.len() as u32;
        sample_block[..4].copy_from_slice(&length.to_le_bytes());
    }

    let header_length = 20_u32;
    let first_variant = 4 + header_length as usize + sample_block.len();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&((first_variant - 4) as u32).to_le_bytes());
    bytes.extend_from_slice(&header_length.to_le_bytes());
    bytes.extend_from_slice(&(variants.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(names.len() as u32).to_le_bytes());
    bytes.extend_from_slice(b"bgen");
    let compression = match codec {
        Codec::None => 0_u32,
        Codec::Zlib => 1,
        Codec::Zstd => 2,
    };
    let flags = compression | (2 << 2) | u32::from(embedded_samples) << 31;
    bytes.extend_from_slice(&flags.to_le_bytes());
    bytes.extend_from_slice(&sample_block);

    let mut rows = Vec::new();
    for variant in variants {
        let offset = bytes.len() as u64;
        put_string_u16(&mut bytes, variant.id);
        put_string_u16(&mut bytes, variant.rsid);
        put_string_u16(&mut bytes, variant.chrom);
        bytes.extend_from_slice(&variant.position.to_le_bytes());
        bytes.extend_from_slice(&(variant.alleles.len() as u16).to_le_bytes());
        for allele in &variant.alleles {
            put_string_u32(&mut bytes, allele);
        }
        let payload_offset = bytes.len() as u64;

        let block = encode_layout2_block(variant);
        match codec {
            Codec::None => {
                bytes.extend_from_slice(&(block.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&block);
            }
            Codec::Zlib => {
                let mut encoder = ZlibEncoder::new(Vec::new(), FlateCompression::fast());
                encoder.write_all(&block).unwrap();
                let compressed = encoder.finish().unwrap();
                bytes.extend_from_slice(&((compressed.len() + 4) as u32).to_le_bytes());
                bytes.extend_from_slice(&(block.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&compressed);
            }
            Codec::Zstd => {
                let compressed = zstd::stream::encode_all(block.as_slice(), 1).unwrap();
                bytes.extend_from_slice(&((compressed.len() + 4) as u32).to_le_bytes());
                bytes.extend_from_slice(&(block.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&compressed);
            }
        }
        rows.push(IndexRow {
            chrom: variant.chrom.to_string(),
            position: variant.position,
            rsid: variant.rsid.to_string(),
            allele_count: variant.alleles.len(),
            allele1: variant.alleles[0].to_string(),
            allele2: variant.alleles.get(1).map(|value| (*value).to_string()),
            offset,
            size: bytes.len() as u64 - offset,
            payload_offset,
        });
    }
    (bytes, rows)
}

fn encode_layout2_block(variant: &Variant) -> Vec<u8> {
    let mut block = Vec::new();
    block.extend_from_slice(&(variant.samples.len() as u32).to_le_bytes());
    block.extend_from_slice(&(variant.alleles.len() as u16).to_le_bytes());
    let min_ploidy = variant
        .samples
        .iter()
        .map(|sample| sample.ploidy)
        .min()
        .unwrap();
    let max_ploidy = variant
        .samples
        .iter()
        .map(|sample| sample.ploidy)
        .max()
        .unwrap();
    block.push(min_ploidy);
    block.push(max_ploidy);
    for sample in &variant.samples {
        block.push(sample.ploidy | u8::from(sample.missing) << 7);
    }
    block.push(u8::from(variant.phased));
    block.push(variant.bits);
    let values = variant
        .samples
        .iter()
        .flat_map(|sample| sample.stored.iter().copied());
    block.extend_from_slice(&pack_bits(values, variant.bits));
    block
}

fn pack_bits(values: impl Iterator<Item = u32>, bits: u8) -> Vec<u8> {
    let mut output = Vec::new();
    let mut bit_offset = 0_usize;
    for value in values {
        for bit in 0..bits as usize {
            let byte_index = bit_offset / 8;
            if byte_index == output.len() {
                output.push(0);
            }
            output[byte_index] |= (((value >> bit) & 1) as u8) << (bit_offset % 8);
            bit_offset += 1;
        }
    }
    output
}

fn encode_layout1(codec: Codec) -> Vec<u8> {
    let values = [[32_768_u16, 0, 0], [0, 0, 0], [0, 32_768, 0]];
    let mut payload = Vec::new();
    for sample in values {
        for value in sample {
            payload.extend_from_slice(&value.to_le_bytes());
        }
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&20_u32.to_le_bytes());
    bytes.extend_from_slice(&20_u32.to_le_bytes());
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&3_u32.to_le_bytes());
    bytes.extend_from_slice(b"bgen");
    let compression = match codec {
        Codec::None => 0_u32,
        Codec::Zlib => 1,
        Codec::Zstd => panic!("Layout 1 does not support zstd"),
    };
    bytes.extend_from_slice(&(compression | (1 << 2)).to_le_bytes());
    bytes.extend_from_slice(&3_u32.to_le_bytes());
    put_string_u16(&mut bytes, "layout1");
    put_string_u16(&mut bytes, "rs-layout1");
    put_string_u16(&mut bytes, "3");
    bytes.extend_from_slice(&40_u32.to_le_bytes());
    put_string_u32(&mut bytes, "A");
    put_string_u32(&mut bytes, "G");
    match codec {
        Codec::None => bytes.extend_from_slice(&payload),
        Codec::Zlib => {
            let mut encoder = ZlibEncoder::new(Vec::new(), FlateCompression::fast());
            encoder.write_all(&payload).unwrap();
            let compressed = encoder.finish().unwrap();
            bytes.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&compressed);
        }
        Codec::Zstd => unreachable!(),
    }
    bytes
}

fn put_string_u16(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u16).to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

fn put_string_u32(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

fn create_bgi(fixture: &Fixture, mutate_prefix: bool) {
    let bgen_bytes = fs::read(&fixture.bgen).unwrap();
    let connection = Connection::open(&fixture.bgi).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE Metadata(
                 filename TEXT,
                 file_size INTEGER,
                 last_write_time INTEGER,
                 first_1000_bytes BLOB,
                 index_creation_time INTEGER
             );
             CREATE TABLE Variant(
                 chromosome TEXT NOT NULL,
                 position INTEGER NOT NULL,
                 rsid TEXT NOT NULL,
                 number_of_alleles INTEGER NOT NULL,
                 allele1 TEXT,
                 allele2 TEXT,
                 file_start_position INTEGER NOT NULL,
                 size_in_bytes INTEGER NOT NULL
             );",
        )
        .unwrap();
    let mut prefix = bgen_bytes[..bgen_bytes.len().min(1000)].to_vec();
    if mutate_prefix {
        prefix[0] ^= 1;
    }
    connection
        .execute(
            "INSERT INTO Metadata VALUES (?1, ?2, 0, ?3, 0)",
            params![
                fixture.bgen.file_name().unwrap().to_string_lossy(),
                bgen_bytes.len() as i64,
                prefix,
            ],
        )
        .unwrap();
    for row in &fixture.rows {
        connection
            .execute(
                "INSERT INTO Variant VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    row.chrom,
                    row.position,
                    row.rsid,
                    row.allele_count,
                    row.allele1,
                    row.allele2,
                    row.offset,
                    row.size,
                ],
            )
            .unwrap();
    }
}

fn path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn context(batch_size: usize) -> SessionContext {
    SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(batch_size),
    )
}

fn probability_values(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column: usize,
    row: usize,
) -> Vec<Option<Vec<f32>>> {
    let genotypes = batch
        .column(column)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let gp = genotypes
        .column_by_name("GP")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let samples = gp.value(row);
    let samples = samples.as_any().downcast_ref::<ListArray>().unwrap();
    (0..samples.len())
        .map(|sample| {
            if samples.is_null(sample) {
                return None;
            }
            let values = samples.value(sample);
            let values = values.as_any().downcast_ref::<Float32Array>().unwrap();
            Some((0..values.len()).map(|index| values.value(index)).collect())
        })
        .collect()
}

fn dosage_values(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column: usize,
    row: usize,
) -> Vec<Option<f32>> {
    let genotypes = batch
        .column(column)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let ds = genotypes
        .column_by_name("DS")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = ds.value(row);
    let values = values.as_any().downcast_ref::<Float32Array>().unwrap();
    (0..values.len())
        .map(|index| (!values.is_null(index)).then(|| values.value(index)))
        .collect()
}

fn ploidy_values(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    column: usize,
    row: usize,
) -> Vec<u8> {
    let genotypes = batch
        .column(column)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let ploidy = genotypes
        .column_by_name("PLOIDY")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = ploidy.value(row);
    let values = values.as_any().downcast_ref::<UInt8Array>().unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}

#[tokio::test]
async fn decodes_layout2_probability_semantics_and_sample_order() {
    let fixture = fixture(Codec::None, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            samples: Some(vec!["s2".to_string(), "s3".to_string(), "s1".to_string()]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(provider.sample_names(), &["s2", "s3", "s1"]);

    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT id, phased, bits, genotypes FROM b ORDER BY start")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
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
        probability_values(&batches[0], 3, 0),
        vec![Some(vec![0.0, 1.0, 0.0]), None, Some(vec![1.0, 0.0, 0.0])]
    );
    assert_eq!(
        probability_values(&batches[0], 3, 1),
        vec![
            Some(vec![0.0, 1.0, 0.0, 1.0]),
            None,
            Some(vec![1.0, 0.0, 0.0, 1.0]),
        ]
    );
    assert_eq!(
        probability_values(&batches[0], 3, 2),
        vec![
            Some(vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0]),
            None,
            Some(vec![1.0, 0.0, 0.0]),
        ]
    );
    assert_eq!(ploidy_values(&batches[0], 3, 2), vec![2, 1, 1]);
}

#[tokio::test]
async fn decodes_each_layout2_compression_mode() {
    for codec in [Codec::None, Codec::Zlib, Codec::Zstd] {
        let fixture = fixture(codec, true);
        let provider = BgenTableProvider::try_new(path(&fixture.bgen), Default::default())
            .await
            .unwrap();
        let context = context(1024);
        context.register_table("b", Arc::new(provider)).unwrap();
        let batches = context
            .sql("SELECT genotypes FROM b WHERE id = 'v1'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            probability_values(&batches[0], 0, 0),
            vec![Some(vec![1.0, 0.0, 0.0]), Some(vec![0.0, 1.0, 0.0]), None]
        );
    }
}

#[tokio::test]
async fn emits_biallelic_dosage_and_rejects_multiallelic_selection() {
    let fixture = fixture(Codec::Zstd, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            output_mode: BgenOutputMode::Dosage,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM b WHERE chrom = '1' ORDER BY start")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        dosage_values(&batches[0], 0, 0),
        vec![Some(0.0), Some(1.0), None]
    );
    assert_eq!(
        dosage_values(&batches[0], 0, 1),
        vec![Some(1.0), Some(2.0), None]
    );
    let error = context
        .sql("SELECT genotypes FROM b WHERE id = 'v3'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("does not support multiallelic"), "{error}");
}

#[tokio::test]
async fn decodes_layout1_uncompressed_and_zlib() {
    for codec in [Codec::None, Codec::Zlib] {
        let dir = TempDir::new().unwrap();
        let bgen = dir.path().join("layout1.bgen");
        fs::write(&bgen, encode_layout1(codec)).unwrap();
        let provider = BgenTableProvider::try_new(path(&bgen), BgenReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            provider.sample_names(),
            &["sample_1", "sample_2", "sample_3"]
        );
        let context = context(1024);
        context.register_table("b", Arc::new(provider)).unwrap();
        let batches = context
            .sql("SELECT genotypes FROM b")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            probability_values(&batches[0], 0, 0),
            vec![Some(vec![1.0, 0.0, 0.0]), None, Some(vec![0.0, 1.0, 0.0])]
        );
    }
}

#[tokio::test]
async fn uses_external_sample_metadata_when_ids_are_not_embedded() {
    let fixture = fixture(Codec::None, false);
    let sample_path = fixture._dir.path().join("cohort.sample");
    fs::write(
        &sample_path,
        "ID_1 ID_2 missing\n0 0 0\nfamily1 x1 0\nfamily2 x2 0\nfamily3 x3 0\n",
    )
    .unwrap();
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            sample_path: Some(path(&sample_path)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(provider.sample_names(), &["x1", "x2", "x3"]);
}

#[tokio::test]
async fn exact_filters_limits_and_metadata_projection_skip_payloads() {
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .unwrap();
    let filter = col("chrom").eq(lit("1"));
    assert_eq!(
        provider.supports_filters_pushdown(&[&filter]).unwrap(),
        vec![TableProviderFilterPushDown::Exact]
    );
    assert_eq!(
        provider
            .supports_filters_pushdown(&[&col("alleles").is_not_null()])
            .unwrap(),
        vec![TableProviderFilterPushDown::Unsupported]
    );

    let context = context(1024);
    let state = context.state();
    let plan = provider
        .scan(&state, Some(&vec![0, 3]), &[filter], Some(1))
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<BgenExec>().unwrap();
    let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let metrics = exec.metrics_snapshot();
    assert_eq!(metrics[GenotypeMetric::CompressedBytes as usize].1, 0);
    assert_eq!(metrics[GenotypeMetric::DecompressedBytes as usize].1, 0);
    assert_eq!(metrics[GenotypeMetric::PayloadsSkipped as usize].1, 1);
    assert_eq!(metrics[GenotypeMetric::SelectedVariants as usize].1, 1);
}

#[tokio::test]
async fn empty_sample_selection_emits_empty_values_without_payload_reads() {
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            samples: Some(vec!["absent".to_string()]),
            missing_sample_policy: MissingSamplePolicy::Ignore,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    let state = context.state();
    let plan = provider
        .scan(&state, Some(&vec![8]), &[], Some(1))
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<BgenExec>().unwrap();
    let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
    assert_eq!(
        probability_values(&batches[0], 0, 0),
        Vec::<Option<Vec<f32>>>::new()
    );
    assert_eq!(
        exec.metrics_snapshot()[GenotypeMetric::CompressedBytes as usize].1,
        0
    );
}

#[tokio::test]
async fn honors_row_and_soft_byte_batch_limits() {
    let fixture = fixture(Codec::None, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            batch_soft_byte_limit: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(2);
    let state = context.state();
    let plan = provider
        .scan(&state, Some(&vec![8]), &[], None)
        .await
        .unwrap();
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(batches.len(), 3);
    assert!(batches.iter().all(|batch| batch.num_rows() == 1));
}

#[tokio::test]
async fn validates_local_bgi_and_reports_index_metadata() {
    let fixture = fixture(Codec::None, true);
    create_bgi(&fixture, false);
    let provider = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .unwrap();
    assert_eq!(
        provider
            .schema()
            .metadata()
            .get("bio.bgen.index")
            .map(String::as_str),
        Some("bgi")
    );
    let context = context(1024);
    let state = context.state();
    let plan = provider
        .scan(&state, Some(&vec![0]), &[col("rsid").eq(lit("rs2"))], None)
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<BgenExec>().unwrap();
    let batches = collect(plan.clone(), context.task_ctx()).await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert!(exec.metrics_snapshot()[GenotypeMetric::CompanionBytesRead as usize].1 > 0);

    let plan = provider
        .scan(
            &state,
            Some(&vec![0]),
            &[col("start").gt_eq(lit(19_u64))],
            None,
        )
        .await
        .unwrap();
    let batches = collect(plan, context.task_ctx()).await.unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
}

#[tokio::test]
async fn applies_stale_bgi_policy_and_never_ignores_an_explicit_index() {
    let fixture = fixture(Codec::None, true);
    create_bgi(&fixture, true);
    let provider = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .unwrap();
    assert_eq!(
        provider
            .schema()
            .metadata()
            .get("bio.bgen.index")
            .map(String::as_str),
        Some("transient")
    );

    let error = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            stale_bgi_policy: StaleBgiPolicy::Error,
            ..Default::default()
        },
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("first_1000_bytes"), "{error}");

    let error = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            bgi_path: Some(path(&fixture.bgi)),
            stale_bgi_policy: StaleBgiPolicy::Ignore,
            ..Default::default()
        },
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("first_1000_bytes"), "{error}");
}

#[tokio::test]
async fn caches_remote_bgi_and_uses_bounded_bgen_ranges() {
    let fixture = fixture(Codec::Zstd, true);
    create_bgi(&fixture, false);
    let server = RangeServer::start(
        fs::read(&fixture.bgen).unwrap(),
        fs::read(&fixture.bgi).unwrap(),
    );
    let cache = fixture._dir.path().join("cache");
    let options = BgenReadOptions {
        bgi_cache_directory: Some(path(&cache)),
        ..Default::default()
    };
    let provider = BgenTableProvider::try_new(server.url("cohort.bgen"), options.clone())
        .await
        .unwrap();
    assert_eq!(
        fs::read_dir(&cache).unwrap().count(),
        1,
        "one content-addressed SQLite cache entry"
    );
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM b WHERE rsid = 'rs2'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert!(
        server
            .get_requests("cohort.bgen")
            .iter()
            .all(|request| request.range.is_some()),
        "BGEN access must remain range-based"
    );

    BgenTableProvider::try_new(server.url("cohort.bgen"), options)
        .await
        .unwrap();
    assert_eq!(fs::read_dir(&cache).unwrap().count(), 1);

    let error = BgenTableProvider::try_new(
        server.url("cohort.bgen"),
        BgenReadOptions {
            bgi_path: Some(server.url("cohort.bgen.bgi")),
            bgi_cache_directory: Some(path(&cache)),
            max_bgi_cache_bytes: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("max_bgi_cache_bytes"), "{error}");
}

#[tokio::test]
async fn rejects_malformed_headers_truncation_and_resource_limit_violations() {
    let fixture = fixture(Codec::Zlib, true);
    let original = fs::read(&fixture.bgen).unwrap();
    for (bytes, expected) in [
        (
            {
                let mut bytes = original.clone();
                bytes[20..24].copy_from_slice(&(3_u32 | (2 << 2)).to_le_bytes());
                bytes
            },
            "compression flag",
        ),
        (
            {
                let mut bytes = original.clone();
                bytes.truncate(bytes.len() - 1);
                bytes
            },
            "beyond object size",
        ),
        (
            {
                let mut bytes = original.clone();
                bytes[16..20].copy_from_slice(b"nope");
                bytes
            },
            "magic",
        ),
    ] {
        fs::write(&fixture.bgen, &bytes).unwrap();
        let error = BgenTableProvider::try_new(path(&fixture.bgen), Default::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
    }
    fs::write(&fixture.bgen, original).unwrap();

    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            max_states_per_sample: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let error = context
        .sql("SELECT genotypes FROM b WHERE id = 'v1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("max_states_per_sample"), "{error}");
}

#[tokio::test]
async fn rejects_malformed_probability_blocks_before_arrow_construction() {
    let fixture = fixture(Codec::Zlib, true);
    let mut bytes = fs::read(&fixture.bgen).unwrap();
    let payload = fixture.rows[0].payload_offset as usize;
    bytes[payload + 4..payload + 8].copy_from_slice(&u32::MAX.to_le_bytes());
    fs::write(&fixture.bgen, bytes).unwrap();
    let provider = BgenTableProvider::try_new(path(&fixture.bgen), Default::default())
        .await
        .unwrap();
    let bomb_context = context(1024);
    bomb_context
        .register_table("bomb", Arc::new(provider))
        .unwrap();
    let error = bomb_context
        .sql("SELECT genotypes FROM bomb WHERE id = 'v1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("max_decompressed_block_bytes"), "{error}");

    let dir = TempDir::new().unwrap();
    let invalid_sum = dir.path().join("invalid-sum.bgen");
    let mut invalid_variants = variants();
    invalid_variants[0].samples[0].stored = vec![200, 100];
    fs::write(
        &invalid_sum,
        encode_layout2(Codec::None, true, &invalid_variants).0,
    )
    .unwrap();
    let provider = BgenTableProvider::try_new(path(&invalid_sum), Default::default())
        .await
        .unwrap();
    let sum_context = context(1024);
    sum_context
        .register_table("invalid_sum", Arc::new(provider))
        .unwrap();
    let error = sum_context
        .sql("SELECT genotypes FROM invalid_sum WHERE id = 'v1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("exceeds denominator"), "{error}");

    let invalid_padding = dir.path().join("invalid-padding.bgen");
    let mut bytes = encode_layout2(Codec::None, true, &variants()).0;
    *bytes.last_mut().unwrap() |= 0xf0;
    fs::write(&invalid_padding, bytes).unwrap();
    let provider = BgenTableProvider::try_new(path(&invalid_padding), Default::default())
        .await
        .unwrap();
    let padding_context = context(1024);
    padding_context
        .register_table("invalid_padding", Arc::new(provider))
        .unwrap();
    let error = padding_context
        .sql("SELECT genotypes FROM invalid_padding WHERE id = 'v3'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("padding bits"), "{error}");

    let inconsistent_samples = dir.path().join("inconsistent-samples.bgen");
    let (mut bytes, rows) = encode_layout2(Codec::None, true, &variants());
    let payload = rows[0].payload_offset as usize;
    bytes[payload + 4..payload + 8].copy_from_slice(&4_u32.to_le_bytes());
    fs::write(&inconsistent_samples, bytes).unwrap();
    let provider = BgenTableProvider::try_new(path(&inconsistent_samples), Default::default())
        .await
        .unwrap();
    let sample_context = context(1024);
    sample_context
        .register_table("inconsistent_samples", Arc::new(provider))
        .unwrap();
    let error = sample_context
        .sql("SELECT genotypes FROM inconsistent_samples WHERE id = 'v1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("differs from header count"), "{error}");
}

#[test]
fn differential_reference_oracles_when_installed() {
    let fixture = fixture(Codec::Zlib, true);
    create_bgi(&fixture, false);
    let bgen = path(&fixture.bgen);
    let required = std::env::var_os("BGEN_REQUIRE_REFERENCE_ORACLES").is_some();
    let mut executed = 0;

    let python = std::env::var("BGEN_REFERENCE_PYTHON").unwrap_or_else(|_| "python3".to_string());
    let snputils = r#"
import sys
import numpy as np
from snputils.snp.io.read.bgen import BGENReader
gp = np.asarray(BGENReader(sys.argv[1]).read(fields=["GP"]).calldata_gp)
assert gp.shape[0] == 3 and gp.shape[1] == 3
np.testing.assert_allclose(gp[0, 0, :3], [1.0, 0.0, 0.0], atol=(1.0 / 255.0))
np.testing.assert_allclose(gp[0, 1, :3], [0.0, 1.0, 0.0], atol=(1.0 / 255.0))
"#;
    executed += usize::from(run_python_oracle(
        &python, "snputils", snputils, &bgen, required,
    ));

    let limix_bgen = r#"
import sys
import numpy as np
from bgen import BgenReader
with BgenReader(sys.argv[1], "", delay_parsing=True) as reader:
    assert len(reader) == 3
    gp = np.asarray(reader[0].probabilities)
    np.testing.assert_allclose(gp[0, :3], [1.0, 0.0, 0.0], atol=(1.0 / 255.0))
    np.testing.assert_allclose(gp[1, :3], [0.0, 1.0, 0.0], atol=(1.0 / 255.0))
"#;
    executed += usize::from(run_python_oracle(
        &python,
        "limix/bgen",
        limix_bgen,
        &bgen,
        required,
    ));

    match Command::new("bgenix").args(["-g", &bgen, "-vcf"]).output() {
        Ok(output) => {
            executed += 1;
            assert!(
                output.status.success(),
                "bgenix failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(stdout.contains("rs1") && stdout.contains("GP"), "{stdout}");
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => {}
        Err(error) => panic!("bgenix oracle is unavailable: {error}"),
    }

    let qctool_output = fixture._dir.path().join("qctool.vcf");
    match Command::new("qctool")
        .args(["-g", &bgen, "-og", &path(&qctool_output)])
        .output()
    {
        Ok(output) => {
            executed += 1;
            assert!(
                output.status.success(),
                "qctool failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let vcf = fs::read_to_string(qctool_output).unwrap();
            assert!(vcf.contains("rs1") && vcf.contains("GP"), "{vcf}");
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => {}
        Err(error) => panic!("qctool oracle is unavailable: {error}"),
    }

    if required {
        assert_eq!(
            executed, 4,
            "BGEN_REQUIRE_REFERENCE_ORACLES requires snputils, limix/bgen, bgenix, and qctool"
        );
    }
}

#[tokio::test]
async fn coalescing_bridges_metadata_gaps_without_collapsing_partitions() {
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .unwrap();

    // Consecutive payloads are separated only by the next variant's metadata,
    // so one sequential partition must bridge those gaps instead of issuing a
    // read per variant.
    let sequential = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(1024),
    );
    let plan = provider
        .scan(&sequential.state(), Some(&vec![8]), &[], None)
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<BgenExec>().unwrap();
    assert_eq!(
        datafusion::physical_plan::ExecutionPlan::properties(exec)
            .partitioning
            .partition_count(),
        1
    );
    collect(plan.clone(), sequential.task_ctx()).await.unwrap();
    let range_requests = exec.metrics_snapshot()[GenotypeMetric::RangeRequests as usize].1;
    assert!(
        range_requests < fixture.rows.len() as u64,
        "expected coalesced reads, got {range_requests} for {} variants",
        fixture.rows.len()
    );

    // Bridging those gaps must not merge the whole file into a single range and
    // leave the other requested partitions empty.
    let parallel = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(2)
            .with_batch_size(1024),
    );
    let parallel_plan = provider
        .scan(&parallel.state(), Some(&vec![8]), &[], None)
        .await
        .unwrap();
    assert_eq!(parallel_plan.properties().partitioning.partition_count(), 2);
    let batches = collect(parallel_plan, parallel.task_ctx()).await.unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        fixture.rows.len()
    );
}

#[tokio::test]
async fn a_cached_bgi_survives_eviction_by_a_later_provider() {
    let fixture = fixture(Codec::Zstd, true);
    create_bgi(&fixture, false);
    let server = RangeServer::start(
        fs::read(&fixture.bgen).unwrap(),
        fs::read(&fixture.bgi).unwrap(),
    );
    let cache = fixture._dir.path().join("cache");
    let provider = BgenTableProvider::try_new(
        server.url("cohort.bgen"),
        BgenReadOptions {
            bgi_cache_directory: Some(path(&cache)),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Simulate a later provider evicting this entry from the shared cache.
    for entry in fs::read_dir(&cache).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|value| value.to_str()) == Some("bgi") {
            fs::remove_file(entry.path()).unwrap();
        }
    }

    // The first provider still resolves its index-backed predicate.
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT rsid FROM b WHERE rsid = 'rs2'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
}

fn run_python_oracle(python: &str, name: &str, script: &str, bgen: &str, required: bool) -> bool {
    let import_name = if name == "limix/bgen" { "bgen" } else { name };
    let available = Command::new(python)
        .args(["-c", &format!("import {import_name}")])
        .output()
        .is_ok_and(|output| output.status.success());
    if !available {
        assert!(!required, "required Python oracle {name} is unavailable");
        return false;
    }
    let output = Command::new(python)
        .args(["-c", script, bgen])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{name} oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    true
}
