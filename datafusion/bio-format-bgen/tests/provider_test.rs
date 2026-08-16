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
    BgenExec, BgenOutputMode, BgenProbabilityLayout, BgenReadOptions, BgenTableProvider,
    StaleBgiPolicy,
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

/// A diploid biallelic sample whose stored probabilities vary with its index,
/// so a wide cohort does not compress away to nothing.
fn sample_probabilities(index: usize) -> SampleProbabilities {
    SampleProbabilities {
        ploidy: 2,
        missing: false,
        stored: vec![(index % 251) as u32, ((index * 7) % 251) as u32],
    }
}

fn sample(ploidy: u8, missing: bool, stored: &[u32]) -> SampleProbabilities {
    SampleProbabilities {
        ploidy,
        missing,
        stored: stored.to_vec(),
    }
}

fn fixture(codec: Codec, embedded_samples: bool) -> Fixture {
    fixture_with_variants(codec, embedded_samples, &variants())
}

fn fixture_with_variants(codec: Codec, embedded_samples: bool, variants: &[Variant]) -> Fixture {
    let dir = TempDir::new().unwrap();
    let bgen = dir.path().join("cohort.bgen");
    let bgi = dir.path().join("cohort.bgen.bgi");
    let (bytes, rows) = encode_layout2(codec, embedded_samples, variants);
    fs::write(&bgen, bytes).unwrap();
    Fixture {
        _dir: dir,
        bgen,
        bgi,
        rows,
    }
}

/// Enough biallelic variants that the object is several times the 1000-byte
/// identity prefix, so a read of the variant region is unmistakable in the
/// server's request log.
fn many_variants(count: usize) -> Vec<Variant> {
    (0..count)
        .map(|index| Variant {
            id: Box::leak(format!("v{index}").into_boxed_str()),
            rsid: Box::leak(format!("rs{index}").into_boxed_str()),
            chrom: "1",
            position: 10 + index as u32,
            alleles: vec!["A", "C"],
            phased: false,
            bits: 8,
            samples: vec![
                sample(2, false, &[255, 0]),
                sample(2, false, &[0, 255]),
                sample(2, true, &[0, 0]),
            ],
        })
        .collect()
}

fn encode_layout2(
    codec: Codec,
    embedded_samples: bool,
    variants: &[Variant],
) -> (Vec<u8>, Vec<IndexRow>) {
    encode_layout2_with_samples(codec, embedded_samples, variants, &["s1", "s2", "s3"])
}

fn encode_layout2_with_samples(
    codec: Codec,
    embedded_samples: bool,
    variants: &[Variant],
    names: &[&str],
) -> (Vec<u8>, Vec<IndexRow>) {
    let mut sample_block = Vec::new();
    if embedded_samples {
        sample_block.extend_from_slice(&0_u32.to_le_bytes());
        sample_block.extend_from_slice(&(names.len() as u32).to_le_bytes());
        for name in names.iter() {
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
    encode_layout1_with(codec, [[32_768_u16, 0, 0], [0, 0, 0], [0, 32_768, 0]])
}

fn encode_layout1_with(codec: Codec, values: [[u16; 3]; 3]) -> Vec<u8> {
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

    // Projecting only the header-derived flags builds no dosage, so the
    // multiallelic restriction of the unprojected output mode must not apply.
    let flags = context
        .sql("SELECT phased, bits FROM b WHERE id = 'v3'")
        .await
        .unwrap()
        .collect()
        .await
        .expect("a flag-only projection must not apply the dosage restriction");
    assert_eq!(flags.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
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
async fn opening_an_indexed_bgen_does_not_read_the_variant_region() {
    // The BGI already records every variant's chromosome, position, RS
    // identifier, allele count and record range. Walking the object to rebuild
    // that at construction duplicates the index, and because variant metadata
    // and genotype payloads are interleaved, the walk's read-ahead drags the
    // payloads along with it — so opening a table can download the whole file.
    let variants = many_variants(200);
    let fixture = fixture_with_variants(Codec::Zstd, true, &variants);
    create_bgi(&fixture, false);
    let object_size = fs::metadata(&fixture.bgen).unwrap().len();
    assert!(
        object_size > 4 * IDENTITY_PREFIX_BYTES,
        "fixture must be several times the identity prefix: {object_size}"
    );

    let server = RangeServer::start(
        fs::read(&fixture.bgen).unwrap(),
        fs::read(&fixture.bgi).unwrap(),
    );
    let cache = fixture._dir.path().join("cache");
    BgenTableProvider::try_new(
        server.url("cohort.bgen"),
        BgenReadOptions {
            bgi_cache_directory: Some(path(&cache)),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Opening the table needs the header and the bytes the BGI's own identity
    // check covers. Nothing beyond that.
    let allowed_end = IDENTITY_PREFIX_BYTES.max(fixture.rows[0].offset);
    let reads = server.get_requests("cohort.bgen");
    for request in &reads {
        let end = request
            .range
            .map_or(object_size, |(_, end)| end as u64 + 1)
            .min(object_size);
        assert!(
            end <= allowed_end,
            "opening an indexed BGEN read to byte {end}, past the header and identity prefix \
             at {allowed_end}: {reads:?}"
        );
    }
}

/// Bytes of the BGEN object the BGI stores for its own identity check.
const IDENTITY_PREFIX_BYTES: u64 = 1000;

#[tokio::test]
async fn verifying_index_records_catches_a_stale_row_that_pruning_would_hide() {
    // Predicates are pushed into the index, so a row with a stale RS identifier
    // is pruned before its record is ever read: the deferred per-record check
    // cannot fire for a variant the index never offers as a candidate, and the
    // query quietly returns nothing. Verification is what closes that, at the
    // cost of walking the object once.
    let fixture = fixture(Codec::Zstd, true);
    create_bgi(&fixture, false);
    let connection = Connection::open(&fixture.bgi).unwrap();
    connection
        .execute(
            "UPDATE Variant SET rsid = 'rs-wrong' WHERE rsid = 'rs2'",
            [],
        )
        .unwrap();
    drop(connection);

    // Trusting the index: the variant really does carry rs2, and it is missed.
    let trusting = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(trusting)).unwrap();
    let batches = context
        .sql("SELECT id FROM b WHERE rsid = 'rs2'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        0,
        "pruning trusts the index, so the stale row hides the variant"
    );

    // Verifying: the same index is rejected when the table is opened.
    let error = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            verify_index_records: true,
            ..Default::default()
        },
    )
    .await
    .expect_err("verification must reject an index that describes other variants")
    .to_string();
    assert!(
        error.contains("RS identifier") && error.contains("does not describe this object"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn resolving_metadata_does_not_bridge_genotype_payloads() {
    // Resolving records reads a bounded prefix of each one. Coalescing those
    // prefixes under the payload gap budget would merge them straight across the
    // probability blocks in between, so a dense file would be downloaded almost
    // whole to answer a metadata query — the very thing moving the index open
    // ahead of the walk was meant to stop.
    let samples: Vec<String> = (0..3_000).map(|index| format!("s{index}")).collect();
    let names: Vec<&str> = samples.iter().map(String::as_str).collect();
    let variants: Vec<Variant> = (0..8)
        .map(|index| Variant {
            id: Box::leak(format!("v{index}").into_boxed_str()),
            rsid: Box::leak(format!("rs{index}").into_boxed_str()),
            chrom: "1",
            position: 10 + index as u32,
            alleles: vec!["A", "C"],
            phased: false,
            bits: 8,
            samples: (0..names.len()).map(sample_probabilities).collect(),
        })
        .collect();

    let dir = TempDir::new().unwrap();
    let bgen = dir.path().join("cohort.bgen");
    let bgi = dir.path().join("cohort.bgen.bgi");
    let (bytes, rows) = encode_layout2_with_samples(Codec::None, true, &variants, &names);
    fs::write(&bgen, bytes).unwrap();
    let fixture = Fixture {
        _dir: dir,
        bgen,
        bgi,
        rows,
    };
    create_bgi(&fixture, false);
    // Each record must be larger than the metadata probe, or there is no
    // payload between the prefixes for coalescing to bridge.
    assert!(
        fixture.rows[0].size > 2 * METADATA_PROBE_BYTES,
        "record size {} must exceed the probe",
        fixture.rows[0].size
    );

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

    let before = server.get_requests("cohort.bgen").len();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT chrom FROM b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        variants.len()
    );

    let during = &server.get_requests("cohort.bgen")[before..];
    for request in during {
        let (start, end) = request.range.expect("metadata reads must be ranged");
        let length = (end - start + 1) as u64;
        assert!(
            length <= 2 * METADATA_PROBE_BYTES,
            "a metadata read of {length} bytes bridged a genotype payload: {during:?}"
        );
    }
}

/// Bytes of each record the metadata probe reads before widening.
const METADATA_PROBE_BYTES: u64 = 4 * 1024;

#[tokio::test]
async fn a_layout2_variant_declaring_one_allele_is_rejected() {
    // Probability states are counts over a variant's alleles, so a single-allele
    // variant encodes one state per sample and carries no genotype. Layout 1
    // fixes the count at two; Layout 2 reads it from the record, so a malformed
    // one has to be rejected rather than decoded into degenerate values.
    let single_allele = vec![Variant {
        id: "v1",
        rsid: "rs1",
        chrom: "1",
        position: 10,
        alleles: vec!["A"],
        phased: false,
        bits: 8,
        samples: vec![
            sample(2, false, &[255]),
            sample(2, false, &[0]),
            sample(2, true, &[0]),
        ],
    }];
    let dir = TempDir::new().unwrap();
    let bgen = dir.path().join("cohort.bgen");
    fs::write(&bgen, encode_layout2(Codec::None, true, &single_allele).0).unwrap();

    let error = BgenTableProvider::try_new(path(&bgen), BgenReadOptions::default())
        .await
        .expect_err("a one-allele Layout 2 variant must be rejected")
        .to_string();
    assert!(
        error.contains("allele count 1 is outside supported range 2..="),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn planning_reads_each_record_once_when_a_query_filters_and_projects_it() {
    // A query that both filters on `id` and projects a variant column resolves
    // records twice over: once to filter the candidates exactly, once to supply
    // the projection. The second pass must reuse the first pass's records
    // rather than fetching them again.
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

    let before = server.get_requests("cohort.bgen").len();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT id FROM b WHERE id != 'missing'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
    let during = server.get_requests("cohort.bgen").len() - before;
    assert_eq!(
        during, 1,
        "the records resolved for the filter must serve the projection too"
    );
}

#[tokio::test]
async fn metadata_longer_than_the_probe_still_resolves() {
    // Resolving an indexed record reads a bounded prefix rather than the whole
    // record, so a variant whose identifier runs past that prefix exercises the
    // widening loop. It has to converge and return the real identifier.
    let long_id: &'static str = Box::leak("v".repeat(9_000).into_boxed_str());
    let mut variants = many_variants(3);
    variants[1].id = long_id;
    let fixture = fixture_with_variants(Codec::Zstd, true, &variants);
    create_bgi(&fixture, false);

    let provider = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT id FROM b ORDER BY start")
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
    assert_eq!(ids.value(1), long_id);
    assert_eq!(ids.value(0), "v0");
    assert_eq!(ids.value(2), "v2");
}

#[tokio::test]
async fn an_indexed_scan_matches_an_unindexed_one_column_for_column() {
    // With an index the catalog comes from the BGI, and the variant identifier,
    // the alleles past the second and the payload position are parsed from each
    // record as the scan reads it. Without one every field is parsed up front.
    // The two must be indistinguishable in the output, including for `v3`,
    // which has three alleles where the index records only two.
    let indexed = fixture(Codec::Zstd, true);
    create_bgi(&indexed, false);
    let unindexed = fixture(Codec::Zstd, true);
    assert!(!unindexed.bgi.exists(), "the second fixture has no index");

    for sql in [
        "SELECT chrom, start, \"end\", id, rsid, alleles FROM b ORDER BY start",
        "SELECT id FROM b WHERE id = 'v2'",
        "SELECT id, alleles FROM b WHERE rsid = 'rs3'",
        "SELECT chrom, start FROM b ORDER BY start",
        "SELECT rsid, phased, bits FROM b ORDER BY start",
        "SELECT genotypes FROM b WHERE rsid = 'rs2'",
        "SELECT id FROM b WHERE id IN ('v1', 'v3') ORDER BY start",
        "SELECT alleles FROM b WHERE start >= 0 ORDER BY start",
    ] {
        let mut rendered = Vec::new();
        for fixture in [&indexed, &unindexed] {
            let provider =
                BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
                    .await
                    .unwrap();
            let context = context(1024);
            context.register_table("b", Arc::new(provider)).unwrap();
            let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
            rendered.push(
                datafusion::arrow::util::pretty::pretty_format_batches(&batches)
                    .unwrap()
                    .to_string(),
            );
        }
        assert_eq!(rendered[0], rendered[1], "indexed vs unindexed for: {sql}");
    }
}

#[tokio::test]
async fn a_scan_rejects_an_index_that_describes_different_variants() {
    // Validating every index row against the object at open time is what made
    // opening a table read the whole file. The check now happens where each
    // record is read, so an index that identifies the right file but describes
    // the wrong variants has to fail the scan rather than emit its own values.
    let fixture = fixture(Codec::Zstd, true);
    create_bgi(&fixture, false);
    let connection = Connection::open(&fixture.bgi).unwrap();
    connection
        .execute(
            "UPDATE Variant SET rsid = 'rs-wrong' WHERE rsid = 'rs2'",
            [],
        )
        .unwrap();
    drop(connection);

    // Opening still succeeds: the file's size and first bytes are untouched, and
    // the row ranges still tile the object.
    let provider = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .expect("the index still identifies this object");

    // Including the columns the index carries itself: emitting those straight
    // from the index would hand back its stale values as though they were the
    // object's.
    for sql in [
        "SELECT id FROM b",
        "SELECT genotypes FROM b",
        "SELECT rsid FROM b",
        "SELECT chrom, start FROM b",
    ] {
        let context = context(1024);
        context
            .register_table("b", Arc::new(provider.clone()))
            .unwrap();
        let error = context
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .expect_err("a mismatched index must fail the scan")
            .to_string();
        assert!(
            error.contains("does not match the record"),
            "unexpected error for {sql}: {error}"
        );
    }
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

#[tokio::test]
async fn a_large_in_list_does_not_fail_index_lookup() {
    let fixture = fixture(Codec::Zlib, true);
    create_bgi(&fixture, false);
    let provider = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();

    // SQLite refuses to prepare a statement with more host parameters than its
    // compiled limit, so the index lookup must defer to catalog evaluation
    // rather than fail a query the catalog can answer.
    // Disjoint from the fixture's positions so the expected result is empty.
    let positions: Vec<String> = (1_000_000..1_002_000)
        .map(|value| value.to_string())
        .collect();
    let sql = format!(
        "SELECT rsid FROM b WHERE start IN ({})",
        positions.join(", ")
    );
    let batches = context
        .sql(&sql)
        .await
        .expect("planning a large IN list must succeed")
        .collect()
        .await
        .expect("executing a large IN list must succeed");
    // None of those positions exist in the fixture.
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        0
    );
}

#[tokio::test]
async fn fixed_probability_layout_matches_the_nested_layout() {
    // Every fixture variant stores a different number of states (unphased
    // biallelic, phased biallelic, multiallelic), so only one variant can share
    // a fixed width. Breadth comes from the cross-reader benchmark, which checks
    // 191 million cells of a real uniform-width file.
    let fixture = fixture(Codec::Zlib, true);
    let context = context(1024);

    let nested = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .unwrap();
    context.register_table("n", Arc::new(nested)).unwrap();
    let nested_rows = context
        .sql("SELECT genotypes FROM n WHERE rsid = 'rs1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let fixed = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // The fixed layout has to advertise the width in the schema.
    let schema = TableProvider::schema(&fixed);
    let genotypes = schema.field_with_name("genotypes").unwrap();
    assert!(
        format!("{:?}", genotypes.data_type()).contains("FixedSizeList"),
        "fixed layout must advertise a fixed-width sample list: {:?}",
        genotypes.data_type()
    );
    context.register_table("f", Arc::new(fixed)).unwrap();
    let fixed_rows = context
        .sql("SELECT genotypes FROM f WHERE rsid = 'rs1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Same values, different physical layout. The fixed layout pads every
    // sample to the schema width, so the nested vector is the prefix and the
    // remainder is padding.
    let nested = probability_values_any(&nested_rows[0], 0, 0);
    let fixed = probability_values_any(&fixed_rows[0], 0, 0);
    for sample in 0..3 {
        match (&nested[sample], &fixed[sample]) {
            (None, None) => {}
            (Some(nested), Some(fixed)) => {
                assert_eq!(
                    &fixed[..nested.len()],
                    nested.as_slice(),
                    "sample {sample} states differ between layouts"
                );
                assert!(
                    fixed[nested.len()..].iter().all(|value| value.is_nan()),
                    "sample {sample} padding must be NaN: {fixed:?}"
                );
            }
            (nested, fixed) => panic!("sample {sample} validity differs: {nested:?} {fixed:?}"),
        }
    }
}

#[tokio::test]
async fn the_fixed_width_covers_the_widest_catalog_variant() {
    // v1 is unphased biallelic diploid, so the probe reports ploidy 2 and
    // unphased. v3 is triallelic, and an unphased triallelic diploid sample
    // stores six states, so the schema has to be six wide rather than v1's
    // three — the width follows from the catalog's allele counts, not from
    // variant 0 alone.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let schema = TableProvider::schema(&provider);
    let genotypes = schema.field_with_name("genotypes").unwrap();
    let rendered = format!("{:?}", genotypes.data_type());
    assert!(
        rendered.contains("FixedSizeList"),
        "the fixed layout must advertise a fixed-width sample list: {rendered}"
    );
    assert!(
        rendered.contains(", 6)"),
        "the width must cover the widest catalog variant, not just variant 0: {rendered}"
    );
}

#[tokio::test]
async fn the_derived_fixed_width_respects_max_states_per_sample() {
    // The width covers the widest catalog variant, so it can exceed the
    // per-sample state limit even when variant 0 is well inside it. Every
    // selected sample is padded to that width whether or not the wide variant
    // is ever decoded, so a filter that excludes it would otherwise allocate
    // the padding without the limit ever being consulted.
    let fixture = fixture(Codec::Zlib, true);
    let error = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            // Variant 0 stores three states and passes; the triallelic variant
            // pushes the derived width to six.
            max_states_per_sample: 5,
            ..Default::default()
        },
    )
    .await
    .expect_err("a derived width above the limit must fail planning")
    .to_string();
    assert!(
        error.contains("max_states_per_sample") && error.contains("nested"),
        "{error}"
    );
}

#[tokio::test]
async fn fixed_probability_layout_pads_a_narrower_variant_with_nan() {
    // The fixture mixes widths on purpose: rs1 is unphased biallelic (three
    // states) and rs3 is triallelic (six for a diploid sample). The schema is
    // six wide and every narrower sample pads rather than being rejected.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("f", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM f WHERE rsid = 'rs1'")
        .await
        .unwrap()
        .collect()
        .await
        .expect("a mixed-width file is padded, not rejected");

    let samples = probability_values_any(&batches[0], 0, 0);
    let called = samples[0].as_ref().expect("sample 0 is called");
    assert_eq!(called.len(), 6, "every sample is the schema width");
    assert_eq!(&called[..3], &[1.0, 0.0, 0.0]);
    assert!(
        called[3..].iter().all(|value| value.is_nan()),
        "padding is NaN: {called:?}"
    );
    assert!(
        samples[2].is_none(),
        "the third sample is missing and stays null"
    );
}

#[tokio::test]
async fn fixed_probability_layout_pads_a_variable_ploidy_variant() {
    // rs3 declares ploidy 1..=2, so it has no single width and the old
    // per-variant check rejected it outright. Padding is decided per sample, so
    // its haploid samples pad to the schema width alongside its diploid one.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("f", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM f WHERE rsid = 'rs3'")
        .await
        .unwrap()
        .collect()
        .await
        .expect("a variable-ploidy variant pads per sample");
    let samples = probability_values_any(&batches[0], 0, 0);
    let haploid = samples[0].as_ref().expect("sample 0 is called");
    assert_eq!(haploid.len(), 6);
    assert!(
        haploid[3..].iter().all(|value| value.is_nan()),
        "a haploid triallelic sample stores three states and pads to six: {haploid:?}"
    );
}

#[tokio::test]
async fn fixed_probability_layout_reports_a_sample_wider_than_the_schema() {
    // The width is derived from variant 0's ploidy, so a later variant whose
    // samples are triploid stores four states where the schema has three.
    // Padding cannot represent that, and truncating would emit a distribution
    // that is not the file's.
    let dir = TempDir::new().unwrap();
    let bgen = dir.path().join("wider.bgen");
    let variants = vec![
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
                sample(2, false, &[0, 0]),
            ],
        },
        Variant {
            id: "v2",
            rsid: "rs2",
            chrom: "1",
            position: 20,
            alleles: vec!["A", "C"],
            phased: false,
            bits: 8,
            samples: vec![
                sample(3, false, &[255, 0, 0]),
                sample(3, false, &[0, 255, 0]),
                sample(3, false, &[0, 0, 255]),
            ],
        },
    ];
    let (bytes, _rows) = encode_layout2(Codec::Zlib, true, &variants);
    fs::write(&bgen, bytes).unwrap();

    let provider = BgenTableProvider::try_new(
        path(&bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let error = context
        .sql("SELECT genotypes FROM b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("fixed probability layout") && error.contains("nested layout"),
        "{error}"
    );
}

/// Reads probabilities from either the nested or the fixed sample layout.
fn probability_values_any(
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
    let read = |sample: usize, values: &dyn datafusion::arrow::array::Array| {
        let values = values.as_any().downcast_ref::<Float32Array>().unwrap();
        let _ = sample;
        (0..values.len()).map(|index| values.value(index)).collect()
    };
    if let Some(list) = samples.as_any().downcast_ref::<ListArray>() {
        (0..list.len())
            .map(|sample| {
                (!list.is_null(sample)).then(|| read(sample, list.value(sample).as_ref()))
            })
            .collect()
    } else {
        let list = samples
            .as_any()
            .downcast_ref::<datafusion::arrow::array::FixedSizeListArray>()
            .unwrap();
        (0..list.len())
            .map(|sample| {
                (!list.is_null(sample)).then(|| read(sample, list.value(sample).as_ref()))
            })
            .collect()
    }
}

#[tokio::test]
async fn fixed_probability_layout_allows_an_empty_sample_selection() {
    // Selecting no sample skips payload reads, so no variant reports a state
    // width. There is nothing to check against the schema width, and the scan
    // must not reject every row for it.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            samples: Some(Vec::new()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM b")
        .await
        .unwrap()
        .collect()
        .await
        .expect("an empty sample selection must not trip the width check");
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        fixture.rows.len()
    );
}

#[tokio::test]
async fn layout1_rejects_a_sample_that_does_not_sum_to_scale() {
    // Layout 1 scales a called sample's probabilities to 32768. A total inside
    // [1, 32766] is neither the missing sentinel nor a valid distribution, and
    // accepting it would emit probabilities that do not sum to one.
    let dir = TempDir::new().unwrap();
    let bgen = dir.path().join("undersummed.bgen");
    fs::write(
        &bgen,
        encode_layout1_with(Codec::None, [[16_000, 0, 0], [0, 0, 0], [0, 32_768, 0]]),
    )
    .unwrap();
    let provider = BgenTableProvider::try_new(path(&bgen), BgenReadOptions::default())
        .await
        .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let error = context
        .sql("SELECT genotypes FROM b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("32767..=32769"), "{error}");
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

#[tokio::test]
async fn the_metadata_limit_applies_inside_the_read_ahead_buffer() {
    // The metadata window reads ahead a megabyte at a time and serves whatever
    // it holds, so a record whose metadata exceeds max_variant_metadata_bytes
    // but fits in that buffer would parse without the limit ever being
    // consulted. The limit has to bind on the bytes handed to the parser, not
    // on the bytes fetched.
    let fixture = fixture(Codec::Zlib, true);
    let error = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            // Far below any real record, and far below the read-ahead buffer.
            max_variant_metadata_bytes: 8,
            ..Default::default()
        },
    )
    .await
    .expect_err("a record larger than the metadata limit must be rejected")
    .to_string();
    assert!(error.contains("max_variant_metadata_bytes"), "{error}");
}

#[tokio::test]
async fn compressed_bytes_counts_payloads_not_bridged_metadata() {
    // A coalesced range bridges the metadata between consecutive payloads.
    // Those bytes are downloaded, so they belong in PrimaryBytesRead, but they
    // are not compressed genotype data and must not inflate CompressedBytes.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .unwrap();
    let context = context(1024);
    let plan = provider
        .scan(&context.state(), Some(&vec![8]), &[], None)
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<BgenExec>().unwrap();
    // PrimaryBytesRead is already seeded with the header, catalog and probe
    // reads at planning, so only its growth during execution is the coalesced
    // range. Comparing against the total would pass even with the bug.
    let planned = exec.metrics_snapshot()[GenotypeMetric::PrimaryBytesRead as usize].1;
    collect(plan.clone(), context.task_ctx()).await.unwrap();
    let snapshot = exec.metrics_snapshot();
    let compressed = snapshot[GenotypeMetric::CompressedBytes as usize].1;
    let range_bytes = snapshot[GenotypeMetric::PrimaryBytesRead as usize].1 - planned;
    assert!(
        range_bytes > 0,
        "the scan should have read a coalesced range"
    );
    assert!(
        compressed < range_bytes,
        "compressed {compressed} must exclude the metadata bridged inside the \
         {range_bytes}-byte coalesced range"
    );
}

#[tokio::test]
async fn a_variant_cannot_reconstruct_more_than_the_block_byte_budget() {
    // Each sample can sit under max_states_per_sample while their sum decodes
    // to far more memory than the block occupies, because low bit precision
    // expands every stored state into an f32. The budget is checked before any
    // of the reconstruction is built.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            // The fixture's largest decompressed block is 19 bytes, so this
            // budget clears the existing block-size check; the reconstruction
            // it implies — three samples of three to six states as f32 — does
            // not fit, and only the new check can catch that.
            max_decompressed_block_bytes: 24,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let error = context
        .sql("SELECT genotypes FROM b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("reconstructing") && error.contains("probability states"),
        "the block-size check must not be what rejected this: {error}"
    );
}

#[tokio::test]
async fn the_reconstruction_budget_counts_fixed_layout_padding() {
    // A fixed layout pads every sample to the catalog-derived width, so what a
    // variant emits is that width rather than what it stores. Sizing the budget
    // from the variant's own states would let a scan filtered to a narrow
    // variant allocate the full padded width unchecked.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            probability_layout: BgenProbabilityLayout::Fixed,
            // rs1 stores three states per sample: 3 x 3 x 4 = 36 bytes, inside
            // this budget. Padded to the schema's six-state width it needs
            // 3 x 6 x 4 = 72, which is not.
            max_decompressed_block_bytes: 50,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("f", Arc::new(provider)).unwrap();
    let error = context
        .sql("SELECT genotypes FROM f WHERE rsid = 'rs1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("reconstructing") && error.contains("probability states"),
        "{error}"
    );
}

#[tokio::test]
async fn the_reconstruction_budget_applies_to_layout1() {
    // Layout 1 stores six bytes per sample and emits three f32 values, so its
    // block passes a budget its reconstruction does not. The bound is promised
    // for both layouts.
    let dir = TempDir::new().unwrap();
    let bgen = dir.path().join("budget.bgen");
    fs::write(
        &bgen,
        encode_layout1_with(
            Codec::None,
            [[32_768, 0, 0], [0, 32_768, 0], [0, 0, 32_768]],
        ),
    )
    .unwrap();
    let provider = BgenTableProvider::try_new(
        path(&bgen),
        BgenReadOptions {
            // The block is three samples x six bytes = 18, inside this budget;
            // the reconstruction is three samples x three states x four = 36.
            max_decompressed_block_bytes: 24,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let error = context
        .sql("SELECT genotypes FROM b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("reconstructing") && error.contains("probability states"),
        "{error}"
    );
}

#[tokio::test]
async fn the_reconstruction_budget_ignores_states_it_never_builds() {
    // The nested layout appends nothing for a sample with no called genotype,
    // so charging one its nominal width would reject a scan whose actual
    // reconstruction fits. rs1 has three samples, one of them missing: two
    // called samples of three states are 24 bytes, inside this budget, while
    // counting the missing one would make it 36 and fail.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(
        path(&fixture.bgen),
        BgenReadOptions {
            max_decompressed_block_bytes: 30,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM b WHERE rsid = 'rs1'")
        .await
        .unwrap()
        .collect()
        .await
        .expect("a missing sample builds no states and must not be charged for them");
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
}

#[tokio::test]
async fn the_layout1_budget_ignores_missing_samples() {
    // A Layout 1 sample whose three values are all zero is the missing
    // sentinel; the nested layout builds nothing for it, so charging it would
    // reject a scan whose reconstruction fits. Two called samples of three
    // states are 24 bytes, inside this budget; counting the third would make it
    // 36 and fail.
    let dir = TempDir::new().unwrap();
    let bgen = dir.path().join("missing.bgen");
    fs::write(
        &bgen,
        encode_layout1_with(Codec::None, [[32_768, 0, 0], [0, 32_768, 0], [0, 0, 0]]),
    )
    .unwrap();
    let provider = BgenTableProvider::try_new(
        path(&bgen),
        BgenReadOptions {
            max_decompressed_block_bytes: 30,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let context = context(1024);
    context.register_table("b", Arc::new(provider)).unwrap();
    let batches = context
        .sql("SELECT genotypes FROM b")
        .await
        .unwrap()
        .collect()
        .await
        .expect("a missing Layout 1 sample builds no states and must not be charged");
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
}

#[tokio::test]
async fn coalesced_ranges_is_a_planning_count() {
    // Coalescing is decided when the scan is planned, so the counter has to be
    // meaningful before any range is read. Incrementing it per read would only
    // restate RangeRequests, which is counted in the same loop.
    let fixture = fixture(Codec::Zlib, true);
    let provider = BgenTableProvider::try_new(path(&fixture.bgen), BgenReadOptions::default())
        .await
        .unwrap();
    let context = context(1024);
    let plan = provider
        .scan(&context.state(), Some(&vec![8]), &[], None)
        .await
        .unwrap();
    let exec = plan.as_any().downcast_ref::<BgenExec>().unwrap();
    let planned = exec.metrics_snapshot()[GenotypeMetric::CoalescedRanges as usize].1;
    assert!(
        planned > 0,
        "coalesced ranges must be counted at planning, before any range is read"
    );
    collect(plan.clone(), context.task_ctx()).await.unwrap();
    assert_eq!(
        exec.metrics_snapshot()[GenotypeMetric::CoalescedRanges as usize].1,
        planned,
        "executing the scan must not add to a planning counter"
    );
}
