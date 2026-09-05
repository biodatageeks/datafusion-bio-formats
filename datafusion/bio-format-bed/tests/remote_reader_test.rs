//! Exercise the real remote readers with a local, range-capable HTTP server.
use datafusion::arrow::array::{Array, Int64Array, StringArray, UInt32Array};
use datafusion::prelude::*;
use datafusion_bio_format_bed::storage::BedRemoteReader;
use datafusion_bio_format_bed::table_provider::{BEDFields, BedTableProvider};
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use flate2::{Compression, write::GzEncoder};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::Duration;

struct HttpFixture {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl HttpFixture {
    fn new(body: Vec<u8>, truncate: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = std::thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                let (mut socket, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("fixture server: {error}"),
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut request = String::new();
                if reader.read_line(&mut request).unwrap_or(0) == 0 {
                    continue;
                }
                let method = request.split_whitespace().next().unwrap().to_owned();
                let missing = request.contains("/missing");
                let mut range = None;
                loop {
                    let mut header = String::new();
                    if reader.read_line(&mut header).unwrap_or(0) == 0 || header == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = header.split_once(':')
                        && name.eq_ignore_ascii_case("range")
                    {
                        let (start, end) = value
                            .trim()
                            .strip_prefix("bytes=")
                            .unwrap()
                            .split_once('-')
                            .unwrap();
                        range = Some((start.parse::<usize>().unwrap(), end.parse::<usize>().ok()));
                    }
                }
                if missing {
                    let _ = write!(
                        socket,
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    continue;
                }
                if method == "HEAD" {
                    let _ = write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    continue;
                }
                let (status, selected, content_range) = if let Some((start, end)) =
                    range.filter(|_| !body.is_empty())
                {
                    if start >= body.len() {
                        let _ = write!(
                            socket,
                            "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        continue;
                    }
                    let end = end.unwrap_or(body.len() - 1).min(body.len() - 1);
                    (
                        "206 Partial Content",
                        &body[start..=end],
                        format!("Content-Range: bytes {start}-{end}/{}\r\n", body.len()),
                    )
                } else {
                    ("200 OK", body.as_slice(), String::new())
                };
                let _ = write!(
                    socket,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{content_range}Accept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                    selected.len()
                );
                let sent = if truncate {
                    &selected[..selected.len() / 2]
                } else {
                    selected
                };
                let _ = socket.write_all(sent);
            }
        });
        Self {
            address,
            stop,
            thread: Some(thread),
        }
    }

    fn url(&self) -> String {
        format!("http://{}/records.bed", self.address)
    }
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}

fn encoded(data: &[u8], compression: &str) -> Vec<u8> {
    match compression {
        "plain" => data.to_vec(),
        "gzip" => {
            let mut writer = GzEncoder::new(Vec::new(), Compression::default());
            writer.write_all(data).unwrap();
            writer.finish().unwrap()
        }
        "bgzf" => {
            let mut writer = noodles_bgzf::io::Writer::new(Vec::new());
            writer.write_all(data).unwrap();
            writer.finish().unwrap()
        }
        _ => unreachable!(),
    }
}

async fn context(
    url: String,
    mode: BEDFields,
    zero_based: bool,
    options: Option<ObjectStorageOptions>,
) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    ctx.register_table(
        "bed",
        Arc::new(BedTableProvider::new(url, mode, options, zero_based).unwrap()),
    )
    .unwrap();
    ctx
}

#[tokio::test]
async fn http_bed3_is_consistent_for_plain_gzip_and_bgzf() {
    for compression in ["plain", "gzip", "bgzf"] {
        for zero_based in [true, false] {
            let server = HttpFixture::new(
                encoded(
                    b"# comment\nchr1\t0\t0\r\nchr1\t4\t8\nchr1\t21\t29",
                    compression,
                ),
                false,
            );
            // None must select sensible remote defaults, never unwrap/panic.
            let ctx = context(server.url(), BEDFields::BED4, zero_based, None).await;
            let batches = ctx
                .sql("SELECT name, start, \"end\" FROM bed")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let starts: Vec<_> = batches
                .iter()
                .flat_map(|b| {
                    b.column(1)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            let offset = u32::from(!zero_based);
            assert_eq!(starts, [offset, 4 + offset, 21 + offset]);
            for batch in batches {
                assert_eq!(batch.column(0).null_count(), batch.num_rows());
            }
            let batches = ctx
                .sql("SELECT COUNT(*) FROM bed")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                3
            );
        }
    }
}

#[tokio::test]
async fn http_all_output_modes_and_optional_fields() {
    for compression in ["plain", "gzip", "bgzf"] {
        let server = HttpFixture::new(
            encoded(b"chr1\t0\t5\tname\t42\t+\t0\t5\t0\t1\t5\t0\n", compression),
            false,
        );
        for (mode, count) in [
            (BEDFields::BED3, 3),
            (BEDFields::BED4, 4),
            (BEDFields::BED5, 5),
            (BEDFields::BED6, 6),
        ] {
            let ctx = context(server.url(), mode, true, None).await;
            let batches = ctx
                .sql("SELECT * FROM bed")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(batches[0].num_columns(), count);
            assert_eq!(batches[0].num_rows(), 1);
            if count >= 4 {
                assert_eq!(
                    batches[0]
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0),
                    "name"
                );
            }
        }
    }
}

#[tokio::test]
async fn http_short_and_invalid_utf8_records_raise() {
    for compression in ["plain", "gzip", "bgzf"] {
        for invalid in [b"chr1\t5".as_slice(), b"chr1\t5\tbad", b"chr1\t5\t8\t\xff"] {
            let server = HttpFixture::new(
                encoded(
                    &[b"# header\nchr1\t0\t5\n".as_slice(), invalid].concat(),
                    compression,
                ),
                false,
            );
            let ctx = context(server.url(), BEDFields::BED4, true, None).await;
            let error = ctx
                .sql("SELECT COUNT(*) FROM bed")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("BED line 3"), "{compression}: {error}");
        }
    }
}

#[tokio::test]
async fn http_missing_object_and_truncated_response_are_errors() {
    let options = ObjectStorageOptions {
        max_retries: Some(0),
        timeout: Some(2),
        ..Default::default()
    };
    let server = HttpFixture::new(b"chr1\t0\t5\nchr1\t5\t8\n".to_vec(), false);
    assert!(
        BedRemoteReader::<3>::new(
            format!("http://{}/missing", server.address),
            options.clone()
        )
        .await
        .is_err()
    );
    let server = HttpFixture::new(b"chr1\t0\t5\nchr1\t5\t8\n".to_vec(), true);
    let ctx = context(server.url(), BEDFields::BED4, true, Some(options)).await;
    assert!(
        ctx.sql("SELECT * FROM bed")
            .await
            .unwrap()
            .collect()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn http_gzip_members_can_split_a_record() {
    let data = b"chr1\t0\t5\nchr1\t5\t8\nchr1\t8\t9\n";
    let bytes = data
        .chunks(4)
        .flat_map(|chunk| encoded(chunk, "gzip"))
        .collect();
    let server = HttpFixture::new(bytes, false);
    let ctx = context(server.url(), BEDFields::BED4, true, None).await;
    let batches = ctx
        .sql("SELECT COUNT(*) FROM bed")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}
