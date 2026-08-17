use crate::object_storage::{
    CompressionType, ObjectStorageOptions, RemoteObject, StorageType, get_compression_type,
    get_storage_type,
};
use futures::TryStreamExt;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

/// Reads one complete HTTP request header from `stream`.
///
/// A TCP read is not an HTTP-message boundary, and a client can open a
/// connection without completing a request on it. Returning `None` for anything
/// incomplete lets a test server skip it; reading once and unwrapping instead
/// fails under a loaded test run, taking the server thread and its `join` with
/// it.
fn read_http_request(stream: &mut std::net::TcpStream) -> Option<String> {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0u8; 1024];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return None,
            Ok(size) if buffer.len() + size <= 8192 => {
                buffer.extend_from_slice(&chunk[..size]);
                if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                    return Some(String::from_utf8_lossy(&buffer).into_owned());
                }
            }
            Ok(_) => return None,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return None;
            }
            Err(error) => panic!("HTTP test server failed to read request: {error}"),
        }
    }
}

#[tokio::test]
async fn test_get_compression_type_gzip() {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(&[0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff])
        .unwrap();
    let path = format!("file://{}", file.path().to_str().unwrap());

    let compression_type = get_compression_type(path, None, ObjectStorageOptions::default()).await;
    assert_eq!(compression_type.unwrap(), CompressionType::GZIP);
}

#[tokio::test]
async fn test_get_compression_type_bgzf() {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(&[
        0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02,
        0x00, 0x00, 0x00,
    ])
    .unwrap();
    let path = format!("file://{}", file.path().to_str().unwrap());

    let compression_type = get_compression_type(path, None, ObjectStorageOptions::default()).await;
    assert_eq!(compression_type.unwrap(), CompressionType::BGZF);
}

#[tokio::test]
async fn test_get_compression_type_none() {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(b"this is not compressed").unwrap();
    let path = format!("file://{}", file.path().to_str().unwrap());

    let compression_type = get_compression_type(path, None, ObjectStorageOptions::default()).await;
    assert_eq!(compression_type.unwrap(), CompressionType::NONE);
}

#[tokio::test]
async fn test_get_compression_type_empty() {
    let file = NamedTempFile::new().unwrap();
    let path = format!("file://{}", file.path().to_str().unwrap());

    let compression_type = get_compression_type(path, None, ObjectStorageOptions::default()).await;
    assert_eq!(compression_type.unwrap(), CompressionType::NONE);
}

#[tokio::test]
async fn remote_http_object_supports_size_full_and_range_reads() {
    const BODY: &[u8] = b"0123456789abcdef";

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut handled = 0;
        while handled < 3 && Instant::now() < deadline {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("HTTP test server failed: {error}"),
            };

            // One read is not one HTTP request: see [`read_http_request`].
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let Some(request) = read_http_request(&mut stream) else {
                continue;
            };
            let is_head = request.starts_with("HEAD ");
            let range = request.lines().find_map(|line| {
                line.strip_prefix("Range: bytes=")
                    .or_else(|| line.strip_prefix("range: bytes="))
                    .and_then(|value| value.split_once('-'))
                    .map(|(start, end)| {
                        (
                            start.parse::<usize>().unwrap(),
                            end.parse::<usize>().unwrap(),
                        )
                    })
            });

            if is_head {
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                    BODY.len()
                )
                .unwrap();
            } else if let Some((start, end)) = range {
                let bytes = &BODY[start..=end];
                write!(
                    stream,
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nConnection: close\r\n\r\n",
                    bytes.len(),
                    start,
                    end,
                    BODY.len()
                )
                .unwrap();
                stream.write_all(bytes).unwrap();
            } else {
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    BODY.len()
                )
                .unwrap();
                stream.write_all(BODY).unwrap();
            }
            handled += 1;
        }
        assert_eq!(handled, 3, "HTTP test server timed out");
    });

    let object = RemoteObject::open(
        format!("http://{address}/fixture.bin"),
        ObjectStorageOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(object.size().await.unwrap(), BODY.len() as u64);
    assert_eq!(object.read_all().await.unwrap().as_ref(), BODY);
    assert_eq!(object.read_range(4..9).await.unwrap().as_ref(), b"45678");
    server.join().unwrap();
}

#[tokio::test]
async fn bounded_remote_range_stream_caps_each_request() {
    const BODY: &[u8] = b"0123456789abcdef";

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server_requests = Arc::clone(&requests);
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut handled = 0;
        while handled < 3 && Instant::now() < deadline {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("HTTP test server failed: {error}"),
            };

            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let Some(request) = read_http_request(&mut stream) else {
                continue;
            };
            assert!(request.starts_with("GET "));
            let (start, end) = request
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if !name.eq_ignore_ascii_case("range") {
                        return None;
                    }
                    let (start, end) = value.trim().strip_prefix("bytes=")?.split_once('-')?;
                    Some((
                        start.parse::<usize>().unwrap(),
                        end.parse::<usize>().unwrap(),
                    ))
                })
                .expect("bounded stream request must contain a byte range");
            server_requests.lock().unwrap().push((start, end));

            let bytes = &BODY[start..=end];
            write!(
                stream,
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes \
                 {}-{}/{}\r\nConnection: close\r\n\r\n",
                bytes.len(),
                start,
                end,
                BODY.len()
            )
            .unwrap();
            stream.write_all(bytes).unwrap();
            handled += 1;
        }
        assert_eq!(handled, 3, "HTTP test server timed out");
    });

    let object = RemoteObject::open(
        format!("http://{address}/fixture.bin"),
        ObjectStorageOptions::default(),
    )
    .await
    .unwrap();
    let chunks: Vec<bytes::Bytes> = object
        .stream_range_bounded(1..15, 5)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let actual = chunks.concat();

    server.join().unwrap();
    assert_eq!(actual, b"123456789abcde");
    assert_eq!(*requests.lock().unwrap(), [(1, 5), (6, 10), (11, 14)]);
}

#[test]
fn an_unrecognized_scheme_is_treated_as_a_local_path() {
    // A scheme this crate does not support arrives from whatever path a user
    // typed. Resolving it to a local path makes the failure a normal "cannot
    // open" error against that path; panicking would take the process down.
    assert!(StorageType::try_from_prefix("ftp").is_none());
    assert!(matches!(
        get_storage_type("ftp://example.test/reads.vcf".to_string()),
        StorageType::LOCAL
    ));
    // Recognized schemes are unaffected.
    for (prefix, expected) in [
        ("s3://bucket/key.vcf", StorageType::S3),
        ("gs://bucket/key.vcf", StorageType::GCS),
        ("file:///tmp/key.vcf", StorageType::LOCAL),
    ] {
        assert!(
            std::mem::discriminant(&get_storage_type(prefix.to_string()))
                == std::mem::discriminant(&expected),
            "{prefix}"
        );
    }
}

#[test]
fn an_unrecognized_compression_name_is_reported_not_guessed() {
    assert!(CompressionType::try_from_string("zstd").is_none());
    assert_eq!(
        CompressionType::try_from_string("BGZ"),
        Some(CompressionType::BGZF)
    );
}

#[tokio::test]
async fn an_unreadable_remote_object_fails_compression_detection() {
    // Reporting an unreachable object as uncompressed would send the caller on
    // to open a BGZF file as plain text, so the real failure surfaces later as a
    // parse error against the wrong reader.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let error = get_compression_type(
        format!("http://{address}/missing.vcf.gz"),
        None,
        ObjectStorageOptions {
            max_retries: Some(0),
            timeout: Some(2),
            ..Default::default()
        },
    )
    .await
    .expect_err("an unreachable object must not be reported as uncompressed");
    assert!(
        error.to_string().contains("detect its compression"),
        "unexpected error: {error}"
    );
}
