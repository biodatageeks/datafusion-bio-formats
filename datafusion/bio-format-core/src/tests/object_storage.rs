use crate::object_storage::{
    CompressionType, ObjectStorageOptions, RemoteObject, get_compression_type,
};
use futures::TryStreamExt;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

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

            let mut request = [0u8; 4096];
            let size = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..size]);
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

            let mut request = [0u8; 4096];
            let size = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..size]);
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
