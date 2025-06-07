use std::io;
use std::slice::SliceIndex;
use std::sync::Arc;
use bytes::Bytes;
use datafusion::arrow::datatypes::DataType::Int64;
use tokio_util::io::{StreamReader, SyncIoBridge};
use noodles_bgzf as bgzf;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::ObjectStore;
use object_store::path::Path;
use datafusion::error::Result;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use futures::{FutureExt, StreamExt, TryStreamExt};
// use noodles_vcf as vcf;
use object_store::buffered::BufReader;
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeek};

// use opendal::Result;
use opendal::layers::LoggingLayer;
use opendal::services;
use opendal::Operator;
use opendal::services::Gcs;
use noodles::{bam, sam, vcf};
use log::{info, log};
use opendal::services::S3;

// const BUCKET: &str = "gcp-public-data--gnomad";
const BUCKET: &str = "gnomad-public-us-east-1";
const NAME: &str = "release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz";
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let builder = S3::default()
        .region("us-east-1")
        .bucket(BUCKET)
        .disable_ec2_metadata()
        .allow_anonymous();

    let operator = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish();

    let stream = operator.reader_with(NAME)
        .chunk(16 * 1024 * 1024)
        .concurrent(2)
        .await?.into_bytes_stream(..).await?;
    // let stream = operator.reader_with(NAME)
    //     .concurrent(8)
    //     .await?.into_bytes_stream(..).await?;
    let inner = bgzf::r#async::Reader::new(StreamReader::new(stream));
    let mut reader = vcf::r#async::io::Reader::new(inner);
    //
    info!("Reading header");
    let mut  count = 0;
    loop {
        let record = reader.records().next().await;
        if record.is_none() {
            break;
        };
        count += 1;
        if count % 100000 == 0 {
            info!("Number of records: {}", count);
        }
    }
    println!("Number of records: {}", count);
    Ok(())
}


let builder = Gcs::default()
    .bucket(BUCKET)
    .disable_vm_metadata()
    .allow_anonymous();
//
// let operator = Operator::new(builder)?.finish();
//
// let stream = operator.reader(NAME).await?.into_bytes_stream(..).await?;
// let inner = StreamReader::new(stream);




// let stdout = io::stdout();
// let mut writer = sam::r#async::io::Writer::new(stdout);
//
// // writer.write_header(&header).await?;

// let mut records = reader.records();
// let mut builder = services::Gcs::default();
// builder.bucket("gcp-public-data--gnomad");
// let op = Operator::new(builder).unwrap()
//     // Init with logging layer enabled.
//     .layer(LoggingLayer::default())
//     .finish();

// let reader = op.blocking().reader("release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz").unwrap().into_std_read(0..u64::MAX);
// let gcs = GoogleCloudStorageBuilder::default().with_bucket_name("gcp-public-data--gnomad").build().unwrap();
// let path = Path::from("release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz");
// let result = gcs.get(&path).await?;
// let reader = object_store::buffered::BufReader::with_capacity(Arc::new(gcs), &result.meta, 1024);
// let mut out = Vec::with_capacity(1024);
// let read = reader.read(&mut out).await.unwrap();
// println!("{}", result.meta.size);
// let byte_stream = result.into_stream();
// let num_zeros = byte_stream
//     .try_fold(0, |acc, bytes| async move {
//         Ok(acc + bytes.iter().filter(|b| **b == 0).count())
//     }).await.unwrap();

// println!("Num zeros in {} is {}", path, num_zeros);
// let async_reader = StreamReader::new(reader);
// let buf = async_reader.read(&mut [0; 1024]).await?;
// println!("{:?}", buf);

// let mut blocking_reader = SyncIoBridge::new(async_reader);
// let bgzf_reader = bgzf::Reader::new(&mut blocking_reader);
// let mut vcf_reader = vcf::io::Reader::new(bgzf_reader);

// let mut vcf_reader = vcf::io::Reader::new(reader);
// let header = vcf_reader.read_header().await?;

// let mut records = vcf_reader.record_bufs(&header);
// let mut raw_header = String::new();
// header_reader.read_to_string(&mut raw_header).await?;
// println!("{:}", raw_header);
// let mut reader = bgzf::MultithreadedReader::new(stream);
// let _inner = reader.get_mut();