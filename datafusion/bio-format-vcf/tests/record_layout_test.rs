//! A record's own INFO and FORMAT key order must survive a read/write round trip.
//!
//! Neither is recoverable from the typed columns: every record carries every key
//! the header declares, in schema order, and a FORMAT key whose value is missing
//! in every sample is null exactly like a key the record never had.

use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use datafusion_bio_format_core::metadata::{VCF_FORMAT_KEYS_COLUMN, VCF_INFO_KEYS_COLUMN};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use noodles_bcf as bcf;
use noodles_vcf as vcf;
use noodles_vcf::variant::io::Write as _;
use std::fs::File;
use std::sync::Arc;
use tokio::fs;

/// INFO is written in an order the header does not declare (`AF` before `DP`),
/// differently per record; `PS` is missing in every sample of the first record
/// and present in the second, where it also follows `DP` instead of preceding it.
const LAYOUT_VCF: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total read depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=PS,Number=1,Type=Integer,Description="Phase set">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Sample read depth">
##contig=<ID=chr1,length=1000>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
chr1	100	rs1	A	T	60	PASS	AF=0.25;DB;DP=50	GT:PS:DP	0/1:.:20	1/1:.:30
chr1	200	rs2	G	C	80	PASS	DP=60;AF=0.1	GT:DP:PS	0/0:25:1	0/1:35:1
"#;

/// The same layout in a single-sample file, which takes the flat FORMAT column
/// path in the serializer rather than the nested `genotypes` one.
const SINGLE_SAMPLE_LAYOUT_VCF: &str = r#"##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total read depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=PS,Number=1,Type=Integer,Description="Phase set">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Sample read depth">
##contig=<ID=chr1,length=1000>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	HG002
chr1	100	rs1	A	T	60	PASS	AF=0.25;DP=50	GT:PS:DP	0/1:.:20
chr1	200	rs2	G	C	80	PASS	DP=60;AF=0.1	GT:DP:PS	0/0:25:1
"#;

async fn write_fixture(name: &str, content: &str) -> String {
    let path = std::env::temp_dir().join(format!("test_record_layout_{name}.vcf"));
    fs::write(&path, content).await.unwrap();
    path.to_string_lossy().into_owned()
}

/// Reads `input` with the layout carried and writes it straight back out.
async fn round_trip(name: &str, content: &str, carry_layout: bool) -> String {
    let input_path = write_fixture(name, content).await;
    let output_path = std::env::temp_dir()
        .join(format!("test_record_layout_{name}_out.vcf"))
        .to_string_lossy()
        .into_owned();

    let source = VcfTableProvider::new(input_path.clone(), None, None, None, true).unwrap();
    let source = if carry_layout {
        source.with_record_layout().unwrap()
    } else {
        source
    };
    let schema = source.schema();
    let info_fields = vec!["DP".to_string(), "AF".to_string(), "DB".to_string()];
    let format_fields = vec!["GT".to_string(), "PS".to_string(), "DP".to_string()];
    let sample_names: Vec<String> = content
        .lines()
        .find(|line| line.starts_with("#CHROM"))
        .unwrap()
        .split('\t')
        .skip(9)
        .map(str::to_string)
        .collect();

    let ctx = SessionContext::new();
    ctx.register_table("source", Arc::new(source)).unwrap();
    ctx.register_table(
        "dest",
        Arc::new(VcfTableProvider::new_for_write(
            output_path.clone(),
            schema,
            info_fields,
            format_fields,
            sample_names,
            true,
        )),
    )
    .unwrap();
    ctx.sql("INSERT OVERWRITE dest SELECT * FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let written = fs::read_to_string(&output_path).await.unwrap();
    let _ = fs::remove_file(&input_path).await;
    let _ = fs::remove_file(&output_path).await;
    written
}

fn body(content: &str) -> Vec<&str> {
    content
        .lines()
        .filter(|line| !line.starts_with('#'))
        .collect()
}

#[tokio::test]
async fn a_carried_layout_round_trips_a_multisample_record_verbatim() {
    let written = round_trip("multisample", LAYOUT_VCF, true).await;
    assert_eq!(body(&written), body(LAYOUT_VCF));
}

#[tokio::test]
async fn a_carried_layout_round_trips_a_single_sample_record_verbatim() {
    let written = round_trip("single", SINGLE_SAMPLE_LAYOUT_VCF, true).await;
    assert_eq!(body(&written), body(SINGLE_SAMPLE_LAYOUT_VCF));
}

/// What the carry exists to fix. Without it the writer has only the typed
/// columns: INFO comes back in schema order, and `PS` — null in every sample of
/// the first record — is dropped as if the record never had it.
#[tokio::test]
async fn without_the_carry_the_layout_is_lost() {
    let written = round_trip("lossy", LAYOUT_VCF, false).await;
    let lines = body(&written);
    assert_eq!(
        lines[0],
        "chr1\t100\trs1\tA\tT\t60\tPASS\tDP=50;AF=0.25;DB\tGT:DP\t0/1:20\t1/1:30"
    );
    assert_eq!(
        lines[1],
        "chr1\t200\trs2\tG\tC\t80\tPASS\tDP=60;AF=0.1\tGT:PS:DP\t0/0:1:25\t0/1:1:35"
    );
}

#[tokio::test]
async fn the_layout_columns_are_absent_unless_asked_for() {
    let path = write_fixture("schema_off", LAYOUT_VCF).await;
    let provider = VcfTableProvider::new(path.clone(), None, None, None, true).unwrap();
    let schema = provider.schema();
    assert!(schema.index_of(VCF_INFO_KEYS_COLUMN).is_err());
    assert!(schema.index_of(VCF_FORMAT_KEYS_COLUMN).is_err());
    let _ = fs::remove_file(&path).await;
}

#[tokio::test]
async fn the_layout_columns_are_the_last_two_when_asked_for() {
    let path = write_fixture("schema_on", LAYOUT_VCF).await;
    let provider = VcfTableProvider::new(path.clone(), None, None, None, true)
        .unwrap()
        .with_record_layout()
        .unwrap();
    let schema = provider.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        &names[names.len() - 2..],
        &[VCF_INFO_KEYS_COLUMN, VCF_FORMAT_KEYS_COLUMN]
    );
    let _ = fs::remove_file(&path).await;
}

/// The columns must survive a projection that names them, which is how an
/// annotation pipeline forwards them from its input scan to its VCF sink.
#[tokio::test]
async fn the_layout_columns_can_be_selected_on_their_own() {
    let path = write_fixture("projected", LAYOUT_VCF).await;
    let ctx = SessionContext::new();
    ctx.register_table(
        "source",
        Arc::new(
            VcfTableProvider::new(path.clone(), None, None, None, true)
                .unwrap()
                .with_record_layout()
                .unwrap(),
        ),
    )
    .unwrap();

    let batches = ctx
        .sql(&format!(
            "SELECT \"{VCF_INFO_KEYS_COLUMN}\", \"{VCF_FORMAT_KEYS_COLUMN}\" FROM source ORDER BY 1"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows: Vec<(String, String)> = batches
        .iter()
        .flat_map(|batch| {
            let info = datafusion::arrow::array::as_string_array(batch.column(0));
            let format = datafusion::arrow::array::as_string_array(batch.column(1));
            (0..batch.num_rows())
                .map(|i| (info.value(i).to_string(), format.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect();

    assert_eq!(
        rows,
        vec![
            ("AF;DB;DP".to_string(), "GT:PS:DP".to_string()),
            ("DP;AF".to_string(), "GT:DP:PS".to_string()),
        ]
    );
    let _ = fs::remove_file(&path).await;
}

/// Reads `sql` from `path` with the layout carried and returns the carried
/// (INFO keys, FORMAT keys) per record.
async fn carried_keys(path: &str, predicate: &str) -> Vec<(String, String)> {
    let ctx = SessionContext::new();
    ctx.register_table(
        "source",
        Arc::new(
            VcfTableProvider::new(path.to_string(), None, None, None, true)
                .unwrap()
                .with_record_layout()
                .unwrap(),
        ),
    )
    .unwrap();
    let batches = ctx
        .sql(&format!(
            "SELECT start, \"{VCF_INFO_KEYS_COLUMN}\", \"{VCF_FORMAT_KEYS_COLUMN}\" \
             FROM source {predicate} ORDER BY start"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let info = datafusion::arrow::array::as_string_array(batch.column(1));
            let format = datafusion::arrow::array::as_string_array(batch.column(2));
            (0..batch.num_rows())
                .map(|i| (info.value(i).to_string(), format.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn expected_keys() -> Vec<(String, String)> {
    vec![
        ("AF;DB;DP".to_string(), "GT:PS:DP".to_string()),
        ("DP;AF".to_string(), "GT:DP:PS".to_string()),
    ]
}

/// The reader has four record loops — plain/BGZF, GZIP, indexed and remote —
/// and each has to carry the layout independently. The round trips above cover
/// the plain/BGZF one; these two cover GZIP and indexed. The remote loop is not
/// reachable from a local fixture.
#[tokio::test]
async fn a_gzip_scan_carries_the_layout() {
    assert_eq!(
        carried_keys("tests/data/record_layout.plain.vcf.gz", "").await,
        expected_keys()
    );
}

#[tokio::test]
async fn an_indexed_scan_carries_the_layout() {
    // A chrom predicate takes the TBI-driven path, which reads records through
    // a query iterator rather than a sequential scan.
    assert_eq!(
        carried_keys("tests/data/record_layout.vcf.gz", "WHERE chrom = 'chr1'").await,
        expected_keys()
    );
}

/// Asking twice must not append the columns twice: the reader locates them by
/// name, and a duplicate name makes that lookup ambiguous.
#[tokio::test]
async fn asking_for_the_layout_twice_is_a_no_op() {
    let path = write_fixture("idempotent", LAYOUT_VCF).await;
    let once = VcfTableProvider::new(path.clone(), None, None, None, true)
        .unwrap()
        .with_record_layout()
        .unwrap();
    let once_fields = once.schema().fields().len();
    let twice = once.with_record_layout().unwrap();

    assert_eq!(twice.schema().fields().len(), once_fields);
    for column in [VCF_INFO_KEYS_COLUMN, VCF_FORMAT_KEYS_COLUMN] {
        let matches = twice
            .schema()
            .fields()
            .iter()
            .filter(|field| field.name() == column)
            .count();
        assert_eq!(matches, 1, "{column} must appear exactly once");
    }
    let _ = fs::remove_file(&path).await;
}

/// A BCF record has no source text, so there is no layout to carry. Refusing is
/// the point: silently carrying nothing would hand the writer empty key lists
/// and drop every INFO field from the output.
#[tokio::test]
async fn a_bcf_source_is_refused_rather_than_carrying_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let vcf_path = dir.path().join("source.vcf");
    let bcf_path = dir.path().join("source.bcf");
    std::fs::write(&vcf_path, LAYOUT_VCF).unwrap();

    let mut reader = vcf::io::reader::Builder::default()
        .build_from_path(&vcf_path)
        .unwrap();
    let header = reader.read_header().unwrap();
    let mut writer = bcf::io::Writer::new(File::create(&bcf_path).unwrap());
    writer.write_header(&header).unwrap();
    for result in reader.records() {
        writer
            .write_variant_record(&header, &result.unwrap())
            .unwrap();
    }
    writer.try_finish().unwrap();

    let provider = VcfTableProvider::new(
        bcf_path.to_string_lossy().into_owned(),
        None,
        None,
        None,
        true,
    )
    .unwrap();
    let error = provider
        .with_record_layout()
        .expect_err("BCF input must be refused")
        .to_string();
    assert!(
        error.contains("only for text VCF input"),
        "unexpected error: {error}"
    );
}

/// A source VCF is free to declare an INFO field with any ID, including the
/// names the carry reserves. Appending a second column with the same name would
/// make every by-name lookup ambiguous — the reader would overwrite the source
/// field with key lists, and the writer would read key lists out of source
/// data. Refuse instead.
#[tokio::test]
async fn a_source_field_colliding_with_a_layout_column_is_refused() {
    let colliding = LAYOUT_VCF.replace(
        r#"##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">"#,
        &format!(
            r#"##INFO=<ID={VCF_INFO_KEYS_COLUMN},Number=1,Type=String,Description="collides">"#
        ),
    );
    let colliding = colliding.replace(";DB;", ";");
    let path = write_fixture("collision", &colliding).await;

    let error = VcfTableProvider::new(path.clone(), None, None, None, true)
        .unwrap()
        .with_record_layout()
        .expect_err("a colliding source field must be refused")
        .to_string();
    assert!(
        error.contains(VCF_INFO_KEYS_COLUMN),
        "unexpected error: {error}"
    );
    let _ = fs::remove_file(&path).await;
}

/// The dangerous shape of the same collision: a source declaring *both*
/// reserved names looks exactly like a schema that already carries the layout,
/// so the provider would report success and then hand the reader two source
/// INFO columns to overwrite with key lists.
#[tokio::test]
async fn a_source_declaring_both_layout_names_is_refused_too() {
    let colliding = LAYOUT_VCF
        .replace(
            r#"##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">"#,
            &format!(
                "{}\n{}",
                format_args!(
                    r#"##INFO=<ID={VCF_INFO_KEYS_COLUMN},Number=1,Type=String,Description="collides">"#
                ),
                format_args!(
                    r#"##INFO=<ID={VCF_FORMAT_KEYS_COLUMN},Number=1,Type=String,Description="collides">"#
                ),
            ),
        )
        .replace(";DB;", ";");
    let path = write_fixture("collision_both", &colliding).await;

    let error = VcfTableProvider::new(path.clone(), None, None, None, true)
        .unwrap()
        .with_record_layout()
        .expect_err("a colliding source must be refused, not silently accepted")
        .to_string();
    assert!(
        error.contains(VCF_INFO_KEYS_COLUMN) || error.contains(VCF_FORMAT_KEYS_COLUMN),
        "unexpected error: {error}"
    );
    let _ = fs::remove_file(&path).await;
}
