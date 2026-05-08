use datafusion::catalog::TableProvider;
use datafusion_bio_format_vcf::zarr::metadata::VcfZarrMetadata;
use datafusion_bio_format_vcf::zarr::{
    VcfZarrReadOptions, VcfZarrTableProvider, metadata::SUPPORTED_VCF_ZARR_VERSION,
};

fn write_v2_root_metadata(root: &std::path::Path, attributes: &str) {
    std::fs::create_dir_all(root).expect("store root should be created");
    std::fs::write(root.join(".zgroup"), "{\"zarr_format\":2}")
        .expect("root group metadata should be written");
    std::fs::write(root.join(".zattrs"), attributes).expect("root attributes should be written");
}

#[test]
fn vcf_zarr_rejects_missing_store() {
    let result = VcfZarrTableProvider::new(
        "/definitely/not/a/store.vcz".to_string(),
        VcfZarrReadOptions::default(),
    );

    let message = result.expect_err("missing store must fail").to_string();
    assert!(
        message.contains("VCF Zarr") && message.contains("not found"),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_requires_version_0_4() {
    let fixture = "tests/data/vcf_zarr/unsupported_version.vcz";
    let result = VcfZarrTableProvider::new(fixture.to_string(), VcfZarrReadOptions::default());

    let message = result
        .expect_err("unsupported vcf_zarr_version must fail")
        .to_string();
    assert!(
        message.contains("unsupported vcf_zarr_version")
            && message.contains("999.0")
            && message.contains("0.4"),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_accepts_version_0_4_fixture() {
    let fixture = "tests/data/vcf_zarr/multi_chrom.vcz";
    let provider = VcfZarrTableProvider::new(fixture.to_string(), VcfZarrReadOptions::default())
        .expect("supported fixture should open");

    assert_eq!(
        provider
            .schema()
            .metadata()
            .get("bio.vcf.zarr.version")
            .map(String::as_str),
        Some(SUPPORTED_VCF_ZARR_VERSION)
    );
}

#[test]
fn vcf_zarr_array_exists_checks_store_relative_array_paths() {
    let fixture = "tests/data/vcf_zarr/multi_chrom.vcz";
    let metadata = VcfZarrMetadata::open_local(fixture).expect("fixture should open");
    let absolute_array_path = std::fs::canonicalize(format!("{fixture}/variant_position"))
        .expect("fixture array should canonicalize");

    assert!(metadata.array_exists("variant_position"));
    assert!(!metadata.array_exists("missing_array"));
    assert!(!metadata.array_exists(""));
    assert!(!metadata.array_exists("."));
    assert!(!metadata.array_exists("../multi_chrom.vcz/variant_position"));
    assert!(!metadata.array_exists(&absolute_array_path.to_string_lossy()));
}

#[test]
fn vcf_zarr_array_exists_requires_zarray_file() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("store.vcz");
    let array = root.join("array");

    std::fs::create_dir_all(array.join(".zarray")).expect("array metadata dir should be created");
    write_v2_root_metadata(
        &root,
        &format!("{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\"}}"),
    );

    let metadata = VcfZarrMetadata::open_local(root.to_str().expect("temp path should be UTF-8"))
        .expect("temp store should open");

    assert!(!metadata.array_exists("array"));
}

#[test]
fn vcf_zarr_reports_malformed_root_metadata() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("malformed.vcz");

    write_v2_root_metadata(&root, "{}");
    std::fs::write(root.join(".zattrs"), "{not json")
        .expect("malformed root attributes should be written");

    let result = VcfZarrMetadata::open_local(root.to_str().expect("temp path should be UTF-8"));
    let message = result
        .expect_err("malformed root metadata must fail")
        .to_string();

    assert!(
        message.contains("Failed to read VCF Zarr root metadata")
            && message.contains(&root.display().to_string()),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_v2_root_attributes_are_authoritative() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("mixed_metadata.vcz");

    write_v2_root_metadata(
        &root,
        &format!("{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\"}}"),
    );
    std::fs::write(
        root.join("zarr.json"),
        r#"{"zarr_format":3,"node_type":"group","attributes":{"vcf_zarr_version":"999.0"}}"#,
    )
    .expect("conflicting v3 group metadata should be written");

    let metadata = VcfZarrMetadata::open_local(root.to_str().expect("temp path should be UTF-8"))
        .expect("temp store should open from v2 metadata");

    assert_eq!(metadata.vcf_zarr_version, SUPPORTED_VCF_ZARR_VERSION);
    assert_eq!(
        metadata
            .root_attributes
            .get("vcf_zarr_version")
            .and_then(|value| value.as_str()),
        Some(SUPPORTED_VCF_ZARR_VERSION)
    );
}

#[test]
fn vcf_zarr_requires_string_vcf_zarr_version_attribute() {
    for attributes in ["{}", "{\"vcf_zarr_version\":0.4}"] {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let root = temp_dir.path().join("bad_version.vcz");
        write_v2_root_metadata(&root, attributes);

        let message =
            VcfZarrMetadata::open_local(root.to_str().expect("temp path should be UTF-8"))
                .expect_err("missing or non-string version must fail")
                .to_string();

        assert!(
            message.contains("vcf_zarr_version") && message.contains("missing or not a string"),
            "unexpected error: {message}"
        );
    }
}
