use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::catalog::TableProvider;
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::metadata::{
    VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY, VCF_FIELD_NUMBER_KEY, VCF_FIELD_TYPE_KEY,
    VCF_FILE_FORMAT_KEY, VCF_SAMPLE_NAMES_KEY, from_json_string,
};
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

fn write_zarray_metadata(root: &std::path::Path, array_name: &str) {
    let array_path = root.join(array_name);
    std::fs::create_dir_all(&array_path).expect("array directory should be created");
    std::fs::write(array_path.join(".zarray"), "{}").expect("array metadata should be written");
}

fn write_required_zarray_metadata(root: &std::path::Path) {
    for array_name in [
        "variant_contig",
        "variant_position",
        "contig_id",
        "variant_allele",
    ] {
        write_zarray_metadata(root, array_name);
    }
}

fn open_multi_chrom_provider(options: VcfZarrReadOptions) -> VcfZarrTableProvider {
    VcfZarrTableProvider::new("tests/data/vcf_zarr/multi_chrom.vcz".to_string(), options)
        .expect("fixture should open")
}

fn assert_field(field: &Field, name: &str, data_type: &DataType, nullable: bool) {
    assert_eq!(field.name(), name);
    assert_eq!(field.data_type(), data_type);
    assert_eq!(
        field.is_nullable(),
        nullable,
        "{name} nullability should be {nullable}"
    );
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
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());

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

#[test]
fn vcf_zarr_requires_core_raw_arrays_for_logical_schema() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("missing_array.vcz");
    write_v2_root_metadata(
        &root,
        &format!("{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\"}}"),
    );
    write_zarray_metadata(&root, "variant_contig");
    write_zarray_metadata(&root, "variant_position");
    write_zarray_metadata(&root, "contig_id");

    let message = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect_err("missing variant_allele array must fail")
    .to_string();

    assert!(
        message.contains("missing required array") && message.contains("variant_allele"),
        "unexpected error: {message}"
    );
}

#[test]
fn vcf_zarr_schema_exposes_logical_vcf_columns() {
    let provider = open_multi_chrom_provider(VcfZarrReadOptions::default());

    let schema = provider.schema();

    let expected = [
        ("chrom", DataType::Utf8, false),
        ("start", DataType::UInt32, false),
        ("end", DataType::UInt32, false),
        ("id", DataType::Utf8, true),
        ("ref", DataType::Utf8, false),
        ("alt", DataType::Utf8, false),
        ("qual", DataType::Float64, true),
        ("filter", DataType::Utf8, true),
    ];

    assert_eq!(schema.fields().len(), expected.len());
    for (index, (name, data_type, nullable)) in expected.iter().enumerate() {
        assert_field(schema.field(index), name, data_type, *nullable);
    }
}

#[test]
fn vcf_zarr_schema_exposes_requested_info_fields() {
    let provider = open_multi_chrom_provider(VcfZarrReadOptions {
        info_fields: Some(vec!["DP".to_string()]),
        ..Default::default()
    });

    let schema = provider.schema();
    let dp = schema.field(schema.index_of("DP").expect("DP field should exist"));

    assert_field(dp, "DP", &DataType::Utf8, true);
    assert_eq!(
        dp.metadata()
            .get(VCF_FIELD_FIELD_TYPE_KEY)
            .map(String::as_str),
        Some("INFO")
    );
    assert_eq!(
        dp.metadata().get(VCF_FIELD_TYPE_KEY).map(String::as_str),
        Some("String")
    );
    assert_eq!(
        dp.metadata().get(VCF_FIELD_NUMBER_KEY).map(String::as_str),
        Some(".")
    );
}

#[test]
fn vcf_zarr_schema_exposes_requested_format_fields() {
    let provider = open_multi_chrom_provider(VcfZarrReadOptions {
        format_fields: Some(vec!["GT".to_string(), "DP".to_string()]),
        ..Default::default()
    });

    let schema = provider.schema();
    let genotypes = schema.field(
        schema
            .index_of("genotypes")
            .expect("genotypes field should exist"),
    );
    assert!(genotypes.is_nullable(), "genotypes should be nullable");

    let DataType::Struct(children) = genotypes.data_type() else {
        panic!(
            "genotypes should be Struct, got {:?}",
            genotypes.data_type()
        );
    };
    assert_eq!(children.len(), 2);

    for (index, expected_name) in ["GT", "DP"].iter().enumerate() {
        let child = &children[index];
        assert_eq!(child.name(), expected_name);
        assert!(child.is_nullable(), "{expected_name} should be nullable");

        let DataType::List(item) = child.data_type() else {
            panic!(
                "{expected_name} should be List<Utf8>, got {:?}",
                child.data_type()
            );
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
        assert!(
            item.is_nullable(),
            "{expected_name} item should be nullable"
        );

        assert_eq!(
            child
                .metadata()
                .get(VCF_FIELD_FIELD_TYPE_KEY)
                .map(String::as_str),
            Some("FORMAT")
        );
        assert_eq!(
            child
                .metadata()
                .get(VCF_FIELD_FORMAT_ID_KEY)
                .map(String::as_str),
            Some(*expected_name)
        );
        assert_eq!(
            child.metadata().get(VCF_FIELD_TYPE_KEY).map(String::as_str),
            Some("String")
        );
        assert_eq!(
            child
                .metadata()
                .get(VCF_FIELD_NUMBER_KEY)
                .map(String::as_str),
            Some(".")
        );
    }
}

#[test]
fn vcf_zarr_schema_omits_genotypes_for_empty_format_fields() {
    let provider = open_multi_chrom_provider(VcfZarrReadOptions {
        format_fields: Some(vec![]),
        ..Default::default()
    });

    assert!(provider.schema().index_of("genotypes").is_err());
}

#[test]
fn vcf_zarr_schema_metadata_uses_zarr_and_vcf_metadata() {
    let options = VcfZarrReadOptions::default();
    let expected_coordinate_system = options.coordinate_system_zero_based.to_string();
    let provider = open_multi_chrom_provider(options);
    let schema = provider.schema();
    let metadata = schema.metadata();

    assert_eq!(
        metadata.get("bio.vcf.zarr.version").map(String::as_str),
        Some(SUPPORTED_VCF_ZARR_VERSION)
    );
    assert_eq!(
        metadata
            .get(COORDINATE_SYSTEM_METADATA_KEY)
            .map(String::as_str),
        Some(expected_coordinate_system.as_str())
    );
    assert_eq!(
        metadata.get(VCF_FILE_FORMAT_KEY).map(String::as_str),
        Some("VCFv4.3")
    );

    let sample_names_json = metadata
        .get(VCF_SAMPLE_NAMES_KEY)
        .expect("sample names metadata should exist");
    let sample_names: Vec<String> =
        from_json_string(sample_names_json).expect("sample names metadata should be JSON");
    assert!(sample_names.is_empty());
}

#[test]
fn vcf_zarr_schema_metadata_prefers_root_fileformat_over_fallback() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let root = temp_dir.path().join("fileformat_4_2.vcz");
    write_v2_root_metadata(
        &root,
        &format!(
            "{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\",\"vcf_meta_information\":[[\"fileformat\",\"VCFv4.2\"]]}}"
        ),
    );
    write_required_zarray_metadata(&root);

    let provider = VcfZarrTableProvider::new(
        root.to_str()
            .expect("temp path should be UTF-8")
            .to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("store with required arrays should open");

    assert_eq!(
        provider
            .schema()
            .metadata()
            .get(VCF_FILE_FORMAT_KEY)
            .map(String::as_str),
        Some("VCFv4.2")
    );
}

#[test]
fn vcf_zarr_schema_metadata_uses_complete_vcf_fileformat_fallback() {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let cases = [
        (
            "no_fileformat",
            format!("{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\"}}"),
        ),
        (
            "malformed_fileformat",
            format!(
                "{{\"vcf_zarr_version\":\"{SUPPORTED_VCF_ZARR_VERSION}\",\"vcf_meta_information\":\"not an array\"}}"
            ),
        ),
    ];

    for (case_name, attributes) in cases {
        let root = temp_dir.path().join(format!("{case_name}.vcz"));
        write_v2_root_metadata(&root, &attributes);
        write_required_zarray_metadata(&root);

        let provider = VcfZarrTableProvider::new(
            root.to_str()
                .expect("temp path should be UTF-8")
                .to_string(),
            VcfZarrReadOptions::default(),
        )
        .expect("store with required arrays should open");

        assert_eq!(
            provider
                .schema()
                .metadata()
                .get(VCF_FILE_FORMAT_KEY)
                .map(String::as_str),
            Some("VCFv4.3"),
            "{case_name} should use complete fileformat fallback"
        );
    }
}
