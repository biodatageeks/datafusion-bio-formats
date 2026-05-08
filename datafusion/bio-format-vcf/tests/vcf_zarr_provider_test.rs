use datafusion_bio_format_vcf::zarr::{VcfZarrReadOptions, VcfZarrTableProvider};

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
#[ignore = "requires VCF Zarr metadata parsing and fixture"]
fn vcf_zarr_requires_version_0_4() {
    let fixture = "tests/data/vcf_zarr/unsupported_version.vcz";
    let result = VcfZarrTableProvider::new(fixture.to_string(), VcfZarrReadOptions::default());

    let message = result
        .expect_err("unsupported vcf_zarr_version must fail")
        .to_string();
    assert!(
        message.contains("unsupported vcf_zarr_version") && message.contains("0.4"),
        "unexpected error: {message}"
    );
}
