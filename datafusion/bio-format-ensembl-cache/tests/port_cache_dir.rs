//! Port tests for `CacheDir.t` rows 25, 28, 30 — provider construction
//! against the real v115-shaped cache fixture
//! (`tests/fixtures/real_vep_115_chr22/`).
//!
//! These are integration tests because `EnsemblCacheTableProvider::for_entity`
//! exercises end-to-end file-discovery + schema-building + serializer
//! validation in `ProviderInner::new` (table_provider.rs). The unit-level
//! parser behaviour for these rows is covered by `info.rs::tests::port_cache_dir`.
//!
//! See `porting-tests/detailed_plans/CacheDir.md` for the audit, and
//! `porting-tests/plans/2026-05-28-port-cache-dir.md` Task 10 for the plan.

use datafusion_bio_format_ensembl_cache::{
    CacheSourceType, EnsemblCacheOptions, EnsemblCacheTableProvider, EnsemblEntityKind,
};

fn fixture_path() -> String {
    format!(
        "{}/tests/fixtures/real_vep_115_chr22",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn ensembl_options() -> EnsemblCacheOptions {
    EnsemblCacheOptions::new(fixture_path()).with_cache_source_type(CacheSourceType::Ensembl)
}

// SUBTEST #25 (Group D — default config activates only transcript):
//   Perl: get_all_AnnotationSources returns 1-element array of Cache::Transcript.
//   Rust: EnsemblCacheTableProvider::for_entity(Transcript, opts) returns Ok.
//   The "ARRAY of one" claim has no analogue (vepyr constructs one provider
//   per call); the testable surface is "Transcript provider is constructable
//   from this cache".
#[test]
fn row25_transcript_provider_constructable() {
    let _provider =
        EnsemblCacheTableProvider::for_entity(EnsemblEntityKind::Transcript, ensembl_options())
            .expect("Transcript provider must construct from v115 fixture");
}

// SUBTEST #28 (Group D — regulatory => 1 activates RegFeat in addition to
// Transcript):
//   Perl: 2 sources [Transcript, RegFeat]; the "list-contains-two" claim has
//   no analogue under vepyr's per-entity construction. Testable surface:
//   RegFeat provider is constructable.
//
// NOTE: if the real_vep_115_chr22 fixture lacks regulatory data, this test
// will error during file discovery in ProviderInner::new. In that case the
// row downgrades to a commented-out test per plan Task 10 rule. See test
// output for the diagnostic.
#[test]
fn row28_regulatory_feature_provider_constructable() {
    match EnsemblCacheTableProvider::for_entity(
        EnsemblEntityKind::RegulatoryFeature,
        ensembl_options(),
    ) {
        Ok(_provider) => {
            // GREEN — fixture exposes regulatory data.
        }
        Err(e) => {
            let msg = e.to_string();
            // Plan Task 10 downgrade rule: if the fixture lacks regulatory
            // files, the row gates on a fixture-extension future-work entry.
            // Surface a clear panic so the controller catches it as a
            // downgrade signal rather than a silent skip.
            panic!(
                "row28 RegulatoryFeature provider failed to construct from v115 fixture: {msg}. \
                 If the fixture lacks regulatory data, downgrade this row per \
                 plan Task 10 rule and add a 'real_vep_115_chr22 fixture extension' \
                 future-work entry."
            );
        }
    }
}

// SUBTEST #30 (Group D — check_existing => 1 activates Variation in addition
// to Transcript):
//   Perl: 2 sources [Transcript, Variation]; testable surface = Variation
//   provider is constructable.
#[test]
fn row30_variation_provider_constructable() {
    match EnsemblCacheTableProvider::for_entity(EnsemblEntityKind::Variation, ensembl_options()) {
        Ok(_provider) => {
            // GREEN — fixture exposes variation data.
        }
        Err(e) => {
            let msg = e.to_string();
            panic!(
                "row30 Variation provider failed to construct from v115 fixture: {msg}. \
                 If the fixture lacks variation data, downgrade this row per \
                 plan Task 10 rule and add a 'real_vep_115_chr22 fixture extension' \
                 future-work entry."
            );
        }
    }
}
