//! Cross-checks the `ALT_COUNT` and `DS` scans against the `GT` scan of the same
//! fileset, on real records and at full scale.
//!
//! The three fields take different decode paths: `GT` applies hardcall phase and
//! goes through the per-sample code buffer, while `ALT_COUNT` and `DS` take the
//! fused common-value decode that never materializes one. They must still agree
//! cell for cell, because an ALT allele count does not depend on which haplotype
//! carries the allele.
//!
//! Each variant is reduced to a hash of its per-sample allele counts, so the
//! comparison covers every cell without holding two whole matrices at once.
//! Missing calls hash as a distinct sentinel, so a validity difference shows up
//! as loudly as a value difference.
//!
//! `DS` is only comparable when the fileset stores no dosage track — a stored
//! dosage of 0.125 is genuinely not a hardcall count. Non-integral values are
//! reported rather than silently treated as a mismatch.
//!
//! Usage:
//!   cargo run --release -p datafusion-bio-format-pgen --example pgen_field_parity \
//!     -- <path.pgen> [partitions]

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, FixedSizeListArray, Float32Array, Int8Array, ListArray, StructArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_bio_format_pgen::{PgenReadOptions, PgenTableProvider};
use futures::StreamExt;

/// The per-sample byte a missing call contributes to a variant's hash.
const MISSING: u8 = 0xff;

#[derive(Default)]
struct Hasher(u64);

impl Hasher {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    #[inline]
    fn write(&mut self, byte: u8) {
        self.0 = (self.0 ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3);
    }
}

/// Reduces one genotype field of one fileset to a per-variant hash of its
/// per-sample ALT allele counts.
async fn field_digests(
    path: &str,
    field: &str,
    partitions: usize,
) -> (Vec<u64>, usize, Option<f32>) {
    let options = PgenReadOptions {
        genotype_fields: Some(vec![field.to_string()]),
        ..Default::default()
    };
    let provider = PgenTableProvider::try_new(path.to_string(), options)
        .await
        .expect("open fileset");
    let config = SessionConfig::new()
        .with_target_partitions(partitions)
        .with_batch_size(1 << 12);
    let context = SessionContext::new_with_config(config);
    context
        .register_table("pgen", Arc::new(provider))
        .expect("register");
    let mut stream = context
        .sql("SELECT genotypes FROM pgen")
        .await
        .expect("plan")
        .execute_stream()
        .await
        .expect("execute");

    let mut digests = Vec::new();
    let mut cells = 0;
    let mut fractional = None;
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch");
        let values = field_list(&batch, field);
        for row in 0..batch.num_rows() {
            let samples = values.value(row);
            let mut hasher = Hasher::new();
            match field {
                "GT" => {
                    let pairs = samples
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .expect("GT is a fixed-size allele pair");
                    let alleles = pairs
                        .values()
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::UInt16Array>()
                        .expect("alleles are u16");
                    for sample in 0..pairs.len() {
                        if pairs.is_null(sample) {
                            hasher.write(MISSING);
                            continue;
                        }
                        let left = alleles.value(sample * 2);
                        let right = alleles.value(sample * 2 + 1);
                        hasher.write(u8::from(left == 1) + u8::from(right == 1));
                    }
                    cells += pairs.len();
                }
                "ALT_COUNT" => {
                    let counts = samples
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .expect("ALT_COUNT is int8");
                    for sample in 0..counts.len() {
                        if counts.is_null(sample) {
                            hasher.write(MISSING);
                        } else {
                            hasher.write(counts.value(sample) as u8);
                        }
                    }
                    cells += counts.len();
                }
                "DS" => {
                    let dosages = samples
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .expect("DS is float32");
                    for sample in 0..dosages.len() {
                        if dosages.is_null(sample) {
                            hasher.write(MISSING);
                            continue;
                        }
                        let dosage = dosages.value(sample);
                        if dosage.fract() != 0.0 && fractional.is_none() {
                            fractional = Some(dosage);
                        }
                        hasher.write(dosage as u8);
                    }
                    cells += dosages.len();
                }
                other => panic!("unsupported field {other}"),
            }
            digests.push(hasher.0);
        }
    }
    (digests, cells, fractional)
}

fn field_list<'a>(batch: &'a RecordBatch, field: &str) -> &'a ListArray {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("genotypes is a struct")
        .column_by_name(field)
        .expect("projected field")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("field is a list")
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .expect("usage: pgen_field_parity <path.pgen> [partitions]");
    let partitions: usize = args
        .next()
        .map(|value| value.parse().expect("partitions must be a number"))
        .unwrap_or(1);

    let (reference, cells, _) = field_digests(&path, "GT", partitions).await;
    println!(
        "GT: {} variants, {cells} cells (the oracle: GT never takes the fused decode)",
        reference.len()
    );

    let mut failures = 0;
    for field in ["ALT_COUNT", "DS"] {
        let (digests, field_cells, fractional) = field_digests(&path, field, partitions).await;
        if let Some(dosage) = fractional {
            println!(
                "{field}: fileset stores fractional dosages (saw {dosage}); \
                 not comparable to hardcall counts"
            );
            continue;
        }
        if digests.len() != reference.len() || field_cells != cells {
            println!(
                "{field}: MISMATCH in shape — {} variants / {field_cells} cells vs {} / {cells}",
                digests.len(),
                reference.len()
            );
            failures += 1;
            continue;
        }
        let differing = digests
            .iter()
            .zip(&reference)
            .filter(|(field, reference)| field != reference)
            .count();
        if differing == 0 {
            println!("{field}: matches GT across all {field_cells} cells");
        } else {
            println!(
                "{field}: MISMATCH on {differing} of {} variants",
                digests.len()
            );
            failures += 1;
        }
    }

    if failures != 0 {
        std::process::exit(1);
    }
}
