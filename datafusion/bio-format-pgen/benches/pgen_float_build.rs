//! Isolates the cost of turning decoded per-sample dosages into an Arrow
//! `List<Float32>`, which profiling showed dominates a whole-chromosome DS
//! scan. Comparing the two strategies in one binary removes the thermal and
//! page-cache drift that makes end-to-end timings hard to A/B.

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{ArrayRef, Float32Array, Float32Builder, ListArray, ListBuilder};
use datafusion::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};

/// One 2,548-sample cohort, the shape of the benchmark fixture.
const SAMPLES: usize = 2548;
/// A batch's worth of variants.
const VARIANTS: usize = 2000;

fn sample_field() -> FieldRef {
    Arc::new(Field::new("sample", DataType::Float32, true))
}

/// Dosages with a realistic missing rate; the callset is dense, so most cells
/// are present and the validity bitmap is nearly all ones.
fn rows() -> Vec<Vec<Option<f32>>> {
    (0..VARIANTS)
        .map(|variant| {
            (0..SAMPLES)
                .map(|sample| {
                    if (variant * SAMPLES + sample).is_multiple_of(997) {
                        None
                    } else {
                        Some(((sample % 3) as f32) * 0.5)
                    }
                })
                .collect()
        })
        .collect()
}

/// The original strategy: one `append_option` per genotype cell.
fn build_with_list_builder(field: &FieldRef, rows: &[Vec<Option<f32>>]) -> ArrayRef {
    let mut builder = ListBuilder::new(Float32Builder::new()).with_field(field.clone());
    for row in rows {
        for value in row {
            builder.values().append_option(*value);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

/// The replacement: fill the values and validity buffers directly, with the
/// total size reserved up front and a repeated-length offset buffer.
fn build_with_buffers(field: &FieldRef, rows: &[Vec<Option<f32>>]) -> ArrayRef {
    let sample_count = rows.first().map(Vec::len).unwrap_or(0);
    let total = rows.len() * sample_count;
    let mut values: Vec<f32> = Vec::with_capacity(total);
    let mut bytes: Vec<u8> = Vec::with_capacity(total.div_ceil(8));
    let mut len = 0usize;
    let mut null_count = 0usize;
    for row in rows {
        for value in row {
            let shift = len % 8;
            if len.is_multiple_of(8) {
                bytes.push(0);
            }
            match value {
                Some(value) => {
                    values.push(*value);
                    let last = bytes.len() - 1;
                    bytes[last] |= 1 << shift;
                }
                None => {
                    values.push(0.0);
                    null_count += 1;
                }
            }
            len += 1;
        }
    }
    let nulls =
        (null_count != 0).then(|| NullBuffer::new(BooleanBuffer::new(Buffer::from(bytes), 0, len)));
    let values = Arc::new(Float32Array::new(values.into(), nulls)) as ArrayRef;
    Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::from_repeated_length(sample_count, rows.len()),
        values,
        None,
    ))
}

fn bench(criterion: &mut Criterion) {
    let field = sample_field();
    let rows = rows();
    let cells = VARIANTS * SAMPLES;

    let mut group = criterion.benchmark_group("ds_array_build");
    group.throughput(criterion::Throughput::Elements(cells as u64));
    group.bench_function("list_builder_append_option", |bencher| {
        bencher.iter(|| build_with_list_builder(&field, &rows))
    });
    group.bench_function("direct_buffers", |bencher| {
        bencher.iter(|| build_with_buffers(&field, &rows))
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
