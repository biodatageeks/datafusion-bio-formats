use std::time::Instant;

// Simulate the filter evaluation overhead
fn evaluate_filters_against_record_simulation(has_filters: bool) -> bool {
    if !has_filters {
        return true; // Early exit - O(1) operation
    }

    // Simulate some filter evaluation work
    for _i in 0..100 {
        let _ = 1 + 1; // Simulate computation
    }
    true
}

fn main() {
    println!("🧪 Filter Evaluation Overhead Micro-benchmark");
    println!("=============================================");

    let iterations = 10_000_000; // 10 million iterations

    // Test 1: No filters (our case)
    let start = Instant::now();
    for _i in 0..iterations {
        let _ = evaluate_filters_against_record_simulation(false);
    }
    let no_filter_duration = start.elapsed();

    // Test 2: With filters
    let start = Instant::now();
    for _i in 0..iterations {
        let _ = evaluate_filters_against_record_simulation(true);
    }
    let with_filter_duration = start.elapsed();

    println!("Results for {} iterations:", iterations);
    println!(
        "• No filters: {:?} ({:.1} ns per call)",
        no_filter_duration,
        no_filter_duration.as_nanos() as f64 / iterations as f64
    );
    println!(
        "• With filters: {:?} ({:.1} ns per call)",
        with_filter_duration,
        with_filter_duration.as_nanos() as f64 / iterations as f64
    );

    let overhead_ns = (no_filter_duration.as_nanos() as f64) / (iterations as f64);

    println!("\n🎯 Overhead Analysis:");
    println!("• Per-record overhead: {:.2} nanoseconds", overhead_ns);
    println!(
        "• For 7.75M records: {:.3} milliseconds total overhead",
        (overhead_ns * 7_750_000.0) / 1_000_000.0
    );

    if overhead_ns < 1.0 {
        println!("• ✅ Negligible overhead - less than 1 nanosecond per record");
    } else if overhead_ns < 10.0 {
        println!("• ✅ Very low overhead - less than 10 nanoseconds per record");
    } else {
        println!(
            "• ⚠️  Measurable overhead - {} nanoseconds per record",
            overhead_ns
        );
    }
}
