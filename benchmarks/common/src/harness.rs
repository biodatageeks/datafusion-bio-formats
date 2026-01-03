use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Instant;
use sysinfo::System;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkCategory {
    Parallelism,
    PredicatePushdown,
    ProjectionPushdown,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SystemInfo {
    pub os: String,
    pub cpu_model: String,
    pub cpu_cores: usize,
    pub total_memory_gb: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metrics {
    pub throughput_records_per_sec: f64,
    pub elapsed_seconds: f64,
    pub total_records: u64,
    pub speedup_vs_baseline: Option<f64>,
    pub peak_memory_mb: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub benchmark_name: String,
    pub format: String,
    pub category: BenchmarkCategory,
    pub timestamp: DateTime<Utc>,
    pub system_info: SystemInfo,
    pub configuration: serde_json::Value,
    pub metrics: Metrics,
}

pub struct BenchmarkResultBuilder {
    benchmark_name: String,
    format: String,
    category: BenchmarkCategory,
    configuration: serde_json::Value,
}

impl BenchmarkResultBuilder {
    pub fn new(
        benchmark_name: impl Into<String>,
        format: impl Into<String>,
        category: BenchmarkCategory,
    ) -> Self {
        Self {
            benchmark_name: benchmark_name.into(),
            format: format.into(),
            category,
            configuration: serde_json::Value::Null,
        }
    }

    pub fn with_config(mut self, config: serde_json::Value) -> Self {
        self.configuration = config;
        self
    }

    pub fn build(
        self,
        total_records: u64,
        elapsed: std::time::Duration,
        speedup_vs_baseline: Option<f64>,
    ) -> BenchmarkResult {
        let elapsed_seconds = elapsed.as_secs_f64();
        let throughput = calculate_throughput(total_records, elapsed_seconds);

        BenchmarkResult {
            benchmark_name: self.benchmark_name,
            format: self.format,
            category: self.category,
            timestamp: Utc::now(),
            system_info: collect_system_info(),
            configuration: self.configuration,
            metrics: Metrics {
                throughput_records_per_sec: throughput,
                elapsed_seconds,
                total_records,
                speedup_vs_baseline,
                peak_memory_mb: None,
            },
        }
    }
}

pub fn calculate_throughput(total_records: u64, elapsed_seconds: f64) -> f64 {
    total_records as f64 / elapsed_seconds
}

pub fn calculate_speedup(baseline_seconds: f64, target_seconds: f64) -> f64 {
    baseline_seconds / target_seconds
}

pub fn collect_system_info() -> SystemInfo {
    let mut sys = System::new_all();
    sys.refresh_all();

    let os = format!(
        "{} {}",
        System::name().unwrap_or_default(),
        System::os_version().unwrap_or_default()
    );
    let cpu_model = sys
        .cpus()
        .first()
        .map(|cpu| cpu.brand().to_string())
        .unwrap_or_default();
    let cpu_cores = sys.cpus().len();
    let total_memory_gb = sys.total_memory() as f64 / 1024.0 / 1024.0 / 1024.0;

    SystemInfo {
        os,
        cpu_model,
        cpu_cores,
        total_memory_gb,
    }
}

pub fn write_result(result: &BenchmarkResult, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)?;

    let filename = format!(
        "{}_{}.json",
        result.benchmark_name.replace(" ", "_"),
        result.timestamp.format("%Y%m%d_%H%M%S")
    );

    let output_path = output_dir.join(filename);
    let json = serde_json::to_string_pretty(result)?;
    std::fs::write(&output_path, json)?;

    println!("✓ Result written to: {}", output_path.display());
    Ok(())
}

pub fn time_operation<F, T>(operation: F) -> (std::time::Duration, T)
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = operation();
    let elapsed = start.elapsed();
    (elapsed, result)
}
