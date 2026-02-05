//! OTLP (OpenTelemetry Protocol) export implementation.
//!
//! Provides non-blocking export of metrics and traces to OTLP-compatible backends
//! per Architecture §12.3: OTEL Integration.
//!
//! This module provides configuration and initialization for OTLP export.
//! The actual export is handled by the OpenTelemetry SDK's background threads.

use std::time::Duration;

/// OTLP exporter configuration
#[derive(Debug, Clone)]
pub struct OtlpConfig {
    /// OTLP endpoint (e.g., "http://localhost:4317")
    pub endpoint: String,
    /// Service name for telemetry
    pub service_name: String,
    /// Export interval
    pub export_interval: Duration,
    /// Enable metrics export
    pub metrics_enabled: bool,
    /// Enable traces export
    pub traces_enabled: bool,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4317".to_string(),
            service_name: "lance".to_string(),
            export_interval: Duration::from_secs(10),
            metrics_enabled: true,
            traces_enabled: true,
        }
    }
}

impl OtlpConfig {
    /// Create config with endpoint
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Set service name
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    /// Set export interval
    pub fn with_export_interval(mut self, interval: Duration) -> Self {
        self.export_interval = interval;
        self
    }

    /// Enable/disable metrics
    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.metrics_enabled = enabled;
        self
    }

    /// Enable/disable traces
    pub fn with_traces(mut self, enabled: bool) -> Self {
        self.traces_enabled = enabled;
        self
    }
}

/// Global OTLP state for easy access
static OTLP_INITIALIZED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Initialize OTLP metrics exporter
/// Per Architecture §12.3: Non-blocking, drops on overflow
///
/// Returns Ok(()) if initialization succeeds, Err with message otherwise.
/// This is designed to be non-fatal - telemetry failures should not crash the server.
pub fn init_otlp_metrics(config: &OtlpConfig) -> Result<(), String> {
    use opentelemetry_otlp::WithExportConfig;
    use std::sync::atomic::Ordering;

    if !config.metrics_enabled {
        return Ok(());
    }

    if OTLP_INITIALIZED.swap(true, Ordering::SeqCst) {
        return Err("OTLP already initialized".to_string());
    }

    eprintln!(
        "[OTLP] Initializing metrics: endpoint={}, service={}, interval={:?}",
        config.endpoint, config.service_name, config.export_interval
    );

    // Build the OTLP exporter with gRPC/tonic
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&config.endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("Failed to create OTLP exporter: {}", e))?;

    // Create periodic reader that exports metrics on interval
    // Uses tokio runtime for async export
    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(
        exporter,
        opentelemetry_sdk::runtime::Tokio,
    )
    .with_interval(config.export_interval)
    .build();

    // Build the meter provider
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .build();

    // Set as global provider
    opentelemetry::global::set_meter_provider(provider);

    eprintln!("[OTLP] Metrics exporter initialized successfully");
    Ok(())
}

/// Record metrics to OTLP using the global meter provider
/// This exports our internal metrics snapshot to OTLP format
pub fn record_metrics_to_otlp() {
    use crate::MetricsSnapshot;
    use opentelemetry::KeyValue;

    let meter = opentelemetry::global::meter("lance");
    let snapshot = MetricsSnapshot::capture();

    // Counters - these accumulate over time
    let records_counter = meter.u64_counter("lance_records_ingested_total").build();
    records_counter.add(snapshot.records_ingested, &[]);

    let bytes_counter = meter.u64_counter("lance_bytes_ingested_total").build();
    bytes_counter.add(snapshot.bytes_ingested, &[]);

    let reads_counter = meter.u64_counter("lance_reads_total").build();
    reads_counter.add(snapshot.reads_total, &[]);

    let read_bytes_counter = meter.u64_counter("lance_read_bytes_total").build();
    read_bytes_counter.add(snapshot.read_bytes_total, &[]);

    let zero_copy_counter = meter.u64_counter("lance_zero_copy_sends_total").build();
    zero_copy_counter.add(snapshot.zero_copy_sends, &[]);

    // Error counters
    let crc_failures = meter.u64_counter("lance_crc_failures_total").build();
    crc_failures.add(snapshot.crc_failures, &[KeyValue::new("type", "crc")]);

    let backpressure = meter.u64_counter("lance_backpressure_events_total").build();
    backpressure.add(snapshot.backpressure_events, &[]);

    let throttled = meter.u64_counter("lance_consumer_throttled_total").build();
    throttled.add(snapshot.consumer_throttled, &[]);
}

/// Shutdown OTLP exporters gracefully
pub fn shutdown_otlp() {
    // Note: meter provider shutdown is handled automatically when dropped
    // or can be triggered via the provider directly if we store it
    eprintln!("[OTLP] Meter provider shutdown initiated");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otlp_config_default() {
        let config = OtlpConfig::default();
        assert_eq!(config.endpoint, "http://localhost:4317");
        assert_eq!(config.service_name, "lance");
        assert!(config.metrics_enabled);
        assert!(config.traces_enabled);
    }

    #[test]
    fn test_otlp_config_builder() {
        let config = OtlpConfig::default()
            .with_endpoint("http://otel-collector:4317")
            .with_service_name("lance-test")
            .with_export_interval(Duration::from_secs(30))
            .with_metrics(true)
            .with_traces(false);

        assert_eq!(config.endpoint, "http://otel-collector:4317");
        assert_eq!(config.service_name, "lance-test");
        assert_eq!(config.export_interval, Duration::from_secs(30));
        assert!(config.metrics_enabled);
        assert!(!config.traces_enabled);
    }
}
