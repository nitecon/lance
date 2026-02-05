//! OpenTelemetry-compatible tracing integration.
//!
//! Provides distributed tracing capabilities for request tracking
//! across the LANCE server and client components.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Simple counter for generating unique IDs
static SPAN_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Tracing configuration
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Service name for traces
    pub service_name: String,
    /// OTLP endpoint (e.g., "http://localhost:4317")
    pub otlp_endpoint: Option<String>,
    /// Sample rate (0.0 to 1.0)
    pub sample_rate: f64,
    /// Enable console output for traces
    pub console_output: bool,
    /// Batch export interval
    pub export_interval: Duration,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            service_name: "lance".to_string(),
            otlp_endpoint: None,
            sample_rate: 1.0,
            console_output: false,
            export_interval: Duration::from_secs(5),
        }
    }
}

impl TracingConfig {
    /// Create config with OTLP endpoint
    pub fn with_otlp(mut self, endpoint: impl Into<String>) -> Self {
        self.otlp_endpoint = Some(endpoint.into());
        self
    }

    /// Set service name
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    /// Set sample rate
    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.sample_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Enable console output
    pub fn with_console(mut self) -> Self {
        self.console_output = true;
        self
    }
}

fn generate_id() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let counter = SPAN_COUNTER.fetch_add(1, Ordering::Relaxed);
    now ^ (counter << 32)
}

/// Span context for distributed tracing
#[derive(Debug, Clone)]
pub struct SpanContext {
    pub trace_id: u128,
    pub span_id: u64,
    pub parent_span_id: Option<u64>,
    pub sampled: bool,
}

impl SpanContext {
    /// Generate a new random trace ID
    pub fn new_trace() -> Self {
        let id = generate_id();
        let trace_id = (id as u128) << 64 | generate_id() as u128;

        Self {
            trace_id,
            span_id: id,
            parent_span_id: None,
            sampled: true,
        }
    }

    /// Create a child span
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: generate_id(),
            parent_span_id: Some(self.span_id),
            sampled: self.sampled,
        }
    }

    /// Parse from W3C traceparent header
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        let trace_id = u128::from_str_radix(parts[1], 16).ok()?;
        let span_id = u64::from_str_radix(parts[2], 16).ok()?;
        let flags = u8::from_str_radix(parts[3], 16).ok()?;

        Some(Self {
            trace_id,
            span_id,
            parent_span_id: None,
            sampled: (flags & 0x01) != 0,
        })
    }

    /// Format as W3C traceparent header
    pub fn to_traceparent(&self) -> String {
        let flags = if self.sampled { "01" } else { "00" };
        format!("00-{:032x}-{:016x}-{}", self.trace_id, self.span_id, flags)
    }
}

/// Operation types for tracing
#[derive(Debug, Clone, Copy)]
pub enum OperationType {
    Ingest,
    Fetch,
    CreateTopic,
    DeleteTopic,
    Replicate,
    Fsync,
    IndexWrite,
    HealthCheck,
}

impl OperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationType::Ingest => "ingest",
            OperationType::Fetch => "fetch",
            OperationType::CreateTopic => "create_topic",
            OperationType::DeleteTopic => "delete_topic",
            OperationType::Replicate => "replicate",
            OperationType::Fsync => "fsync",
            OperationType::IndexWrite => "index_write",
            OperationType::HealthCheck => "health_check",
        }
    }
}

/// Simple span for local tracing
pub struct LocalSpan {
    pub context: SpanContext,
    pub operation: OperationType,
    pub start_time: std::time::Instant,
    pub attributes: Vec<(String, String)>,
}

impl LocalSpan {
    pub fn new(operation: OperationType) -> Self {
        Self {
            context: SpanContext::new_trace(),
            operation,
            start_time: std::time::Instant::now(),
            attributes: Vec::new(),
        }
    }

    pub fn with_context(context: SpanContext, operation: OperationType) -> Self {
        Self {
            context,
            operation,
            start_time: std::time::Instant::now(),
            attributes: Vec::new(),
        }
    }

    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.attributes.push((key.into(), value.into()));
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get trace ID as hex string
    pub fn trace_id_hex(&self) -> String {
        format!("{:032x}", self.context.trace_id)
    }

    /// Get span ID as hex string
    pub fn span_id_hex(&self) -> String {
        format!("{:016x}", self.context.span_id)
    }
}

/// Initialize tracing (placeholder - actual implementation would use tracing-subscriber)
pub fn init_tracing(config: &TracingConfig) {
    // Log initialization
    eprintln!(
        "[TRACE] Initialized tracing: service={}, sample_rate={}, otlp={:?}",
        config.service_name, config.sample_rate, config.otlp_endpoint
    );
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_tracing_config_default() {
        let config = TracingConfig::default();
        assert_eq!(config.service_name, "lance");
        assert_eq!(config.sample_rate, 1.0);
    }

    #[test]
    fn test_span_context_new() {
        let ctx = SpanContext::new_trace();
        assert!(ctx.trace_id != 0);
        assert!(ctx.span_id != 0);
        assert!(ctx.sampled);
    }

    #[test]
    fn test_span_context_child() {
        let parent = SpanContext::new_trace();
        let child = parent.child();

        assert_eq!(child.trace_id, parent.trace_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id));
        assert_ne!(child.span_id, parent.span_id);
    }

    #[test]
    fn test_traceparent_roundtrip() {
        let ctx = SpanContext::new_trace();
        let header = ctx.to_traceparent();

        let parsed = SpanContext::from_traceparent(&header).unwrap();
        assert_eq!(parsed.trace_id, ctx.trace_id);
        assert_eq!(parsed.span_id, ctx.span_id);
        assert_eq!(parsed.sampled, ctx.sampled);
    }

    #[test]
    fn test_local_span() {
        let span = LocalSpan::new(OperationType::Ingest);
        assert_eq!(span.operation.as_str(), "ingest");
        assert!(span.elapsed() < Duration::from_secs(1));
    }
}
