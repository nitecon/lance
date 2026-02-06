# lnc-metrics

[![Crates.io](https://img.shields.io/crates/v/lnc-metrics.svg)](https://crates.io/crates/lnc-metrics)
[![Documentation](https://docs.rs/lnc-metrics/badge.svg)](https://docs.rs/lnc-metrics)
[![License](https://img.shields.io/crates/l/lnc-metrics.svg)](https://github.com/nitecon/lance/blob/main/LICENSE)

Non-blocking telemetry and metrics for the [LANCE](https://github.com/nitecon/lance) streaming platform.

## Overview

This crate provides metrics and observability infrastructure:

- **Prometheus metrics** - Export metrics in Prometheus format
- **OpenTelemetry** - OTLP export for distributed tracing
- **Golden signals** - Latency, traffic, errors, saturation

## Usage

This crate is primarily used as a dependency of other LANCE crates. Most users should depend on `lnc-client` directly.

```bash
cargo add lnc-metrics
```

## License

Apache License 2.0 - see [LICENSE](https://github.com/nitecon/lance/blob/main/LICENSE) for details.
