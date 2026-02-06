# lnc-core

[![Crates.io](https://img.shields.io/crates/v/lnc-core.svg)](https://crates.io/crates/lnc-core)
[![Documentation](https://docs.rs/lnc-core/badge.svg)](https://docs.rs/lnc-core)
[![License](https://img.shields.io/crates/l/lnc-core.svg)](https://github.com/nitecon/lance/blob/main/LICENSE)

Core types and primitives for the [LANCE](https://github.com/nitecon/lance) streaming platform.

## Overview

This crate provides the foundational types used throughout the LANCE ecosystem:

- **TLV encoding** - Type-Length-Value record format
- **SortKey** - 128-bit deterministic ordering keys
- **Buffer pool** - Zero-allocation buffer management
- **Checksum** - CRC32 validation utilities
- **Backpressure** - Flow control primitives

## Usage

This crate is primarily used as a dependency of `lnc-client` and other LANCE crates. Most users should depend on `lnc-client` directly.

```bash
cargo add lnc-core
```

## License

Apache License 2.0 - see [LICENSE](https://github.com/nitecon/lance/blob/main/LICENSE) for details.
