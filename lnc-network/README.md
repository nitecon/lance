# lnc-network

[![Crates.io](https://img.shields.io/crates/v/lnc-network.svg)](https://crates.io/crates/lnc-network)
[![Documentation](https://docs.rs/lnc-network/badge.svg)](https://docs.rs/lnc-network)
[![License](https://img.shields.io/crates/l/lnc-network.svg)](https://github.com/nitecon/lance/blob/main/LICENSE)

Network layer for the [LANCE](https://github.com/nitecon/lance) streaming platform.

## Overview

This crate implements the Lance Wire Protocol (LWP):

- **Protocol framing** - 44-byte frame headers with CRC validation
- **Connection handling** - Async TCP/TLS connections
- **Zero-copy parsing** - Efficient frame parsing with minimal allocations
- **Compression** - Optional LZ4 and Zstd compression

## Features

- `tls` - Enable TLS support (rustls)
- `lz4` - Enable LZ4 compression
- `compression` - Enable all compression algorithms

## Usage

This crate is primarily used as a dependency of `lnc-client`. Most users should depend on `lnc-client` directly.

```bash
cargo add lnc-network
```

## License

Apache License 2.0 - see [LICENSE](https://github.com/nitecon/lance/blob/main/LICENSE) for details.
