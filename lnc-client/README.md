# lnc-client

[![Crates.io](https://img.shields.io/crates/v/lnc-client.svg)](https://crates.io/crates/lnc-client)
[![Documentation](https://docs.rs/lnc-client/badge.svg)](https://docs.rs/lnc-client)
[![License](https://img.shields.io/crates/l/lnc-client.svg)](https://github.com/nitecon/lance/blob/main/LICENSE)

Async Rust client library for the [LANCE](https://github.com/nitecon/lance) streaming platform.

## Features

- **Async/await** - Built on Tokio for high-performance async I/O
- **Producer** - Batch records with configurable batching and backpressure
- **Consumer** - Standalone and grouped consumer modes
- **Connection pooling** - Automatic reconnection and cluster-aware routing
- **TLS support** - Secure connections with rustls
- **Zero-copy** - Efficient record parsing with minimal allocations

## Installation

```bash
cargo add lnc-client
```

## Quick Start

### Producer

```rust
use lnc_client::{ClientConfig, Producer, ProducerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new("localhost:9000");
    let producer = Producer::new(config, ProducerConfig::default()).await?;
    
    producer.send("my-topic", b"Hello, LANCE!").await?;
    producer.flush().await?;
    
    Ok(())
}
```

### Consumer (Standalone)

```rust
use lnc_client::{ClientConfig, StandaloneConsumer, StandaloneConfig, SeekPosition};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new("localhost:9000");
    let consumer = StandaloneConsumer::new(
        config,
        StandaloneConfig::new("my-topic", 0)
            .seek_position(SeekPosition::Earliest),
    ).await?;
    
    while let Some(record) = consumer.poll().await? {
        println!("Received: {:?}", record);
    }
    
    Ok(())
}
```

### Consumer (Grouped)

```rust
use lnc_client::{ClientConfig, GroupedConsumer, GroupConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new("localhost:9000");
    let consumer = GroupedConsumer::new(
        config,
        GroupConfig::new("my-group", vec!["my-topic"]),
    ).await?;
    
    loop {
        let records = consumer.poll().await?;
        for record in records {
            println!("Received: {:?}", record);
        }
        consumer.commit().await?;
    }
}
```

## Configuration

### Client Configuration

```rust
use lnc_client::{ClientConfig, TlsClientConfig};

let config = ClientConfig::new("localhost:9000")
    .with_tls(TlsClientConfig::default())
    .with_auth("username", "password");
```

### Producer Configuration

```rust
use lnc_client::ProducerConfig;
use std::time::Duration;

let config = ProducerConfig::default()
    .batch_size(16384)
    .linger(Duration::from_millis(5))
    .max_in_flight(5);
```

### Consumer Configuration

```rust
use lnc_client::{ConsumerConfig, SeekPosition};
use std::time::Duration;

let config = ConsumerConfig::default()
    .fetch_max_bytes(1048576)
    .poll_timeout(Duration::from_secs(30));
```

## Connection Pooling

For high-throughput scenarios, use the connection pool:

```rust
use lnc_client::{ConnectionPool, ConnectionPoolConfig};

let pool = ConnectionPool::new(
    ConnectionPoolConfig::default()
        .max_connections(10)
        .idle_timeout(Duration::from_secs(60)),
).await?;

let client = pool.get().await?;
```

## Error Handling

All operations return `Result<T, ClientError>`:

```rust
use lnc_client::ClientError;

match producer.send("topic", data).await {
    Ok(ack) => println!("Sent at offset {}", ack.offset),
    Err(ClientError::NotLeader { leader }) => {
        println!("Redirect to leader: {:?}", leader);
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

## Platform Support

The client library supports all major platforms:
- Linux (x86_64, aarch64)
- macOS (x86_64, aarch64)
- Windows (x86_64)

> **Note**: The LANCE server requires Linux with io_uring support, but the client works everywhere.

## License

Apache License 2.0 - see [LICENSE](https://github.com/nitecon/lance/blob/main/LICENSE) for details.
