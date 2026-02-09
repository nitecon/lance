# lnc-client

[![Crates.io](https://img.shields.io/crates/v/lnc-client.svg)](https://crates.io/crates/lnc-client)
[![Documentation](https://docs.rs/lnc-client/badge.svg)](https://docs.rs/lnc-client)
[![License](https://img.shields.io/crates/l/lnc-client.svg)](https://github.com/nitecon/lance/blob/main/LICENSE)

Async Rust client library for the [LANCE](https://github.com/nitecon/lance) streaming platform.

## Features

- **Async/await** — Built on Tokio for high-performance async I/O
- **Producer** — Batch records with configurable batching, linger, and backpressure
- **Consumer** — Standalone and grouped consumer modes with offset persistence
- **Topic Management** — Create, list, and delete topics by name; produce/consume by numeric ID
- **Auto-reconnect** — Transparent reconnection with exponential backoff and leader redirection
- **Connection pooling** — Pool management with health checking and idle timeouts
- **TLS/mTLS** — Secure connections with rustls (server TLS and mutual TLS)
- **Zero-copy** — Efficient record handling with `bytes::Bytes` for minimal allocations

## Installation

```bash
cargo add lnc-client
```

## Quick Start

### Topic Management

Topics are created by name and referenced by numeric ID for produce/consume operations.
Use the low-level `LanceClient` for topic administration:

```rust
use lnc_client::{ClientConfig, LanceClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = LanceClient::connect(ClientConfig::new("127.0.0.1:1992")).await?;

    // Create a topic by name — returns TopicInfo with numeric id
    let topic = client.create_topic("market-data").await?;
    println!("Created topic '{}' with id {}", topic.name, topic.id);

    // List all topics
    let topics = client.list_topics().await?;
    for t in &topics {
        println!("  topic id={} name={}", t.id, t.name);
    }

    // Look up a specific topic
    let info = client.get_topic(topic.id).await?;

    // Create with retention policy (max_age_secs, max_bytes — 0 = unlimited)
    let _t = client.create_topic_with_retention("logs", 86400, 0).await?;

    Ok(())
}
```

### Producer

```rust
use lnc_client::{Producer, ProducerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = Producer::connect(
        "127.0.0.1:1992",
        ProducerConfig::new()
            .with_batch_size(16 * 1024)
            .with_linger_ms(5),
    ).await?;

    // Send to a topic by numeric ID
    let ack = producer.send(1, b"Hello, LANCE!").await?;
    println!("Sent with batch_id: {}", ack.batch_id);

    // Batch multiple sends
    for i in 0..1000 {
        producer.send(1, format!("message-{}", i).as_bytes()).await?;
    }

    // Ensure all buffered records are sent
    producer.flush().await?;
    producer.close().await?;

    Ok(())
}
```

### Consumer (Standalone)

```rust
use lnc_client::{StandaloneConsumer, StandaloneConfig};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut consumer = StandaloneConsumer::connect(
        "127.0.0.1:1992",
        StandaloneConfig::new("my-consumer", 1)  // (consumer_id, topic_id)
            .with_offset_dir(Path::new("/var/lib/lance/offsets")),
    ).await?;

    loop {
        if let Some(result) = consumer.poll().await? {
            println!("Received {} bytes at offset {}", result.data.len(), result.current_offset);
            consumer.commit().await?;
        }
    }
}
```

### Consumer (Grouped)

```rust
use lnc_client::{AssignmentStrategy, GroupCoordinator, GroupConfig, GroupedConsumer, WorkerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start coordinator (typically one per consumer group)
    let coordinator = GroupCoordinator::start(
        "127.0.0.1:1992",
        GroupConfig::new("my-group")
            .with_topics(vec![1, 2, 3])
            .with_assignment_strategy(AssignmentStrategy::RoundRobin),
    ).await?;

    // Workers join the group and receive topic assignments
    let mut worker = GroupedConsumer::join(
        "127.0.0.1:1992",
        coordinator.join_address(),
        WorkerConfig::new("worker-1"),
    ).await?;

    loop {
        let topics: Vec<u32> = worker.assignments().to_vec();
        for topic_id in topics {
            if let Some(_records) = worker.poll(topic_id).await? {
                worker.commit(topic_id).await?;
            }
        }
    }
}
```

## Configuration

### Producer Configuration

```rust
use lnc_client::ProducerConfig;

let config = ProducerConfig::new()
    .with_batch_size(16 * 1024)            // 16KB batch trigger
    .with_linger_ms(5)                     // 5ms max linger before flush
    .with_max_in_flight(5)                 // concurrent in-flight requests
    .with_buffer_memory(32 * 1024 * 1024)  // 32MB buffer limit
    .with_compression(false)               // LZ4 compression
    .with_connect_timeout(std::time::Duration::from_secs(30))
    .with_request_timeout(std::time::Duration::from_secs(30));
```

### Consumer Configuration

```rust
use lnc_client::{StandaloneConfig, SeekPosition};
use std::path::Path;
use std::time::Duration;

let config = StandaloneConfig::new("my-consumer", 1)
    .with_max_fetch_bytes(1_048_576)                   // 1MB per poll
    .with_start_position(SeekPosition::Beginning)      // or SeekPosition::End / SeekPosition::Offset(n)
    .with_offset_dir(Path::new("/var/lib/lance/offsets"))
    .with_auto_commit_interval(Some(Duration::from_secs(5)))
    .with_poll_timeout(Duration::from_millis(100));
```

### TLS Configuration

```rust
use lnc_client::{ClientConfig, TlsClientConfig};

// Plain TCP
let config = ClientConfig::new("127.0.0.1:1992");

// TLS with system root certificates
let config = ClientConfig::new("lance.example.com:1992")
    .with_tls(TlsClientConfig::new());

// TLS with custom CA
let config = ClientConfig::new("lance.example.com:1992")
    .with_tls(TlsClientConfig::new().with_ca_cert("/path/to/ca.pem"));

// Mutual TLS (mTLS)
let config = ClientConfig::new("lance.example.com:1992")
    .with_tls(
        TlsClientConfig::new()
            .with_ca_cert("/path/to/ca.pem")
            .with_client_cert("/path/to/client.pem", "/path/to/client-key.pem"),
    );
```

## Connection Pooling

For high-throughput scenarios or shared connection management:

```rust
use lnc_client::{ConnectionPool, ConnectionPoolConfig};
use std::time::Duration;

let pool = ConnectionPool::new(
    "127.0.0.1:1992",
    ConnectionPoolConfig::new()
        .with_max_connections(10)
        .with_idle_timeout(Duration::from_secs(60))
        .with_health_check_interval(30)
        .with_auto_reconnect(true),
).await?;

// Get a connection from the pool
let mut conn = pool.get().await?;

// Use the underlying LanceClient
conn.ping().await?;

// Connection is returned to pool when dropped
```

## Low-Level Client

`LanceClient` is the unified low-level client that handles all protocol operations on
a single TCP connection. `Producer` and `StandaloneConsumer` are higher-level abstractions
built on top of it. Use `LanceClient` directly for topic administration or when you need
fine-grained control:

```rust
use bytes::Bytes;
use lnc_client::{ClientConfig, LanceClient};

let mut client = LanceClient::connect(ClientConfig::new("127.0.0.1:1992")).await?;

// Topic management
let topic = client.create_topic("my-topic").await?;
let topics = client.list_topics().await?;
client.delete_topic(topic.id).await?;

// Retention policies
client.set_retention(topic.id, 86400, 0).await?;

// Direct ingest (without Producer batching)
let payload = Bytes::from_static(b"raw data");
let batch_id = client.send_ingest_to_topic(topic.id, payload, 1, None).await?;

// Fetch data from a topic at an offset
let fetch = client.fetch(topic.id, 0, 65536).await?;

// Subscribe for streaming consumption (topic_id, start_offset, max_batch_bytes, consumer_id)
client.subscribe(topic.id, 0, 65536, 42).await?;
client.commit_offset(topic.id, 42, 1024).await?;

// Latency measurement
let rtt = client.ping().await?;
```

## Error Handling

All operations return `Result<T, ClientError>`:

```rust
use lnc_client::ClientError;

match producer.send(1, data).await {
    Ok(ack) => println!("Sent with batch_id: {}", ack.batch_id),
    Err(ClientError::NotLeader { leader_addr }) => {
        println!("Redirect to leader: {:?}", leader_addr);
        // ReconnectingClient handles this automatically
    }
    Err(ClientError::ServerCatchingUp { server_offset }) => {
        println!("Server at offset {}, backing off", server_offset);
    }
    Err(ClientError::Timeout) => {
        // Retryable
    }
    Err(e) if e.is_retryable() => {
        // ConnectionFailed, IoError, ServerBackpressure are retryable
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

Apache License 2.0 — see [LICENSE](https://github.com/nitecon/lance/blob/main/LICENSE) for details.
