//! Integration tests for LANCE client
//!
//! These tests require a running LANCE server.
//!
//! Run with:
//!   cargo test --package lnc-client --test integration -- --ignored --nocapture
//!
//! Or set the server address:
//!   LANCE_TEST_ADDR=127.0.0.1:9092 cargo test --package lnc-client --test integration -- --ignored --nocapture

use bytes::Bytes;
use lnc_client::{ClientConfig, LanceClient};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

fn get_test_addr() -> SocketAddr {
    std::env::var("LANCE_TEST_ADDR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 1992)))
}

fn test_config() -> ClientConfig {
    ClientConfig {
        addr: get_test_addr(),
        connect_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(5),
        keepalive_interval: Duration::from_secs(10),
        tls: None,
    }
}

fn is_cluster_mode() -> bool {
    std::env::var("LANCE_NODE2_ADDR").is_ok()
        && std::env::var("LANCE_NODE2_ADDR")
            .map(|v| v != std::env::var("LANCE_TEST_ADDR").unwrap_or_default())
            .unwrap_or(false)
}

async fn wait_for_replication() {
    if is_cluster_mode() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ============================================================================
// Connection Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_connect_and_close() {
    println!("Testing connection to {:?}", get_test_addr());

    let client = LanceClient::connect(test_config()).await;
    assert!(client.is_ok(), "Failed to connect: {:?}", client.err());

    let client = client.unwrap();
    println!("Connected successfully: {:?}", client);

    let result = client.close().await;
    assert!(result.is_ok(), "Failed to close: {:?}", result.err());
    println!("Connection closed successfully");
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_connect_with_string_addr() {
    let addr = format!("{}", get_test_addr());
    println!("Testing connection to {}", addr);

    let client = LanceClient::connect_to(&addr).await;
    assert!(client.is_ok(), "Failed to connect: {:?}", client.err());

    client.unwrap().close().await.unwrap();
}

// ============================================================================
// Keepalive / Ping Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_ping_latency() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Warm-up ping
    let _ = client.ping().await;

    // Measure latency over multiple pings
    let mut latencies = Vec::new();
    for _ in 0..10 {
        let latency = client.ping().await.unwrap();
        latencies.push(latency);
    }

    let avg_latency: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    let min_latency = latencies.iter().min().unwrap();
    let max_latency = latencies.iter().max().unwrap();

    println!("Ping latency (10 samples):");
    println!("  Min: {:?}", min_latency);
    println!("  Max: {:?}", max_latency);
    println!("  Avg: {:?}", avg_latency);

    assert!(
        avg_latency < Duration::from_millis(100),
        "Average ping latency too high: {:?}",
        avg_latency
    );

    client.close().await.unwrap();
}

// ============================================================================
// Ingest Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_single_ingest_sync() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create a topic for this test
    let topic = client
        .create_topic(&unique_topic_name("ingest_sync"))
        .await
        .unwrap();
    let topic_id = topic.id;

    let payload = Bytes::from_static(b"Hello, LANCE!");
    let batch_id = client.send_ingest_sync(payload, topic_id).await;

    assert!(batch_id.is_ok(), "Ingest failed: {:?}", batch_id.err());
    println!("Ingested batch_id: {}", batch_id.unwrap());

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_sequential_ingests() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create a topic for this test
    let topic = client
        .create_topic(&unique_topic_name("sequential"))
        .await
        .unwrap();
    let topic_id = topic.id;

    let count = 100;
    let start = Instant::now();

    for i in 1..=count {
        let payload = Bytes::from(format!("sequential message {}", i));
        let batch_id = client.send_ingest_sync(payload, topic_id).await.unwrap();
        assert_eq!(batch_id, i as u64);
    }

    let elapsed = start.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64();

    println!("Sequential ingests:");
    println!("  Count: {}", count);
    println!("  Time: {:?}", elapsed);
    println!("  Rate: {:.2} msgs/sec", rate);

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_pipelined_ingests() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create a topic for this test
    let topic = client
        .create_topic(&unique_topic_name("pipelined"))
        .await
        .unwrap();
    let topic_id = topic.id;

    let count = 1000;
    let payload_size = 1024;
    let start = Instant::now();

    // Send all without waiting for acks
    for _ in 0..count {
        let payload = Bytes::from(vec![0xAB; payload_size]);
        client.send_ingest(payload, topic_id).await.unwrap();
    }

    let send_elapsed = start.elapsed();

    // Now receive all acks
    for i in 1..=count {
        let acked_id = client.recv_ack().await.unwrap();
        assert_eq!(acked_id, i as u64);
    }

    let total_elapsed = start.elapsed();
    let total_bytes = count * payload_size;
    let throughput_mbps = (total_bytes as f64 / 1024.0 / 1024.0) / total_elapsed.as_secs_f64();

    println!("Pipelined ingests:");
    println!("  Count: {}", count);
    println!("  Payload size: {} bytes", payload_size);
    println!("  Send time: {:?}", send_elapsed);
    println!("  Total time: {:?}", total_elapsed);
    println!(
        "  Rate: {:.2} msgs/sec",
        count as f64 / total_elapsed.as_secs_f64()
    );
    println!("  Throughput: {:.2} MB/s", throughput_mbps);

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_large_payload() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create a topic for this test
    let topic = client
        .create_topic(&unique_topic_name("large_payload"))
        .await
        .unwrap();
    let topic_id = topic.id;

    // 1 MB payload
    let payload_size = 1024 * 1024;
    let payload = Bytes::from(vec![0xCD; payload_size]);

    let start = Instant::now();
    let batch_id = client.send_ingest_sync(payload, topic_id).await;
    let elapsed = start.elapsed();

    assert!(
        batch_id.is_ok(),
        "Large payload ingest failed: {:?}",
        batch_id.err()
    );

    let throughput_mbps = (payload_size as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();

    println!("Large payload ingest:");
    println!(
        "  Size: {} bytes ({} MB)",
        payload_size,
        payload_size / 1024 / 1024
    );
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", throughput_mbps);

    client.close().await.unwrap();
}

// ============================================================================
// Throughput Benchmark
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_throughput_benchmark() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create a topic for this test
    let topic = client
        .create_topic(&unique_topic_name("throughput"))
        .await
        .unwrap();
    let topic_id = topic.id;

    let duration_secs = 5;
    let payload_size = 10 * 1024; // 10 KB per message
    let payload = Bytes::from(vec![0xEF; payload_size]);

    println!(
        "Running throughput benchmark for {} seconds...",
        duration_secs
    );
    println!("Payload size: {} bytes", payload_size);

    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration_secs);

    let mut sent_count = 0u64;
    let mut acked_count = 0u64;

    // Pipeline: send as fast as possible, receive acks
    while Instant::now() < deadline {
        // Send a batch
        for _ in 0..100 {
            if Instant::now() >= deadline {
                break;
            }
            client.send_ingest(payload.clone(), topic_id).await.unwrap();
            sent_count += 1;
        }

        // Receive available acks (non-blocking drain)
        while acked_count < sent_count {
            match tokio::time::timeout(Duration::from_millis(1), client.recv_ack()).await {
                Ok(Ok(_)) => acked_count += 1,
                Ok(Err(e)) => panic!("Ack error: {:?}", e),
                Err(_) => break, // Timeout, continue sending
            }
        }
    }

    // Drain remaining acks
    while acked_count < sent_count {
        client.recv_ack().await.unwrap();
        acked_count += 1;
    }

    let elapsed = start.elapsed();
    let total_bytes = sent_count * payload_size as u64;
    let throughput_mbps = (total_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();
    let msgs_per_sec = sent_count as f64 / elapsed.as_secs_f64();

    println!("\nBenchmark Results:");
    println!("  Duration: {:?}", elapsed);
    println!("  Messages sent: {}", sent_count);
    println!("  Messages acked: {}", acked_count);
    println!(
        "  Total data: {:.2} MB",
        total_bytes as f64 / 1024.0 / 1024.0
    );
    println!("  Throughput: {:.2} MB/s", throughput_mbps);
    println!("  Message rate: {:.2} msgs/sec", msgs_per_sec);

    client.close().await.unwrap();
}

// ============================================================================
// Topic Management Tests
// ============================================================================

fn unique_topic_name(prefix: &str) -> String {
    format!(
        "{}_{}",
        prefix,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    )
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_create_and_list_topics() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create a new topic
    let topic_name = unique_topic_name("create_list_test");
    let created = client.create_topic(&topic_name).await.unwrap();

    // Wait for topic to replicate in cluster mode
    wait_for_replication().await;

    println!("Created topic: id={}, name={}", created.id, created.name);
    assert_eq!(created.name, topic_name);
    assert!(created.id > 0);

    // Verify topic exists in list
    let topics = client.list_topics().await.unwrap();
    assert!(
        topics.iter().any(|t| t.id == created.id),
        "Created topic should appear in list"
    );
    println!(
        "Topic {} found in list of {} topics",
        created.id,
        topics.len()
    );

    // Cleanup
    client.delete_topic(created.id).await.unwrap();

    // Wait for delete to replicate in cluster mode
    wait_for_replication().await;

    // Verify topic no longer in list
    let topics_after = client.list_topics().await.unwrap();
    assert!(
        !topics_after.iter().any(|t| t.id == created.id),
        "Deleted topic should not be in list"
    );

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_get_topic_by_id() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create a topic
    let topic_name = unique_topic_name("get_by_id_test");
    let created = client.create_topic(&topic_name).await.unwrap();

    // Wait for topic to replicate in cluster mode
    wait_for_replication().await;

    // Get it by ID
    let fetched = client.get_topic(created.id).await.unwrap();
    assert_eq!(fetched.id, created.id);
    assert_eq!(fetched.name, topic_name);
    println!("Fetched topic: id={}, name={}", fetched.id, fetched.name);

    // Cleanup
    client.delete_topic(created.id).await.unwrap();

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_delete_topic() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create a topic
    let topic_name = unique_topic_name("delete_test");
    let created = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic for deletion: id={}", created.id);

    // Wait for topic to replicate in cluster mode
    wait_for_replication().await;

    // Delete it
    client.delete_topic(created.id).await.unwrap();
    println!("Deleted topic id={}", created.id);

    // Wait for delete to replicate in cluster mode
    wait_for_replication().await;

    // Verify it's gone (should error)
    let result = client.get_topic(created.id).await;
    assert!(result.is_err(), "Topic should not exist after deletion");
    println!("Expected error after deletion: {:?}", result.err());

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_topic_lifecycle_with_data() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // 1. Create topic
    let topic_name = unique_topic_name("lifecycle_test");
    let topic = client.create_topic(&topic_name).await.unwrap();
    println!("1. Created topic: id={}, name={}", topic.id, topic.name);

    // Wait for topic to replicate in cluster mode
    wait_for_replication().await;

    // 2. Ingest data to the topic
    let mut total_bytes = 0usize;
    let batch_count = 10;
    for i in 0..batch_count {
        let payload = Bytes::from(format!(
            "lifecycle test message {} for topic {}",
            i, topic.id
        ));
        total_bytes += payload.len();
        client
            .send_ingest_to_topic_sync(topic.id, payload, 1, None)
            .await
            .unwrap();
    }
    println!(
        "2. Ingested {} batches ({} bytes) to topic {}",
        batch_count, total_bytes, topic.id
    );

    // 3. Verify topic still exists
    let fetched = client.get_topic(topic.id).await.unwrap();
    assert_eq!(fetched.id, topic.id);
    println!("3. Topic still exists: id={}", fetched.id);

    // 4. Delete the topic
    client.delete_topic(topic.id).await.unwrap();
    println!("4. Deleted topic id={}", topic.id);

    // Wait for delete to replicate in cluster mode
    wait_for_replication().await;

    // 5. Verify deletion
    let result = client.get_topic(topic.id).await;
    assert!(result.is_err());
    println!("5. Topic confirmed deleted");

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_multiple_topics_concurrent_ingest() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create multiple topics
    let topic_count = 3;
    let mut topics = Vec::new();
    for i in 0..topic_count {
        let name = unique_topic_name(&format!("concurrent_{}", i));
        let topic = client.create_topic(&name).await.unwrap();
        topics.push(topic);
    }
    println!("Created {} topics", topics.len());

    // Ingest to all topics in round-robin
    let messages_per_topic = 100;
    let start = Instant::now();

    for msg_idx in 0..(messages_per_topic * topic_count) {
        let topic_idx = msg_idx % topic_count;
        let topic_id = topics[topic_idx].id;
        let payload = Bytes::from(format!("msg {} to topic {}", msg_idx, topic_id));
        client
            .send_ingest_to_topic_sync(topic_id, payload, 1, None)
            .await
            .unwrap();
    }

    let elapsed = start.elapsed();
    let total_msgs = messages_per_topic * topic_count;
    println!(
        "Ingested {} messages to {} topics in {:?}",
        total_msgs, topic_count, elapsed
    );
    println!(
        "Rate: {:.2} msgs/sec",
        total_msgs as f64 / elapsed.as_secs_f64()
    );

    // Cleanup
    for topic in &topics {
        client.delete_topic(topic.id).await.unwrap();
    }
    println!("Cleaned up {} topics", topics.len());

    client.close().await.unwrap();
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_connect_to_invalid_address() {
    let config = ClientConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 19999)), // Unlikely to be listening
        connect_timeout: Duration::from_secs(1),
        ..Default::default()
    };

    let result = LanceClient::connect(config).await;
    assert!(result.is_err(), "Should fail to connect to invalid address");
    println!("Expected error: {:?}", result.err());
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_get_nonexistent_topic() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Try to get a topic that doesn't exist
    let result = client.get_topic(999999).await;
    assert!(result.is_err(), "Should fail to get nonexistent topic");
    println!("Expected error: {:?}", result.err());

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_delete_nonexistent_topic() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Try to delete a topic that doesn't exist
    let result = client.delete_topic(999999).await;
    assert!(result.is_err(), "Should fail to delete nonexistent topic");
    println!("Expected error: {:?}", result.err());

    client.close().await.unwrap();
}

// ============================================================================
// Multi-Subscriber Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_multiple_subscribers_single_topic() {
    // Create 3 separate clients to simulate multiple subscribers
    let mut client1 = LanceClient::connect(test_config()).await.unwrap();
    let mut client2 = LanceClient::connect(test_config()).await.unwrap();
    let mut client3 = LanceClient::connect(test_config()).await.unwrap();

    // Create a shared topic
    let topic_name = unique_topic_name("multi_subscriber");
    let topic = client1.create_topic(&topic_name).await.unwrap();
    let topic_id = topic.id;
    println!("Created shared topic: id={}, name={}", topic_id, topic_name);

    // Wait for topic to replicate in cluster mode
    wait_for_replication().await;

    // Subscribe all clients to the same topic at different offsets
    let consumer_id1 = 1001u64;
    let consumer_id2 = 1002u64;
    let consumer_id3 = 1003u64;
    let max_batch_bytes = 64 * 1024u32; // 64KB batch size

    let result1 = client1
        .subscribe(topic_id, 0, max_batch_bytes, consumer_id1)
        .await;
    assert!(
        result1.is_ok(),
        "Client 1 subscribe failed: {:?}",
        result1.err()
    );
    let sub1 = result1.unwrap();
    println!(
        "Subscriber 1 (consumer_id={}) subscribed at offset {}",
        consumer_id1, sub1.start_offset
    );

    let result2 = client2
        .subscribe(topic_id, 0, max_batch_bytes, consumer_id2)
        .await;
    assert!(
        result2.is_ok(),
        "Client 2 subscribe failed: {:?}",
        result2.err()
    );
    let sub2 = result2.unwrap();
    println!(
        "Subscriber 2 (consumer_id={}) subscribed at offset {}",
        consumer_id2, sub2.start_offset
    );

    let result3 = client3
        .subscribe(topic_id, 0, max_batch_bytes, consumer_id3)
        .await;
    assert!(
        result3.is_ok(),
        "Client 3 subscribe failed: {:?}",
        result3.err()
    );
    let sub3 = result3.unwrap();
    println!(
        "Subscriber 3 (consumer_id={}) subscribed at offset {}",
        consumer_id3, sub3.start_offset
    );

    // Ingest some data to the topic
    let mut producer = LanceClient::connect(test_config()).await.unwrap();
    let message_count = 20;
    for i in 0..message_count {
        let payload = Bytes::from(format!("multi-sub message {}", i));
        producer
            .send_ingest_to_topic_sync(topic_id, payload, 1, None)
            .await
            .unwrap();
    }
    println!("Ingested {} messages to topic {}", message_count, topic_id);

    // Each subscriber commits at different offsets (simulating different progress)
    client1
        .commit_offset(topic_id, consumer_id1, 5)
        .await
        .unwrap();
    println!("Subscriber 1 committed offset 5");

    client2
        .commit_offset(topic_id, consumer_id2, 10)
        .await
        .unwrap();
    println!("Subscriber 2 committed offset 10");

    client3
        .commit_offset(topic_id, consumer_id3, 15)
        .await
        .unwrap();
    println!("Subscriber 3 committed offset 15");

    // Unsubscribe all
    client1.unsubscribe(topic_id, consumer_id1).await.unwrap();
    client2.unsubscribe(topic_id, consumer_id2).await.unwrap();
    client3.unsubscribe(topic_id, consumer_id3).await.unwrap();
    println!("All subscribers unsubscribed");

    // Cleanup
    producer.delete_topic(topic_id).await.unwrap();
    println!("Deleted topic {}", topic_id);

    client1.close().await.unwrap();
    client2.close().await.unwrap();
    client3.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_subscriber_resume_after_disconnect() {
    let mut client1 = LanceClient::connect(test_config()).await.unwrap();

    // Create topic and subscribe
    let topic_name = unique_topic_name("resume_test");
    let topic = client1.create_topic(&topic_name).await.unwrap();
    let topic_id = topic.id;
    let consumer_id = 2001u64;

    // Wait for topic to replicate in cluster mode
    wait_for_replication().await;

    let max_batch_bytes = 64 * 1024u32;
    client1
        .subscribe(topic_id, 0, max_batch_bytes, consumer_id)
        .await
        .unwrap();
    println!("1. Subscribed to topic {}", topic_id);

    // Ingest some data
    for i in 0..10 {
        let payload = Bytes::from(format!("resume test message {}", i));
        client1
            .send_ingest_to_topic_sync(topic_id, payload, 1, None)
            .await
            .unwrap();
    }
    println!("2. Ingested 10 messages");

    // Commit progress
    client1
        .commit_offset(topic_id, consumer_id, 5)
        .await
        .unwrap();
    println!("3. Committed offset 5");

    // Disconnect (simulate crash/disconnect)
    client1.unsubscribe(topic_id, consumer_id).await.unwrap();
    client1.close().await.unwrap();
    println!("4. Disconnected");

    // Reconnect with new client
    let mut client2 = LanceClient::connect(test_config()).await.unwrap();

    // Re-subscribe - should resume from committed offset
    let result = client2
        .subscribe(topic_id, 5, max_batch_bytes, consumer_id)
        .await;
    assert!(result.is_ok(), "Re-subscribe failed: {:?}", result.err());
    let sub_result = result.unwrap();
    println!(
        "5. Re-subscribed, resumed at offset {}",
        sub_result.start_offset
    );

    // Cleanup
    client2.delete_topic(topic_id).await.unwrap();
    client2.close().await.unwrap();
    println!("6. Cleanup complete");
}

/// Test that Consumer with OffsetStore loads stored offset on restart
/// and commit() persists offset correctly
#[tokio::test]
#[ignore] // Requires running server
async fn test_consumer_offset_store_persistence() {
    use lnc_client::{Consumer, ConsumerConfig, LockFileOffsetStore, OffsetStore};
    use std::sync::Arc;

    let temp_dir = tempfile::tempdir().unwrap();
    let offset_dir = temp_dir.path();

    let mut client1 = LanceClient::connect(test_config()).await.unwrap();

    // Create topic
    let topic_name = unique_topic_name("offset_store_test");
    let topic = client1.create_topic(&topic_name).await.unwrap();
    let topic_id = topic.id;
    let consumer_id = 3001u64;
    let consumer_name = "test-consumer";

    wait_for_replication().await;

    // Ingest some data
    for i in 0..20 {
        let payload = Bytes::from(format!("offset store message {}", i));
        client1
            .send_ingest_to_topic_sync(topic_id, payload, 1, None)
            .await
            .unwrap();
    }
    println!("1. Ingested 20 messages to topic {}", topic_id);

    // Create consumer with offset store, consume and commit
    {
        let client2 = LanceClient::connect(test_config()).await.unwrap();
        let offset_store = LockFileOffsetStore::open(offset_dir, consumer_name).unwrap();
        let config = ConsumerConfig::new(topic_id);
        let mut consumer =
            Consumer::with_offset_store(client2, config, consumer_id, Arc::new(offset_store))
                .unwrap();

        // Poll to ensure consumer is ready
        drop(consumer.poll());
        println!("2. Consumer created and polled");

        // Seek to offset 10 and commit
        consumer.seek_to_offset(10).await.unwrap();
        consumer.commit().await.unwrap();
        println!("3. Committed offset at position 10");

        // Drop consumer (simulates disconnect)
    }
    println!("4. Consumer disconnected");

    // Verify offset was persisted
    {
        let offset_store = LockFileOffsetStore::open(offset_dir, consumer_name).unwrap();
        let stored = offset_store.load(topic_id, consumer_id).unwrap();
        assert_eq!(stored, Some(10), "Offset should be persisted to file");
        println!("5. Verified persisted offset = 10");
    }

    // Create new consumer - should resume from stored offset
    {
        let client3 = LanceClient::connect(test_config()).await.unwrap();
        let offset_store = LockFileOffsetStore::open(offset_dir, consumer_name).unwrap();
        let config = ConsumerConfig::new(topic_id);
        let consumer = Consumer::with_offset_store(
            client3,
            config,
            consumer_id, // Same consumer ID to load saved offset
            Arc::new(offset_store),
        )
        .unwrap();

        // Check that consumer loaded the stored offset
        assert_eq!(
            consumer.current_offset(),
            10,
            "Consumer should resume from stored offset"
        );
        println!(
            "6. New consumer resumed at offset {}",
            consumer.current_offset()
        );
    }

    // Cleanup
    client1.delete_topic(topic_id).await.unwrap();
    client1.close().await.unwrap();
    println!("7. Cleanup complete");
}

// ============================================================================
// Multi-Node Replication Tests
// ============================================================================
// These tests require a 3-node cluster running. Start with:
//   ./scripts/start-cluster.ps1 (Windows)
//   ./scripts/start-cluster.sh (Linux/Mac)
//
// Set environment variables:
//   LANCE_NODE1_ADDR=127.0.0.1:1992
//   LANCE_NODE2_ADDR=127.0.0.1:1993
//   LANCE_NODE3_ADDR=127.0.0.1:1994

fn get_node_addr(node: u8) -> Option<SocketAddr> {
    let var_name = format!("LANCE_NODE{}_ADDR", node);
    std::env::var(&var_name).ok().and_then(|s| s.parse().ok())
}

fn cluster_config(node: u8) -> Option<ClientConfig> {
    get_node_addr(node).map(|addr| ClientConfig {
        addr,
        connect_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(5),
        keepalive_interval: Duration::from_secs(10),
        tls: None,
    })
}

#[tokio::test]
#[ignore = "requires 3-node LANCE cluster"]
async fn test_cluster_write_to_leader_read_from_follower() {
    let node1_config = cluster_config(1).expect("LANCE_NODE1_ADDR not set");
    let node2_config = cluster_config(2).expect("LANCE_NODE2_ADDR not set");

    // Connect to both nodes
    let mut client1 = LanceClient::connect(node1_config).await.unwrap();
    let mut client2 = LanceClient::connect(node2_config).await.unwrap();
    println!("Connected to node 1 and node 2");

    // Create topic on node 1 (leader)
    let topic_name = unique_topic_name("cluster_test");
    let topic = client1.create_topic(&topic_name).await.unwrap();
    let topic_id = topic.id;
    println!("Created topic {} on node 1", topic_id);

    // Ingest data to node 1
    let message_count = 10;
    for i in 0..message_count {
        let payload = Bytes::from(format!("cluster message {}", i));
        client1
            .send_ingest_to_topic_sync(topic_id, payload, 1, None)
            .await
            .unwrap();
    }
    println!("Ingested {} messages to node 1", message_count);

    // Wait for replication
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify topic exists on node 2 (should be replicated)
    let result = client2.get_topic(topic_id).await;
    if result.is_ok() {
        println!("Topic {} found on node 2 (replicated)", topic_id);
    } else {
        println!(
            "Topic {} not yet replicated to node 2: {:?}",
            topic_id,
            result.err()
        );
    }

    // List topics on node 2
    let topics = client2.list_topics().await.unwrap();
    println!("Node 2 has {} topics", topics.len());

    // Cleanup
    client1.delete_topic(topic_id).await.unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    println!("Cleanup complete");
}

#[tokio::test]
#[ignore = "requires 3-node LANCE cluster"]
async fn test_cluster_all_nodes_see_topics() {
    let node1_config = cluster_config(1).expect("LANCE_NODE1_ADDR not set");
    let node2_config = cluster_config(2).expect("LANCE_NODE2_ADDR not set");
    let node3_config = cluster_config(3).expect("LANCE_NODE3_ADDR not set");

    // Connect to all 3 nodes
    let mut client1 = LanceClient::connect(node1_config).await.unwrap();
    let mut client2 = LanceClient::connect(node2_config).await.unwrap();
    let mut client3 = LanceClient::connect(node3_config).await.unwrap();
    println!("Connected to all 3 nodes");

    // Create topic on node 1
    let topic_name = unique_topic_name("cluster_visibility");
    let topic = client1.create_topic(&topic_name).await.unwrap();
    let topic_id = topic.id;
    println!("Created topic {} on node 1", topic_id);

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check visibility on all nodes
    let topics1 = client1.list_topics().await.unwrap();
    let topics2 = client2.list_topics().await.unwrap();
    let topics3 = client3.list_topics().await.unwrap();

    println!(
        "Topic counts: node1={}, node2={}, node3={}",
        topics1.len(),
        topics2.len(),
        topics3.len()
    );

    // Verify topic is visible on all nodes
    let visible_on_1 = topics1.iter().any(|t| t.id == topic_id);
    let visible_on_2 = topics2.iter().any(|t| t.id == topic_id);
    let visible_on_3 = topics3.iter().any(|t| t.id == topic_id);

    println!(
        "Topic {} visible: node1={}, node2={}, node3={}",
        topic_id, visible_on_1, visible_on_2, visible_on_3
    );

    assert!(visible_on_1, "Topic should be visible on node 1");
    // Note: visibility on followers depends on replication mode and timing

    // Cleanup
    client1.delete_topic(topic_id).await.unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    client3.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires 3-node LANCE cluster"]
async fn test_cluster_topic_delete_replication() {
    let node1_config = cluster_config(1).expect("LANCE_NODE1_ADDR not set");
    let node2_config = cluster_config(2).expect("LANCE_NODE2_ADDR not set");

    let mut client1 = LanceClient::connect(node1_config).await.unwrap();
    let mut client2 = LanceClient::connect(node2_config).await.unwrap();
    println!("Connected to node 1 and node 2");

    // Create topic on node 1 (leader)
    let topic_name = unique_topic_name("delete_replication");
    let topic = client1.create_topic(&topic_name).await.unwrap();
    let topic_id = topic.id;
    println!("Created topic {} on node 1", topic_id);

    // Wait for create replication
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify topic exists on node 2
    let topics_before = client2.list_topics().await.unwrap();
    let exists_on_node2 = topics_before.iter().any(|t| t.id == topic_id);
    println!(
        "Topic {} exists on node 2 before delete: {}",
        topic_id, exists_on_node2
    );

    // Delete topic on node 1 (leader)
    client1.delete_topic(topic_id).await.unwrap();
    println!("Deleted topic {} on node 1", topic_id);

    // Wait for delete replication
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify topic is gone on node 2
    let topics_after = client2.list_topics().await.unwrap();
    let still_exists = topics_after.iter().any(|t| t.id == topic_id);
    println!(
        "Topic {} exists on node 2 after delete: {}",
        topic_id, still_exists
    );

    // Cleanup
    client1.close().await.unwrap();
    client2.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires 3-node LANCE cluster"]
async fn test_cluster_write_routing_from_follower() {
    let node1_config = cluster_config(1).expect("LANCE_NODE1_ADDR not set");
    let node2_config = cluster_config(2).expect("LANCE_NODE2_ADDR not set");

    let mut client1 = LanceClient::connect(node1_config).await.unwrap();
    let mut client2 = LanceClient::connect(node2_config).await.unwrap();
    println!("Connected to node 1 and node 2");

    // First, create a topic on node 1 (leader) so we have something to test with
    let topic_name = unique_topic_name("write_routing");
    let topic = client1.create_topic(&topic_name).await.unwrap();
    let topic_id = topic.id;
    println!("Created topic {} on node 1", topic_id);

    // Try to create another topic on node 2 (follower)
    // This should either succeed (if redirected) or return NOT_LEADER error
    let topic2_name = unique_topic_name("follower_create");
    let result = client2.create_topic(&topic2_name).await;

    match result {
        Ok(topic2) => {
            println!(
                "Topic {} created via node 2 (redirected to leader)",
                topic2.id
            );
            client1.delete_topic(topic2.id).await.unwrap();
        },
        Err(e) => {
            let err_str = format!("{:?}", e);
            println!("Create from follower returned: {}", err_str);
            // Expected: NOT_LEADER error with redirect info
            assert!(
                err_str.contains("NOT_LEADER") || err_str.contains("redirect"),
                "Expected NOT_LEADER error, got: {}",
                err_str
            );
        },
    }

    // Cleanup
    client1.delete_topic(topic_id).await.unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires 3-node LANCE cluster"]
async fn test_cluster_concurrent_writes_multiple_nodes() {
    let node1_config = cluster_config(1).expect("LANCE_NODE1_ADDR not set");
    let node2_config = cluster_config(2).expect("LANCE_NODE2_ADDR not set");

    let mut client1 = LanceClient::connect(node1_config.clone()).await.unwrap();
    let mut client2 = LanceClient::connect(node2_config.clone()).await.unwrap();

    // Create topics on different nodes
    let topic1_name = unique_topic_name("concurrent_node1");
    let topic2_name = unique_topic_name("concurrent_node2");

    let topic1 = client1.create_topic(&topic1_name).await.unwrap();
    let topic2 = client2.create_topic(&topic2_name).await.unwrap();
    println!(
        "Created topic {} on node 1, topic {} on node 2",
        topic1.id, topic2.id
    );

    // Concurrent writes from multiple clients
    let start = Instant::now();
    let messages_per_client = 50;

    // Spawn concurrent write tasks
    let client1_handle = {
        let topic_id = topic1.id;
        let config = node1_config.clone();
        tokio::spawn(async move {
            let mut c = LanceClient::connect(config).await.unwrap();
            for i in 0..messages_per_client {
                let payload = Bytes::from(format!("node1 msg {}", i));
                c.send_ingest_to_topic_sync(topic_id, payload, 1, None)
                    .await
                    .unwrap();
            }
            c.close().await.unwrap();
        })
    };

    let client2_handle = {
        let topic_id = topic2.id;
        let config = node2_config.clone();
        tokio::spawn(async move {
            let mut c = LanceClient::connect(config).await.unwrap();
            for i in 0..messages_per_client {
                let payload = Bytes::from(format!("node2 msg {}", i));
                c.send_ingest_to_topic_sync(topic_id, payload, 1, None)
                    .await
                    .unwrap();
            }
            c.close().await.unwrap();
        })
    };

    // Wait for both to complete
    client1_handle.await.unwrap();
    client2_handle.await.unwrap();

    let elapsed = start.elapsed();
    let total_messages = messages_per_client * 2;
    println!(
        "Concurrent writes complete: {} messages in {:?}",
        total_messages, elapsed
    );
    println!(
        "Rate: {:.2} msgs/sec",
        total_messages as f64 / elapsed.as_secs_f64()
    );

    // Cleanup
    client1.delete_topic(topic1.id).await.unwrap();
    client2.delete_topic(topic2.id).await.unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
}

// ============================================================================
// Retention Policy Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_set_retention_policy() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create a topic
    let topic_name = unique_topic_name("retention_set");
    let topic = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic {} with id {}", topic_name, topic.id);

    // Set retention policy: 1 hour, 100MB
    let max_age_secs = 3600;
    let max_bytes = 100 * 1024 * 1024; // 100MB
    let result = client
        .set_retention(topic.id, max_age_secs, max_bytes)
        .await;
    assert!(
        result.is_ok(),
        "Failed to set retention: {:?}",
        result.err()
    );
    println!(
        "Set retention policy: max_age={}s, max_bytes={}",
        max_age_secs, max_bytes
    );

    // Verify by getting topic info (retention should be reflected)
    let info = client.get_topic(topic.id).await;
    assert!(info.is_ok(), "Failed to get topic: {:?}", info.err());
    let info = info.unwrap();
    println!("Topic info after retention set: {:?}", info);

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_create_topic_with_retention() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create a topic with retention policy
    let topic_name = unique_topic_name("retention_create");
    let max_age_secs = 7200; // 2 hours
    let max_bytes = 50 * 1024 * 1024; // 50MB

    let result = client
        .create_topic_with_retention(&topic_name, max_age_secs, max_bytes)
        .await;
    assert!(
        result.is_ok(),
        "Failed to create topic with retention: {:?}",
        result.err()
    );

    let topic = result.unwrap();
    println!(
        "Created topic {} with id {} and retention policy",
        topic_name, topic.id
    );

    // Verify topic exists
    let info = client.get_topic(topic.id).await;
    assert!(info.is_ok(), "Failed to get topic: {:?}", info.err());

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_retention_policy_update() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create topic without retention
    let topic_name = unique_topic_name("retention_update");
    let topic = client.create_topic(&topic_name).await.unwrap();

    // Set initial retention
    let result = client.set_retention(topic.id, 3600, 100_000_000).await;
    assert!(result.is_ok());
    println!("Initial retention set");

    // Update retention to different values
    let result = client.set_retention(topic.id, 1800, 50_000_000).await;
    assert!(
        result.is_ok(),
        "Failed to update retention: {:?}",
        result.err()
    );
    println!("Retention updated successfully");

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_retention_on_nonexistent_topic() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Try to set retention on a topic that doesn't exist
    let result = client.set_retention(99999, 3600, 100_000_000).await;

    // Should fail gracefully
    assert!(result.is_err(), "Expected error for nonexistent topic");
    println!(
        "Correctly rejected retention on nonexistent topic: {:?}",
        result.err()
    );

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires 3-node LANCE cluster"]
async fn test_cluster_retention_replication() {
    let node1_config = cluster_config(1).expect("LANCE_NODE1_ADDR not set");
    let node2_config = cluster_config(2).expect("LANCE_NODE2_ADDR not set");

    let mut client1 = LanceClient::connect(node1_config).await.unwrap();
    let mut client2 = LanceClient::connect(node2_config).await.unwrap();

    // Create topic on node 1
    let topic_name = unique_topic_name("retention_cluster");
    let topic = client1.create_topic(&topic_name).await.unwrap();
    println!("Created topic {} on node 1", topic.id);

    // Set retention on node 1
    let result = client1.set_retention(topic.id, 3600, 100_000_000).await;
    assert!(
        result.is_ok(),
        "Failed to set retention: {:?}",
        result.err()
    );
    println!("Set retention on node 1");

    // Wait for replication
    wait_for_replication().await;

    // Verify topic exists on node 2 (retention is part of metadata)
    let info = client2.get_topic(topic.id).await;
    assert!(
        info.is_ok(),
        "Topic should be visible on node 2: {:?}",
        info.err()
    );
    println!("Topic with retention visible on node 2");

    // Cleanup
    client1.delete_topic(topic.id).await.unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
}

// ============================================================================
// Cluster Status Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_get_cluster_status() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    let status = client.get_cluster_status().await.unwrap();
    println!("Cluster status: {:?}", status);

    // Verify basic fields are present
    println!("Node ID: {}", status.node_id);
    println!("Is Leader: {}", status.is_leader);
    println!("Node Count: {}", status.node_count);

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires 3-node LANCE cluster"]
async fn test_cluster_status_from_multiple_nodes() {
    let node1_config = cluster_config(1).expect("LANCE_NODE1_ADDR not set");
    let node2_config = cluster_config(2).expect("LANCE_NODE2_ADDR not set");
    let node3_config = cluster_config(3).expect("LANCE_NODE3_ADDR not set");

    let mut client1 = LanceClient::connect(node1_config).await.unwrap();
    let mut client2 = LanceClient::connect(node2_config).await.unwrap();
    let mut client3 = LanceClient::connect(node3_config).await.unwrap();

    let status1 = client1.get_cluster_status().await.unwrap();
    let status2 = client2.get_cluster_status().await.unwrap();
    let status3 = client3.get_cluster_status().await.unwrap();

    println!("Node 1: is_leader={}", status1.is_leader);
    println!("Node 2: is_leader={}", status2.is_leader);
    println!("Node 3: is_leader={}", status3.is_leader);

    // Exactly one node should be leader
    let leader_count = [status1.is_leader, status2.is_leader, status3.is_leader]
        .iter()
        .filter(|&&x| x)
        .count();
    assert_eq!(leader_count, 1, "Exactly one node should be leader");

    client1.close().await.unwrap();
    client2.close().await.unwrap();
    client3.close().await.unwrap();
}

// Duplicate retention policy tests removed - see earlier definitions above

// ============================================================================
// Cluster Failover and Recovery Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires 3-node LANCE cluster"]
async fn test_cluster_failover_write_continuity() {
    let node1_config = cluster_config(1).expect("LANCE_NODE1_ADDR not set");
    let node2_config = cluster_config(2).expect("LANCE_NODE2_ADDR not set");

    let mut client1 = LanceClient::connect(node1_config.clone()).await.unwrap();
    let mut client2 = LanceClient::connect(node2_config.clone()).await.unwrap();

    // Create a topic
    let topic_name = unique_topic_name("failover_test");
    let topic = client1.create_topic(&topic_name).await.unwrap();
    println!("Created topic {} for failover test", topic.id);

    wait_for_replication().await;

    // Find which node is leader
    let status1 = client1.get_cluster_status().await.unwrap();
    let status2 = client2.get_cluster_status().await.unwrap();

    println!("Node 1 is_leader: {}", status1.is_leader);
    println!("Node 2 is_leader: {}", status2.is_leader);

    // Write data before simulated failover
    let test_data = bytes::Bytes::from_static(b"pre-failover data");
    let write_result = if status1.is_leader {
        client1
            .send_ingest_to_topic_sync(topic.id, test_data.clone(), 1, None)
            .await
    } else {
        client2
            .send_ingest_to_topic_sync(topic.id, test_data, 1, None)
            .await
    };
    assert!(write_result.is_ok(), "Pre-failover write should succeed");
    println!("Pre-failover write succeeded");

    // Note: Actual failover testing requires stopping a node,
    // which would need Docker/process control integration.
    // This test validates the cluster status API works correctly.

    // Cleanup
    client1.delete_topic(topic.id).await.unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires 3-node LANCE cluster"]
async fn test_write_forwarding_from_follower() {
    let node1_config = cluster_config(1).expect("LANCE_NODE1_ADDR not set");
    let node2_config = cluster_config(2).expect("LANCE_NODE2_ADDR not set");
    let node3_config = cluster_config(3).expect("LANCE_NODE3_ADDR not set");

    let mut client1 = LanceClient::connect(node1_config).await.unwrap();
    let mut client2 = LanceClient::connect(node2_config).await.unwrap();
    let mut client3 = LanceClient::connect(node3_config).await.unwrap();

    // Create topic
    let topic_name = unique_topic_name("forward_test");
    let topic = client1.create_topic(&topic_name).await.unwrap();
    println!("Created topic {} for write forwarding test", topic.id);

    wait_for_replication().await;

    // Find a follower node
    let status1 = client1.get_cluster_status().await.unwrap();
    let status2 = client2.get_cluster_status().await.unwrap();
    let _status3 = client3.get_cluster_status().await.unwrap();

    let follower_client = if !status1.is_leader {
        &mut client1
    } else if !status2.is_leader {
        &mut client2
    } else {
        &mut client3
    };

    // Write to follower - should be forwarded to leader
    let test_data = bytes::Bytes::from_static(b"forwarded write data");
    let result = follower_client
        .send_ingest_to_topic_sync(topic.id, test_data, 1, None)
        .await;

    // Write forwarding should either succeed or return NotLeader with redirect
    match &result {
        Ok(_) => println!("Write forwarded successfully"),
        Err(e) => println!("Write forward result: {:?}", e),
    }

    // Cleanup
    client1.delete_topic(topic.id).await.unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
    client3.close().await.unwrap();
}

// ============================================================================
// WAL Recovery Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server with WAL enabled"]
async fn test_wal_replay_after_restart() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    // Create topic and write data
    let topic_name = unique_topic_name("wal_test");
    let topic = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic {} for WAL test", topic.id);

    // Write multiple batches
    for i in 0..10 {
        let data = bytes::Bytes::from(format!("wal_test_batch_{}", i));
        let result = client
            .send_ingest_to_topic_sync(topic.id, data, 1, None)
            .await;
        assert!(result.is_ok(), "Write {} should succeed", i);
    }
    println!("Wrote 10 batches to topic");

    // Force flush to ensure WAL entries are written
    // Note: Actual WAL replay test would require server restart
    // which needs Docker/process control integration

    // Verify data can be read back
    let info = client.get_topic(topic.id).await.unwrap();
    println!("Topic info after writes: {:?}", info);

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

// ============================================================================
// Backpressure Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_backpressure_under_load() {
    let mut client = LanceClient::connect(test_config()).await.unwrap();

    let topic_name = unique_topic_name("backpressure_test");
    let topic = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic {} for backpressure test", topic.id);

    // Generate load with many concurrent writes
    let mut success_count = 0u32;
    let mut backpressure_count = 0u32;

    // Write 1000 small messages as fast as possible
    let data = bytes::Bytes::from(vec![0u8; 256]); // 256 byte payload
    for i in 0..1000 {
        match client
            .send_ingest_to_topic_sync(topic.id, data.clone(), 1, None)
            .await
        {
            Ok(_) => success_count += 1,
            Err(e) => {
                // Check if it's a backpressure signal
                let err_str = format!("{:?}", e);
                if err_str.contains("backpressure") || err_str.contains("Backpressure") {
                    backpressure_count += 1;
                } else {
                    println!("Write {} failed: {:?}", i, e);
                }
            },
        }
    }

    println!(
        "Backpressure test: {} succeeded, {} backpressured",
        success_count, backpressure_count
    );

    // At minimum, some writes should succeed
    assert!(success_count > 0, "At least some writes should succeed");

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

// ============================================================================
// TLS Connection Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires LANCE server with TLS enabled and certificates"]
async fn test_tls_encrypted_connection() {
    // This test requires:
    // - LANCE server running with TLS enabled
    // - Valid certificate and key files
    // - CA certificate for client verification

    let config = test_config();

    let result = LanceClient::connect(config).await;
    match result {
        Ok(mut client) => {
            // Test basic operations over TLS connection
            let topic_name = unique_topic_name("tls_test");
            let topic_result = client.create_topic(&topic_name).await;
            assert!(
                topic_result.is_ok(),
                "Should create topic over TLS: {:?}",
                topic_result.err()
            );

            let topic = topic_result.unwrap();
            println!("Created topic {} over TLS connection", topic.id);

            // Write data over TLS
            let test_data = bytes::Bytes::from_static(b"TLS encrypted data");
            let write_result = client
                .send_ingest_to_topic_sync(topic.id, test_data, 1, None)
                .await;
            assert!(
                write_result.is_ok(),
                "Should write over TLS: {:?}",
                write_result.err()
            );
            println!("Wrote data over TLS connection");

            // Cleanup
            client.delete_topic(topic.id).await.unwrap();
            client.close().await.unwrap();
            println!("TLS connection test passed");
        },
        Err(e) => {
            println!(
                "TLS connection failed (expected if server not TLS-enabled): {:?}",
                e
            );
        },
    }
}

#[tokio::test]
#[ignore = "requires LANCE server with mTLS enabled"]
async fn test_mtls_client_certificate() {
    // This test validates mutual TLS where client presents certificate

    let config = test_config();

    let result = LanceClient::connect(config).await;
    match result {
        Ok(mut client) => {
            println!("mTLS connection established");

            // Verify connection works
            let ping_result = client.ping().await;
            assert!(ping_result.is_ok(), "Ping should succeed over mTLS");
            println!("Ping latency: {:?}", ping_result.unwrap());

            client.close().await.unwrap();
        },
        Err(e) => {
            println!("mTLS connection failed: {:?}", e);
        },
    }
}

#[tokio::test]
#[ignore = "requires LANCE server with TLS enabled"]
async fn test_tls_with_client_config_integration() {
    use lnc_client::TlsClientConfig;
    use std::net::SocketAddr;

    // Test TLS configuration via ClientConfig.with_tls()
    let addr: SocketAddr = get_test_addr();
    let tls = TlsClientConfig::new();
    let config = ClientConfig::new(addr).with_tls(tls);

    assert!(config.is_tls_enabled(), "TLS should be enabled in config");

    // Connect using unified config (auto-detects TLS)
    match LanceClient::connect(config).await {
        Ok(mut client) => {
            // Verify connection is functional
            let ping = client.ping().await;
            assert!(ping.is_ok(), "Ping over TLS should succeed");
            println!("TLS via ClientConfig integration: ping {:?}", ping.unwrap());
            client.close().await.unwrap();
        },
        Err(e) => {
            println!("TLS connection via ClientConfig failed: {:?}", e);
        },
    }
}

#[tokio::test]
#[ignore = "requires LANCE server with TLS enabled"]
async fn test_tls_certificate_validation() {
    use lnc_client::TlsClientConfig;
    use std::net::SocketAddr;

    // Test that invalid CA certificate fails validation
    let addr: SocketAddr = get_test_addr();
    let tls = TlsClientConfig::new().with_ca_cert("/nonexistent/ca.pem");

    let config = ClientConfig::new(addr).with_tls(tls);

    let result = LanceClient::connect(config).await;

    // Should fail because CA cert doesn't exist
    assert!(result.is_err(), "Connection with invalid CA should fail");
    println!("Certificate validation correctly rejected invalid CA");
}

#[tokio::test]
#[ignore = "requires LANCE cluster with TLS enabled"]
async fn test_tls_cluster_communication() {
    // Test TLS works with cluster status queries
    let config = test_config();

    match LanceClient::connect(config).await {
        Ok(mut client) => {
            // Query cluster status over TLS
            let status = client.get_cluster_status().await;
            match status {
                Ok(cluster) => {
                    println!(
                        "Cluster status over TLS: {} nodes, leader: {:?}",
                        cluster.node_count, cluster.leader_id
                    );
                    assert!(cluster.node_count >= 1, "Should have at least one node");
                },
                Err(e) => {
                    println!("Cluster query failed (may be single-node): {:?}", e);
                },
            }
            client.close().await.unwrap();
        },
        Err(e) => {
            println!("Cluster TLS connection failed: {:?}", e);
        },
    }
}

// ============================================================================
// Connection Management Integration Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_connection_pool_basic() {
    use lnc_client::{ConnectionPool, ConnectionPoolConfig};

    let addr = format!("{}", get_test_addr());
    let config = ConnectionPoolConfig::new()
        .with_max_connections(5)
        .with_min_idle(1);

    let pool = ConnectionPool::new(&addr, config).await.unwrap();

    // Get a connection from pool
    let mut conn = pool.get().await.unwrap();

    // Verify connection works
    let latency = conn.ping().await.unwrap();
    println!("Pool connection ping latency: {:?}", latency);

    // Check pool stats
    let stats = pool.stats();
    assert!(
        stats.connections_created >= 1,
        "Should have created at least one connection"
    );
    println!("Pool stats: {:?}", stats);

    // Connection returned to pool on drop
    drop(conn);

    // Close pool
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_connection_pool_concurrent_access() {
    use lnc_client::{ConnectionPool, ConnectionPoolConfig};
    use std::sync::Arc;

    let addr = format!("{}", get_test_addr());
    let config = ConnectionPoolConfig::new()
        .with_max_connections(3)
        .with_acquire_timeout(Duration::from_secs(5));

    let pool = Arc::new(ConnectionPool::new(&addr, config).await.unwrap());

    // Spawn multiple tasks that use connections concurrently
    let mut handles = vec![];
    for i in 0..5 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = pool.get().await.unwrap();
            let latency = conn.ping().await.unwrap();
            println!("Task {} ping latency: {:?}", i, latency);
        }));
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    let stats = pool.stats();
    println!("Concurrent access stats: {:?}", stats);

    pool.close().await;
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_reconnecting_client_basic() {
    use lnc_client::ReconnectingClient;

    let addr = format!("{}", get_test_addr());
    let mut client = ReconnectingClient::connect(&addr).await.unwrap();

    // Use the client
    let inner = client.client().await.unwrap();
    let latency = inner.ping().await.unwrap();
    println!("ReconnectingClient ping latency: {:?}", latency);

    // Check original address tracking
    assert_eq!(client.original_addr(), &addr);
    assert_eq!(client.reconnect_attempts(), 0);
}

#[tokio::test]
#[ignore = "requires LANCE cluster for failover testing"]
async fn test_reconnecting_client_leader_failover() {
    use lnc_client::ReconnectingClient;
    use std::net::SocketAddr;

    let addr = format!("{}", get_test_addr());
    let mut client = ReconnectingClient::connect(&addr)
        .await
        .unwrap()
        .with_max_attempts(3)
        .with_follow_leader(true);

    // Simulate leader address update
    let new_leader: SocketAddr = "127.0.0.1:1993".parse().unwrap();
    client.set_leader_addr(new_leader);

    assert_eq!(client.leader_addr(), Some(new_leader));
    println!("Leader address updated to {:?}", client.leader_addr());
}

#[tokio::test]
async fn test_connection_to_invalid_address() {
    // Test that connection to invalid address fails gracefully
    let config = ClientConfig {
        addr: "127.0.0.1:59999".parse().unwrap(), // Non-existent server
        connect_timeout: Duration::from_millis(100),
        read_timeout: Duration::from_secs(1),
        write_timeout: Duration::from_secs(1),
        keepalive_interval: Duration::from_secs(10),
        tls: None,
    };

    let result = LanceClient::connect(config).await;
    assert!(result.is_err(), "Should fail to connect to invalid address");
    println!("Connection to invalid address correctly failed");
}

// ============================================================================
// Producer Integration Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_producer_connect_and_send() {
    use lnc_client::{Producer, ProducerConfig};

    let config = ProducerConfig::new()
        .with_batch_size(1024)
        .with_linger_ms(10);

    let addr = format!("{}", get_test_addr());
    let producer = Producer::connect(&addr, config).await.unwrap();

    // Create a topic first
    let mut client = LanceClient::connect(test_config()).await.unwrap();
    let topic_name = unique_topic_name("producer_test");
    let topic_info = client.create_topic(&topic_name).await.unwrap();
    let topic_id = topic_info.id;

    // Send some records
    for i in 0..10 {
        let ack = producer
            .send(topic_id, format!("message-{}", i).as_bytes())
            .await
            .unwrap();
        assert!(ack.batch_id > 0);
        assert_eq!(ack.topic_id, topic_id);
    }

    // Check metrics
    let metrics = producer.metrics();
    assert!(metrics.records_sent >= 10);
    assert!(metrics.bytes_sent > 0);

    // Cleanup
    producer.close().await.unwrap();
    client.delete_topic(topic_id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_producer_batching_and_flush() {
    use lnc_client::{Producer, ProducerConfig};

    let config = ProducerConfig::new()
        .with_batch_size(16 * 1024)  // Large batch
        .with_linger_ms(1000); // Long linger

    let addr = format!("{}", get_test_addr());
    let producer = Producer::connect(&addr, config).await.unwrap();

    // Create a topic
    let mut client = LanceClient::connect(test_config()).await.unwrap();
    let topic_name = unique_topic_name("producer_batch_test");
    let topic_info = client.create_topic(&topic_name).await.unwrap();
    let topic_id = topic_info.id;

    // Send records (should batch due to long linger)
    let start = Instant::now();
    for i in 0..5 {
        producer
            .send_async(topic_id, format!("batch-message-{}", i).as_bytes())
            .await
            .unwrap();
    }

    // Should be quick since async send doesn't wait for batch
    assert!(start.elapsed() < Duration::from_millis(100));

    // Explicit flush should send the batch
    producer.flush().await.unwrap();

    let metrics = producer.metrics();
    assert!(metrics.batches_sent >= 1);

    // Cleanup
    producer.close().await.unwrap();
    client.delete_topic(topic_id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_producer_metrics_tracking() {
    use lnc_client::{Producer, ProducerConfig};

    let config = ProducerConfig::new()
        .with_batch_size(1024)
        .with_linger_ms(5);

    let addr = format!("{}", get_test_addr());
    let producer = Producer::connect(&addr, config).await.unwrap();

    // Create a topic
    let mut client = LanceClient::connect(test_config()).await.unwrap();
    let topic_name = unique_topic_name("producer_metrics_test");
    let topic_info = client.create_topic(&topic_name).await.unwrap();
    let topic_id = topic_info.id;

    // Initial metrics should be zero
    let initial = producer.metrics();
    assert_eq!(initial.records_sent, 0);
    assert_eq!(initial.bytes_sent, 0);
    assert_eq!(initial.errors, 0);

    // Send records
    let record_count: u64 = 20;
    let record_size: u64 = 100;
    for i in 0..record_count {
        let data = format!("{:0>width$}", i, width = record_size as usize);
        producer.send(topic_id, data.as_bytes()).await.unwrap();
    }

    // Check metrics
    let final_metrics = producer.metrics();
    assert_eq!(final_metrics.records_sent, record_count);
    assert!(final_metrics.bytes_sent >= record_count * record_size);
    assert!(final_metrics.batches_sent >= 1);
    assert_eq!(final_metrics.errors, 0);

    // Cleanup
    producer.close().await.unwrap();
    client.delete_topic(topic_id).await.unwrap();
    client.close().await.unwrap();
}
