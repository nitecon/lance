use crate::{ClientConfig, LanceClient};
use bytes::Bytes;
use std::net::SocketAddr;
use std::time::Duration;

fn test_config() -> ClientConfig {
    ClientConfig {
        addr: std::env::var("LANCE_TEST_ADDR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 1992))),
        connect_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(5),
        keepalive_interval: Duration::from_secs(10),
        tls: None,
    }
}

// =============================================================================
// Connection Tests
// =============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_connect() {
    let config = test_config();
    let client = LanceClient::connect(config).await;
    assert!(client.is_ok(), "Failed to connect: {:?}", client.err());

    let client = client.unwrap();
    assert!(client.close().await.is_ok());
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_ping() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    let latency = client.ping().await;
    assert!(latency.is_ok(), "Ping failed: {:?}", latency.err());

    let latency = latency.unwrap();
    println!("Ping latency: {:?}", latency);
    assert!(
        latency < Duration::from_secs(1),
        "Ping took too long: {:?}",
        latency
    );

    client.close().await.unwrap();
}

// =============================================================================
// Ingest Tests
// =============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_single_ingest() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    let payload = Bytes::from_static(b"test payload data");
    let batch_id = client.send_ingest_sync(payload, 1).await;

    assert!(batch_id.is_ok(), "Ingest failed: {:?}", batch_id.err());
    assert_eq!(batch_id.unwrap(), 1);

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_multiple_ingests() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    for i in 1..=10 {
        let payload = Bytes::from(format!("payload {}", i));
        let batch_id = client.send_ingest_sync(payload, 1).await;

        assert!(
            batch_id.is_ok(),
            "Ingest {} failed: {:?}",
            i,
            batch_id.err()
        );
        assert_eq!(batch_id.unwrap(), i as u64);
    }

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_pipelined_ingests() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    let batch_count = 100;

    for i in 1..=batch_count {
        let payload = Bytes::from(vec![0u8; 1024]);
        let batch_id = client.send_ingest(payload, 1).await.unwrap();
        assert_eq!(batch_id, i as u64);
    }

    for i in 1..=batch_count {
        let acked_id = client.recv_ack().await.unwrap();
        assert_eq!(acked_id, i as u64);
    }

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_large_payload() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    let payload = Bytes::from(vec![0xAB; 1024 * 1024]);
    let batch_id = client.send_ingest_sync(payload, 1000).await;

    assert!(
        batch_id.is_ok(),
        "Large ingest failed: {:?}",
        batch_id.err()
    );

    client.close().await.unwrap();
}

// =============================================================================
// Topic Management Tests
// =============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_create_topic() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    let topic_name = format!(
        "test_topic_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let result = client.create_topic(&topic_name).await;
    assert!(result.is_ok(), "Create topic failed: {:?}", result.err());

    let topic = result.unwrap();
    assert_eq!(topic.name, topic_name);
    assert!(topic.id > 0, "Topic ID should be non-zero");
    println!("Created topic: id={}, name={}", topic.id, topic.name);

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_list_topics() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    let result = client.list_topics().await;
    assert!(result.is_ok(), "List topics failed: {:?}", result.err());

    let topics = result.unwrap();
    println!("Found {} topics", topics.len());
    for topic in &topics {
        println!("  - id={}, name={}", topic.id, topic.name);
    }

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_get_topic() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // First create a topic
    let topic_name = format!(
        "get_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let created = client.create_topic(&topic_name).await.unwrap();

    // Now get it by ID
    let result = client.get_topic(created.id).await;
    assert!(result.is_ok(), "Get topic failed: {:?}", result.err());

    let topic = result.unwrap();
    assert_eq!(topic.id, created.id);
    assert_eq!(topic.name, topic_name);

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_delete_topic() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // First create a topic
    let topic_name = format!(
        "delete_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let created = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic for deletion: id={}", created.id);

    // Delete it
    let result = client.delete_topic(created.id).await;
    assert!(result.is_ok(), "Delete topic failed: {:?}", result.err());
    println!("Successfully deleted topic id={}", created.id);

    // Verify it's gone (should error)
    let get_result = client.get_topic(created.id).await;
    assert!(get_result.is_err(), "Topic should not exist after deletion");

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_topic_lifecycle() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // 1. Create a new topic
    let topic_name = format!(
        "lifecycle_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let created = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic: id={}, name={}", created.id, created.name);

    // 2. Verify topic exists in list
    let topics = client.list_topics().await.unwrap();
    assert!(
        topics.iter().any(|t| t.id == created.id),
        "Created topic should be in list"
    );

    // 3. Ingest data to the topic
    let payload = Bytes::from_static(b"lifecycle test data");
    let batch_id = client
        .send_ingest_to_topic_sync(created.id, payload, 1, None)
        .await;
    assert!(
        batch_id.is_ok(),
        "Ingest to topic failed: {:?}",
        batch_id.err()
    );
    println!("Ingested data to topic, batch_id={}", batch_id.unwrap());

    // 4. Delete the topic
    client.delete_topic(created.id).await.unwrap();
    println!("Deleted topic id={}", created.id);

    // 5. Verify topic no longer exists in list
    let topics_after = client.list_topics().await.unwrap();
    assert!(
        !topics_after.iter().any(|t| t.id == created.id),
        "Deleted topic should not be in list"
    );

    client.close().await.unwrap();
}

// =============================================================================
// Topic Ingest Tests
// =============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_ingest_to_specific_topic() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Create a topic
    let topic_name = format!(
        "ingest_topic_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let topic = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic: id={}", topic.id);

    // Ingest multiple batches to this topic
    for i in 1..=5 {
        let payload = Bytes::from(format!("topic {} batch {}", topic.id, i));
        let batch_id = client
            .send_ingest_to_topic_sync(topic.id, payload, 1, None)
            .await;
        assert!(
            batch_id.is_ok(),
            "Ingest {} to topic failed: {:?}",
            i,
            batch_id.err()
        );
        println!("  Ingested batch {} to topic {}", i, topic.id);
    }

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_ingest_to_multiple_topics() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Create multiple topics
    let mut topic_ids = Vec::new();
    for i in 0..3 {
        let topic_name = format!(
            "multi_topic_{}_{}",
            i,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let topic = client.create_topic(&topic_name).await.unwrap();
        topic_ids.push(topic.id);
        println!("Created topic {}: id={}", i, topic.id);
    }

    // Ingest to each topic in round-robin
    for batch in 0..10 {
        let topic_id = topic_ids[batch % topic_ids.len()];
        let payload = Bytes::from(format!("batch {} to topic {}", batch, topic_id));
        let result = client
            .send_ingest_to_topic_sync(topic_id, payload, 1, None)
            .await;
        assert!(
            result.is_ok(),
            "Failed to ingest batch {} to topic {}: {:?}",
            batch,
            topic_id,
            result.err()
        );
    }
    println!("Successfully ingested 10 batches across 3 topics");

    // Cleanup
    for topic_id in topic_ids {
        client.delete_topic(topic_id).await.unwrap();
    }

    client.close().await.unwrap();
}

// =============================================================================
// Streaming Control Tests
// =============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_subscribe_to_topic() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Create a topic
    let topic_name = format!(
        "subscribe_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let topic = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic: id={}", topic.id);

    // Subscribe to the topic
    let consumer_id = 12345u64;
    let result = client.subscribe(topic.id, 0, 65536, consumer_id).await;
    assert!(result.is_ok(), "Subscribe failed: {:?}", result.err());

    let sub_result = result.unwrap();
    assert_eq!(sub_result.consumer_id, consumer_id);
    println!(
        "Subscribed: consumer_id={}, start_offset={}",
        sub_result.consumer_id, sub_result.start_offset
    );

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_subscribe_to_nonexistent_topic() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Try to subscribe to a topic that doesn't exist
    let result = client.subscribe(99999, 0, 65536, 12345).await;
    assert!(
        result.is_err(),
        "Subscribe to nonexistent topic should fail"
    );
    println!(
        "Subscribe to nonexistent topic correctly failed: {:?}",
        result.err()
    );

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_unsubscribe_from_topic() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Create a topic
    let topic_name = format!(
        "unsubscribe_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let topic = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic: id={}", topic.id);

    // Subscribe first
    let consumer_id = 54321u64;
    let sub_result = client
        .subscribe(topic.id, 0, 65536, consumer_id)
        .await
        .unwrap();
    println!("Subscribed: consumer_id={}", sub_result.consumer_id);

    // Unsubscribe
    let result = client.unsubscribe(topic.id, consumer_id).await;
    assert!(result.is_ok(), "Unsubscribe failed: {:?}", result.err());
    println!("Unsubscribed successfully");

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_commit_offset() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Create a topic
    let topic_name = format!(
        "commit_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let topic = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic: id={}", topic.id);

    // Subscribe
    let consumer_id = 99999u64;
    client
        .subscribe(topic.id, 0, 65536, consumer_id)
        .await
        .unwrap();

    // Commit an offset
    let result = client.commit_offset(topic.id, consumer_id, 1000).await;
    assert!(result.is_ok(), "Commit offset failed: {:?}", result.err());

    let commit_result = result.unwrap();
    assert_eq!(commit_result.consumer_id, consumer_id);
    assert_eq!(commit_result.committed_offset, 1000);
    println!(
        "Committed offset: consumer_id={}, offset={}",
        commit_result.consumer_id, commit_result.committed_offset
    );

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_subscribe_resumes_from_committed_offset() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Create a topic
    let topic_name = format!(
        "resume_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let topic = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic: id={}", topic.id);

    let consumer_id = 77777u64;

    // Subscribe and commit an offset
    client
        .subscribe(topic.id, 0, 65536, consumer_id)
        .await
        .unwrap();
    client
        .commit_offset(topic.id, consumer_id, 5000)
        .await
        .unwrap();
    client.unsubscribe(topic.id, consumer_id).await.unwrap();
    println!("Committed offset 5000 and unsubscribed");

    // Re-subscribe with offset 0 (Beginning) - should resume from committed offset
    let result = client
        .subscribe(topic.id, 0, 65536, consumer_id)
        .await
        .unwrap();
    assert_eq!(
        result.start_offset, 5000,
        "Should resume from committed offset"
    );
    println!(
        "Re-subscribed and resumed from offset: {}",
        result.start_offset
    );

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_streaming_consumer_lifecycle() {
    use crate::{SeekPosition, StreamingConsumer, StreamingConsumerConfig};

    let config = test_config();
    let client = LanceClient::connect(config).await.unwrap();

    // Create a topic
    let topic_name = format!(
        "streaming_lifecycle_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let mut setup_client = LanceClient::connect(test_config()).await.unwrap();
    let topic = setup_client.create_topic(&topic_name).await.unwrap();
    println!("Created topic: id={}", topic.id);

    // Ingest some data
    for i in 0..5 {
        let payload = Bytes::from(format!("streaming test data {}", i));
        setup_client
            .send_ingest_to_topic_sync(topic.id, payload, 1, None)
            .await
            .unwrap();
    }
    println!("Ingested 5 records");

    // Create streaming consumer
    let consumer_config = StreamingConsumerConfig::new(topic.id)
        .with_start_position(SeekPosition::Beginning)
        .with_auto_commit_interval(0); // Manual commit only

    let mut consumer = StreamingConsumer::new(client, consumer_config);

    // Start streaming
    consumer.start().await.unwrap();
    assert!(consumer.is_subscribed());
    println!("Consumer started, subscribed={}", consumer.is_subscribed());

    // Poll for data
    let mut total_records = 0;
    while let Ok(Some(result)) = consumer.poll().await {
        total_records += result.record_count;
        println!(
            "Polled: {} records, offset={}",
            result.record_count, result.current_offset
        );

        if result.end_of_stream || total_records >= 5 {
            break;
        }
    }

    // Commit progress
    consumer.commit().await.unwrap();
    println!("Committed offset: {}", consumer.committed_offset());

    // Stop streaming
    consumer.stop().await.unwrap();
    assert!(!consumer.is_subscribed());
    println!("Consumer stopped");

    // Get client back and cleanup
    let client = consumer.into_client().await.unwrap();
    setup_client.delete_topic(topic.id).await.unwrap();
    drop(client);
    setup_client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_multiple_consumers_same_topic() {
    let config = test_config();
    let mut client1 = LanceClient::connect(config.clone()).await.unwrap();
    let mut client2 = LanceClient::connect(config).await.unwrap();

    // Create a topic
    let topic_name = format!(
        "multi_consumer_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let topic = client1.create_topic(&topic_name).await.unwrap();
    println!("Created topic: id={}", topic.id);

    // Subscribe both consumers
    let consumer1_id = 11111u64;
    let consumer2_id = 22222u64;

    let sub1 = client1
        .subscribe(topic.id, 0, 65536, consumer1_id)
        .await
        .unwrap();
    let sub2 = client2
        .subscribe(topic.id, 100, 65536, consumer2_id)
        .await
        .unwrap();

    println!("Consumer 1 subscribed at offset: {}", sub1.start_offset);
    println!("Consumer 2 subscribed at offset: {}", sub2.start_offset);

    // Each consumer has independent offset
    assert_eq!(sub1.start_offset, 0);
    assert_eq!(sub2.start_offset, 100);

    // Commit different offsets
    client1
        .commit_offset(topic.id, consumer1_id, 500)
        .await
        .unwrap();
    client2
        .commit_offset(topic.id, consumer2_id, 1000)
        .await
        .unwrap();

    // Unsubscribe both
    client1.unsubscribe(topic.id, consumer1_id).await.unwrap();
    client2.unsubscribe(topic.id, consumer2_id).await.unwrap();

    // Cleanup
    client1.delete_topic(topic.id).await.unwrap();
    client1.close().await.unwrap();
    client2.close().await.unwrap();
}

// =============================================================================
// Retention Management Tests
// =============================================================================

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_set_retention_on_topic() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Create a topic first
    let topic_name = format!(
        "retention_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let topic = client.create_topic(&topic_name).await.unwrap();
    println!("Created topic: id={}", topic.id);

    // Set retention policy
    let max_age_secs = 86400; // 1 day
    let max_bytes = 1024 * 1024 * 100; // 100 MB

    let result = client
        .set_retention(topic.id, max_age_secs, max_bytes)
        .await;
    assert!(result.is_ok(), "Set retention failed: {:?}", result.err());
    println!(
        "Set retention: max_age={}s, max_bytes={}",
        max_age_secs, max_bytes
    );

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_set_retention_invalid_topic() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Try to set retention on non-existent topic
    let result = client.set_retention(99999, 86400, 1024 * 1024).await;
    assert!(
        result.is_err(),
        "Set retention on invalid topic should fail"
    );
    println!(
        "Set retention on invalid topic correctly failed: {:?}",
        result.err()
    );

    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_set_retention_zero_values() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Create a topic
    let topic_name = format!(
        "retention_zero_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let topic = client.create_topic(&topic_name).await.unwrap();

    // Set retention with zero values (should disable retention limits)
    let result = client.set_retention(topic.id, 0, 0).await;
    assert!(
        result.is_ok(),
        "Set retention with zero values failed: {:?}",
        result.err()
    );
    println!("Set retention with zero values (unlimited) succeeded");

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_create_topic_with_retention() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    let topic_name = format!(
        "topic_with_retention_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let max_age_secs = 3600; // 1 hour
    let max_bytes = 1024 * 1024 * 50; // 50 MB

    let result = client
        .create_topic_with_retention(&topic_name, max_age_secs, max_bytes)
        .await;
    assert!(
        result.is_ok(),
        "Create topic with retention failed: {:?}",
        result.err()
    );

    let topic = result.unwrap();
    assert_eq!(topic.name, topic_name);
    assert!(topic.id > 0);
    println!(
        "Created topic with retention: id={}, name={}",
        topic.id, topic.name
    );

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_create_topic_with_retention_duplicate_name() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    let topic_name = format!(
        "dup_retention_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create first topic
    let topic = client
        .create_topic_with_retention(&topic_name, 3600, 1024 * 1024)
        .await
        .unwrap();

    // Try to create duplicate
    let result = client
        .create_topic_with_retention(&topic_name, 7200, 2048 * 1024)
        .await;
    assert!(result.is_err(), "Creating duplicate topic should fail");
    println!(
        "Duplicate topic creation correctly failed: {:?}",
        result.err()
    );

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running LANCE server"]
async fn test_retention_update_values() {
    let config = test_config();
    let mut client = LanceClient::connect(config).await.unwrap();

    // Create topic with initial retention
    let topic_name = format!(
        "retention_update_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let topic = client
        .create_topic_with_retention(&topic_name, 3600, 1024 * 1024)
        .await
        .unwrap();
    println!("Created topic with initial retention: 1h, 1MB");

    // Update retention to different values
    let result = client.set_retention(topic.id, 7200, 2048 * 1024).await;
    assert!(
        result.is_ok(),
        "Update retention failed: {:?}",
        result.err()
    );
    println!("Updated retention to: 2h, 2MB");

    // Update again with larger values
    let result = client
        .set_retention(topic.id, 86400, 1024 * 1024 * 100)
        .await;
    assert!(
        result.is_ok(),
        "Second update retention failed: {:?}",
        result.err()
    );
    println!("Updated retention to: 24h, 100MB");

    // Cleanup
    client.delete_topic(topic.id).await.unwrap();
    client.close().await.unwrap();
}
