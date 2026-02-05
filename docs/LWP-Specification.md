[← Back to Docs Index](./README.md)

# Lance Wire Protocol (LWP) Specification

**Version:** 1.0  
**Status:** Released  
**Last Updated:** 2026-02-04

## Table of Contents

1. [Overview](#1-overview)
2. [Transport Layer](#2-transport-layer)
3. [Wire Format](#3-wire-format)
4. [Frame Types](#4-frame-types)
5. [Control Frames](#5-control-frames)
6. [Topics](#6-topics)
7. [Authentication](#7-authentication)
8. [TLV Record Format](#8-tlv-record-format)
9. [CRC32C Checksum](#9-crc32c-checksum)
10. [Message Flow](#10-message-flow)
11. [Error Handling](#11-error-handling)
12. [Error Codes](#12-error-codes)
13. [Implementation Notes](#13-implementation-notes)
14. [Reference Implementation](#14-reference-implementation)
15. [Consumer Client API](#15-consumer-client-api)
16. [Client Consumption Modes](#16-client-consumption-modes)
17. [SDK Generation Guidelines](#17-sdk-generation-guidelines)
- [Appendix A: Test Vectors](#appendix-a-test-vectors)
- [Revision History](#revision-history)

---

## 1. Overview

The Lance Wire Protocol (LWP) is a binary protocol designed for high-performance, low-latency data ingestion. It prioritizes:

- **Zero-copy operations** - Payloads can be processed without copying
- **Hardware-accelerated checksums** - CRC32C with SSE4.2/ARM CRC support
- **Simple parsing** - Fixed-size headers, no variable-length fields in header
- **Backpressure signaling** - Built-in flow control

### 1.1 Design Goals

| Goal | Mechanism |
|------|-----------|
| Low latency | Fixed 44-byte header, no parsing ambiguity |
| Data integrity | CRC32C on header and payload separately |
| Flow control | Explicit backpressure frames |
| Connection health | Keepalive frames with 30-second timeout |
| Batch efficiency | Batch mode for high-throughput scenarios |

### 1.2 Protocol Constants

```
PROTOCOL_VERSION    = 1
MAGIC_BYTES         = [0x4C, 0x41, 0x4E, 0x43]  // ASCII "LANC"
HEADER_SIZE         = 44 bytes
INGEST_HEADER_SIZE  = 32 bytes
KEEPALIVE_TIMEOUT   = 30 seconds
DEFAULT_PORT        = 1992
```

---

## 2. Transport Layer

### 2.1 Connection

- **Protocol:** TCP
- **Default Port:** 1992
- **Byte Order:** Little-endian for all multi-byte integers
- **Connection Model:** Persistent connections, multiplexed batches

### 2.2 Connection Lifecycle

```
Client                              Server
   |                                   |
   |  -------- TCP Connect --------->  |
   |                                   |
   |  -------- Ingest Frame --------> |
   |  <-------- Ack Frame ----------  |
   |                                   |
   |  -------- Keepalive -----------> |
   |  <-------- Keepalive ----------  |
   |                                   |
   |  <------ Backpressure ---------  |  (slow down)
   |                                   |
   |  -------- TCP Close -----------> |
```

### 2.3 Keepalive

- Clients SHOULD send a Keepalive frame every 10 seconds if no other traffic
- Servers MUST respond to Keepalive with a Keepalive
- Connections with no traffic for 30 seconds MAY be closed

---

## 3. Wire Format

### 3.1 LWP Header (44 bytes)

All LWP frames begin with a 44-byte header:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Magic (4 bytes)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|    Version    |     Flags     |          Reserved             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Header CRC32C                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                        Batch ID (8 bytes)                     +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                     Timestamp NS (8 bytes)                    +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Record Count                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Payload Length                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Payload CRC32C                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Topic ID                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 3.2 Field Descriptions

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Magic | Must be `[0x4C, 0x41, 0x4E, 0x43]` ("LANC") |
| 4 | 1 | Version | Protocol version, currently `1` |
| 5 | 1 | Flags | Bitfield (see [Flags](#33-flags)) |
| 6 | 2 | Reserved | Must be `[0x00, 0x00]` |
| 8 | 4 | Header CRC | CRC32C of bytes 0-7 (little-endian) |
| 12 | 8 | Batch ID | Client-assigned batch identifier |
| 20 | 8 | Timestamp NS | Nanoseconds since Unix epoch |
| 28 | 4 | Record Count | Number of records in payload |
| 32 | 4 | Payload Length | Payload size in bytes (0 for control frames) |
| 36 | 4 | Payload CRC | CRC32C of payload (0 if no payload) |
| 40 | 4 | Topic ID | Target topic identifier (0 = default topic) |

### 3.3 Flags

Flags is a single byte bitfield:

| Bit | Value | Name | Description |
|-----|-------|------|-------------|
| 0 | 0x01 | Compressed | Payload is LZ4 compressed |
| 1 | 0x02 | Encrypted | Payload is encrypted (reserved) |
| 2 | 0x04 | BatchMode | Frame contains batch data |
| 3 | 0x08 | Ack | Acknowledgment frame |
| 4 | 0x10 | Backpressure | Server requesting slowdown |
| 5 | 0x20 | Keepalive | Connection health check |
| 6 | 0x40 | Control | Control/management frame |
| 7 | - | Reserved | Must be 0 |

### 3.4 Complete Frame

A complete frame consists of:

```
+-------------------+-------------------+
|   LWP Header      |     Payload       |
|   (44 bytes)      | (variable length) |
+-------------------+-------------------+
```

Total frame size = 44 + Payload Length

---

## 4. Frame Types

### 4.1 Ingest Frame

**Purpose:** Send data records to the server

**Flags:** `0x04` (BatchMode) or `0x05` (BatchMode + Compressed)

**Fields:**
- `Batch ID`: Non-zero, unique per connection
- `Timestamp NS`: Ingestion timestamp
- `Record Count`: Number of records (≥1)
- `Payload Length`: Size of payload (>0)
- `Payload CRC`: CRC32C of payload bytes

**Payload Format:**
The payload contains TLV-encoded records (see separate TLV specification).

**Example (hex):**
```
4C 41 4E 43    // Magic: "LANC"
01             // Version: 1
04             // Flags: BatchMode
00 00          // Reserved
XX XX XX XX    // Header CRC32C
01 00 00 00 00 00 00 00    // Batch ID: 1
XX XX XX XX XX XX XX XX    // Timestamp NS
0A 00 00 00    // Record Count: 10
00 10 00 00    // Payload Length: 4096
XX XX XX XX    // Payload CRC32C
00 00 00 00    // Padding
[... 4096 bytes of payload ...]
```

### 4.2 Ack Frame

**Purpose:** Server acknowledges receipt of an Ingest frame

**Flags:** `0x08` (Ack)

**Fields:**
- `Batch ID`: The Batch ID being acknowledged
- All other fields: 0

**Payload:** None (Payload Length = 0)

**Example (hex):**
```
4C 41 4E 43    // Magic
01             // Version
08             // Flags: Ack
00 00          // Reserved
XX XX XX XX    // Header CRC32C
01 00 00 00 00 00 00 00    // Batch ID: 1 (being acked)
00 00 00 00 00 00 00 00    // Timestamp: 0
00 00 00 00    // Record Count: 0
00 00 00 00    // Payload Length: 0
00 00 00 00    // Payload CRC: 0
00 00 00 00    // Padding
```

### 4.3 Keepalive Frame

**Purpose:** Connection health check

**Flags:** `0x20` (Keepalive)

**Fields:** All Ingest Header fields are 0

**Payload:** None

**Behavior:**
- Client sends Keepalive
- Server MUST respond with Keepalive
- Used to detect dead connections

### 4.4 Backpressure Frame

**Purpose:** Server signals client to slow down

**Flags:** `0x10` (Backpressure)

**Fields:** All Ingest Header fields are 0

**Payload:** None

**Behavior:**
- Server sends when buffers are full
- Client SHOULD reduce send rate by 50%
- Client MAY resume normal rate after successful Acks

---

## 5. Control Frames

Control frames are used for topic management and administrative operations.

### 5.1 Control Frame Format

**Flags:** `0x40` (Control)

**Fields:**
- `Batch ID`: Contains the control command code (see below)
- `Payload Length`: Length of command payload (may be 0)
- `Payload CRC`: CRC32C of payload
- Other fields: 0

### 5.2 Control Commands

| Command | Code | Payload | Description |
|---------|------|---------|-------------|
| CreateTopic | 0x01 | Topic name (UTF-8) | Create a new topic |
| DeleteTopic | 0x02 | Topic ID (4 bytes, LE) | Delete an existing topic |
| ListTopics | 0x03 | None | List all topics |
| GetTopic | 0x04 | Topic ID (4 bytes, LE) | Get topic metadata |
| SetRetention | 0x05 | See §5.10 | Set topic retention policy |
| CreateTopicWithRetention | 0x06 | See §5.11 | Create topic with retention |
| Fetch | 0x10 | See §5.8 | Fetch data from topic |
| FetchResponse | 0x11 | See §5.9 | Server fetch response |
| Subscribe | 0x20 | See §5.12 | Subscribe to topic streaming |
| Unsubscribe | 0x21 | See §5.13 | Unsubscribe from topic |
| CommitOffset | 0x22 | See §5.14 | Commit consumer offset |
| SubscribeAck | 0x23 | Consumer ID + Offset | Server subscription ack |
| CommitAck | 0x24 | Consumer ID + Offset | Server commit ack |
| TopicResponse | 0x80 | JSON response | Server response to topic commands |
| ErrorResponse | 0xFF | Error message (UTF-8) | Server error response |

### 5.3 CreateTopic Request

**Purpose:** Create a new topic

**Payload:** Topic name as UTF-8 string (max 255 bytes)

**Example:**
```
Flags: 0x40 (Control)
Batch ID: 0x01 (CreateTopic)
Payload: "my-events"
```

**Response:** TopicResponse with JSON:
```json
{
  "id": 1,
  "name": "my-events",
  "created_at": 1706918400
}
```

### 5.4 ListTopics Request

**Purpose:** List all available topics

**Payload:** None

**Response:** TopicResponse with JSON:
```json
{
  "topics": [
    {"id": 1, "name": "my-events", "created_at": 1706918400},
    {"id": 2, "name": "logs", "created_at": 1706918500}
  ]
}
```

### 5.5 GetTopic Request

**Purpose:** Get metadata for a specific topic

**Payload:** Topic ID as 4-byte little-endian integer

**Response:** TopicResponse with JSON or ErrorResponse if not found

### 5.6 DeleteTopic Request

**Purpose:** Delete a topic and all its data

**Payload:** Topic ID as 4-byte little-endian integer

**Response:** TopicResponse with JSON:
```json
{
  "deleted": 1
}
```

### 5.7 ErrorResponse

**Purpose:** Server error response to control commands

**Payload:** Error message as UTF-8 string

**Example errors:**
- `"Topic name required"`
- `"Topic 'my-events' already exists"`
- `"Topic not found"`
- `"Unknown topic"`

### 5.8 Fetch Request

**Purpose:** Fetch data from a topic starting at a given offset

**Command Code:** `0x10`

**Payload Format (16 bytes):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Topic ID | Topic identifier (little-endian) |
| 4 | 8 | Start Offset | Byte offset to start reading from |
| 12 | 4 | Max Bytes | Maximum bytes to return |

**Example:**
```
Flags: 0x40 (Control)
Batch ID: 0x10 (Fetch)
Payload: [topic_id: u32][start_offset: u64][max_bytes: u32]
```

**Response:** FetchResponse with binary data (see §5.9)

### 5.9 Fetch Response

**Purpose:** Server response containing fetched data

**Command Code:** `0x11`

**Payload Format:**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | Next Offset | Offset for next fetch request |
| 8 | 4 | Bytes Returned | Number of data bytes in response |
| 12 | 4 | Record Count | Number of records in data |
| 16 | N | Data | Raw segment data (TLV-encoded records) |

**Example Response Payload:**
```
[next_offset: u64][bytes_returned: u32][record_count: u32][data...]
```

**Error Cases:**
- Topic not found → ErrorResponse
- Rate limit exceeded → ErrorResponse with "Rate limit exceeded"
- Invalid offset → Empty data with current offset as next_offset

### 5.10 SetRetention Request

**Purpose:** Set or update retention policy for an existing topic

**Command Code:** `0x05`

**Payload Format (20 bytes):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Topic ID | Topic identifier (little-endian) |
| 4 | 8 | Max Age Secs | Maximum age in seconds (0 = no limit) |
| 12 | 8 | Max Bytes | Maximum size in bytes (0 = no limit) |

**Example:**
```
Flags: 0x40 (Control)
Batch ID: 0x05 (SetRetention)
Payload: [topic_id: u32][max_age_secs: u64][max_bytes: u64]
```

**Response:** TopicResponse with JSON:
```json
{
  "topic_id": 1,
  "max_age_secs": 86400,
  "max_bytes": 1073741824
}
```

**Error Cases:**
- Topic not found → ErrorResponse with "Topic not found"
- Invalid payload → ErrorResponse with "Invalid set retention payload"

### 5.11 CreateTopicWithRetention Request

**Purpose:** Create a new topic with retention policy in a single operation

**Command Code:** `0x06`

**Payload Format (variable length):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 2 | Name Length | Length of topic name (little-endian) |
| 2 | N | Topic Name | Topic name as UTF-8 string |
| 2+N | 8 | Max Age Secs | Maximum age in seconds (0 = no limit) |
| 10+N | 8 | Max Bytes | Maximum size in bytes (0 = no limit) |

**Example:**
```
Flags: 0x40 (Control)
Batch ID: 0x06 (CreateTopicWithRetention)
Payload: [name_len: u16]["events"][max_age_secs: u64][max_bytes: u64]
```

**Response:** TopicResponse with JSON:
```json
{
  "id": 1,
  "name": "events",
  "created_at": 1706918400,
  "max_age_secs": 86400,
  "max_bytes": 1073741824
}
```

**Error Cases:**
- Topic already exists → ErrorResponse
- Invalid payload → ErrorResponse with "Invalid create topic payload"

### 5.12 Subscribe Request

**Purpose:** Subscribe to a topic for streaming data

**Command Code:** `0x20`

**Payload Format (24 bytes):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Topic ID | Topic identifier (little-endian) |
| 4 | 8 | Start Offset | Byte offset to start streaming from |
| 12 | 4 | Max Batch Bytes | Maximum bytes per batch |
| 16 | 8 | Consumer ID | Unique consumer identifier |

**Response:** SubscribeAck with consumer_id and actual_offset

### 5.13 Unsubscribe Request

**Purpose:** Unsubscribe from a topic

**Command Code:** `0x21`

**Payload Format (12 bytes):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Topic ID | Topic identifier (little-endian) |
| 4 | 8 | Consumer ID | Consumer identifier |

**Response:** Ack frame

### 5.14 CommitOffset Request

**Purpose:** Commit consumer offset for checkpointing (client-managed offsets)

**Command Code:** `0x22`

**Payload Format (20 bytes):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Topic ID | Topic identifier (little-endian) |
| 4 | 8 | Consumer ID | Consumer identifier |
| 12 | 8 | Offset | Offset to commit |

**Response:** CommitAck with consumer_id and committed_offset

**Note:** Per architecture decision, offset persistence is client-managed. The server acknowledges the commit but clients are responsible for storing offsets locally using `LockFileOffsetStore` or similar mechanisms.

---

## 6. Topics

Topics provide logical separation of data streams within a LANCE server.

### 6.1 Topic Concepts

- **Topic ID 0:** Default/legacy topic for backward compatibility
- **Topic IDs 1+:** User-created topics
- Each topic has isolated storage under `segments/{topic_id}/`
- Topics persist across server restarts

### 6.2 Topic Metadata

Each topic stores metadata in `segments/{topic_id}/metadata.json`:

```json
{
  "id": 1,
  "name": "my-events",
  "created_at": 1706918400,
  "auth": {
    "mtls_enabled": true,
    "client_ca_path": "/etc/lance/ca.crt",
    "allowed_cns": ["client1.example.com", "client2.example.com"],
    "allowed_topics": [1, 2, 3]
  },
  "retention": {
    "max_bytes": 10737418240,
    "max_age_secs": 604800
  }
}
```

### 6.3 Topic Storage Layout

```
data/
  segments/
    0/                          # Default topic (ID 0)
      0_1706918400000000.lnc    # Segment files
      1_1706918500000000.lnc
    1/                          # Topic ID 1
      metadata.json             # Topic metadata
      0_1706918400000000.lnc
    2/                          # Topic ID 2
      metadata.json
      0_1706918400000000.lnc
```

### 6.4 Ingesting to Topics

To ingest data to a specific topic, set the `Topic ID` field in the header:

```
Topic ID = 0    → Default topic (backward compatible)
Topic ID = 1    → Topic with ID 1
Topic ID = N    → Topic with ID N
```

If the topic does not exist, the server returns an ErrorResponse.

---

## 7. Authentication

LANCE supports mutual TLS (mTLS) authentication with per-topic access control.

### 7.1 mTLS Configuration

Server-side mTLS is configured globally and per-topic:

**Global Server Config:**
```toml
[tls]
enabled = true
cert_path = "/etc/lance/server.crt"
key_path = "/etc/lance/server.key"
client_ca_path = "/etc/lance/ca.crt"
require_client_cert = true
```

### 7.2 Per-Topic Access Control

Each topic can define which clients are allowed to access it:

```json
{
  "auth": {
    "mtls_enabled": true,
    "client_ca_path": "/etc/lance/topic-ca.crt",
    "allowed_cns": [
      "producer1.example.com",
      "producer2.example.com"
    ],
    "allowed_topics": [1, 2]
  }
}
```

**Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `mtls_enabled` | bool | Whether mTLS is required for this topic |
| `client_ca_path` | string | Path to CA certificate for client validation |
| `allowed_cns` | array | List of allowed client certificate Common Names |
| `allowed_topics` | array | List of topic IDs this client can access |

### 7.3 Authentication Flow

```
Client                              Server
   |                                   |
   |  -------- TLS Handshake -------> |
   |  <------- Server Cert ---------- |
   |  -------- Client Cert ---------> |
   |                                   |
   |  [Server validates client CN]    |
   |                                   |
   |  -- Ingest(topic=1, data) -----> |
   |                                   |
   |  [Server checks topic access]    |
   |                                   |
   |  <-------- Ack or Error -------  |
```

### 7.4 Access Denied Response

If a client attempts to access a topic they are not authorized for:

```
Flags: 0x40 (Control)
Batch ID: 0xFF (ErrorResponse)
Payload: "Access denied: client 'foo.example.com' not authorized for topic 1"
```

### 7.5 Client Configuration

Clients provide authentication credentials when connecting:

**Rust:**
```rust
let auth = AuthConfig {
    mtls_enabled: true,
    client_cert_path: Some("/path/to/client.crt".into()),
    client_key_path: Some("/path/to/client.key".into()),
};

client.send_ingest_to_topic(topic_id, payload, 1, Some(&auth)).await?;
```

**Python:**
```python
import ssl

context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.load_cert_chain('/path/to/client.crt', '/path/to/client.key')
context.load_verify_locations('/path/to/ca.crt')

client = LwpClient('localhost', 1992, ssl_context=context)
client.send_ingest(topic_id=1, payload=data)
```

---

## 8. TLV Record Format

This section defines the Type-Length-Value (TLV) encoding used for individual records within LWP payloads.

### 8.1 TLV Record Structure

Each record in a payload is encoded as:

| Offset | Size (bytes) | Field | Description |
|--------|--------------|-------|-------------|
| 0 | 1 | Type | Record type identifier |
| 1 | 4 | Length | Total length of value field (little-endian u32) |
| 5 | N | Value | Record data (N = Length) |

**Total record size:** 5 + Length bytes

### 8.2 Record Types

| Type | Name | Description |
|------|------|-------------|
| 0x00 | Reserved | Must not be used |
| 0x01 | RawData | Unstructured binary data |
| 0x02 | JSON | JSON-encoded record |
| 0x03 | MessagePack | MessagePack-encoded record |
| 0x04 | Protobuf | Protocol Buffers encoded record |
| 0x05 | Avro | Apache Avro encoded record |
| 0x10 | KeyValue | Key-value pair (key_len:2 + key + value) |
| 0x11 | Timestamped | Timestamp:8 + data |
| 0x12 | KeyTimestamped | Timestamp:8 + key_len:2 + key + value |
| 0x80-0xFE | UserDefined | Application-specific types |
| 0xFF | Null | Empty/tombstone record (Length must be 0) |

### 8.3 Record Type Details

#### RawData (0x01)
```
+------+--------+-----------------+
| 0x01 | Length | Raw binary data |
+------+--------+-----------------+
```
Used for unstructured binary payloads where the application handles serialization.

#### KeyValue (0x10)
```
+------+--------+----------+-----+-----------+
| 0x10 | Length | KeyLen:2 | Key | Value     |
+------+--------+----------+-----+-----------+
```
- KeyLen: 2-byte little-endian key length
- Key: UTF-8 encoded key bytes
- Value: Remaining bytes after key

#### Timestamped (0x11)
```
+------+--------+--------------+------+
| 0x11 | Length | Timestamp:8  | Data |
+------+--------+--------------+------+
```
- Timestamp: 8-byte little-endian nanoseconds since Unix epoch
- Data: Remaining bytes after timestamp

#### KeyTimestamped (0x12)
```
+------+--------+--------------+----------+-----+-------+
| 0x12 | Length | Timestamp:8  | KeyLen:2 | Key | Value |
+------+--------+--------------+----------+-----+-------+
```
Combines timestamp and key-value for event sourcing use cases.

### 8.4 Multiple Records in Payload

Payloads contain concatenated TLV records:

```
+----------+----------+----------+-----+----------+
| Record 1 | Record 2 | Record 3 | ... | Record N |
+----------+----------+----------+-----+----------+
```

The `Record Count` field in the LWP header specifies N.

### 8.5 Parsing Algorithm

```rust
fn parse_tlv_records(payload: &[u8], expected_count: u32) -> Vec<Record> {
    let mut records = Vec::with_capacity(expected_count as usize);
    let mut offset = 0;
    
    while offset + 5 <= payload.len() && records.len() < expected_count as usize {
        let record_type = payload[offset];
        let length = u32::from_le_bytes([
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
            payload[offset + 4],
        ]) as usize;
        
        if offset + 5 + length > payload.len() {
            break; // Truncated record
        }
        
        let value = &payload[offset + 5..offset + 5 + length];
        records.push(Record { record_type, value: value.to_vec() });
        
        offset += 5 + length;
    }
    
    records
}
```

### 8.6 Encoding Example

Encoding two records: "hello" and "world":

```
Record 1: Type=0x01 (RawData), Value="hello" (5 bytes)
01 05 00 00 00 68 65 6C 6C 6F

Record 2: Type=0x01 (RawData), Value="world" (5 bytes)
01 05 00 00 00 77 6F 72 6C 64

Complete payload (20 bytes):
01 05 00 00 00 68 65 6C 6C 6F 01 05 00 00 00 77 6F 72 6C 64
```

### 8.7 Maximum Record Size

- Maximum single record value length: 16 MB (16,777,216 bytes)
- Records exceeding this limit MUST be rejected with error code 0x03 (PayloadTooLarge)

### 8.8 Alignment and Padding

TLV records are NOT aligned or padded. Records are packed contiguously to minimize overhead.

### 8.9 Compression Interaction

When the LWP Compressed flag (0x01) is set:
1. All TLV records are encoded first
2. The complete TLV-encoded payload is then compressed with LZ4
3. The decompressed payload follows standard TLV format

---

## 9. CRC32C Checksum

### 9.1 Algorithm

LWP uses **CRC32C** (Castagnoli polynomial: `0x1EDC6F41`).

This is the same CRC used by:
- iSCSI
- SCTP
- ext4 filesystem
- Btrfs

### 9.2 Hardware Acceleration

CRC32C has hardware support on:
- **x86-64:** SSE4.2 `crc32` instruction
- **ARM64:** CRC32 extension
- **POWER:** `vpmsumd` instruction

### 9.3 Header CRC Calculation

The Header CRC covers bytes 0-7 (Magic through Reserved):

```python
header_crc = crc32c(header_bytes[0:8])
# Store at bytes 8-11 (little-endian)
```

### 9.4 Payload CRC Calculation

```python
payload_crc = crc32c(payload_bytes)
# Store at bytes 36-39 of header (little-endian)
```

### 9.5 Reference Implementation

```python
# Using crc32c library (pip install crc32c)
import crc32c

def compute_header_crc(header_bytes):
    """Compute CRC32C of first 8 bytes of header."""
    return crc32c.crc32c(header_bytes[0:8])

def compute_payload_crc(payload_bytes):
    """Compute CRC32C of entire payload."""
    return crc32c.crc32c(payload_bytes)
```

---

## 10. Message Flow

### 10.1 Basic Ingestion

```
Client                              Server
   |                                   |
   |  -- Ingest(batch=1, data) -----> |
   |  <-------- Ack(batch=1) -------  |
   |                                   |
   |  -- Ingest(batch=2, data) -----> |
   |  <-------- Ack(batch=2) -------  |
```

### 10.2 Pipelined Ingestion

Clients MAY send multiple Ingest frames without waiting for Acks:

```
Client                              Server
   |                                   |
   |  -- Ingest(batch=1) -----------> |
   |  -- Ingest(batch=2) -----------> |
   |  -- Ingest(batch=3) -----------> |
   |  <-------- Ack(batch=1) -------  |
   |  <-------- Ack(batch=2) -------  |
   |  <-------- Ack(batch=3) -------  |
```

### 10.3 Backpressure Handling

```
Client                              Server
   |                                   |
   |  -- Ingest(batch=1) -----------> |
   |  -- Ingest(batch=2) -----------> |
   |  <------ Backpressure ---------  |  (buffer full)
   |                                   |
   |  [Client pauses/slows down]      |
   |                                   |
   |  <-------- Ack(batch=1) -------  |
   |  <-------- Ack(batch=2) -------  |
   |                                   |
   |  [Client resumes]                |
```

### 10.4 Batch ID Management

- Batch IDs MUST be unique per connection
- Batch IDs SHOULD be monotonically increasing
- Servers MAY reject duplicate Batch IDs
- Clients SHOULD track unacknowledged batches for retry

---

## 11. Error Handling

### 11.1 Invalid Magic

If Magic bytes don't match `"LANC"`:
- Server MUST close connection immediately
- No error frame is sent

### 11.2 Version Mismatch

If Version != 1:
- Server MUST close connection
- Future: Version negotiation may be added

### 11.3 CRC Mismatch

If Header CRC or Payload CRC validation fails:
- Server MUST close connection
- Client SHOULD reconnect and retry

### 11.4 Connection Timeout

If no frames received for 30 seconds:
- Either side MAY close connection
- Clients SHOULD send Keepalive before timeout

---

## 12. Error Codes

This section enumerates all error codes that may be returned in ErrorResponse frames.

### 12.1 Error Code Format

Error responses contain a JSON payload with the following structure:

```json
{
  "code": 0x01,
  "message": "Human-readable error description",
  "details": {}  // Optional additional context
}
```

### 12.2 Error Code Enumeration

| Code | Name | Description |
|------|------|-------------|
| 0x00 | Success | Operation completed successfully (not used in ErrorResponse) |
| 0x01 | UnknownError | Unclassified server error |
| 0x02 | InvalidMagic | Frame magic bytes do not match "LANC" |
| 0x03 | PayloadTooLarge | Payload exceeds maximum allowed size (16 MB) |
| 0x04 | InvalidPayload | Payload format is invalid or malformed |
| 0x05 | CrcMismatch | Header or payload CRC validation failed |
| 0x06 | VersionMismatch | Protocol version not supported |
| 0x10 | TopicNotFound | Specified topic ID does not exist |
| 0x11 | TopicAlreadyExists | Topic with specified name already exists |
| 0x12 | InvalidTopicName | Topic name contains invalid characters or is too long |
| 0x13 | TopicDeleted | Topic was deleted and is no longer accessible |
| 0x20 | NotLeader | This node is not the cluster leader; use leader_addr for redirect |
| 0x21 | QuorumNotReached | Write operation failed to reach quorum |
| 0x22 | LeaderUnknown | Cluster leader is not yet known |
| 0x23 | ClusterNotReady | Cluster is still initializing |
| 0x30 | RateLimited | Client has exceeded rate limits |
| 0x31 | Backpressure | Server buffers are full; client should slow down |
| 0x32 | ConnectionLimit | Maximum connection limit reached |
| 0x40 | AuthenticationRequired | Request requires authentication |
| 0x41 | AuthenticationFailed | Authentication credentials are invalid |
| 0x42 | AccessDenied | Client not authorized for requested operation |
| 0x50 | InvalidOffset | Requested offset is invalid or out of range |
| 0x51 | OffsetOutOfRange | Offset is beyond current log end |
| 0x52 | ConsumerNotFound | Consumer ID is not registered |
| 0x60 | InternalError | Internal server error |
| 0x61 | StorageError | Error reading/writing to storage |
| 0x62 | TimeoutError | Operation timed out |
| 0xFF | Reserved | Reserved for future use |

### 12.3 Error Response Examples

#### Topic Not Found
```json
{
  "code": 16,
  "message": "Topic 42 not found"
}
```

#### Not Leader (with redirect)
```json
{
  "code": 32,
  "message": "Not leader - redirect to leader",
  "details": {
    "leader_addr": "192.168.1.10:1992"
  }
}
```

#### Rate Limited
```json
{
  "code": 48,
  "message": "Rate limit exceeded: 1000 requests/sec",
  "details": {
    "retry_after_ms": 100
  }
}
```

### 12.4 Client Error Handling Recommendations

1. **Retriable Errors** (0x20-0x23, 0x30-0x31, 0x62): Implement exponential backoff
2. **Redirect Errors** (0x20): Extract `leader_addr` from details and reconnect
3. **Authentication Errors** (0x40-0x42): Re-authenticate or fail permanently
4. **Data Errors** (0x50-0x52): Check offset validity before retry
5. **Fatal Errors** (0x02, 0x05, 0x06): Close connection and report failure

---

## 13. Implementation Notes

### 13.1 Buffer Sizes

Recommended buffer sizes:

| Buffer | Minimum | Recommended |
|--------|---------|-------------|
| Read buffer | 64 KB | 256 KB |
| Write buffer | 64 KB | 256 KB |
| Max payload | 1 MB | 16 MB |

### 13.2 Threading Model

- Single connection per thread is simplest
- For high throughput: connection pooling with round-robin
- Parser is stateless; can process partial frames

### 13.3 Memory Alignment

For optimal performance:
- Align header buffers to 8 bytes
- Align payload buffers to 64 bytes (cache line)
- Use memory-mapped I/O where available

### 13.4 Language-Specific Considerations

#### Rust
- Use `bytes::Bytes` for zero-copy payload handling
- Use `crc32fast` crate with hardware acceleration

#### C/C++
- Use `__builtin_ia32_crc32` intrinsics on x86
- Ensure struct packing matches wire format

#### Go
- Use `hash/crc32` with `MakeTable(crc32.Castagnoli)`
- Use `encoding/binary.LittleEndian`

#### Python
- Use `crc32c` package (not `zlib.crc32` - wrong polynomial)
- Use `struct.pack('<Q', value)` for little-endian encoding

#### Java
- Use `java.util.zip.CRC32C` (Java 9+)
- Use `ByteBuffer.order(ByteOrder.LITTLE_ENDIAN)`

---

## 14. Reference Implementation

### 14.1 Python Client Example

```python
import socket
import struct
import time
import crc32c

MAGIC = b'LANC'
VERSION = 1
HEADER_SIZE = 44
FLAG_BATCH_MODE = 0x04
FLAG_ACK = 0x08
FLAG_KEEPALIVE = 0x20

class LwpClient:
    def __init__(self, host: str, port: int = 1992):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.batch_id = 0
    
    def _make_header(self, flags: int, batch_id: int, timestamp_ns: int,
                     record_count: int, payload_length: int, payload_crc: int) -> bytes:
        # Build first 8 bytes for header CRC
        header_start = struct.pack('<4sBBH', MAGIC, VERSION, flags, 0)
        header_crc = crc32c.crc32c(header_start)
        
        # Build complete header
        header = struct.pack(
            '<4sBBHIQQIIII',
            MAGIC,           # 4 bytes
            VERSION,         # 1 byte
            flags,           # 1 byte
            0,               # 2 bytes reserved
            header_crc,      # 4 bytes
            batch_id,        # 8 bytes
            timestamp_ns,    # 8 bytes
            record_count,    # 4 bytes
            payload_length,  # 4 bytes
            payload_crc,     # 4 bytes
            0                # 4 bytes padding
        )
        return header
    
    def send_ingest(self, payload: bytes, record_count: int = 1) -> int:
        """Send an ingest frame. Returns batch_id."""
        self.batch_id += 1
        timestamp_ns = int(time.time() * 1_000_000_000)
        payload_crc = crc32c.crc32c(payload)
        
        header = self._make_header(
            flags=FLAG_BATCH_MODE,
            batch_id=self.batch_id,
            timestamp_ns=timestamp_ns,
            record_count=record_count,
            payload_length=len(payload),
            payload_crc=payload_crc
        )
        
        self.sock.sendall(header + payload)
        return self.batch_id
    
    def recv_ack(self) -> int:
        """Receive an ack frame. Returns acknowledged batch_id."""
        data = self._recv_exact(HEADER_SIZE)
        
        magic, version, flags = struct.unpack_from('<4sBB', data, 0)
        if magic != MAGIC:
            raise ValueError(f"Invalid magic: {magic}")
        if flags != FLAG_ACK:
            raise ValueError(f"Expected ACK, got flags: {flags:#x}")
        
        batch_id = struct.unpack_from('<Q', data, 12)[0]
        return batch_id
    
    def send_keepalive(self):
        """Send a keepalive frame."""
        header = self._make_header(
            flags=FLAG_KEEPALIVE,
            batch_id=0,
            timestamp_ns=0,
            record_count=0,
            payload_length=0,
            payload_crc=0
        )
        self.sock.sendall(header)
    
    def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes."""
        data = b''
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed")
            data += chunk
        return data
    
    def close(self):
        self.sock.close()


# Usage example
if __name__ == '__main__':
    client = LwpClient('localhost', 1992)
    
    # Send some data
    payload = b'Hello, LANCE!'
    batch_id = client.send_ingest(payload)
    print(f"Sent batch {batch_id}")
    
    # Wait for ack
    acked_id = client.recv_ack() 
    print(f"Received ack for batch {acked_id}")
    
    client.close()
```

### 14.2 Go Client Example

```go
package lwp

import (
    "encoding/binary"
    "hash/crc32"
    "net"
    "time"
)

var (
    Magic        = [4]byte{'L', 'A', 'N', 'C'}
    CRCTable     = crc32.MakeTable(crc32.Castagnoli)
    HeaderSize   = 44
)

const (
    FlagBatchMode   = 0x04
    FlagAck         = 0x08
    FlagBackpressure = 0x10
    FlagKeepalive   = 0x20
)

type Header struct {
    Magic         [4]byte
    Version       uint8
    Flags         uint8
    Reserved      uint16
    HeaderCRC     uint32
    BatchID       uint64
    TimestampNS   uint64
    RecordCount   uint32
    PayloadLength uint32
    PayloadCRC    uint32
    Padding       uint32
}

func (h *Header) Encode() []byte {
    buf := make([]byte, HeaderSize)
    
    copy(buf[0:4], h.Magic[:])
    buf[4] = h.Version
    buf[5] = h.Flags
    binary.LittleEndian.PutUint16(buf[6:8], h.Reserved)
    
    // Compute header CRC over first 8 bytes
    h.HeaderCRC = crc32.Checksum(buf[0:8], CRCTable)
    binary.LittleEndian.PutUint32(buf[8:12], h.HeaderCRC)
    
    binary.LittleEndian.PutUint64(buf[12:20], h.BatchID)
    binary.LittleEndian.PutUint64(buf[20:28], h.TimestampNS)
    binary.LittleEndian.PutUint32(buf[28:32], h.RecordCount)
    binary.LittleEndian.PutUint32(buf[32:36], h.PayloadLength)
    binary.LittleEndian.PutUint32(buf[36:40], h.PayloadCRC)
    binary.LittleEndian.PutUint32(buf[40:44], h.Padding)
    
    return buf
}

func ParseHeader(buf []byte) (*Header, error) {
    if len(buf) < HeaderSize {
        return nil, ErrBufferTooSmall
    }
    
    h := &Header{}
    copy(h.Magic[:], buf[0:4])
    
    if h.Magic != Magic {
        return nil, ErrInvalidMagic
    }
    
    h.Version = buf[4]
    h.Flags = buf[5]
    h.Reserved = binary.LittleEndian.Uint16(buf[6:8])
    h.HeaderCRC = binary.LittleEndian.Uint32(buf[8:12])
    
    // Verify header CRC
    computedCRC := crc32.Checksum(buf[0:8], CRCTable)
    if computedCRC != h.HeaderCRC {
        return nil, ErrCRCMismatch
    }
    
    h.BatchID = binary.LittleEndian.Uint64(buf[12:20])
    h.TimestampNS = binary.LittleEndian.Uint64(buf[20:28])
    h.RecordCount = binary.LittleEndian.Uint32(buf[28:32])
    h.PayloadLength = binary.LittleEndian.Uint32(buf[32:36])
    h.PayloadCRC = binary.LittleEndian.Uint32(buf[36:40])
    h.Padding = binary.LittleEndian.Uint32(buf[40:44])
    
    return h, nil
}

type Client struct {
    conn    net.Conn
    batchID uint64
}

func NewClient(addr string) (*Client, error) {
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        return nil, err
    }
    return &Client{conn: conn}, nil
}

func (c *Client) SendIngest(payload []byte, recordCount uint32) (uint64, error) {
    c.batchID++
    
    h := &Header{
        Magic:         Magic,
        Version:       1,
        Flags:         FlagBatchMode,
        BatchID:       c.batchID,
        TimestampNS:   uint64(time.Now().UnixNano()),
        RecordCount:   recordCount,
        PayloadLength: uint32(len(payload)),
        PayloadCRC:    crc32.Checksum(payload, CRCTable),
    }
    
    headerBytes := h.Encode()
    
    if _, err := c.conn.Write(headerBytes); err != nil {
        return 0, err
    }
    if _, err := c.conn.Write(payload); err != nil {
        return 0, err
    }
    
    return c.batchID, nil
}

func (c *Client) RecvAck() (uint64, error) {
    buf := make([]byte, HeaderSize)
    if _, err := io.ReadFull(c.conn, buf); err != nil {
        return 0, err
    }
    
    h, err := ParseHeader(buf)
    if err != nil {
        return 0, err
    }
    
    if h.Flags != FlagAck {
        return 0, ErrUnexpectedFrame
    }
    
    return h.BatchID, nil
}

func (c *Client) Close() error {
    return c.conn.Close()
}
```

### 14.3 C Header

```c
/* lwp.h - Lance Wire Protocol */
#ifndef LWP_H
#define LWP_H

#include <stdint.h>
#include <stddef.h>

#define LWP_MAGIC           0x434E414C  /* "LANC" in little-endian */
#define LWP_VERSION         1
#define LWP_HEADER_SIZE     44

/* Flags */
#define LWP_FLAG_COMPRESSED   0x01
#define LWP_FLAG_ENCRYPTED    0x02
#define LWP_FLAG_BATCH_MODE   0x04
#define LWP_FLAG_ACK          0x08
#define LWP_FLAG_BACKPRESSURE 0x10
#define LWP_FLAG_KEEPALIVE    0x20

/* Error codes */
#define LWP_OK              0
#define LWP_ERR_INVALID_MAGIC   -1
#define LWP_ERR_VERSION         -2
#define LWP_ERR_CRC_MISMATCH    -3
#define LWP_ERR_BUFFER_TOO_SMALL -4

#pragma pack(push, 1)

typedef struct {
    uint64_t batch_id;
    uint64_t timestamp_ns;
    uint32_t record_count;
    uint32_t payload_length;
    uint32_t payload_crc;
    uint32_t padding;
} lwp_ingest_header_t;

typedef struct {
    uint8_t  magic[4];
    uint8_t  version;
    uint8_t  flags;
    uint16_t reserved;
    uint32_t header_crc;
    lwp_ingest_header_t ingest;
} lwp_header_t;

#pragma pack(pop)

/* CRC32C - use hardware acceleration when available */
uint32_t lwp_crc32c(const void* data, size_t len);

/* Header operations */
void lwp_header_init(lwp_header_t* h, uint8_t flags);
void lwp_header_encode(const lwp_header_t* h, uint8_t* buf);
int  lwp_header_parse(const uint8_t* buf, size_t len, lwp_header_t* h);

/* Frame operations */
int lwp_make_ingest_frame(
    uint8_t* buf,
    size_t buf_len,
    uint64_t batch_id,
    uint64_t timestamp_ns,
    uint32_t record_count,
    const void* payload,
    uint32_t payload_len
);

int lwp_make_ack_frame(uint8_t* buf, size_t buf_len, uint64_t batch_id);
int lwp_make_keepalive_frame(uint8_t* buf, size_t buf_len);

#endif /* LWP_H */
```

---

## Appendix A: Test Vectors

### A.1 Keepalive Frame

**Input:** Keepalive frame with all zeros in ingest header

**Hex:**
```
4C 41 4E 43 01 20 00 00
E3 A5 B4 5C 00 00 00 00
00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00
00 00 00 00
```

**Fields:**
- Magic: `4C 41 4E 43` ("LANC")
- Version: `01`
- Flags: `20` (Keepalive)
- Reserved: `00 00`
- Header CRC: `E3 A5 B4 5C` (CRC32C of first 8 bytes)
- Remaining: all zeros

### A.2 Ingest Frame

**Input:** Ingest frame with batch_id=1, 1 record, payload="test"

**Hex:**
```
4C 41 4E 43 01 04 00 00
XX XX XX XX 01 00 00 00
00 00 00 00 XX XX XX XX
XX XX XX XX 01 00 00 00
04 00 00 00 XX XX XX XX
00 00 00 00 74 65 73 74
```

Where XX = computed CRC values.

### A.3 CRC32C Test Vectors

| Input | CRC32C (hex) |
|-------|--------------|
| `""` (empty) | `00000000` |
| `"a"` | `C1D04330` |
| `"hello"` | `9A71BB4C` |
| `"LANC\x01\x04\x00\x00"` | (header crc) |

---

## Revision History

| Version | Date | Changes |
|---------|------|---------|
| 1.2 | 2026-02-04 | Added SetRetention (§5.10), CreateTopicWithRetention (§5.11), Subscribe (§5.12), Unsubscribe (§5.13), CommitOffset (§5.14) commands |
| 1.1 | 2026-02-02 | Added Fetch (§5.8) and FetchResponse (§5.9) commands |
| 1.0 | 2026-02-02 | Initial specification |

---

## 15. Consumer Client API

The `lnc-client` crate provides a high-level `Consumer` abstraction for reading from LANCE streams with support for rewinding and replaying data.

### 15.1 Consumer Overview

The Consumer API wraps the low-level Fetch protocol command (§5.8) with convenient methods for:
- **Offset tracking** - Automatically maintains current position
- **Seek/Rewind** - Navigate to any point in the stream
- **Continuous polling** - Iterate over records efficiently

### 15.2 Creating a Consumer

```rust
use lnc_client::{LanceClient, Consumer, ConsumerConfig, SeekPosition};

// Connect to server
let client = LanceClient::connect_to("127.0.0.1:1992").await?;

// Create consumer for topic 1, starting from beginning
let config = ConsumerConfig::new(1)
    .with_max_fetch_bytes(64 * 1024)
    .with_start_position(SeekPosition::Beginning);
let mut consumer = Consumer::new(client, config);

// Or use convenience constructors
let consumer = Consumer::from_beginning(client, topic_id);
let consumer = Consumer::from_offset(client, topic_id, 5000);
```

### 15.3 Seek Positions

| Position | Description |
|----------|-------------|
| `SeekPosition::Beginning` | Start of stream (offset 0) |
| `SeekPosition::End` | End of stream (latest data) |
| `SeekPosition::Offset(n)` | Specific byte offset |

### 15.4 Rewinding and Replaying

```rust
// Rewind to beginning to replay all data
consumer.rewind().await?;

// Or seek to a specific offset
consumer.seek_to_offset(10000).await?;

// Seek to end to skip to latest
let end_offset = consumer.seek_to_end().await?;

// Generic seek
consumer.seek(SeekPosition::Offset(5000)).await?;
```

### 15.5 Polling for Records

```rust
// Poll returns Option<PollResult> - None when no data
while let Some(result) = consumer.poll().await? {
    println!("Got {} bytes at offset {}", 
        result.data.len(), 
        result.current_offset);
    
    // Process the data
    process_records(&result.data);
    
    if result.end_of_stream {
        break;
    }
}

// Or use poll_blocking which always returns PollResult
loop {
    let result = consumer.poll_blocking().await?;
    if result.is_empty() {
        // No new data, wait and retry
        tokio::time::sleep(Duration::from_millis(100)).await;
        continue;
    }
    process_records(&result.data);
}
```

### 15.6 PollResult Fields

| Field | Type | Description |
|-------|------|-------------|
| `data` | `Vec<u8>` | Raw record data |
| `current_offset` | `u64` | Offset after this fetch |
| `record_count` | `u32` | Number of records (estimate) |
| `end_of_stream` | `bool` | True if no more data available |

### 15.7 Example: Replay from Checkpoint

```rust
// Load last checkpoint
let checkpoint_offset = load_checkpoint()?;

// Create consumer from checkpoint
let mut consumer = Consumer::from_offset(client, topic_id, checkpoint_offset);

// Process records
while let Some(result) = consumer.poll().await? {
    for record in parse_records(&result.data) {
        process_record(record)?;
    }
    
    // Periodically save checkpoint
    save_checkpoint(consumer.current_offset())?;
}
```

### 15.8 Example: Tail Stream (Live Following)

```rust
// Start at end for live data only
let mut consumer = Consumer::new(
    client,
    ConsumerConfig::new(topic_id)
        .with_start_position(SeekPosition::End)
);

// Continuously poll for new data
loop {
    let result = consumer.poll_blocking().await?;
    if !result.is_empty() {
        handle_new_data(&result.data);
    } else {
        // No new data, short sleep before retry
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
```
### 14.4 Retention Command Examples

Reference implementations for SetRetention (§5.10) and CreateTopicWithRetention (§5.11) commands.

#### Rust - Retention Commands

```rust
use bytes::{BufMut, BytesMut};

/// Control command codes
const CMD_SET_RETENTION: u64 = 0x05;
const CMD_CREATE_TOPIC_WITH_RETENTION: u64 = 0x06;

/// Build SetRetention payload
/// 
/// Payload format:
/// - topic_id: u32 (4 bytes, LE)
/// - max_age_secs: u64 (8 bytes, LE)
/// - max_bytes: u64 (8 bytes, LE)
pub fn build_set_retention_payload(
    topic_id: u32,
    max_age_secs: u64,
    max_bytes: u64,
) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(20);
    buf.put_u32_le(topic_id);
    buf.put_u64_le(max_age_secs);
    buf.put_u64_le(max_bytes);
    buf.to_vec()
}

/// Build CreateTopicWithRetention payload
///
/// Payload format:
/// - name_len: u32 (4 bytes, LE)
/// - name: [u8; name_len] (UTF-8)
/// - max_age_secs: u64 (8 bytes, LE)
/// - max_bytes: u64 (8 bytes, LE)
pub fn build_create_topic_with_retention_payload(
    name: &str,
    max_age_secs: u64,
    max_bytes: u64,
) -> Vec<u8> {
    let name_bytes = name.as_bytes();
    let mut buf = BytesMut::with_capacity(4 + name_bytes.len() + 16);
    buf.put_u32_le(name_bytes.len() as u32);
    buf.put_slice(name_bytes);
    buf.put_u64_le(max_age_secs);
    buf.put_u64_le(max_bytes);
    buf.to_vec()
}

impl LanceClient {
    /// Set retention policy on an existing topic
    pub async fn set_retention(
        &mut self,
        topic_id: u32,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<()> {
        let payload = build_set_retention_payload(topic_id, max_age_secs, max_bytes);
        let frame = Frame::new_control(CMD_SET_RETENTION, Some(payload.into()));
        self.send_frame(&frame).await?;
        self.recv_ack().await
    }

    /// Create a topic with initial retention policy
    pub async fn create_topic_with_retention(
        &mut self,
        name: &str,
        max_age_secs: u64,
        max_bytes: u64,
    ) -> Result<u32> {
        let payload = build_create_topic_with_retention_payload(name, max_age_secs, max_bytes);
        let frame = Frame::new_control(CMD_CREATE_TOPIC_WITH_RETENTION, Some(payload.into()));
        self.send_frame(&frame).await?;
        let response = self.recv_topic_response().await?;
        Ok(response.id)
    }
}
```

#### Go - Retention Commands

```go
package lwp

import (
    "encoding/binary"
    "io"
)

const (
    CmdSetRetention             = 0x05
    CmdCreateTopicWithRetention = 0x06
)

// SetRetentionPayload builds the payload for SetRetention command
func SetRetentionPayload(topicID uint32, maxAgeSecs, maxBytes uint64) []byte {
    buf := make([]byte, 20)
    binary.LittleEndian.PutUint32(buf[0:4], topicID)
    binary.LittleEndian.PutUint64(buf[4:12], maxAgeSecs)
    binary.LittleEndian.PutUint64(buf[12:20], maxBytes)
    return buf
}

// CreateTopicWithRetentionPayload builds the payload for CreateTopicWithRetention
func CreateTopicWithRetentionPayload(name string, maxAgeSecs, maxBytes uint64) []byte {
    nameBytes := []byte(name)
    buf := make([]byte, 4+len(nameBytes)+16)
    binary.LittleEndian.PutUint32(buf[0:4], uint32(len(nameBytes)))
    copy(buf[4:4+len(nameBytes)], nameBytes)
    offset := 4 + len(nameBytes)
    binary.LittleEndian.PutUint64(buf[offset:offset+8], maxAgeSecs)
    binary.LittleEndian.PutUint64(buf[offset+8:offset+16], maxBytes)
    return buf
}

// SetRetention sets retention policy on a topic
func (c *Client) SetRetention(topicID uint32, maxAgeSecs, maxBytes uint64) error {
    payload := SetRetentionPayload(topicID, maxAgeSecs, maxBytes)
    return c.sendControl(CmdSetRetention, payload)
}

// CreateTopicWithRetention creates a topic with initial retention
func (c *Client) CreateTopicWithRetention(name string, maxAgeSecs, maxBytes uint64) (uint32, error) {
    payload := CreateTopicWithRetentionPayload(name, maxAgeSecs, maxBytes)
    if err := c.sendControl(CmdCreateTopicWithRetention, payload); err != nil {
        return 0, err
    }
    return c.recvTopicID()
}

func (c *Client) sendControl(cmd uint64, payload []byte) error {
    h := &Header{
        Magic:         Magic,
        Version:       1,
        Flags:         FlagControl,
        BatchID:       cmd,
        PayloadLength: uint32(len(payload)),
    }
    if len(payload) > 0 {
        h.PayloadCRC = crc32.Checksum(payload, CRCTable)
    }
    
    headerBytes := h.Encode()
    if _, err := c.conn.Write(headerBytes); err != nil {
        return err
    }
    if len(payload) > 0 {
        if _, err := c.conn.Write(payload); err != nil {
            return err
        }
    }
    return nil
}
```

#### C - Retention Commands

```c
/* lwp_retention.h - Retention command helpers */
#ifndef LWP_RETENTION_H
#define LWP_RETENTION_H

#include "lwp.h"

/* Build SetRetention payload
 * Returns payload length, or -1 on error
 * Buffer must be at least 20 bytes
 */
int lwp_build_set_retention_payload(
    uint8_t* buf,
    size_t buf_len,
    uint32_t topic_id,
    uint64_t max_age_secs,
    uint64_t max_bytes
) {
    if (buf_len < 20) return -1;
    
    /* topic_id (4 bytes LE) */
    buf[0] = topic_id & 0xFF;
    buf[1] = (topic_id >> 8) & 0xFF;
    buf[2] = (topic_id >> 16) & 0xFF;
    buf[3] = (topic_id >> 24) & 0xFF;
    
    /* max_age_secs (8 bytes LE) */
    for (int i = 0; i < 8; i++) {
        buf[4 + i] = (max_age_secs >> (i * 8)) & 0xFF;
    }
    
    /* max_bytes (8 bytes LE) */
    for (int i = 0; i < 8; i++) {
        buf[12 + i] = (max_bytes >> (i * 8)) & 0xFF;
    }
    
    return 20;
}

/* Build CreateTopicWithRetention payload
 * Returns payload length, or -1 on error
 */
int lwp_build_create_topic_with_retention_payload(
    uint8_t* buf,
    size_t buf_len,
    const char* name,
    size_t name_len,
    uint64_t max_age_secs,
    uint64_t max_bytes
) {
    size_t required = 4 + name_len + 16;
    if (buf_len < required) return -1;
    
    /* name_len (4 bytes LE) */
    buf[0] = name_len & 0xFF;
    buf[1] = (name_len >> 8) & 0xFF;
    buf[2] = (name_len >> 16) & 0xFF;
    buf[3] = (name_len >> 24) & 0xFF;
    
    /* name (UTF-8) */
    memcpy(buf + 4, name, name_len);
    
    size_t offset = 4 + name_len;
    
    /* max_age_secs (8 bytes LE) */
    for (int i = 0; i < 8; i++) {
        buf[offset + i] = (max_age_secs >> (i * 8)) & 0xFF;
    }
    
    /* max_bytes (8 bytes LE) */
    for (int i = 0; i < 8; i++) {
        buf[offset + 8 + i] = (max_bytes >> (i * 8)) & 0xFF;
    }
    
    return (int)required;
}

#endif /* LWP_RETENTION_H */
```

#### Usage Examples

**Rust:**
```rust
// Set 7-day retention, max 1GB
client.set_retention(topic_id, 7 * 24 * 3600, 1024 * 1024 * 1024).await?;

// Create topic with 30-day retention
let id = client.create_topic_with_retention("events", 30 * 86400, 0).await?;
```

**Go:**
```go
// Set 7-day retention, max 1GB
err := client.SetRetention(topicID, 7*24*3600, 1024*1024*1024)

// Create topic with 30-day retention
id, err := client.CreateTopicWithRetention("events", 30*86400, 0)
```

**C:**
```c
// Build SetRetention frame
uint8_t payload[20];
int len = lwp_build_set_retention_payload(payload, 20, topic_id, 7*86400, 1024*1024*1024);

uint8_t frame[LWP_HEADER_SIZE + 20];
lwp_make_control_frame(frame, sizeof(frame), 0x05, payload, len);
send(sock, frame, sizeof(frame), 0);
```

---

### 5.9.1 High Water Mark in FetchResponse

The FetchResponse includes a `high_water_mark` field indicating the highest committed offset in the topic partition. This allows consumers to:

1. **Track lag** - Calculate consumer lag as `high_water_mark - current_offset`
2. **End-of-stream detection** - Know when all available data has been consumed
3. **Progress monitoring** - Display consumption progress in monitoring dashboards

**FetchResponse Payload Structure (Extended):**

```
+------------------+------------------+------------------+------------------+
| Start Offset     | End Offset       | High Water Mark  | Data             |
| (8 bytes LE)     | (8 bytes LE)     | (8 bytes LE)     | (variable)       |
+------------------+------------------+------------------+------------------+
```

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | Start Offset | First offset in response data |
| 8 | 8 | End Offset | Last offset in response data (exclusive) |
| 16 | 8 | High Water Mark | Highest committed offset in partition |
| 24 | N | Data | TLV-encoded records |

**Example Usage (Rust):**
```rust
let response = client.fetch(topic_id, offset, max_bytes).await?;

// Calculate consumer lag
let lag = response.high_water_mark.saturating_sub(response.end_offset);
println!("Consumer lag: {} records", lag);

// Check if caught up
if response.end_offset >= response.high_water_mark {
    println!("Consumer is caught up to head of log");
}
```

**Example Usage (Go):**
```go
resp, err := client.Fetch(topicID, offset, maxBytes)
if err != nil {
    return err
}

lag := resp.HighWaterMark - resp.EndOffset
log.Printf("Consumer lag: %d records", lag)
```

---

## 16. Client Consumption Modes

### 16.1 Overview

LANCE clients support two primary consumption modes, each optimized for different use cases:

| Mode | Use Case | Offset Management | Coordination |
|------|----------|-------------------|--------------|
| **Standalone** | Simple consumers, CLI tools, batch jobs | Client-managed | None |
| **Grouped** | Scalable consumer groups, microservices | Server-coordinated | Automatic rebalancing |

### 16.2 Standalone Mode

In Standalone mode, the client maintains complete control over offset tracking and partition assignment.

**Characteristics:**
- Client explicitly specifies which offsets to consume from
- No server-side consumer group coordination
- Offset persistence is client's responsibility
- Best for: batch processing, replay scenarios, CLI tools

**Configuration:**
```rust
use lnc_client::{StandaloneConfig, Consumer};

let config = StandaloneConfig::new()
    .with_auto_commit(false)           // Manual offset control
    .with_offset_reset("earliest")     // Where to start if no offset
    .with_max_poll_records(500);       // Records per poll

let consumer = Consumer::standalone(client, config)?;

// Explicitly seek to offset
consumer.seek(topic_id, 1000).await?;

// Poll and manually track offsets
while let Some(records) = consumer.poll().await? {
    process(records);
    // Persist offset externally (Redis, database, file)
    offset_store.save(topic_id, records.end_offset);
}
```

**Offset Stores:**
- `MemoryOffsetStore` - In-memory (testing only)
- `LockFileOffsetStore` - File-based with advisory locking
- Custom implementations via `OffsetStore` trait

### 16.3 Grouped Mode

In Grouped mode, consumers join a consumer group and receive automatic partition assignment and rebalancing.

**Characteristics:**
- Server manages partition assignment across group members
- Automatic rebalancing when members join/leave
- Server-side offset tracking with commit protocol
- Best for: scalable microservices, high-availability deployments

**Configuration:**
```rust
use lnc_client::{GroupConfig, GroupedConsumer};

let config = GroupConfig::new("my-consumer-group")
    .with_auto_commit(true)
    .with_auto_commit_interval_ms(5000)
    .with_session_timeout_ms(30000)
    .with_assignment_strategy(AssignmentStrategy::RoundRobin);

let consumer = GroupedConsumer::connect(addrs, config).await?;

// Subscribe to topics (partition assignment is automatic)
consumer.subscribe(&["events", "metrics"]).await?;

// Poll - partitions may change between polls
while let Some(records) = consumer.poll().await? {
    process(records);
    // Offsets committed automatically (or call commit() for manual control)
}
```

**Assignment Strategies:**
- `Range` - Assigns contiguous partition ranges
- `RoundRobin` - Distributes partitions evenly
- `Sticky` - Minimizes partition movement during rebalancing

### 16.4 Mode Selection Guidelines

| Scenario | Recommended Mode |
|----------|------------------|
| Single consumer instance | Standalone |
| Exactly-once processing | Standalone with external offset store |
| Horizontally scalable consumers | Grouped |
| Replaying historical data | Standalone |
| CI/CD test consumers | Standalone |
| Production microservices | Grouped |
| Batch ETL jobs | Standalone |

### 16.5 Protocol Commands by Mode

**Standalone Mode Commands:**
- `Fetch` (0x10) - Pull data from specific offset
- No Subscribe/Unsubscribe needed

**Grouped Mode Commands:**
- `Subscribe` (0x20) - Join group and subscribe to topics
- `Unsubscribe` (0x21) - Leave group
- `CommitOffset` (0x22) - Commit consumed offsets
- `GetClusterStatus` (0x30) - Discover group coordinator

---

## 17. SDK Generation Guidelines

### 17.1 Overview

This section provides guidelines for generating client SDKs in various languages from the LWP specification. The goal is consistent behavior across all official and community-maintained client libraries.

### 17.2 Required SDK Components

Every compliant LANCE SDK MUST implement:

| Component | Description |
|-----------|-------------|
| **Connection** | TCP/TLS connection with keepalive and reconnection |
| **Frame Parser** | LWP header parsing with CRC validation |
| **TLV Decoder** | Type-Length-Value record parsing |
| **Producer** | Batched message production with acks |
| **Consumer** | Offset-based message consumption |
| **Error Handling** | Protocol errors, timeouts, backpressure |

### 17.3 Frame Handling Requirements

```
1. Parse 44-byte header completely before processing payload
2. Validate header CRC32C before trusting header fields
3. Handle backpressure (0x04) by pausing sends
4. Respond to keepalive (0x03) within 5 seconds
5. Support streaming multiple frames per TCP read
```

### 17.4 Type Mappings

| LWP Type | Rust | Go | Python | Java | TypeScript |
|----------|------|-----|--------|------|------------|
| u8 | u8 | uint8 | int | byte | number |
| u16 | u16 | uint16 | int | short | number |
| u32 | u32 | uint32 | int | int | number |
| u64 | u64 | uint64 | int | long | bigint |
| i64 | i64 | int64 | int | long | bigint |
| bytes | Bytes | []byte | bytes | byte[] | Uint8Array |
| string | String | string | str | String | string |

### 17.5 Error Handling Patterns

SDKs MUST handle these error categories:

```
CONNECTION_ERRORS:
  - ConnectionRefused: Server unreachable
  - ConnectionClosed: Unexpected disconnect
  - Timeout: Operation exceeded deadline

PROTOCOL_ERRORS:
  - InvalidFrame: CRC mismatch or malformed header
  - UnsupportedVersion: Protocol version mismatch
  - InvalidResponse: Unexpected frame type

APPLICATION_ERRORS:
  - NotLeader: Redirect to leader (includes leader address)
  - TopicNotFound: Invalid topic ID
  - Backpressure: Server requested slowdown
```

### 17.6 Reconnection Strategy

SDKs SHOULD implement exponential backoff:

```
base_delay = 100ms
max_delay = 30s
max_attempts = unlimited (configurable)

for attempt in 1..max_attempts:
    delay = min(base_delay * 2^attempt, max_delay)
    delay += random_jitter(0, delay * 0.1)
    sleep(delay)
    try_connect()
```

### 17.7 Producer Requirements

| Requirement | Details |
|-------------|---------|
| Batching | Collect records until batch_size OR linger_ms |
| Compression | Optional LZ4 compression (flag 0x04) |
| Acks | Wait for server ACK before confirming |
| Ordering | Maintain order within topic partition |
| Idempotence | Use batch_id for deduplication |

### 17.8 Consumer Requirements

| Requirement | Details |
|-------------|---------|
| Offset Tracking | Client maintains current offset |
| Fetch Batching | Request multiple records per fetch |
| High Water Mark | Track via FetchResponse.high_water_mark |
| Seek | Support seek to offset, earliest, latest |
| Auto-commit | Optional periodic offset commits |

### 17.9 Testing Requirements

SDK implementations MUST pass:

1. **Unit Tests**: Frame parsing, TLV encoding/decoding
2. **Integration Tests**: Connect, produce, consume with real server
3. **Chaos Tests**: Reconnection after network partition
4. **Compatibility Tests**: Cross-version protocol negotiation

### 17.10 Documentation Requirements

Each SDK MUST provide:

- Getting Started guide with minimal example
- API reference for all public types
- Configuration options table
- Error handling best practices
- Performance tuning guide
---

[↑ Back to Top](#lance-wire-protocol-lwp-specification) | [← Back to Docs Index](./README.md)
