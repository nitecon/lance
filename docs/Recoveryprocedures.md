[← Back to Docs Index](./README.md)

# RecoveryProcedures

## Table of Contents

- [1. Overview](#1-overview)
- [2. Common Recovery Scenarios](#2-common-recovery-scenarios)
- [3. Data Verification](#3-data-verification)
- [4. Segment Recovery](#4-segment-recovery)
- [5. Node Failure Recovery](#5-node-failure-recovery)
- [6. Cluster Recovery](#6-cluster-recovery)
- [7. Backup and Restore](#7-backup-and-restore)
- [8. Emergency Procedures](#8-emergency-procedures)
- [Appendix: CLI Command Reference](#appendix-cli-command-reference)

Runbook for common LANCE recovery scenarios including data corruption, node failure, and disaster recovery.

---

## 1. Overview

This document provides step-by-step procedures for recovering LANCE from various failure scenarios. Always verify backups exist before attempting recovery operations.

### 1.1 Prerequisites

- Access to LANCE CLI (`lnc`)
- Access to data directory
- Sufficient disk space for recovery operations
- Network access to cluster nodes (for cluster recovery)

### 1.2 Recovery Priority

| Priority | Scenario | Impact |
|----------|----------|--------|
| P1 | Total data loss | Critical - immediate action |
| P2 | Leader node failure | High - automatic failover should occur |
| P3 | Follower node failure | Medium - reduced redundancy |
| P4 | Corrupt segment | Low - single segment affected |

---

## 2. Common Recovery Scenarios

### 2.1 Corrupt Segment

**Symptoms:**
- CRC validation failures during reads
- Error logs showing "checksum mismatch"
- Consumer unable to read specific offset ranges

**Recovery Steps:**

```bash
# 1. Identify the corrupt segment
lnc verify-data --path /data/lance --topic <topic_id>

# 2. If recoverable, repair the segment
lnc repair --path /data/lance/topic_<id>/<segment>.lnc

# 3. If not recoverable, truncate to last valid offset
lnc scan --path /data/lance/topic_<id>/<segment>.lnc
# Note the last valid offset, then truncate

# 4. Rebuild the index
lnc rebuild-index --path /data/lance/topic_<id>
```

### 2.2 Lost Index File

**Symptoms:**
- Fast lookups fail
- Fallback to linear scan (slow reads)
- Missing `.idx` file in topic directory

**Recovery Steps:**

```bash
# Rebuild index from segment data
lnc rebuild-index --path /data/lance/topic_<id>

# Verify the rebuilt index
lnc inspect-index --path /data/lance/topic_<id>/index.idx
```

### 2.3 WAL Corruption

**Symptoms:**
- Server fails to start
- Error: "WAL replay failed"
- Incomplete writes before crash

**Recovery Steps:**

```bash
# 1. Backup current state
cp -r /data/lance /data/lance.backup

# 2. Attempt WAL repair
lnc repair --path /data/lance/wal

# 3. If repair fails, skip corrupt entries
# WARNING: This may lose recent uncommitted data
lnc repair --path /data/lance/wal --skip-corrupt

# 4. Restart the server
systemctl start lance
```

---

## 3. Data Verification

### 3.1 Full Topic Verification

```bash
# Verify all data for a topic
lnc verify-data --path /data/lance --topic <topic_id>

# Stop on first error (for scripting)
lnc verify-data --path /data/lance --topic <topic_id> --fail-fast
```

### 3.2 Segment-Level Verification

```bash
# Scan individual segment
lnc scan --path /data/lance/topic_<id>/<segment>.lnc

# Output includes:
# - Record count
# - Offset range
# - CRC status for each record
```

### 3.3 Continuous Verification

For production, run periodic verification:

```bash
# Cron job (daily at 3 AM)
0 3 * * * /usr/local/bin/lnc verify-data --path /data/lance --topic 1 >> /var/log/lance-verify.log 2>&1
```

---

## 4. Segment Recovery

### 4.1 Truncate Corrupt Tail

When a segment has corruption at the end (common after crash):

```bash
# 1. Find last valid record
lnc scan --path /data/lance/topic_<id>/<segment>.lnc

# 2. Note the offset of last valid record

# 3. Truncate using repair
lnc repair --path /data/lance/topic_<id>/<segment>.lnc
```

### 4.2 Remove Corrupt Segment

If a segment is completely unrecoverable:

```bash
# 1. Backup the corrupt segment
mv /data/lance/topic_<id>/<segment>.lnc /data/lance/quarantine/

# 2. Update metadata to skip the segment
# This creates a gap in offsets - consumers must handle this

# 3. Rebuild index to reflect changes
lnc rebuild-index --path /data/lance/topic_<id>
```

---

## 5. Node Failure Recovery

### 5.1 Leader Node Failure

**Expected Behavior:** Automatic failover within election timeout (150-300ms)

**If Automatic Failover Fails:**

```bash
# 1. Check cluster status from any healthy node
lnc cluster-status --server <healthy_node>:1992

# 2. If no leader elected, check quorum
# Need majority of nodes online for election

# 3. If split-brain suspected, isolate the partition
# Stop the minority partition nodes

# 4. Force election on majority partition
# (Automatic - nodes will elect after timeout)
```

### 5.2 Follower Node Recovery

```bash
# 1. Start the failed node
systemctl start lance

# 2. Node will automatically:
#    - Rejoin cluster
#    - Receive missed replication data
#    - Become available for reads

# 3. Verify node health
lnc cluster-status --server <recovered_node>:1992
```

### 5.3 Complete Node Replacement

When adding a new node to replace a failed one:

```bash
# 1. Update cluster configuration with new node address
# Edit lance.toml: cluster.peers

# 2. Start the new node
systemctl start lance

# 3. Node will receive full state via replication

# 4. Verify cluster health
lnc cluster-status --server <any_node>:1992
```

---

## 6. Cluster Recovery

### 6.1 Split-Brain Recovery

**Symptoms:**
- Two leaders accepting writes
- Divergent data between partitions

**Recovery Steps:**

```bash
# 1. Identify which partition has more recent data
# Check term numbers - higher term is authoritative

# 2. Stop nodes in the stale partition
systemctl stop lance  # on stale nodes

# 3. Export any unique data from stale partition
lnc export-topic --path /data/lance --topic <id> --output /backup/stale_data.lance

# 4. Wipe stale partition data
rm -rf /data/lance/*

# 5. Restart nodes - they will sync from authoritative partition
systemctl start lance

# 6. Manually merge exported data if needed
```

### 6.2 Total Cluster Loss

**Recovery from backup:**

```bash
# 1. Start one node as standalone
lance --standalone --data-dir /data/lance

# 2. Import backup data
lnc import-topic --path /data/lance --name <topic_name> --input /backup/topic.lance

# 3. Repeat for all topics

# 4. Stop standalone mode, restart in cluster mode
systemctl restart lance

# 5. Add remaining nodes to cluster
```

---

## 7. Backup and Restore

### 7.1 Create Backup

```bash
# Export single topic
lnc export-topic --path /data/lance --topic <id> --output /backup/topic_<id>.lance

# Export all topics (script)
for topic in $(lnc list-topics --server localhost:1992 | jq -r '.[] .id'); do
    lnc export-topic --path /data/lance --topic $topic --output /backup/topic_$topic.lance
done
```

### 7.2 Restore from Backup

```bash
# Restore single topic
lnc import-topic --path /data/lance --name "restored_topic" --input /backup/topic.lance

# Verify restored data
lnc verify-data --path /data/lance --topic <new_id>
```

### 7.3 Backup Best Practices

- **Frequency:** Daily full backup, continuous replication for DR
- **Retention:** 7 days minimum, 30 days recommended
- **Testing:** Monthly restore test to verify backup integrity
- **Storage:** Off-site backup for disaster recovery

---

## 8. Emergency Procedures

### 8.1 Emergency Stop

If data corruption is spreading:

```bash
# Immediate stop all nodes
for node in node1 node2 node3; do
    ssh $node "systemctl stop lance"
done
```

### 8.2 Read-Only Mode

To prevent further writes while investigating:

```bash
# Set read-only flag (requires restart)
echo "read_only = true" >> /etc/lance/lance.toml
systemctl restart lance
```

### 8.3 Emergency Contacts

| Role | Contact |
|------|---------|
| On-Call Engineer | [PagerDuty/OpsGenie] |
| Database Lead | [Email/Slack] |
| Infrastructure | [Email/Slack] |

---

## Appendix: CLI Command Reference

| Command | Purpose |
|---------|---------|
| `lnc verify-data` | Check data integrity |
| `lnc repair` | Repair corrupt segments |
| `lnc rebuild-index` | Rebuild topic index |
| `lnc scan` | Scan segment contents |
| `lnc export-topic` | Export topic for backup |
| `lnc import-topic` | Import topic from backup |
| `lnc cluster-status` | Check cluster health |
---

[↑ Back to Top](#recoveryprocedures) | [← Back to Docs Index](./README.md)
