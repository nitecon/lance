[← Back to Docs Index](./README.md)

# CHANGELOG

## Table of Contents

- [Unreleased](#unreleased)
- [Breaking Changes](#breaking-changes)

---

Significant changes, breaking changes, and migration guides for LANCE operators.

---

## Unreleased

---

### Breaking Change: Name-Based Topic Storage

**Affects:** All deployments with existing on-disk data

**Summary:** Topic segment directories are now keyed by topic name rather than numeric ID. Existing deployments will not have their data auto-migrated. New deployments are unaffected.

---

#### What Changed

Previously, topic data was stored under a directory named after the topic's numeric ID:

```
/var/lib/lance/segments/
  1/
    0_1700000000000000000.lnc
    0_1700000000000000000.idx
    metadata.json
  2/
    0_1700000000000000000.lnc
    ...
```

Topics are now stored under a directory named after the topic name:

```
/var/lib/lance/segments/
  rithmic-dev/
    0_1700000000000000000.lnc
    0_1700000000000000000.idx
    metadata.json
  market-data/
    0_1700000000000000000.lnc
    ...
```

**Wire protocol is unchanged.** Numeric topic IDs are still used on the wire between client and server. This change affects only on-disk storage layout.

---

#### Why This Change Was Made

Numeric IDs were not stable across server restarts. When a topic was deleted and recreated, or when the server restarted in certain failure scenarios, the same topic could be assigned a different numeric ID. This caused previously written segment directories to become unreachable — the server would create a new `segments/3/` directory for a topic that had previously written data under `segments/1/`.

Name-based directories are stable by definition: a topic named `rithmic-dev` always maps to `segments/rithmic-dev/`, regardless of what numeric ID the server assigns it at runtime.

---

#### Topic Name Validation

Topic names must match the pattern `[a-zA-Z0-9-]` — alphanumeric characters and dashes only. No spaces, underscores, dots, or slashes.

Valid examples:
- `rithmic-dev`
- `market-data`
- `trades-live`
- `bench-test`

Invalid examples:
- `market_data` (underscore not allowed)
- `trades.live` (dot not allowed)
- `my topic` (space not allowed)

---

#### Client Library Changes

Client libraries have been updated to accept topic names directly. The name-to-ID resolution happens internally on the server. No client-side code changes are required beyond ensuring topics are referenced by name (which was already the standard API).

---

#### Migration Guide

**New deployments:** No action required. Topics will be created with name-based segment directories automatically.

**Existing deployments with data to preserve:**

The server will not auto-migrate data from numeric ID directories. You have two options:

**Option 1 — Clean start (recommended for non-critical streaming data)**

Stop the server, wipe the segments directory, and restart. Topics will be recreated on first write.

```bash
# Stop the server
systemctl stop lance
# or, in Kubernetes:
kubectl scale statefulset lance --replicas=0 -n lance

# Remove existing segment directories
rm -rf /var/lib/lance/segments/

# Restart
systemctl start lance
# or:
kubectl scale statefulset lance --replicas=3 -n lance
```

**Option 2 — Manual migration (required for data that must be preserved)**

Each numeric ID directory contains a `metadata.json` file that records the topic name. Use this to rename each directory.

```bash
# Stop the server before migrating
systemctl stop lance

cd /var/lib/lance/segments/

# For each numeric ID directory, read the topic name from metadata.json
# and rename the directory accordingly.
for dir in */; do
  dir="${dir%/}"
  # Skip directories that are already name-based (contain letters)
  if [[ "$dir" =~ ^[0-9]+$ ]]; then
    name=$(jq -r '.name' "$dir/metadata.json" 2>/dev/null)
    if [ -n "$name" ] && [ "$name" != "null" ]; then
      echo "Renaming segments/$dir -> segments/$name"
      mv "$dir" "$name"
    else
      echo "WARNING: could not determine topic name for segments/$dir — skipping"
    fi
  fi
done

# Restart the server
systemctl start lance
```

After migration, verify each topic is readable:

```bash
lnc list-topics --server localhost:1992
```

**If `metadata.json` is missing or corrupt** in any directory, that topic's data cannot be automatically recovered by name. Options are to restore from backup or accept the data loss and let the topic be recreated.

---

#### Kubernetes Notes

For Kubernetes StatefulSet deployments, segment data lives on a PersistentVolumeClaim mounted at the `data_dir` path. The migration steps above apply inside each pod's volume.

For a clean-start migration on a StatefulSet:

```bash
# Scale down
kubectl scale statefulset lance --replicas=0 -n lance

# For each pod's PVC, exec or mount and clear the segments directory.
# Example for a single-node deployment:
kubectl run --rm -it migration-helper \
  --image=busybox \
  --restart=Never \
  --overrides='{"spec":{"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"data-lance-0"}}],"containers":[{"name":"migration-helper","image":"busybox","command":["sh"],"stdin":true,"tty":true,"volumeMounts":[{"name":"data","mountPath":"/data"}]}]}}' \
  -- sh -c "rm -rf /data/segments/ && echo done"

# Scale back up
kubectl scale statefulset lance --replicas=3 -n lance
```

For multi-node clusters, repeat the PVC cleanup for each pod (`data-lance-0`, `data-lance-1`, `data-lance-2`) before scaling back up.

---

[↑ Back to Top](#changelog) | [← Back to Docs Index](./README.md)
