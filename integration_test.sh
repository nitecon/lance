#!/bin/zsh
# Ultra High Throughput Integration Test for LANCE
# This script runs aggressive benchmark and chaos tests to validate
# platform performance under extreme load conditions.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

NAMESPACE="${LANCE_NAMESPACE:-lance}"
STATEFULSET="${LANCE_STATEFULSET:-lance}"

# Configuration for ultra high throughput
BENCH_ENDPOINT="${BENCH_ENDPOINT:-10.0.10.11:1992}"
BENCH_DURATION="${BENCH_DURATION:-60}"           # 60s sustained test
BENCH_CONNECTIONS="${BENCH_CONNECTIONS:-64}"      # 64 parallel connections
BENCH_PIPELINE="${BENCH_PIPELINE:-256}"           # 256 in-flight per conn
BENCH_BATCH_SIZE="${BENCH_BATCH_SIZE:-262144}"    # 256KB batches
BENCH_LINGER_MS="${BENCH_LINGER_MS:-5}"           # 5ms linger for batching
BENCH_MSG_SIZE="${BENCH_MSG_SIZE:-1024}"          # 1KB messages

echo "=============================================="
echo "  LANCE Ultra High Throughput Test"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Duration:     ${BENCH_DURATION}s"
echo "  Connections:  ${BENCH_CONNECTIONS}"
echo "  Pipeline:     ${BENCH_PIPELINE}"
echo "  Batch Size:   ${BENCH_BATCH_SIZE} bytes"
echo "  Linger:       ${BENCH_LINGER_MS}ms"
echo "  Message Size: ${BENCH_MSG_SIZE} bytes"
echo ""

echo "Waiting for StatefulSet ${STATEFULSET} in namespace ${NAMESPACE} to be ready..."
kubectl rollout status "statefulset/${STATEFULSET}" -n "${NAMESPACE}" --timeout=240s
kubectl wait --for=condition=Ready pod -l app=lance -n "${NAMESPACE}" --timeout=180s

# Allow brief post-ready stabilization for leader election/peer reconnect convergence.
sleep 20

echo ""
echo "=============================================="
echo "  Phase 1: Ultra High Throughput Benchmark"
echo "=============================================="
echo ""

./target/release/lnc-bench \
    --endpoint "${BENCH_ENDPOINT}" \
    --topic-name bench-ultra \
    --duration "${BENCH_DURATION}" \
    --connections "${BENCH_CONNECTIONS}" \
    --pipeline "${BENCH_PIPELINE}" \
    --batch-size "${BENCH_BATCH_SIZE}" \
    --linger-ms "${BENCH_LINGER_MS}" \
    --msg-size "${BENCH_MSG_SIZE}" \
    --skip-latency-gate \
    --warmup 10

echo ""
echo "=============================================="
echo "  Phase 2: High-Rate Chaos Test (High Volume)"
echo "=============================================="
echo ""

./target/release/lnc-chaos \
    --endpoint "${BENCH_ENDPOINT}" \
    --statefulset lance \
    --namespace lance \
    --topic chaos-high-volume \
    --rate 300000 \
    --payload-size 512 \
    --warmup-secs 10 \
    --post-roll-secs 15 \
    --duration 30

echo ""
echo "=============================================="
echo "  Phase 3: Large Payload Stress Test"
echo "=============================================="
echo ""

./target/release/lnc-chaos \
    --endpoint "${BENCH_ENDPOINT}" \
    --statefulset lance \
    --namespace lance \
    --topic chaos-large-payload \
    --rate 100000 \
    --payload-size 4096 \
    --warmup-secs 5 \
    --post-roll-secs 10 \
    --duration 20

echo ""
echo "=============================================="
echo "  All Ultra High Throughput Tests Complete!"
echo "=============================================="
