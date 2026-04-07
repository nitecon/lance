#!/usr/bin/env bash
# =============================================================================
# Lance Name-Based Storage — Build, Push to DockerHub, Run Tests
# =============================================================================
#
# This script:
#   1. Builds and pushes the Lance Docker image to DockerHub (nitecon/lance)
#      using the existing build.sh
#   2. Runs the integration test suite from scripts/run-integration-tests.sh
#      (local cluster + unit tests + compliance checks)
#
# IMPORTANT: This does NOT touch any K8s clusters.
#            Tests run locally via scripts/run-integration-tests.sh.
#
# Prerequisites:
#   - Docker logged into DockerHub
#   - Rust toolchain installed
#
# Run from: tdesk/lance/ (lance project root)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================"
echo "Step 1: Build and push Docker image"
echo "============================================"
echo "Building nitecon/lance:latest and pushing to DockerHub."
bash ./build.sh
echo ""


echo "Generating the updated cluster:"
bash ./genCluster.sh

echo "Now building the tools to run the tests"
bash ./build-tools.sh

echo "============================================"
echo "Step 2: Run integration tests"
echo "============================================"
echo "Running unit tests, compliance checks, and integration tests..."
echo ""

bash ./integration_test.sh
echo ""

echo "============================================"
echo "All validation complete!"
echo "============================================"
echo ""
echo "Docker image nitecon/lance:latest has been pushed to DockerHub."
echo "All integration tests passed with name-based topic storage."
echo ""
echo "Next steps:"
echo "  1. Deploy test cluster in lance namespace:"
echo "     kubectl delete statefulset lance -n lance --ignore-not-found"
echo "     kubectl delete pvc -l app=lance -n lance --ignore-not-found"
echo "     kubectl apply -f k8s/lance-cluster.yaml -n lance"
echo "     kubectl rollout status statefulset/lance -n lance --timeout=240s"
echo ""
echo "  2. Run bench/chaos against the K8s test cluster:"
echo "     BENCH_ENDPOINT=<lance-svc-ip>:1992 ./integration_test.sh"
echo ""
echo "  3. Verify name-based dirs on disk:"
echo "     kubectl exec lance-0 -n lance -- ls /var/lib/lance/segments/"
echo ""
echo "  4. If validated, migrate rithmic namespace (DO NOT skip validation):"
echo "     kubectl delete statefulset lance -n rithmic --ignore-not-found"
echo "     kubectl delete pvc -l app=lance -n rithmic --ignore-not-found"
echo "     kubectl apply -f k8s/lance.yaml -n rithmic"
