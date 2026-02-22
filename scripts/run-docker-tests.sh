#!/bin/bash
# LANCE Docker Integration Test Runner
# Builds and runs a 3-node cluster in Docker, runs tests, then tears down
#
# Usage:
#   ./scripts/run-docker-tests.sh                     # Run all integration tests
#   ./scripts/run-docker-tests.sh --filter "multi"    # Run tests matching filter
#   ./scripts/run-docker-tests.sh --skip-cluster      # Skip cluster tests
#   ./scripts/run-docker-tests.sh --no-build          # Skip Docker build
#   ./scripts/run-docker-tests.sh --keep              # Keep containers running after tests
#   ./scripts/run-docker-tests.sh --tee               # Run TEE-specific tests (Linux only)
#   ./scripts/run-docker-tests.sh --benchmark         # Run performance benchmarks
#   ./scripts/run-docker-tests.sh --all               # Run all tests including TEE and benchmarks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.test.yml"
ARTIFACTS_DIR="$PROJECT_DIR/.cassa_artifacts"
TEE_LOG_DIR="$ARTIFACTS_DIR/tee"
BENCH_RESULTS_DIR="$ARTIFACTS_DIR/benchmarks"

# Options
TEST_FILTER=""
SKIP_CLUSTER=false
NO_BUILD=false
KEEP_RUNNING=false
VERBOSE=false
RUN_TEE_TESTS=false
RUN_BENCHMARKS=false
RUN_ALL=false
LOCAL_ONLY=false
TARGET_ENDPOINT=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --filter)
            TEST_FILTER="$2"
            shift 2
            ;;
        --skip-cluster)
            SKIP_CLUSTER=true
            shift
            ;;
        --no-build)
            NO_BUILD=true
            shift
            ;;
        --keep)
            KEEP_RUNNING=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --tee)
            RUN_TEE_TESTS=true
            shift
            ;;
        --benchmark)
            RUN_BENCHMARKS=true
            shift
            ;;
        --all)
            RUN_ALL=true
            RUN_TEE_TESTS=true
            RUN_BENCHMARKS=true
            shift
            ;;
        --local)
            LOCAL_ONLY=true
            shift
            ;;
        --target)
            TARGET_ENDPOINT="$2"
            shift 2
            ;;
        --target=*)
            TARGET_ENDPOINT="${1#*=}"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --filter PATTERN   Run only tests matching pattern"
            echo "  --skip-cluster     Skip cluster tests (single node only)"
            echo "  --no-build         Skip Docker image build"
            echo "  --keep             Keep containers running after tests"
            echo "  --tee              Run TEE-specific tests (requires Linux kernel 5.15+)"
            echo "  --benchmark        Run performance benchmarks"
            echo "  --all              Run all tests including TEE and benchmarks"
            echo "  --local            Run local tests only (no Docker, no cluster)"
            echo "  --target ENDPOINT  Run tests against a running endpoint (e.g., localhost:1992)"
            echo "  --verbose, -v      Verbose output"
            echo "  --help, -h         Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

TEST_EXIT_CODE=0
START_TIME=$(date +%s)

log() {
    echo "[$(date '+%H:%M:%S')] $1"
}

log_error() {
    echo "[$(date '+%H:%M:%S')] ERROR: $1" >&2
}

cleanup() {
    if [ "$KEEP_RUNNING" = false ]; then
        log "Stopping Docker containers..."
        docker-compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
        log "Containers stopped."
    else
        log "Keeping containers running (use 'docker-compose -f docker-compose.test.yml down' to stop)"
    fi
    
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    
    echo ""
    echo "========================================"
    log "Total time: ${ELAPSED} seconds"
    echo "========================================"
    
    exit $TEST_EXIT_CODE
}

trap cleanup EXIT INT TERM

check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        # Try docker compose (v2)
        if ! docker compose version &> /dev/null; then
            log_error "docker-compose is not installed"
            exit 1
        fi
        # Use docker compose v2
        COMPOSE_CMD="docker compose"
    else
        COMPOSE_CMD="docker-compose"
    fi
}

check_kernel_tee_support() {
    log "Checking kernel TEE/splice support..."
    
    # Check kernel version (need 5.15+ for IORING_OP_TEE)
    KERNEL_VERSION=$(uname -r | cut -d. -f1-2)
    KERNEL_MAJOR=$(echo "$KERNEL_VERSION" | cut -d. -f1)
    KERNEL_MINOR=$(echo "$KERNEL_VERSION" | cut -d. -f2)
    
    log "  Kernel version: $(uname -r)"
    
    if [ "$KERNEL_MAJOR" -lt 5 ] || ([ "$KERNEL_MAJOR" -eq 5 ] && [ "$KERNEL_MINOR" -lt 15 ]); then
        log_error "Kernel version $KERNEL_VERSION does not support IORING_OP_TEE"
        log_error "TEE requires Linux kernel 5.15 or later"
        return 1
    fi
    
    log " 	[OK] Kernel supports io_uring TEE operations"
    
    # Check if io_uring is available
    if [ ! -e /proc/sys/kernel/io_uring_disabled ]; then
        # Older kernels don't have this sysctl, check differently
        if [ -e /proc/sys/kernel/io_uring_group ]; then
            log "  	[OK] io_uring is available"
        else
            log "  [WARN]  Cannot verify io_uring status (assuming available)"
        fi
    else
        IO_URING_DISABLED=$(cat /proc/sys/kernel/io_uring_disabled 2>/dev/null || echo "0")
        if [ "$IO_URING_DISABLED" != "0" ]; then
            log_error "io_uring is disabled on this system"
            return 1
        fi
        log "  	[OK] io_uring is enabled"
    fi
    
    return 0
}

build_images() {
    if [ "$NO_BUILD" = true ]; then
        log "Skipping Docker build..."
        return
    fi
    
    log "Building Docker images..."
    cd "$PROJECT_DIR"
    
    # Always show build output to see errors
    if ! $COMPOSE_CMD -f "$COMPOSE_FILE" build 2>&1; then
        log_error "Docker build failed!"
        log_error "Try running with --no-build and building locally first:"
        log_error "  cargo build --release --package lance"
        log_error "Or check the build logs above for errors."
        exit 1
    fi
    
    log "Docker images built."
}

start_cluster() {
    log "Starting 3-node LANCE cluster..."
    cd "$PROJECT_DIR"
    
    # Stop any existing containers
    $COMPOSE_CMD -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    
    # Start containers
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d
    
    # Wait for containers to be healthy
    log "Waiting for containers to be healthy..."
    MAX_WAIT=60
    READY=false
    
    for wait in $(seq 1 $MAX_WAIT); do
        HEALTHY=0
        for node in lance-node0 lance-node1 lance-node2; do
            STATUS=$(docker inspect --format='{{.State.Health.Status}}' "$node" 2>/dev/null || echo "missing")
            if [ "$STATUS" = "healthy" ]; then
                HEALTHY=$((HEALTHY + 1))
            fi
        done
        
        if [ $HEALTHY -eq 3 ]; then
            READY=true
            break
        fi
        
        printf "."
        sleep 1
    done
    echo ""
    
    if [ "$READY" = false ]; then
        log_error "Containers failed to become healthy. Checking logs..."
        for node in lance-node0 lance-node1 lance-node2; do
            log_error "=== $node logs ==="
            docker logs "$node" --tail 20 2>&1 | sed 's/^/  /'
        done
        exit 1
    fi
    
    log "Cluster is ready!"
    log "  Node 0: localhost:1992"
    log "  Node 1: localhost:1993"
    log "  Node 2: localhost:1994"
}

set_environment() {
    log "Setting environment variables..."
    
    export LANCE_TEST_ADDR="127.0.0.1:1992"
    export LANCE_NODE1_ADDR="127.0.0.1:1992"
    export LANCE_NODE2_ADDR="127.0.0.1:1993"
    export LANCE_NODE3_ADDR="127.0.0.1:1994"
    
    log "  LANCE_TEST_ADDR = $LANCE_TEST_ADDR"
    log "  LANCE_NODE1_ADDR = $LANCE_NODE1_ADDR"
    log "  LANCE_NODE2_ADDR = $LANCE_NODE2_ADDR"
    log "  LANCE_NODE3_ADDR = $LANCE_NODE3_ADDR"
}

run_tests() {
    log "Running integration tests..."
    
    TEST_ARGS=(
        "test"
        "--package" "lnc-client"
        "--test" "integration"
        "--"
        "--ignored"
        "--nocapture"
    )
    
    # Add test filter if specified
    if [ -n "$TEST_FILTER" ]; then
        TEST_ARGS+=("$TEST_FILTER")
        log "  Filter: $TEST_FILTER"
    fi
    
    # Exclude cluster tests if requested
    if [ "$SKIP_CLUSTER" = true ]; then
        log "  Skipping cluster tests"
        TEST_ARGS+=("--skip" "cluster")
    fi
    
    log "  Command: cargo ${TEST_ARGS[*]}"
    echo ""
    
    cd "$PROJECT_DIR"
    cargo "${TEST_ARGS[@]}" || TEST_EXIT_CODE=$?
    
    echo ""
    if [ $TEST_EXIT_CODE -eq 0 ]; then
        log "All tests passed!"
    else
        log_error "Some tests failed (exit code: $TEST_EXIT_CODE)"
    fi
}

run_tee_tests() {
    log "Running TEE-specific tests..."
    echo ""
    mkdir -p "$TEE_LOG_DIR"
    TEE_TEST_LOG_FILE="$TEE_LOG_DIR/lance-tee-tests.log"
    
    # First run unit tests for TEE primitives
    log "=== TEE Unit Tests (lnc-io) ==="
    cd "$PROJECT_DIR"
    cargo test --package lnc-io --lib -- tee 2>&1 | tee "$TEE_TEST_LOG_FILE" || {
        log_error "TEE unit tests failed"
        TEST_EXIT_CODE=1
    }
    
    echo ""
    log "=== TEE Forwarding Tests (lnc-replication) ==="
    cargo test --package lnc-replication --lib -- tee 2>&1 | tee -a "$TEE_TEST_LOG_FILE" || {
        log_error "TEE forwarding tests failed"
        TEST_EXIT_CODE=1
    }
    
    echo ""
    log "=== Audit Logging Tests ==="
    cargo test --package lnc-replication --lib -- audit 2>&1 | tee -a "$TEE_TEST_LOG_FILE" || {
        log_error "Audit logging tests failed"
        TEST_EXIT_CODE=1
    }
    
    echo ""
    log "=== Splice/TEE Probe Tests ==="
    cargo test --package lnc-io --lib -- splice 2>&1 | tee -a "$TEE_TEST_LOG_FILE" || {
        log_error "Splice tests failed"
        TEST_EXIT_CODE=1
    }
    
    echo ""
    if [ $TEST_EXIT_CODE -eq 0 ]; then
        log "[OK] All TEE tests passed!"
    else
        log_error "Some TEE tests failed"
    fi
    
    log "Test log saved to: $TEE_TEST_LOG_FILE"
}

run_benchmarks() {
    log "Running performance benchmarks..."
    echo ""

    mkdir -p "$BENCH_RESULTS_DIR"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    BENCH_FILE="$BENCH_RESULTS_DIR/benchmark_${TIMESTAMP}.txt"
    
    log "Results will be saved to: $BENCH_FILE"
    echo ""
    
    {
        echo "========================================"
        echo "LANCE Performance Benchmark Results"
        echo "========================================"
        echo "Date: $(date)"
        echo "Kernel: $(uname -r)"
        echo "CPU: $(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
        echo "Memory: $(free -h | awk '/^Mem:/ {print $2}')"
        echo ""
        
        # Check TEE support
        echo "=== io_uring Support ==="
        if check_kernel_tee_support 2>&1; then
            echo "TEE: Supported"
        else
            echo "TEE: Not Supported"
        fi
        echo ""
        
        # Run cargo benchmarks if available
        echo "=== Cargo Benchmarks ==="
    } | tee "$BENCH_FILE"
    
    cd "$PROJECT_DIR"
    
    # Run benchmarks (use release mode for accurate results)
    log "Building in release mode..."
    cargo build --release --package lnc-io 2>&1 | tail -5
    
    log "Running io benchmarks..."
    {
        echo ""
        echo "=== IO Backend Benchmarks ==="
        cargo test --release --package lnc-io --lib -- --nocapture backend 2>&1 || true
        
        echo ""
        echo "=== Priority Queue Benchmarks ==="
        cargo test --release --package lnc-io --lib -- --nocapture priority 2>&1 || true
        
        echo ""
        echo "=== Forward Config Benchmarks ==="
        cargo test --release --package lnc-replication --lib -- --nocapture forward 2>&1 || true
        
        echo ""
        echo "=== TEE vs Splice Performance Benchmark ==="
        cargo test --release --package lnc-io --lib -- --nocapture benchmark_tee_vs_splice 2>&1 || true
        
        echo ""
        echo "=== to_vec() Overhead Benchmark (Compression None) ==="
        cargo test --release --package lnc-network --lib -- --nocapture benchmark_to_vec 2>&1 || true
        
    } | tee -a "$BENCH_FILE"
    
    # Summary
    {
        echo ""
        echo "========================================"
        echo "Benchmark Summary"
        echo "========================================"
        echo "Completed at: $(date)"
        echo ""
    } | tee -a "$BENCH_FILE"
    
    log "Benchmark results saved to: $BENCH_FILE"
}

run_l3_quorum_tests() {
    log "Running L3 Quorum integration tests..."
    echo ""
    
    cd "$PROJECT_DIR"
    
    log "=== L3 Sync Quorum Tests ==="
    cargo test --package lnc-replication --lib -- quorum 2>&1 || {
        log_error "L3 quorum tests failed"
        TEST_EXIT_CODE=1
    }
    
    echo ""
    log "=== Raft Consensus Tests ==="
    cargo test --package lnc-replication --lib -- raft 2>&1 || {
        log_error "Raft tests failed"
        TEST_EXIT_CODE=1
    }
    
    echo ""
    log "=== Leader Forwarding Tests ==="
    cargo test --package lnc-replication --lib -- forward 2>&1 || {
        log_error "Forward tests failed"
        TEST_EXIT_CODE=1
    }
    
    echo ""
    if [ $TEST_EXIT_CODE -eq 0 ]; then
        log "[OK] All L3 quorum tests passed!"
    fi
}

verify_target_endpoint() {
    local endpoint="$1"
    local host="${endpoint%:*}"
    local port="${endpoint#*:}"
    
    log "Verifying target endpoint: $endpoint"
    
    # Extract metrics and health ports (assume standard offsets from client port)
    # Client: 1992, Metrics: 9090, Health: 8080
    local metrics_port=9090
    local health_port=8080
    
    # If port is 1992, use defaults. Otherwise calculate offset
    if [ "$port" != "1992" ]; then
        metrics_port=$((port + 7098))  # 1992 -> 9090
        health_port=$((port + 6088))   # 1992 -> 8080
    fi
    
    echo ""
    log "=== Endpoint Connectivity ==="
    
    # Test client port
    if nc -z "$host" "$port" 2>/dev/null; then
        log "  [OK] Client port ($host:$port): reachable"
    else
        log_error "  [FAIL] Client port ($host:$port): not reachable"
        TEST_EXIT_CODE=1
        return 1
    fi
    
    echo ""
    log "=== Health Check ==="
    
    # Try health endpoint
    local health_response
    health_response=$(curl -s --connect-timeout 5 "http://$host:$health_port/health/ready" 2>/dev/null || echo "")
    
    if echo "$health_response" | grep -q '"status":"ready"'; then
        log "  [OK] Health endpoint ($host:$health_port): ready"
        log "    Response: $health_response"
    elif [ -n "$health_response" ]; then
        log "  [WARN]  Health endpoint ($host:$health_port): responded but not ready"
        log "    Response: $health_response"
    else
        log "  [WARN]  Health endpoint ($host:$health_port): not reachable (may be on different port)"
    fi
    
    echo ""
    log "=== Metrics Verification ==="
    
    # Try metrics endpoint
    local metrics_response
    metrics_response=$(curl -s --connect-timeout 5 "http://$host:$metrics_port/metrics" 2>/dev/null || echo "")
    local metrics_length=${#metrics_response}
    
    if [ "$metrics_length" -gt 100 ]; then
        local metric_count=$(echo "$metrics_response" | grep -c "^lance_" 2>/dev/null || echo "0")
        if [ "$metric_count" -gt 0 ]; then
            log "  [OK] Metrics endpoint ($host:$metrics_port): OK"
            log "    Response size: $metrics_length bytes"
            log "    LANCE metrics found: $metric_count"
            echo ""
            log "  Sample metrics:"
            echo "$metrics_response" | grep "^lance_" | head -10 | while read -r line; do
                log "    $line"
            done
        else
            log "  [WARN]  Metrics endpoint ($host:$metrics_port): responding but no lance_* metrics"
            log "    Response size: $metrics_length bytes"
            log "    This may indicate metrics are not being exported properly."
            TEST_EXIT_CODE=1
        fi
    elif [ -n "$metrics_response" ]; then
        log "  [WARN]  Metrics endpoint ($host:$metrics_port): response too short ($metrics_length bytes)"
        log "    Response: $metrics_response"
        TEST_EXIT_CODE=1
    else
        log "  [WARN]  Metrics endpoint ($host:$metrics_port): not reachable (may be on different port)"
    fi
    
    return 0
}


run_target_tests() {
    local endpoint="$1"
    
    log "Running tests against target endpoint: $endpoint"
    echo ""
    
    # First verify the endpoint
    verify_target_endpoint "$endpoint"
    
    echo ""
    log "=== Integration Tests ==="
    
    # Set environment for tests
    export LANCE_TEST_ADDR="$endpoint"
    export LANCE_NODE1_ADDR="$endpoint"
    export LANCE_NODE2_ADDR="$endpoint"
    export LANCE_NODE3_ADDR="$endpoint"
    
    log "  LANCE_TEST_ADDR = $LANCE_TEST_ADDR"
    echo ""
    
    # Run integration tests
    TEST_ARGS=(
        "test"
        "--package" "lnc-client"
        "--test" "integration"
        "--"
        "--ignored"
        "--nocapture"
    )
    
    # Add test filter if specified
    if [ -n "$TEST_FILTER" ]; then
        TEST_ARGS+=("$TEST_FILTER")
        log "  Filter: $TEST_FILTER"
    fi
    
    # Skip cluster tests since we're targeting a single endpoint
    TEST_ARGS+=("--skip" "cluster")
    
    log "  Command: cargo ${TEST_ARGS[*]}"
    echo ""
    
    cd "$PROJECT_DIR"
    cargo "${TEST_ARGS[@]}" || TEST_EXIT_CODE=$?
    
    echo ""
    if [ $TEST_EXIT_CODE -eq 0 ]; then
        log "[OK] All target tests passed!"
    else
        log_error "Some tests failed (exit code: $TEST_EXIT_CODE)"
    fi
}

# Main
echo ""
echo "========================================"
echo "  LANCE Docker Integration Test Runner"
echo "========================================"
echo ""

# Show what will be run
log "Configuration:"
log "  Local only: $LOCAL_ONLY"
log "  Skip cluster: $SKIP_CLUSTER"
log "  TEE tests: $RUN_TEE_TESTS"
log "  Benchmarks: $RUN_BENCHMARKS"
log "  Run all: $RUN_ALL"
if [ -n "$TARGET_ENDPOINT" ]; then
    log "  Target endpoint: $TARGET_ENDPOINT"
fi
if [ -n "$TEST_FILTER" ]; then
    log "  Filter: $TEST_FILTER"
fi
echo ""

# Check kernel support if running TEE tests
if [ "$RUN_TEE_TESTS" = true ]; then
    if ! check_kernel_tee_support; then
        log_error "Cannot run TEE tests: kernel requirements not met"
        log "Continuing with standard tests only..."
        RUN_TEE_TESTS=false
    fi
fi

# Target mode: run tests against a running endpoint
if [ -n "$TARGET_ENDPOINT" ]; then
    log "Running in target mode against: $TARGET_ENDPOINT"
    echo ""
    
    run_target_tests "$TARGET_ENDPOINT"
    
    # Run benchmarks if requested
    if [ "$RUN_BENCHMARKS" = true ]; then
        echo ""
        echo "========================================"
        echo "  Performance Benchmarks"
        echo "========================================"
        echo ""
        run_benchmarks
    fi
    
    # Final summary for target mode
    echo ""
    echo "========================================"
    echo "  Target Test Summary"
    echo "========================================"
    if [ $TEST_EXIT_CODE -eq 0 ]; then
        log "[OK] All target tests completed successfully!"
    else
        log_error "Some tests failed (exit code: $TEST_EXIT_CODE)"
    fi
    
    exit $TEST_EXIT_CODE
fi

# Local mode: skip Docker entirely
if [ "$LOCAL_ONLY" = true ]; then
    log "Running in local mode (no Docker cluster)..."
    echo ""
    
    # Run local unit/lib tests only
    run_l3_quorum_tests
    
    # Run TEE tests if requested
    if [ "$RUN_TEE_TESTS" = true ]; then
        echo ""
        echo "========================================"
        echo "  TEE-Specific Tests"
        echo "========================================"
        echo ""
        run_tee_tests
    fi
    
    # Run benchmarks if requested
    if [ "$RUN_BENCHMARKS" = true ]; then
        echo ""
        echo "========================================"
        echo "  Performance Benchmarks"
        echo "========================================"
        echo ""
        run_benchmarks
    fi
    
    # Final summary for local mode
    echo ""
    echo "========================================"
    echo "  Local Test Summary"
    echo "========================================"
    if [ $TEST_EXIT_CODE -eq 0 ]; then
        log "[OK] All local tests completed successfully!"
    else
        log_error "Some tests failed (exit code: $TEST_EXIT_CODE)"
    fi
    
    exit $TEST_EXIT_CODE
fi

# Full Docker mode
check_docker
build_images
start_cluster
set_environment

# Run standard integration tests
run_tests

# Run L3 quorum tests
run_l3_quorum_tests

# Run TEE-specific tests if requested
if [ "$RUN_TEE_TESTS" = true ]; then
    echo ""
    echo "========================================"
    echo "  TEE-Specific Tests"
    echo "========================================"
    echo ""
    run_tee_tests
fi

# Run benchmarks if requested
if [ "$RUN_BENCHMARKS" = true ]; then
    echo ""
    echo "========================================"
    echo "  Performance Benchmarks"
    echo "========================================"
    echo ""
    run_benchmarks
fi

# Final summary
echo ""
echo "========================================"
echo "  Test Summary"
echo "========================================"
if [ $TEST_EXIT_CODE -eq 0 ]; then
    log "[OK] All tests completed successfully!"
else
    log_error "Some tests failed (exit code: $TEST_EXIT_CODE)"
fi
