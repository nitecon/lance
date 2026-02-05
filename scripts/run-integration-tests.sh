#!/bin/bash
# LANCE Integration Test Runner
# Runs integration tests in both single-node and 3-node cluster modes
#
# Usage:
#   ./scripts/run-integration-tests.sh                     # Run all tests (both modes)
#   ./scripts/run-integration-tests.sh --single            # Single-node mode only
#   ./scripts/run-integration-tests.sh --cluster           # Cluster mode only
#   ./scripts/run-integration-tests.sh --filter "multi"    # Run tests matching filter
#   ./scripts/run-integration-tests.sh --release           # Use release build

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Configuration
BASE_PORT=1992
BASE_METRICS_PORT=9090
BASE_HEALTH_PORT=8080
DATA_DIR_BASE="$PROJECT_DIR/test-data"
LOG_DIR="$PROJECT_DIR/test-logs"

# Options
TEST_FILTER=""
MODE="both"  # single, cluster, or both
RELEASE=false
VERBOSE=false
RUN_UNIT_TESTS=true
RUN_COMPLIANCE=true

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --filter)
            TEST_FILTER="$2"
            shift 2
            ;;
        --single)
            MODE="single"
            shift
            ;;
        --cluster)
            MODE="cluster"
            shift
            ;;
        --skip-unit)
            RUN_UNIT_TESTS=false
            shift
            ;;
        --skip-compliance)
            RUN_COMPLIANCE=false
            shift
            ;;
        --release)
            RELEASE=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --single           Run single-node tests only (L1 mode)"
            echo "  --cluster          Run 3-node cluster tests only (L3 mode)"
            echo "  --filter PATTERN   Run only tests matching pattern"
            echo "  --skip-unit        Skip unit tests"
            echo "  --skip-compliance  Skip compliance checks"
            echo "  --release          Use release build"
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

# Track PIDs for cleanup
PIDS=()
TEST_EXIT_CODE=0
START_TIME=$(date +%s)

log() {
    echo "[$(date '+%H:%M:%S')] $1"
}

log_error() {
    echo "[$(date '+%H:%M:%S')] ERROR: $1" >&2
}

cleanup() {
    log "Stopping all LANCE servers..."
    
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            log "  Stopping server (PID: $pid)..."
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    
    # Kill any remaining processes on our ports
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        port=$((BASE_PORT + i))
        pid=$(lsof -ti:$port 2>/dev/null || true)
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    
    log "Servers stopped."
    
    # Clean test data
    log "Cleaning up test data..."
    rm -rf "$DATA_DIR_BASE" 2>/dev/null || true
    log "Cleanup complete."
    
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    
    echo ""
    echo "========================================"
    log "Total time: ${ELAPSED} seconds"
    echo "========================================"
    
    exit $TEST_EXIT_CODE
}

trap cleanup EXIT INT TERM

# Build LANCE binary if needed
build_lance() {
    if [ "$RELEASE" = true ]; then
        BUILD_TYPE="release"
        LANCE_BINARY="$PROJECT_DIR/target/release/lance"
    else
        BUILD_TYPE="debug"
        LANCE_BINARY="$PROJECT_DIR/target/debug/lance"
    fi
    
    if [ ! -f "$LANCE_BINARY" ]; then
        log "Building LANCE ($BUILD_TYPE)..."
        cd "$PROJECT_DIR"
        if [ "$RELEASE" = true ]; then
            cargo build --release
        else
            cargo build
        fi
    fi
    
    if [ ! -f "$LANCE_BINARY" ]; then
        log_error "LANCE binary not found at $LANCE_BINARY"
        exit 1
    fi
}

# Start single node (L1 mode - no replication)
start_single_node() {
    log "Starting LANCE single node (L1 mode)..."
    
    mkdir -p "$DATA_DIR_BASE"
    mkdir -p "$LOG_DIR"
    
    build_lance
    
    DATA_DIR="$DATA_DIR_BASE/single"
    LOG_FILE="$LOG_DIR/single.log"
    
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    
    log "  Starting single node on port $BASE_PORT..."
    
    "$LANCE_BINARY" \
        --node-id 0 \
        --listen "127.0.0.1:$BASE_PORT" \
        --metrics "127.0.0.1:$BASE_METRICS_PORT" \
        --health "127.0.0.1:$BASE_HEALTH_PORT" \
        --data-dir "$DATA_DIR" \
        --replication-mode l1 \
        > "$LOG_FILE" 2>&1 &
    
    PIDS+=($!)
    
    if [ "$VERBOSE" = true ]; then
        log "    PID: ${PIDS[-1]}, Log: $LOG_FILE"
    fi
    
    # Wait for server to be ready
    log "Waiting for server to initialize..."
    wait_for_server "$BASE_PORT" 10
    
    log "Single node is ready at 127.0.0.1:$BASE_PORT"
}

# Wait for a server to be ready on a port
wait_for_server() {
    local port=$1
    local max_wait=$2
    
    for wait in $(seq 1 $max_wait); do
        sleep 1
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
            return 0
        fi
        printf "."
    done
    echo ""
    
    log_error "Server failed to start on port $port"
    return 1
}

start_cluster() {
    NODE_COUNT=3
    log "Starting LANCE cluster with $NODE_COUNT nodes (L3 mode)..."
    
    mkdir -p "$DATA_DIR_BASE"
    mkdir -p "$LOG_DIR"
    
    build_lance
    
    # Build peer list
    PEERS=""
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if [ -n "$PEERS" ]; then
            PEERS="$PEERS,"
        fi
        PEERS="${PEERS}127.0.0.1:$((BASE_PORT + 100 + i))"
    done
    
    # Start each node
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        NODE_ID=$i
        PORT=$((BASE_PORT + i))
        METRICS_PORT=$((BASE_METRICS_PORT + i))
        HEALTH_PORT=$((BASE_HEALTH_PORT + i))
        DATA_DIR="$DATA_DIR_BASE/node$NODE_ID"
        LOG_FILE="$LOG_DIR/node$NODE_ID.log"
        
        # Clean and create data directory
        rm -rf "$DATA_DIR"
        mkdir -p "$DATA_DIR"
        
        log "  Starting node $NODE_ID on port $PORT..."
        
        "$LANCE_BINARY" \
            --node-id "$NODE_ID" \
            --listen "127.0.0.1:$PORT" \
            --metrics "127.0.0.1:$METRICS_PORT" \
            --health "127.0.0.1:$HEALTH_PORT" \
            --data-dir "$DATA_DIR" \
            --replication-mode l3 \
            --peers "$PEERS" \
            > "$LOG_FILE" 2>&1 &
        
        PIDS+=($!)
        
        if [ "$VERBOSE" = true ]; then
            log "    PID: ${PIDS[-1]}, Log: $LOG_FILE"
        fi
    done
    
    # Wait for servers to be ready
    log "Waiting for servers to initialize..."
    MAX_WAIT=10
    READY=false
    
    for wait in $(seq 1 $MAX_WAIT); do
        sleep 1
        ALL_READY=true
        
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            port=$((BASE_PORT + i))
            if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
                ALL_READY=false
                break
            fi
        done
        
        if [ "$ALL_READY" = true ]; then
            READY=true
            break
        fi
        
        printf "."
    done
    echo ""
    
    if [ "$READY" = false ]; then
        log_error "Servers failed to start. Check logs in $LOG_DIR"
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            ERR_FILE="$LOG_DIR/node$i.log"
            if [ -f "$ERR_FILE" ]; then
                log_error "Node $i log (last 10 lines):"
                tail -10 "$ERR_FILE" | sed 's/^/  /'
            fi
        done
        exit 1
    fi
    
    log "Cluster is ready!"
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        log "  Node $i: 127.0.0.1:$((BASE_PORT + i))"
    done
}

set_environment() {
    log "Setting environment variables..."
    
    export LANCE_TEST_ADDR="127.0.0.1:$BASE_PORT"
    export LANCE_NODE1_ADDR="127.0.0.1:$((BASE_PORT + 0))"
    export LANCE_NODE2_ADDR="127.0.0.1:$((BASE_PORT + 1))"
    export LANCE_NODE3_ADDR="127.0.0.1:$((BASE_PORT + 2))"
    
    log "  LANCE_TEST_ADDR = $LANCE_TEST_ADDR"
    log "  LANCE_NODE1_ADDR = $LANCE_NODE1_ADDR"
    log "  LANCE_NODE2_ADDR = $LANCE_NODE2_ADDR"
    log "  LANCE_NODE3_ADDR = $LANCE_NODE3_ADDR"
}

run_integration_tests() {
    local test_mode=$1
    log "Running integration tests (mode: $test_mode)..."
    
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
    
    # Filter tests based on mode
    if [ "$test_mode" = "single" ]; then
        TEST_ARGS+=("--skip" "cluster")
        log "  Skipping cluster-specific tests"
    fi
    
    log "  Command: cargo ${TEST_ARGS[*]}"
    echo ""
    
    cd "$PROJECT_DIR"
    cargo "${TEST_ARGS[@]}" || TEST_EXIT_CODE=$?
    
    echo ""
    if [ $TEST_EXIT_CODE -eq 0 ]; then
        log "Integration tests passed!"
    else
        log_error "Some integration tests failed (exit code: $TEST_EXIT_CODE)"
    fi
}

run_unit_tests() {
    log "Running unit tests..."
    
    cd "$PROJECT_DIR"
    cargo test --workspace -- --skip integration || {
        local exit_code=$?
        log_error "Unit tests failed (exit code: $exit_code)"
        TEST_EXIT_CODE=$exit_code
        return $exit_code
    }
    
    log "Unit tests passed!"
}

run_compliance_checks() {
    log "Running compliance checks..."
    
    cd "$PROJECT_DIR"
    
    # Check formatting
    log "  Checking code formatting..."
    cargo fmt --all -- --check || {
        log_error "Code formatting check failed"
        TEST_EXIT_CODE=1
        return 1
    }
    
    # Check clippy lints
    log "  Running clippy..."
    cargo clippy --workspace -- -D warnings || {
        log_error "Clippy check failed"
        TEST_EXIT_CODE=1
        return 1
    }
    
    log "Compliance checks passed!"
}

stop_servers() {
    log "Stopping all servers..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    PIDS=()
    
    # Clean test data
    rm -rf "$DATA_DIR_BASE" 2>/dev/null || true
    log "Servers stopped and data cleaned."
}

# Main
echo ""
echo "========================================"
echo "  LANCE Integration Test Runner"
echo "========================================"
echo ""

# Run unit tests first (if not skipped)
if [ "$RUN_UNIT_TESTS" = true ]; then
    run_unit_tests
    if [ $TEST_EXIT_CODE -ne 0 ]; then
        log_error "Unit tests failed, stopping."
        exit $TEST_EXIT_CODE
    fi
fi

# Run compliance checks (if not skipped)
if [ "$RUN_COMPLIANCE" = true ]; then
    run_compliance_checks
    if [ $TEST_EXIT_CODE -ne 0 ]; then
        log_error "Compliance checks failed, stopping."
        exit $TEST_EXIT_CODE
    fi
fi

# Run single-node tests
if [ "$MODE" = "single" ] || [ "$MODE" = "both" ]; then
    echo ""
    echo "----------------------------------------"
    echo "  Single-Node Tests (L1 Mode)"
    echo "----------------------------------------"
    start_single_node
    set_environment
    run_integration_tests "single"
    stop_servers
    
    if [ $TEST_EXIT_CODE -ne 0 ] && [ "$MODE" = "both" ]; then
        log_error "Single-node tests failed, stopping."
        exit $TEST_EXIT_CODE
    fi
fi

# Run cluster tests
if [ "$MODE" = "cluster" ] || [ "$MODE" = "both" ]; then
    echo ""
    echo "----------------------------------------"
    echo "  Cluster Tests (L3 Mode - 3 Nodes)"
    echo "----------------------------------------"
    start_cluster
    set_environment
    run_integration_tests "cluster"
fi

echo ""
echo "========================================"
if [ $TEST_EXIT_CODE -eq 0 ]; then
    log "All tests passed!"
else
    log_error "Some tests failed."
fi
echo "========================================"
