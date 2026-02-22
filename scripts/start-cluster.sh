#!/bin/bash
# LANCE Cluster Startup Script
# Launches 3 LANCE server instances for replication testing
#
# Usage:
#   ./scripts/start-cluster.sh           # Start cluster
#   ./scripts/start-cluster.sh stop      # Stop cluster
#   ./scripts/start-cluster.sh clean     # Stop and clean data directories

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Configuration
BASE_PORT=1992
BASE_METRICS_PORT=9090
BASE_HEALTH_PORT=8080
BASE_REPLICATION_PORT=1993
ARTIFACTS_DIR="$PROJECT_DIR/.cassa_artifacts"
DATA_DIR_BASE="$ARTIFACTS_DIR/test-data"
LOG_DIR="$ARTIFACTS_DIR/test-logs"
NODE_COUNT=3
PID_FILE="$ARTIFACTS_DIR/test-cluster.pids"

stop_cluster() {
    echo "Stopping LANCE cluster..."
    
    if [ -f "$PID_FILE" ]; then
        while read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                echo "  Stopping node (PID: $pid)..."
                kill "$pid" 2>/dev/null || true
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    
    # Also kill any lance processes on our ports
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        port=$((BASE_PORT + i))
        pid=$(lsof -ti:$port 2>/dev/null || true)
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    
    echo "Cluster stopped."
}

clean_data() {
    echo "Cleaning data directories..."
    
    if [ -d "$DATA_DIR_BASE" ]; then
        rm -rf "$DATA_DIR_BASE"
        echo "  Removed $DATA_DIR_BASE"
    fi
    
    if [ -d "$LOG_DIR" ]; then
        rm -rf "$LOG_DIR"
        echo "  Removed $LOG_DIR"
    fi
    
    echo "Data cleaned."
}

start_cluster() {
    echo "Starting LANCE cluster with $NODE_COUNT nodes..."
    
    # Stop any existing cluster first
    stop_cluster
    
    # Create directories
    mkdir -p "$DATA_DIR_BASE"
    mkdir -p "$LOG_DIR"
    
    # Build the project first
    echo "Building LANCE..."
    cd "$PROJECT_DIR"
    cargo build --release
    
    LANCE_BINARY="$PROJECT_DIR/target/release/lance"
    if [ ! -f "$LANCE_BINARY" ]; then
        LANCE_BINARY="$PROJECT_DIR/target/debug/lance"
    fi
    
    if [ ! -f "$LANCE_BINARY" ]; then
        echo "LANCE binary not found! Run 'cargo build' first."
        exit 1
    fi
    
    # Build peer list
    PEERS=""
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if [ -n "$PEERS" ]; then
            PEERS="$PEERS,"
        fi
        PEERS="${PEERS}127.0.0.1:$((BASE_REPLICATION_PORT + i))"
    done
    
    # Start nodes
    > "$PID_FILE"  # Clear pid file
    
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        NODE_ID=$i
        PORT=$((BASE_PORT + i))
        METRICS_PORT=$((BASE_METRICS_PORT + i))
        HEALTH_PORT=$((BASE_HEALTH_PORT + i))
        DATA_DIR="$DATA_DIR_BASE/node$NODE_ID"
        LOG_FILE="$LOG_DIR/node$NODE_ID.log"
        
        mkdir -p "$DATA_DIR"
        
        echo "  Starting node $NODE_ID on port $PORT..."
        echo "    Peers: $PEERS"
        
        "$LANCE_BINARY" \
            --node-id "$NODE_ID" \
            --listen "127.0.0.1:$PORT" \
            --metrics "127.0.0.1:$METRICS_PORT" \
            --health "127.0.0.1:$HEALTH_PORT" \
            --data-dir "$DATA_DIR" \
            --replication-mode l3 \
            --peers "$PEERS" \
            > "$LOG_FILE" 2>&1 &
        
        PID=$!
        echo "$PID" >> "$PID_FILE"
        echo "    Started (PID: $PID)"
    done
    
    # Wait for nodes to start
    echo ""
    echo "Waiting for nodes to initialize..."
    sleep 2
    
    # Check if nodes are running
    ALL_RUNNING=true
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        PORT=$((BASE_PORT + i))
        if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
            echo "  Node $i is listening on port $PORT"
        else
            echo "  Node $i is NOT responding on port $PORT"
            ALL_RUNNING=false
        fi
    done
    
    if [ "$ALL_RUNNING" = true ]; then
        echo ""
        echo "Cluster is ready!"
        echo ""
        echo "Node addresses:"
        for i in $(seq 0 $((NODE_COUNT - 1))); do
            echo "  Node $i: 127.0.0.1:$((BASE_PORT + i))"
        done
        echo ""
        echo "To run integration tests against the cluster:"
        echo "  export LANCE_TEST_ADDR='127.0.0.1:$BASE_PORT'"
        echo "  export LANCE_NODE1_ADDR='127.0.0.1:$((BASE_PORT + 0))'"
        echo "  export LANCE_NODE2_ADDR='127.0.0.1:$((BASE_PORT + 1))'"
        echo "  export LANCE_NODE3_ADDR='127.0.0.1:$((BASE_PORT + 2))'"
        echo "  cargo test --package lnc-client --test integration -- --ignored --nocapture"
        echo ""
        echo "To stop the cluster:"
        echo "  ./scripts/start-cluster.sh stop"
    else
        echo ""
        echo "Some nodes failed to start. Check logs in $LOG_DIR"
    fi
}

# Main
case "${1:-start}" in
    stop)
        stop_cluster
        ;;
    clean)
        stop_cluster
        clean_data
        ;;
    start|*)
        start_cluster
        ;;
esac
