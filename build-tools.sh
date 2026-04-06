#!/bin/zsh
# LANCE Tools Build Script
# Builds benchmark and chaos binaries separately from main build pipeline.
# These binaries don't change frequently and can be built independently.
# Usage: ./build-tools.sh
#
# Exit on first failure
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "  LANCE Tools Build"
echo "=============================================="
echo ""

# ── Build benchmark and chaos binaries ──────────
echo "▶ Building release benchmark + chaos binaries..."
cargo build --release -p lnc-bench -p lnc-chaos
echo "✅ Release benchmark/chaos binaries built"
echo ""

echo "=============================================="
echo "  Tools build complete!"
echo "=============================================="
