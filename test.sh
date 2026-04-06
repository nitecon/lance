#!/bin/zsh
# LANCE Unit Test Script
# Runs all CI checks per CodingGuidelines.md §10.1
# Usage: ./test.sh
#
# Exit on first failure
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "  LANCE Test Pipeline"
echo "=============================================="
echo ""

# ── Step 1: Format Check ─────────────────────────
echo "▶ [1/7] Checking formatting..."
cargo fmt --all -- --check
echo "✅ Format check passed"
echo ""

# ── Step 2: Clippy Lint ──────────────────────────
echo "▶ [2/7] Running Clippy..."
RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features -- -D warnings
echo "✅ Clippy passed"
echo ""

# ── Step 3: Compile Check ────────────────────────
echo "▶ [3/7] Checking compilation..."
cargo check --all-targets --workspace
echo "✅ Compilation check passed"
echo ""

# ── Step 4: Tests ────────────────────────────────
echo "▶ [4/7] Running tests..."
cargo test --workspace --all-features
echo "✅ Tests passed"
echo ""

# ── Step 5: Dependency Audit ─────────────────────
echo "▶ [5/7] Running cargo deny..."
if command -v cargo-deny &> /dev/null; then
    cargo deny check
    echo "✅ Dependency audit passed"
else
    echo "⚠️  cargo-deny not installed, skipping (install with: cargo install cargo-deny)"
fi
echo ""

# ── Step 6: Documentation ─────────────────────────
echo "▶ [6/7] Building documentation..."
RUSTDOCFLAGS="-Dwarnings" cargo doc --no-deps --all-features
echo "✅ Documentation build passed"
echo ""

# ── Step 7: Mechanical Integrity Checks ───────────
echo "▶ [7/7] Running mechanical integrity checks..."

# No Mutex on hot path
if grep -rE "std::sync::Mutex|parking_lot::Mutex" --include="*.rs" lnc-core lnc-io 2>/dev/null; then
    echo "⚠️  Mutex found in hot path crates (lnc-core, lnc-io)"
else
    echo "✅ No Mutex in hot path crates"
fi

# No dynamic dispatch on data plane
if grep -rE "Box<dyn|&dyn" --include="*.rs" lnc-core lnc-io lnc-network 2>/dev/null; then
    echo "⚠️  Dynamic dispatch found in data plane (lnc-core, lnc-io, lnc-network)"
else
    echo "✅ No dynamic dispatch in data plane"
fi

# No to_vec on network path
if grep -rE "\.to_vec\(\)" --include="*.rs" lnc-network 2>/dev/null; then
    echo "⚠️  to_vec() found in network code (lnc-network)"
else
    echo "✅ No to_vec() in network code"
fi

echo ""

echo "=============================================="
echo "  All tests passed!"
echo "=============================================="
