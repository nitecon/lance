#!/bin/zsh
# LANCE Build Script
# Runs all CI checks per CodingGuidelines.md §10.1, then builds and pushes Docker image.
# Usage: ./build.sh
#
# Exit on first failure
set -euo pipefail

cargo fmt --all
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

IMAGE_NAME="nitecon/lance"
IMAGE_TAG="latest"

# Ensure docker buildx is available even without Docker Desktop's bundled plugins.
if ! docker buildx version > /dev/null 2>&1 && command -v docker-buildx > /dev/null 2>&1; then
    mkdir -p "$HOME/.docker/cli-plugins"
    ln -sf "$(command -v docker-buildx)" "$HOME/.docker/cli-plugins/docker-buildx"
fi

if ! docker version > /dev/null 2>&1; then
    echo "❌ Docker daemon is not reachable"
    echo "   Start Docker and re-run ./build.sh"
    exit 1
fi

# Parse args
for arg in "$@"; do
    case "$arg" in
        --skip-push|--skip-tests)
            echo "❌ Unsupported argument: $arg"
            echo "   build.sh enforces the full pipeline: build -> deploy(push) -> test"
            exit 1
            ;;
        --help|-h)
            echo "Usage: $0"
            echo "  No skip flags are supported."
            echo "  Pipeline is always: format -> clippy -> check -> test -> build -> push"
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg"
            exit 1
            ;;
    esac
done

echo "=============================================="
echo "  LANCE Build Pipeline"
echo "=============================================="
echo ""

# ── Step 1: Format Check ─────────────────────────
echo "▶ [1/6] Checking formatting..."
cargo fmt --all -- --check
echo "✅ Format check passed"
echo ""

# ── Step 2: Clippy Lint ──────────────────────────
echo "▶ [2/6] Running Clippy..."
RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets -- -D warnings
echo "✅ Clippy passed"
echo ""

# ── Step 3: Compile Check ────────────────────────
echo "▶ [3/6] Checking compilation..."
cargo check --all-targets --workspace
echo "✅ Compilation check passed"
echo ""

# ── Step 4: Tests ────────────────────────────────
echo "▶ [4/6] Running tests..."
cargo test --workspace --all-features
echo "✅ Tests passed"
echo ""

# ── Step 5: Dependency Audit ─────────────────────
# NOTE: Benchmark/chaos binaries are now in ./build-tools.sh
# Run that script separately when those tools need updating.
echo "▶ [5/6] Running cargo deny..."
if command -v cargo-deny &> /dev/null; then
    cargo deny check
    echo "✅ Dependency audit passed"
else
    echo "⚠️  cargo-deny not installed, skipping (install with: cargo install cargo-deny)"
fi
echo ""

# ── Step 6: Docker Build & Push ──────────────────
# Detect host OS/arch to decide build strategy.
# On macOS (arm64) we must cross-compile to linux/amd64 for K8s nodes.
# On Linux we build natively for the host architecture.
HOST_OS="$(uname -s)"
HOST_ARCH="$(uname -m)"

if [ "$HOST_OS" = "Darwin" ]; then
    # macOS — cross-compile to linux/amd64 via Docker buildx
    BUILD_PLATFORM="linux/amd64"
    echo "▶ [6/6] Building Docker image (cross-compile: ${BUILD_PLATFORM} from ${HOST_OS}/${HOST_ARCH})..."

    if docker buildx version > /dev/null 2>&1; then
        docker buildx build --platform "${BUILD_PLATFORM}" \
            -t "${IMAGE_NAME}:${IMAGE_TAG}" --push .
        echo "✅ Docker image built and pushed: ${IMAGE_NAME}:${IMAGE_TAG} (${BUILD_PLATFORM})"
    else
        echo "❌ Missing required tool: docker buildx plugin"
        echo "   Install/enable Docker buildx and re-run ./build.sh"
        exit 1
    fi
else
    # Linux — native build
    echo "▶ [6/6] Building Docker image (native: ${HOST_OS}/${HOST_ARCH})..."
    docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .
    echo "✅ Docker image built: ${IMAGE_NAME}:${IMAGE_TAG}"
    echo ""

    echo "▶ Pushing ${IMAGE_NAME}:${IMAGE_TAG}..."
    docker push "${IMAGE_NAME}:${IMAGE_TAG}"
    echo "✅ Pushed ${IMAGE_NAME}:${IMAGE_TAG}"
fi

echo ""
echo "=============================================="
echo "  Build complete!"
echo "==============================================" 

#kubectl rollout restart statefulset/lance
#kubectl rollout status statefulset/lance
