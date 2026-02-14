#!/bin/zsh
# LANCE Build Script
# Runs all CI checks per CodingGuidelines.md §10.1, then builds and pushes Docker image.
# Usage: ./build.sh [--skip-push] [--skip-tests]
#
# Exit on first failure
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

IMAGE_NAME="nitecon/lance"
IMAGE_TAG="latest"
SKIP_PUSH=false
SKIP_TESTS=false

# Parse args
for arg in "$@"; do
    case "$arg" in
        --skip-push)  SKIP_PUSH=true ;;
        --skip-tests) SKIP_TESTS=true ;;
        --help|-h)
            echo "Usage: $0 [--skip-push] [--skip-tests]"
            echo "  --skip-push   Build but don't push to Docker Hub"
            echo "  --skip-tests  Skip cargo test (faster iteration)"
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
if [ "$SKIP_TESTS" = true ]; then
    echo "▶ [4/6] Skipping tests (--skip-tests)"
else
    echo "▶ [4/6] Running tests..."
    cargo test --workspace --all-features
    echo "✅ Tests passed"
fi
echo ""

# ── Step 5: Dependency Audit ─────────────────────
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
    # macOS — always cross-compile to linux/amd64 via buildx
    BUILD_PLATFORM="linux/amd64"
    echo "▶ [6/6] Building Docker image (cross-compile: ${BUILD_PLATFORM} from ${HOST_OS}/${HOST_ARCH})..."

    # Ensure buildx builder exists
    if ! docker buildx inspect lance-builder &> /dev/null; then
        echo "  Creating buildx builder 'lance-builder'..."
        docker buildx create --name lance-builder --use
    else
        docker buildx use lance-builder
    fi

    if [ "$SKIP_PUSH" = true ]; then
        # Build and load locally (single platform only)
        docker buildx build --platform "${BUILD_PLATFORM}" \
            -t "${IMAGE_NAME}:${IMAGE_TAG}" --load .
        echo "✅ Docker image built: ${IMAGE_NAME}:${IMAGE_TAG} (${BUILD_PLATFORM})"
        echo ""
        echo "⏭  Skipping push (--skip-push)"
    else
        # Build and push in one step (buildx can push multi-platform manifests)
        docker buildx build --platform "${BUILD_PLATFORM}" \
            -t "${IMAGE_NAME}:${IMAGE_TAG}" --push .
        echo "✅ Docker image built and pushed: ${IMAGE_NAME}:${IMAGE_TAG} (${BUILD_PLATFORM})"
    fi
else
    # Linux — native build
    echo "▶ [6/6] Building Docker image (native: ${HOST_OS}/${HOST_ARCH})..."
    docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .
    echo "✅ Docker image built: ${IMAGE_NAME}:${IMAGE_TAG}"
    echo ""

    if [ "$SKIP_PUSH" = true ]; then
        echo "⏭  Skipping push (--skip-push)"
    else
        echo "▶ Pushing ${IMAGE_NAME}:${IMAGE_TAG}..."
        docker push "${IMAGE_NAME}:${IMAGE_TAG}"
        echo "✅ Pushed ${IMAGE_NAME}:${IMAGE_TAG}"
    fi
fi

echo ""
echo "=============================================="
echo "  Build complete!"
echo "==============================================" 

kubectl rollout restart statefulset/lance
kubectl rollout status statefulset/lance
