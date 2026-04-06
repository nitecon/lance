#!/bin/zsh
# LANCE Build Script
# Builds and pushes Docker image only.
# For tests, run ./test.sh; for integration tests, run ./integration_test.sh
# Usage: ./build.sh
#
# Exit on first failure
set -euo pipefail

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
        --help|-h)
            echo "Usage: $0"
            echo "  Builds and pushes the Docker image."
            echo "  Run ./test.sh for unit tests."
            echo "  Run ./integration_test.sh for integration tests."
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

# Detect host OS/arch to decide build strategy.
# On macOS (arm64) we must cross-compile to linux/amd64 for K8s nodes.
# On Linux we build natively for the host architecture.
HOST_OS="$(uname -s)"
HOST_ARCH="$(uname -m)"

if [ "$HOST_OS" = "Darwin" ]; then
    # macOS — cross-compile to linux/amd64 via Docker buildx
    BUILD_PLATFORM="linux/amd64"
    echo "▶ Building Docker image (cross-compile: ${BUILD_PLATFORM} from ${HOST_OS}/${HOST_ARCH})..."

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
    echo "▶ Building Docker image (native: ${HOST_OS}/${HOST_ARCH})..."
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
