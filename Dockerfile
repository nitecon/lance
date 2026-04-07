# LANCE Dockerfile
# Production-ready multi-stage build
#
# Build args:
#   VERSION     - Version tag (default: from Cargo.toml)
#   BUILD_MODE  - "release" (default) or "debug" for dev builds
#   FEATURES    - Additional cargo features to enable
#   CARGO_BUILD_JOBS - Cargo parallel jobs inside Docker build (default: 1)
#
# Examples:
#   docker build -t lance .                                    # Production build
#   docker build -t lance:dev --build-arg BUILD_MODE=debug .   # Dev build
#   docker build -t lance --build-arg FEATURES=tls .           # With TLS

# =============================================================================
# Build Arguments
# =============================================================================
ARG VERSION=latest
ARG BUILD_MODE=release
ARG FEATURES=""
ARG CARGO_BUILD_JOBS=1

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM rust:1.85-bookworm AS builder

ARG BUILD_MODE
ARG FEATURES
ARG CARGO_BUILD_JOBS

# Keep Dockerized release builds memory-stable under Colima/QEMU.
# The local benchmark binaries are still built natively in build.sh.
ENV CARGO_PROFILE_RELEASE_LTO=false
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
ENV CARGO_PROFILE_RELEASE_OPT_LEVEL=2
ENV CMAKE_BUILD_PARALLEL_LEVEL=1

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CC=clang

# Copy entire project
COPY . .

# Build based on mode
RUN if [ "$BUILD_MODE" = "debug" ]; then \
        if [ -n "$FEATURES" ]; then \
            cargo build -j "$CARGO_BUILD_JOBS" --package lance --features "$FEATURES"; \
        else \
            cargo build -j "$CARGO_BUILD_JOBS" --package lance; \
        fi \
    else \
        if [ -n "$FEATURES" ]; then \
            cargo build -j "$CARGO_BUILD_JOBS" --release --package lance --features "$FEATURES"; \
        else \
            cargo build -j "$CARGO_BUILD_JOBS" --release --package lance; \
        fi \
    fi

# Strip binary in release mode for smaller size
RUN if [ "$BUILD_MODE" = "release" ]; then \
        strip target/release/lance; \
    fi

# =============================================================================
# Stage 2: Runtime (Production)
# =============================================================================
FROM debian:stable-slim AS runtime

ARG VERSION
ARG BUILD_MODE=release

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash lance

# Create directories
RUN mkdir -p /var/lib/lance /etc/lance && chown -R lance:lance /var/lib/lance /etc/lance

# Copy binary from builder (handles both release and debug builds)
COPY --from=builder /build/target/${BUILD_MODE}/lance /usr/local/bin/lance

# Set ownership
RUN chown lance:lance /usr/local/bin/lance && chmod +x /usr/local/bin/lance

# Switch to non-root user
USER lance

# Set working directory
WORKDIR /var/lib/lance

# Expose ports:
#   1992 - LWP client connections (producers/consumers)
#   1993 - Replication port (inter-node Raft/log sync, internal cluster traffic)
#   1994 - Resync server port (internal catch-up replication)
#   1995 - Data plane replication port (internal leader-to-follower data replication)
#   9090 - Prometheus metrics scrape endpoint
#   8080 - Health checks (/health/live, /health/ready for k8s probes)
EXPOSE 1992 1993 1994 1995 9090 8080

# Health check using netcat (more reliable than binary flag)
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=5 \
    CMD nc -z localhost 1992 || exit 1

# Default entrypoint - args passed via docker-compose or command line
ENTRYPOINT ["/usr/local/bin/lance"]

# =============================================================================
# Labels
# =============================================================================
LABEL org.opencontainers.image.title="LANCE"
LABEL org.opencontainers.image.description="High-performance, non-blocking stream engine"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.vendor="Nitecon"
LABEL org.opencontainers.image.source="https://github.com/Nitecon/lance"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Environment
ENV LANCE_VERSION=${VERSION}
