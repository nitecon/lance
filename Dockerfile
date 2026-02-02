# LANCE Dockerfile
# Multi-stage build for minimal production image

# Build arguments
ARG VERSION=dev

# =============================================================================
# Stage 1: Build
# =============================================================================
FROM rust:1.75-bookworm AS builder

ARG VERSION

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock* ./

# Create dummy src for dependency compilation
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies only (cached layer)
RUN cargo build --release && rm -rf src target/release/deps/lance*

# Copy actual source code
COPY src/ src/

# Build the actual binary
RUN cargo build --release --locked

# Strip the binary for smaller size
RUN strip target/release/lance

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM debian:bookworm-slim AS runtime

ARG VERSION

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash lance

# Create data directory
RUN mkdir -p /var/lib/lance && chown lance:lance /var/lib/lance

# Copy binary from builder
COPY --from=builder /build/target/release/lance /usr/local/bin/lance

# Copy default config if exists
# COPY --from=builder /build/lance.toml /etc/lance/lance.toml

# Set ownership
RUN chown lance:lance /usr/local/bin/lance

# Switch to non-root user
USER lance

# Set working directory
WORKDIR /var/lib/lance

# Expose default port
EXPOSE 9000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/lance", "--health-check"] || exit 1

# Default command
ENTRYPOINT ["/usr/local/bin/lance"]
CMD ["--config", "/etc/lance/lance.toml"]

# =============================================================================
# Labels
# =============================================================================
LABEL org.opencontainers.image.title="LANCE"
LABEL org.opencontainers.image.description="High-performance, non-blocking stream engine"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.vendor="Nitecon"
LABEL org.opencontainers.image.source="https://github.com/Nitecon/lance"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Environment variable for version (accessible at runtime)
ENV LANCE_VERSION=${VERSION}
