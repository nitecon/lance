# Release Process

This document describes how to create a new release of LANCE.

## Automated Releases

Releases are automatically built and published via GitHub Actions when you push a version tag.

### Creating a Release

1. **Update version** in `Cargo.toml`:
   ```toml
   [package]
   version = "1.0.0"
   ```

2. **Commit all changes** to main branch:
   ```bash
   git add -A
   git commit -m "chore: bump version to v1.0.0"
   ```

3. **Create and push a version tag:**
   ```bash
   git tag v1.0.0
   git push origin main
   git push origin v1.0.0
   ```

4. **GitHub Actions will automatically:**
    - Run full test suite
    - Build optimized release binaries for Linux and macOS
    - Create a GitHub Release with formatted description
    - Attach the built binaries to the release
    - Generate release notes from commits

### Release Artifacts

The following artifacts are created and attached to the GitHub Release:

| Platform | Artifact | Architecture | Notes |
|----------|----------|--------------|-------|
| **Linux** | `lance-linux-x86_64.tar.gz` | x86_64 | Requires kernel 5.15+ for io_uring |
| **Linux** | `lance-linux-aarch64.tar.gz` | ARM64 | For AWS Graviton, etc. |
| **macOS** | `lance-macos-x86_64.tar.gz` | Intel | macOS 11+ |
| **macOS** | `lance-macos-aarch64.tar.gz` | Apple Silicon | macOS 11+ |

> **Windows Users**: Run LANCE in a Docker container. See [Docker Deployment](../docs/Architecture.md#10-deployment--containerization).

### Release Page Format

Each GitHub Release includes:

- **Release Title**: "LANCE v1.0.0" (automatically set from tag)
- **Description**: Formatted markdown with:
    - Changelog highlights
    - Breaking changes (if any)
    - Performance improvements
    - Download instructions per platform
- **Auto-Generated Changelog**: List of commits since last release
- **Binary Attachments**: All platform builds attached as downloadable assets

---

## Manual Build (Local Testing)

To test the build process locally:

### Prerequisites

```bash
# Rust toolchain (1.75+)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable

# For cross-compilation (optional)
rustup target add x86_64-unknown-linux-gnu
rustup target add aarch64-unknown-linux-gnu
rustup target add x86_64-apple-darwin
rustup target add aarch64-apple-darwin
```

### Debug Build

```bash
cargo build
# Output: target/debug/lance
```

### Release Build (Optimized)

```bash
cargo build --release
# Output: target/release/lance
```

### Cross-Compile for Linux (from macOS)

```bash
# Install cross-compilation toolchain
cargo install cross

# Build for Linux x86_64
cross build --release --target x86_64-unknown-linux-gnu
```

---

## Build Configuration

Build settings are configured in `Cargo.toml`:

```toml
[profile.release]
opt-level = 3          # Maximum optimization
lto = "fat"            # Link-time optimization
codegen-units = 1      # Single codegen unit for better optimization
panic = "abort"        # Smaller binary, no unwinding
strip = true           # Strip symbols
```

### Feature Flags

| Flag | Description | Default |
|------|-------------|---------|
| `io_uring` | Enable io_uring support (Linux only) | Enabled |
| `simd` | Enable SIMD-accelerated CRC32 | Enabled |
| `jemalloc` | Use jemalloc allocator | Disabled |

```bash
# Build with specific features
cargo build --release --features "jemalloc"

# Build without io_uring (for older kernels)
cargo build --release --no-default-features
```

---

## Troubleshooting

### io_uring not available

LANCE requires Linux kernel 5.15+ for full io_uring support. On older kernels:
- LANCE will automatically fall back to `pwritev2`
- A warning is logged at startup
- Performance will be degraded (~30% slower writes)

### Build fails on macOS

Ensure Xcode Command Line Tools are installed:
```bash
xcode-select --install
```

### Linking errors

For Linux builds, ensure `musl` is available for static linking:
```bash
# Ubuntu/Debian
sudo apt-get install musl-tools

# Build with musl for portable binary
cargo build --release --target x86_64-unknown-linux-musl
```

### Binary size optimization

Release binaries are typically:
- Linux x86_64: ~5-8 MB (stripped)
- macOS Universal: ~10-15 MB

To further reduce size:
```bash
# Strip debug symbols (already in release profile)
strip target/release/lance

# Use UPX compression (optional)
upx --best target/release/lance
```

---

## CI/CD Workflows

### CI Workflow (`.github/workflows/ci.yml`)

Runs on every push to `main` and all pull requests:

- **Lint**: `cargo fmt --check`, `cargo clippy`
- **Test**: `cargo test` on Linux and macOS
- **Build**: Verify release build compiles
- **Audit**: Check for security vulnerabilities in dependencies
- **Mechanical Integrity**: Enforce hot-path rules from CodingGuidelines

### Release Workflow (`.github/workflows/release.yml`)

Triggered by version tags (`v*`):

- Builds optimized binaries for Linux (x86_64, aarch64) and macOS (x86_64, aarch64)
- Creates GitHub Release with binaries attached
- Publishes Docker image to GitHub Container Registry
- Generates SHA256 checksums for verification
- Can be manually triggered via `workflow_dispatch`

---

## Version Numbering

LANCE follows [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking changes to API, storage format, or wire protocol
- **MINOR**: New features, backwards-compatible
- **PATCH**: Bug fixes, performance improvements

### Pre-release Versions

```bash
# Alpha (unstable, breaking changes expected)
git tag v1.0.0-alpha.1

# Beta (feature-complete, bug fixes only)
git tag v1.0.0-beta.1

# Release Candidate (ready for production testing)
git tag v1.0.0-rc.1
```

---

## Docker Images

Docker images are published to GitHub Container Registry on each release:

```bash
# Pull specific version
docker pull ghcr.io/nitecon/lance:v1.0.0

# Pull latest
docker pull ghcr.io/nitecon/lance:latest

# Run with volume mounts
docker run -d \
  --name lance \
  -v /data/lance:/var/lib/lance \
  -p 9000:9000 \
  ghcr.io/nitecon/lance:v1.0.0
```

### Multi-architecture Support

Docker images are built for:
- `linux/amd64` (x86_64)
- `linux/arm64` (aarch64)

Docker will automatically pull the correct architecture for your platform.
