# Contributing to LANCE

Thank you for your interest in contributing to LANCE! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- **Rust**: 1.75+ (install via [rustup](https://rustup.rs/))
- **Python**: 3.8+ (for pre-commit hooks)
- **Git**: 2.x+

### Initial Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Nitecon/lance.git
   cd lance
   ```

2. **Install Rust toolchain**:
   ```bash
   rustup update stable
   rustup component add rustfmt clippy
   ```

3. **Install pre-commit hooks**:
   ```bash
   pip install pre-commit
   pre-commit install
   pre-commit install --hook-type commit-msg
   ```

4. **Verify setup**:
   ```bash
   cargo build
   cargo test
   pre-commit run --all-files
   ```

## Pre-commit Hooks

We use pre-commit hooks to ensure code quality before commits. The hooks will:

- **Format code** with `rustfmt`
- **Lint code** with `clippy` (strict mode)
- **Check compilation** with `cargo check`
- **Enforce mechanical integrity** rules from [CodingGuidelines](docs/CodingGuidelines.md)
- **Validate commit messages** (Conventional Commits format)

### Running Hooks Manually

```bash
# Run all hooks on all files
pre-commit run --all-files

# Run specific hook
pre-commit run cargo-fmt --all-files
pre-commit run cargo-clippy --all-files

# Skip hooks temporarily (NOT recommended)
git commit --no-verify -m "wip: temporary commit"
```

### Updating Hooks

```bash
pre-commit autoupdate
```

## Commit Message Format

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting, no code change |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf` | Performance improvement |
| `test` | Adding or updating tests |
| `build` | Build system or dependencies |
| `ci` | CI/CD configuration |
| `chore` | Other changes (e.g., .gitignore) |
| `deps` | Dependency updates |

### Examples

```bash
feat(network): add LWP protocol framing
fix(io): handle io_uring EAGAIN correctly
perf(core): use SIMD for CRC32 calculation
docs: update architecture diagram
deps: bump bytes crate to 1.5
```

## Code Style

### Mandatory Rules

See [CodingGuidelines.md](docs/CodingGuidelines.md) for the complete list. Key rules:

1. **No allocations on hot path** — Use `LoanableBatch`, `Bytes` slicing
2. **No locks on data plane** — Use atomics, lock-free queues
3. **No `.unwrap()` or `.expect()`** — Use `Result` with `?`
4. **No dynamic dispatch** — Use `T: Trait`, not `Box<dyn Trait>`
5. **Document all `unsafe`** — Include `// SAFETY:` comments

### Formatting

Code is auto-formatted with `rustfmt`. Configuration is in `rustfmt.toml`.

```bash
cargo fmt --all
```

### Linting

We use strict Clippy settings:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

## Testing

### Running Tests

```bash
# All tests
cargo test --all-features

# Specific test
cargo test test_name

# With output
cargo test -- --nocapture
```

### Writing Tests

- Place unit tests in the same file as the code
- Place integration tests in `tests/`
- Include latency assertions for performance-critical code:

```rust
#[test]
fn ingestion_latency_regression() {
    let result = benchmark_ingestion(1_000_000);
    assert!(result.p99_ns < 5_000, "P99 regression: {} ns", result.p99_ns);
}
```

## Pull Request Process

1. **Create a feature branch**:
   ```bash
   git checkout -b feat/my-feature
   ```

2. **Make changes** following the coding guidelines

3. **Ensure all checks pass**:
   ```bash
   cargo fmt --all
   cargo clippy --all-targets --all-features -- -D warnings
   cargo test --all-features
   pre-commit run --all-files
   ```

4. **Push and create PR**:
   ```bash
   git push origin feat/my-feature
   ```

5. **PR will be reviewed** for:
   - Mechanical integrity (no allocations/locks on hot path)
   - Test coverage
   - Documentation
   - Performance impact

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions or ideas

Thank you for contributing! 🦀
