<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# Repository Guidelines

## Project Structure & Modules
- Workspace: Rust monorepo managed by `Cargo.toml` at root.
- Crates: format-specific sources under `datafusion/bio-format-*` (e.g., `...-gff`, `...-vcf`, `...-fastq`).
- Code: `src/` per crate; examples in `examples/`; integration tests in `tests/`.
- Utilities: pre-commit hooks in `.pre-commit-config.yaml`; formatting in `rustfmt.toml`.

## Build, Test, and Develop
- Build all crates: `cargo build --workspace --all-targets`
- Check fast: `cargo check --workspace`
- Run tests (all): `cargo test --workspace`
- Test a crate: `cargo test -p datafusion-bio-format-gff`
- Run an example: `cargo run -p datafusion-bio-format-vcf --example datafusion_integration`
- Run GFF benchmark bin: `cargo run -p datafusion-bio-format-gff --bin benchmark_bgzf_threads`
- Format: `cargo fmt --all` (also via pre-commit)

## Coding Style & Naming
- Rust 2024 edition; idiomatic Rust.
- Formatting: enforced by `rustfmt`; 4-space indent, standard line wrapping.
- Naming: `snake_case` for files/functions, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for consts.
- Imports: group by crate; prefer explicit `use` paths over glob imports.
- Lints: run `cargo clippy --all-targets -- -D warnings` locally when changing APIs or performance-critical code.

## Testing Guidelines
- Framework: Rust built-in tests. Place crate-level tests in `tests/` and unit tests under `src/*` with `#[cfg(test)]`.
- Conventions: name tests after behavior, e.g., `parses_bgzf_blocks`.
- Data: keep fixtures small; use temp files for large cases. Avoid committing large datasets.
- Run a single test: `cargo test -p datafusion-bio-format-fastq parse_record -- --nocapture`

## Commit & Pull Requests
- Commits: concise, imperative subject (max ~72 chars). Example: `Optimize GFF: reduce allocations in parser`.
- Scope: group related changes; include brief body for rationale, perf notes, or migration steps.
- PRs: include description, linked issues, reproduction/usage snippet, and before/after benchmarks if performance changes (e.g., output from `benchmark_bgzf_threads`).
- CI hygiene: ensure `cargo fmt`, `cargo check`, and tests pass locally.

## Security & Configuration
- Storage backends via `opendal`: prefer environment credentials (e.g., `AWS_*`, `GOOGLE_APPLICATION_CREDENTIALS`, `AZURE_STORAGE_*`). Do not commit secrets or large data files.
- Logging: enable with `RUST_LOG=info` for debugging.

