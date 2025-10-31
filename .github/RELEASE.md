# Release Process

This document describes how to create a new release for datafusion-bio-formats.

## Prerequisites

- You must have write access to the repository
- The workflow can only be triggered from the `master` branch
- All tests and checks must pass

## Creating a Release

### 1. Manual Release via GitHub Actions

1. Go to the [Actions tab](../../actions) in GitHub
2. Select the "Release" workflow from the left sidebar
3. Click "Run workflow" button (top right)
4. Select the version bump type:
   - **patch**: Bug fixes and minor changes (0.1.0 → 0.1.1)
   - **minor**: New features, backward compatible (0.1.0 → 0.2.0)
   - **major**: Breaking changes (0.1.0 → 1.0.0)
5. Optionally mark as pre-release
6. Click "Run workflow"

### What the Workflow Does

The release workflow will automatically:

1. ✅ Bump the version in all crate `Cargo.toml` files
2. ✅ Update `Cargo.lock`
3. ✅ Run all tests to ensure everything passes
4. ✅ Run clippy checks
5. ✅ Build documentation
6. ✅ Commit the version changes
7. ✅ Create and push a git tag (e.g., `v0.2.0`)
8. ✅ Generate a changelog from git commits
9. ✅ Create a GitHub Release with the changelog

### Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** version: Incompatible API changes
- **MINOR** version: Add functionality in a backward compatible manner
- **PATCH** version: Backward compatible bug fixes

### Current Version

Current version: **v0.1.0**

### Version History

- `v0.1.0` - Initial release

## Publishing to crates.io

Publishing to crates.io is currently disabled in the workflow. To enable it:

1. Add a `CARGO_TOKEN` secret to your GitHub repository:
   - Go to Settings → Secrets and variables → Actions
   - Add a new secret named `CARGO_TOKEN` with your crates.io API token
2. Uncomment the "Publish to crates.io" step in `.github/workflows/release.yml`

## Rollback

If you need to roll back a release:

1. Delete the tag locally and remotely:
   ```bash
   git tag -d v0.2.0
   git push origin :refs/tags/v0.2.0
   ```
2. Delete the GitHub Release in the Releases page
3. Revert the version bump commit:
   ```bash
   git revert <commit-hash>
   git push origin master
   ```

## Troubleshooting

### Workflow fails on "Only run on master branch"

Make sure you're running the workflow from the `master` branch, not `main` or any other branch.

### Tests fail during release

The workflow will abort if any tests fail. Fix the issues and try again.

### Tag already exists

If the tag already exists, you'll need to delete it first or bump to a different version.

## Example Commit Messages

Good commit messages help generate better changelogs:

- ✅ `feat: add support for VCF 4.3 format`
- ✅ `fix: resolve memory leak in BGZF reader`
- ✅ `docs: update installation instructions`
- ✅ `perf: improve GFF parsing performance by 30%`
- ✅ `refactor: simplify table provider interface`

## Questions?

For questions or issues with the release process, please:
- Open an issue
- Contact the maintainers
