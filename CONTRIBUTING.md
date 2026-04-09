# Contributing to FrogDB

## Development Setup

See the [README](README.md) for build prerequisites. The project uses
[just](https://just.systems/) as its task runner — run `just --list` to see available
recipes.

## Pull Request Workflow

1. Fork the repo and create a branch from `main`.
2. Make your changes. Run `just check && just test && just lint` before pushing.
3. Open a PR against `main`. Fill out the [PR template](.github/pull_request_template.md).
4. CI must pass before merge. The `CI Pass` aggregation job gates all checks.

## Commit Messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/) with the
following prefixes:

| Prefix | Purpose | Appears in changelog |
|--------|---------|---------------------|
| `feat:` | New feature | Yes |
| `fix:` | Bug fix | Yes |
| `refactor:` | Code refactoring | Yes |
| `perf:` | Performance improvement | Yes |
| `deps:` | Dependency update | Yes |
| `docs:` | Documentation | No |
| `ci:` | CI/CD changes | No |
| `chore:` | Miscellaneous | No |

Use a `!` suffix for breaking changes (e.g., `feat!: rename config key`).

## Release Process

Releases are automated via [release-please](https://github.com/googleapis/release-please):

1. Conventional commit messages on `main` are tracked automatically.
2. Release-please opens a PR titled "chore: release vX.Y.Z" with a version bump and
   changelog update.
3. Merging that PR creates a `vX.Y.Z` tag on `main`.
4. The tag triggers the [release workflow](.github/workflows/release.yml), which:
   - Builds multi-arch Docker images (GHCR + Docker Hub)
   - Builds Linux and macOS binaries
   - Creates a GitHub Release with tarballs, `.deb` packages, checksums, and cosign
     signatures
   - Publishes a Helm chart to GitHub Pages
   - Updates the Homebrew tap
   - Updates the APT repository on GitHub Pages
5. Publishing jobs require manual approval from a maintainer in the GitHub Actions UI
   (deployment environment gate).

## Security

See [SECURITY.md](SECURITY.md) for the vulnerability disclosure policy and artifact
verification instructions.
