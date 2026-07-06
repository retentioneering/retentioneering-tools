# ADR-0012: Literal versioning and tag-driven releases

Status: Accepted (recorded 2026-07)

## Context

Dynamic versioning (deriving the package version from git tags) makes local
builds and CI artifacts ambiguous, and the release flow needs to be safe
while `v5-migration` is not yet merged to protected `master`.

## Decision

- `pyproject.toml`'s `version = "..."` is the **literal source of truth**;
  it is not derived from git tags. Bumping a tag alone changes nothing about
  what the built package identifies as.
- Publishing is triggered **only** by pushing a `v*` tag (`release.yml`):
  build JS → test → `uv build` → verify `widget.js` landed in the wheel →
  `uv publish` via OIDC trusted publishing (no stored token) → GitHub
  Release with notes extracted from the matching `CHANGELOG.md` section.
- Merging to `master` does not release. `master` is protected: PRs only,
  required checks (`lint`, `test 3.11/3.12/3.13`), no force-push.
- Safety-valve flows in the Makefile: `make test-release` publishes to
  TestPyPI from any branch; `make rc-release` publishes real `rcN`
  pre-releases from any branch (rc suffix enforced); `make release` refuses
  to run anywhere but an up-to-date `master` with a matching CHANGELOG
  section.

## Consequences

- Every (Test)PyPI version string is burned forever — bump the version
  between repeated test publishes.
- The release ritual is: bump version + CHANGELOG by hand → PR into master →
  `make release VERSION=x.y.z`.
