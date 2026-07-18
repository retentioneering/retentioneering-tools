<!-- Thanks for contributing to retentioneering! Keep the PR focused on one logical change. -->

## What & why

<!-- One to three sentences. Link the issue this addresses: Fixes #NNN -->

## Before / after

<!-- For a bug fix: a minimal repro and the behavior before vs after.
     For a feature: the use case and the new usage. -->

## Checklist

- [ ] Ran `make install-dev` (one-time per clone) so the git hook is active
- [ ] `uv run pre-commit run --all-files` passes (ruff lint + format, hygiene, gitleaks) — this is CI's `lint` job
- [ ] `uv run pytest tests/ -q` passes
- [ ] Added/updated tests (a bug fix includes the failing-before test)
- [ ] Docstrings updated and, if they changed, `uv run python docs/scripts/render_pages.py` re-run
- [ ] Added an entry under `CHANGELOG.md`'s `[Unreleased]` section (skip only for docs/CI/internal-only changes)
- [ ] Sync obligations handled where applicable (MCP tool layer, JS metric editor / widget contract)

## Breaking changes

<!-- None, or describe them plus a migration note. Note: PRs target `master`; releases are tag-driven. -->
