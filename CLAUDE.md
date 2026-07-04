# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

This repo is mid-migration (branch `v5-migration`, not yet merged to `master`). The Python package
was completely rewritten and imported from a private companion project (internal codename
"hopscotch"), replacing the pandas/iframe-based 3.3.0 engine with a DuckDB-backed `Eventstream` and
`anywidget`-based Jupyter widgets. `master` still reflects the old 3.3.0 engine; a `3.x` branch
preserves that state for reference/patches.

`README.md` and `docs/` describe the **old 3.3.0 API** (pandas `Eventstream`, preprocessing GUI,
Cohorts/StatTests/Sequences tools) and have not been updated for the rewrite — don't treat their
technical claims as current. `CHANGELOG.md`'s `[5.0.0]` entry is the accurate description of what
changed vs. 3.3.0, including a full list of removed/added/renamed `Eventstream` methods.

**Known gaps vs. 3.3.0** (deliberately not carried over, may return later): the interactive
Preprocessing Graph (visual no-code pipeline builder), and the `Cohorts`, `StatTests`, `Sequences`
tools.

## Commands

```bash
uv sync                    # install Python deps (+ dev group: pytest, ruff, pre-commit)
cd js && npm install       # install JS deps (npm workspaces: viz-core, widget)
# or: make install         # does both

make build                 # build JS and drop widget.js/widget-static.js into src/retentioneering/static/
make watch                 # vite --watch for js/widget, for iterating on widget UI

uv run pytest tests/ -v                          # full test suite
uv run pytest tests/data_processors/add_events_test.py -v          # single file
uv run pytest tests/data_processors/add_events_test.py::test_name -v   # single test
# or: make test            # full suite only

uv run pre-commit run --all-files    # ruff (lint+format) + gitleaks + hygiene hooks
uv run pre-commit install            # one-time, makes hooks run automatically on `git commit`

uv build                   # sdist + wheel; requires `make build` to have run first so the
                            # JS bundle exists on disk (hatchling force-includes it via
                            # [tool.hatch.build] artifacts in pyproject.toml even though
                            # it's gitignored)
```

Widget tests (anywidget rendering) aren't part of the suite — `tests/` covers `Eventstream`,
data processors, and headless `tools/` only. JS has no test suite, only `vite build` as a
correctness signal.

## Architecture

### Python: Eventstream is the hub

`src/retentioneering/eventstream/eventstream.py` — `Eventstream` wraps a DuckDB-queryable
`pandas`/`pyarrow` dataframe (`self._df`) and is the single entry point users interact with.
Nearly every data processor and tool is exposed as an `Eventstream` method (`add_events`,
`filter_events`, `transition_graph`, `step_matrix`, ...), implemented in `data_processors/`,
`tools/`, and `widgets/` respectively, then wired onto the class.

**Load-bearing DuckDB quirk**: many methods build a SQL string like `f"SELECT ... FROM df"` and
then do `df = self.df` (or `self._df`, `df_with_start_end`, etc.) as a *separate* statement right
before calling `duckdb.sql(query)`/`duckdb.query(query)`. This isn't dead code — DuckDB's
"replacement scan" resolves an unqualified table name in a SQL string by looking up a
same-named Python variable in the caller's scope. Static analysis (ruff's F841) can't see this,
so these lines carry `# noqa: F841 -- referenced by name via DuckDB replacement scan ...`.
**Never delete a variable assignment just because it looks unused — check for a matching
`FROM <name>` in a nearby SQL string first.** The same pattern extends to user-supplied SQL: some
data processors accept a `sql=` kwarg where the *caller's* query is expected to reference the
data as `FROM eventstream` (see `filter_events`, `add_events` docstrings/tests) — the processor
exposes its dataframe under that specific variable name for exactly this reason.

### Python: tools vs. widgets

- `tools/` — headless computation classes (`TransitionMatrix`, `StepMatrix`, `Funnel`,
  `SegmentOverview`, `ClusterAnalysis`); return dataframes, no UI.
- `widgets/` — `anywidget.AnyWidget` subclasses pairing 1:1 with most tools (`TransitionGraphWidget`,
  `StepMatrixWidget`, etc.), rendered in Jupyter. `TransitionGraphWidget` and `StepMatrixWidget`
  additionally inherit `CloudMixin` (`widgets/cloud_mixin.py`) for cloud save/load of widget
  display state; the other widgets wrap their content in `AuthGate` with `disabled={true}`
  hardcoded on the JS side (dead code path, not wired to CloudMixin).
- `widgets/_esm.py` resolves the JS bundle: it only ever looks for
  `src/retentioneering/static/widget.js` and raises `FileNotFoundError` with instructions to
  run `make build` if it's missing. There is no runtime download/CDN fallback (unlike the old
  3.3.0 iframe widgets, and unlike this project's own hopscotch-era prior art) — the JS is built
  in CI and embedded directly into the wheel.
- `widgets/_html_export.py` handles static HTML export using `widget-static.js` (an IIFE build,
  vs. `widget.js`'s ESM build for live anywidget use) — this one *is* shipped inside the wheel
  unconditionally since exported HTML needs to work with no Python kernel behind it.

### Cloud save/load is present but off by default

`widgets/cloud.py` and `cloud_mixin.py` implement Supabase-backed cloud save/load, fully
functional, but gated by env vars with empty/false defaults — this repo doesn't run a backend for
it:
- `RETENTIONEERING_CLOUD_ENABLED` (bool) — hides the cloud icon entirely on the JS side when unset.
- `RETENTIONEERING_CLOUD_SUPABASE_URL` / `_ANON_KEY` — read at both build time (JS, via Vite
  `define`) and runtime (Python, via `os.environ`); empty by default.
- `RETENTIONEERING_CLOUD_MANAGE_URL` — "Manage saved widgets" link; hidden when unset.

Don't hardcode real credentials here even for testing — the whole point of this design is that
the public repo ships no bundled backend.

### JS: two workspace packages, one build target

`js/` is an npm workspace (`js/viz-core`, `js/widget` — no root `package.json` deps beyond the
workspace declaration). `@retentioneering/viz-core` holds shared React/MobX/Cytoscape components
(including the cloud auth UI); `@retentioneering/widget` is the actual anywidget entry point,
importing from viz-core and building to **`src/retentioneering/static/`** (`widget.js` ESM +
`widget-static.js` IIFE + `widget.css`). `widget.css` is a hand-maintained source file (just a
`--retentioneering-yellow` CSS variable), **not build output** — nothing in the Vite config
regenerates it, so it's tracked in git while `widget.js`/`widget-static.js` are gitignored.

### MCP server

`mcp/server.py` exposes the eventstream to LLM agents over SSE (`retentioneering.mcp.serve()`).
`mcp/playbook.md` holds canonical analysis recipes surfaced via the `playbook()` tool.

### CI/CD

- `.github/workflows/ci.yml` — on push/PR: `lint` job (pre-commit --all-files) + `test` job
  (matrix over Python 3.11-3.13, builds JS, runs pytest).
- `.github/workflows/release.yml` — on `v*` tag push (including PEP 440 `rc` tags, e.g.
  `v5.0.0rc1`): build JS, test, `uv build`, verify `widget.js` actually landed in the wheel,
  `uv publish` to real PyPI via OIDC trusted publishing (`environment: pypi`, no stored token),
  create a GitHub Release with notes extracted from the matching `## [VERSION]` section of
  `CHANGELOG.md`.
- `.github/workflows/test-release.yml` — manual (`workflow_dispatch`), same pipeline but
  publishes to TestPyPI (`environment: testpypi`) instead. Run via `make test-release` from any
  branch. TestPyPI (like real PyPI) permanently reserves each version string once published, so
  bump `pyproject.toml`'s version between repeated test runs.
- `master` is protected by a GitHub ruleset: PRs only (no direct push), no force-push, no
  deletion, required status checks (`lint`, `test (3.11)`, `test (3.12)`, `test (3.13)`), 0
  required approvals, `role:admin` can bypass. Merging to `master` does **not** trigger a
  release — only pushing a `v*` tag does. See the `release` target's comments in `Makefile` for
  the intended flow (bump version + CHANGELOG by hand → PR into master → `make release
  VERSION=x.y.z` once merged, which tags and pushes).
- `.git-blame-ignore-revs` lists the one-time full-codebase `ruff format` commit so `git blame`
  (with `git config blame.ignoreRevsFile .git-blame-ignore-revs`) and GitHub's web blame skip
  past it to real authorship.

### Versioning

No dynamic versioning — `pyproject.toml`'s `version = "..."` is the literal source of truth for
what gets built and published; it is not derived from git tags. Bumping the git tag alone does
not change what version the built package identifies as.
