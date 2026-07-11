# AGENTS.md

Guidance for coding agents (Claude Code, Codex, Cursor, and others) working
in this repository. Claude Code loads this file through the `@AGENTS.md`
import in `CLAUDE.md`; Claude-specific instructions, if any, go in
`CLAUDE.md` below the import.

Deep context lives in the **Architecture Decision Records** — see
[docs/adr/README.md](docs/adr/README.md) for the index. Load the ADRs
relevant to your task; the highest-stakes one is ADR-0002 (DuckDB
replacement scan — why "unused" variables must never be deleted).

## Project background

retentioneering 5.x is a ground-up rewrite of the 3.3.0 engine: the pandas-based `Eventstream`
and the iframe/CDN-loaded widgets were replaced with a DuckDB-backed `Eventstream` and
`anywidget`-based Jupyter widgets. The `3.x` branch preserves the legacy engine for reference
and patches. `CHANGELOG.md`'s `[5.0.0]` entry documents the full delta vs. 3.3.0, including
removed/added/renamed `Eventstream` methods and the 5.0 naming conventions. `docs/` (guide
pages, generated reference, ADRs) describes the current API.

**Known gaps vs. 3.3.0** (deliberately not carried over, may return later — ADR-0012): the
interactive Preprocessing Graph (visual no-code pipeline builder), and the `Cohorts`, `StatTests`,
`Sequences` tools.

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

uv run python docs/scripts/render_pages.py           # regenerate reference docs from docstrings
uv run python docs/scripts/generate_widget_demos.py  # regenerate static widget demos

uv build                   # sdist + wheel; requires `make build` to have run first so the
                            # JS bundle exists on disk (hatchling force-includes it via
                            # [tool.hatch.build] artifacts in pyproject.toml even though
                            # it's gitignored)
```

Widget tests (anywidget rendering) aren't part of the suite — `tests/` covers `Eventstream`,
data processors, and headless `tools/` only. JS has no test suite, only `vite build` as a
correctness signal (plus docs demo generation, which constructs real widgets).

## Architecture

The full rationale for each area is in `docs/adr/`; this is the working summary.

### Python: Eventstream is the hub (ADR-0001, ADR-0003)

`src/retentioneering/eventstream/eventstream.py` — `Eventstream` wraps a DuckDB-queryable
`pandas`/`pyarrow` dataframe (`self._df`) and is the single entry point users interact with.
Nearly every data processor and tool is exposed as an `Eventstream` method (`add_events`,
`filter_events`, `transition_graph`, `step_matrix`, ...), implemented in `data_processors/`,
`tools/`, and `widgets/` respectively, then wired onto the class. Processors are immutable:
every method returns a **new** `Eventstream`.

**Load-bearing DuckDB quirk (ADR-0002)**: many methods build a SQL string like
`f"SELECT ... FROM df"` and then do `df = self.df` (or `self._df`, `df_with_start_end`, etc.)
as a *separate* statement right before calling `duckdb.sql(query)`/`duckdb.query(query)`.
This isn't dead code — DuckDB's "replacement scan" resolves an unqualified table name in a SQL
string by looking up a same-named Python variable in the caller's scope. Static analysis
(ruff's F841) can't see this, so these lines carry
`# noqa: F841 -- referenced by name via DuckDB replacement scan ...`.
**Never delete a variable assignment just because it looks unused — check for a matching
`FROM <name>` in a nearby SQL string first.** The same pattern extends to user-supplied SQL:
some data processors accept a `sql=` kwarg where the *caller's* query is expected to reference
the data as `FROM eventstream` (see `filter_events`, `add_events` docstrings/tests) — the
processor exposes its dataframe under that specific variable name for exactly this reason.

### Naming conventions (ADR-0008)

One concept — one name: `path_col` / `event_col` / `timestamp_col` / `session_id_col` /
`segment_col`; window anchors are always `start_event` / `end_event`; the diff sentinel is
`<REST>`; duration inputs are strings with units (`"30m"`) or `pd.Timedelta`, time outputs are
seconds; processors are verb-first, widgets are nouns, headless methods are `<widget>_data`.
Follow these rules for any new API.

### Python: tools vs. widgets (ADR-0006)

- `tools/` — headless computation classes (`TransitionMatrix`, `StepMatrix`, `Funnel`,
  `SegmentOverview`, `ClusterAnalysis`); return dataframes, no UI.
- `widgets/` — `anywidget.AnyWidget` subclasses pairing 1:1 with most tools (`TransitionGraphWidget`,
  `StepMatrixWidget`, etc.), rendered in Jupyter. Every widget has a headless `*_data` twin on
  `Eventstream`; a widget's *data parameters* are exactly the twin's signature.
- `widgets/_esm.py` resolves the JS bundle: it only ever looks for
  `src/retentioneering/static/widget.js` and raises `FileNotFoundError` with instructions to
  run `make build` if it's missing. There is no runtime download/CDN fallback (ADR-0005) — the
  JS is built in CI and embedded directly into the wheel.
- `widgets/_html_export.py` handles static HTML export using `widget-static.js` (an IIFE build,
  vs. `widget.js`'s ESM build for live anywidget use) — this one *is* shipped inside the wheel
  unconditionally since exported HTML needs to work with no Python kernel behind it (ADR-0010).

### Path metrics (ADR-0007)

`metrics/metric_builder.py` is the single registry of per-path metrics (`length`, `duration`,
`event_count`, `has_event`, `time_between`, `first_event_time`, `active_days`,
`matches_pattern`, `in_segment`). The same `{"metric": ..., "metric_args": ...}` configs feed
clustering (`features`), segment comparison (`metrics` / `overview_metrics`), and
`filter_paths` conditions. Adding a metric also requires updating the JS metric editor and the
Path Metrics docs page.

### JS: two workspace packages, one build target (ADR-0005)

`js/` is an npm workspace (`js/viz-core`, `js/widget` — no root `package.json` deps beyond the
workspace declaration). `@retentioneering/viz-core` holds shared React/MobX/Cytoscape components;
`@retentioneering/widget` is the actual anywidget entry point,
importing from viz-core and building to **`src/retentioneering/static/`** (`widget.js` ESM +
`widget-static.js` IIFE + `widget.css`). `widget.css` is a hand-maintained source file (just a
`--retentioneering-yellow` CSS variable), **not build output** — nothing in the Vite config
regenerates it, so it's tracked in git while `widget.js`/`widget-static.js` are gitignored.
Python widget traitlets and JS `model.get/set` keys are one protocol: rename them together.

### MCP server (ADR-0009)

`mcp/server.py` is transport wiring only (FastMCP/SSE, `retentioneering.mcp.serve()`) — the
report-building logic lives in `mcp/_agent_logic.py` (`_apply_preprocessors`, summary builders),
per-session state in `mcp/_report_session.py`'s `ReportSession`, and the system prompt in
`mcp/_prompts.py`. `mcp/playbook.md` holds canonical analysis recipes surfaced via the
`playbook()` tool. MCP preprocessor step types mirror `Eventstream` method names exactly — any
API rename must be propagated to `_apply_preprocessors`, the tool docstrings, and the playbook
in the same change.

### Docs pipeline (ADR-0013)

Reference pages are rendered from `Eventstream` docstrings (`docs/scripts/render_pages.py` +
`docs/templates/*.jinja`); hand-written guides live in `docs/guide/`; widget demos are generated
by executing the `<DemoWidget>` tags against the bundled ecom dataset. Docstrings are the single
source of truth — change the docstring, re-render, done.

### CI/CD (ADR-0011)

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

### Versioning (ADR-0011)

No dynamic versioning — `pyproject.toml`'s `version = "..."` is the literal source of truth for
what gets built and published; it is not derived from git tags. Bumping the git tag alone does
not change what version the built package identifies as.
