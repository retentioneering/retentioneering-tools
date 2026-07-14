# Repository conventions (retentioneering-tools, branch v5-migration)

Condensed from AGENTS.md, CONTRIBUTING.md, and docs/adr/ of this checkout. When in
doubt, those files win; ADR index: `docs/adr/README.md`.

## Commands

```bash
uv sync                              # python deps incl. dev group (pytest, ruff, pre-commit)
cd js && npm install                 # only when touching JS (npm workspaces)
make build                           # build widget.js/widget-static.js into src/retentioneering/static/
make watch                           # vite --watch for widget UI iteration

uv run pytest tests/ -v              # full suite; single file/test supported
uv run pre-commit run --all-files    # ruff lint+format, gitleaks, hygiene hooks
uv run python docs/scripts/render_pages.py          # regen reference docs from docstrings
uv run python docs/scripts/generate_widget_demos.py # regen static widget demos
```

## Architecture rules that gate PRs

- **Unified query engine (L1)** — all DuckDB execution goes through the query-engine
  layer. The old "replacement scan" idiom (bare `duckdb.sql(f"... FROM df")` resolving
  caller-scope variables; ADR-0002) is SUPERSEDED — do not reintroduce it. User-facing
  `sql=` parameters still expose the data under the `eventstream` alias.
- **Eventstream is immutable**: processors return new instances; `stream.df` is a
  read-only property. Lineage is recorded (`recipe()` / `from_recipe()`) — new processors
  must serialize into the ops model (see `ops.py`).
- **Tools vs widgets split** (ADR-0006): headless computation in `tools/`, anywidget
  wrappers in `widgets/`; every widget has a `<name>_data` twin whose data parameters
  match the widget's exactly.
- **Docstring-driven docs** (ADR-0013): the docs site's reference pages render from
  docstrings — a docs fix is usually a docstring fix + `render_pages.py`. Guide pages
  live in `docs/guide/*.md`.
- **Naming** (ADR-0008): one concept, one name — `path_col`, `event_col`,
  `timestamp_col`, `session_col`, `segment_col`; windows are `start_event`/`end_event`;
  duration inputs are unit-strings ("30m") or Timedelta, outputs are seconds; processors
  are verb-first, widgets are nouns; diff sentinel is `"<REST>"`.
- **Metric schema registry**: metric parse/validate and the JS metric editor's name list
  are generated from one schema registry — adding a metric means extending the registry,
  not hand-editing parallel lists.

## Cross-cutting sync obligations (change one → update all in the same PR)

| You changed | Also update |
|---|---|
| Any public Eventstream method name/signature | MCP tool layer (`mcp/`), its docstrings, playbook |
| Metric definitions | schema registry (drives both Python validation and JS editor) |
| Widget traitlets / data contract | JS side (`js/viz-core`, `js/widget`) — Python traitlets and JS `model.get/set` keys are one protocol |
| Docstrings of public API | `render_pages.py` regen (commit output if the repo tracks it) |
| User-visible behavior | `CHANGELOG.md` entry |

## Quality bar

- Tests colocated under `tests/<area>/`; bug fixes ship with the failing-before test.
- House error style: fail fast and helpful — list valid values/keys in the message
  (e.g., schema errors name valid keys; metric errors name the required arg). Silent
  data loss or silent empty results are auto-reject.
- Determinism: seeded stochastic steps; stable ordering on timestamp ties (ORDER BY
  timestamp, subindex in aggregations).
- CI (GitHub Actions): `lint` + `test` matrix on Python 3.11–3.13, builds JS first.
  `master` is protected: PRs only; release is tag-driven (`v*`), not merge-driven.
- Telemetry code paths: never send data values — method names/arg names only; any change
  near `_tracking.py` must preserve this and the documented opt-out.

## Small map of the tree

`src/retentioneering/{eventstream,data_processors,tools,widgets,metrics,mcp,utils}` ·
`js/{viz-core,widget}` (npm workspaces → build outputs land in `src/.../static/`) ·
`docs/{guide,adr,scripts}` · `tests/` mirrors `src` areas.
