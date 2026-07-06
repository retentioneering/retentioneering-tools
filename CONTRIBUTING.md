# Contributing to retentioneering

Thanks for considering a contribution! This document covers everything you
need to run the project locally. For the *why* behind the architecture, see
the [Architecture Decision Records](docs/adr/README.md); the working summary
for both humans and coding agents lives in [AGENTS.md](AGENTS.md).

## Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** — manages the Python environment and
  dependencies (`uv sync` creates `.venv` automatically)
- **Node.js 20+ and npm** — only needed if you touch the widget JS or want
  to render widgets locally
- **make**

## Local setup

```bash
git clone https://github.com/retentioneering/retentioneering-tools.git
cd retentioneering-tools

make install     # = uv sync (Python deps incl. dev group) + npm install in js/
make build       # build the widget JS bundles into src/retentioneering/static/

uv run pre-commit install   # one-time; runs ruff/gitleaks/hygiene hooks on every commit
```

`make build` matters more than it looks: the JS bundles (`widget.js`,
`widget-static.js`) are **gitignored** and built from source — without them,
rendering any widget raises `FileNotFoundError`. Pure-Python work
(data processors, tools, tests) doesn't need them.

Quick sanity check that everything works:

```bash
uv run pytest tests/ -q
uv run python -c "
from retentioneering.datasets.ecom import load_ecom
from retentioneering import Eventstream
print(Eventstream(load_ecom()).get_event_counts())
"
```

## Day-to-day commands

```bash
uv run pytest tests/ -v                                   # full test suite
uv run pytest tests/data_processors/add_events_test.py -v # single file
uv run pytest tests/data_processors/add_events_test.py::test_name -v

uv run pre-commit run --all-files    # lint + format + hygiene, same as CI's lint job

make watch                           # vite --watch for js/widget — iterate on widget UI
                                     # against a running JupyterLab (uv run jupyter lab)
```

## Project layout

```
src/retentioneering/
  eventstream/      # Eventstream (the hub class) + schema
  data_processors/  # chainable transformations (filter_events, split_sessions, ...)
  tools/            # headless computations (TransitionMatrix, Funnel, ...)
  widgets/          # anywidget classes + static HTML export
  metrics/          # the single path-metrics registry
  mcp/              # MCP server exposing the eventstream to LLM agents
  datasets/         # bundled sample data
js/
  viz-core/         # shared React/MobX/Cytoscape components
  widget/           # anywidget entry point; builds into src/retentioneering/static/
docs/
  guide/            # hand-written guide pages (source of docs/build/guide)
  templates/        # Jinja templates for docstring-rendered reference pages
  scripts/          # docs pipeline (render_pages.py, generate_widget_demos.py)
  adr/              # architecture decision records
tests/              # pytest suite: eventstream, data processors, headless tools, MCP
```

## Things to know before changing code

- **Never delete a variable that "looks unused" near a SQL string.** DuckDB
  resolves `FROM df` by finding a Python variable named `df` in scope
  (replacement scan). These lines carry `# noqa: F841` comments explaining
  this. Details: [ADR-0002](docs/adr/0002-duckdb-replacement-scan.md).
- **Docstrings are the documentation.** Reference pages are rendered from
  `Eventstream` docstrings — if you change a public signature or behavior,
  update the docstring. No need to re-run `docs/scripts/render_pages.py`
  ([ADR-0013](docs/adr/0013-docstring-driven-docs.md)) because the docs
  is rebuilt automatically when the code is merged.
- **Follow the naming conventions** for any new public API — one concept,
  one name; `*_col` vocabulary; `start_event`/`end_event` anchors; duration
  strings for time inputs ([ADR-0008](docs/adr/0008-naming-conventions.md)).
- **Every widget has a headless `*_data` twin** and their signatures stay in
  lockstep ([ADR-0006](docs/adr/0006-widget-headless-twins.md)).
- **Python widget traitlets and JS model keys are one protocol** — rename
  them together, then `make build`.
- **Never hardcode credentials or secrets**, including for tests — gitleaks
  runs in pre-commit as a backstop, but don't rely on it.

## Tests

- The suite covers `Eventstream`, data processors, headless tools, and the
  MCP server. Widget *rendering* is not covered — `vite build` plus the docs
  demo generation (which constructs real widgets) are the JS correctness
  signals.
- New behavior needs tests. Prefer behavior-based assertions over pinning
  engine-dependent values (e.g. don't hardcode which rows a random sample
  picks — assert counts, integrity, and reproducibility instead).
- Test files must end with `_test.py` to be discovered.

## Submitting changes

1. Create a branch, make your changes, keep commits clean (pre-commit hooks
   run automatically if you installed them).
2. Run `uv run pytest tests/ -q` and `uv run pre-commit run --all-files`.
3. Open a PR against `master`. CI must pass: `lint` + `test` on Python
   3.11, 3.12, and 3.13 (CI builds the JS itself).
4. `master` is protected — PRs only. Merging does **not** publish a release;
   releases are cut by maintainers via `v*` tags
   ([ADR-0011](docs/adr/0011-versioning-and-release.md)).

## Questions

Open an [issue](https://github.com/retentioneering/retentioneering-tools/issues),
join the [Discord](https://discord.com/invite/hBnuQABEV2) or
[Telegram](https://t.me/retentioneering_support), or write to
retentioneering@gmail.com.
