# Contributing to retentioneering

Thanks for considering a contribution!

We welcome all possible types of contributions, including, but not limited to bug reports, documentation improvements, examples, visualizations, analytical recipes, agent skills, integrations, performance improvements, API proposals, and new analytical capabilities.

This document covers everything you
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
                 #   + pre-commit install (git hook that runs ruff/gitleaks/hygiene on every commit)
make build       # build the widget JS bundles into src/retentioneering/static/
```

`make install` wires up the pre-commit git hook for you, so your commits are
auto-checked. If you set up the environment without `make install`, run
`uv run pre-commit install` once yourself — otherwise commits skip the hooks
locally and CI's `lint` job (which runs `pre-commit run --all-files`) will flag
the formatting on your PR instead.

`make build` matters more than it looks: the JS bundles (`widget.js`,
`widget-static.js`) are **gitignored** and built from source — without them,
rendering any widget raises `FileNotFoundError`. Pure-Python work
(data processors, tools, tests) doesn't need them.

Quick sanity check that everything works:

```bash
uv run pytest tests/ -q
uv run python -c "
import retentioneering as rete
print(rete.datasets.load_ecom().describe())
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




## Before submitting a contribution

Please open an issue or discussion before starting substantial work. This helps prevent duplicated effort and ensures that the proposed change fits the project architecture and roadmap.

For small fixes, documentation improvements, tests, and isolated bug fixes, pull requests may be submitted directly.

## Contribution terms

By submitting a contribution to this repository, you certify that:

1. You have the right to submit the contribution.
2. You created the contribution yourself, or you have the legal right to submit it.
3. Your contribution does not knowingly include confidential information, proprietary code, or third-party material that cannot be licensed under Apache-2.0.
4. You agree that your contribution may be distributed as part of Retentioneering Core under the Apache License, Version 2.0.

All commits must include a Signed-off-by line in accordance with the Developer Certificate of Origin.

Example:

Signed-off-by: Your Name [your.email@example.com](mailto:your.email@example.com)

## Copyright

Contributors retain copyright in their original contributions unless they have entered into a separate written agreement with the Retentioneering rights holder.

By submitting a contribution, you grant the project the rights necessary to distribute that contribution as part of Retentioneering Core under Apache-2.0.

## Maintainers

Contributors who demonstrate sustained technical judgment, constructive collaboration, and responsibility for project quality may be invited to become reviewers or maintainers.

Maintainers participate in code review, release planning, architectural discussions, issue triage, and roadmap development. Maintainers may be asked to sign a separate contributor or maintainer agreement before receiving merge permissions.

## Recognition

Meaningful contributions are recognized in release notes, documentation, contributor records, and relevant project materials.

We aim to make Retentioneering a place where contributors can build public technical reputation, gain ownership of meaningful domains, and participate in the evolution of the product analytics ecosystem.



## Questions

Open an [issue](https://github.com/retentioneering/retentioneering-tools/issues),
join the [Discord](https://discord.com/invite/hBnuQABEV2) or
[Telegram](https://t.me/retentioneering_support), or write to
retentioneering@gmail.com.
