# Architecture Decision Records

Durable architectural decisions of retentioneering 5.x, one decision per file,
in [Nygard format](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
(Context → Decision → Consequences). Most were made during the 5.0 rewrite and
recorded retrospectively in July 2026.

These files are the intended deep-context source for both humans and coding
agents. The agent entrypoint file (`AGENTS.md` in the repo root, with
`CLAUDE.md` symlinked to it) carries the day-to-day essentials and points
here; load individual ADRs when a task touches the corresponding area.

| # | Title | One-liner |
|---|---|---|
| [0001](0001-duckdb-backed-eventstream.md) | DuckDB-backed Eventstream | SQL engine over pandas storage replaces the 3.x pandas engine |
| [0002](0002-duckdb-replacement-scan.md) | DuckDB replacement scan idiom | **Superseded by `engine.py`** — historical idiom, kept for context; new code uses `engine.run()` |
| [0003](0003-immutable-eventstream-and-processor-contract.md) | Immutable Eventstream, hub API | every processor returns a new Eventstream; everything is a method on one class |
| [0004](0004-schema-and-grain-neutral-paths.md) | Schema model & grain-neutral paths | `path_col` defines the unit of analysis; "path" is deliberately grain-neutral |
| [0005](0005-anywidget-with-wheel-embedded-js.md) | anywidget + wheel-embedded JS | widget JS is built in CI and shipped inside the wheel; no CDN, no runtime download |
| [0006](0006-widget-headless-twins.md) | Widget / headless `*_data` twins | every widget has a headless twin; data params = twin's signature |
| [0007](0007-single-path-metrics-registry.md) | Single Path Metrics registry | one metric registry feeds clustering, overview, and path filtering |
| [0008](0008-naming-conventions.md) | 5.0 naming conventions | one concept — one name; industry priors; units in names |
| [0009](0009-mcp-server-as-agent-interface.md) | MCP server as first-class agent interface | LLM agents get compact summaries + a validated HTML report workflow |
| [0010](0010-static-html-export.md) | Self-contained static HTML export | exported reports work with no Python kernel behind them |
| [0011](0011-versioning-and-release.md) | Literal versioning, tag-driven release | pyproject version is the source of truth; only `v*` tags publish |
| [0012](0012-features-dropped-from-3x.md) | Features deliberately dropped from 3.x | Preprocessing Graph, Cohorts, StatTests, Sequences — cut, may return |
| [0013](0013-docstring-driven-docs.md) | Docstring-driven documentation | reference docs are rendered from docstrings; docstrings are the source of truth |

## Adding a new ADR

Copy the section skeleton of any existing record, number it sequentially,
add a row to the table above. Supersede rather than edit: if a decision
changes, write a new ADR and mark the old one `Superseded by NNNN`.
