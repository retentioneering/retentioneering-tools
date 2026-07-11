# ADR-0009: MCP server as a first-class agent interface

Status: Accepted (5.0; recorded 2026-07)

## Context

LLM agents are a primary consumer of the library alongside humans. Handing an
agent raw DataFrames wastes context and produces unverifiable analysis
("38% drop-off" with no source).

## Decision

- `retentioneering.mcp.serve(stream, context=...)` starts a local MCP server
  (SSE) exposing the eventstream to any MCP client.
- The tool design follows a **report-builder workflow**: `describe()` →
  optional `update_base_stream(preprocessors)` (session baseline; the
  original stream is never mutated) → `add_transition_graph` /
  `add_step_matrix` / `add_segment_overview` (each returns a *compact
  summary* for reasoning and registers a tab) → `check_analysis(text)` →
  `export_report()` producing a static HTML report (ADR-0010).
- Tool results are compact summaries (top-N edges, per-step tops, spread-
  sorted tables), never full matrices — the full interactive visualisation
  lives in the report.
- Every number in the analysis text must carry a `[tab:element]` anchor link
  to its source; `check_analysis` enforces this mechanically before export.
- Preprocessor steps are `{"type": "<eventstream_method>", ...kwargs}` and
  mirror the Python API names exactly; `describe_tool()` serves the method
  docstrings, and `playbook.md` holds canonical multi-step recipes
  (temporal anomaly, conversion drop-off, ...).

## Consequences

- The Python API's docstrings and naming are agent-facing surfaces: MCP tool
  docs quote them, so docstring quality and enum completeness directly
  affect agent behavior (see ADR-0008, ADR-0013).
- Any Eventstream API rename must be propagated to `mcp/server.py`
  (tool docstrings, system instructions), `mcp/_agent_logic.py`
  (`_apply_preprocessors`), and `mcp/playbook.md` in the same change.
- `mcp/server.py` is transport/protocol wiring only (FastMCP/SSE, `@mcp.tool()`
  registration). Every tool body lives in `mcp/tools.py` as a plain
  `(session, **params) -> dict` function — `server.py`'s `@mcp.tool()`
  closures are one-line adapters (`json.dumps(tools.foo(session, ...))`).
  These functions only touch `session`'s public protocol (`active_stream`,
  `update_base_stream()`, `reset_base_stream()`, `add_tab()`, `pending_tabs`,
  `context_events`, `package()`), so they work unchanged against any
  `ReportSession`-shaped object — this is what lets the platform's chat
  assistant reuse them directly via `client.beta.messages.tool_runner`,
  against a `PlatformAgentSession` subclass that overrides only
  `update_base_stream`/`reset_base_stream` to fork/reset a persisted DAG node
  instead of replaying an in-memory stream.
- The numeric/text logic behind those tools (`_apply_preprocessors`, summary
  builders, `_find_unlinked_numbers`) lives in `mcp/_agent_logic.py`;
  per-session state in `mcp/_report_session.py`'s `ReportSession` (active
  stream, pending tabs — what used to be closure variables rebound by hand);
  the system prompt/playbook text in `mcp/_prompts.py`.
- `ReportSession.export(title, analysis, path)` (notebook-only: packages tabs
  *and* writes a static HTML file) is split from `ReportSession.package(title,
  analysis)` (pure — returns `{title, analysis, tabs}`, no file I/O).
  `tools.export_report()` calls `package()`; `server.py`'s own `export_report`
  tool wrapper calls `export()` (to keep writing the notebook's HTML file).
  A non-notebook caller — the platform, which renders tabs inline via
  `staticHost`/`renderStatic` instead of a static export — has no destination
  to write a file to, so it only ever needs `package()`.
- `mcp/_prompts.py::_static_instructions()` (public alias:
  `retentioneering.mcp.static_instructions`) holds the transport- and
  stream-independent portion of the system prompt (workflow, analysis-link
  syntax, canonical patterns) — safe to reuse as a prompt-caching prefix.
  `_system_instructions()` (notebook-only) prepends a small per-stream stats
  header that is *not* cacheable, which is why the platform skips it
  entirely and relies on the agent's own first workflow step (`describe()`)
  to learn the current data shape instead.
