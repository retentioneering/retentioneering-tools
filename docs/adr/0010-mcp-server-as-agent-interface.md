# ADR-0010: MCP server as a first-class agent interface

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
  `export_report()` producing a static HTML report (ADR-0011).
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
  affect agent behavior (see ADR-0008, ADR-0014).
- Any Eventstream API rename must be propagated to `mcp/server.py`
  (`_apply_preprocessors`, tool docstrings, system instructions) and
  `mcp/playbook.md` in the same change.
