"""MCP agent tools as plain, transport-independent functions.

Each function here is `(session, **params) -> dict` — no FastMCP, no JSON
serialization, no closure over server-lifetime state. `server.py` wraps each
one as a thin `@mcp.tool()` adapter (`json.dumps(tools.foo(session, ...))`)
for the notebook flow; any other caller (e.g. the platform's chat assistant,
via `client.beta.messages.tool_runner`) can call these directly against its
own `ReportSession` (or a subclass overriding `update_base_stream`/
`reset_base_stream` to fork/reset a persisted DAG instead of an in-memory
stream — see the platform's `PlatformAgentSession`). These functions only
touch `session`'s public protocol (`active_stream`, `context_events`,
`update_base_stream()`, `reset_base_stream()`, `add_tab()`, `pending_tabs`,
`package()`), so any object shaped like `ReportSession` works.

Docstrings here are the ones FastMCP quotes as tool descriptions for the
notebook/MCP-client flow (see ADR-0009) — the platform's own `@beta_tool`
adapters in `apps/api/app/assistant.py` write their own Google-style
docstrings rather than reusing these numpy-style ones verbatim.
"""

from __future__ import annotations

import inspect
import json
from typing import Any

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.mcp._agent_logic import (
    _apply_preprocessors,
    _find_unlinked_numbers,
    _segment_overview_summary,
    _step_matrix_summary,
    _transition_graph_summary,
)
from retentioneering.mcp._prompts import (
    _PLAYBOOK,
    _STATIC_TOOL_DOCS,
    _playbook_index,
    _tool_docs_index,
)
from retentioneering.mcp._report_session import ReportSession


def _require_stream(session: Any) -> dict | None:
    """Guard for tools that operate on the active stream. Returns an error
    dict if serve() was started without a stream and load_data() hasn't been
    called yet; None if a stream is present and the caller can proceed.
    """
    if session.active_stream is None:
        return {
            "error": (
                "No eventstream loaded yet. Call load_data(path=..., schema=...) "
                "first to load a CSV file into this session."
            )
        }
    return None


def load_data(
    session: Any,
    path: str,
    schema: dict | None = None,
    context: dict | None = None,
) -> dict:
    """
    Load an eventstream from a local CSV file and set it as the base stream
    for this session, replacing whatever stream is currently active (from
    serve() or an earlier load_data call).

    Call this FIRST, before any other tool, if serve() was started without a
    stream (data-agnostic mode) — every other tool returns an error until
    this succeeds. Also usable later to switch the session to an entirely
    different dataset.

    Parameters
    ----------
    path:
        Path to a CSV file, readable by the process running the MCP server
        (same filesystem as wherever serve() was invoked from).
    schema:
        Optional column mapping:
          {"path_cols": [...], "event_cols": [...], "timestamp_col": "...",
           "segment_cols": [...]}
        Defaults to path_cols=["user_id"], event_cols=["event"],
        timestamp_col="timestamp" for any key left unspecified.
    context:
        Optional semantic layer — same shape as serve()'s context argument
        (description, events, kpis). Replaces any context set previously.

    Returns
    -------
    Stream stats: n_paths, n_events_total, events list.
    {"error": ...} if the file can't be read or doesn't match the schema.
    """
    try:
        stream = Eventstream(path, schema=schema)
    except Exception as exc:
        return {"error": str(exc)}
    session.load_data(stream, context)
    return {"status": "data loaded", **ReportSession.stream_stats(stream)}


def update_base_stream(session: Any, preprocessors: list) -> dict:
    """
    Apply preprocessing to the original eventstream and set it as the active
    base stream for the session. All subsequent add_* calls will use this stream.

    Call this only when the user explicitly wants to change the analysis baseline
    (e.g. "filter short paths and analyse"). Always confirm with the user before
    calling — it replaces the current context.

    Always applies to the ORIGINAL stream from serve(), not the current active one,
    so repeated calls never stack filters accidentally.

    Parameters
    ----------
    preprocessors:
        Ordered list of steps. Each step is {"type": "<name>", ...args}.
        Available types: collapse_events, filter_paths, filter_events, truncate_paths,
        rename_events, edit_events, drop_events, add_events, to_daily_states,
        add_segment, drop_segment, add_clusters, urls_to_events, sample_paths,
        split_sessions.
        Call describe_tool("<type>") for full parameter reference before using any step.

    Returns
    -------
    Stream stats: n_paths, n_events_total, events list.
    """
    if (err := _require_stream(session)) is not None:
        return err
    try:
        new_stream = session.update_base_stream(preprocessors)
    except Exception as exc:
        return {"error": str(exc)}
    return {"status": "base stream updated", **ReportSession.stream_stats(new_stream)}


def reset_base_stream(session: Any) -> dict:
    """
    Reset the active stream to the original eventstream passed to serve().
    Use this when the user wants to start a new analysis branch from scratch
    or undo all preprocessing.

    Returns updated stream stats.
    """
    if (err := _require_stream(session)) is not None:
        return err
    new_stream = session.reset_base_stream()
    return {
        "status": "base stream reset to original",
        **ReportSession.stream_stats(new_stream),
    }


def playbook(scenario: str = "") -> dict:
    """
    Return the canonical recipe for a named analysis scenario.
    Call this when you recognise a pattern in the user's question but need
    the step-by-step procedure. Do NOT improvise — look it up here first.

    Pass "" (empty) to list all available scenarios.
    """
    return _PLAYBOOK.get(scenario.strip(), _playbook_index())


def describe_tool(tool: str = "") -> dict:
    """
    Return full parameter documentation for a preprocessor type or internal tool.
    Call this before using any preprocessor you are not 100% sure about —
    do NOT guess parameters or read source files.

    Pass "" (empty) to list all documented tools.

    Preprocessors (use as {"type": "<name>", ...} in update_base_stream / local_preprocessors):
      collapse_events  filter_paths   filter_events   truncate_paths
      rename_events    edit_events    drop_events     add_events
      to_daily_states  add_segment    drop_segment    add_clusters
      urls_to_events   sample_paths   split_sessions

    Reference topics:
      report_links   (anchor link syntax for analysis text)
    """
    t = tool.strip()
    if not t:
        return _tool_docs_index()
    if t in _STATIC_TOOL_DOCS:
        return {"topic": t, "docs": _STATIC_TOOL_DOCS[t]}
    method = getattr(Eventstream, t, None)
    doc = inspect.getdoc(method) if method and callable(method) else None
    if doc:
        return {"preprocessor": t, "docs": doc}
    return {"error": f"Unknown tool {t!r}.", **_tool_docs_index()}


def describe(session: Any, context: dict) -> dict:
    """Return schema, event list, unique path counts and available segments.
    Reflects the current active stream (after any update_base_stream calls)."""
    if (err := _require_stream(session)) is not None:
        return err
    cur = session.active_stream
    s = cur.schema
    ec, pc = s.event_col, s.path_col
    df = cur.df
    ts_col = s.timestamp_col
    result = {
        "n_paths": int(df[pc].nunique()),
        "n_events_total": len(df),
        "event_col": ec,
        "path_col": pc,
        "path_cols": s.path_cols,
        "segment_cols": s.segment_cols,
        "timestamp_col": ts_col,
        "date_range": {
            "min": str(df[ts_col].min())[:10],
            "max": str(df[ts_col].max())[:10],
        },
        "events": sorted(df[ec].astype(str).unique().tolist()),
    }
    if context.get("events"):
        result["event_descriptions"] = context["events"]
    if context.get("segments"):
        result["segment_descriptions"] = context["segments"]
    return result


def add_transition_graph(
    session: Any,
    label: str,
    edge_weight: str = "proba_out",
    diff: list | None = None,
    path_col: str | None = None,
    local_preprocessors: list | None = None,
) -> dict:
    """
    Compute a transition graph and register it as a tab in the pending report.
    Returns the transition matrix so you can analyse it before calling export_report().

    Call this (possibly multiple times with different parameters) before
    calling export_report().

    Parameters
    ----------
    label:
        Tab label shown in the report. Use a short descriptive name,
        e.g. "Overall Flow" or "Mobile vs Desktop". No colons in the label.
    edge_weight:
        How to weight edges. One of: proba_out, proba_in, count, unique_paths,
        share_of_total, avg_per_path, time_median, time_q95.
    diff:
        Optional diff: [segment_col, value1, value2].
    path_col:
        Override the path ID column.

    Returns
    -------
    tab_id, events list, values matrix (and group1/group2 in diff mode).
    Use tab_id to reference this tab in the analysis text as [label:event].

    local_preprocessors:
        Optional one-off preprocessing applied on top of the base stream for
        this visualisation only. Same format as update_base_stream preprocessors.
        Use sparingly — prefer update_base_stream when the same preprocessing
        applies to multiple visualisations.
    """
    if (err := _require_stream(session)) is not None:
        return err
    src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    widget = src.transition_graph(
        edge_weight=edge_weight, diff=diff, path_col=path_col or None
    )
    if widget.error:
        return {"error": widget.error, "label": label}
    data = {
        "widget_type": "transition_graph",
        "result": json.loads(widget.result or "{}"),
        "edge_weight": widget.edge_weight,
        "diff": json.loads(widget.diff) if widget.diff else None,
        "event_counts": json.loads(widget.event_counts or "{}"),
        "event_counts_g1": json.loads(widget.event_counts_g1 or "{}"),
        "event_counts_g2": json.loads(widget.event_counts_g2 or "{}"),
        "node_positions": {},
        "event_visibility": {},
        "segment_levels": json.loads(widget.segment_levels or "{}"),
        "path_cols": json.loads(widget.path_cols or "[]"),
        "path_col": widget.path_col or "",
        "height": widget.height,
        "sidebar_open": False,
    }
    tab_id = session.add_tab(label, data, local_preprocessors or [])

    result_raw = json.loads(widget.result or "{}")
    summary = _transition_graph_summary(result_raw, session.context_events, edge_weight)
    summary["tab_id"] = tab_id
    summary["label"] = label
    return summary


def add_step_matrix(
    session: Any,
    label: str,
    max_steps: int = 10,
    diff: list | None = None,
    path_pattern: str | None = None,
    path_col: str | None = None,
    local_preprocessors: list | None = None,
) -> dict:
    """
    Compute a step matrix and register it as a tab in the pending report.
    Returns the matrix data so you can analyse it before calling export_report().

    Call this (possibly multiple times) before calling export_report().

    Parameters
    ----------
    label:
        Tab label shown in the report. No colons in the label.
    max_steps:
        Maximum steps before/after anchor.
    diff:
        Optional diff: [segment_col, value1, value2].
    path_pattern:
        Filter paths, e.g. "add_to_cart->.*->purchase".
    path_col:
        Override the path ID column.

    Returns
    -------
    tab_id, matrices list, event_counts.

    local_preprocessors: same as in add_transition_graph.
    """
    if (err := _require_stream(session)) is not None:
        return err
    src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    widget = src.step_matrix(
        max_steps=max_steps,
        diff=diff,
        path_col=path_col or None,
        path_pattern=path_pattern or None,
    )
    if widget.error:
        return {"error": widget.error, "label": label}
    data = {
        "widget_type": "step_matrix",
        "result": json.loads(widget.result or "{}"),
        "max_steps": widget.max_steps,
        "diff": json.loads(widget.diff) if widget.diff else None,
        "path_col": widget.path_col or "",
        "path_pattern": widget.path_pattern or "",
        "path_cols": json.loads(widget.path_cols or "[]"),
        "segment_levels": json.loads(widget.segment_levels or "{}"),
        "event_list": json.loads(widget.event_list or "[]"),
        "height": widget.height,
        "sidebar_open": False,
    }
    tab_id = session.add_tab(label, data, local_preprocessors or [])

    result_raw = json.loads(widget.result or "{}")
    summary = _step_matrix_summary(result_raw, session.context_events)
    summary["tab_id"] = tab_id
    summary["label"] = label
    return summary


def add_segment_overview(
    session: Any,
    label: str,
    segment_col: str,
    metrics: list | None = None,
    path_col: str | None = None,
    local_preprocessors: list | None = None,
) -> dict:
    """
    Compute a segment overview and register it as a tab in the pending report.
    Returns the overview table so you can analyse it before calling export_report().

    Call this (possibly multiple times with different segment columns) before
    calling export_report().

    Parameters
    ----------
    label:
        Tab label shown in the report. No colons in the label.
    segment_col:
        Column to segment by (must be listed in segment_cols from describe()).
    metrics:
        List of additional metric dicts. segment_size and segment_share are
        always computed automatically and do not need to be specified.

        Each dict: {"metric": <name>, "metric_args": {...}, "agg": <agg>}
        "agg" choices: "mean" (default), "median", "complement_distance",
                       "q5", "q25", "q75", "q95"

        Available metrics and their metric_args:
          {"metric": "length"}
              — number of events per path
          {"metric": "duration"}
              — duration in seconds (first to last event)
          {"metric": "event_count", "metric_args": {"events": "purchase"}}
              — how many times the event occurred; events can also be a list
          {"metric": "has_event", "metric_args": {"events": "purchase"}}
              — 0/1 whether the path contains the event (conversion rate)
          {"metric": "time_between",
           "metric_args": {"start_event": "add_to_cart", "end_event": "purchase"}}
              — seconds between first occurrences of two events
          {"metric": "matches_pattern",
           "metric_args": {"pattern": "add_to_cart->.*->purchase"}}
              — 0/1 whether path matches the pattern

        Examples:
          Conversion rate to purchase by platform:
            metrics=[
              {"metric": "has_event",  "metric_args": {"events": "purchase"}, "agg": "mean"},
              {"metric": "length"},
              {"metric": "duration", "agg": "median"},
            ]
          Time-to-purchase by acquisition channel:
            metrics=[
              {"metric": "time_between",
               "metric_args": {"start_event": "home", "end_event": "purchase"},
               "agg": "median"},
            ]
    path_col:
        Override the path ID column.

    Returns
    -------
    tab_id, metrics list, segments list, values matrix.

    local_preprocessors: same as in add_transition_graph.
    """
    if (err := _require_stream(session)) is not None:
        return err
    src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
    widget = src.segment_overview(
        segment_col=segment_col,
        metrics=metrics or [],
        path_col=path_col or None,
    )
    if widget.error:
        return {"error": widget.error, "label": label}
    data = {
        "widget_type": "segment_overview",
        "result": json.loads(widget.result or "{}"),
        "segment_col": widget.segment_col or "",
        "path_col": widget.path_col or "",
        "metrics": json.loads(widget.metrics or "[]"),
        "segment_cols": json.loads(widget.segment_cols or "[]"),
        "segment_levels": json.loads(widget.segment_levels or "{}"),
        "path_cols": json.loads(widget.path_cols or "[]"),
        "event_list": json.loads(widget.event_list or "[]"),
        "height": widget.height,
        "sidebar_open": False,
    }
    tab_id = session.add_tab(label, data, local_preprocessors or [])

    result_raw = json.loads(widget.result or "{}")
    summary = _segment_overview_summary(result_raw, session.context_events)
    summary["tab_id"] = tab_id
    summary["label"] = label
    return summary


def check_analysis(analysis: str) -> dict:
    """
    Validate analysis text BEFORE calling export_report.

    Scans for percentages (%) and multipliers (×) that have no [tab:ref] anchor
    link within 200 characters. Checks per number, not per line — a line with
    one link and one unlinked % will still be flagged for that %.
    Numbers in backtick spans `2.2×` are exempt (agent-computed values).

    Workflow (mandatory):
      1. Write your full analysis text.
      2. Call check_analysis(analysis).
      3. If status == "needs_fixes": fix EVERY listed line, then call check_analysis again.
      4. Only call export_report when check_analysis returns {"status": "ok"}.

    Returns
    -------
    {"status": "ok"}  — ready to export.
    {"status": "needs_fixes", "issues": [...]}  — lines to fix before exporting.
    """
    issues = _find_unlinked_numbers(analysis)
    if not issues:
        return {
            "status": "ok",
            "message": "All numbers have links. Proceed with export_report.",
        }
    return {
        "status": "needs_fixes",
        "count": len(issues),
        "issues": issues,
        "instruction": (
            "Add a [tab_label:element] anchor link to each listed line. "
            "For transitions use edge links [tab:src->tgt]. "
            "Then call check_analysis again until status is 'ok'."
        ),
    }


def export_report(
    session: Any,
    title: str = "Analysis Report",
    analysis: str | None = None,
) -> dict:
    """
    Finalize the report with all widgets added via add_transition_graph /
    add_step_matrix / add_segment_overview. Resets the pending widget list
    afterwards.

    Parameters
    ----------
    title:
        Report title.
    analysis:
        Full written analysis in markdown. Reference specific tabs and events
        with [tab_label:event_name] — clicking the link will activate that tab
        and focus the event. Use [event_name] (no label prefix) to focus in
        whichever tab is currently active.
        Example:
          "Drop-off at [Overall Flow:basket] is 45%. Timing shows
           [Timing:basket] takes 2.3 min on average. In the funnel
           [Purchase Funnel:payment_details] is the main bottleneck."
        Supports markdown: # headings, **bold**, *italic*, tables, - lists.
    """
    if not session.pending_tabs:
        return {
            "error": "No widgets added. Call add_transition_graph or add_step_matrix first."
        }
    return session.package(title, analysis)
