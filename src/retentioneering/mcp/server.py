"""
retentioneering MCP server.

Usage in a Jupyter notebook:
    from retentioneering.mcp import serve
    serve(stream)
    serve(stream, context={"description": "...", "events": {...}})

Transport/protocol wiring only (FastMCP/SSE, `@mcp.tool()` registration,
tracking-context tagging) — the report-building logic lives in
`_agent_logic.py`, per-session state in `_report_session.py`
(`ReportSession`), and the system prompt/playbook text in `_prompts.py`.
`_apply_preprocessors`/`_find_unlinked_numbers` are re-exported below since
`tests/mcp/server_test.py` imports them from here; their canonical home is
`_agent_logic.py`.
"""

from __future__ import annotations

import inspect
import json
import os
import threading
from functools import wraps

from mcp.server.fastmcp import FastMCP

from retentioneering._tracking import _caller_type as _tracking_caller_type
from retentioneering._tracking import track as _track
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
    _system_instructions,
    _tool_docs_index,
)
from retentioneering.mcp._report_session import ReportSession

__all__ = ["serve", "_apply_preprocessors", "_find_unlinked_numbers"]


def serve(
    stream: "Eventstream",
    context: dict | None = None,
    port: int = 8765,
) -> None:
    """
    Start a local MCP server exposing *stream* to Claude (or any MCP client).

    Parameters
    ----------
    stream:
        The prepared Eventstream to analyse.
    context:
        Optional semantic layer — descriptions of events, segments, KPIs, etc.
        Example::

            serve(stream, context={
                "description": "E-commerce store. Main KPI — purchase conversion.",
                "events": {"basket": "Added to cart", "purchase": "Completed purchase"},
            })
    port:
        HTTP port for the SSE transport (default 8765).

    Notes
    -----
    **Prompt caching (programmatic agents):** when calling the Anthropic API
    directly, add ``cache_control`` to the system message and tool definitions
    to cache the large static prefix::

        system=[{"type": "text", "text": instructions,
                 "cache_control": {"type": "ephemeral"}}]

    Claude Desktop handles caching automatically.
    """
    _track("mcp_serve", {"has_context": bool(context)})
    mcp = _build_server(stream, context or {}, port=port, notebook_dir=os.getcwd())
    thread = threading.Thread(
        target=lambda: mcp.run(transport="sse"),
        daemon=True,
    )
    thread.start()
    print(
        f"retentioneering MCP server running on port {port}.\n"
        f"Add to Claude Desktop config:\n"
        f'  "retentioneering": {{"url": "http://localhost:{port}/sse"}}'
    )


# ── server builder ─────────────────────────────────────────────────────────────


def _build_server(
    stream: "Eventstream",
    context: dict,
    port: int = 8765,
    notebook_dir: str = "",
) -> FastMCP:
    mcp = FastMCP(
        "retentioneering",
        instructions=_system_instructions(stream, context, notebook_dir=notebook_dir),
        port=port,
    )

    def _tool():
        """@mcp.tool() that sets caller='mcp' in the tracking context."""

        def decorator(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                token = _tracking_caller_type.set("mcp")
                try:
                    return fn(*args, **kwargs)
                finally:
                    _tracking_caller_type.reset(token)

            return mcp.tool()(wrapper)

        return decorator

    session = ReportSession(stream, context)

    @_tool()
    def update_base_stream(preprocessors: list) -> str:
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
        JSON with updated stream stats: n_paths, n_events_total, events list.
        """
        try:
            new_stream = session.update_base_stream(preprocessors)
        except Exception as exc:
            return json.dumps({"error": str(exc)})
        return json.dumps(
            {"status": "base stream updated", **ReportSession.stream_stats(new_stream)},
            ensure_ascii=False,
        )

    @_tool()
    def reset_base_stream() -> str:
        """
        Reset the active stream to the original eventstream passed to serve().
        Use this when the user wants to start a new analysis branch from scratch
        or undo all preprocessing.

        Returns updated stream stats.
        """
        new_stream = session.reset_base_stream()
        return json.dumps(
            {
                "status": "base stream reset to original",
                **ReportSession.stream_stats(new_stream),
            },
            ensure_ascii=False,
        )

    @_tool()
    def playbook(scenario: str = "") -> str:
        """
        Return the canonical recipe for a named analysis scenario.
        Call this when you recognise a pattern in the user's question but need
        the step-by-step procedure. Do NOT improvise — look it up here first.

        Pass "" (empty) to list all available scenarios.
        """
        return json.dumps(
            _PLAYBOOK.get(scenario.strip(), _playbook_index()), ensure_ascii=False
        )

    @_tool()
    def describe_tool(tool: str = "") -> str:
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
            return json.dumps(_tool_docs_index(), ensure_ascii=False)
        if t in _STATIC_TOOL_DOCS:
            return json.dumps(
                {"topic": t, "docs": _STATIC_TOOL_DOCS[t]}, ensure_ascii=False
            )
        method = getattr(Eventstream, t, None)
        doc = inspect.getdoc(method) if method and callable(method) else None
        if doc:
            return json.dumps({"preprocessor": t, "docs": doc}, ensure_ascii=False)
        return json.dumps(
            {"error": f"Unknown tool {t!r}.", **_tool_docs_index()}, ensure_ascii=False
        )

    @_tool()
    def describe() -> str:
        """Return schema, event list, unique path counts and available segments.
        Reflects the current active stream (after any update_base_stream calls)."""
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
        return json.dumps(result, ensure_ascii=False)

    @_tool()
    def add_transition_graph(
        label: str,
        edge_weight: str = "proba_out",
        diff: list | None = None,
        path_col: str | None = None,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Compute a transition graph and register it as a tab in the pending report.
        Returns the transition matrix so you can analyse it before writing the report.

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
        JSON with tab_id, events list, values matrix (and group1/group2 in diff mode).
        Use tab_id to reference this tab in the analysis text as [label:event].

        local_preprocessors:
            Optional one-off preprocessing applied on top of the base stream for
            this visualisation only. Same format as update_base_stream preprocessors.
            Use sparingly — prefer update_base_stream when the same preprocessing
            applies to multiple visualisations.
        """
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
        widget = src.transition_graph(
            edge_weight=edge_weight, diff=diff, path_col=path_col or None
        )
        if widget.error:
            return json.dumps({"error": widget.error, "label": label})
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
        summary = _transition_graph_summary(
            result_raw, session.context_events, edge_weight
        )
        summary["tab_id"] = tab_id
        summary["label"] = label
        return json.dumps(summary, ensure_ascii=False)

    @_tool()
    def add_step_matrix(
        label: str,
        max_steps: int = 10,
        diff: list | None = None,
        path_pattern: str | None = None,
        path_col: str | None = None,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Compute a step matrix and register it as a tab in the pending report.
        Returns the matrix data so you can analyse it before writing the report.

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
        JSON with tab_id, matrices list, event_counts.

        local_preprocessors: same as in add_transition_graph.
        """
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
        widget = src.step_matrix(
            max_steps=max_steps,
            diff=diff,
            path_col=path_col or None,
            path_pattern=path_pattern or None,
        )
        if widget.error:
            return json.dumps({"error": widget.error, "label": label})
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
        return json.dumps(summary, ensure_ascii=False)

    @_tool()
    def add_segment_overview(
        label: str,
        segment_col: str,
        metrics: list | None = None,
        path_col: str | None = None,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Compute a segment overview and register it as a tab in the pending report.
        Returns the overview table so you can analyse it before writing the report.

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
        JSON with tab_id, metrics list, segments list, values matrix.

        local_preprocessors: same as in add_transition_graph.
        """
        src = _apply_preprocessors(session.active_stream, local_preprocessors or [])
        widget = src.segment_overview(
            segment_col=segment_col,
            metrics=metrics or [],
            path_col=path_col or None,
        )
        if widget.error:
            return json.dumps({"error": widget.error, "label": label})
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
        return json.dumps(summary, ensure_ascii=False)

    @_tool()
    def check_analysis(analysis: str) -> str:
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
            return json.dumps(
                {
                    "status": "ok",
                    "message": "All numbers have links. Proceed with export_report.",
                },
                ensure_ascii=False,
            )
        return json.dumps(
            {
                "status": "needs_fixes",
                "count": len(issues),
                "issues": issues,
                "instruction": (
                    "Add a [tab_label:element] anchor link to each listed line. "
                    "For transitions use edge links [tab:src->tgt]. "
                    "Then call check_analysis again until status is 'ok'."
                ),
            },
            ensure_ascii=False,
        )

    @_tool()
    def export_report(
        title: str = "Analysis Report",
        analysis: str | None = None,
        path: str | None = None,
    ) -> str:
        """
        Generate the HTML report with all widgets added via add_transition_graph /
        add_step_matrix. Resets the pending widget list afterwards.

        Parameters
        ----------
        title:
            Report title shown in the browser tab and as the heading.
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
        path:
            Destination file path. If None, a temp file is created.
        """
        if not session.pending_tabs:
            return json.dumps(
                {
                    "error": "No widgets added. Call add_transition_graph or add_step_matrix first."
                }
            )
        return json.dumps(session.export(title, analysis, path))

    return mcp
