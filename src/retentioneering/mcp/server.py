"""
retentioneering MCP server.

Usage in a Jupyter notebook:
    from retentioneering.mcp import serve
    serve(stream)
    serve(stream, context={"description": "...", "events": {...}})
    serve()  # data-agnostic: agent loads data itself via the load_data tool

Transport/protocol wiring only (MCPServer/SSE, `@mcp.tool()` registration,
tracking-context tagging). Every tool is a one-line adapter over a function
in `mcp/tools.py` (the actual report-building logic — plain, transport-
independent `(session, **params) -> dict` functions, reusable outside the SDK,
e.g. by the platform's Anthropic-SDK tool runner). Per-session state lives in
`mcp/_report_session.py` (`ReportSession`), and the system prompt/playbook
text in `mcp/_prompts.py`.
`_apply_preprocessors`/`_find_unlinked_numbers` are re-exported below since
`tests/mcp/server_test.py` imports them from here; their canonical home is
`_agent_logic.py`.
"""

from __future__ import annotations

import json
import os
import socket
import threading
from functools import wraps

from mcp.server.mcpserver import MCPServer

from retentioneering._tracking import caller_context as _tracking_caller_context
from retentioneering._tracking import track as _track
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.mcp import tools
from retentioneering.mcp._agent_logic import (
    _apply_preprocessors,
    _find_unlinked_numbers,
)
from retentioneering.mcp._prompts import _system_instructions
from retentioneering.mcp._report_session import ReportSession

__all__ = ["serve", "_apply_preprocessors", "_find_unlinked_numbers"]


def serve(
    stream: "Eventstream | None" = None,
    context: dict | None = None,
    port: int = 8765,
    assignment=None,
) -> None:
    """
    Start a local MCP server exposing *stream* to Claude (or any MCP client).

    Parameters
    ----------
    stream:
        The prepared Eventstream to analyse. Optional — omit it to start in
        data-agnostic mode: the server has no data until the connected agent
        calls the load_data tool with a CSV path (every other tool errors
        with a clear message until then).
    context:
        Optional semantic layer — descriptions of events, segments, KPIs, etc.
        Example::

            serve(stream, context={
                "description": "E-commerce store. Main KPI — purchase conversion.",
                "events": {"basket": "Added to cart", "purchase": "Completed purchase"},
            })
    port:
        HTTP port for the SSE transport (default 8765).
    assignment:
        Optional experiment assignment table, as for `Eventstream.get_ab_test`
        (a Series path id -> arm, or a DataFrame indexed by path id with column
        `arm` and optionally `assigned_at`). When given, the A/B tools
        (get_sample_ratio, get_ab_test, add_ab_test, get_ab_bootstrap,
        get_ab_transitions, get_ab_steps, get_ab_transitions_power,
        get_ab_steps_power, get_ab_compression) count every assigned path, silent ones included (intention to treat), and `horizon=` /
        since-assignment periods become available to the agent. The table stays
        in this process; describe() reports its size and arms. Example::

            serve(stream, assignment=load_ab_test_assignment())

    Notes
    -----
    **Prompt caching (programmatic agents):** when calling the Anthropic API
    directly, add ``cache_control`` to the system message and tool definitions
    to cache the large static prefix::

        system=[{"type": "text", "text": instructions,
                 "cache_control": {"type": "ephemeral"}}]

    Claude Desktop handles caching automatically.

    Raises
    ------
    OSError
        If *port* is already bound by another process (e.g. another local
        server, or an unrelated app defaulting to the same port).
    """
    _track(
        "mcp_serve", {"has_context": bool(context), "has_stream": stream is not None}
    )
    _check_port_available(port)
    mcp = _build_server(
        stream, context or {}, notebook_dir=os.getcwd(), assignment=assignment
    )
    thread = threading.Thread(
        target=lambda: mcp.run(transport="sse", port=port),
        daemon=True,
    )
    thread.start()
    print(
        f"retentioneering MCP server running on port {port}.\n"
        f"Add to Claude Desktop config:\n"
        f'  "retentioneering": {{"url": "http://localhost:{port}/sse"}}'
    )


def _check_port_available(port: int) -> None:
    """Bind-and-release *port* so a port already held by another process
    (e.g. an unrelated local app defaulting to the same port) raises here,
    in the caller's thread — instead of failing silently inside the daemon
    thread `serve()` starts the actual server on, which would otherwise leave
    the notebook printing a "running" message while no MCP server is
    reachable and every client-side tool call/connection just goes nowhere.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError as e:
        raise OSError(
            f"Port {port} is already in use by another process, so the "
            f"retentioneering MCP server can't start there. Free the port, or "
            f"pass a different one: serve(stream, port=<free_port>) — and update "
            f"the client's MCP config (e.g. Claude Desktop/Code) to match."
        ) from e
    finally:
        sock.close()


# ── server builder ─────────────────────────────────────────────────────────────


def _build_server(
    stream: "Eventstream | None",
    context: dict,
    notebook_dir: str = "",
    assignment=None,
) -> MCPServer:
    mcp = MCPServer(
        "retentioneering",
        instructions=_system_instructions(
            stream, context, notebook_dir=notebook_dir, assignment=assignment
        ),
    )

    session = ReportSession(stream, context, assignment=assignment)

    def _tool():
        """@mcp.tool() that sets caller='mcp' in the tracking context and holds
        `session.lock` for the duration of the call.

        The lock serializes tool calls. mcp 2.x runs synchronous handlers on
        worker threads instead of inline on the event loop, so two calls from
        the same agent can overlap — and every tool here reads or writes the
        one `ReportSession` (active stream, pending tabs), with `export_report`
        check-then-acting on `pending_tabs`. Running them one at a time is what
        1.x did implicitly.
        """

        def decorator(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                with session.lock, _tracking_caller_context("mcp"):
                    return fn(*args, **kwargs)

            return mcp.tool()(wrapper)

        return decorator

    @_tool()
    def load_data(
        path: str, schema: dict | None = None, context: dict | None = None
    ) -> str:
        """
        Load an eventstream from a local CSV file and set it as the base stream
        for this session, replacing whatever stream is currently active (from
        serve() or an earlier load_data call).

        Call this FIRST, before any other tool, if serve() was started without
        a stream (data-agnostic mode) — every other tool errors until this
        succeeds. Also usable later to switch the session to an entirely
        different dataset.

        Parameters
        ----------
        path:
            Path to a CSV file, readable by the process running the MCP server
            (same filesystem as wherever serve() was invoked from).
        schema:
            Optional column mapping:
              {"path_cols": [...], "event_col": "...", "timestamp_col": "...",
               "segment_cols": [...]}
            Defaults to path_cols=["user_id"], event_col="event",
            timestamp_col="timestamp" for any key left unspecified.
        context:
            Optional semantic layer — same shape as serve()'s context argument
            (description, events, kpis). Replaces any context set previously.

        Returns
        -------
        JSON with updated stream stats: n_paths, n_events_total, events list.
        """
        return json.dumps(
            tools.load_data(session, path, schema, context), ensure_ascii=False
        )

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
        return json.dumps(
            tools.update_base_stream(session, preprocessors), ensure_ascii=False
        )

    @_tool()
    def reset_base_stream() -> str:
        """
        Reset the active stream to the original eventstream passed to serve().
        Use this when the user wants to start a new analysis branch from scratch
        or undo all preprocessing.

        Returns updated stream stats.
        """
        return json.dumps(tools.reset_base_stream(session), ensure_ascii=False)

    @_tool()
    def playbook(scenario: str = "") -> str:
        """
        Return the canonical recipe for a named analysis scenario.
        Call this when you recognise a pattern in the user's question but need
        the step-by-step procedure. Do NOT improvise — look it up here first.

        Pass "" (empty) to list all available scenarios.
        """
        return json.dumps(tools.playbook(scenario), ensure_ascii=False)

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
          urls_to_events   sample_paths   split_sessions  compress_events

        Analysis tools (called directly, not as a preprocessor step):
          get_conversion_rate  get_sample_ratio  get_ab_test  get_ab_bootstrap
          get_ab_transitions  get_ab_steps  get_ab_transitions_power
          get_ab_steps_power  get_ab_compression

        Reference topics:
          report_links   (anchor link syntax for analysis text)
          metrics        (the metric registry: names, metric_args, how A/B reads them)
        """
        return json.dumps(tools.describe_tool(tool), ensure_ascii=False)

    @_tool()
    def describe() -> str:
        """Return schema, event list, unique path counts and available segments.
        Reflects the current active stream (after any update_base_stream calls).
        Also segment_levels (arm names, up to 20 per column),
        segments_constant_per_path (the path columns each segment is constant on:
        an arm or a stratum must be), value_columns (numeric columns for
        column_sum / column_mean) and, when serve() got one, assignment (the
        experiment table's size and arms)."""
        return json.dumps(tools.describe(session, session.context), ensure_ascii=False)

    @_tool()
    def add_transition_graph(
        label: str,
        edge_weight: str = "proba_out",
        diff: list | None = None,
        path_col: str | None = None,
        views: list | None = None,
        local_preprocessors: list | None = None,
    ) -> str:
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
        views:
            Optional named visual presets rendered as pills above the graph and
            addressable from the analysis text as [label:view=Name]. Each view is
            {"name": str, "focus"?: {"type": "node", "event": e} |
            {"type": "edge", "source": a, "target": b} |
            {"type": "path", "nodes": [e1, e2, ...]},
            "edgeFilter"?: {"mode": "topk", "k": n} | {"mode": "range", "range": [lo, hi]},
            "hiddenEvents"?: [e, ...]}. Views change only the visual state
            (focus, filters, viewport) — never the computed data. Add one view
            per key claim of your analysis so the reader lands on the exact
            picture the claim describes.

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
        result = tools.add_transition_graph(
            session, label, edge_weight, diff, path_col, views, local_preprocessors
        )
        return json.dumps(result, ensure_ascii=False)

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
            Filter paths, e.g. "add_to_cart->.*->purchase",
            "add_to_cart->.{0,4}->purchase" (at most four events between),
            "(cart->.*){2}->purchase" (a repeated sequence, one block per copy).
        path_col:
            Override the path ID column.

        Returns
        -------
        JSON with tab_id, label, and a summary: top events per step (a `blocks`
        list, one per segment, when path_pattern is set), plus full rows for
        context events.

        local_preprocessors: same as in add_transition_graph.
        """
        result = tools.add_step_matrix(
            session, label, max_steps, diff, path_pattern, path_col, local_preprocessors
        )
        return json.dumps(result, ensure_ascii=False)

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
              {"metric": "event_count", "metric_args": {"event": "purchase"}}
                  — how many times the event occurred (single event only)
              {"metric": "has_event", "metric_args": {"event": "purchase"}}
                  — 0/1 whether the path contains the event (conversion rate)
              {"metric": "event_count_bulk", "metric_args": {"events": ["view", "purchase"]}}
                  — one event_count column per event; omit "events" for every event
              {"metric": "has_event_bulk", "metric_args": {"events": ["view", "purchase"]}}
                  — one has_event column per event; omit "events" for every event
              {"metric": "has_all_events", "metric_args": {"events": ["view", "purchase"]}}
                  — 0/1 whether ALL of the listed events occurred (AND)
              {"metric": "has_any_event", "metric_args": {"events": ["view", "purchase"]}}
                  — 0/1 whether ANY of the listed events occurred (OR)
              {"metric": "time_between",
               "metric_args": {"start_event": "add_to_cart", "end_event": "purchase"}}
                  — seconds between first occurrences of two events
              {"metric": "matches_pattern",
               "metric_args": {"pattern": "add_to_cart->.*->purchase"}}
                  — 0/1 whether path matches the pattern
              {"metric": "in_segment",
               "metric_args": {"segment_name": "channel", "segment_level": "mobile"}}
                  — 0/1 whether the path belongs to that segment level;
                    "mode": "any" (default) / "all" / "event_share" (+ "threshold")
              {"metric": "in_segment_bulk", "metric_args": {"segment_name": "channel"}}
                  — one in_segment column per level of that segment column;
                    omit "segment_name" for every level of every segment column

            Examples:
              Conversion rate to purchase by platform:
                metrics=[
                  {"metric": "has_event",  "metric_args": {"event": "purchase"}, "agg": "mean"},
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
        result = tools.add_segment_overview(
            session, label, segment_col, metrics, path_col, local_preprocessors
        )
        return json.dumps(result, ensure_ascii=False)

    @_tool()
    def add_ab_test(
        label: str,
        diff: list,
        metrics: list,
        strata_col: str | None = None,
        period: str | None = None,
        period_population: str = "itt",
        correction: str = "holm",
        ci_level: float = 0.95,
        path_col: str | None = None,
        strata_pre_exposure: bool | None = None,
        local_preprocessors: list | None = None,
        horizon: str | None = None,
        period_basis: str = "calendar",
        test: str = "auto",
        correction_scope: str = "metric",
    ) -> str:
        """
        Register an A/B test scorecard tab in the pending report.

        The tab is the ab_test widget (table and forest plot of each metric's
        delta with its interval, coloured by verdict; Strata / Periods tabs with
        strata_col / period). Same arguments and numbers as get_ab_test; run
        get_sample_ratio first; use get_ab_test when you need numbers only. An
        A/B analysis that used only numbers-only tools still needs one tab to
        export a report: this is the A/B one.

        Parameters
        ----------
        label:
            Tab label shown in the report. No colons in the label.
        diff, metrics, strata_col, period, period_population, correction,
        ci_level, path_col, strata_pre_exposure, local_preprocessors, horizon,
        period_basis, test, correction_scope:
            As in get_ab_test.

        Returns
        -------
        JSON with tab_id, label, metrics (verdict, delta, delta_ci_low,
        delta_ci_high, p_value_adjusted, mde, support_warning, reading when
        available) and warnings. Link a metric as [label:metric_name].
        """
        result = tools.add_ab_test(
            session,
            label,
            diff,
            metrics,
            strata_col,
            period,
            period_population,
            correction,
            ci_level,
            path_col,
            strata_pre_exposure,
            local_preprocessors,
            horizon,
            period_basis,
            test,
            correction_scope,
        )
        return json.dumps(result, ensure_ascii=False, allow_nan=False)

    @_tool()
    def get_conversion_rate(
        start_anchor: str | dict | list,
        end_anchor: str | dict | list,
        within: int | str | None = None,
        path_col: str | None = None,
        local_preprocessors: list | None = None,
        ci_level: float | None = None,
        diff: list | None = None,
    ) -> str:
        """
        Measure how often one event is followed by another, against its baseline.

        Answers "given that Y happened, how often does X follow — and is that
        different from usual?".

        Returns numbers only — this registers NO tab in the report, unlike the
        add_* tools. Every figure you quote from it is therefore a number you
        computed: wrap it in backticks in the analysis text (`30.0%`, `2.3x`),
        which check_analysis exempts from the anchor-link requirement.

        Prefer this over reading a pair off a transition graph: a graph edge is
        a share of transitions between adjacent events, this is a share of
        paths where one event was followed by another at any distance.

        Parameters
        ----------
        start_anchor:
            The condition. An event name, or an anchor spec
            {"pattern", "at", "occurrence", "offset"} — the same specs
            truncate_paths takes. A LIST means several separate questions, one
            row each, NOT one anchor assembled from several parts.
        end_anchor:
            The target(s) looked for after it, same forms. "path_start" /
            "path_end" are ordinary names: end_anchor="path_end" with within=1
            is an exit rate.
        within:
            Window measured from the start anchor, far edge included. An int
            counts events, a string counts time ("30m"). Omit for "any time
            later in the path".
        path_col:
            Override the path ID column. Pass "session_id" (if describe()
            lists it) when the question is about a visit, not a person.
        local_preprocessors:
            One-off preprocessing for this measurement only. Same format as
            update_base_stream.
        ci_level:
            e.g. 0.95: adds conversion_rate_ci_low / _ci_high (Wilson).
        diff:
            ["segment_col", "value1", "value2"]: group1_* / group2_* columns,
            delta (group1 - group2) with delta_ci_low / delta_ci_high and
            p_value. Assumes path_col is the unit that was assigned. The
            denominator is paths with the start anchor: unless it is
            "path_start" the row is conditional=true and the p_value describes
            selected paths, it does not test; use get_ab_test with a
            matches_pattern metric for the effect.

        Returns
        -------
        JSON with path_col, within, and rows — one per (start, end) pair:
        paths_with_start (the denominator), converted, conversion_rate,
        base_rate (share of ALL paths containing the target) and lift.
        ALWAYS report the denominator and the lift next to the rate; lift below
        1 means the start event makes the target LESS likely.
        """
        result = tools.get_conversion_rate(
            session,
            start_anchor,
            end_anchor,
            within,
            path_col,
            local_preprocessors,
            ci_level,
            diff,
        )
        return json.dumps(result, ensure_ascii=False, allow_nan=False)

    @_tool()
    def get_sample_ratio(
        segment_col: str | None = None,
        expected_shares: dict | None = None,
        alpha: float = 0.001,
        path_col: str | None = None,
        strata_col: str | None = None,
        period: str | None = None,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Check an A/B test's arm sizes against the expected split (sample ratio mismatch).

        Run this FIRST on any experiment, before reading a metric: if the arms do
        not have the planned sizes, paths were lost from one arm and every
        comparison is suspect. Numbers only, no report tab: quote in backticks.
        With the session's assignment table (describe()['assignment']) the counts
        are the table's rows, silent users included, and segment_col is optional.

        Parameters
        ----------
        segment_col:
            Segment column holding the arm (see describe()); optional with an
            assignment table.
        expected_shares:
            {arm: share} summing to 1; omit for an equal split.
        alpha:
            Mismatch threshold, default 0.001.
        path_col:
            The randomised unit (default: primary path column).
        strata_col:
            Repeat per stratum: a segment fixed at assignment (signup platform).
        period:
            Repeat per calendar unit of assigned_at (or the first event): "D",
            "W" or "M".
        local_preprocessors:
            One-off preprocessing. Never filter on behaviour before an SRM
            check: on such a subset a share difference is an effect, not a bug.

        Returns
        -------
        JSON with mismatch, p_value, chi2, dof, multi_valued_paths (assignment
        bug when > 0), silent_paths, unassigned_paths, basis, reading (one sentence: expected vs
        observed split, p and the conclusion; quote it), levels (paths, share,
        expected_share per arm) and, with strata_col, strata (per stratum: paths,
        p_value, p_value_adjusted by Holm across strata, mismatch), with period,
        periods (the same per period).
        """
        result = tools.get_sample_ratio(
            session,
            segment_col,
            expected_shares,
            alpha,
            path_col,
            strata_col,
            period,
            local_preprocessors,
        )
        return json.dumps(result, ensure_ascii=False, allow_nan=False)

    @_tool()
    def get_ab_test(
        diff: list,
        metrics: list,
        strata_col: str | None = None,
        period: str | None = None,
        period_population: str = "itt",
        correction: str = "holm",
        ci_level: float = 0.95,
        path_col: str | None = None,
        strata_pre_exposure: bool | None = None,
        local_preprocessors: list | None = None,
        horizon: str | None = None,
        period_basis: str = "calendar",
        test: str = "auto",
        correction_scope: str = "metric",
    ) -> str:
        """
        Compare two arms of an experiment on path metrics: delta, interval, verdict, MDE.

        Run get_sample_ratio first. Numbers only, no report tab: quote in backticks.
        Ask for the decision metric BEFORE this call. "inconclusive" is NOT "no
        effect": quote the row's reading (what the interval allows, what the
        design detects, the paths a new test needs for a smaller effect); never
        propose running this test longer or re-reading it at another horizon
        (peeking): a smaller effect needs a new test sized before it starts. The
        mde is not a bound on the effect. Population (`population`): with the session's assignment table
        (serve(stream, assignment=...)) every assigned user, silent ones included;
        without one the stream paths (users with >= 1 event) by the segment in
        diff, so silent assigned users are not counted. Metrics are not corrected
        against each other (correction_scope="metric"): read the decision metric,
        and each guardrail on its own against a harm margin declared up front.

        Parameters
        ----------
        diff:
            ["segment_col", "group1", "group2"] (three elements); delta = group1 -
            group2. With an assignment table diff[0] may be "arm". The segment must
            be constant per path; path_col must be the randomised unit.
        metrics:
            Registry metric configs (one number per path) with optional "name" and
            "denominator" (ratio of sums, e.g. revenue / orders); arguments under
            "metric_args". describe_tool("metrics") lists the registry.
        strata_col:
            A per-path segment fixed BEFORE exposure: adds the top 3 strata by
            |delta| per metric (strata_selection labels it; adjusted with
            `correction`), stratum_vs_rest_p_value and a heterogeneity p-value;
            strata_pre_exposure / exploratory flag post-exposure strata.
        period:
            "D", "W", "M", "Q" or "7D": adds ITT rows per period.
        period_basis:
            "calendar" (default) or "since_assignment" (days since assigned_at,
            needs the assignment table; period a fixed length such as "7D").
        period_population:
            "itt" (default) or "active" (survivors only; descriptive, no p).
        horizon:
            e.g. "28D": only each user's first 28 days count; users not observed
            that long are left out (needs the assignment table's assigned_at).
            Different horizons answer different questions: choose before reading.
        test:
            "auto" (default), "z_test", "welch_t_test" or "bootstrap".
        correction:
            "holm" (default), "bonferroni", "bh" or "none".
        correction_scope:
            "metric" (default: within one metric's rows) or "all" (every row of
            every metric in one family: a scorecard read without one decision
            metric; never for guardrails, which are read alone).
        ci_level:
            Interval level, default 0.95.
        path_col:
            The randomised unit (default: primary path column).
        strata_pre_exposure:
            true declares the strata fixed before exposure (platform at signup);
            omit to read the recipe.
        local_preprocessors:
            One-off preprocessing. Never filter on post-exposure behaviour first.

        Returns
        -------
        JSON with population, unassigned_paths (stream paths in neither group of
        diff) and metrics: per metric population, group sizes, group1_support /
        group2_support and conditional (a metric defined only for some paths
        compares those paths), values, delta with delta_ci_low / delta_ci_high,
        rel_delta, p_value (raw), p_value_adjusted, mde (effect detected with 80%
        power at mde_alpha), verdict (increase, decrease, inconclusive,
        descriptive, insufficient_data), test, correction_family, mde_alpha,
        support_warning, reading (one sentence per row, from its numbers, naming
        the window it covers; also on strata and periods rows), unit ("pp" or
        "value": the unit reading quotes) and scope (what the row is corrected
        against; strata/period intervals are unadjusted; on every row); warnings;
        strata (top 3), strata_selection, heterogeneity_p_value,
        strata_correction_family, strata_pre_exposure, strata_pre_exposure_source
        and exploratory with strata_col; periods and periods_correction_family
        with period; immature_paths with horizon.
        """
        result = tools.get_ab_test(
            session,
            diff,
            metrics,
            strata_col,
            period,
            period_population,
            correction,
            ci_level,
            path_col,
            strata_pre_exposure,
            local_preprocessors,
            horizon,
            period_basis,
            test,
            correction_scope,
        )
        return json.dumps(result, ensure_ascii=False, allow_nan=False)

    @_tool()
    def get_ab_bootstrap(
        diff: list,
        metrics: list,
        period: str,
        contrast: dict,
        period_basis: str = "calendar",
        horizon: str | None = None,
        strata_col: str | None = None,
        strata_pre_exposure: bool | None = None,
        ci_level: float = 0.95,
        n_bootstrap: int = 1000,
        path_col: str | None = None,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Contrast an A/B effect between periods with a joint bootstrap interval (novelty).

        "Is the early effect larger than the late one?" in one interval: every
        period is evaluated on the same resampled users, both sides on the users
        observed in all contrasted periods. Numbers only, no report tab: quote in
        backticks.

        Parameters
        ----------
        diff, metrics, path_col, local_preprocessors, horizon, strata_col,
        strata_pre_exposure:
            As in get_ab_test.
        period:
            The bins the contrast names: "14D" with period_basis="since_assignment"
            gives "0-13", "14-27", ...; or a calendar unit ("W").
        period_basis:
            "calendar" (default) or "since_assignment" (needs the assignment table).
        contrast:
            {"name": [periods_a, periods_b]}, e.g. {"novelty": [["0-13"], ["28-41"]]}.
        ci_level:
            Level of the percentile interval (default 0.95).
        n_bootstrap:
            Replicates (default 1000, at least 100).

        Returns
        -------
        JSON with population, n_bootstrap, warnings and rows: per metric (and
        stratum) and contrast, group1_value / group2_value (each arm's change
        between the sides), delta with delta_ci_low / delta_ci_high (percentile,
        unadjusted), p_value (bootstrap SE, unadjusted), share_positive (share of
        replicates with delta > 0, not a p-value), valid_replicates and unit.
        """
        result = tools.get_ab_bootstrap(
            session,
            diff,
            metrics,
            period,
            contrast,
            period_basis,
            horizon,
            strata_col,
            strata_pre_exposure,
            ci_level,
            n_bootstrap,
            path_col,
            local_preprocessors,
        )
        return json.dumps(result, ensure_ascii=False, allow_nan=False)

    @_tool()
    def get_ab_transitions(
        diff: list,
        edge_weight: str = "unique_paths",
        edges: list | None = None,
        horizon: str | None = None,
        correction: str = "bh",
        ci_level: float = 0.95,
        path_col: str | None = None,
        top: int = 15,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Screen every transition (edge) of the graph for a difference between two arms.

        An exploratory screen AFTER the scorecard (get_ab_test), never the decision:
        which transitions differ, with per-edge intervals and p-values adjusted over
        the tested edges (Benjamini-Hochberg by default). A significant edge is a
        hypothesis to declare as the metric of a new test (e.g. matches_pattern).
        Numbers only, no report tab: quote in backticks. Population and unit are
        get_ab_test's (the session's assignment table gives intention to treat).

        Parameters
        ----------
        diff, horizon, path_col, local_preprocessors:
            As in get_ab_test.
        edge_weight:
            "unique_paths" (default: share of paths with the transition),
            "avg_per_path" (transitions per path), "proba_out" / "proba_in" /
            "share_of_total" (ratios with post-treatment denominators: conditional,
            they compare transitions, not the arms). "count" and time weights are
            refused.
        edges:
            Optional [[source, target], ...] declared before the test: only these
            edges form the family (a confirmatory one; scope and readings say so).
        correction:
            "bh" (default), "holm", "bonferroni" or "none".
        ci_level:
            Per-edge interval level (default 0.95; not simultaneous).
        top:
            Rows returned, by adjusted p-value then |delta| (default 15).

        Returns
        -------
        JSON with n_rows, n_tested (the correction family), n_increase, n_decrease,
        n_inconclusive, n_untested, group sizes, unassigned_paths, scope, warnings
        and top: source, target, group1_edge_paths, group2_edge_paths, group1_value,
        group2_value, delta, delta_ci_low, delta_ci_high, p_value, p_value_adjusted,
        mde, verdict, support_warning, conditional, reading (quote it) and unit.
        """
        result = tools.get_ab_transitions(
            session,
            diff,
            edge_weight,
            edges,
            horizon,
            correction,
            ci_level,
            path_col,
            top,
            local_preprocessors,
        )
        return json.dumps(result, ensure_ascii=False, allow_nan=False)

    @_tool()
    def get_ab_steps(
        diff: list,
        max_steps: int = 10,
        horizon: str | None = None,
        correction: str = "bh",
        ci_level: float = 0.95,
        path_col: str | None = None,
        top: int = 15,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Screen every step-matrix cell (event at step k) for a difference between arms.

        An exploratory screen AFTER the scorecard (get_ab_test), never the decision:
        the share of each arm's paths whose k-th event is the event (path_end: the
        path is shorter), with per-cell intervals and p-values adjusted over the
        tested cells (Benjamini-Hochberg by default). Numbers only, no report tab.

        Parameters
        ----------
        diff, horizon, correction, ci_level, path_col, top, local_preprocessors:
            As in get_ab_transitions.
        max_steps:
            Steps compared, from 1 (default 10).

        Returns
        -------
        JSON as get_ab_transitions, with event and step per row (shares, in pp).
        """
        result = tools.get_ab_steps(
            session,
            diff,
            max_steps,
            horizon,
            correction,
            ci_level,
            path_col,
            top,
            local_preprocessors,
        )
        return json.dumps(result, ensure_ascii=False, allow_nan=False)

    @_tool()
    def get_ab_transitions_power(
        diff: list,
        edge_weight: str = "unique_paths",
        edges: list | None = None,
        horizon: str | None = None,
        correction: str = "bh",
        path_col: str | None = None,
        scale: list | None = None,
        target_delta: float | None = None,
        top: int = 15,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        How much data the get_ab_transitions screen needs: its edges at other sizes.

        Each edge is its own test: an edge with few supporting paths is not tested,
        and the family is every testable edge. Projects the screen to `scale` times
        the current paths (both arms alike): per scale the testable edges and the
        median mde, per edge the multiple of the paths that makes it testable.
        Sizing, not inference: for a NEW test, or to explain a screen full of
        insufficient_data; never to run this test longer. Numbers only.

        Parameters
        ----------
        diff, edge_weight, edges, horizon, correction, path_col, local_preprocessors:
            As in get_ab_transitions.
        scale:
            Multiples of the current paths (default [0.25, 0.5, 1, 2, 4, 8, 16]).
        target_delta:
            Optional absolute effect (a share: 0.01 is 1 pp): adds the paths per
            arm each edge's mde needs to reach it.
        top:
            Rows returned (default 15): the edges not testable now (or, with
            target_delta, not reaching it), nearest first.

        Returns
        -------
        JSON with n_rows, n_testable, n_never_testable, group sizes, curve (per
        scale: paths per arm, cells, testable, family_size, mde_alpha, mde_median,
        mde_q25, mde_q75, share_reaching_target), level, assumption, scope, listed
        and top (source, target, pooled_support, testable, mde, rel_mde,
        scale_to_test, *_paths_needed, scale_needed), unit, warnings.
        """
        result = tools.get_ab_transitions_power(
            session,
            diff,
            edge_weight,
            edges,
            horizon,
            correction,
            path_col,
            scale,
            target_delta,
            top,
            local_preprocessors,
        )
        return json.dumps(result, ensure_ascii=False, allow_nan=False)

    @_tool()
    def get_ab_steps_power(
        diff: list,
        max_steps: int = 10,
        horizon: str | None = None,
        correction: str = "bh",
        path_col: str | None = None,
        scale: list | None = None,
        target_delta: float | None = None,
        top: int = 15,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        How much data the get_ab_steps screen needs: its cells at other sizes.

        As get_ab_transitions_power, for the step-matrix cells (event at step k).

        Parameters
        ----------
        diff, horizon, correction, path_col, scale, target_delta, top,
        local_preprocessors:
            As in get_ab_transitions_power.
        max_steps:
            Steps compared, from 1 (default 10).

        Returns
        -------
        JSON as get_ab_transitions_power, with event and step per row (shares, pp).
        """
        result = tools.get_ab_steps_power(
            session,
            diff,
            max_steps,
            horizon,
            correction,
            path_col,
            scale,
            target_delta,
            top,
            local_preprocessors,
        )
        return json.dumps(result, ensure_ascii=False, allow_nan=False)

    @_tool()
    def get_ab_compression(
        diff: list,
        screen: str = "transitions",
        edge_weight: str | None = None,
        max_steps: int | None = None,
        edges: list | None = None,
        horizon: str | None = None,
        min_path_share: float = 0.01,
        chain_threshold: float = 0.8,
        synonym_threshold: float = 0.1,
        bridge_threshold: float = 0.9,
        max_events: int | None = None,
        max_group_size: int = 5,
        target: str | float = "untestable_share",
        step: int | None = None,
        keep: list | None = None,
        other: str = "other",
        by: str | None = None,
        by_pre_exposure: bool | None = None,
        correction: str = "bh",
        path_col: str | None = None,
        target_delta: float | None = None,
        top: int = 15,
        local_preprocessors: list | None = None,
    ) -> str:
        """
        Propose a smaller event vocabulary for an A/B screen, chosen without the arms.

        When get_ab_transitions / get_ab_steps leave many rows insufficient_data,
        a smaller vocabulary makes more rows testable on the same data: merge
        chains, relabel rare events to `other` or drop them, merge thin synonyms,
        one step at a time; `target` picks the step. Arm-blind (pooled paths and
        arm sizes only), so the screen on the compressed stream keeps its error
        control. Apply the returned preprocessor in local_preprocessors of the
        screen; fix the plan BEFORE reading that screen.

        Parameters
        ----------
        diff, horizon, correction, path_col, local_preprocessors:
            As in get_ab_test.
        screen:
            "transitions" (default) or "steps".
        edge_weight, edges:
            The transitions screen's; refused with screen="steps".
        max_steps:
            The steps screen's (default 10); refused with "transitions".
        min_path_share, chain_threshold, synonym_threshold, bridge_threshold:
            Rare below this share of paths (0.01); chain when P(next) and P(prev)
            reach 0.8; synonyms within Jensen-Shannon distance 0.1; drop a rare
            event when its neighbours join faithfully (0.9), else relabel it.
        max_events:
            Optional: compress until at most this many events; above it each step
            takes the cheapest merge of two neighbours (a direct transition
            between them carries 30 % of the runs of one); a frequent event is
            never dropped or relabelled, so the path may stop short of the cap.
        max_group_size:
            The most events one merged label may hold (default 5).
        target:
            "untestable_share" (default: the earliest step whose observations in
            cells too thin to test are near their lowest, without artificial
            transitions; a dense screen stays at step 0), "testable", "mde" or a
            number (the first step whose median mde reaches it).
        step:
            Optional: the plan at this step of the path instead.
        keep:
            Events never changed: the treatment's own and the metric's events.
        other:
            Label of relabelled rare events (default "other").
        by, by_pre_exposure:
            One plan per segment fixed before exposure.
        target_delta:
            Optional effect to detect: adds per step median_scale_needed and
            scale_to_test_90 (multiples of the users).
        top:
            Changed events returned per segment (default 15).

        Returns
        -------
        JSON with screen, population, group sizes, target, arm_blind, unit,
        warnings and segments: chosen_step, steps, before / after (vocabulary,
        cells, testable, untestable, untestable_share, median_mde, mass_retained,
        artificial_transitions), reading (quote it), homogeneity, path (to one
        step after the chosen), n_changed, changes (event, action, label, members,
        step, operation, path_share, reason) and preprocessor (compress_events).
        """
        result = tools.get_ab_compression(
            session,
            diff,
            screen,
            edge_weight,
            max_steps,
            edges,
            horizon,
            min_path_share,
            chain_threshold,
            synonym_threshold,
            bridge_threshold,
            max_events,
            max_group_size,
            target,
            step,
            keep,
            other,
            by,
            by_pre_exposure,
            correction,
            path_col,
            target_delta,
            top,
            local_preprocessors,
        )
        return json.dumps(result, ensure_ascii=False, allow_nan=False)

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
        return json.dumps(tools.check_analysis(analysis), ensure_ascii=False)

    @_tool()
    def export_report(
        title: str = "Analysis Report",
        analysis: str | None = None,
        path: str | None = None,
    ) -> str:
        """
        Generate the HTML report with all widgets added via the add_* tools
        (add_transition_graph, add_step_matrix, add_segment_overview, add_ab_test).
        Needs at least one tab: numbers-only tools (get_ab_test, get_sample_ratio,
        get_conversion_rate) register none, so an A/B analysis adds its scorecard
        with add_ab_test. Resets the pending widget list afterwards.

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
                    "error": (
                        "No widgets added. Call an add_* tool first "
                        "(add_transition_graph, add_step_matrix, add_segment_overview, "
                        "or add_ab_test for an A/B analysis whose numbers came from "
                        "numbers-only tools)."
                    )
                }
            )
        return json.dumps(session.export(title, analysis, path))

    return mcp
