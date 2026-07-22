"""Static text assembly for the MCP agent: the system prompt, the playbook
(canonical analysis recipes, loaded from `playbook.md`), and the
`describe_tool`/`playbook` tool reference indexes. Pure string/markdown
building — no numeric logic (see `_agent_logic.py` for that) and no FastMCP/
transport concerns (see `server.py`).
"""

from __future__ import annotations

import inspect
import pathlib

from retentioneering.eventstream.eventstream import Eventstream


def _load_playbook() -> dict[str, str]:
    """Parse playbook.md into {scenario_key: content_string}."""
    md = (pathlib.Path(__file__).parent / "playbook.md").read_text(encoding="utf-8")
    sections: dict[str, str] = {}
    current_key: str | None = None
    buf: list[str] = []
    for line in md.splitlines():
        if line.startswith("## "):
            if current_key:
                sections[current_key] = "\n".join(buf).strip()
            current_key = line[3:].strip()
            buf = []
        elif not line.startswith("<!--"):
            buf.append(line)
    if current_key:
        sections[current_key] = "\n".join(buf).strip()
    return sections


_PLAYBOOK: dict[str, str] = _load_playbook()

# Static reference topics that don't map to an Eventstream method
_STATIC_TOOL_DOCS: dict[str, str] = {
    "report_links": """\
Anchor link syntax for the analysis text passed to export_report().

  [Tab:event]           open tab, focus node in transition graph
  [Tab:src->tgt]        open tab, focus edge (dims the rest, fits the pair)
  [Tab:view=Name]       open tab, apply a named view (visual preset passed
                        via add_transition_graph(views=[...]))
  [Tab:event@step]      open tab, scroll to step-matrix cell; step_window expands automatically
  [Tab:metric@segment]  open tab, highlight segment overview cell
  [Tab:segment_value]   open tab, highlight segment overview column
  [Tab:]                open tab only, no focus
  [Tab Name]            bare tab name — same as [Tab Name:]
  [event]               focus event/edge in the currently active tab

Rule: every cited number must be followed by an anchor link to its source.
      Use edge links [Tab:src->tgt] for transition percentages — NEVER node-only links.

Examples:
  '**[Flow:add_to_cart->purchase]** reaches 38% conversion'
  'Mobile shows **12%** [Segments:mobile], desktop **31%** [Segments:desktop]'
  '67% of sessions exit at **[Funnel:checkout@4]**'
  'See [Overview:] for the full KPI comparison'""",
}


def _playbook_index() -> dict:
    return {
        "usage": "Call playbook(scenario) with one of the keys below.",
        "scenarios": sorted(_PLAYBOOK.keys()),
    }


def _tool_docs_index() -> dict:
    preprocessors = sorted(
        m
        for m in dir(Eventstream)
        if not m.startswith("_")
        and callable(getattr(Eventstream, m))
        and inspect.getdoc(getattr(Eventstream, m))
        and m
        in {
            "filter_events",
            "filter_paths",
            "add_segment",
            "collapse_events",
            "truncate_paths",
            "rename_events",
            "edit_events",
            "drop_events",
            "add_events",
            "to_daily_states",
            "drop_segment",
            "add_clusters",
            "urls_to_events",
            "sample_paths",
            "split_sessions",
        }
    )
    return {
        "usage": "Call describe_tool(topic) with one of the keys below.",
        "preprocessors": preprocessors,
        "reference_topics": sorted(_STATIC_TOOL_DOCS.keys()),
    }


def _static_instructions(tail: str = "") -> str:
    """The stream/context-independent portion of the system prompt: role
    framing, workflow, analysis-text rules, preprocessing guidance, and the
    canonical analysis patterns. Contains no per-`serve()`-call content (no
    stream stats, no context dict), so it's safe to reuse verbatim as a
    prompt-caching prefix across turns/sessions — see the platform's
    `assistant.py`, which puts this behind a `cache_control` breakpoint
    instead of recomputing/rebaking it per chat message the way the
    notebook's `_system_instructions` does (below).

    `tail`: the final "where to save reports" line. Notebook callers pass a
    concrete path via `_system_instructions`; platform callers (which have no
    local filesystem destination) pass their own instruction, or omit it for
    the generic fallback line.
    """
    lines = [
        "You are a product analytics assistant with access to a user behaviour eventstream.",
        "",
        "## Workflow",
        "",
        "IMPORTANT — before doing anything else, classify the user's question:",
        "  A) Mentions a specific date range or time window?",
        "     → Apply the TEMPORAL ANOMALY pattern (see ## Analysis patterns below).",
        "     → This REQUIRES calling update_base_stream with add_segment(rules=[...]).",
        "     → Do NOT skip this step and jump straight to add_transition_graph.",
        "  B) Asks why conversion from A to B is dropping?",
        "     → Apply the CONVERSION DROP-OFF pattern (funnel_events).",
        "  C) General exploration or comparison between segments?",
        "     → Follow the standard workflow below.",
        "",
        "1. Call describe() to understand the data (always first).",
        "2. Optionally call update_base_stream(preprocessors) to filter/transform the stream",
        "   for the entire analysis session (e.g. remove noise events, filter short paths).",
        "   Ask the user for confirmation before calling — it resets the analysis context.",
        "   All subsequent add_* calls use this preprocessed stream automatically.",
        "3. Call add_transition_graph(), add_step_matrix(), and/or add_segment_overview().",
        "   Each call returns a compact summary for analysis and registers a tab in the report.",
        "   The full interactive visualisation is always embedded in the report.",
        "   PARALLELISM: add_* calls are independent — if your client supports parallel",
        "   tool calls, issue them simultaneously to save time.",
        "4. Write your analysis text (see ## Analysis text below).",
        "   Numbers you computed yourself (not read from a tab) → wrap in backticks: `2.2×`.",
        "5. Call check_analysis(analysis) — MANDATORY before export_report.",
        "   Fix every issue it returns. Repeat until status is 'ok'.",
        "6. Call export_report(analysis=<validated_text>) to finalize the report",
        "   with all added tabs and the analysis panel.",
        "",
        "## Analysis text",
        "- Use markdown: # Heading, ## Sub, **bold**, *italic*, - list, | table |.",
        "- Reference widgets using [tab_label:ref] syntax:",
        "    [Overall Flow:basket]               → focus node 'basket' in transition graph",
        "    [Overall Flow:basket->purchase]      → focus edge basket→purchase (dims the rest)",
        "    [Overall Flow:view=Checkout]         → apply the named view passed via add_transition_graph(views=[...])",
        "    [Purchase Funnel:basket@4]           → scroll to cell basket at step 4 in step matrix",
        "                                           (step_window expands automatically if needed)",
        "    [Platform Breakdown:mobile]            → highlight 'mobile' column in segment_overview",
        "                                             (displayed as 'platform: mobile' in report)",
        "    [Platform Breakdown:has_purchase@mobile] → highlight specific cell: metric@segment",
        "    [basket]                             → focus in whichever tab is currently active",
        "- Tab labels must not contain colons.",
        "- Numbers you derived yourself (not read from a tab): wrap in backticks `2.2×` — exempt from link requirement.",
        "  EXCEPTION: backticks are ONLY for plain numbers/%. NEVER use backticks for edges (A → B).",
        "  Edges (transition A → B) must ALWAYS be [tab:A->B] links — never plain text, never backticks.",
        "- MANDATORY: EVERY number/% READ from a tab MUST have an anchor link.",
        "  Numbers without links are FORBIDDEN — in prose, tables, and lists alike.",
        "- When mentioning a transition between two events — anywhere in the text — ALWAYS use",
        "  an edge link [tab:src->tgt]. NEVER write 'A → B' as plain text. Examples:",
        "    CORRECT: '**[Flow:review_order->purchase]** — **38%** в spike'",
        "    CORRECT: | **[Flow:review_order->purchase]** | **38%** | **22%** |",
        "    WRONG: 'review_order → purchase: 38%'   ← no link",
        "    WRONG: '[Flow:review_order]: 38%'        ← node link instead of edge",
        "- Segment KPI → use column or cell link:",
        "    '**[Segments:mobile]**: **12%** vs **[Segments:desktop]**: **31%**'",
        "- Tab reference without specific element → bare tab name or [Tab:]:",
        "    '**[KPI по периодам]** показывает ...'   OR   '**[KPI по периодам:]**'",
        "- Always call export_report() when you're done — its result is what the user sees.",
        "",
        "## Preprocessing",
        "- update_base_stream(preprocessors): changes the baseline for the whole session.",
        "  Use when the user says 'filter X and analyse', 'remove noise', 'work with Y'.",
        "  ALWAYS ask for confirmation first: 'This will replace the current stream. Proceed?'",
        "  After confirmation, call it ONCE — all add_* tools will use the new stream.",
        "  Calling it again starts a new analysis branch from the original stream.",
        "- local_preprocessors in add_*: one-off preprocessing for a single visualisation.",
        "  Use only when the preprocessing is specific to that visualisation and would NOT",
        "  make sense applied globally (e.g. 'show a graph with only mobile users' while",
        "  the rest of the analysis remains on all users).",
        "  Do NOT repeat the same local_preprocessors across multiple add_* calls —",
        "  use update_base_stream instead.",
        "",
        "## Analysis patterns",
        "",
        "### Temporal anomaly — spike, drop, incident, unusual period",
        "Trigger: user asks about a specific date range ('why did X spike on Jan 20-22?',",
        "  'what happened during those dates?', etc.).",
        "Do NOT execute Python code. Call update_base_stream (ask user to confirm) with:",
        "  preprocessors=[{",
        "    'type': 'add_segment', 'name': 'period',",
        "    'rules': [",
        "      ['{timestamp_col}', '<',  'YYYY-MM-DD', 'normal'],",
        "      ['{timestamp_col}', '>',  'YYYY-MM-DD', 'normal'],",
        "      ['anomaly']",
        "    ]",
        "  }]",
        "  Replace {timestamp_col} with the value from describe() ('timestamp_col' field).",
        "  The first date is the start of the anomaly window, the second is the end.",
        "  Rules format: each entry is [column, op, value, segment_label];",
        "  the last entry ['anomaly'] is the ELSE fallback.",
        "Then use diff=['period', 'anomaly', 'normal'] in add_transition_graph / add_step_matrix.",
        "Also: add_segment_overview(segment_col='period') to compare all KPIs.",
        "",
        "### Segment comparison (e.g. new vs loyal, mobile vs desktop)",
        "The segment column is already in the stream — no preprocessing needed.",
        "Use diff=['segment_col', 'val1', 'val2'] directly in add_transition_graph / add_step_matrix.",
        "",
        "### Funnel / path analysis (conversion from event A to event B)",
        "Use add_step_matrix with path_pattern='A->.*->B' to focus on paths",
        "that pass through both anchor events.",
        "",
        "### Conversion drop-off analysis (why is conversion from A to B dropping?)",
        "Use add_segment with funnel_events to create an N+1 level funnel segment.",
        "Each level is named after the LAST funnel event reached (in sequential order).",
        "Example for a 2-step funnel [add_to_cart, purchase]:",
        "  preprocessors=[{",
        "    'type': 'add_segment', 'name': 'funnel',",
        "    'funnel_events': ['add_to_cart', 'purchase']",
        "  }]",
        "  → segment levels: 'out_of_funnel', 'add_to_cart', 'purchase'",
        "  Then: diff=['funnel', 'add_to_cart', 'purchase']  (compare dropped vs converted)",
        "For a 3-step funnel [add_to_cart, checkout, purchase]:",
        "  funnel_events: ['add_to_cart', 'checkout', 'purchase']",
        "  → levels: 'out_of_funnel', 'add_to_cart', 'checkout', 'purchase'",
        "  To analyse checkout→purchase drop-off: diff=['funnel', 'checkout', 'purchase']",
        "  To analyse add_to_cart→checkout drop-off: diff=['funnel', 'add_to_cart', 'checkout']",
        "",
        "### Noise removal before any analysis",
        "Call update_base_stream first (ask user to confirm) with one or more of:",
        "  collapse_events consecutive=True  — removes self-loops",
        "  filter_paths length > N          — removes very short sessions",
        "  filter_events column/values      — removes specific noise events",
        tail or "- Save reports to a convenient local path.",
    ]
    return "\n".join(lines)


def _system_instructions(
    stream: "Eventstream | None", context: dict, notebook_dir: str = ""
) -> str:
    """Full system prompt for the notebook `serve()` flow: a small per-stream
    header (stats, business context) computed fresh at server-start, followed
    by the cacheable `_static_instructions()` block. The header is not
    cacheable (it depends on the specific stream/context passed to `serve()`)
    — platform callers skip it entirely and rely on the agent's own first
    workflow step (`describe()`) to learn the current data shape instead.

    `stream` is None when `serve()` was started in data-agnostic mode (no
    stream passed) — the header then tells the agent to call `load_data`
    before anything else, instead of reporting stats for data that doesn't
    exist yet.
    """
    if stream is None:
        header = [
            "No eventstream is loaded yet — serve() was started without one "
            "(data-agnostic mode).",
            "Call load_data(path=..., schema=...) FIRST, before any other tool "
            "(every other tool errors until data is loaded).",
        ]
    else:
        s = stream.schema
        df = stream.df
        n_paths = int(df[s.path_col].nunique())
        events = sorted(df[s.event_col].astype(str).unique().tolist())
        header = [
            f"The stream contains {n_paths} unique paths and {len(events)} distinct events.",
            f"Event column: '{s.event_col}'. Path column: '{s.path_col}'.",
        ]
    if context.get("description"):
        header.append(f"Business context: {context['description']}")
    if context.get("events"):
        descs = ", ".join(f"'{k}': {v}" for k, v in context["events"].items())
        header.append(f"Event meanings: {descs}")
    if context.get("kpis"):
        descs = ", ".join(f"'{k}': {v}" for k, v in context["kpis"].items())
        header.append(f"Key metrics: {descs}")

    tail = (
        f"- Save reports to the notebook directory: {notebook_dir}"
        if notebook_dir
        else ""
    )
    return "\n".join(header) + "\n\n" + _static_instructions(tail=tail)


# Public alias — non-notebook callers (e.g. the platform's chat assistant)
# import this name directly instead of reaching into the private module.
static_instructions = _static_instructions
