import json

import traitlets

from retentioneering.exceptions import InvalidParameterError, RetentioneeringError
from retentioneering.widgets._base import _UNSET, RetentioneeringWidget
from retentioneering.widgets._utils import parse_diff as _parse_diff
from retentioneering.widgets._html_export import write_html


def _validate_view_events(view: dict, prefix: str, available: set) -> None:
    """Raise if a hand-authored view dict (views=/view=) names an event that
    doesn't exist — same contract as steps=/source_events=/mapping= etc.
    elsewhere: fail loudly at construction time instead of silently
    rendering an empty Ego view / focus in the browser."""
    focus = view.get("focus")
    if isinstance(focus, dict):
        focus_type = focus.get("type")
        if focus_type == "node":
            node_id = focus.get("event")
            if node_id not in available:
                raise InvalidParameterError(
                    f"{prefix}.focus.event", node_id, sorted(available)
                )
        elif focus_type == "edge":
            for key in ("source", "target"):
                event_id = focus.get(key)
                if event_id not in available:
                    raise InvalidParameterError(
                        f"{prefix}.focus.{key}", event_id, sorted(available)
                    )
        elif focus_type == "path":
            for node_id in focus.get("nodes") or []:
                if node_id not in available:
                    raise InvalidParameterError(
                        f"{prefix}.focus.nodes", node_id, sorted(available)
                    )
    for event_id in view.get("hiddenEvents") or []:
        if event_id not in available:
            raise InvalidParameterError(
                f"{prefix}.hiddenEvents", event_id, sorted(available)
            )
    ego_node = view.get("egoNode")
    if ego_node is not None and ego_node not in available:
        raise InvalidParameterError(f"{prefix}.egoNode", ego_node, sorted(available))


class TransitionGraphWidget(RetentioneeringWidget):
    widget_type = traitlets.Unicode("transition_graph").tag(sync=True)

    # ── recompute triggers ─────────────────────────────────────────────────────
    edge_weight = traitlets.Unicode("proba_out").tag(sync=True)
    diff = traitlets.Unicode("").tag(sync=True)
    path_col = traitlets.Unicode("").tag(sync=True)

    # ── catalogues ─────────────────────────────────────────────────────────────
    path_cols = traitlets.Unicode("[]").tag(sync=True)
    event_counts = traitlets.Unicode("{}").tag(sync=True)
    event_counts_g1 = traitlets.Unicode("{}").tag(sync=True)
    event_counts_g2 = traitlets.Unicode("{}").tag(sync=True)
    segment_levels = traitlets.Unicode("{}").tag(sync=True)

    # ── result (is_loading/error are inherited from RetentioneeringWidget) ─────
    result = traitlets.Unicode("{}").tag(sync=True)

    # ── display / persistent ───────────────────────────────────────────────────
    height = traitlets.Int(500).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)
    node_positions = traitlets.Unicode("{}").tag(sync=True)
    event_visibility = traitlets.Unicode("{}").tag(sync=True)
    # "" (default: per-node top-k auto mode)
    # | "[min, max]"              — manual edge weight range, normalized 0..1
    # | '{"mode": "topk", "k": n}' — per-node top-k auto mode with explicit k
    edge_filter = traitlets.Unicode("").tag(sync=True)
    # "" | "[min, max]" — event count (population) filter, absolute counts
    event_count_filter = traitlets.Unicode("").tag(sync=True)
    # "" | '{"zoom": z, "pan": {"x": x, "y": y}}' — canvas zoom/pan
    viewport = traitlets.Unicode("").tag(sync=True)
    # ── GraphView: named visual presets (see js/.../graph-view.ts) ─────────────
    # JSON list of view objects — rendered as pills, addressable by name.
    # Views describe only the VISUAL state (focus/filters/viewport), never
    # data parameters, so they work identically in static exports.
    views = traitlets.Unicode("[]").tag(sync=True)
    # Initial view: JSON view object, a name from `views`, or (static export
    # only) a base64url blob injected from the #view= URL fragment.
    view = traitlets.Unicode("").tag(sync=True)

    _persist_names = (
        "edge_weight",
        "diff",
        "path_col",
        "height",
        "sidebar_open",
        "node_positions",
        "event_visibility",
        "edge_filter",
        "event_count_filter",
        "viewport",
        "views",
    )

    def __init__(
        self,
        eventstream,
        edge_weight=_UNSET,
        diff=_UNSET,
        path_col=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        views=_UNSET,
        view=_UNSET,
        state_file=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False
        self._load_state_file(state_file)

        try:
            self.segment_levels = json.dumps(eventstream.get_segment_levels())
        except Exception:
            self.segment_levels = "{}"
        self.path_cols = json.dumps(eventstream.schema.path_cols)
        try:
            _event_counts = eventstream.get_event_counts()
        except Exception:
            _event_counts = {}
        self.event_counts = json.dumps(_event_counts)
        _available_events = set(_event_counts.keys())

        self.edge_weight = edge_weight if edge_weight is not _UNSET else "proba_out"
        _diff_val = diff if diff is not _UNSET else None
        self.diff = json.dumps(list(_diff_val)) if _diff_val else ""
        self.path_col = path_col if path_col is not _UNSET else ""

        # Stable per-graph identity instead of the base class's random uuid:
        # the browser namespaces node positions by widget_id, and a
        # data-derived id lets a manual arrangement survive cell re-runs
        # (same data → same namespace) without leaking across different
        # graphs. Reuses the eventstream's content fingerprint (the same one
        # recipes rely on) — note it includes event counts and row count, so
        # any data change resets to the computed layout; use state_file to
        # pin an arrangement to a logical graph regardless of data updates.
        # Only manual drags are ever persisted, so untouched graphs still
        # get the fresh computed layout every time.
        try:
            self.widget_id = "tg-" + eventstream.fingerprint[:12]
        except Exception:
            pass  # keep the base class's random uuid
        self.height = height if height is not _UNSET else 500
        self.sidebar_open = sidebar_open if sidebar_open is not _UNSET else True
        self.node_positions = "{}"

        _views_val = views if views is not _UNSET else None
        if _views_val is None:
            self.views = "[]"
        else:
            if isinstance(_views_val, dict):
                # list(dict) would silently produce the KEYS — a single view
                # passed here by mistake must fail loudly instead.
                raise TypeError(
                    "views= expects a list of view dicts (rendered as pills). "
                    "To apply a single view on load, pass it as view={...}."
                )
            _views_list = list(_views_val)
            if not all(isinstance(v, dict) for v in _views_list):
                raise TypeError("views= must be a list of view dicts")
            for _i, _v in enumerate(_views_list):
                _validate_view_events(_v, f"views[{_i}]", _available_events)
            self.views = json.dumps(_views_list)
        _view_val = view if view is not _UNSET else None
        if _view_val is None:
            self.view = ""
        elif isinstance(_view_val, str):
            self.view = _view_val  # a name referencing an entry in `views`
        elif isinstance(_view_val, dict):
            _validate_view_events(_view_val, "view", _available_events)
            self.view = json.dumps(_view_val)
        else:
            raise TypeError(
                "view= must be a view dict or the name of an entry in views="
            )

        self._apply_saved_state(
            exclude={
                name
                for name, arg in (
                    ("edge_weight", edge_weight),
                    ("diff", diff),
                    ("path_col", path_col),
                    ("height", height),
                    ("sidebar_open", sidebar_open),
                    ("views", views),
                )
                if arg is not _UNSET
            }
        )

        self._recompute()

        self._initialized = True
        self.observe(self._on_params_change, names=["edge_weight", "diff", "path_col"])
        self.observe(self._on_positions_change, names=["node_positions"])
        self.observe(self._on_event_visibility_change, names=["event_visibility"])
        self._start_state_autosave()

    # ── widget-specific observers ──────────────────────────────────────────────

    def _on_params_change(self, _change):
        if not self._initialized:
            return
        self._recompute()

    def _on_positions_change(self, _change):
        if not self._initialized:
            return

    def _on_event_visibility_change(self, _change):
        if not self._initialized:
            return

    # ── dispatch ───────────────────────────────────────────────────────────────

    def _tool_transition_graph_data(self, params: dict):
        return self._compute_tm_raw(
            edge_weight=params.get("edge_weight", self.edge_weight),
            path_col=params.get("path_col") or self.path_col or None,
            diff=_parse_diff(params.get("diff")),
        )

    def _tool_graph_layout(self, params: dict):
        return self._compute_graph_layout(params)

    def _tool_route_stats(self, params: dict):
        return self._eventstream.route_stats(
            nodes=params.get("nodes") or [],
            path_col=params.get("path_col") or self.path_col or None,
        )

    #: See RetentioneeringWidget.compute_tools — a platform backend can call
    #: these directly via ``dispatch_compute`` (or by looking this table up
    #: on the class) instead of hand-rolling a tool -> Eventstream-method map.
    compute_tools = {
        "transition_graph_data": _tool_transition_graph_data,
        "graph_layout": _tool_graph_layout,
        "route_stats": _tool_route_stats,
    }

    # ── computations ───────────────────────────────────────────────────────────

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            diff_list = _parse_diff(self.diff)
            result = self._compute_tm_raw(
                edge_weight=self.edge_weight,
                path_col=self.path_col or None,
                diff=diff_list,
            )
            self.result = json.dumps(result)

            if diff_list:
                try:
                    s1, s2 = self._eventstream._split_two(
                        diff_list, path_col=self.path_col or None
                    )
                    c1 = s1.get_event_counts()
                    c2 = s2.get_event_counts()
                    pid = self.path_col or s1.schema.path_cols[0]
                    for counts, s in ((c1, s1), (c2, s2)):
                        n = int(s.df[pid].nunique())
                        counts.setdefault("path_start", n)
                        counts.setdefault("path_end", n)
                    self.event_counts_g1 = json.dumps(c1)
                    self.event_counts_g2 = json.dumps(c2)
                except Exception:
                    self.event_counts_g1 = "{}"
                    self.event_counts_g2 = "{}"
            else:
                self.event_counts_g1 = "{}"
                self.event_counts_g2 = "{}"
        except RetentioneeringError:
            raise
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
        finally:
            self.is_loading = False

    def _compute_tm_raw(self, edge_weight: str, path_col=None, diff=None) -> dict:
        tm = self._eventstream.transition_graph_data(
            edge_weight=edge_weight,
            path_col=path_col,
            diff=diff,
        )
        if diff is not None:
            tm, tm1, tm2 = tm
            return {
                "events": tm.index.tolist(),
                "values": _df_to_list(tm),
                "group1": {"events": tm1.index.tolist(), "values": _df_to_list(tm1)},
                "group2": {"events": tm2.index.tolist(), "values": _df_to_list(tm2)},
            }
        # Raw transition counts ride along as a sparse mapping so the client
        # can derive exact incoming/outgoing shares (proba_in / proba_out for
        # the ego view) regardless of the displayed edge weight — including
        # in static HTML exports, where no compute backend is available.
        cnt = (
            tm
            if edge_weight == "count"
            else self._eventstream.transition_graph_data(
                edge_weight="count", path_col=path_col, diff=None
            )
        )
        counts: dict = {}
        for src, row in cnt.iterrows():
            nonzero = {dst: int(v) for dst, v in row.items() if v}
            if nonzero:
                counts[src] = nonzero
        return {
            "events": tm.index.tolist(),
            "values": _df_to_list(tm),
            "counts": counts,
        }

    def semantic_layout_positions(self) -> dict:
        """Best-effort semantic layout positions for static consumers (HTML
        export, MCP report tabs) that have no kernel to compute them lazily.
        Returns {} when the layout cannot be computed."""
        return self._compute_graph_layout({}).get("result") or {}

    def _compute_graph_layout(self, params: dict) -> dict:
        try:
            from retentioneering.tools.graph_layout import GraphLayout

            result = GraphLayout(self._eventstream).fit(
                path_col=params.get("path_col") or self.path_col or None,
            )
            return {"result": result}
        except Exception as exc:
            # Layout is an enhancement, not a requirement — the JS side falls
            # back to its own deterministic layout. Surface the reason instead
            # of swallowing it so client code can at least log it.
            return {"result": {}, "error": str(exc)}

    # ── HTML export ───────────────────────────────────────────────────────────

    def export_html(
        self,
        path: str,
        title: str = "Transition Graph",
        analysis: str | None = None,
        sidebar_open: bool | None = None,
    ) -> None:
        """
        Export the current graph as a standalone interactive HTML file.

        Parameters
        ----------
        path:
            Destination file path.
        title:
            Title shown in the browser tab.
        analysis:
            Optional analysis text. Wrap event names in square brackets to make
            them clickable, e.g. `"Drop-off at [basket]: 78% of users leave here."`.
            Supports basic markdown (bold, italic, bullet lists, tables, headings).
        sidebar_open:
            Whether the settings sidebar starts open in the exported file.
            Defaults to the widget's current ``sidebar_open`` value.
        """
        self._raise_if_error()
        # A static export cannot call the graph_layout compute (no kernel), so
        # when the user hasn't arranged nodes by hand, bake the semantic
        # layout positions in at export time.
        node_positions = json.loads(self.node_positions or "{}")
        if not node_positions:
            node_positions = self.semantic_layout_positions()
        data = {
            "widget_type": "transition_graph",
            "widget_id": self.widget_id,
            "result": json.loads(self.result or "{}"),
            "edge_weight": self.edge_weight,
            "diff": json.loads(self.diff) if self.diff else None,
            "event_counts": json.loads(self.event_counts or "{}"),
            "event_counts_g1": json.loads(self.event_counts_g1 or "{}"),
            "event_counts_g2": json.loads(self.event_counts_g2 or "{}"),
            "node_positions": node_positions,
            "event_visibility": json.loads(self.event_visibility or "{}"),
            "edge_filter": json.loads(self.edge_filter) if self.edge_filter else None,
            "event_count_filter": json.loads(self.event_count_filter)
            if self.event_count_filter
            else None,
            "viewport": json.loads(self.viewport) if self.viewport else None,
            "segment_levels": json.loads(self.segment_levels or "{}"),
            "path_cols": json.loads(self.path_cols or "[]"),
            "path_col": self.path_col or "",
            "height": self.height,
            "sidebar_open": sidebar_open
            if sidebar_open is not None
            else self.sidebar_open,
            "views": json.loads(self.views or "[]"),
            "view": self.view,
        }
        write_html(path, title, "Transition Graph", data, analysis)


# ── helpers ────────────────────────────────────────────────────────────────────


def _df_to_list(df) -> list:
    import pandas as pd

    rows = []
    for _, row in df.iterrows():
        cells = []
        for v in row:
            if pd.isna(v):
                cells.append(None)
            elif isinstance(v, pd.Timedelta):
                cells.append(v.total_seconds())
            elif hasattr(v, "__float__"):
                cells.append(float(v))
            else:
                cells.append(v)
        rows.append(cells)
    return rows
