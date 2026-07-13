import json

import traitlets

from retentioneering.exceptions import RetentioneeringError
from retentioneering.widgets._base import _UNSET, RetentioneeringWidget
from retentioneering.widgets._html_export import write_html


class SegmentOverviewWidget(RetentioneeringWidget):
    widget_type = traitlets.Unicode("segment_overview").tag(sync=True)

    # ── config traitlets ───────────────────────────────────────────────────
    segment_col = traitlets.Unicode("").tag(sync=True)
    path_col = traitlets.Unicode("").tag(sync=True)
    metrics = traitlets.Unicode("[]").tag(sync=True)  # JSON list
    apply_trigger = traitlets.Unicode("").tag(sync=True)  # any change → recompute

    # ── catalogues ────────────────────────────────────────────────────────
    segment_cols = traitlets.Unicode("[]").tag(sync=True)
    segment_levels = traitlets.Unicode("{}").tag(sync=True)
    path_cols = traitlets.Unicode("[]").tag(sync=True)
    event_list = traitlets.Unicode("[]").tag(sync=True)

    # ── result (is_loading/error are inherited from RetentioneeringWidget) ─
    result = traitlets.Unicode("{}").tag(sync=True)

    # ── distribution request/result ───────────────────────────────────────
    dist_request = traitlets.Unicode("").tag(sync=True)
    dist_result = traitlets.Unicode("{}").tag(sync=True)

    # ── display ───────────────────────────────────────────────────────────
    widget_id = traitlets.Unicode("").tag(sync=True)
    height = traitlets.Int(480).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)

    _persist_names = ("segment_col", "metrics", "path_col", "height", "sidebar_open")

    def __init__(
        self,
        eventstream,
        segment_col=_UNSET,
        metrics=_UNSET,
        path_col=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        state_file=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False
        self.widget_id = ""
        self.widget_type = "segment_overview"
        self._load_state_file(state_file)

        # Catalogues
        try:
            all_events = sorted(
                eventstream.df[eventstream.schema.event_col]
                .astype(str)
                .unique()
                .tolist()
            )
            self.event_list = json.dumps(all_events)
        except Exception:
            self.event_list = "[]"
        self.segment_cols = json.dumps(eventstream.schema.segment_cols)
        try:
            self.segment_levels = json.dumps(eventstream.get_segment_levels())
        except Exception:
            self.segment_levels = "{}"
        self.path_cols = json.dumps(eventstream.schema.path_cols)

        self.segment_col = segment_col if segment_col is not _UNSET else ""
        self.path_col = path_col if path_col is not _UNSET else ""
        _mc = metrics if metrics is not _UNSET else []
        self.metrics = json.dumps(_mc) if isinstance(_mc, list) else (_mc or "[]")
        self.height = height if height is not _UNSET else 480
        self.sidebar_open = sidebar_open if sidebar_open is not _UNSET else True

        self._apply_saved_state(
            exclude={
                name
                for name, arg in (
                    ("segment_col", segment_col),
                    ("metrics", metrics),
                    ("path_col", path_col),
                    ("height", height),
                    ("sidebar_open", sidebar_open),
                )
                if arg is not _UNSET
            }
        )

        if self.segment_col:
            self._recompute()

        self._initialized = True
        self.observe(self._on_apply, names=["apply_trigger"])
        self.observe(self._on_dist_request, names=["dist_request"])
        self._start_state_autosave()

    # ── observers ─────────────────────────────────────────────────────────

    def _on_apply(self, _change):
        if not self._initialized:
            return
        self._recompute()

    def _on_dist_request(self, change):
        raw = change["new"]
        if not raw:
            return
        try:
            req = json.loads(raw)
        except Exception:
            return
        self._compute_distribution(req)

    # ── dispatch ─────────────────────────────────────────────────────────────

    def _tool_segment_overview_data(self, params: dict):
        metrics = params.get("metrics")
        if metrics is None:
            metrics = json.loads(self.metrics) if self.metrics else []
        return self._compute_raw(
            segment_col=params.get("segment_col") or self.segment_col,
            metrics=metrics,
            path_col=params.get("path_col") or self.path_col or None,
        )

    def _tool_get_metric_distribution(self, params: dict):
        return self._eventstream.get_metric_distribution(
            segment_col=params["segment_col"],
            segment_value=params["segment_value"],
            metric=params["metric"],
            complement=params.get("complement", False),
            path_col=params.get("path_col"),
        )

    #: See RetentioneeringWidget.compute_tools.
    compute_tools = {
        "segment_overview_data": _tool_segment_overview_data,
        "get_metric_distribution": _tool_get_metric_distribution,
    }

    # ── computation ───────────────────────────────────────────────────────

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            metrics = json.loads(self.metrics) if self.metrics else []
            self.result = json.dumps(
                self._compute_raw(
                    segment_col=self.segment_col,
                    metrics=metrics,
                    path_col=self.path_col or None,
                )
            )
        except RetentioneeringError:
            raise
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
        finally:
            self.is_loading = False

    def _compute_raw(self, segment_col, metrics, path_col=None) -> dict:
        df = self._eventstream.segment_overview_data(
            segment_col=segment_col,
            metrics=metrics,
            path_col=path_col,
        )
        return {
            "metrics": df.index.tolist(),
            "segments": df.columns.tolist(),
            "values": [[_safe(v) for v in df.loc[m].tolist()] for m in df.index],
        }

    # ── HTML export ───────────────────────────────────────────────────────────

    def export_html(
        self,
        path: str,
        title: str = "Segment Overview",
        analysis: str | None = None,
        sidebar_open: bool | None = None,
    ) -> None:
        self._raise_if_error()
        data = {
            "widget_type": "segment_overview",
            "result": json.loads(self.result or "{}"),
            "segment_col": self.segment_col or "",
            "path_col": self.path_col or "",
            "metrics": json.loads(self.metrics or "[]"),
            "segment_cols": json.loads(self.segment_cols or "[]"),
            "segment_levels": json.loads(self.segment_levels or "{}"),
            "path_cols": json.loads(self.path_cols or "[]"),
            "event_list": json.loads(self.event_list or "[]"),
            "height": self.height,
            "sidebar_open": sidebar_open
            if sidebar_open is not None
            else self.sidebar_open,
        }
        write_html(path, title, "Segment Overview", data, analysis)

    def _compute_distribution(self, req: dict):
        self.is_loading = True
        try:
            result = self._eventstream.get_metric_distribution(
                segment_col=req["segment_col"],
                segment_value=req["segment_value"],
                metric=req["metric"],
                complement=req.get("complement", False),
                path_col=req.get("path_col"),
            )
            self.dist_result = json.dumps(
                result,
                default=lambda x: None if (isinstance(x, float) and x != x) else x,
            )
        except Exception as exc:
            self.dist_result = json.dumps({"error": str(exc)})
        finally:
            self.is_loading = False


def _safe(v):
    import math

    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v
