import json
import pathlib

import anywidget
import traitlets

_STATIC = pathlib.Path(__file__).parent.parent / "static"
_UNSET = object()

from retentioneering.widgets._esm import _get_esm  # noqa: E402
from retentioneering.widgets._html_export import write_html  # noqa: E402


class SegmentOverviewWidget(anywidget.AnyWidget):
    _esm = _get_esm()
    _css = _STATIC / "widget.css"

    widget_type = traitlets.Unicode("segment_overview").tag(sync=True)

    # ── config traitlets ───────────────────────────────────────────────────
    segment_col = traitlets.Unicode("").tag(sync=True)
    path_id_col = traitlets.Unicode("").tag(sync=True)
    metrics_config = traitlets.Unicode("[]").tag(sync=True)  # JSON list
    apply_trigger = traitlets.Unicode("").tag(sync=True)  # any change → recompute

    # ── catalogues ────────────────────────────────────────────────────────
    segment_cols = traitlets.Unicode("[]").tag(sync=True)
    segment_levels = traitlets.Unicode("{}").tag(sync=True)
    path_cols = traitlets.Unicode("[]").tag(sync=True)
    event_list = traitlets.Unicode("[]").tag(sync=True)

    # ── result ────────────────────────────────────────────────────────────
    result = traitlets.Unicode("{}").tag(sync=True)
    is_loading = traitlets.Bool(False).tag(sync=True)
    error = traitlets.Unicode("").tag(sync=True)

    # ── distribution request/result ───────────────────────────────────────
    dist_request = traitlets.Unicode("").tag(sync=True)
    dist_result = traitlets.Unicode("{}").tag(sync=True)

    # ── display ───────────────────────────────────────────────────────────
    widget_id = traitlets.Unicode("").tag(sync=True)
    height = traitlets.Int(480).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)

    def __init__(
        self,
        eventstream,
        segment_col=_UNSET,
        metrics_config=_UNSET,
        path_id_col=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False
        self.widget_id = ""
        self.widget_type = "segment_overview"

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
            self.segment_levels = json.dumps(eventstream.get_all_segment_levels())
        except Exception:
            self.segment_levels = "{}"
        self.path_cols = json.dumps(eventstream.schema.path_cols)

        self.segment_col = segment_col if segment_col is not _UNSET else ""
        self.path_id_col = path_id_col if path_id_col is not _UNSET else ""
        _mc = metrics_config if metrics_config is not _UNSET else []
        self.metrics_config = (
            json.dumps(_mc) if isinstance(_mc, list) else (_mc or "[]")
        )
        self.height = height if height is not _UNSET else 480
        self.sidebar_open = sidebar_open if sidebar_open is not _UNSET else True

        if self.segment_col:
            self._recompute()

        self._initialized = True
        self.observe(self._on_apply, names=["apply_trigger"])
        self.observe(self._on_dist_request, names=["dist_request"])

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

    # ── computation ───────────────────────────────────────────────────────

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            metrics = json.loads(self.metrics_config) if self.metrics_config else []
            df = self._eventstream.segment_overview_data(
                segment_col=self.segment_col,
                metrics_config=metrics,
                path_id_col=self.path_id_col or None,
            )
            self.result = json.dumps(
                {
                    "metrics": df.index.tolist(),
                    "segments": df.columns.tolist(),
                    "values": [
                        [_safe(v) for v in df.loc[m].tolist()] for m in df.index
                    ],
                }
            )
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
        finally:
            self.is_loading = False

    # ── HTML export ───────────────────────────────────────────────────────────

    def export_html(
        self,
        path: str,
        title: str = "Segment Overview",
        analysis: str | None = None,
    ) -> None:
        data = {
            "widget_type": "segment_overview",
            "result": json.loads(self.result or "{}"),
            "segment_col": self.segment_col or "",
            "path_id_col": self.path_id_col or "",
            "metrics_config": json.loads(self.metrics_config or "[]"),
            "segment_cols": json.loads(self.segment_cols or "[]"),
            "segment_levels": json.loads(self.segment_levels or "{}"),
            "path_cols": json.loads(self.path_cols or "[]"),
            "event_list": json.loads(self.event_list or "[]"),
            "height": self.height,
            "sidebar_open": False,
        }
        write_html(path, title, "Segment Overview", data, analysis)

    def _compute_distribution(self, req: dict):
        self.is_loading = True
        try:
            result = self._eventstream.metric_distribution(
                segment_col=req["segment_col"],
                segment_value=req["segment_value"],
                metric=req["metric"],
                complement=req.get("complement", False),
                path_id_col=req.get("path_id_col"),
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
