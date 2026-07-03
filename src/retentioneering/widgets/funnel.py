import json
import pathlib

import anywidget
import traitlets

_STATIC = pathlib.Path(__file__).parent.parent / "static"
_UNSET = object()

from retentioneering.widgets._esm import _get_esm  # noqa: E402
from retentioneering.widgets._utils import parse_diff as _parse_diff  # noqa: E402


class FunnelWidget(anywidget.AnyWidget):
    _esm = _get_esm()
    _css = _STATIC / "widget.css"

    widget_type = traitlets.Unicode("funnel").tag(sync=True)

    # ── recompute triggers ────────────────────────────────────────────────────
    steps = traitlets.Unicode("[]").tag(sync=True)
    diff = traitlets.Unicode("").tag(sync=True)
    path_id_col = traitlets.Unicode("").tag(sync=True)

    # ── catalogues ────────────────────────────────────────────────────────────
    event_list = traitlets.Unicode("[]").tag(sync=True)
    path_cols = traitlets.Unicode("[]").tag(sync=True)
    segment_levels = traitlets.Unicode("{}").tag(sync=True)

    # ── result ────────────────────────────────────────────────────────────────
    result = traitlets.Unicode("{}").tag(sync=True)
    is_loading = traitlets.Bool(False).tag(sync=True)
    error = traitlets.Unicode("").tag(sync=True)

    # ── display ───────────────────────────────────────────────────────────────
    widget_id = traitlets.Unicode("").tag(sync=True)
    height = traitlets.Int(420).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)

    def __init__(
        self,
        eventstream,
        steps=_UNSET,
        diff=_UNSET,
        path_id_col=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False

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
        try:
            self.segment_levels = json.dumps(eventstream.get_all_segment_levels())
        except Exception:
            self.segment_levels = "{}"
        self.path_cols = json.dumps(eventstream.schema.path_cols)

        _steps_val = steps if steps is not _UNSET else []
        self.steps = (
            json.dumps(_steps_val)
            if isinstance(_steps_val, list)
            else (_steps_val or "[]")
        )
        _diff_val = diff if diff is not _UNSET else None
        self.diff = json.dumps(list(_diff_val)) if _diff_val else ""
        self.path_id_col = path_id_col if path_id_col is not _UNSET else ""
        self.height = height if height is not _UNSET else 420
        self.sidebar_open = sidebar_open if sidebar_open is not _UNSET else True

        self._recompute()
        self._initialized = True
        self.observe(self._on_params_change, names=["steps", "diff", "path_id_col"])

    def _on_params_change(self, _change):
        if not self._initialized:
            return
        self._recompute()

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            steps = json.loads(self.steps) if self.steps else []
            diff = _parse_diff(self.diff)
            pid = self.path_id_col or None
            result = self._eventstream.funnel_data(
                steps=steps, diff=diff, path_id_col=pid
            )
            if diff and len(diff) == 3:
                result["group1_label"] = str(diff[1])
                result["group2_label"] = str(diff[2])
                steps_list = result.get("steps", [])
                if steps_list:
                    r1 = steps_list[0].get("funnel1_conversion_rate") or 0
                    r2 = steps_list[0].get("funnel2_conversion_rate") or 0
                    up1 = steps_list[0].get("funnel1_unique_paths", 0)
                    up2 = steps_list[0].get("funnel2_unique_paths", 0)
                    result["group1_total"] = round(up1 / r1) if r1 > 0 else up1
                    result["group2_total"] = round(up2 / r2) if r2 > 0 else up2
            else:
                steps_list = result.get("steps", [])
                if steps_list:
                    r = steps_list[0].get("conversion_rate") or 0
                    up = steps_list[0].get("unique_paths", 0)
                    result["total_paths"] = round(up / r) if r > 0 else up
            self.result = json.dumps(result)
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
        finally:
            self.is_loading = False
