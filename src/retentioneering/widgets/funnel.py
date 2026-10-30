import json

import traitlets

from retentioneering.exceptions import RetentioneeringError
from retentioneering.widgets._base import _UNSET, RetentioneeringWidget
from retentioneering.widgets._utils import parse_diff as _parse_diff


class FunnelWidget(RetentioneeringWidget):
    widget_type = traitlets.Unicode("funnel").tag(sync=True)

    # ── recompute triggers ────────────────────────────────────────────────────
    steps = traitlets.Unicode("[]").tag(sync=True)
    diff = traitlets.Unicode("").tag(sync=True)
    path_col = traitlets.Unicode("").tag(sync=True)
    # 0.0 = intervals off. A Float rather than Float(allow_none) keeps the JS
    # side a plain number (host.get("ci_level") || 0).
    ci_level = traitlets.Float(0.0).tag(sync=True)
    correction = traitlets.Unicode("none").tag(sync=True)

    # ── catalogues ────────────────────────────────────────────────────────────
    event_list = traitlets.Unicode("[]").tag(sync=True)
    path_cols = traitlets.Unicode("[]").tag(sync=True)
    segment_levels = traitlets.Unicode("{}").tag(sync=True)

    # ── result (is_loading/error are inherited from RetentioneeringWidget) ─────
    result = traitlets.Unicode("{}").tag(sync=True)

    # ── display ───────────────────────────────────────────────────────────────
    widget_id = traitlets.Unicode("").tag(sync=True)
    height = traitlets.Int(420).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)

    _persist_names = (
        "steps",
        "diff",
        "path_col",
        "ci_level",
        "correction",
        "height",
        "sidebar_open",
    )

    def __init__(
        self,
        eventstream,
        steps=_UNSET,
        diff=_UNSET,
        path_col=_UNSET,
        ci_level=_UNSET,
        correction=_UNSET,
        height=_UNSET,
        sidebar_open=_UNSET,
        state_file=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._eventstream = eventstream
        self._initialized = False
        self._load_state_file(state_file)

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
            self.segment_levels = json.dumps(eventstream.get_segment_levels())
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
        self.path_col = path_col if path_col is not _UNSET else ""
        self.ci_level = (
            float(ci_level) if ci_level is not _UNSET and ci_level is not None else 0.0
        )
        self.correction = (
            correction if correction is not _UNSET and correction else "none"
        )
        self.height = height if height is not _UNSET else 420
        self.sidebar_open = sidebar_open if sidebar_open is not _UNSET else True

        self._apply_saved_state(
            exclude={
                name
                for name, arg in (
                    ("steps", steps),
                    ("diff", diff),
                    ("path_col", path_col),
                    ("ci_level", ci_level),
                    ("correction", correction),
                    ("height", height),
                    ("sidebar_open", sidebar_open),
                )
                if arg is not _UNSET
            }
        )

        self._recompute()
        self._initialized = True
        self.observe(
            self._on_params_change,
            names=["steps", "diff", "path_col", "ci_level", "correction"],
        )
        self._start_state_autosave()

    def _on_params_change(self, _change):
        if not self._initialized:
            return
        self._recompute()

    # ── dispatch ───────────────────────────────────────────────────────────────

    def _tool_funnel_data(self, params: dict):
        steps = params.get("steps")
        if steps is None:
            steps = json.loads(self.steps) if self.steps else []
        return self._compute_raw(
            steps=steps,
            diff=_parse_diff(params.get("diff")),
            path_col=params.get("path_col") or self.path_col or None,
            ci_level=params.get("ci_level", self.ci_level) or None,
            correction=params.get("correction") or self.correction or "none",
        )

    #: See RetentioneeringWidget.compute_tools.
    compute_tools = {"funnel_data": _tool_funnel_data}

    # ── computation ────────────────────────────────────────────────────────────

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            steps = json.loads(self.steps) if self.steps else []
            diff = _parse_diff(self.diff)
            pid = self.path_col or None
            self.result = self._encode_result(
                self._compute_raw(
                    steps=steps,
                    diff=diff,
                    path_col=pid,
                    ci_level=self.ci_level or None,
                    correction=self.correction or "none",
                )
            )
        except RetentioneeringError as exc:
            # At construction the caller's arguments are wrong: raise in the cell.
            # Later the change came from the sidebar (e.g. intervals switched on
            # for a diff whose groups overlap): show it in the widget instead of
            # raising inside a traitlet observer, where nobody sees it.
            if not self._initialized:
                raise
            self.error = str(exc)
            self.result = "{}"
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
        finally:
            self.is_loading = False

    def _compute_raw(
        self, steps, diff=None, path_col=None, ci_level=None, correction="none"
    ) -> dict:
        result = self._eventstream.funnel_data(
            steps=steps,
            diff=diff,
            path_col=path_col,
            ci_level=ci_level,
            # the sidebar keeps its correction while intervals are off
            correction=correction if ci_level else "none",
        )
        if diff:
            if len(diff) == 3:
                result["group1_label"] = str(diff[1])
                result["group2_label"] = str(diff[2])
            else:
                result["group1_label"] = "Group 1"
                result["group2_label"] = "Group 2"
            # The tool returns the group sizes; the old back-calculation
            # (unique_paths / conversion_rate of step 1) gave 0 whenever no path
            # of a group reached step 1.
            if result.get("steps"):
                result["group1_total"] = result.get("funnel1_total_paths", 0)
                result["group2_total"] = result.get("funnel2_total_paths", 0)
        return result

    # ── HTML export (export_html/render_static: see RetentioneeringWidget) ────

    _export_label = "Funnel"

    def _export_data(self, sidebar_open: bool | None = None) -> dict:
        return {
            "widget_type": "funnel",
            "result": json.loads(self.result or "{}"),
            "steps": json.loads(self.steps or "[]"),
            "diff": json.loads(self.diff) if self.diff else None,
            "path_col": self.path_col or "",
            "ci_level": self.ci_level,
            "correction": self.correction or "none",
            "path_cols": json.loads(self.path_cols or "[]"),
            "segment_levels": json.loads(self.segment_levels or "{}"),
            "event_list": json.loads(self.event_list or "[]"),
            "height": self.height,
            "sidebar_open": sidebar_open
            if sidebar_open is not None
            else self.sidebar_open,
        }
