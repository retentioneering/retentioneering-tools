"""A/B steps widget: the `get_ab_steps` screen as an event x step matrix.

Every cell is one row of `Eventstream.ab_steps_data` (the widget's headless twin,
an alias of `get_ab_steps`): its background is the delta, its text the delta and
the interval, its outline whether the delta is significantly non-zero after the
correction. See `widgets/_ab_matrix.py` for the payload.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import traitlets

from retentioneering.widgets._ab_matrix import ABMatrixWidget

_DEFAULTS: dict[str, Any] = {
    "max_steps": 10,
    "horizon": "",
    "path_col": "",
    "correction": "bh",
    "ci_level": 0.95,
    "power": 0.8,
    "min_support": 10,
    # get_ab_steps refuses both in this release; kept so that the widget's data
    # parameters are its twin's signature (ADR-0006)
    "path_pattern": "",
    "anchor": "",
}


class ABStepsWidget(ABMatrixWidget):
    widget_type = traitlets.Unicode("ab_steps").tag(sync=True)

    # ── data parameters (the arguments of ab_steps_data) ───────────────────────
    max_steps = traitlets.Int(10).tag(sync=True)
    path_pattern = traitlets.Unicode("").tag(sync=True)
    # "" | the anchor as text, a dict anchor as JSON
    anchor = traitlets.Unicode("").tag(sync=True)

    _defaults = _DEFAULTS
    _editable = ("max_steps", "correction", "ci_level", "min_support")
    _tool = "get_ab_steps"
    _export_label = "A/B Steps"

    def _to_trait(self, name: str, value: Any) -> Any:
        if name in ("path_pattern", "anchor"):
            if isinstance(value, dict):
                return json.dumps(value, default=str)
            return value or ""
        return super()._to_trait(name, value)

    def _params(self, overrides: dict | None = None) -> dict:
        params = super()._params(overrides)
        params["path_pattern"] = params["path_pattern"] or None
        params["anchor"] = params["anchor"] or None
        return params

    def _twin(self, params: dict) -> pd.DataFrame:
        return self._eventstream.ab_steps_data(self._diff, **params)

    def _payload_extra(self, frame: pd.DataFrame, params: dict) -> dict:
        return {
            "max_steps": (frame.attrs or {}).get("max_steps", params["max_steps"]),
            "unit": "pp",
            "conditional": False,
        }

    def _tool_ab_steps_data(self, params: dict):
        return self._compute_raw(self._params(params))

    #: See RetentioneeringWidget.compute_tools.
    compute_tools = {"ab_steps_data": _tool_ab_steps_data}
