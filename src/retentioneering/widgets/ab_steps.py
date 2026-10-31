"""A/B steps widget: the `get_ab_steps` screen as an event x step matrix.

Every cell is one row of `Eventstream.ab_steps_data` (the widget's headless twin,
an alias of `get_ab_steps`): its background is the delta, its text the delta and
the interval, its outline whether the delta is significantly non-zero after the
correction. See `widgets/_ab_matrix.py` for the payload.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import traitlets

from retentioneering.widgets._ab_matrix import ABMatrixWidget
from retentioneering.widgets._base import _UNSET

_DEFAULTS: dict[str, Any] = {
    "max_steps": 10,
    "horizon": "",
    "path_col": "",
    "correction": "bh",
    "ci_level": 0.95,
    "power": 0.8,
    "min_support": 10,
}


class ABStepsWidget(ABMatrixWidget):
    widget_type = traitlets.Unicode("ab_steps").tag(sync=True)

    # ── data parameters (the arguments of ab_steps_data) ───────────────────────
    max_steps = traitlets.Int(10).tag(sync=True)

    _defaults = _DEFAULTS
    _editable = ("max_steps", "correction", "ci_level", "min_support")
    _tool = "get_ab_steps"
    _export_label = "A/B Steps"

    def __init__(
        self,
        eventstream,
        diff,
        assignment=None,
        state_file=None,
        path_pattern=_UNSET,
        anchor=_UNSET,
        **args,
    ):
        # ab_steps takes its twin's signature (ADR-0006), path_pattern and anchor
        # included, but get_ab_steps refuses both in this release: a passed one
        # goes to the twin with the other arguments as written, which raises the
        # getter's own error. They have no traitlet, no sidebar entry and are
        # never saved.
        refused = {
            name: value
            for name, value in (("path_pattern", path_pattern), ("anchor", anchor))
            if value is not _UNSET and value is not None
        }
        if refused:
            given = {
                name: value
                for name, value in args.items()
                if name in self._defaults and value is not _UNSET
            }
            eventstream.ab_steps_data(diff, assignment=assignment, **given, **refused)
        super().__init__(
            eventstream, diff, assignment=assignment, state_file=state_file, **args
        )

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
