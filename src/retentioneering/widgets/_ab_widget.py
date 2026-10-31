"""Shared machinery of the A/B widgets that draw one frame of a headless twin:
``ab_transitions``, ``ab_steps`` (``_ab_matrix.py``) and ``ab_compression``.

A widget holds the twin's data parameters as traitlets (the sidebar edits a few
and recomputes, the others are fixed at construction), calls the twin, and sends
what the browser draws as the ``result`` traitlet (``_payload``); view
parameters live in the browser and are saved with ``state_file``. A wrong
argument at construction raises the twin's own error.
"""

from __future__ import annotations

import json
import math
import warnings
from typing import Any

import numpy as np
import pandas as pd
import traitlets

from retentioneering.exceptions import RetentioneeringError
from retentioneering.widgets._base import _UNSET, RetentioneeringWidget
from retentioneering.widgets.ab_test import _assignment_info, _duration_str


def jsonable(value: Any) -> Any:
    """``value`` with every key a string, NaN / inf as None and numpy scalars as
    Python ones: strict JSON."""
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if isinstance(value, (str, bool, int)) or value is None:
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    try:
        return jsonable(value.item())  # numpy scalars
    except AttributeError:
        return str(value)


def diff_text(diff: Any) -> Any:
    """What the read-only sidebar shows of ``diff``: the segment form as it is, an
    id-list diff as the sizes of its lists."""
    if isinstance(diff, (list, tuple)) and len(diff) == 3 and isinstance(diff[0], str):
        return [str(v) for v in diff]
    try:
        return {"group1_ids": len(diff[0]), "group2_ids": len(diff[1])}
    except Exception:
        return str(diff)


class ABFrameWidget(RetentioneeringWidget):
    """Common part of the A/B widgets drawn from one frame of a twin."""

    # ── data parameters shared by every twin ─────────────────────────────────
    horizon = traitlets.Unicode("").tag(sync=True)
    path_col = traitlets.Unicode("").tag(sync=True)
    correction = traitlets.Unicode("bh").tag(sync=True)
    ci_level = traitlets.Float(0.95).tag(sync=True)
    power = traitlets.Float(0.8).tag(sync=True)
    min_support = traitlets.Int(10).tag(sync=True)

    # ── fixed context ──────────────────────────────────────────────────────────
    #: the diff, for the read-only sidebar (JSON: the segment form, or list sizes)
    diff = traitlets.Unicode("").tag(sync=True)
    #: "" without an assignment table, "arm" / "arm,assigned_at" with one
    assignment_info = traitlets.Unicode("").tag(sync=True)

    # ── result (is_loading/error are inherited) ───────────────────────────────
    result = traitlets.Unicode("{}").tag(sync=True)

    # ── display ────────────────────────────────────────────────────────────────
    height = traitlets.Int(600).tag(sync=True)
    sidebar_open = traitlets.Bool(True).tag(sync=True)

    #: Data parameters with the twin's defaults; set by each subclass.
    _defaults: dict[str, Any] = {}
    #: Data parameters the sidebar edits (the others are read-only there).
    _editable: tuple[str, ...] = ()
    #: View parameters with their defaults; set by each subclass.
    _view_defaults: dict[str, Any] = {}
    _display_defaults: dict[str, Any] = {"height": 600, "sidebar_open": True}
    #: The getter the twin is an alias of.
    _tool: str = ""

    def __init__(
        self,
        eventstream,
        diff,
        assignment=None,
        state_file=None,
        **args,
    ):
        known = set(self._defaults) | set(self._display_defaults)
        unknown = sorted(set(args) - known)
        if unknown:
            raise TypeError(
                f"{type(self).__name__} got unexpected arguments {unknown}; it takes "
                f"{sorted(known)}"
            )
        super().__init__()
        self._eventstream = eventstream
        self._diff = diff
        # A table, not JSON: kept on the instance, never synced or saved.
        self._assignment = assignment
        self._initialized = False
        self._load_state_file(state_file)

        self.diff = json.dumps(diff_text(diff), default=str)
        self.assignment_info = _assignment_info(assignment)
        given = {
            name: value
            for name, value in args.items()
            if value is not _UNSET and value is not None
        }
        # The data arguments as the caller wrote them (a traitlet holds a horizon
        # as text, a level as a float): an error at construction is raised for
        # these, so it reads as the getter's own.
        self._given = {k: v for k, v in given.items() if k in self._defaults}
        for name, default in {**self._defaults, **self._display_defaults}.items():
            try:
                setattr(self, name, self._to_trait(name, given.get(name, default)))
            except traitlets.TraitError:
                if name in self._defaults:
                    # a value of the wrong type: the twin's own error says what it
                    # takes (the same message as the getter's)
                    self._twin(self._params(self._given))
                raise

        self._apply_saved_state(
            exclude={name for name, value in args.items() if value is not _UNSET}
        )

        self._recompute()
        self._initialized = True
        self.observe(self._on_params_change, names=list(self._defaults))
        self._start_state_autosave()

    @property
    def _data_names(self) -> tuple[str, ...]:
        return tuple(self._defaults)

    @property
    def _persist_names(self) -> tuple[str, ...]:  # type: ignore[override]
        return (
            tuple(self._defaults)
            + tuple(self._display_defaults)
            + tuple(self._view_defaults)
        )

    def _to_trait(self, name: str, value: Any) -> Any:
        """A constructor argument as the traitlet holds it."""
        if isinstance(value, np.generic):
            value = value.item()  # numpy scalars: the traitlets take Python ones
        if name == "horizon":
            return _duration_str(value)
        if name == "path_col":
            return value or ""
        return value

    def _on_params_change(self, _change):
        if not self._initialized:
            return
        self._recompute()

    # ── parameters ─────────────────────────────────────────────────────────────

    def _params(self, overrides: dict | None = None) -> dict:
        """The data parameters as keyword arguments of the twin (without ``diff``)."""
        raw = {name: getattr(self, name) for name in self._defaults}
        raw.update(overrides or {})
        params = dict(raw)
        params["horizon"] = raw["horizon"] or None
        params["path_col"] = raw["path_col"] or None
        params["assignment"] = self._assignment
        return params

    # ── computation ────────────────────────────────────────────────────────────

    def _twin(self, params: dict) -> pd.DataFrame:
        raise NotImplementedError

    def _recompute(self):
        self.is_loading = True
        self.error = ""
        try:
            self.result = self._encode_result(self._compute_raw(self._params()))
        except RetentioneeringError as exc:
            # At construction the caller's arguments are wrong: raise in the cell.
            # Later the change came from the sidebar: show it in the widget.
            if not self._initialized:
                self._twin(self._params(self._given))
                raise
            self.error = str(exc)
            self.result = "{}"
        except Exception as exc:
            self.error = str(exc)
            self.result = "{}"
        finally:
            self.is_loading = False

    def _compute_raw(self, params: dict) -> dict:
        """The frame of the twin, as the widget draws it."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            frame = self._twin(params)
        return self._payload(frame, params, [str(w.message) for w in caught])

    def _payload(self, frame: pd.DataFrame, params: dict, caught: list[str]) -> dict:
        raise NotImplementedError

    # ── HTML export ────────────────────────────────────────────────────────────

    def _export_data(self, sidebar_open: bool | None = None) -> dict:
        data: dict[str, Any] = {
            "widget_type": self.widget_type,
            "result": json.loads(self.result or "{}"),
            "diff": json.loads(self.diff) if self.diff else None,
            "assignment_info": self.assignment_info,
        }
        for name in tuple(self._defaults) + tuple(self._view_defaults):
            data[name] = getattr(self, name)
        data["height"] = self.height
        data["sidebar_open"] = (
            sidebar_open if sidebar_open is not None else self.sidebar_open
        )
        return data
