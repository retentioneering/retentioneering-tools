"""A/B compression widget: the plan of `get_ab_compression`, step by step.

The widget draws one frame of its headless twin, `Eventstream.ab_compression_data`
(an alias of `get_ab_compression`, ADR-0006/ADR-0017): the compression path as a
chart (vocabulary size on x; testable cells, median MDE and transition mass
retained, the untestable share and, with ``target_delta``, the users needed on
y; steps that create artificial transitions marked), a step slider,
the mapping of every event at the selected step (its new label, dropped or
`other`, with the reason) and the screen's numbers before and after.

The twin's ``attrs`` hold every step (``path``, ``mappings``), so the slider moves
in the browser without a recompute, also in a static export. The payload sends,
per segment, the path rows and, per event, the steps at which its label changes
(``changes``): the label at step ``k`` is the last change at or before ``k``, which
is ``attrs["mappings"][k]``. A plan at another step is
``ab_compression_data(..., step=k)``, which ``compress_events`` applies.
"""

from __future__ import annotations

import inspect
import json
from typing import Any

import pandas as pd
import traitlets

from retentioneering.tools.ab_compression import (
    TARGETS,
    ab_compression,
    check_arguments,
)
from retentioneering.widgets._ab_widget import ABFrameWidget, jsonable
from retentioneering.widgets._base import _UNSET

_DEFAULTS: dict[str, Any] = {
    "screen": "transitions",
    "horizon": "",
    "path_col": "",
    "edge_weight": "",
    "max_steps": 0,
    "edges": "",
    "min_path_share": 0.01,
    "chain_threshold": 0.8,
    "synonym_threshold": 0.1,
    "bridge_threshold": 0.9,
    "max_events": 0,
    "max_group_size": 5,
    "target": "untestable_share",
    "step": -1,
    "keep": "",
    "other": "other",
    "by": "",
    "by_pre_exposure": "",
    "correction": "bh",
    "ci_level": 0.95,
    "power": 0.8,
    "min_support": 10,
    "target_delta": 0.0,
}

#: View parameters: set in the browser, saved with ``state_file``.
VIEW_DEFAULTS: dict[str, Any] = {
    # the step shown by the slider; -1 = the plan's chosen step
    "selected_step": -1,
    # the mapping table lists the events whose label changed only
    "changed_only": True,
    # the segment shown (``by``); "" = the first
    "segment": "",
}

#: The path columns sent to the browser.
PATH_FIELDS = (
    "step",
    "operation",
    "events",
    "label",
    "members",
    "vocabulary",
    "cells",
    "testable",
    "untestable",
    "untestable_share",
    "median_mde",
    "median_scale_needed",
    "scale_to_test_90",
    "transitions",
    "mass_retained",
    "artificial_transitions",
    "artificial_mass",
    "reason",
)


#: The getter's defaults, for the export line's arguments.
_GETTER_DEFAULTS = {
    name: parameter.default
    for name, parameter in inspect.signature(ab_compression).parameters.items()
    if name not in ("stream", "diff")
}


def _same(value: Any, default: Any) -> bool:
    try:
        return bool(value == default) and type(value) is type(default)
    except Exception:
        return False


def _none_if_empty(value: Any) -> Any:
    return None if value in ("", None) else value


def _json_list(value: Any) -> Any:
    """A list argument as the twin takes it: a traitlet holds it as JSON."""
    if value in ("", None):
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def action_of(event: str, label: Any, other: str) -> str:
    """The plan's ``action`` of an event mapped to ``label``."""
    if label is None:
        return "drop"
    if label == event:
        return "keep"
    if label == other:
        return "other"
    return "merge"


def label_changes(mappings: list[dict]) -> dict[str, list]:
    """Per event, ``[[step, label], ...]``: its label at step 0 and at every step
    where it changes (``None``: dropped). ``mappings[k]`` is the label of every
    event after step ``k`` (``attrs["mappings"]`` of the twin)."""
    changes: dict[str, list] = {}
    previous: dict = {}
    for step, mapping in enumerate(mappings):
        for event, label in mapping.items():
            if step == 0 or previous.get(event, event) != label:
                changes.setdefault(event, []).append([step, label])
        previous = mapping
    return changes


class ABCompressionWidget(ABFrameWidget):
    widget_type = traitlets.Unicode("ab_compression").tag(sync=True)

    # ── data parameters (the arguments of ab_compression_data) ───────────────
    screen = traitlets.Unicode("transitions").tag(sync=True)
    edge_weight = traitlets.Unicode("").tag(sync=True)
    max_steps = traitlets.Int(0).tag(sync=True)
    # "" | JSON list of [source, target] pairs
    edges = traitlets.Unicode("").tag(sync=True)
    min_path_share = traitlets.Float(0.01).tag(sync=True)
    chain_threshold = traitlets.Float(0.8).tag(sync=True)
    synonym_threshold = traitlets.Float(0.1).tag(sync=True)
    bridge_threshold = traitlets.Float(0.9).tag(sync=True)
    max_events = traitlets.Int(0).tag(sync=True)
    max_group_size = traitlets.Int(5).tag(sync=True)
    # "untestable_share", "testable", "mde" or a number as text
    target = traitlets.Unicode("untestable_share").tag(sync=True)
    step = traitlets.Int(-1).tag(sync=True)
    # "" | JSON list of events
    keep = traitlets.Unicode("").tag(sync=True)
    other = traitlets.Unicode("other").tag(sync=True)
    by = traitlets.Unicode("").tag(sync=True)
    # "" (not declared), "true" or "false"
    by_pre_exposure = traitlets.Unicode("").tag(sync=True)
    # 0 = no target effect (no "users needed" columns)
    target_delta = traitlets.Float(0.0).tag(sync=True)

    # ── view (browser-only, persisted) ─────────────────────────────────────────
    selected_step = traitlets.Int(-1).tag(sync=True)
    changed_only = traitlets.Bool(True).tag(sync=True)
    segment = traitlets.Unicode("").tag(sync=True)

    _defaults = _DEFAULTS
    _editable = (
        "screen",
        "min_path_share",
        "chain_threshold",
        "max_events",
        "target",
        "target_delta",
    )
    _view_defaults = VIEW_DEFAULTS
    _display_defaults: dict[str, Any] = {"height": 640, "sidebar_open": True}
    _tool = "get_ab_compression"
    _export_label = "A/B Compression"

    def __init__(self, eventstream, diff, assignment=None, state_file=None, **args):
        # The arguments as the caller wrote them are checked as the getter checks
        # them, before they become traitlets: a trait's sentinel (0 = no cap,
        # -1 = the chosen step, 0.0 = no target effect) must not turn an invalid
        # argument (max_events=0, step=-2, target_delta=0) into a valid one. An
        # argument of the other screen is the getter's error too (the widget drops
        # it only when the sidebar switches the screen).
        given = {
            name: value
            for name, value in args.items()
            if name in self._defaults and value is not _UNSET and value is not None
        }
        check_arguments(**given)
        super().__init__(
            eventstream, diff, assignment=assignment, state_file=state_file, **args
        )

    def _to_trait(self, name: str, value: Any) -> Any:
        if name in ("edges", "keep"):
            if value is None or value == "":
                return ""
            if isinstance(value, str):
                return value
            if name == "edges":
                return json.dumps([list(pair) for pair in value], default=str)
            return json.dumps([str(v) for v in value])
        if name in ("edge_weight", "by"):
            return value or ""
        if name in ("max_steps", "max_events"):
            return value or 0
        if name == "target_delta":
            return float(value) if value else 0.0
        if name == "step":
            return -1 if value is None else value
        if name == "target":
            return str(value)
        if name == "by_pre_exposure":
            return "" if value in (None, "") else ("true" if value else "false")
        return super()._to_trait(name, value)

    def _params(self, overrides: dict | None = None) -> dict:
        raw = {name: getattr(self, name) for name in self._defaults}
        params: dict[str, Any] = {
            "screen": raw["screen"],
            "horizon": raw["horizon"] or None,
            "path_col": raw["path_col"] or None,
            "edge_weight": _none_if_empty(raw["edge_weight"]),
            "max_steps": raw["max_steps"] or None,
            "edges": _json_list(raw["edges"]),
            "min_path_share": raw["min_path_share"],
            "chain_threshold": raw["chain_threshold"],
            "synonym_threshold": raw["synonym_threshold"],
            "bridge_threshold": raw["bridge_threshold"],
            "max_events": raw["max_events"] or None,
            "max_group_size": raw["max_group_size"],
            "target": raw["target"],
            "step": None if raw["step"] < 0 else raw["step"],
            "keep": _json_list(raw["keep"]),
            "other": raw["other"],
            "by": raw["by"] or None,
            "by_pre_exposure": {"": None, "true": True, "false": False}.get(
                raw["by_pre_exposure"], raw["by_pre_exposure"]
            ),
            "correction": raw["correction"],
            "ci_level": raw["ci_level"],
            "power": raw["power"],
            "min_support": raw["min_support"],
            "target_delta": raw["target_delta"] or None,
        }
        if params["target"] not in TARGETS:
            try:
                params["target"] = float(params["target"])
            except (TypeError, ValueError):
                pass  # the twin says what it takes
        if params["edges"] is not None:
            params["edges"] = [tuple(pair) for pair in params["edges"]]
        # the sidebar switches the screen: the other screen's arguments wait
        if params["screen"] == "transitions":
            params["max_steps"] = None
        else:
            params["edge_weight"] = None
            params["edges"] = None
        params.update(overrides or {})
        params["assignment"] = self._assignment
        return params

    def _twin(self, params: dict) -> pd.DataFrame:
        return self._eventstream.ab_compression_data(self._diff, **params)

    def _call_args(self, params: dict) -> str:
        """The arguments of the plan shown, as Python keyword arguments of
        ``ab_compression_data`` (those that differ from its defaults; ``step`` is
        the slider's): the export line reproduces this plan, not the default."""
        out = []
        for name, value in params.items():
            if name in ("step", "assignment"):
                continue
            default = _GETTER_DEFAULTS.get(name, inspect.Parameter.empty)
            if value is None and default in (None, inspect.Parameter.empty):
                continue
            if default is not inspect.Parameter.empty and _same(value, default):
                continue
            if name == "edges" and value is not None:
                value = [tuple(pair) for pair in value]
            out.append(f"{name}={value!r}")
        if params.get("assignment") is not None:
            out.insert(0, "assignment=assignment")
        return ", ".join(out)

    def _payload(self, frame: pd.DataFrame, params: dict, caught: list[str]) -> dict:
        meta = dict(frame.attrs or {})
        by = meta.get("by")
        path = meta["path"]
        mappings = meta["mappings"]
        chosen = meta["chosen_step"]
        homogeneity = meta.get("homogeneity")
        if isinstance(homogeneity, dict):
            homogeneity = [homogeneity]
        other = meta.get("other", "other")
        segments = []
        for key, maps in mappings.items():
            if by is not None:
                part = path[path[by].astype(str) == key]
                plan = frame[frame[by].astype(str) == key]
            else:
                part, plan = path, frame
            steps = [
                {field: row[field] for field in PATH_FIELDS if field in row}
                for row in part.to_dict("records")
            ]
            kept = {
                row["event"]: row["reason"]
                for row in plan.to_dict("records")
                if row["action"] == "keep"
            }
            share = dict(zip(plan["event"].astype(str), plan["path_share"]))
            events = sorted(maps[0]) if maps else []
            segments.append(
                {
                    "key": key,
                    "chosen_step": int(
                        chosen[key] if isinstance(chosen, dict) else chosen
                    ),
                    "path": steps,
                    "events": events,
                    "path_share": [share.get(e) for e in events],
                    "changes": label_changes(maps),
                    "kept_reason": kept,
                    "homogeneity": next(
                        (h for h in homogeneity or [] if str(h.get("segment")) == key),
                        None,
                    ),
                }
            )
        weight = meta.get("edge_weight")
        return jsonable(
            {
                "tool": self._tool,
                "status": meta.get("status", "ok"),
                "call_args": self._call_args(params),
                "screen": meta.get("screen"),
                "edge_weight": weight,
                "max_steps": meta.get("max_steps"),
                "unit": "per_path" if weight == "avg_per_path" else "pp",
                "target": meta.get("target"),
                "target_delta": meta.get("target_delta"),
                "other": other,
                "keep": meta.get("keep"),
                "by": by,
                "by_pre_exposure": meta.get("by_pre_exposure"),
                "min_path_share": meta.get("min_path_share"),
                "chain_threshold": meta.get("chain_threshold"),
                "synonym_threshold": meta.get("synonym_threshold"),
                "bridge_threshold": meta.get("bridge_threshold"),
                "max_events": meta.get("max_events"),
                "max_group_size": meta.get("max_group_size"),
                "min_support": meta.get("min_support"),
                "correction": meta.get("correction"),
                "ci_level": meta.get("ci_level"),
                "power": meta.get("power"),
                "population": meta.get("population"),
                "horizon": meta.get("horizon"),
                "path_col": meta.get("path_col"),
                "group1_paths": meta.get("group1_paths"),
                "group2_paths": meta.get("group2_paths"),
                "segments": segments,
                "warnings": list(dict.fromkeys(caught)),
            }
        )

    def _tool_ab_compression_data(self, params: dict):
        return self._compute_raw(self._params(params))

    #: See RetentioneeringWidget.compute_tools.
    compute_tools = {"ab_compression_data": _tool_ab_compression_data}
