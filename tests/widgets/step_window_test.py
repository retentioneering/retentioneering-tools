"""`step_window` only slices columns `max_steps` already computed, so a window
wider than the computed depth has to deepen it instead of silently showing
fewer steps than asked for."""

import json

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.widgets.step_matrix import StepMatrixWidget
from retentioneering.widgets.step_sankey import StepSankeyWidget

WIDGETS = [StepMatrixWidget, StepSankeyWidget]


def _stream() -> Eventstream:
    """One 40-event path, deep enough for any window these tests ask for."""
    ts = pd.Timestamp("2024-01-01")
    rows = [
        {
            "user_id": "u1",
            "event": f"e{i % 4}",
            "timestamp": ts + pd.Timedelta(minutes=i),
        }
        for i in range(40)
    ]
    return Eventstream(pd.DataFrame(rows))


def _last_column(widget) -> int:
    return json.loads(widget.result)["matrices"][0]["columns"][-1]


def _count_computes(monkeypatch, widget_cls) -> list[int]:
    """The `max_steps` each `_compute_raw` call ran with, in order."""
    seen: list[int] = []
    original = widget_cls._compute_raw

    def spy(self, max_steps, *args, **kwargs):
        seen.append(max_steps)
        return original(self, max_steps, *args, **kwargs)

    monkeypatch.setattr(widget_cls, "_compute_raw", spy)
    return seen


@pytest.mark.parametrize("widget_cls", WIDGETS)
class TestStepWindowExpandsMaxSteps:
    def test__window_argument_deepens_max_steps(self, widget_cls) -> None:
        widget = widget_cls(_stream(), step_window=15)

        assert widget.error == ""
        assert widget.max_steps == 25
        assert _last_column(widget) == 25

    def test__window_argument_costs_one_pass(self, widget_cls, monkeypatch) -> None:
        """Taken into account immediately: the widened depth is what the first
        compute runs with, not a second pass after it."""
        seen = _count_computes(monkeypatch, widget_cls)

        widget_cls(_stream(), step_window=15)

        assert seen == [25]

    def test__explicit_max_steps_wide_enough_is_left_alone(self, widget_cls) -> None:
        widget = widget_cls(_stream(), max_steps=30, step_window=15)

        assert widget.max_steps == 30
        assert _last_column(widget) == 30

    def test__window_from_the_sidebar_deepens_and_recomputes(self, widget_cls) -> None:
        widget = widget_cls(_stream())
        assert widget.max_steps == 10

        widget.step_window = 15  # what the sidebar slider sets

        assert widget.error == ""
        assert widget.max_steps == 25
        assert _last_column(widget) == 25

    def test__narrowing_the_window_keeps_the_computed_depth(
        self, widget_cls, monkeypatch
    ) -> None:
        widget = widget_cls(_stream(), step_window=15)
        seen = _count_computes(monkeypatch, widget_cls)

        widget.step_window = 2

        assert widget.max_steps == 25
        assert seen == []  # narrowing is pure slicing, done in the browser

    def test__window_within_the_computed_depth_does_not_recompute(
        self, widget_cls, monkeypatch
    ) -> None:
        widget = widget_cls(_stream())
        seen = _count_computes(monkeypatch, widget_cls)

        widget.step_window = 10

        assert widget.max_steps == 10
        assert seen == []

    def test__the_widened_depth_is_what_gets_exported(self, widget_cls) -> None:
        """A static export has no kernel to deepen anything later, so the
        columns the window needs have to be in the exported result already."""
        widget = widget_cls(_stream(), step_window=15)

        data = widget._export_data()

        assert data["max_steps"] == 25
        assert data["step_window"] == 15
        assert data["result"]["matrices"][0]["columns"][-1] == 25
