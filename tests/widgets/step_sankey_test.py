import json

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.widgets.step_sankey import StepSankeyWidget


class TestStepSankeyWidgetAnchorEvent:
    """The frontend lays a Step Sankey out from pattern tokens, so in anchor
    mode the result has to name the event the anchor centres on — otherwise
    the single block is drawn as if it started at `path_start`."""

    @staticmethod
    def _stream() -> Eventstream:
        rows = []
        ts = pd.Timestamp("2024-01-01")
        for i, event in enumerate(["cart", "x", "cart", "y", "pay"]):
            rows.append(
                {
                    "user_id": "u1",
                    "event": event,
                    "timestamp": ts + pd.Timedelta(minutes=i),
                }
            )
        return Eventstream(pd.DataFrame(rows))

    @staticmethod
    def _anchor_event(widget):
        assert widget.error == ""
        return json.loads(widget.result)["anchor_event"]

    @pytest.mark.parametrize(
        "anchor, expected",
        [
            ("cart", "cart"),
            ({"pattern": "cart", "occurrence": "last"}, "cart"),
            ({"pattern": "cart->.*->pay"}, "pay"),
            ({"pattern": "cart->.*->pay", "at": "start"}, "cart"),
            ({"pattern": "x->cart->y", "at": 1}, "cart"),
        ],
    )
    def test__names_the_anchored_event(self, anchor, expected) -> None:
        widget = StepSankeyWidget(self._stream(), anchor=anchor)

        assert self._anchor_event(widget) == expected

    def test__offset_has_no_fixed_event(self) -> None:
        widget = StepSankeyWidget(self._stream(), anchor={"pattern": "x", "offset": 1})

        assert self._anchor_event(widget) == ""

    def test__pattern_mode_has_no_anchor_event(self) -> None:
        widget = StepSankeyWidget(self._stream(), path_pattern="cart->.*->pay")

        assert self._anchor_event(widget) is None

    def test__a_pattern_typed_in_the_sidebar_drops_the_anchor_event(self) -> None:
        widget = StepSankeyWidget(self._stream(), anchor="cart")
        assert self._anchor_event(widget) == "cart"

        widget.path_pattern = "cart->.*->pay"

        assert self._anchor_event(widget) is None
