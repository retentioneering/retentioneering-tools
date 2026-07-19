"""GraphView plumbing: widget kwargs → traitlets → HTML export payload."""

import json
import re

import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.widgets import _html_export


@pytest.fixture(autouse=True)
def fake_bundle(tmp_path, monkeypatch):
    bundle = tmp_path / "widget-static.js"
    bundle.write_text("/* stub bundle */", encoding="utf-8")
    monkeypatch.setattr(_html_export, "_BUNDLE_PATH", bundle)


def _stream() -> Eventstream:
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 2, 2],
            "event": ["A", "B", "C", "A", "C"],
            "timestamp": pd.date_range("2024-01-01", periods=5, freq="1min"),
        }
    )
    return Eventstream(df)


VIEWS = [
    {
        "name": "Checkout",
        "focus": {"type": "node", "id": "A"},
        "edgeFilter": {"mode": "topk", "k": 2},
    }
]


class TestGraphViewPlumbing:
    def test_views_kwarg_and_named_initial_view(self):
        widget = _stream().transition_graph(views=VIEWS, view="Checkout")

        assert json.loads(widget.views) == VIEWS
        assert widget.view == "Checkout"

    def test_view_dict_kwarg(self):
        view = {"focus": {"type": "edge", "source": "A", "target": "B"}}
        widget = _stream().transition_graph(view=view)

        assert json.loads(widget.view) == view
        assert json.loads(widget.views) == []

    def test_single_dict_in_views_raises(self):
        # list(dict) would silently serialize the dict KEYS — must fail loudly
        with pytest.raises(TypeError, match="view="):
            _stream().transition_graph(views=VIEWS[0])

    def test_non_dict_view_raises(self):
        with pytest.raises(TypeError, match="view dict"):
            _stream().transition_graph(view=42)

    def test_defaults_empty(self):
        widget = _stream().transition_graph()

        assert widget.views == "[]"
        assert widget.view == ""

    def test_export_carries_views(self, tmp_path):
        widget = _stream().transition_graph(views=VIEWS, view="Checkout")
        out = tmp_path / "graph.html"
        widget.export_html(str(out), title="t")
        html = out.read_text(encoding="utf-8")

        payload = re.search(
            r"<script>window\.__HS_DATA__ = (.*?);</script>", html, flags=re.DOTALL
        )
        assert payload
        data = json.loads(payload.group(1))
        assert data["views"] == VIEWS
        assert data["view"] == "Checkout"
