"""``render_static()``: same payload as ``export_html()``, wrapped for a
notebook cell's own output (survives ``jupyter nbconvert`` without a kernel).
"""

import html as html_mod
import json
import re

import pandas as pd
import pytest
from IPython.core.display import HTML

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.widgets import _html_export
from retentioneering.widgets._html_export import _ROOT_MARGIN_PX


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


def _payload_from_iframe(rendered: HTML) -> dict:
    assert isinstance(rendered, HTML)
    m = re.match(
        r'<iframe srcdoc="(.*)" style="([^"]*)"></iframe>'
        r"<script>.*document\.currentScript\.previousElementSibling.*</script>$",
        rendered.data,
        re.DOTALL,
    )
    assert m, rendered.data
    page = html_mod.unescape(m.group(1))
    data_match = re.search(
        r"<script>window\.__HS_DATA__ = (.*?);</script>", page, flags=re.DOTALL
    )
    assert data_match
    resize_listener = "ResizeObserver" in page and "postMessage" in page
    assert resize_listener, "exported page should report its height to the parent"
    return json.loads(data_match.group(1)), m.group(2)


class TestRenderStatic:
    def test_transition_graph_matches_export_html(self, tmp_path):
        widget = _stream().transition_graph()
        out = tmp_path / "tg.html"
        widget.export_html(str(out), title="t")
        file_payload = re.search(
            r"<script>window\.__HS_DATA__ = (.*?);</script>",
            out.read_text(encoding="utf-8"),
            flags=re.DOTALL,
        )
        assert file_payload

        rendered = widget.render_static(title="t")
        data, style = _payload_from_iframe(rendered)

        assert json.loads(file_payload.group(1)) == data
        # The iframe's starting height is the widget's own height *plus* the
        # exported page's root margin, so the very first paint already
        # matches what the page's own ResizeObserver would report — without
        # this, every export starts short and visibly (if briefly) scrolls.
        assert f"height:{widget.height + 2 * _ROOT_MARGIN_PX}px" in style

    def test_custom_height_accepts_css_length(self):
        # A caller-supplied CSS length (vs. a plain int) is used verbatim —
        # only a widget's own pixel height gets the root-margin adjustment,
        # since a CSS length isn't a "how tall is the content" measurement.
        widget = _stream().transition_graph()
        rendered = widget.render_static(height="80vh")
        _, style = _payload_from_iframe(rendered)
        assert "height:80vh" in style

    def test_funnel_render_static(self):
        widget = _stream().funnel(steps=["A", "C"])
        data, _ = _payload_from_iframe(widget.render_static())
        assert data["widget_type"] == "funnel"
        assert data["steps"] == ["A", "C"]

    def test_step_matrix_render_static(self):
        widget = _stream().step_matrix()
        data, _ = _payload_from_iframe(widget.render_static())
        assert data["widget_type"] == "step_matrix"

    def test_step_sankey_render_static(self):
        widget = _stream().step_sankey()
        data, _ = _payload_from_iframe(widget.render_static())
        assert data["widget_type"] == "step_sankey"

    def test_cluster_analysis_render_static(self):
        widget = _stream().cluster_analysis()
        data, _ = _payload_from_iframe(widget.render_static())
        assert data["widget_type"] == "cluster_analysis"

    def test_segment_overview_render_static(self):
        widget = _stream().segment_overview(segment_col="user_id")
        data, _ = _payload_from_iframe(widget.render_static())
        assert data["widget_type"] == "segment_overview"
