"""``transition_graph(diff=..., ab_transitions=frame)``: the significance overlay.

The frame of ``get_ab_transitions`` is validated against the graph's ``diff``,
``edge_weight`` and ``path_col`` and shipped as the ``ab_transitions`` traitlet; the
static export carries it, ``state_file`` does not persist it (it is data).
"""

import json
import re
import html as html_mod

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.widgets import _html_export

DIFF = ("arm", "A", "B")


@pytest.fixture(autouse=True)
def fake_bundle(tmp_path, monkeypatch):
    bundle = tmp_path / "widget-static.js"
    bundle.write_text("/* stub bundle */", encoding="utf-8")
    monkeypatch.setattr(_html_export, "_BUNDLE_PATH", bundle)


def _stream() -> Eventstream:
    rng = np.random.default_rng(0)
    rows = []
    for user in range(200):
        arm = "A" if user % 2 else "B"
        events = ["home", "catalog"]
        if arm == "A" and rng.random() < 0.6:
            events += ["quick_add", "cart"]
        elif rng.random() < 0.3:
            events += ["add_to_cart", "cart"]
        for i, event in enumerate(events):
            rows.append(
                (user, event, pd.Timestamp("2024-01-01") + pd.Timedelta(i, "min"), arm)
            )
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm"])
    return Eventstream(df, {"segment_cols": ["arm"]})


@pytest.fixture(scope="module")
def stream() -> Eventstream:
    return _stream()


@pytest.fixture(scope="module")
def frame(stream) -> pd.DataFrame:
    return stream.get_ab_transitions(DIFF)


def test_frame_becomes_the_overlay_payload(stream, frame) -> None:
    widget = stream.transition_graph(diff=DIFF, ab_transitions=frame)
    assert widget.error == ""
    assert widget.edge_weight == "unique_paths"  # the frame's weight by default
    payload = json.loads(widget.ab_transitions)
    assert payload["diff"] == list(DIFF)
    assert payload["n_tested"] == frame.attrs["n_tested"]
    edges = {(e["source"], e["target"]): e for e in payload["edges"]}
    quick = edges[("catalog", "quick_add")]
    assert quick["verdict"] == "increase"
    assert quick["p_adjusted"] is not None and quick["ci_low"] > 0
    # NaN reaches the browser as null
    untested = [e for e in payload["edges"] if e["verdict"] == "insufficient_data"]
    assert all(e["p_adjusted"] is None for e in untested)


def test_no_overlay_without_the_frame(stream) -> None:
    assert stream.transition_graph(diff=DIFF).ab_transitions == ""


def test_mismatches_raise(stream, frame) -> None:
    with pytest.raises(InvalidParameterError, match="diff"):
        stream.transition_graph(diff=("arm", "B", "A"), ab_transitions=frame)
    with pytest.raises(InvalidParameterError, match="diff"):
        stream.transition_graph(ab_transitions=frame)
    with pytest.raises(InvalidParameterError, match="edge_weight"):
        stream.transition_graph(
            diff=DIFF, edge_weight="proba_out", ab_transitions=frame
        )
    with pytest.raises(InvalidParameterError, match="ab_transitions"):
        stream.transition_graph(
            diff=DIFF, ab_transitions=frame.drop(columns=["p_value_adjusted"])
        )
    with pytest.raises(InvalidParameterError, match="ab_transitions"):
        stream.transition_graph(diff=DIFF, ab_transitions=frame.to_dict("records"))


def test_path_col_must_match(frame) -> None:
    df = _stream().to_dataframe()
    df["session_id"] = df["user_id"].astype(str) + "_1"
    sessions = Eventstream(
        df, {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm"]}
    )
    with pytest.raises(InvalidParameterError, match="path_col"):
        sessions.transition_graph(
            diff=DIFF, path_col="session_id", ab_transitions=frame
        )


def test_filtered_frame_is_accepted(stream, frame) -> None:
    top = frame[frame["verdict"] != "insufficient_data"]
    widget = stream.transition_graph(diff=DIFF, ab_transitions=top)
    assert len(json.loads(widget.ab_transitions)["edges"]) == len(top)


def test_static_export_carries_the_overlay(stream, frame, tmp_path) -> None:
    widget = stream.transition_graph(diff=DIFF, ab_transitions=frame)
    data = widget._export_data()
    assert data["ab_transitions"]["edge_weight"] == "unique_paths"
    out = tmp_path / "graph.html"
    widget.export_html(str(out))
    assert '"ab_transitions"' in out.read_text(encoding="utf-8")
    rendered = widget.render_static()
    srcdoc = re.match(r'<iframe srcdoc="(.*?)"', rendered.data, flags=re.DOTALL)
    assert srcdoc and "ab_transitions" in html_mod.unescape(srcdoc.group(1))


def test_state_file_does_not_persist_the_overlay(stream, frame, tmp_path) -> None:
    path = tmp_path / "graph.json"
    stream.transition_graph(diff=DIFF, ab_transitions=frame, state_file=str(path))
    state = json.loads(path.read_text())["state"]
    assert "ab_transitions" not in state
    assert state["edge_weight"] == "unique_paths"
