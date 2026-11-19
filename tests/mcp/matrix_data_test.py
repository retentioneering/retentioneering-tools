"""Numerical and transport contracts for the two report-independent matrix tools."""

import asyncio
import json
from copy import deepcopy

import pandas as pd
import pytest
from anywidget import AnyWidget

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.mcp import ReportSession, tools
from retentioneering.mcp.server import _build_server


@pytest.fixture
def session(monkeypatch):
    df = pd.DataFrame(
        [
            ("u1", "s1", "mobile", "a"),
            ("u1", "s1", "mobile", "b"),
            ("u1", "s1", "mobile", "c"),
            ("u1", "s2", "mobile", "a"),
            ("u2", "s3", "desktop", "a"),
            ("u2", "s3", "desktop", "x"),
            ("u2", "s3", "desktop", "b"),
            ("u2", "s3", "desktop", "c"),
            ("u2", "s3", "desktop", "noise"),
        ],
        columns=["user_id", "session_id", "platform", "event"],
    )
    df["timestamp"] = pd.date_range("2025-01-01", periods=len(df), freq="1250ms")
    stream = Eventstream(
        df,
        schema={"path_cols": ["user_id", "session_id"], "segment_cols": ["platform"]},
    )
    session = ReportSession(stream)
    session.add_tab("Existing tab", {"payload": [1]}, [])
    session.update_base_stream(
        [{"type": "filter_events", "drop": {"event": ["noise"]}}]
    )
    active = session.active_stream
    data = active.df.copy(deep=True)
    tabs = deepcopy(session.pending_tabs)
    base = deepcopy(session.base_preprocessors)

    def unexpected(*args, **kwargs):
        pytest.fail("A data-only tool tried to create a widget or register a tab")

    monkeypatch.setattr(AnyWidget, "__init__", unexpected)
    monkeypatch.setattr(ReportSession, "add_tab", unexpected)
    yield session
    assert session.pending_tabs == tabs
    assert session.base_preprocessors == base
    assert session.active_stream is active
    pd.testing.assert_frame_equal(active.df, data)


def cell(matrix, row, col):
    return matrix["data"][matrix["index"].index(row)][matrix["columns"].index(col)]


def test_graph_counts_and_path_selection(session):
    result = tools.transition_graph_data(
        session, edge_weight="count", path_col="session_id", path_pattern="b->c"
    )
    (block,) = result["blocks"]
    matrix = block["matrix"]
    assert "noise" not in matrix["index"]
    assert cell(matrix, "path_start", "a") == 2  # excludes s2
    assert cell(matrix, "c", "a") == 0  # no cross-session transition
    assert cell(matrix, "a", "x") == 1  # keep events outside the match
    assert cell(matrix, "b", "c") == 2


@pytest.mark.parametrize("weight", ["time_median", "time_q95"])
def test_graph_times_are_seconds_or_null_including_diff(session, weight):
    result = tools.transition_graph_data(
        session, edge_weight=weight, diff=["platform", "mobile", "<REST>"]
    )
    (block,) = json.loads(json.dumps(result, allow_nan=False))["blocks"]
    assert cell(block["group1"], "a", "b") == 1.25
    assert cell(block["group2"], "a", "b") is None
    assert cell(block["diff"], "a", "b") is None
    assert cell(block["diff"], "b", "c") == 0


@pytest.mark.parametrize("compare", [False, True])
@pytest.mark.parametrize("pattern", [None, "a->.*->b->.*->path_end"])
def test_step_matrices_preserve_all_axes_and_group_order(session, pattern, compare):
    diff = [["s1"], ["s3"]] if compare else None
    kwargs = dict(max_steps=2, path_col="session_id", path_pattern=pattern, diff=diff)
    expected = session.active_stream.step_matrix_data(**kwargs)
    result = tools.step_matrix_data(session, **kwargs)
    blocks = json.loads(json.dumps(result, allow_nan=False))["blocks"]
    assert len(blocks) == (3 if pattern else 1)
    expected_groups = (
        dict(zip(("diff", "group1", "group2"), expected, strict=True))
        if compare
        else {"matrix": expected}
    )
    for name, matrices in expected_groups.items():
        for block, frame in zip(
            blocks, matrices if pattern else [matrices], strict=True
        ):
            assert block.keys() == expected_groups.keys()
            actual = block[name]
            assert actual["index"] == frame.index.tolist()
            assert actual["columns"] == frame.columns.tolist()
            assert actual["data"] == frame.to_numpy().tolist()
    if pattern:
        primary = "diff" if compare else "matrix"
        assert blocks[0][primary]["columns"] == [-2, -1, 0, 1, 2]
        assert blocks[-1][primary]["columns"] == [-2, -1, 0]
    if compare:
        step = 1 if pattern else 2
        assert cell(blocks[0]["group1"], "b", step) == 1
        assert cell(blocks[0]["group2"], "b", step) == 0
        assert cell(blocks[0]["diff"], "b", step) == 1


def test_anchor_and_local_preprocessing(session):
    result = tools.step_matrix_data(
        session,
        max_steps=1,
        anchor={"pattern": "a", "occurrence": "last"},
        local_preprocessors=[{"type": "filter_events", "drop": {"event": ["x"]}}],
    )
    (block,) = result["blocks"]
    assert "x" not in block["matrix"]["index"]
    assert cell(block["matrix"], "a", 0) == 1
    assert cell(block["matrix"], "b", 1) == 0.5


@pytest.mark.parametrize(
    ("tool", "params", "message"),
    [
        (tools.transition_graph_data, {"path_col": "unknown"}, "path_col"),
        (tools.step_matrix_data, {"anchor": "b", "path_pattern": "a"}, "anchor"),
        (tools.step_matrix_data, {"max_steps": 0}, "max_steps"),
        (tools.transition_graph_data, {"diff": ["missing", "a", "b"]}, "segment"),
    ],
)
def test_input_errors(session, tool, params, message):
    assert message in tool(session, **params)["error"]


@pytest.mark.parametrize("tool", [tools.transition_graph_data, tools.step_matrix_data])
def test_loader_and_preprocessing_errors(session, tool):
    assert "load_data" in tool(ReportSession(None))["error"]
    assert (
        "local_preprocessors:"
        in tool(session, local_preprocessors=[{"type": "typo"}])["error"]
    )


def test_mcp_discovery_and_calls(session):
    async def exercise():
        server = _build_server(session.active_stream, {})
        names = {tool.name for tool in await server.list_tools()}
        for name in ("transition_graph_data", "step_matrix_data"):
            assert name in names
            assert name in tools.describe_tool()["analysis_tools"]
            assert tools.describe_tool(name)["method"] == name
        graph = await server.call_tool(
            "transition_graph_data",
            {
                "diff": [["u1"], ["u2"]],
                "local_preprocessors": [
                    {"type": "filter_events", "drop": {"event": ["c"]}}
                ],
            },
        )
        assert graph.structured_content == json.loads(graph.content[0].text)
        (block,) = graph.structured_content["blocks"]
        assert "c" not in block["diff"]["index"]
        assert cell(block["diff"], "a", "b") == 0.5
        steps = await server.call_tool(
            "step_matrix_data", {"max_steps": 0, "anchor": "a"}
        )
        assert steps.structured_content == json.loads(steps.content[0].text)
        (block,) = steps.structured_content["blocks"]
        assert block["matrix"]["columns"] == [0]
        assert cell(block["matrix"], "a", 0) == 1

    asyncio.run(exercise())
