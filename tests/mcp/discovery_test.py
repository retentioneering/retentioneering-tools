"""The discovery index must cover the executable processor surface."""

import inspect

import pandas as pd

from retentioneering import Eventstream
from retentioneering.mcp import tools
from retentioneering.mcp._prompts import _PREPROCESSOR_INDEX_EXCLUSIONS
from retentioneering.mcp._report_session import ReportSession
from retentioneering.ops import registered_ops


def test_index_covers_registered_preprocessors():
    index = tools.describe_tool("")
    listed = index["preprocessors"]

    assert listed == sorted(registered_ops().difference(_PREPROCESSOR_INDEX_EXCLUSIONS))
    assert set(listed).isdisjoint(index["analysis_tools"])


def test_index_exclusions_are_explicit_and_still_registered():
    assert set(_PREPROCESSOR_INDEX_EXCLUSIONS) == {"add_start_end_events"}
    assert set(_PREPROCESSOR_INDEX_EXCLUSIONS) <= registered_ops()
    assert all(reason.strip() for reason in _PREPROCESSOR_INDEX_EXCLUSIONS.values())
    assert "#174" in _PREPROCESSOR_INDEX_EXCLUSIONS["add_start_end_events"]


def test_every_indexed_preprocessor_has_canonical_docs():
    for name in tools.describe_tool("")["preprocessors"]:
        method = getattr(Eventstream, name)
        assert callable(method)
        doc = inspect.getdoc(method)
        assert doc, f"Missing Eventstream documentation for {name}"
        assert tools.describe_tool(name) == {"preprocessor": name, "docs": doc}


def test_rename_segment_levels_is_discoverable_and_executable():
    name = "rename_segment_levels"
    assert name in tools.describe_tool("")["preprocessors"]
    docs = tools.describe_tool(name)["docs"]
    assert "segment_col" in docs
    assert "mapping" in docs

    stream = Eventstream(
        pd.DataFrame(
            {
                "user_id": ["u1", "u2"],
                "event": ["view", "view"],
                "timestamp": pd.to_datetime(["2026-01-01", "2026-01-01"]),
                "plan": ["free", "paid"],
            }
        ),
        {"segment_cols": ["plan"]},
    )
    session = ReportSession(stream, {})
    result = tools.update_base_stream(
        session,
        [{"type": name, "segment_col": "plan", "mapping": {"free": "trial"}}],
    )

    assert result["status"] == "base stream updated"
    assert set(session.active_stream.df["plan"]) == {"trial", "paid"}
    assert set(stream.df["plan"]) == {"free", "paid"}


def test_boundary_exclusion_does_not_disable_explicit_lookup_or_execution():
    name = "add_start_end_events"
    assert name not in tools.describe_tool("")["preprocessors"]
    assert tools.describe_tool(name) == {
        "preprocessor": name,
        "docs": inspect.getdoc(Eventstream.add_start_end_events),
    }
    stream = Eventstream(
        pd.DataFrame(
            {"user_id": ["u1"], "event": ["view"], "timestamp": ["2026-01-01"]}
        )
    )
    session = ReportSession(stream, {})

    result = tools.update_base_stream(session, [{"type": name}])

    assert result["status"] == "base stream updated"
    assert list(session.active_stream.df["event"]) == ["path_start", "view", "path_end"]
    assert list(stream.df["event"]) == ["view"]
