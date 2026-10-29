"""Numbers-only MCP tools of the A/B toolkit."""

import json

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.mcp import tools
from retentioneering.mcp._report_session import ReportSession


def _session(n_t: int = 60, n_c: int = 40) -> ReportSession:
    rows = []
    for i in range(n_t + n_c):
        arm = "treatment" if i < n_t else "control"
        day = pd.Timestamp("2024-01-01") + pd.Timedelta(days=i % 2)
        rows += [
            [f"u{i}", "visit", day, arm],
            [
                f"u{i}",
                "order" if i % 3 == 0 else "view",
                day + pd.Timedelta(minutes=1),
                arm,
            ],
        ]
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm"])
    return ReportSession(Eventstream(df, {"segment_cols": ["arm"]}), {})


class TestGetSampleRatio:
    def test__numbers_only_and_json_safe(self) -> None:
        session = _session()
        result = tools.get_sample_ratio(session, "arm")
        json.dumps(result, allow_nan=False)
        assert result["mismatch"] is False
        assert result["dof"] == 1
        assert {row["level"]: row["paths"] for row in result["levels"]} == {
            "control": 40,
            "treatment": 60,
        }
        assert result["p_value"] == round(result["p_value"], 8)
        assert session.pending_tabs == []

    def test__expected_shares_and_by(self) -> None:
        session = _session()
        result = tools.get_sample_ratio(
            session, "arm", expected_shares={"treatment": 0.6, "control": 0.4}, by="day"
        )
        assert result["chi2"] == 0.0
        assert len(result["levels"]) == 2
        assert [row["by"] for row in result["by_levels"]] == [
            "2024-01-01",
            "2024-01-02",
        ]
        json.dumps(result, allow_nan=False)

    def test__errors_are_returned(self) -> None:
        session = _session()
        assert "error" in tools.get_sample_ratio(session, "event")
        assert "error" in tools.get_sample_ratio(
            session, "arm", local_preprocessors=[{"type": "nope"}]
        )
        assert "error" in tools.get_sample_ratio(ReportSession(None, None), "arm")

    def test__documented_as_an_analysis_tool(self) -> None:
        assert "get_sample_ratio" in tools.describe_tool("")["analysis_tools"]
        docs = tools.describe_tool("get_sample_ratio")
        assert docs["method"] == "get_sample_ratio"
        assert "mismatch" in docs["docs"]
