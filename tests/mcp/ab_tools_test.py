"""Numbers-only MCP tools of the A/B toolkit."""

import inspect
import json

import pandas as pd
import pytest

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


def _ab_session(n: int = 400) -> ReportSession:
    rows = []
    for i in range(n):
        arm = "treatment" if i % 2 == 0 else "control"
        platform = "ios" if i % 3 else "web"
        day = pd.Timestamp("2024-01-01") + pd.Timedelta(days=i % 14)
        rows.append([f"u{i}", "visit", day, arm, platform])
        if i % 4 in (0, 1) or (arm == "treatment" and i % 10 == 2):
            rows.append(
                [f"u{i}", "order", day + pd.Timedelta(minutes=3), arm, platform]
            )
    df = pd.DataFrame(
        rows, columns=["user_id", "event", "timestamp", "arm", "platform"]
    )
    return ReportSession(Eventstream(df, {"segment_cols": ["arm", "platform"]}), {})


_METRICS = [
    {"name": "conversion", "metric": "has_event", "metric_args": {"event": "order"}},
    {"name": "orders", "metric": "event_count", "metric_args": {"event": "order"}},
]


class TestGetAbTest:
    def test__numbers_only_and_json_safe(self) -> None:
        session = _ab_session()
        result = tools.get_ab_test(session, ["arm", "treatment", "control"], _METRICS)
        json.dumps(result, allow_nan=False)
        assert session.pending_tabs == []
        assert [m["metric"] for m in result["metrics"]] == ["conversion", "orders"]
        conv = result["metrics"][0]
        assert conv["group1_paths"] == 200 and conv["group2_paths"] == 200
        assert conv["delta"] == pytest.approx(0.1)
        assert conv["delta_ci_low"] < conv["delta"] < conv["delta_ci_high"]
        assert conv["verdict"] == "increase"
        assert conv["test"] == "z_test"
        assert conv["correction_family"] == "metric=conversion"
        assert conv["alpha_used"] == pytest.approx(0.05)
        assert "strata" not in conv and "periods" not in conv
        assert result["excluded_paths"] == 0

    def test__strata_top_three_and_periods(self) -> None:
        session = _ab_session()
        result = tools.get_ab_test(
            session,
            ["arm", "treatment", "control"],
            _METRICS[:1],
            strata_col="platform",
            period="W",
        )
        json.dumps(result, allow_nan=False)
        conv = result["metrics"][0]
        assert conv["strata_count"] == 2
        assert len(conv["strata"]) == 2
        deltas = [abs(s["delta"]) for s in conv["strata"]]
        assert deltas == sorted(deltas, reverse=True)
        assert {s["stratum"] for s in conv["strata"]} == {"ios", "web"}
        # Holm over the two strata: alpha / 2 per comparison
        assert conv["strata_correction_family"] == "metric=conversion/strata"
        assert [s["alpha_used"] for s in conv["strata"]] == [pytest.approx(0.025)] * 2
        # a source column: the recipe cannot show it was fixed before exposure
        assert conv["strata_pre_exposure"] is False
        assert conv["strata_pre_exposure_source"] == "not_pre_exposure"
        assert conv["exploratory"] is True
        assert conv["strata_selection"] == "top 2 by |delta| of 2"
        assert all("stratum_vs_rest_p_value" in s for s in conv["strata"])
        assert any("strata_pre_exposure=True" in w for w in result["warnings"])
        assert conv["support_warning"] == ""
        assert conv["periods_correction_family"] == "metric=conversion/periods"
        assert [p["period"] for p in conv["periods"]] == [
            "2024-01-01/2024-01-07",
            "2024-01-08/2024-01-14",
        ]
        assert result["period_population"] == "itt"

    def test__declared_pre_exposure_strata(self) -> None:
        result = tools.get_ab_test(
            _ab_session(),
            ["arm", "treatment", "control"],
            _METRICS[:1],
            strata_col="platform",
            strata_pre_exposure=True,
        )
        conv = result["metrics"][0]
        assert conv["strata_pre_exposure"] is True
        assert conv["strata_pre_exposure_source"] == "declared"
        assert conv["exploratory"] is False
        assert result["warnings"] == []

    def test__errors_are_returned(self) -> None:
        session = _ab_session()
        assert "error" in tools.get_ab_test(session, ["arm", "a", "b"], _METRICS)
        assert "error" in tools.get_ab_test(
            session, ["arm", "treatment", "control"], [{"metric": "has_event_bulk"}]
        )
        assert "error" in tools.get_ab_test(
            session, ["arm", "treatment", "control"], _METRICS, correction="sidak"
        )
        assert "error" in tools.get_ab_test(
            ReportSession(None, None), ["arm"], _METRICS
        )

    def test__documented_as_an_analysis_tool(self) -> None:
        assert "get_ab_test" in tools.describe_tool("")["analysis_tools"]
        docs = tools.describe_tool("get_ab_test")
        assert docs["method"] == "get_ab_test"
        assert "verdict" in docs["docs"]
        assert "get_ab_test" in tools.playbook("ab_test")


class TestAbDocsInSync:
    """ADR-0009: the MCP tool, its server wrapper and the playbook say the same."""

    def test__get_ab_test_labels(self) -> None:
        from retentioneering.mcp import server

        server_src = inspect.getsource(server)
        for doc in (tools.get_ab_test.__doc__, server_src):
            for label in (
                "descriptive",
                "correction_family",
                "alpha_used",
                "strata_pre_exposure",
                "exploratory",
                "assignment=",
                "strata_selection",
                "stratum_vs_rest_p_value",
                "silent assigned users are not counted",
            ):
                assert label in doc, label
            assert "Holm-adjusted" not in doc
        playbook = json.dumps(tools.playbook("ab_test"))
        for label in ("alpha_used", "exploratory", "descriptive", "assignment="):
            assert label in playbook, label

    def test__sample_ratio_names_the_holm_family(self) -> None:
        from retentioneering.mcp import server

        assert "Holm across `by` levels" in tools.get_sample_ratio.__doc__
        assert "Holm across `by` levels" in inspect.getsource(server)

    def test__descriptive_period_rows(self) -> None:
        result = tools.get_ab_test(
            _ab_session(),
            ["arm", "treatment", "control"],
            _METRICS[:1],
            period="W",
            period_population="active",
        )
        json.dumps(result, allow_nan=False)
        assert {p["verdict"] for p in result["metrics"][0]["periods"]} == {
            "descriptive"
        }
