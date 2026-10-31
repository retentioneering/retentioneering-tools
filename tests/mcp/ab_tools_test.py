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
        assert result["p_value"] == float(f"{result['p_value']:.3g}")
        assert session.pending_tabs == []

    def test__expected_shares_and_period(self) -> None:
        session = _session()
        result = tools.get_sample_ratio(
            session,
            "arm",
            expected_shares={"treatment": 0.6, "control": 0.4},
            period="D",
        )
        assert result["chi2"] == 0.0
        assert len(result["levels"]) == 2
        assert result["period"] == "D"
        assert [row["period"] for row in result["periods"]] == [
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
        assert conv["mde_alpha"] == pytest.approx(0.05)
        assert "strata" not in conv and "periods" not in conv
        assert result["unassigned_paths"] == 0

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
        assert [s["mde_alpha"] for s in conv["strata"]] == [pytest.approx(0.025)] * 2
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
                "mde_alpha",
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
        for label in ("mde_alpha", "exploratory", "descriptive", "assignment="):
            assert label in playbook, label

    def test__sample_ratio_names_the_holm_family(self) -> None:
        from retentioneering.mcp import server

        assert "Holm across the strata" in tools.get_sample_ratio.__doc__
        assert "Holm across strata" in inspect.getsource(server)

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


class TestReading:
    """The `reading` sentence reaches the agent on every row."""

    def test__get_ab_test_rows_carry_the_reading(self) -> None:
        session = _ab_session()
        direct = session.active_stream.get_ab_test(
            ("arm", "treatment", "control"), _METRICS
        )
        result = tools.get_ab_test(
            session,
            ["arm", "treatment", "control"],
            _METRICS,
            strata_col="platform",
            period="W",
        )
        json.dumps(result, allow_nan=False)
        for entry, expected in zip(result["metrics"], direct["reading"]):
            assert entry["reading"] == expected
            assert all(s["reading"] for s in entry["strata"])
            assert all(p["reading"] for p in entry["periods"])
        conv, orders = result["metrics"]
        assert conv["reading"].startswith(
            "over the whole observation period: interval ["
        )
        assert "excludes 0 (p " in conv["reading"]
        assert conv["reading"].endswith("name one decision metric")
        assert all("exploratory:" in s["reading"] for s in conv["strata"])
        assert conv["periods"][0]["reading"].startswith("calendar week 2024-01-01: ")
        assert conv["unit"] == "pp" and orders["unit"] == "value"
        for row in conv["strata"] + conv["periods"]:
            assert row["unit"] == "pp"
            assert row["p_value"] <= row["p_value_adjusted"]

    def test__sample_ratio_reading(self) -> None:
        result = tools.get_sample_ratio(_session(), "arm")
        assert result["reading"].startswith(
            "expected 50/50, observed 40/60 (control/treatment), p "
        )
        assert result["reading"].endswith(": no mismatch")

    def test__descriptions_and_playbook_ask_to_quote_it(self) -> None:
        from retentioneering.mcp import _prompts, server

        server_src = inspect.getsource(server)
        for doc in (
            tools.get_ab_test.__doc__,
            tools.get_sample_ratio.__doc__,
            server_src,
        ):
            assert "reading" in doc
        text = " ".join(str(tools.playbook("ab_test")).split())
        assert "quote its `reading` verbatim" in text
        for step in (
            "More paths for the next test",
            "A horizon declared for the next test",
            "A pre-declared stratum",
            "A cheaper metric",
        ):
            assert step in text, step
        # R3D-01 / R3D-09: no advice to extend a finished test; the decision
        # metric is asked for before the scorecard is shown
        assert "run longer or on more traffic" not in text and "rerun with" not in text
        assert "Before the first `get_ab_test` call, ask which metric decides" in text
        prompts = " ".join(
            inspect.getsource(_prompts).replace('",', "").replace('"', "").split()
        )
        assert "quote the row's reading" in prompts
        assert "a longer horizon" not in prompts
        assert "BEFORE the first get_ab_test call" in prompts


# ── wave 5: assignment table, horizon, scope, bootstrap, describe ────────────


def _itt_session(n: int = 400) -> ReportSession:
    """Users assigned over 20 days; every 10th user is silent (table only)."""
    rows, table = [], {}
    for i in range(n):
        arm = "treatment" if i % 2 == 0 else "control"
        at = pd.Timestamp("2024-01-01") + pd.Timedelta(hours=12 * (i % 40))
        table[f"u{i}"] = (arm, at)
        if i % 10 == 9:
            continue
        rows.append([f"u{i}", "visit", at + pd.Timedelta(hours=1), arm, "ios"])
        buys = i % 4 in (0, 1) or (arm == "treatment" and i % 10 == 2)
        if buys:
            rows.append([f"u{i}", "order", at + pd.Timedelta(days=i % 9), arm, "ios"])
            rows.append(
                [f"u{i}", "order", at + pd.Timedelta(days=i % 9, hours=1), arm, "ios"]
            )
    df = pd.DataFrame(
        rows, columns=["user_id", "event", "timestamp", "arm", "platform"]
    )
    df["value"] = [
        10.0 + (i % 7) if e == "order" else None for i, e in enumerate(df["event"])
    ]
    stream = Eventstream(
        df, {"segment_cols": ["arm", "platform"], "custom_cols": ["value"]}
    )
    assignment = pd.DataFrame.from_dict(
        table, orient="index", columns=["arm", "assigned_at"]
    )
    return ReportSession(stream, {}, assignment=assignment)


class TestAssignmentThroughMcp:
    """U10: serve(stream, assignment=) reaches the A/B tools."""

    def test_sample_ratio_counts_the_table(self) -> None:
        session = _itt_session()
        result = tools.get_sample_ratio(session)
        json.dumps(result, allow_nan=False)
        assert result["silent_paths"] == 40
        assert sum(row["paths"] for row in result["levels"]) == 400
        assert result["basis"].startswith("assignment table")
        # the stream's arm segment is checked against the table
        assert tools.get_sample_ratio(session, "arm")["multi_valued_paths"] == 0

    def test_itt_scorecard_horizon_and_scope(self) -> None:
        session = _itt_session()
        metrics = _METRICS + [
            {
                "name": "aov",
                "metric": "column_sum",
                "metric_args": {"column": "value"},
                "denominator": {
                    "metric": "event_count",
                    "metric_args": {"event": "order"},
                },
            }
        ]
        result = tools.get_ab_test(
            session, ["arm", "treatment", "control"], metrics, horizon="14D"
        )
        json.dumps(result, allow_nan=False)
        assert "error" not in result, result
        assert result["population"] == "assignment"
        assert result["immature_paths"] > 0
        direct = session.active_stream.get_ab_test(
            ("arm", "treatment", "control"),
            metrics,
            assignment=session.assignment,
            horizon="14D",
        )
        conv, orders, aov = result["metrics"]
        assert conv["group1_paths"] == direct["group1_paths"].iloc[0]
        assert conv["reading"] == direct["reading"].iloc[0]
        assert conv["reading"].startswith("within 14 days of assignment: ")
        assert conv["population"] == "assignment"
        assert not conv["conditional"] and aov["conditional"]
        assert aov["group1_support"] < aov["group1_paths"]
        for entry in result["metrics"]:
            assert "not corrected against each other" in entry["scope"]
            assert "p_value" in entry
        joint = tools.get_ab_test(
            session,
            ["arm", "treatment", "control"],
            _METRICS,
            correction_scope="all",
            test="auto",
            period="7D",
            period_basis="since_assignment",
        )
        assert "error" not in joint, joint
        assert joint["metrics"][0]["correction_family"] == "all_metrics"
        assert joint["metrics"][0]["scope"].startswith("holm across every row")
        assert joint["metrics"][0]["periods"][0]["period"] == "0-6"
        assert all(p["scope"] for p in joint["metrics"][0]["periods"])

    def test_without_a_table_horizon_is_an_error(self) -> None:
        result = tools.get_ab_test(
            _ab_session(), ["arm", "treatment", "control"], _METRICS, horizon="7D"
        )
        assert "error" in result and "horizon" in result["error"]
        # m7: the error says where MCP gets the table
        assert "serve(stream, assignment=table)" in result["error"]
        added = tools.add_ab_test(
            _ab_session(), "AB", ["arm", "treatment", "control"], _METRICS, horizon="7D"
        )
        assert "serve(stream, assignment=table)" in added["error"]

    def test_add_ab_test_uses_the_table(self) -> None:
        session = _itt_session()
        result = tools.add_ab_test(
            session, "AB", ["arm", "treatment", "control"], _METRICS, horizon="7D"
        )
        assert "error" not in result, result
        tab = session.pending_tabs[0]["data"]
        assert tab["result"]["overall"][0]["population"] == "assignment"
        assert tab["horizon"] == "7D"
        # R3C-06: who was counted and what was corrected, as get_ab_test says
        entry = result["metrics"][0]
        for key in ("population", "correction_family", "mde_alpha", "scope", "unit"):
            assert key in entry, key
        assert entry["population"] == "assignment"
        json.dumps(result, allow_nan=False)
        strata = tools.add_ab_test(
            _itt_session(),
            "AB",
            ["arm", "treatment", "control"],
            _METRICS,
            strata_col="platform",
        )
        entry = strata["metrics"][0]
        assert entry["exploratory"] is True
        assert entry["strata_pre_exposure"] is False
        assert entry["strata_pre_exposure_source"] == "not_pre_exposure"

    def test_bootstrap_contrast(self) -> None:
        session = _itt_session()
        result = tools.get_ab_bootstrap(
            session,
            ["arm", "treatment", "control"],
            _METRICS[:1],
            period="7D",
            contrast={"early_late": [["0-6"], ["7-13"]]},
            period_basis="since_assignment",
            n_bootstrap=200,
        )
        json.dumps(result, allow_nan=False)
        assert "error" not in result, result
        (row,) = result["rows"]
        assert row["contrast"] == "early_late" and row["unit"] == "pp"
        assert row["delta"] == pytest.approx(
            row["group1_value"] - row["group2_value"], abs=1e-5
        )
        assert row["delta_ci_low"] <= row["delta_ci_high"]
        assert 0 <= row["share_positive"] <= 1
        assert result["population"] == "assignment"
        assert "error" in tools.get_ab_bootstrap(
            session, ["arm", "treatment", "control"], _METRICS[:1], "7D", {}
        )

    def test_load_data_drops_the_table(self, tmp_path) -> None:
        session = _itt_session()
        path = tmp_path / "events.csv"
        session.active_stream.to_dataframe().to_csv(path, index=False)
        assert "error" not in tools.load_data(session, str(path))
        assert session.assignment is None


DIFF = ["arm", "treatment", "control"]


class TestGetAbTransitionsAndSteps:
    """The screens through MCP: counts and the top rows of the Python frame."""

    def test_transitions_are_the_python_rows(self) -> None:
        session = _ab_session()
        result = tools.get_ab_transitions(session, DIFF, top=3)
        json.dumps(result, allow_nan=False)
        assert "error" not in result, result
        assert session.pending_tabs == []
        frame = session.active_stream.get_ab_transitions(tuple(DIFF))
        assert result["n_rows"] == len(frame)
        assert result["n_tested"] == frame.attrs["n_tested"]
        assert result["n_increase"] == int((frame["verdict"] == "increase").sum())
        assert result["edge_weight"] == "unique_paths"
        assert result["population"] == "stream"
        assert len(result["top"]) == 3
        for row, (_, ref) in zip(result["top"], frame.head(3).iterrows()):
            assert (row["source"], row["target"]) == (ref["source"], ref["target"])
            assert row["delta"] == pytest.approx(ref["delta"], abs=1e-6)
            assert row["verdict"] == ref["verdict"]
            assert row["reading"] == ref["reading"]
            assert row["unit"] == "pp"
        visit_order = next(
            r for r in result["top"] if (r["source"], r["target"]) == ("visit", "order")
        )
        assert visit_order["verdict"] == "increase"
        assert "bh over" in result["scope"]

    def test_declared_edges_and_weights(self) -> None:
        session = _ab_session()
        result = tools.get_ab_transitions(
            session, DIFF, "avg_per_path", edges=[["visit", "order"]]
        )
        assert "error" not in result, result
        assert result["n_rows"] == 1 and result["n_tested"] == 1
        assert result["top"][0]["unit"] == "value"
        # a declared family, one tested row: neither "screen" nor "1 tested rows"
        assert result["scope"].startswith(
            "bh over the 1 tested row of this call (a declared family: confirmatory"
        )
        assert "screen" not in result["scope"]
        assert "declared family" in result["top"][0]["reading"]
        screen = tools.get_ab_transitions(session, DIFF)
        assert "exploratory screen" in screen["scope"]
        assert f"over the {screen['n_tested']} tested rows" in screen["scope"]
        ratio = tools.get_ab_transitions(session, DIFF, "proba_out")
        assert all(r["conditional"] for r in ratio["top"])
        assert "error" in tools.get_ab_transitions(session, DIFF, "count")

    def test_counts_add_up_to_the_family(self) -> None:
        # visit -> order is the one way into order: proba_in is 1 in both arms,
        # no p-value, so the row is untested, not inconclusive
        result = tools.get_ab_transitions(_itt_session(), DIFF, "proba_in", top=50)
        assert "error" not in result, result
        tested = result["n_increase"] + result["n_decrease"] + result["n_inconclusive"]
        assert tested == result["n_tested"]
        assert tested + result["n_untested"] == result["n_rows"]
        row = next(
            r for r in result["top"] if (r["source"], r["target"]) == ("visit", "order")
        )
        assert row["verdict"] == "insufficient_data" and row["p_value"] is None
        assert "constant in both arms" in row["reading"]

    def test_itt_with_horizon(self) -> None:
        result = tools.get_ab_transitions(_itt_session(), DIFF, horizon="7D")
        json.dumps(result, allow_nan=False)
        assert "error" not in result, result
        assert result["population"] == "assignment"
        assert result["horizon"] == "7D"
        assert result["unassigned_paths"] == 0
        assert "immature_paths" in result

    def test_steps(self) -> None:
        session = _ab_session()
        result = tools.get_ab_steps(session, DIFF, max_steps=3, top=50)
        json.dumps(result, allow_nan=False)
        assert "error" not in result, result
        frame = session.active_stream.get_ab_steps(tuple(DIFF), 3)
        assert result["n_tested"] == frame.attrs["n_tested"]
        assert result["max_steps"] == 3
        cells = {(r["event"], r["step"]): r for r in result["top"]}
        assert cells[("order", 2)]["verdict"] == "increase"
        assert all(r["unit"] == "pp" for r in result["top"])
        assert "error" in tools.get_ab_steps(session, ["arm", "x", "y"])
        for bad in (0, -3, 2.5, "5", True):
            assert "top" in tools.get_ab_steps(session, DIFF, top=bad)["error"]
            assert "top" in tools.get_ab_transitions(session, DIFF, top=bad)["error"]

    def test_describe_tool_names_the_mcp_result(self) -> None:
        out = tools.describe_tool("get_ab_transitions")
        assert out["method"] == "get_ab_transitions"
        assert "top rows" in out["mcp_tool"]
        assert "exploratory screen" in out["mcp_docs"]
        assert "get_ab_steps" in (tools.describe_tool.__doc__ or "")
        steps = tools.describe_tool("get_ab_steps")
        assert steps["method"] == "get_ab_steps" and "top cells" in steps["mcp_tool"]


@pytest.fixture(scope="module")
def northwind() -> ReportSession:
    """The bundled experiment with its assignment table (ITT)."""
    from retentioneering import datasets

    return ReportSession(
        datasets.load_ab_test(), {}, assignment=datasets.load_ab_test_assignment()
    )


class TestPowerTools:
    """The data a screen needs, through MCP: the curve and the nearest rows."""

    def test_transitions_power_is_the_python_frame(self, northwind) -> None:
        result = tools.get_ab_transitions_power(northwind, DIFF, top=50)
        json.dumps(result, allow_nan=False)
        assert "error" not in result, result
        frame = northwind.active_stream.get_ab_transitions_power(
            tuple(DIFF), assignment=northwind.assignment
        )
        screen = northwind.active_stream.get_ab_transitions(
            tuple(DIFF), assignment=northwind.assignment
        )
        now = frame[frame["scale"] == 1]
        assert result["n_rows"] == len(now) == len(screen)
        assert result["n_testable"] == screen.attrs["n_tested"]
        assert result["population"] == "assignment"
        curve = frame.attrs["curve"]
        assert [c["scale"] for c in result["curve"]] == [c["scale"] for c in curve]
        for sent, ref in zip(result["curve"], curve):
            assert sent["testable"] == ref["testable"]
            assert sent["mde_median"] == pytest.approx(ref["mde_median"], abs=1e-6)
        # the rows not testable now, nearest to the support rule first
        untestable = now[~now["testable"]]
        assert len(result["top"]) == len(untestable) > 0
        assert not any(r["testable"] for r in result["top"])
        reach = [r["scale_to_test"] for r in result["top"]]
        assert reach == sorted(reach)
        assert "Bonferroni" in result["level"] and "NEW test" in result["scope"]

    def test_steps_power_with_a_target(self, northwind) -> None:
        result = tools.get_ab_steps_power(
            northwind, DIFF, max_steps=5, scale=[1, 4], target_delta=0.01, top=5
        )
        json.dumps(result, allow_nan=False)
        assert "error" not in result, result
        frame = northwind.active_stream.get_ab_steps_power(
            tuple(DIFF),
            5,
            assignment=northwind.assignment,
            scale=(1, 4),
            target_delta=0.01,
        )
        now = frame[frame["scale"] == 1].set_index(["event", "step"])
        reached = int((now["scale_needed"] <= 1).sum())
        assert result["n_reaching_target"] == reached
        assert [c["scale"] for c in result["curve"]] == [1.0, 4.0]
        assert "share_reaching_target" in result["curve"][0]
        needed = [r["scale_needed"] for r in result["top"]]
        assert all(n is None or n > 1 for n in needed)
        assert [n for n in needed if n is not None] == sorted(
            n for n in needed if n is not None
        )
        for row in result["top"]:
            ref = now.loc[(row["event"], row["step"])]
            assert row["group1_paths_needed"] == int(ref["group1_paths_needed"])
            assert row["unit"] == "pp"
        assert result["max_steps"] == 5

    def test_errors_are_returned(self, northwind) -> None:
        for call in (
            lambda: tools.get_ab_transitions_power(northwind, DIFF, scale=[0]),
            lambda: tools.get_ab_steps_power(northwind, DIFF, target_delta=-1),
            lambda: tools.get_ab_steps_power(northwind, ["arm", "x", "y"]),
            lambda: tools.get_ab_transitions_power(northwind, DIFF, top=0),
        ):
            assert "error" in call()


class TestCompressionTool:
    """The plan through MCP: numbers before and after, the path to the chosen step,
    the changes with their reasons and a preprocessor step that applies it."""

    ARGS = {"keep": ["quick_add", "cart"], "max_events": 10}

    def test_the_plan_and_its_preprocessor(self, northwind) -> None:
        result = tools.get_ab_compression(northwind, DIFF, top=3, **self.ARGS)
        json.dumps(result, allow_nan=False)
        assert "error" not in result, result
        plan = northwind.active_stream.get_ab_compression(
            tuple(DIFF), assignment=northwind.assignment, **self.ARGS
        )
        (segment,) = result["segments"]
        chosen = plan.attrs["chosen_step"]
        path = plan.attrs["path"].set_index("step")
        assert segment["chosen_step"] == chosen > 0
        assert segment["steps"] == path.index.max()
        assert segment["before"]["testable"] == path.loc[0, "testable"]
        assert segment["after"]["cells"] == path.loc[chosen, "cells"]
        assert [r["step"] for r in segment["path"]] == list(
            range(min(chosen + 1, segment["steps"]) + 1)
        )
        changed = plan[plan["action"] != "keep"]
        assert segment["n_changed"] == len(changed) and len(segment["changes"]) == 3
        assert all(r["reason"] for r in segment["changes"])
        assert result["arm_blind"].startswith("the plan reads the pooled paths")
        assert f"step {chosen} of" in segment["reading"]
        # the step applies the plan: the screen on it has the plan's numbers
        step = segment["preprocessor"]
        assert step["type"] == "compress_events"
        assert {r["event"] for r in step["plan"]} == set(changed["event"])
        screen = tools.get_ab_transitions(northwind, DIFF, local_preprocessors=[step])
        assert screen["n_rows"] == segment["after"]["cells"]
        assert screen["n_tested"] == segment["after"]["testable"]
        direct = northwind.active_stream.compress_events(plan).get_ab_transitions(
            tuple(DIFF), assignment=northwind.assignment
        )
        assert screen["n_rows"] == len(direct)

    def test_an_empty_plan_says_so(self, northwind) -> None:
        result = tools.get_ab_compression(northwind, DIFF, "steps")
        assert "error" not in result, result
        (segment,) = result["segments"]
        assert segment["chosen_step"] == 0
        assert segment["preprocessor"]["plan"] == []
        assert "stays as it is" in segment["reading"]
        assert segment["before"] == segment["after"]

    def test_segments(self, northwind) -> None:
        diff = ["exp_group", "treatment", "control"]
        result = tools.get_ab_compression(
            northwind, diff, "steps", by="platform", by_pre_exposure=True, top=2
        )
        json.dumps(result, allow_nan=False)
        assert "error" not in result, result
        plan = northwind.active_stream.get_ab_compression(
            tuple(diff), "steps", by="platform", by_pre_exposure=True
        )
        keys = [s["segment"] for s in result["segments"]]
        assert keys == list(plan.attrs["mappings"]) and len(keys) > 1
        for segment in result["segments"]:
            assert (
                segment["chosen_step"] == plan.attrs["chosen_step"][segment["segment"]]
            )
            assert segment["homogeneity"]["effective_vocabulary"] > 0

    def test_errors_are_returned(self, northwind) -> None:
        for kwargs in (
            {"max_steps": 5},
            {"screen": "graph"},
            {"keep": ["nope"]},
            {"top": 0},
            {"local_preprocessors": [{"type": "nope"}]},
        ):
            assert "error" in tools.get_ab_compression(northwind, DIFF, **kwargs)

    def test_documented(self) -> None:
        index = tools.describe_tool("")
        assert "compress_events" in index["preprocessors"]
        for name in (
            "get_ab_transitions_power",
            "get_ab_steps_power",
            "get_ab_compression",
        ):
            assert name in index["analysis_tools"]
            out = tools.describe_tool(name)
            assert out["method"] == name and "mcp_tool" in out
            assert name in (tools.describe_tool.__doc__ or "")
        assert "preprocessor" in tools.describe_tool("compress_events")
        assert (
            "compress_events" in tools.describe_tool("get_ab_compression")["mcp_tool"]
        )


class TestDescribeForExperiments:
    def test_levels_values_constant_flags_and_table(self) -> None:
        session = _itt_session()
        result = tools.describe(session, {})
        json.dumps(result)
        assert result["segment_levels"]["arm"] == ["control", "treatment"]
        assert result["segments_constant_per_path"]["arm"] == ["user_id"]
        assert result["value_columns"] == ["value"]
        assert result["assignment"]["paths"] == 400
        assert result["assignment"]["arms"] == {"control": 200, "treatment": 200}
        assert result["assignment"]["has_assigned_at"] is True
        assert "assignment" not in tools.describe(_ab_session(), {})

    def test_metrics_topic_lists_the_registry(self) -> None:
        from retentioneering.metrics.metric_builder import VALID_METRICS

        docs = tools.describe_tool("metrics")["docs"]
        for metric in VALID_METRICS:
            assert metric in docs, metric
        assert "metric_args" in docs

    def test_python_only_parameters_are_named(self) -> None:
        out = tools.describe_tool("get_ab_test")
        note = out["through_mcp"]
        for name in ("assignment", "n_bootstrap", "min_support", "power"):
            assert name in note, name
        for name in ("horizon", "period_basis", "correction_scope"):
            assert name not in note.split("available: ")[1].split(".")[0], name
        boot = tools.describe_tool("get_ab_bootstrap")
        assert boot["method"] == "get_ab_bootstrap"
        # m7: the MCP tool returns a contrast summary, not the replicate table
        assert "contrast summary" in boot["mcp_tool"]
        assert "period and contrast are required" in boot["mcp_tool"]
        assert "ci_level" in boot["mcp_docs"] and "share_positive" in boot["mcp_docs"]
        assert "mcp_tool" not in out


class TestPValues:
    """p-values keep three significant digits, however small (eight fixed decimals
    turned 4.28e-09 into 0.0)."""

    def test_json_p_value(self) -> None:
        assert tools._json_p_value(4.275451e-09) == 4.28e-09
        assert tools._json_p_value(2.5652706e-08) == 2.57e-08
        assert tools._json_p_value(0.4791) == 0.479
        assert tools._json_p_value(1.0) == 1.0
        assert tools._json_p_value(0.0) == 0.0
        assert tools._json_p_value(float("nan")) is None
        assert tools._json_p_value(None) is None

    def test_a_small_p_value_survives_both_tools(self) -> None:
        from retentioneering import datasets

        stream = datasets.load_ab_test()
        table = datasets.load_ab_test_assignment()
        session = ReportSession(stream, {}, assignment=table)
        diff = ["exp_group", "treatment", "control"]
        frame = stream.get_ab_transitions(tuple(diff), assignment=table)
        ref = frame.set_index(["source", "target"]).loc[("checkout_start", "home")]
        result = tools.get_ab_transitions(session, diff, top=20)
        row = next(
            r
            for r in result["top"]
            if (r["source"], r["target"]) == ("checkout_start", "home")
        )
        assert 0 < ref["p_value"] < 1e-8
        assert row["p_value"] == float(f"{ref['p_value']:.3g}")
        assert row["p_value_adjusted"] == float(f"{ref['p_value_adjusted']:.3g}")
        # the scorecard's p-values go through the same formatter
        metric = {
            "name": "abandon",
            "metric": "matches_pattern",
            "metric_args": {"pattern": "checkout_start->home"},
        }
        card = tools.get_ab_test(session, diff, [metric])
        assert card["metrics"][0]["p_value"] == row["p_value"]


class TestServerWrappersInSync:
    """ADR-0009: each A/B server wrapper takes the tool's parameters."""

    def test_signatures(self) -> None:
        from retentioneering.mcp.server import _build_server

        server = _build_server(None, {})
        registered = {t.name: t for t in server._tool_manager.list_tools()}
        for name in (
            "get_sample_ratio",
            "get_ab_test",
            "add_ab_test",
            "get_ab_bootstrap",
            "get_ab_transitions",
            "get_ab_steps",
            "get_ab_transitions_power",
            "get_ab_steps_power",
            "get_ab_compression",
        ):
            tool_params = list(inspect.signature(getattr(tools, name)).parameters)[1:]
            schema = registered[name].parameters["properties"]
            assert list(schema) == tool_params, name


class TestStepToCode:
    """m8: the exported code keeps every add_segment argument."""

    def test_pre_exposure_survives_the_export(self) -> None:
        from retentioneering.mcp._agent_logic import _step_to_code

        code = _step_to_code(
            {
                "type": "add_segment",
                "name": "entry",
                "rules": [{"segment": "x", "filter": "length > 3"}],
                "pre_exposure": True,
            }
        )
        assert code.startswith("stream.add_segment(name='entry', rules=")
        assert code.endswith("pre_exposure=True)")
        plain = _step_to_code({"type": "add_segment", "name": "g", "sql": "SELECT 1"})
        assert plain == "stream.add_segment(name='g', sql='SELECT 1')"
