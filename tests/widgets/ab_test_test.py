"""ABTestWidget: traitlets, the numbers against get_ab_test, strict JSON, state, export."""

import inspect
import json
import math

import numpy as np
import pandas as pd
import pytest

from retentioneering.datasets.ab_test import load_ab_test, load_ab_test_assignment
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidMetricConfigError
from retentioneering.widgets.ab_test import ABTestWidget

DIFF = ("grp", "t", "c")
METRICS = [
    {"name": "conversion", "metric": "has_event", "metric_args": {"event": "buy"}},
    {"name": "events", "metric": "length"},
    {"metric": "duration"},
]


def _strict(text: str):
    return json.loads(
        text, parse_constant=lambda c: pytest.fail(f"bare {c} in widget JSON")
    )


def _stream(seed: int = 0) -> Eventstream:
    rng = np.random.default_rng(seed)
    rows = []
    for g, p in (("t", 0.6), ("c", 0.45)):
        for i in range(80):
            t = pd.Timestamp("2026-01-01") + pd.Timedelta(days=int(rng.integers(0, 20)))
            platform = "ios" if i % 2 else "web"
            rows.append([f"{g}{i}", "home", t, g, platform])
            if rng.random() < p:
                rows.append([f"{g}{i}", "buy", t + pd.Timedelta("5min"), g, platform])
    df = pd.DataFrame(
        rows, columns=["user_id", "event", "timestamp", "grp", "platform"]
    )
    return Eventstream(df, {"segment_cols": ["grp", "platform"]})


def _expected_cell(value):
    """What a get_ab_test cell becomes in the widget's JSON."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return value.isoformat()
    if isinstance(value, (float, np.floating)):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    return value


def _assert_same_cells(rows: list[dict], frame: pd.DataFrame) -> None:
    assert len(rows) == len(frame)
    assert all(list(row) == list(frame.columns) for row in rows)
    for row, (_, expected) in zip(rows, frame.iterrows()):
        for column in frame.columns:
            assert row[column] == _expected_cell(expected[column]), (
                row["metric"],
                column,
            )


# ── the widget's numbers are get_ab_test's ("together" test) ──────────────────

NORTHWIND_DIFF = ("exp_group", "treatment", "control")
NORTHWIND_METRICS = [
    {
        "name": "conversion",
        "metric": "has_event",
        "metric_args": {"event": "order_success"},
    },
    {
        "name": "revenue_per_user",
        "metric": "column_sum",
        "metric_args": {"column": "order_value"},
    },
    {
        "name": "aov",
        "metric": "column_sum",
        "metric_args": {"column": "order_value"},
        "denominator": {
            "metric": "event_count",
            "metric_args": {"event": "order_success"},
        },
    },
    {"metric": "duration"},
]


@pytest.fixture(scope="module")
def northwind():
    stream = load_ab_test()
    assignment = load_ab_test_assignment()
    widget = stream.ab_test(
        NORTHWIND_DIFF,
        NORTHWIND_METRICS,
        assignment=assignment,
        strata_col="platform",
        strata_pre_exposure=True,
        period="W",
        n_bootstrap=200,
    )
    return stream, assignment, widget, _strict(widget.result)


def _get_ab_test(stream, assignment, **kwargs):
    return stream.get_ab_test(
        NORTHWIND_DIFF,
        NORTHWIND_METRICS,
        assignment=assignment,
        n_bootstrap=200,
        **kwargs,
    )


class TestNumbersEqualGetAbTest:
    def test__overall_rows_equal_get_ab_test_cell_by_cell(self, northwind) -> None:
        stream, assignment, widget, result = northwind
        assert widget.error == ""
        _assert_same_cells(result["overall"], _get_ab_test(stream, assignment))

    def test__strata_rows_equal_get_ab_test_cell_by_cell(self, northwind) -> None:
        stream, assignment, _, result = northwind
        expected = _get_ab_test(
            stream, assignment, strata_col="platform", strata_pre_exposure=True
        )
        _assert_same_cells(result["strata"], expected)

    def test__period_rows_equal_get_ab_test_cell_by_cell(self, northwind) -> None:
        stream, assignment, _, result = northwind
        expected = _get_ab_test(stream, assignment, period="W")
        _assert_same_cells(result["periods"], expected)
        # timestamps travel as ISO strings, completeness as a bool
        assert result["periods"][0]["period_start"].startswith("2026-")
        assert result["periods"][-1]["period_complete"] is False

    def test__metric_kinds_and_units(self, northwind) -> None:
        *_, result = northwind
        assert result["metric_kinds"] == {
            "conversion": "binary",
            "revenue_per_user": "count",
            "aov": "ratio",
            "duration": "value",
        }
        assert result["metric_units"]["duration"] == "seconds"
        assert result["metric_units"]["conversion"] == ""
        assert (result["group1_label"], result["group2_label"]) == (
            "treatment",
            "control",
        )

    def test__dispatch_compute_returns_the_same_numbers(self, northwind) -> None:
        *_, widget, result = northwind
        assert widget.dispatch_compute("ab_test_data", {}) == result


class TestHeadlessTwin:
    def test__ab_test_data_is_get_ab_test(self) -> None:
        stream = _stream()
        pd.testing.assert_frame_equal(
            stream.ab_test_data(DIFF, METRICS, ci_level=0.9),
            stream.get_ab_test(DIFF, METRICS, ci_level=0.9),
        )

    def test__signatures_match(self) -> None:
        twin = inspect.signature(Eventstream.ab_test_data).parameters
        getter = inspect.signature(Eventstream.get_ab_test).parameters
        assert list(twin) == list(getter)
        assert all(twin[n].default == getter[n].default for n in getter)
        widget = inspect.signature(Eventstream.ab_test).parameters
        assert list(widget)[: len(getter)] == list(getter)
        assert list(widget)[len(getter) :] == ["height", "sidebar_open", "state_file"]

    def test__docstrings_are_built_from_get_ab_test(self) -> None:
        getter = inspect.getdoc(Eventstream.get_ab_test)
        twin = inspect.getdoc(Eventstream.ab_test_data)
        widget = inspect.getdoc(Eventstream.ab_test)
        assert twin.splitlines()[0] == getter.splitlines()[0]
        assert "Documented alias of `get_ab_test`" in twin
        params = getter.split("Parameters\n----------\n")[1].split("\n\nReturns")[0]
        assert params in twin and params in widget
        assert "height : int, default 560" in widget
        assert "Returns\n-------" in twin


# ── traitlets, errors, NaN ───────────────────────────────────────────────────


class TestTraitlets:
    def test__arguments_land_in_traitlets(self) -> None:
        w = _stream().ab_test(
            DIFF,
            METRICS,
            strata_col="platform",
            period=pd.Timedelta(days=7),
            horizon=None,
            correction="bh",
            ci_level=0.9,
            n_bootstrap="auto",
        )
        assert json.loads(w.diff) == list(DIFF)
        assert json.loads(w.metrics) == METRICS
        assert (w.strata_col, w.period, w.correction) == ("platform", "7D", "bh")
        assert w.ci_level == 0.9 and w.n_bootstrap == "auto"
        assert w.strata_pre_exposure is None and w.horizon == ""
        assert w.height == 560 and w.sidebar_open is True

    def test__defaults_are_get_ab_test_defaults(self) -> None:
        w = ABTestWidget(_stream(), diff=DIFF, metrics=METRICS)
        params = w._params()
        sig = inspect.signature(Eventstream.get_ab_test).parameters
        for name in ("test", "correction", "ci_level", "power", "min_support"):
            assert params[name] == sig[name].default, name
        assert params["n_bootstrap"] == 1000

    def test__a_traitlet_change_recomputes(self) -> None:
        w = _stream().ab_test(DIFF, METRICS)
        assert _strict(w.result)["strata"] is None
        w.strata_col = "platform"
        result = _strict(w.result)
        assert {r["stratum"] for r in result["strata"]} == {"ios", "web"}
        w.ci_level = 0.99
        assert _strict(w.result)["ci_level"] == 0.99
        w.metrics = json.dumps(METRICS[:1])
        assert [r["metric"] for r in _strict(w.result)["overall"]] == ["conversion"]
        w.period = "W"
        assert _strict(w.result)["periods"]

    def test__nan_is_null_in_strict_json(self) -> None:
        # nobody in group 2 buys and few in group 1: no test, NaN p-value
        stream = _stream()
        df = stream.to_dataframe()
        df = df[~((df["grp"] == "c") & (df["event"] == "buy"))]
        sparse = Eventstream(df, {"segment_cols": ["grp", "platform"]})
        w = sparse.ab_test(DIFF, METRICS[:1], min_support=1000)
        row = _strict(w.result)["overall"][0]
        assert row["verdict"] == "insufficient_data"
        assert row["p_value"] is None and row["valid_replicates"] is None
        json.dumps(_strict(w.result), allow_nan=False)

    def test__bad_arguments_raise_at_construction(self) -> None:
        with pytest.raises(InvalidMetricConfigError):
            _stream().ab_test(DIFF, [{"metric": "event_count_bulk"}])

    def test__a_bad_sidebar_change_shows_an_error(self) -> None:
        w = _stream().ab_test(DIFF, METRICS)
        w.test = "z_test"  # not for the length / duration metrics
        assert "z_test" in w.error
        assert w.result == "{}"
        w.test = "auto"
        assert w.error == "" and _strict(w.result)["overall"]

    def test__no_metrics_or_no_diff_means_an_empty_result(self) -> None:
        assert ABTestWidget(_stream(), diff=DIFF).result == "{}"
        assert ABTestWidget(_stream(), metrics=METRICS).result == "{}"

    def test__library_warnings_are_shown_not_lost(self) -> None:
        w = _stream().ab_test(DIFF, METRICS, strata_col="platform")
        warnings = _strict(w.result)["warnings"]
        assert any("strata_pre_exposure" in message for message in warnings)
        assert all(r["exploratory"] for r in _strict(w.result)["strata"])

    def test__assignment_info(self) -> None:
        stream = load_ab_test()
        table = load_ab_test_assignment()
        assert stream.ab_test(NORTHWIND_DIFF).assignment_info == ""
        w = ABTestWidget(stream, diff=NORTHWIND_DIFF, assignment=table)
        assert w.assignment_info == "arm,assigned_at"
        assert ABTestWidget(stream, assignment=table["arm"]).assignment_info == "arm"

    def test__table_arms_offered_as_the_arm_column(self) -> None:
        """R3C-05: diff=("arm", ...) names the table's arms; the sidebar lists them."""
        stream = load_ab_test()
        table = load_ab_test_assignment()
        w = stream.ab_test(
            ("arm", "treatment", "control"), NORTHWIND_METRICS[:1], assignment=table
        )
        assert json.loads(w.segment_levels)["arm"] == ["control", "treatment"]
        assert "arm" not in json.loads(stream.ab_test(NORTHWIND_DIFF).segment_levels)
        assert not w.error and _strict(w.result)["overall"]


# ── state file and static export ─────────────────────────────────────────────


class TestStateAndExport:
    def test__state_is_saved_and_restored(self, tmp_path) -> None:
        path = tmp_path / "ab.json"
        stream = _stream()
        first = stream.ab_test(DIFF, METRICS, state_file=str(path))
        first.metrics = json.dumps(METRICS[:2])
        first.correction = "bonferroni"
        first.strata_col = "platform"
        first.strata_pre_exposure = True
        payload = json.loads(path.read_text())
        assert payload["widget_type"] == "ab_test"
        assert set(payload["state"]) == set(ABTestWidget._persist_names)

        second = stream.ab_test(state_file=str(path))
        assert json.loads(second.diff) == list(DIFF)
        assert json.loads(second.metrics) == METRICS[:2]
        assert (second.correction, second.strata_col) == ("bonferroni", "platform")
        assert second.strata_pre_exposure is True
        assert _strict(second.result) == _strict(first.result)

    def test__explicit_arguments_override_the_saved_state(self, tmp_path) -> None:
        path = tmp_path / "ab.json"
        stream = _stream()
        stream.ab_test(DIFF, METRICS, ci_level=0.9, state_file=str(path))
        again = stream.ab_test(DIFF, METRICS[:1], state_file=str(path))
        assert again.ci_level == 0.9
        assert json.loads(again.metrics) == METRICS[:1]

    def test__export_data_is_strict_and_complete(self) -> None:
        w = _stream().ab_test(DIFF, METRICS, period="W", sidebar_open=False)
        data = w._export_data()
        _strict(json.dumps(data, allow_nan=False))
        assert data["widget_type"] == "ab_test"
        assert data["result"] == _strict(w.result)
        assert data["diff"] == list(DIFF) and data["metrics"] == METRICS
        assert data["period"] == "W" and data["sidebar_open"] is False
        assert data["segment_cols"] == ["grp", "platform"]
        assert w._export_data(sidebar_open=True)["sidebar_open"] is True

    def test__export_html_and_render_static(self, tmp_path) -> None:
        w = _stream().ab_test(DIFF, METRICS)
        out = tmp_path / "ab.html"
        w.export_html(str(out))
        html = out.read_text()
        assert '"widget_type": "ab_test"' in html
        assert "<title>A/B Test</title>" in html
        assert "srcdoc" in w.render_static().data

    def test__export_refuses_a_failed_result(self, tmp_path) -> None:
        from retentioneering.exceptions import WidgetExportError

        w = _stream().ab_test(DIFF, METRICS)
        w.test = "z_test"
        with pytest.raises(WidgetExportError):
            w.export_html(str(tmp_path / "ab.html"))


# ── MCP: the scorecard as a report tab ───────────────────────────────────────


class TestMcpReportTab:
    def test__add_ab_test_registers_the_widget_as_a_tab(self, tmp_path) -> None:
        from retentioneering.mcp import tools
        from retentioneering.mcp._report_session import ReportSession

        stream = _stream()
        session = ReportSession(stream)
        out = tools.add_ab_test(
            session, "AB", list(DIFF), METRICS, strata_col="platform", period="W"
        )
        json.dumps(out, allow_nan=False)
        assert out["tab_id"] == "tab-0" and out["label"] == "AB"
        data = session.pending_tabs[0]["data"]
        assert data["widget_type"] == "ab_test" and data["sidebar_open"] is False
        assert data["result"]["strata"] and data["result"]["periods"]
        expected = stream.get_ab_test(DIFF, METRICS)
        assert [m["metric"] for m in out["metrics"]] == expected["metric"].tolist()
        for entry, (_, row) in zip(out["metrics"], expected.iterrows()):
            assert entry["verdict"] == row["verdict"]
            assert entry["delta"] == pytest.approx(row["delta"], abs=1e-6)
            assert entry["p_value_adjusted"] == pytest.approx(
                row["p_value_adjusted"], abs=1e-8
            )
        assert any("strata_pre_exposure" in w for w in out["warnings"])

        path = tmp_path / "report.html"
        session.export("Report", "Conversion: [AB:conversion].", str(path))
        assert '"widget_type": "ab_test"' in path.read_text()

    def test__add_ab_test_reports_errors_without_a_tab(self) -> None:
        from retentioneering.mcp import tools
        from retentioneering.mcp._report_session import ReportSession

        session = ReportSession(_stream())
        out = tools.add_ab_test(
            session, "AB", list(DIFF), [{"metric": "event_count_bulk"}]
        )
        assert "error" in out and session.pending_tabs == []
