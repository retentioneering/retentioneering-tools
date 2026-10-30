"""The A/B difference matrices: ``ab_transitions`` and ``ab_steps``.

Every cell of the matrices is a row of ``get_ab_transitions`` / ``get_ab_steps`` (via
the twins ``ab_transitions_data`` / ``ab_steps_data``), so the payload is checked cell
by cell against the frame; the point estimates against the descriptive tools
(``transition_graph_data``, ``step_matrix_data``); the outline class against the
adjusted p-values (on Northwind and on the A/A dataset); and the matrices against
the graph overlay and the diff step matrix widget. Then the widget mechanics:
signatures, defaults, recompute, errors, ``state_file`` and the static export.
"""

from __future__ import annotations

import inspect
import json
import math

import numpy as np
import pandas as pd
import pytest

from retentioneering import datasets
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    DiffConfigError,
    InvalidParameterError,
    RetentioneeringError,
)
from retentioneering.tools.ab_tables import cell_status, matrix_axes
from retentioneering.tools.ab_transitions import EDGE_WEIGHTS
from retentioneering.widgets._ab_matrix import VIEW_DEFAULTS, untested_reason
from retentioneering.widgets.ab_steps import ABStepsWidget
from retentioneering.widgets.ab_transitions import ABTransitionsWidget

DIFF = ("exp_group", "treatment", "control")
ARM_DIFF = ("arm", "treatment", "control")
BOOTSTRAPPED = ("proba_out", "proba_in", "share_of_total")


def _strict(text: str):
    return json.loads(
        text, parse_constant=lambda c: pytest.fail(f"bare {c} in widget JSON")
    )


def _expected_cell(value):
    """What a frame cell becomes in the widget's JSON."""
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


def _assert_rows_are_the_frame(rows: list[dict], frame: pd.DataFrame) -> int:
    """Every row of the payload is the frame's row, cell by cell; the number of
    cells compared."""
    assert len(rows) == len(frame)
    compared = 0
    for row, (_, expected) in zip(rows, frame.iterrows()):
        assert list(row) == list(frame.columns)
        for column in frame.columns:
            assert row[column] == _expected_cell(expected[column]), column
            compared += 1
    return compared


@pytest.fixture(scope="module")
def stream() -> Eventstream:
    return datasets.load_ab_test()


@pytest.fixture(scope="module")
def assignment() -> pd.DataFrame:
    return datasets.load_ab_test_assignment()


@pytest.fixture(scope="module")
def transitions(stream):
    widget = stream.ab_transitions(DIFF)
    return widget, _strict(widget.result)


@pytest.fixture(scope="module")
def steps(stream):
    widget = stream.ab_steps(DIFF)
    return widget, _strict(widget.result)


# ── 1. the payload is the frame ──────────────────────────────────────────────


class TestPayloadIsTheFrame:
    @pytest.mark.parametrize("weight", EDGE_WEIGHTS)
    def test__every_weight_cell_by_cell(self, stream, weight) -> None:
        options = {"n_bootstrap": 200} if weight in BOOTSTRAPPED else {}
        widget = stream.ab_transitions(DIFF, weight, **options)
        payload = _strict(widget.result)
        frame = stream.get_ab_transitions(DIFF, weight, **options)
        assert _assert_rows_are_the_frame(payload["rows"], frame) > 0
        assert payload["edge_weight"] == weight
        assert payload["n_tested"] == frame.attrs["n_tested"]
        assert payload["n_rows"] == frame.attrs["n_rows"] == len(frame)
        assert payload["correction"] == "bh" and payload["alpha"] == 0.05
        assert payload["unit"] == ("per_path" if weight == "avg_per_path" else "pp")
        assert payload["conditional"] is (
            weight in ("proba_out", "proba_in", "share_of_total")
        )
        rows, cols = matrix_axes(frame)
        assert payload["row_keys"] == rows and payload["col_keys"] == cols
        # every row has its cell on the axes
        assert set(frame["source"]) <= set(rows)
        assert set(frame["target"]) <= set(cols)

    def test__steps_cell_by_cell(self, stream, steps) -> None:
        _, payload = steps
        frame = stream.get_ab_steps(DIFF)
        assert _assert_rows_are_the_frame(payload["rows"], frame) > 0
        assert payload["max_steps"] == 10 and payload["unit"] == "pp"
        assert payload["col_keys"] == list(range(1, 11))
        assert payload["row_keys"][-1] == "path_end"
        assert "path_start" not in payload["row_keys"]

    def test__itt_cell_by_cell(self, stream, assignment) -> None:
        widget = stream.ab_transitions(ARM_DIFF, assignment=assignment)
        payload = _strict(widget.result)
        frame = stream.get_ab_transitions(ARM_DIFF, assignment=assignment)
        _assert_rows_are_the_frame(payload["rows"], frame)
        assert payload["population"] == "assignment"
        assert widget.assignment_info == "arm,assigned_at"
        steps = stream.ab_steps(ARM_DIFF, assignment=assignment, max_steps=6)
        frame = stream.get_ab_steps(ARM_DIFF, 6, assignment=assignment)
        _assert_rows_are_the_frame(_strict(steps.result)["rows"], frame)

    def test__dispatch_compute_returns_the_same_payload(
        self, transitions, steps
    ) -> None:
        widget, payload = transitions
        assert widget.dispatch_compute("ab_transitions_data", {}) == payload
        widget, payload = steps
        assert widget.dispatch_compute("ab_steps_data", {}) == payload

    def test__nan_is_null_in_strict_json(self, transitions) -> None:
        widget, payload = transitions
        untested = [r for r in payload["rows"] if r["p_value_adjusted"] is None]
        assert untested and all(r["delta"] is not None for r in untested)
        json.dumps(payload, allow_nan=False)
        json.dumps(widget._export_data(), allow_nan=False)


class TestTwins:
    @pytest.mark.parametrize("weight", ["unique_paths", "avg_per_path"])
    def test__ab_transitions_data_is_get_ab_transitions(self, stream, weight) -> None:
        twin = stream.ab_transitions_data(DIFF, weight, ci_level=0.9)
        frame = stream.get_ab_transitions(DIFF, weight, ci_level=0.9)
        pd.testing.assert_frame_equal(twin, frame)
        assert twin.attrs == frame.attrs

    def test__ab_steps_data_is_get_ab_steps(self, stream, assignment) -> None:
        twin = stream.ab_steps_data(ARM_DIFF, 7, assignment=assignment)
        frame = stream.get_ab_steps(ARM_DIFF, 7, assignment=assignment)
        pd.testing.assert_frame_equal(twin, frame)
        assert twin.attrs == frame.attrs

    @pytest.mark.parametrize(
        ("widget", "twin", "getter"),
        [
            ("ab_transitions", "ab_transitions_data", "get_ab_transitions"),
            ("ab_steps", "ab_steps_data", "get_ab_steps"),
        ],
    )
    def test__signatures(self, widget, twin, getter) -> None:
        get = inspect.signature(getattr(Eventstream, getter)).parameters
        alias = inspect.signature(getattr(Eventstream, twin)).parameters
        assert list(alias) == list(get)
        assert all(alias[n].default == get[n].default for n in get)
        assert all(alias[n].kind == get[n].kind for n in get)
        params = inspect.signature(getattr(Eventstream, widget)).parameters
        assert list(params)[: len(get)] == list(get)
        assert list(params)[len(get) :] == ["height", "sidebar_open", "state_file"]
        assert all(params[n].kind == get[n].kind for n in get)

    @pytest.mark.parametrize(
        ("widget", "twin", "getter"),
        [
            ("ab_transitions", "ab_transitions_data", "get_ab_transitions"),
            ("ab_steps", "ab_steps_data", "get_ab_steps"),
        ],
    )
    def test__docstrings_are_built_from_the_getter(self, widget, twin, getter) -> None:
        get = inspect.getdoc(getattr(Eventstream, getter))
        alias = inspect.getdoc(getattr(Eventstream, twin))
        draw = inspect.getdoc(getattr(Eventstream, widget))
        assert alias.splitlines()[0] == get.splitlines()[0]
        assert f"Documented alias of `{getter}`" in alias
        params = get.split("Parameters\n----------\n")[1].split("\n\nReturns")[0]
        assert params in alias and params in draw
        assert "height : int, default 600" in draw
        assert "state_file" in draw and "black" in draw and "dashed" in draw
        assert "Returns\n-------" in alias

    @pytest.mark.parametrize(
        ("cls", "getter"),
        [
            (ABTransitionsWidget, "get_ab_transitions"),
            (ABStepsWidget, "get_ab_steps"),
        ],
    )
    def test__widget_defaults_are_the_getters(self, stream, cls, getter) -> None:
        widget = cls(stream, DIFF)
        params = widget._params()
        sig = inspect.signature(getattr(Eventstream, getter)).parameters
        for name in params:
            assert params[name] == sig[name].default, name


# ── 2. the point estimates are the descriptive tools' ─────────────────────────


def _cells(payload: dict, column: str) -> pd.Series:
    keys = (
        ("event", "step") if payload["tool"] == "get_ab_steps" else ("source", "target")
    )
    return pd.Series(
        {(r[keys[0]], r[keys[1]]): r[column] for r in payload["rows"]}, dtype=float
    )


class TestPointEstimatesAreTheGraphsAndTheStepMatrixs:
    @pytest.mark.parametrize("weight", EDGE_WEIGHTS)
    def test__transitions_equal_transition_graph_data(self, stream, weight) -> None:
        options = {"n_bootstrap": 200} if weight in BOOTSTRAPPED else {}
        payload = _strict(stream.ab_transitions(DIFF, weight, **options).result)
        diff_matrix, g1, g2 = stream.transition_graph_data(weight, diff=DIFF)
        delta = _cells(payload, "delta")
        index = pd.MultiIndex.from_tuples(delta.index)
        rows = payload["rows"][0]
        n1, n2 = rows["group1_paths"], rows["group2_paths"]
        if weight == "unique_paths":
            # the graph counts paths with the edge; the screen compares shares
            expected = g1.stack() / n1 - g2.stack() / n2
            arms = (_cells(payload, "group1_edge_paths"), g1.stack())
        else:
            expected = diff_matrix.stack()
            arms = (_cells(payload, "group1_value"), g1.stack())
        expected = expected.reindex(index)
        # a ratio weight has no value in an arm where the source (target) never
        # occurs: NaN in the screen, 0 in the graph's matrix of that arm
        finite = delta.notna().to_numpy()
        if weight in ("unique_paths", "avg_per_path"):
            assert finite.all()
        for r, c in delta.index[~finite]:
            row = next(
                x for x in payload["rows"] if (x["source"], x["target"]) == (r, c)
            )
            assert row["group1_value"] is None or row["group2_value"] is None
        np.testing.assert_allclose(
            delta.to_numpy()[finite],
            expected.to_numpy(dtype=float)[finite],
            rtol=1e-12,
            atol=1e-15,
        )
        ours, graph = arms
        present = ours.notna().to_numpy()
        np.testing.assert_allclose(
            ours.to_numpy()[present],
            graph.reindex(index).to_numpy(dtype=float)[present],
            rtol=1e-12,
        )
        # no edge of the graph is missing from the matrix
        both = (g1.stack() != 0) | (g2.stack() != 0)
        assert set(both[both].index) == set(index)

    def test__steps_equal_step_matrix_data(self, steps, stream) -> None:
        _, payload = steps
        diff_matrix, g1, g2 = stream.step_matrix_data(max_steps=10, diff=DIFF)
        delta = _cells(payload, "delta")
        index = pd.MultiIndex.from_tuples(delta.index)
        for column, matrix in (
            ("delta", diff_matrix),
            ("group1_value", g1),
            ("group2_value", g2),
        ):
            np.testing.assert_allclose(
                _cells(payload, column).to_numpy(),
                matrix.stack().reindex(index).to_numpy(dtype=float),
                rtol=1e-12,
                atol=1e-15,
            )
        # every non-zero cell of the step matrix (from step 1) is in the screen
        sm = (g1 + g2).drop(columns=0).drop(index="path_start").stack()
        assert set(sm[sm != 0].index) <= set(index)


# ── 3. the outline class is the adjusted p-value's ───────────────────────────


def _status_counts(payload: dict) -> dict[str, int]:
    counts = {"significant": 0, "not_significant": 0, "untested": 0}
    for row in payload["rows"]:
        counts[cell_status(row, payload["alpha"])] += 1
    return counts


class TestOutlines:
    def test__northwind_outlines_follow_the_adjusted_p(
        self, stream, transitions, steps
    ) -> None:
        for (_, payload), frame in (
            (transitions, stream.get_ab_transitions(DIFF)),
            (steps, stream.get_ab_steps(DIFF)),
        ):
            p = frame["p_value_adjusted"]
            counts = _status_counts(payload)
            assert counts["significant"] == int((p < 0.05).sum())
            assert counts["not_significant"] == int((p >= 0.05).sum())
            assert counts["untested"] == int(p.isna().sum())
            assert (
                counts["significant"] + counts["not_significant"]
                == (frame.attrs["n_tested"])
            )
            # a significant cell is an increase or a decrease, with its sign
            for row in payload["rows"]:
                if cell_status(row, payload["alpha"]) == "significant":
                    expected = "increase" if row["delta"] > 0 else "decrease"
                    assert row["verdict"] == expected

    def test__aa_calibration_draws_no_black_outline(self) -> None:
        aa = datasets.load_ab_test("aa_calibration")
        for widget in (aa.ab_transitions(DIFF), aa.ab_steps(DIFF)):
            counts = _status_counts(_strict(widget.result))
            assert counts["significant"] == 0, counts
            assert counts["not_significant"] > 0

    def test__untested_reasons(self, transitions) -> None:
        _, payload = transitions
        for row, reason in zip(payload["rows"], payload["untested_reason"]):
            assert (reason is None) == (row["p_value_adjusted"] is not None)
            if reason is not None:
                assert "min_support 10" in reason

    @pytest.mark.parametrize(
        ("row", "kind", "expected"),
        [
            ({"p_value_adjusted": 0.3}, "binary", None),
            ({"support_warning": "no_data"}, "binary", "nothing to compare"),
            ({"support_warning": "invalid_replicates"}, "other", "bootstrap"),
            (
                {
                    "group1_edge_paths": 3,
                    "group2_edge_paths": 4,
                    "group1_paths": 50,
                    "group2_paths": 50,
                },
                "binary",
                "pooled support 7 < min_support 10",
            ),
            (
                {
                    "group1_edge_paths": 48,
                    "group2_edge_paths": 47,
                    "group1_paths": 50,
                    "group2_paths": 50,
                },
                "binary",
                "pooled paths without it 5 < min_support 10",
            ),
            (
                {
                    "group1_value": 0.5,
                    "group2_value": 0.5,
                    "group1_paths": 40,
                    "group2_paths": 40,
                },
                "binary",
                "no spread",
            ),
        ],
    )
    def test__untested_reason_reads_the_columns(self, row, kind, expected) -> None:
        reason = untested_reason(
            {"p_value_adjusted": float("nan"), **row}, kind=kind, min_support=10
        )
        if expected is None:
            assert reason is None
        else:
            assert expected in reason


# ── 4. the matrices agree with the graph overlay and the diff step matrix ────


def _overlay_class(edge: dict | None) -> str:
    """viz-core ``abEdgeClass`` (js/viz-core/src/utils/ab-transitions.ts)."""
    if edge is None:
        return "absent"
    if edge["verdict"] in ("increase", "decrease"):
        return edge["verdict"]
    if edge["verdict"] == "inconclusive" and edge["p_adjusted"] is not None:
        return "inconclusive"
    return "untested"


class TestAgreement:
    @pytest.mark.parametrize("weight", ["unique_paths", "avg_per_path", "proba_out"])
    def test__ab_transitions_agrees_with_the_graph_overlay(
        self, stream, weight
    ) -> None:
        options = {"n_bootstrap": 200} if weight in BOOTSTRAPPED else {}
        frame = stream.get_ab_transitions(DIFF, weight, **options)
        graph = stream.transition_graph(diff=DIFF, ab_transitions=frame)
        overlay = {
            (e["source"], e["target"]): e
            for e in json.loads(graph.ab_transitions)["edges"]
        }
        payload = _strict(stream.ab_transitions(DIFF, weight, **options).result)
        assert len(overlay) == len(payload["rows"])
        for row in payload["rows"]:
            edge = overlay[(row["source"], row["target"])]
            status = cell_status(row, payload["alpha"])
            cls = _overlay_class(edge)
            assert edge["delta"] == pytest.approx(row["delta"], abs=0)
            if status == "significant":
                assert cls == ("increase" if row["delta"] > 0 else "decrease")
            elif status == "not_significant":
                assert cls == "inconclusive"
            else:
                assert cls == "untested"

    def test__ab_steps_agrees_with_the_diff_step_matrix(self, stream, steps) -> None:
        _, payload = steps
        result = _strict(stream.step_matrix(max_steps=10, diff=DIFF).result)
        block = result["matrices"][0]
        diff_values = {
            (event, step): block["values"][i][j]
            for i, event in enumerate(block["events"])
            for j, step in enumerate(block["columns"])
        }
        for row in payload["rows"]:
            assert diff_values[(row["event"], row["step"])] == pytest.approx(
                row["delta"], rel=1e-12, abs=1e-15
            )
            # the same sign, hence the same hue (red-500 / blue-500)
            assert np.sign(diff_values[(row["event"], row["step"])]) == np.sign(
                row["delta"]
            )


# ── 6. errors at construction are the getters' ───────────────────────────────


def _message(call) -> tuple[type, str]:
    with pytest.raises(RetentioneeringError) as caught:
        call()
    return type(caught.value), str(caught.value)


class TestErrors:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"edge_weight": "count"},
            {"edge_weight": "time_median"},
            {"correction": "sidak"},
            {"ci_level": 1.5},
            {"min_support": 0},
            {"test": "mann_whitney"},
            {"edges": [("home", "nowhere")]},
            {"n_bootstrap": 10, "edge_weight": "proba_out"},
            {"path_col": "no_such_column"},
            {"n_bootstrap": "many"},
            {"horizon": pd.Timedelta(days=7)},
            {"ci_level": 0},
        ],
    )
    def test__transitions(self, stream, kwargs) -> None:
        assert _message(lambda: stream.ab_transitions(DIFF, **kwargs)) == _message(
            lambda: stream.get_ab_transitions(DIFF, **kwargs)
        )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_steps": 0},
            {"correction": "sidak"},
            {"ci_level": 0},
            {"min_support": -1},
            {"path_pattern": "cart"},
            {"anchor": {"event": "cart"}},
        ],
    )
    def test__steps(self, stream, kwargs) -> None:
        assert _message(lambda: stream.ab_steps(DIFF, **kwargs)) == _message(
            lambda: stream.get_ab_steps(DIFF, **kwargs)
        )

    @pytest.mark.parametrize(
        "diff",
        [("exp_group", "treatment", "nobody"), ("no_such_segment", "a", "b"), None],
    )
    def test__bad_diff(self, stream, diff) -> None:
        assert _message(lambda: stream.ab_transitions(diff)) == _message(
            lambda: stream.get_ab_transitions(diff)
        )
        assert _message(lambda: stream.ab_steps(diff)) == _message(
            lambda: stream.get_ab_steps(diff)
        )

    def test__a_bad_sidebar_change_shows_an_error(self, stream) -> None:
        widget = stream.ab_steps(DIFF, max_steps=4)
        widget.min_support = 0
        assert "min_support" in widget.error and widget.result == "{}"
        widget.min_support = 10
        assert widget.error == "" and _strict(widget.result)["rows"]

    def test__unknown_constructor_argument(self, stream) -> None:
        with pytest.raises(TypeError, match="max_steps"):
            ABTransitionsWidget(stream, DIFF, max_steps=4)
        with pytest.raises(DiffConfigError):
            stream.ab_steps(("exp_group", "treatment", "treatment"))
        with pytest.raises(InvalidParameterError):
            stream.ab_steps(DIFF, max_steps=np.int64(0))


# ── traitlets and recompute ──────────────────────────────────────────────────


class TestTraitlets:
    def test__arguments_land_in_traitlets(self, stream, assignment) -> None:
        widget = stream.ab_transitions(
            ARM_DIFF,
            "avg_per_path",
            assignment=assignment,
            horizon=pd.Timedelta(days=7),
            edges=[("catalog", "quick_add"), ("cart", "cart")],
            correction="holm",
            ci_level=0.9,
            min_support=np.int64(5),
            height=480,
            sidebar_open=False,
        )
        assert widget.edge_weight == "avg_per_path" and widget.correction == "holm"
        assert widget.horizon == "7D" and widget.ci_level == 0.9
        assert json.loads(widget.edges) == [["catalog", "quick_add"], ["cart", "cart"]]
        assert widget.min_support == 5
        assert (widget.height, widget.sidebar_open) == (480, False)
        payload = _strict(widget.result)
        assert payload["edges"] == "declared" and payload["n_rows"] == 2
        frame = stream.get_ab_transitions(
            ARM_DIFF,
            "avg_per_path",
            assignment=assignment,
            horizon=pd.Timedelta(days=7),
            min_support=5,
            edges=[("catalog", "quick_add"), ("cart", "cart")],
            correction="holm",
            ci_level=0.9,
        )
        _assert_rows_are_the_frame(payload["rows"], frame)
        assert pd.Timedelta(payload["horizon"]) == pd.Timedelta(frame.attrs["horizon"])
        assert json.loads(widget.diff) == list(ARM_DIFF)

    def test__sidebar_changes_recompute(self, stream) -> None:
        widget = stream.ab_transitions(DIFF)
        widget.edge_weight = "proba_out"
        payload = _strict(widget.result)
        assert payload["edge_weight"] == "proba_out" and payload["conditional"]
        assert payload["rows"][0]["edge_weight"] == "proba_out"
        widget.correction = "bonferroni"
        payload = _strict(widget.result)
        assert payload["simultaneous"] and payload["correction"] == "bonferroni"
        assert "delta_ci_low_adj" in payload["rows"][0]
        widget.ci_level = 0.99
        assert _strict(widget.result)["alpha"] == pytest.approx(0.01)
        before = _strict(widget.result)["n_tested"]
        widget.min_support = 400
        assert _strict(widget.result)["n_tested"] < before

        steps = stream.ab_steps(DIFF, max_steps=4)
        steps.max_steps = 6
        assert _strict(steps.result)["col_keys"] == [1, 2, 3, 4, 5, 6]

    def test__view_changes_do_not_recompute(self, stream) -> None:
        widget = stream.ab_steps(DIFF, max_steps=3)
        result = widget.result
        calls = []
        widget.observe(lambda change: calls.append(change), names=["result"])
        widget.significance_filter = "hide"
        widget.color_clip = 0.02
        widget.event_visibility = json.dumps({"cart": {"isHidden": True}})
        assert widget.result == result and calls == []

    def test__id_list_diff(self, stream) -> None:
        df = stream.to_dataframe()
        ids1 = df.loc[df["exp_group"] == "treatment", "user_id"].unique()[:600]
        ids2 = df.loc[df["exp_group"] == "control", "user_id"].unique()[:600]
        widget = stream.ab_steps((list(ids1), list(ids2)), max_steps=3)
        assert json.loads(widget.diff) == {"group1_ids": 600, "group2_ids": 600}
        frame = stream.get_ab_steps((list(ids1), list(ids2)), 3)
        _assert_rows_are_the_frame(_strict(widget.result)["rows"], frame)

    def test__library_warnings_are_shown(self, stream) -> None:
        widget = stream.ab_transitions(DIFF, n_bootstrap=500)
        warnings = _strict(widget.result)["warnings"]
        with pytest.warns(Warning) as caught:
            stream.get_ab_transitions(DIFF, n_bootstrap=500)
        frame_warns = [str(w.message) for w in caught]
        assert warnings and set(warnings) <= set(frame_warns)


# ── state file and static export ─────────────────────────────────────────────


class TestStateAndExport:
    def test__state_is_saved_and_restored(self, stream, tmp_path) -> None:
        path = tmp_path / "matrix.json"
        first = stream.ab_transitions(DIFF, state_file=str(path))
        first.edge_weight = "avg_per_path"
        first.correction = "holm"
        first.significance_filter = "dim"
        first.row_sort = "max_abs_delta"
        first.color_clip = 0.5
        first.event_visibility = json.dumps({"cart": {"isPinned": True}})
        first.reading_hint = False
        saved = json.loads(path.read_text())
        assert saved["widget_type"] == "ab_transitions"
        assert set(saved["state"]) == set(first._persist_names)
        assert "diff" not in saved["state"] and "assignment" not in saved["state"]
        assert set(VIEW_DEFAULTS) <= set(saved["state"])

        second = stream.ab_transitions(DIFF, state_file=str(path))
        assert (second.edge_weight, second.correction) == ("avg_per_path", "holm")
        assert (second.significance_filter, second.row_sort) == ("dim", "max_abs_delta")
        assert second.color_clip == 0.5 and second.reading_hint is False
        assert json.loads(second.event_visibility) == {"cart": {"isPinned": True}}
        assert _strict(second.result) == _strict(first.result)

    def test__explicit_arguments_override_the_saved_state(
        self, stream, tmp_path
    ) -> None:
        path = tmp_path / "steps.json"
        stream.ab_steps(DIFF, max_steps=4, ci_level=0.9, state_file=str(path))
        again = stream.ab_steps(DIFF, max_steps=3, state_file=str(path))
        assert again.max_steps == 3 and again.ci_level == 0.9
        assert _strict(again.result)["col_keys"] == [1, 2, 3]

    def test__export_data_is_strict_and_complete(self, transitions, steps) -> None:
        for widget, payload in (transitions, steps):
            data = widget._export_data()
            _strict(json.dumps(data, allow_nan=False))
            assert data["widget_type"] == widget.widget_type
            assert data["result"] == payload and data["diff"] == list(DIFF)
            for name in widget._persist_names:
                assert name in data, name
            assert widget._export_data(sidebar_open=False)["sidebar_open"] is False

    def test__export_html_and_render_static(self, transitions, steps, tmp_path) -> None:
        for (widget, _), title in (
            (transitions, "A/B Transitions"),
            (steps, "A/B Steps"),
        ):
            out = tmp_path / f"{widget.widget_type}.html"
            widget.export_html(str(out))
            html = out.read_text()
            assert f'"widget_type": "{widget.widget_type}"' in html
            assert f"<title>{title}</title>" in html
            assert "srcdoc" in widget.render_static().data
