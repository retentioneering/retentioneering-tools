"""Transition and step-matrix comparison between A/B arms (``tools/ab_transitions.py``).

The point estimates must be the descriptive tools' numbers (``transition_graph_data`` and
``step_matrix_data`` in diff mode), the tests and intervals the scorecard's primitives,
and the population, support rule, verdict and reading the scorecard's.
"""

from __future__ import annotations

import pathlib
import time

import numpy as np
import pandas as pd
import pytest

from retentioneering import datasets
from retentioneering.datasets import ab_test as ab_data
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    DiffConfigError,
    EmptyEventstreamError,
    InvalidParameterError,
    PathIdNotFoundError,
)
from retentioneering.tools import _stats
from retentioneering.tools.ab_transitions import (
    EDGE_WEIGHTS,
    STEP_COLUMNS,
    TRANSITION_COLUMNS,
    transitions_payload,
)

FIXTURE_DIR = pathlib.Path(__file__).resolve().parents[1] / "datasets" / "fixtures"
DIFF = ("exp_group", "treatment", "control")
ARM_DIFF = ("arm", "treatment", "control")


# ---------------------------------------------------------------- Northwind fixture
@pytest.fixture(scope="module")
def compact():
    return ab_data._read_fixture(FIXTURE_DIR)


@pytest.fixture(scope="module")
def stream(compact) -> Eventstream:
    events, users = compact
    df = ab_data._assemble(events, users, with_truth=False)
    schema = {key: list(value) for key, value in ab_data._SCHEMA.items()}
    return Eventstream(df, schema)


@pytest.fixture(scope="module")
def assignment(compact) -> pd.DataFrame:
    return ab_data._assignment(compact[1])


# ---------------------------------------------------------------- tiny exact stream
#   u1 (A): a b a b      u2 (A): a c        u3 (A): b
#   u4 (B): a b          u5 (B): c c c      u6 (B): a          u7 (B, silent)
_TINY = [
    ("u1", "a"),
    ("u1", "b"),
    ("u1", "a"),
    ("u1", "b"),
    ("u2", "a"),
    ("u2", "c"),
    ("u3", "b"),
    ("u4", "a"),
    ("u4", "b"),
    ("u5", "c"),
    ("u5", "c"),
    ("u5", "c"),
    ("u6", "a"),
]


def _tiny() -> Eventstream:
    df = pd.DataFrame(_TINY, columns=["user_id", "event"])
    df["timestamp"] = pd.Timestamp("2024-01-01") + pd.to_timedelta(
        np.arange(len(df)), unit="min"
    )
    df["arm"] = np.where(df["user_id"].isin(["u1", "u2", "u3"]), "A", "B")
    return Eventstream(df, {"segment_cols": ["arm"]})


def _tiny_table() -> pd.Series:
    arms = {"u1": "A", "u2": "A", "u3": "A", "u4": "B", "u5": "B", "u6": "B"}
    arms["u7"] = "B"
    return pd.Series(arms, name="arm").rename_axis("user_id")


def _row(frame: pd.DataFrame, source: str, target: str) -> pd.Series:
    hit = frame[(frame["source"] == source) & (frame["target"] == target)]
    assert len(hit) == 1, (source, target)
    return hit.iloc[0]


class TestTinyStream:
    def test_unique_paths_is_the_z_test_of_the_scorecard(self) -> None:
        frame = _tiny().get_ab_transitions(("arm", "A", "B"), min_support=1)
        row = _row(frame, "a", "b")
        # A: u1 has a->b, u2, u3 do not; B: u4 has it, u5, u6 do not
        assert (row["group1_edge_paths"], row["group2_edge_paths"]) == (1, 1)
        assert row["group1_value"] == pytest.approx(1 / 3)
        assert row["group2_value"] == pytest.approx(1 / 3)
        _, p, se = _stats.two_proportion_z(1, 3, 1, 3)
        low, high = _stats.newcombe_ci(1, 3, 1, 3, 0.95)
        assert row["delta"] == 0
        assert row["delta_ci_low"] == pytest.approx(float(low))
        assert row["delta_ci_high"] == pytest.approx(float(high))
        assert row["p_value"] == pytest.approx(float(p))
        assert row["delta_se"] == pytest.approx(float(se))
        assert row["test"] == "z_test" and row["ci_basis"] == "wilson"
        # pooled paths without the edge: 4 >= min_support 1, so tested
        assert row["verdict"] == "inconclusive"

    def test_avg_per_path_is_welch(self) -> None:
        frame = _tiny().get_ab_transitions(
            ("arm", "A", "B"), "avg_per_path", min_support=1
        )
        row = _row(frame, "a", "b")
        x1, x2 = np.array([2.0, 0, 0]), np.array([1.0, 0, 0])
        _, df, p, se = _stats.welch_t(
            x1.mean(), x1.var(ddof=1), 3, x2.mean(), x2.var(ddof=1), 3
        )
        assert row["group1_value"] == pytest.approx(2 / 3)
        assert row["group2_value"] == pytest.approx(1 / 3)
        assert row["p_value"] == pytest.approx(float(p))
        assert row["delta_se"] == pytest.approx(float(se))
        from scipy import stats

        q = stats.t.ppf(0.975, float(df))
        assert row["delta_ci_low"] == pytest.approx(1 / 3 - q * float(se))
        assert row["test"] == "welch_t_test"

    def test_ratio_weights_are_ratios_of_sums(self) -> None:
        stream = _tiny()
        out = stream.get_ab_transitions(("arm", "A", "B"), "proba_out", min_support=1)
        row = _row(out, "a", "b")
        # out of a in A: u1 a->b, b? no: u1 a->b twice, u2 a->c once -> 2/3
        assert row["group1_value"] == pytest.approx(2 / 3)
        # in B: u4 a->b, u6 a->path_end -> 1/2
        assert row["group2_value"] == pytest.approx(1 / 2)
        assert row["test"] == "bootstrap" and row["conditional"]
        assert row["valid_replicates"] > 0
        share = stream.get_ab_transitions(
            ("arm", "A", "B"), "share_of_total", min_support=1
        )
        # A: transitions per path are 5, 3, 2 (with path start and end)
        assert _row(share, "a", "b")["group1_value"] == pytest.approx(2 / 10)

    def test_untested_rows_stay_out_of_the_family(self) -> None:
        frame = _tiny().get_ab_transitions(("arm", "A", "B"))  # min_support 10
        assert frame.attrs["n_tested"] == 0
        assert (frame["verdict"] == "insufficient_data").all()
        assert frame["p_value"].isna().all() and frame["p_value_adjusted"].isna().all()
        assert frame["reading"].str.contains("not tested").all()

    def test_silent_paths_enter_the_denominators(self) -> None:
        stream = _tiny()
        stream_rows = stream.get_ab_transitions(("arm", "A", "B"), min_support=1)
        itt = stream.get_ab_transitions(
            ARM_DIFF[:1] + ("A", "B"), assignment=_tiny_table(), min_support=1
        )
        assert (stream_rows["population"] == "stream").all()
        assert (itt["population"] == "assignment").all()
        assert (itt["group2_paths"] == 4).all() and (
            itt["group2_silent_paths"] == 1
        ).all()
        row = _row(itt, "a", "b")
        assert row["group2_value"] == pytest.approx(1 / 4)
        assert _row(stream_rows, "a", "b")["group2_value"] == pytest.approx(1 / 3)

    def test_declared_edges_are_the_family(self) -> None:
        frame = _tiny().get_ab_transitions(
            ("arm", "A", "B"), edges=[("a", "b"), ("b", "c")], min_support=1
        )
        assert list(zip(frame["source"], frame["target"])) == [("a", "b"), ("b", "c")]
        absent = _row(frame, "b", "c")
        assert absent["support_warning"] == "no_data"
        assert absent["verdict"] == "insufficient_data"
        assert frame.attrs["edges"] == "declared"
        assert (frame["correction_family"] == "edges=declared/unique_paths").all()

    def test_steps_are_shares_of_paths_at_each_step(self) -> None:
        frame = _tiny().get_ab_steps(("arm", "A", "B"), max_steps=3, min_support=1)
        cell = frame[(frame["event"] == "c") & (frame["step"] == 2)].iloc[0]
        # step 2: A u2 has c; B u5 has c
        assert cell["group1_value"] == pytest.approx(1 / 3)
        assert cell["group2_value"] == pytest.approx(1 / 3)
        ended = frame[(frame["event"] == "path_end") & (frame["step"] == 2)].iloc[0]
        # A: u3 has one event; B: u6 has one event
        assert ended["group1_value"] == pytest.approx(1 / 3)
        assert ended["group2_value"] == pytest.approx(1 / 3)


# ---------------------------------------------------------------- equality with the graph
class TestDescriptiveEquality:
    @pytest.mark.parametrize("weight", EDGE_WEIGHTS)
    def test_group_values_equal_the_graph_matrices(self, stream, weight) -> None:
        frame = stream.get_ab_transitions(DIFF, weight)
        _, g1, g2 = stream.transition_graph_data(weight, diff=DIFF)
        for arm, matrix in (("group1", g1), ("group2", g2)):
            graph = matrix.stack()
            graph = graph[graph != 0]
            values = frame.set_index(["source", "target"])[f"{arm}_value"]
            if weight == "unique_paths":
                # the graph counts paths, the row gives their share
                values = frame.set_index(["source", "target"])[f"{arm}_edge_paths"]
            ours = values[values.fillna(0) != 0]
            assert set(ours.index) == set(graph.index)
            np.testing.assert_allclose(
                ours.reindex(graph.index).to_numpy(dtype=float),
                graph.to_numpy(dtype=float),
                rtol=1e-12,
            )
        assert len(frame) == (g1 + g2 != 0).to_numpy().sum()
        assert list(frame.columns) == [
            c for c in TRANSITION_COLUMNS if c in frame.columns
        ]

    @pytest.mark.parametrize("weight", ["proba_out", "proba_in", "unique_paths"])
    def test_itt_keeps_the_graph_numerators(self, stream, assignment, weight) -> None:
        """With the table, ratio weights are unchanged (silent paths have no
        transition) and the paths with an edge too; per-path shares divide by the
        ITT denominators instead (documented)."""
        frame = stream.get_ab_transitions(ARM_DIFF, weight, assignment=assignment)
        _, g1, _ = stream.transition_graph_data(weight, diff=DIFF)
        row = _row(frame, "product_view", "quick_add")
        if weight == "unique_paths":
            assert row["group1_edge_paths"] == g1.loc["product_view", "quick_add"]
            assert row["group1_paths"] == 4960
        else:
            assert row["group1_value"] == pytest.approx(
                g1.loc["product_view", "quick_add"], rel=1e-12
            )

    def test_steps_equal_the_step_matrix(self, stream) -> None:
        frame = stream.get_ab_steps(DIFF, max_steps=10)
        _, g1, g2 = stream.step_matrix_data(max_steps=10, diff=DIFF)
        for arm, matrix in (("group1", g1), ("group2", g2)):
            sm = matrix.drop(columns=0).drop(index="path_start").stack()
            sm = sm[sm != 0]
            values = frame.set_index(["event", "step"])[f"{arm}_value"]
            ours = values[values != 0]
            assert set(ours.index) == set(sm.index)
            np.testing.assert_allclose(
                ours.reindex(sm.index).to_numpy(), sm.to_numpy(), rtol=1e-12
            )
        assert list(frame.columns) == [c for c in STEP_COLUMNS if c in frame.columns]


# ---------------------------------------------------------------- population and window
class TestPopulation:
    def test_horizon_drops_the_scorecards_immature_paths(
        self, stream, assignment
    ) -> None:
        metric = [
            {"name": "c", "metric": "has_event", "metric_args": {"event": "cart"}}
        ]
        card = stream.get_ab_test(
            ARM_DIFF, metric, assignment=assignment, horizon="28D"
        ).iloc[0]
        for frame in (
            stream.get_ab_transitions(ARM_DIFF, assignment=assignment, horizon="28D"),
            stream.get_ab_steps(ARM_DIFF, assignment=assignment, horizon="28D"),
        ):
            row = frame.iloc[0]
            for column in (
                "immature_paths",
                "group1_paths",
                "group2_paths",
                "group1_silent_paths",
                "group2_silent_paths",
                "unassigned_paths",
            ):
                assert row[column] == card[column], column
            assert frame["reading"].str.startswith("within 28 days of assignment").all()

    def test_horizon_counts_only_the_window(self, stream, assignment) -> None:
        full = stream.get_ab_transitions(ARM_DIFF, assignment=assignment)
        early = stream.get_ab_transitions(ARM_DIFF, assignment=assignment, horizon="7D")
        a = _row(full, "product_view", "quick_add")
        b = _row(early, "product_view", "quick_add")
        # every user's 7-day window is closed (assigned in the first six of eight
        # weeks): the same paths, fewer of their transitions
        assert b["group1_paths"] == a["group1_paths"] and b["immature_paths"] == 0
        assert b["group1_edge_paths"] < a["group1_edge_paths"]

    def test_session_grain_of_a_user_experiment_raises(self, stream) -> None:
        with pytest.raises(InvalidParameterError, match="randomised unit"):
            stream.get_ab_transitions(DIFF, path_col="session_id")
        with pytest.raises(InvalidParameterError, match="randomised unit"):
            stream.get_ab_steps(DIFF, path_col="session_id")


# ---------------------------------------------------------------- planted truth
class TestNorthwindPlantedTruth:
    def test_quick_add_and_the_cart_loop_are_found(self, stream) -> None:
        frame = stream.get_ab_transitions(DIFF)
        quick = _row(frame, "product_view", "quick_add")
        assert quick["verdict"] == "increase"
        assert quick["support_warning"] == "one_arm_only"
        old = _row(frame, "product_view", "add_to_cart")
        assert old["verdict"] == "decrease"
        loop = _row(frame, "cart", "cart")
        assert loop["verdict"] == "increase" and loop["group2_edge_paths"] == 0
        assert frame.attrs["n_tested"] > 50
        assert "screen result" in quick["reading"]

    def test_the_checkout_shortcut_shows_as_a_conditional_drop(self, stream) -> None:
        frame = stream.get_ab_transitions(DIFF, "proba_out")
        row = _row(frame, "checkout_start", "payment")
        assert row["verdict"] == "decrease" and row["conditional"]
        assert "conditional on post-treatment behaviour" in row["reading"]

    def test_unique_paths_runs_fast(self, stream) -> None:
        started = time.perf_counter()
        stream.get_ab_transitions(DIFF)
        assert time.perf_counter() - started < 5

    @pytest.mark.slow
    def test_bootstrap_runs_within_budget(self, stream) -> None:
        for weight in ("proba_out", "proba_in", "share_of_total"):
            started = time.perf_counter()
            stream.get_ab_transitions(DIFF, weight, n_bootstrap=1000)
            assert time.perf_counter() - started < 20


# ---------------------------------------------------------------- calibration
def _discoveries(frame: pd.DataFrame) -> int:
    return int(frame["verdict"].isin(["increase", "decrease"]).sum())


class TestCalibration:
    def test_aa_calibration_finds_nothing(self) -> None:
        stream = datasets.load_ab_test("aa_calibration")
        for weight in ("unique_paths", "avg_per_path", "proba_out"):
            frame = stream.get_ab_transitions(DIFF, weight, n_bootstrap=300)
            assert _discoveries(frame) == 0, weight
        assert _discoveries(stream.get_ab_steps(DIFF)) == 0

    def test_permuted_arms_on_northwind_find_nothing(self, stream) -> None:
        users = pd.Index(pd.unique(stream.df["user_id"])).sort_values()
        shuffled = np.random.default_rng(7).permutation(len(users))
        ids1, ids2 = users[shuffled[::2]], users[shuffled[1::2]]
        assert _discoveries(stream.get_ab_transitions((list(ids1), list(ids2)))) == 0
        assert _discoveries(stream.get_ab_steps((list(ids1), list(ids2)))) == 0

    @pytest.mark.slow
    def test_any_discovery_rate_across_aa_seeds(self) -> None:
        """Under a complete null BH at q = 0.05 controls the chance of any
        discovery at about 5 %; 20 A/A seeds and 20 permutations of Northwind
        allow a few runs with one."""
        runs, hits = 0, 0
        for seed in range(20):
            stream = datasets.load_ab_test("aa_calibration", seed=seed)
            for frame in (
                stream.get_ab_transitions(DIFF),
                stream.get_ab_steps(DIFF),
            ):
                runs += 1
                hits += _discoveries(frame) > 0
        assert hits <= 4, (hits, runs)


# ---------------------------------------------------------------- errors
class TestErrors:
    def test_count_is_refused_with_a_hint(self, stream) -> None:
        with pytest.raises(InvalidParameterError, match="avg_per_path"):
            stream.get_ab_transitions(DIFF, "count")

    @pytest.mark.parametrize("weight", ["time_median", "time_q95"])
    def test_time_weights_are_deferred(self, stream, weight) -> None:
        with pytest.raises(InvalidParameterError, match="not tested in this release"):
            stream.get_ab_transitions(DIFF, weight)

    def test_unknown_weight_and_options(self, stream) -> None:
        with pytest.raises(InvalidParameterError, match="edge_weight"):
            stream.get_ab_transitions(DIFF, "probability")
        with pytest.raises(InvalidParameterError, match="z_test"):
            stream.get_ab_transitions(DIFF, "proba_out", test="z_test")
        with pytest.raises(InvalidParameterError, match="correction"):
            stream.get_ab_transitions(DIFF, correction="fdr")
        with pytest.raises(InvalidParameterError, match="n_bootstrap"):
            stream.get_ab_transitions(DIFF, "proba_out", n_bootstrap=10)
        with pytest.raises(InvalidParameterError, match="max_steps"):
            stream.get_ab_steps(DIFF, max_steps=0)

    def test_edges_must_name_events(self, stream) -> None:
        with pytest.raises(InvalidParameterError, match="edges"):
            stream.get_ab_transitions(DIFF, edges=[("cart", "kart")])
        with pytest.raises(InvalidParameterError, match="edges"):
            stream.get_ab_transitions(DIFF, edges=["cart->checkout_start"])
        with pytest.raises(InvalidParameterError, match="edges"):
            stream.get_ab_transitions(DIFF, edges=[])

    def test_id_list_with_silent_ids_raises(self, stream, assignment) -> None:
        silent = assignment.index.difference(pd.unique(stream.df["user_id"]))
        with pytest.raises(PathIdNotFoundError):
            stream.get_ab_transitions(([silent[0], 100001], [100002]))

    def test_empty_arm_and_overlap(self) -> None:
        stream = _tiny()
        with pytest.raises(EmptyEventstreamError):
            stream.get_ab_transitions((["u1"], []))
        with pytest.raises(DiffConfigError):
            stream.get_ab_steps((["u1", "u2"], ["u2"]))


# ---------------------------------------------------------------- graph payload
class TestGraphPayload:
    def test_payload_matches_the_frame(self, stream) -> None:
        frame = stream.get_ab_transitions(DIFF)
        payload = transitions_payload(frame, DIFF, "unique_paths")
        assert payload["n_tested"] == frame.attrs["n_tested"]
        assert len(payload["edges"]) == len(frame)
        first = payload["edges"][0]
        assert first["verdict"] == frame["verdict"].iloc[0]
        assert payload["edges"][0]["p_adjusted"] == pytest.approx(
            frame["p_value_adjusted"].iloc[0]
        )
        top = frame.head(5)  # a filtered frame keeps its attrs
        assert len(transitions_payload(top, DIFF, None)["edges"]) == 5

    def test_mismatches_raise(self, stream, assignment) -> None:
        frame = stream.get_ab_transitions(DIFF)
        with pytest.raises(InvalidParameterError, match="diff"):
            transitions_payload(frame, ("exp_group", "control", "treatment"), None)
        with pytest.raises(InvalidParameterError, match="diff"):
            transitions_payload(frame, ("platform", "treatment", "control"), None)
        with pytest.raises(InvalidParameterError, match="diff"):
            transitions_payload(frame, None, None)
        with pytest.raises(InvalidParameterError, match="edge_weight"):
            transitions_payload(frame, DIFF, "proba_out")
        with pytest.raises(InvalidParameterError, match="ab_transitions"):
            transitions_payload(frame.drop(columns="verdict"), DIFF, None)
        with pytest.raises(InvalidParameterError, match="attrs"):
            transitions_payload(pd.DataFrame(frame.to_dict("list")), DIFF, None)
        # an ITT frame's "arm" diff matches the stream's arm segment
        itt = stream.get_ab_transitions(ARM_DIFF, assignment=assignment)
        assert transitions_payload(itt, DIFF, None)["population"] == "assignment"

    def test_id_lists_must_be_the_same(self) -> None:
        stream = _tiny()
        diff = (["u1", "u2", "u3"], ["u4", "u5", "u6"])
        frame = stream.get_ab_transitions(diff, min_support=1)
        assert transitions_payload(frame, (["u3", "u2", "u1"], diff[1]), None)
        with pytest.raises(InvalidParameterError, match="diff"):
            transitions_payload(frame, (["u1", "u2"], diff[1]), None)
