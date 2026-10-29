"""Acceptance tests for the "Northwind Home" A/B dataset (``datasets/ab_test.py``).

Every planted story of the module docstring has a test here, so the dataset cannot
drift away from the tutorial's narrative. The story tests read the cached fixture of
the default call (``tests/datasets/fixtures/``, refreshed by
``scripts/refresh_ab_fixture.py``); ``test__fixture_matches_the_generator`` checks
that the generator still reproduces it.

TODO: once ``ab/a2-ab-test`` lands, switch the statistics below (pooled z, Holm,
path bootstrap) to ``get_ab_test`` / ``get_ab_bootstrap`` / ``get_sample_ratio`` so the
stories are asserted through the library's own scorecard.
"""

from __future__ import annotations

import math
import pathlib
import time

import numpy as np
import pandas as pd
import pytest
from sklearn.metrics import adjusted_rand_score

from retentioneering.datasets import ab_test
from retentioneering.datasets import load_ab_test, load_ab_test_assignment
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError

FIXTURE_DIR = pathlib.Path(__file__).parent / "fixtures"
DIFF = ("exp_group", "treatment", "control")
FUNNEL_STEPS = ["product_view", "cart", "checkout_start", "payment", "order_success"]
PRE_EXPOSURE_END = [
    {"pattern": "add_to_cart", "offset": -1},
    {"pattern": "quick_add", "offset": -1},
    "path_end",
]
ALPHA = 0.05


# ---------------------------------------------------------------------------
# Statistics helpers (numpy only; replaced by get_ab_test later, see TODO above)
# ---------------------------------------------------------------------------


def _norm_sf2(z: float) -> float:
    """Two-sided normal p-value."""
    return math.erfc(abs(z) / math.sqrt(2))


def _pooled_z(x1: int, n1: int, x2: int, n2: int) -> tuple[float, float, float]:
    """Two-proportion pooled z-test: ``(delta, p_value, pooled_se)``, delta = 1 - 2."""
    p1, p2 = x1 / n1, x2 / n2
    pooled = (x1 + x2) / (n1 + n2)
    se = math.sqrt(pooled * (1 - pooled) * (1 / n1 + 1 / n2))
    return p1 - p2, _norm_sf2((p1 - p2) / se), se


def _wilson(x: int, n: int, z: float = 1.959964) -> tuple[float, float]:
    """Wilson score interval of a proportion."""
    p = x / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return centre - half, centre + half


def _holm(p_values) -> np.ndarray:
    """Holm-adjusted p-values (step-down, monotone, capped at 1)."""
    p = np.asarray(p_values, dtype=float)
    order = np.argsort(p)
    adjusted = np.empty_like(p)
    running = 0.0
    for rank, i in enumerate(order):
        running = max(running, (len(p) - rank) * p[i])
        adjusted[i] = min(running, 1.0)
    return adjusted


def _two_arm_test(flag: pd.Series, arm: pd.Series) -> tuple[float, float, float]:
    """Pooled z on a 0/1 per-user flag; ``arm`` is "treatment"/"control" per user."""
    treated = (arm == "treatment").to_numpy()
    y = flag.to_numpy().astype(bool)
    return _pooled_z(
        int(y[treated].sum()),
        int(treated.sum()),
        int(y[~treated].sum()),
        int((~treated).sum()),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def compact():
    return ab_test._read_fixture(FIXTURE_DIR)


@pytest.fixture(scope="module")
def stream(compact) -> Eventstream:
    events, users = compact
    df = ab_test._assemble(events, users, with_truth=True)
    schema = {key: list(value) for key, value in ab_test._SCHEMA.items()}
    schema["custom_cols"].append("user_type")
    return Eventstream(df, schema)


@pytest.fixture(scope="module")
def assignment(compact) -> pd.DataFrame:
    return ab_test._assignment(compact[1])


@pytest.fixture(scope="module")
def frame(stream) -> pd.DataFrame:
    return stream.to_dataframe()


@pytest.fixture(scope="module")
def user_arm(frame) -> pd.Series:
    return frame.groupby("user_id")["exp_group"].first()


@pytest.fixture(scope="module")
def truth(frame) -> pd.Series:
    return frame.groupby("user_id")["user_type"].first()


@pytest.fixture(scope="module")
def converted(stream) -> pd.Series:
    metric = {"metric": "has_event", "metric_args": {"event": "order_success"}}
    return stream.get_metrics([metric]).iloc[:, 0].astype(bool)


@pytest.fixture(scope="module")
def pre_exposure(stream) -> Eventstream:
    return stream.truncate_paths("path_start", PRE_EXPOSURE_END)


@pytest.fixture(scope="module")
def clusters(pre_exposure) -> pd.Series:
    clustered = pre_exposure.add_clusters(
        "behaviour",
        features=[{"metric": "has_event_bulk"}, {"metric": "active_days"}],
        method="kmeans",
        method_args={"n_clusters": 3, "n_init": 4, "random_state": 42},
        scaler="minmax",
    )
    return clustered.to_dataframe().groupby("user_id")["behaviour"].first()


def _strata_tests(labels: pd.Series, converted: pd.Series, user_arm: pd.Series):
    """Per-stratum conversion test with Holm across strata: {label: (delta, p_adj)}."""
    rows = {}
    for label in sorted(labels.unique()):
        users = labels.index[labels == label]
        delta, p, _ = _two_arm_test(converted.reindex(users), user_arm.reindex(users))
        rows[label] = (delta, p)
    adjusted = _holm([p for _, p in rows.values()])
    return {k: (d, a) for (k, (d, _)), a in zip(rows.items(), adjusted)}


# ---------------------------------------------------------------------------
# Contract
# ---------------------------------------------------------------------------


class TestContract:
    def test__default_returns_configured_eventstream(self) -> None:
        stream = load_ab_test(n_users=300)

        assert isinstance(stream, Eventstream)
        assert stream.schema.path_cols == ["user_id", "session_id"]
        assert stream.schema.segment_cols == ["exp_group", "platform", "cohort_week"]
        assert stream.schema.custom_cols == ["order_value", "order_items"]
        assert "user_type" not in stream.df.columns

    def test__as_dataframe_columns_and_types(self) -> None:
        df = load_ab_test(as_dataframe=True, n_users=300)

        assert list(df.columns) == [
            "user_id",
            "session_id",
            "event",
            "timestamp",
            "exp_group",
            "platform",
            "cohort_week",
            "order_value",
            "order_items",
        ]
        assert set(df["event"]) <= set(ab_test.EVENTS)
        assert str(df["order_items"].dtype) == "Int64"
        orders = df["event"] == "order_success"
        assert df.loc[orders, "order_value"].notna().all()
        assert df.loc[~orders, "order_value"].isna().all()
        assert df.loc[orders, "order_items"].notna().all()
        assert df.loc[~orders, "order_items"].isna().all()
        assert df["timestamp"].min() >= ab_test.START
        assert df["timestamp"].max() < ab_test.START + pd.Timedelta(days=ab_test.N_DAYS)

    def test__with_truth_adds_user_type(self) -> None:
        df = load_ab_test(as_dataframe=True, with_truth=True, n_users=300)
        stream = load_ab_test(with_truth=True, n_users=300)

        assert set(df["user_type"]) == {"scanner", "curator", "other"}
        assert "user_type" in stream.schema.custom_cols
        assert "user_type" not in stream.schema.segment_cols

    def test__fixture_matches_the_generator(self, compact) -> None:
        events, users = ab_test._generate(ab_test.DEFAULT_N_USERS, ab_test.DEFAULT_SEED)

        pd.testing.assert_frame_equal(events, compact[0])
        pd.testing.assert_frame_equal(users, compact[1])

    def test__deterministic_and_seed_sensitive(self) -> None:
        a = load_ab_test(as_dataframe=True, n_users=400, seed=7)
        b = load_ab_test(as_dataframe=True, n_users=400, seed=7)
        c = load_ab_test(as_dataframe=True, n_users=400, seed=8)

        pd.testing.assert_frame_equal(a, b)
        assert not a.equals(c)

    def test__default_generation_under_three_seconds(self) -> None:
        started = time.perf_counter()
        stream = load_ab_test()
        elapsed = time.perf_counter() - started

        assert elapsed < 3.0
        assert stream.df["user_id"].nunique() > 9_000

    def test__rejects_non_positive_n_users(self) -> None:
        with pytest.raises(InvalidParameterError):
            load_ab_test(n_users=0)

    def test__segments_constant_per_user(self, frame) -> None:
        per_user = frame.groupby("user_id")[
            ["exp_group", "platform", "cohort_week", "user_type"]
        ].nunique()

        assert (per_user == 1).all().all()

    def test__quick_add_is_treatment_only(self, frame) -> None:
        groups = frame.loc[frame["event"] == "quick_add", "exp_group"].unique()

        assert list(groups) == ["treatment"]

    def test__assignment_table(self, assignment, frame) -> None:
        public = load_ab_test_assignment(n_users=300)
        first_event = frame.groupby("user_id")["timestamp"].min()

        assert list(public.columns) == ["arm", "assigned_at"]
        assert public.index.name == "user_id"
        assert len(public) == 300
        assert len(assignment) == ab_test.DEFAULT_N_USERS
        silent = assignment.index.difference(first_event.index)
        assert 0.01 < len(silent) / len(assignment) < 0.06
        assigned = assignment["assigned_at"].reindex(first_event.index)
        assert (assigned < first_event).all()
        arm = frame.groupby("user_id")["exp_group"].first()
        assert (assignment["arm"].reindex(arm.index) == arm).all()


# ---------------------------------------------------------------------------
# Planted stories
# ---------------------------------------------------------------------------


class TestStories:
    def test_story1_top_line_grey_and_sample_ratio_fine(
        self, assignment, converted
    ) -> None:
        # ITT over the assignment table: silent users count as not converted.
        flag = converted.reindex(assignment.index, fill_value=False)
        delta, p, se = _two_arm_test(flag, assignment["arm"])
        mde = (1.959964 + 0.841621) * se  # alpha 0.05 two-sided, power 0.8

        assert p > 0.2
        assert abs(delta) < mde / 2
        n_t = int((assignment["arm"] == "treatment").sum())
        n = len(assignment)
        chi2 = (n_t - n / 2) ** 2 / (n / 4)  # 1 df, 50/50 expected
        assert _norm_sf2(math.sqrt(chi2)) > 0.05

    def test_story2_overall_funnel_steps_grey_after_holm(
        self, stream, user_arm
    ) -> None:
        steps = stream.funnel_data(FUNNEL_STEPS, diff=DIFF)["steps"]
        n_t = int((user_arm == "treatment").sum())
        n_c = int((user_arm == "control").sum())
        p_values = [
            _pooled_z(s["funnel1_unique_paths"], n_t, s["funnel2_unique_paths"], n_c)[1]
            for s in steps
        ]

        assert (_holm(p_values) > ALPHA).all()

    def test_story2_conditional_step_naive_p(self, stream) -> None:
        # The naive two-proportion p of a step-to-step rate (previous step as the
        # denominator, pooled z). The funnel never reports it: the denominator is a
        # post-exposure population, which is the tutorial's point.
        steps = stream.funnel_data(FUNNEL_STEPS, diff=DIFF)["steps"]
        naive = {
            cur["step"]: _pooled_z(
                cur["funnel1_unique_paths"],
                prev["funnel1_unique_paths"],
                cur["funnel2_unique_paths"],
                prev["funnel2_unique_paths"],
            )
            for prev, cur in zip(steps, steps[1:])
        }

        assert naive["payment"][1] < ALPHA
        assert naive["payment"][0] < 0
        assert min(p for _, p, _ in naive.values()) < ALPHA

    def test_story3_blind_clusters_recover_types(self, clusters, truth) -> None:
        ari = adjusted_rand_score(truth.reindex(clusters.index), clusters)

        assert ari >= 0.5

    def test_story3_both_signs_survive_holm(
        self, clusters, truth, converted, user_arm
    ) -> None:
        majority = pd.crosstab(clusters, truth.reindex(clusters.index)).idxmax(axis=1)
        results = _strata_tests(clusters, converted, user_arm)

        assert sorted(majority) == ["curator", "other", "scanner"]
        by_type = {majority[label]: res for label, res in results.items()}
        assert by_type["scanner"][0] > 0 and by_type["scanner"][1] < ALPHA
        assert by_type["curator"][0] < 0 and by_type["curator"][1] < ALPHA

    def test_story3_planted_effects(self, truth, converted, user_arm) -> None:
        results = _strata_tests(truth, converted, user_arm)

        assert 0.04 < results["scanner"][0] < 0.15
        assert -0.12 < results["curator"][0] < -0.04
        assert abs(results["other"][0]) < 0.02 and results["other"][1] > ALPHA

    def test_story4_cart_loop_in_treatment_curators_only(self, stream, truth) -> None:
        _, treatment, control = stream.transition_graph_data(
            edge_weight="count", diff=DIFF
        )
        loop = {
            "metric": "matches_pattern",
            "metric_args": {"pattern": "cart->cart->cart"},
        }
        loopers = stream.get_metrics([loop]).iloc[:, 0].astype(bool)
        loopers = loopers.index[loopers]

        assert treatment.loc["cart", "cart"] > 0
        assert control.loc["cart", "cart"] == 0
        assert len(loopers) > 100
        assert (truth.reindex(loopers) == "curator").all()

    def test_story5_novelty_since_assignment(self, frame, assignment) -> None:
        orders = frame.loc[frame["event"] == "order_success", ["user_id", "timestamp"]]
        since = (
            orders["timestamp"]
            - assignment["assigned_at"].reindex(orders["user_id"]).values
        )
        days = since.dt.total_seconds().to_numpy() / 86_400
        end = ab_test.START + pd.Timedelta(days=ab_test.N_DAYS)
        arm = (assignment["arm"] == "treatment").to_numpy()

        def window(lo: int, hi: int) -> tuple[np.ndarray, np.ndarray]:
            users = orders["user_id"].to_numpy()[(days >= lo) & (days < hi)]
            y = assignment.index.isin(users).astype(float)
            # ITT over the users whose window lies inside the data, silent ones included
            eligible = (
                assignment["assigned_at"] + pd.Timedelta(days=hi) <= end
            ).to_numpy()
            return y, eligible

        y_early, e_early = window(0, 14)
        y_late, e_late = window(28, 42)

        def contrast(idx: np.ndarray) -> tuple[float, float, float]:
            t, ee, el, ye, yl = (
                arm[idx],
                e_early[idx],
                e_late[idx],
                y_early[idx],
                y_late[idx],
            )
            early = ye[t & ee].mean() - ye[~t & ee].mean()
            late = yl[t & el].mean() - yl[~t & el].mean()
            return early - late, early, late

        point, early, late = contrast(np.arange(len(arm)))
        rng = np.random.default_rng(0)
        t_idx, c_idx = np.flatnonzero(arm), np.flatnonzero(~arm)
        reps = np.array(
            [
                contrast(
                    np.concatenate(
                        [rng.choice(t_idx, len(t_idx)), rng.choice(c_idx, len(c_idx))]
                    )
                )[0]
                for _ in range(400)
            ]
        )
        low, high = np.quantile(reps, [0.025, 0.975])

        assert early > 0 > late
        assert low > 0
        assert low < point < high

    def test_story6_aov_inconclusive(self, frame) -> None:
        orders = frame.loc[frame["event"] == "order_success"]
        per_user = orders.groupby("user_id").agg(
            value=("order_value", "sum"),
            n=("order_value", "size"),
            arm=("exp_group", "first"),
        )
        rng = np.random.default_rng(0)

        def aov(table: pd.DataFrame, idx: np.ndarray) -> float:
            return (
                table["value"].to_numpy()[idx].sum() / table["n"].to_numpy()[idx].sum()
            )

        t = per_user[per_user["arm"] == "treatment"]
        c = per_user[per_user["arm"] == "control"]
        point = aov(t, np.arange(len(t))) - aov(c, np.arange(len(c)))
        reps = np.array(
            [
                aov(t, rng.integers(0, len(t), len(t)))
                - aov(c, rng.integers(0, len(c), len(c)))
                for _ in range(1000)
            ]
        )
        low, high = np.quantile(reps, [0.025, 0.975])

        assert low < 0 < high
        assert abs(point) < 0.05 * aov(c, np.arange(len(c)))

    def test_story7_sign_flip_by_pre_exposure_length(
        self, pre_exposure, converted, user_arm
    ) -> None:
        binned = pre_exposure.add_segment(
            "length_bin",
            metric_bins={
                "metric": {"metric": "length"},
                "edges": [ab_test._LENGTH_EDGE],
                "segment_levels": ["short", "long"],
            },
        )
        labels = binned.to_dataframe().groupby("user_id")["length_bin"].first()
        results = _strata_tests(labels, converted, user_arm)

        assert results["short"][0] < 0 and results["short"][1] < ALPHA
        assert results["long"][0] > 0 and results["long"][1] < ALPHA

    def test_pre_exposure_window_is_arm_independent(
        self, pre_exposure, user_arm
    ) -> None:
        features = pre_exposure.get_metrics(
            [{"metric": "length"}, {"metric": "has_event_bulk"}]
        )
        arm = user_arm.reindex(features.index)
        assert "quick_add" not in features.columns
        for column in features.columns:
            values = features[column].astype(float)
            t, c = values[arm == "treatment"], values[arm == "control"]
            se = math.sqrt(t.var() / len(t) + c.var() / len(c))
            if se > 0:
                assert _norm_sf2((t.mean() - c.mean()) / se) > 0.001, column

    def test_wilson_helper(self) -> None:
        low, high = _wilson(30, 100)

        assert low == pytest.approx(0.2189, abs=1e-4)
        assert high == pytest.approx(0.3958, abs=1e-4)
