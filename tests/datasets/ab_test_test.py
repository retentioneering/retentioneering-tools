"""Acceptance tests for the "Northwind Home" A/B dataset (``datasets/ab_test.py``).

Every planted story of the module docstring has a test here, so the dataset cannot
drift away from the tutorial's narrative. The story tests read the cached fixture of
the default call (``tests/datasets/fixtures/``, refreshed by
``scripts/refresh_ab_fixture.py``); ``test__fixture_matches_the_generator`` checks
that the generator still reproduces it.

The stories are asserted through the library's own tools (``get_ab_test``,
``get_ab_bootstrap``, ``get_sample_ratio``, ``funnel_data(ci_level=...)``), so the
dataset, the tools and the tutorial cannot disagree. Two helpers stay hand-written on
purpose: the naive conditional-step p-value of story 2, which no library tool reports,
and the per-feature balance check of the pre-exposure window.
"""

from __future__ import annotations

import math
import pathlib
import time

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
CONVERSION = {
    "name": "conversion",
    "metric": "has_event",
    "metric_args": {"event": "order_success"},
}


# ---------------------------------------------------------------------------
# Statistics helpers (only what no library tool reports)
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


def _by_stratum(scorecard: pd.DataFrame) -> pd.DataFrame:
    return scorecard.set_index("stratum")


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
        # the design is a 50/50 split; the realised shares stay within a point of it
        shares = assignment["arm"].value_counts(normalize=True)
        assert set(shares.index) == {"treatment", "control"}
        assert (shares - 0.5).abs().max() < 0.01


# ---------------------------------------------------------------------------
# Planted stories
# ---------------------------------------------------------------------------


class TestStories:
    def test_story1_top_line_grey_and_sample_ratio_fine(
        self, stream, assignment
    ) -> None:
        # ITT over the assignment table: silent users count as not converted.
        row = stream.get_ab_test(DIFF, [CONVERSION], assignment=assignment).iloc[0]
        srm = stream.get_sample_ratio("exp_group", assignment=assignment)

        assert row["verdict"] == "inconclusive"
        assert row["p_value"] > 0.2
        assert abs(row["delta"]) < row["mde"] / 2
        assert srm["silent_paths"] > 0
        assert srm["p_value"] > 0.05 and not srm["mismatch"]
        levels = srm["levels"].set_index("level")
        assert (levels["expected_share"] == 0.5).all()
        assert (levels["share"] - 0.5).abs().max() < 0.01

    def test_story2_overall_funnel_steps_grey_after_holm(self, stream) -> None:
        funnel = stream.funnel_data(
            FUNNEL_STEPS, diff=DIFF, ci_level=0.95, correction="holm"
        )
        adjusted = [
            s["delta_conversion_rate_p_value_adjusted"] for s in funnel["steps"]
        ]

        assert all(p > ALPHA for p in adjusted)

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
        self, stream, assignment, clusters, truth
    ) -> None:
        majority = pd.crosstab(clusters, truth.reindex(clusters.index)).idxmax(axis=1)
        carried = stream.add_segment("behaviour", path_values=clusters)
        # the labels were read off a plain frame, so the recipe cannot vouch for
        # them: the window before the first add is declared pre-exposure
        rows = _by_stratum(
            carried.get_ab_test(
                DIFF,
                [CONVERSION],
                assignment=assignment,
                strata_col="behaviour",
                strata_pre_exposure=True,
            )
        )

        assert sorted(majority) == ["curator", "other", "scanner"]
        by_type = {majority[label]: rows.loc[label] for label in majority.index}
        assert by_type["scanner"]["verdict"] == "increase"
        assert by_type["curator"]["verdict"] == "decrease"
        assert rows["strata_pre_exposure"].all()
        assert set(rows["strata_pre_exposure_source"]) == {"declared"}
        assert not rows["exploratory"].any()

    def test_story3_planted_effects(self, stream, assignment) -> None:
        # user_type is the planted truth, not a recipe-visible pre-exposure cut
        with pytest.warns(UserWarning, match="was fixed before exposure"):
            rows = _by_stratum(
                stream.add_segment("user_type").get_ab_test(
                    DIFF, [CONVERSION], assignment=assignment, strata_col="user_type"
                )
            )

        assert 0.04 < rows.loc["scanner", "delta"] < 0.15
        assert -0.12 < rows.loc["curator", "delta"] < -0.04
        assert abs(rows.loc["other", "delta"]) < 0.02
        assert rows.loc["other", "verdict"] == "inconclusive"

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

    def test_story5_novelty_since_assignment(self, stream, assignment) -> None:
        # ITT per since-assignment window, over the users whose window lies inside
        # the data (silent users included); the contrast has a joint interval.
        periods = {"period": "14D", "period_basis": "since_assignment"}
        rows = stream.get_ab_test(
            DIFF, [CONVERSION], assignment=assignment, **periods
        ).set_index("period")
        reps = stream.get_ab_bootstrap(
            DIFF,
            [CONVERSION],
            assignment=assignment,
            contrast={"novelty": (["0-13"], ["28-41"])},
            n_bootstrap=400,
            random_state=0,
            **periods,
        )
        early, late = rows.loc["0-13", "delta"], rows.loc["28-41", "delta"]
        novelty = reps.loc[reps["contrast"] == "novelty", "delta"]
        low, high = novelty.quantile([0.025, 0.975])
        # the contrast runs both sides on the same paths (mature for days 0-41),
        # the population a 42-day horizon gives every bin
        same = stream.get_ab_test(
            DIFF, [CONVERSION], assignment=assignment, horizon="42D", **periods
        ).set_index("period")
        balanced = same.loc["0-13", "delta"] - same.loc["28-41", "delta"]

        assert early > 0 > late
        assert same["group1_paths"].nunique() == 1
        assert low > 0
        assert low < balanced < high
        assert abs(novelty.mean() - balanced) < 0.005

    def test_story6_aov_inconclusive(self, stream, assignment) -> None:
        aov = {
            "name": "aov",
            "metric": "column_sum",
            "metric_args": {"column": "order_value"},
            "denominator": {
                "metric": "event_count",
                "metric_args": {"event": "order_success"},
            },
        }
        row = stream.get_ab_test(DIFF, [aov], assignment=assignment).iloc[0]

        assert row["test"] == "bootstrap"
        assert row["verdict"] == "inconclusive"
        assert row["delta_ci_low"] < 0 < row["delta_ci_high"]
        assert abs(row["delta"]) < 0.05 * row["group2_value"]

    def test_story7_sign_flip_by_pre_exposure_length(
        self, stream, assignment, pre_exposure
    ) -> None:
        binned = pre_exposure.add_segment(
            "length_bin",
            metric_bins={
                "metric": {"metric": "length"},
                "edges": [ab_test._LENGTH_EDGE],
                "segment_levels": ["short", "long"],
            },
        )
        labels = binned.get_path_segments("length_bin")
        rows = _by_stratum(
            stream.add_segment("length_bin", path_values=labels).get_ab_test(
                DIFF, [CONVERSION], assignment=assignment, strata_col="length_bin"
            )
        )

        assert rows.loc["short", "verdict"] == "decrease"
        assert rows.loc["long", "verdict"] == "increase"

    def test_story7_median_split_gives_the_same_picture(
        self, stream, assignment, pre_exposure
    ) -> None:
        labels = pre_exposure.add_segment(
            "length_bin",
            metric_bins={
                "metric": {"metric": "length"},
                "quantiles": 2,
                "segment_levels": ["short", "long"],
            },
        ).get_path_segments("length_bin")
        rows = _by_stratum(
            stream.add_segment("length_bin", path_values=labels).get_ab_test(
                DIFF, [CONVERSION], assignment=assignment, strata_col="length_bin"
            )
        )

        assert rows.loc["short", "verdict"] == "decrease"
        assert rows.loc["long", "verdict"] == "increase"

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
