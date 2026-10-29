"""The ``reading`` sentence of the A/B scorecard and of the sample ratio check."""

import math
import warnings

import numpy as np
import pandas as pd
import pytest

from retentioneering.datasets import load_ab_test, load_ab_test_assignment
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.tools import _stats
from retentioneering.tools.ab_test import (
    ABTest,
    _p_text,
    _paths_needed,
    _round_down_125,
    _signed,
    row_reading,
)
from retentioneering.tools.sample_ratio import sample_ratio_reading

EXPLORATORY = "exploratory: strata not pre-exposure (declare strata_pre_exposure=True if they are)"


def _row(**fields) -> dict:
    """A scorecard row with an inconclusive 0/1 metric under Holm over four rows."""
    row = {
        "metric": "conversion",
        "delta": 0.006,
        "delta_se": 0.0085,
        "delta_ci_low": -0.0107,
        "delta_ci_high": 0.0228,
        "rel_delta": 0.025,
        "p_value": 0.12,
        "p_value_adjusted": 0.48,
        "mde_alpha": 0.0125,
        "mde": 0.0239,
        "verdict": "inconclusive",
        "support_warning": "",
    }
    row.update(fields)
    return row


def _read(row: dict, **kwargs) -> str:
    options = {
        "kind": "binary",
        "correction": "holm",
        "alpha": 0.05,
        "min_support": 10,
        "support_count": 2000,
        "non_support_count": 8000,
    }
    options.update(kwargs)
    return row_reading(row, **options)


# ----------------------------------------------------------------- formatting
class TestFormatting:
    @pytest.mark.parametrize(
        "p, text",
        [
            (0.48, "0.48"),
            (0.8, "0.80"),
            (0.0996, "0.10"),
            (0.0007, "0.00070"),
            (0.00003, "< 0.0001"),
            (0.999, "1"),
            (1.0, "1"),
        ],
    )
    def test_p_values_have_two_significant_digits(self, p, text) -> None:
        assert _p_text(p) == text

    def test_signed_numbers_use_an_ascii_minus_and_no_negative_zero(self) -> None:
        assert _signed(-1.07, 2) == "-1.07"
        assert _signed(2.28, 2) == "+2.28"
        assert _signed(-0.001, 2) == "0.00"
        assert float(_signed(-1.07, 2)) == -1.07

    @pytest.mark.parametrize(
        "value, rounded",
        [(1.195, 1), (2.0, 2), (4.99, 2), (5.0, 5), (0.87, 0.5), (0.0123, 0.01)],
    )
    def test_round_down_to_1_2_5(self, value, rounded) -> None:
        assert _round_down_125(value) == pytest.approx(rounded)

    def test_paths_needed_is_the_squared_ratio(self) -> None:
        target, factor = _paths_needed(2.39)
        assert target == 1
        assert factor == pytest.approx(2.39**2)
        assert _paths_needed(float("nan")) is None
        assert _paths_needed(0.0) is None

    def test_the_multiplier_brings_the_mde_down_to_the_target(self) -> None:
        # the claim of the sentence: with factor x the paths, the design detects target
        mde = float(_stats.mde_proportion(0.24, 4960, 5040, 0.05, 0.8))
        target, factor = _paths_needed(mde * 100)
        scaled = float(
            _stats.mde_proportion(0.24, 4960 * factor, 5040 * factor, 0.05, 0.8)
        )
        assert scaled * 100 == pytest.approx(target)


# ----------------------------------------------------------------- templates
class TestInconclusive:
    def test_interval_including_zero_after_the_correction(self) -> None:
        assert _read(_row()) == (
            "interval [-1.07, +2.28] pp includes 0 after holm (p 0.48); effects "
            "outside it are unlikely; the design detects 2.39 pp at alpha 0.0125 — "
            "to detect 1 pp, about 5.7× the paths are needed"
        )

    def test_without_an_effective_correction(self) -> None:
        # a family of one: mde_alpha == alpha, no "after holm"
        text = _read(_row(mde_alpha=0.05))
        assert text.startswith("interval [-1.07, +2.28] pp includes 0 (p 0.48);")
        assert "at alpha 0.05 —" in text
        none = _read(_row(mde_alpha=0.05), correction="none")
        assert "after" not in none

    def test_metric_in_its_own_unit(self) -> None:
        text = _read(
            _row(delta_ci_low=-0.28, delta_ci_high=3.28, mde=2.55, mde_alpha=0.05),
            kind="count",
        )
        assert text.startswith("interval [-0.28, +3.28] includes 0 (p 0.48)")
        assert "pp" not in text
        assert "the design detects 2.55 at alpha 0.05 — to detect 1, about 6.5×" in text

    def test_small_values_keep_three_significant_digits(self) -> None:
        text = _read(
            _row(
                delta_ci_low=-0.0086, delta_ci_high=0.0377, mde=0.0331, mde_alpha=0.05
            ),
            kind="value",
        )
        assert "interval [-0.0086, +0.0377] includes 0" in text
        assert "detects 0.0331 at alpha 0.05 — to detect 0.01, about 11.0×" in text

    def test_interval_excluding_zero_before_the_correction(self) -> None:
        text = _read(
            _row(delta_ci_low=0.0033, delta_ci_high=0.0222, p_value_adjusted=0.057)
        )
        assert text.startswith(
            "interval [+0.33, +2.22] pp excludes 0 per comparison but not after holm "
            "(p 0.057);"
        )
        assert "effects outside it" not in text

    def test_interval_excluding_zero_without_a_correction_is_borderline(self) -> None:
        text = _read(
            _row(
                delta_ci_low=0.0001,
                delta_ci_high=0.02,
                p_value_adjusted=0.051,
                mde_alpha=0.05,
            )
        )
        assert text.startswith(
            "interval [+0.01, +2.00] pp excludes 0 but p 0.051 is not below alpha 0.05 "
            "(borderline"
        )

    def test_no_mde_no_design_clause(self) -> None:
        text = _read(_row(mde=float("nan")))
        assert text == (
            "interval [-1.07, +2.28] pp includes 0 after holm (p 0.48); effects "
            "outside it are unlikely"
        )

    def test_no_interval(self) -> None:
        text = _read(_row(delta_ci_low=float("nan"), delta_ci_high=float("nan")))
        assert text.startswith("no interval; p 0.48 after holm; the design detects")

    def test_invalid_replicates_bootstrap(self) -> None:
        text = _read(
            _row(
                p_value=float("nan"),
                p_value_adjusted=float("nan"),
                delta_ci_low=float("nan"),
                delta_ci_high=float("nan"),
                mde=float("nan"),
                support_warning="invalid_replicates",
            ),
            kind="ratio",
        )
        assert text.startswith("no interval or p-value: more than 1 % of bootstrap")
        assert "estimate shown descriptively" in text
        assert "design" not in text

    def test_constant_arms(self) -> None:
        text = _read(
            _row(p_value=float("nan"), p_value_adjusted=float("nan"), delta_se=0.0)
        )
        assert text == "no p-value: the metric is constant in both arms"

    def test_no_spread(self) -> None:
        text = _read(
            _row(
                p_value=float("nan"),
                p_value_adjusted=float("nan"),
                delta_se=float("nan"),
            ),
            kind="value",
        )
        assert text.startswith("no p-value: the spread cannot be estimated")


class TestSignificant:
    def test_increase(self) -> None:
        text = _read(
            _row(
                verdict="increase",
                delta_ci_low=0.0138,
                delta_ci_high=0.0412,
                p_value_adjusted=0.0007,
                rel_delta=0.12,
            )
        )
        assert text == (
            "interval [+1.38, +4.12] pp excludes 0 after holm (p 0.00070); "
            "relative +12 %"
        )

    def test_decrease_small_relative_change_and_no_relative(self) -> None:
        row = _row(
            verdict="decrease",
            delta_ci_low=-0.0263,
            delta_ci_high=-0.0033,
            p_value_adjusted=0.023,
            rel_delta=-0.042,
            mde_alpha=0.05,
        )
        assert _read(row) == (
            "interval [-2.63, -0.33] pp excludes 0 (p 0.023); relative -4.2 %"
        )
        row["rel_delta"] = float("nan")
        assert _read(row) == "interval [-2.63, -0.33] pp excludes 0 (p 0.023)"

    def test_significant_but_the_interval_touches_zero(self) -> None:
        text = _read(
            _row(
                verdict="increase",
                delta_ci_low=-0.0001,
                delta_ci_high=0.03,
                p_value_adjusted=0.049,
                mde_alpha=0.05,
                rel_delta=float("nan"),
            )
        )
        assert text == (
            "p 0.049 is below alpha 0.05, though the interval [-0.01, +3.00] pp "
            "includes 0 (borderline: test and interval disagree at the threshold)"
        )

    def test_significant_without_an_interval(self) -> None:
        text = _read(
            _row(
                verdict="increase",
                delta_ci_low=float("nan"),
                delta_ci_high=float("nan"),
                p_value_adjusted=0.001,
                rel_delta=float("nan"),
            )
        )
        assert text == "p 0.0010 after holm"

    def test_simultaneous_interval_with_bonferroni(self) -> None:
        text = _read(
            _row(
                verdict="increase",
                delta_ci_low_adj=0.0004,
                delta_ci_high_adj=0.034,
                p_value_adjusted=0.04,
                rel_delta=0.31,
            ),
            correction="bonferroni",
        )
        assert text == (
            "simultaneous interval [+0.04, +3.40] pp excludes 0 after bonferroni "
            "(p 0.040); relative +31 %"
        )


class TestOtherVerdicts:
    def test_insufficient_pooled_support(self) -> None:
        text = _read(
            _row(verdict="insufficient_data", p_value_adjusted=float("nan")),
            support_count=3,
            min_support=20,
        )
        assert text == "pooled support 3 < min_support 20; estimate shown descriptively"

    def test_insufficient_paths_without_the_event(self) -> None:
        text = _read(
            _row(verdict="insufficient_data", p_value_adjusted=float("nan")),
            support_count=400,
            non_support_count=4,
        )
        assert text == (
            "pooled paths without the event 4 < min_support 10; estimate shown "
            "descriptively"
        )

    def test_descriptive(self) -> None:
        text = _read(_row(verdict="descriptive", p_value_adjusted=float("nan")))
        assert text == (
            "active population: not randomised, describes who was active in the period"
        )


class TestFlags:
    def test_exploratory_stratum(self) -> None:
        assert _read(_row(exploratory=True)).endswith("; " + EXPLORATORY)
        assert EXPLORATORY not in _read(_row(exploratory=False))

    def test_incomplete_period(self) -> None:
        assert _read(_row(period_complete=False)).endswith("; window not complete")
        assert "window" not in _read(_row(period_complete=True))

    def test_support_warnings(self) -> None:
        one = _read(_row(support_warning="one_arm_only"), support_count=40)
        assert one.endswith("; one_arm_only: the behaviour exists in one arm only")
        none = _read(
            _row(verdict="insufficient_data", support_warning="no_data"),
            support_count=0,
        )
        assert none == "no data in either arm"
        low = _read(_row(support_warning="low_support"), kind="ratio")
        assert low.endswith(
            "; low_support: under 30 supporting paths in an arm, the bootstrap "
            "interval may under-cover"
        )

    def test_window_comes_first(self) -> None:
        text = _read(_row(), window="within 28 days of assignment")
        assert text.startswith("within 28 days of assignment: interval [-1.07, ")
        none = _read(
            _row(verdict="insufficient_data", support_warning="no_data"),
            support_count=0,
            window="days 49-55 since assignment",
        )
        assert none == "days 49-55 since assignment: no data in either arm"

    def test_several_metrics_name_one_decision_metric(self) -> None:
        text = _read(_row(), several_metrics=True)
        assert text.endswith(
            "; corrected within this metric only; name one decision metric"
        )
        none = _read(_row(mde_alpha=0.05), correction="none", several_metrics=True)
        assert none.endswith("; not corrected across metrics; name one decision metric")
        assert "decision metric" not in _read(_row())

    def test_flags_in_order(self) -> None:
        text = _read(
            _row(exploratory=True, period_complete=False, support_warning="low_support")
        )
        tail = text.split("are needed; ")[1]
        assert tail.startswith(EXPLORATORY + "; window not complete; low_support:")


# ----------------------------------------------------------------- through the tool
def _stream(n: int = 2000, seed: int = 1, effect: float = 0.0) -> Eventstream:
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n):
        arm = "treatment" if i % 2 == 0 else "control"
        platform = "ios" if i % 3 else "web"
        day = pd.Timestamp("2024-01-01") + pd.Timedelta(days=int(rng.integers(0, 21)))
        rows.append((f"u{i}", "view", day, arm, platform))
        if rng.random() < 0.2 + (effect if arm == "treatment" else 0.0):
            rows.append((f"u{i}", "order", day + pd.Timedelta(hours=1), arm, platform))
        if i < 6:
            rows.append((f"u{i}", "chat", day + pd.Timedelta(hours=2), arm, platform))
    df = pd.DataFrame(
        rows, columns=["user_id", "event", "timestamp", "arm", "platform"]
    )
    return Eventstream(df, {"segment_cols": ["arm", "platform"]})


DIFF = ("arm", "treatment", "control")
CONV = {"name": "conv", "metric": "has_event", "metric_args": {"event": "order"}}
CHAT = {"name": "chat", "metric": "has_event", "metric_args": {"event": "chat"}}
ORDERS = {"name": "orders", "metric": "event_count", "metric_args": {"event": "order"}}


class TestScorecardColumn:
    def test_every_row_has_a_reading_last(self) -> None:
        out = ABTest.from_stream(_stream(effect=0.1), DIFF, [CONV, CHAT, ORDERS]).fit()
        assert out.columns[-1] == "reading"
        assert out["reading"].map(lambda r: isinstance(r, str) and r != "").all()
        by_metric = out.set_index("metric")
        assert by_metric.loc["conv", "verdict"] == "increase"
        assert " pp excludes 0 (p " in by_metric.loc["conv", "reading"]
        assert by_metric.loc["chat", "reading"].startswith(
            "over the whole observation period: pooled support 6 < "
        )
        assert (
            out["reading"]
            .str.endswith(
                "; corrected within this metric only; name one decision metric"
            )
            .all()
        )
        assert "pp" not in by_metric.loc["orders", "reading"]

    def test_the_reading_quotes_the_row(self) -> None:
        out = ABTest.from_stream(_stream(), DIFF, [CONV]).fit()
        row = out.iloc[0]
        low, high = row["delta_ci_low"] * 100, row["delta_ci_high"] * 100
        assert f"[{low:+.2f}, {high:+.2f}] pp" in row["reading"]
        assert f"detects {row['mde'] * 100:.2f} pp" in row["reading"]

    def test_strata_and_period_rows(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            strata = ABTest.from_stream(
                _stream(), DIFF, [CONV], strata_col="platform"
            ).fit()
        assert strata["reading"].str.endswith(EXPLORATORY).all()
        assert " after holm (p " in strata["reading"].iloc[0]
        declared = ABTest.from_stream(
            _stream(), DIFF, [CONV], strata_col="platform", strata_pre_exposure=True
        ).fit()
        assert not declared["reading"].str.contains("exploratory").any()
        periods = ABTest.from_stream(_stream(), DIFF, [CONV], period="W").fit()
        incomplete = ~periods["period_complete"]
        assert incomplete.any()
        assert (
            periods.loc[incomplete, "reading"].str.contains("window not complete").all()
        )
        assert not periods.loc[~incomplete, "reading"].str.contains("window").any()
        active = ABTest.from_stream(
            _stream(), DIFF, [CONV], period="W", period_population="active"
        ).fit()
        assert (
            active["reading"]
            .str.contains(r"^calendar week 2024-01-\d\d: active population: not rand")
            .all()
        )

    def test_bootstrap_replicates_have_no_reading(self) -> None:
        reps = ABTest.from_stream(_stream(), DIFF, [CONV], n_bootstrap=100).bootstrap()
        assert "reading" not in reps.columns


@pytest.fixture(scope="module")
def data():
    return load_ab_test(), load_ab_test_assignment()


class TestNoData:
    """A row with no supporting path in either arm is `no_data`, never one-arm."""

    @pytest.mark.parametrize("metric", [CONV, ORDERS, CHAT])
    def test_an_empty_stratum_for_every_metric_type(self, metric) -> None:
        # the "web" stratum never has an order or a chat: 0 paths vs 0 paths
        df = _stream().to_dataframe()
        df = df.loc[~((df["platform"] == "web") & df["event"].isin(["order", "chat"]))]
        stream = Eventstream(
            df[["user_id", "event", "timestamp", "arm", "platform"]],
            {"segment_cols": ["arm", "platform"]},
        )
        out = ABTest.from_stream(
            stream, DIFF, [metric], strata_col="platform", strata_pre_exposure=True
        ).fit()
        web = out.set_index("stratum").loc["web"]
        assert web["verdict"] == "insufficient_data"
        assert web["support_warning"] == "no_data"
        assert web["reading"] == (
            "over the whole observation period: no data in either arm"
        )

    def test_one_arm_only_needs_one_arm_with_support(self) -> None:
        df = _stream().to_dataframe()
        df = df.loc[~((df["arm"] == "control") & (df["event"] == "chat"))]
        stream = Eventstream(
            df[["user_id", "event", "timestamp", "arm", "platform"]],
            {"segment_cols": ["arm", "platform"]},
        )
        row = ABTest.from_stream(stream, DIFF, [CHAT], min_support=1).fit().iloc[0]
        assert row["support_warning"] == "one_arm_only"
        assert row["reading"].endswith(
            "; one_arm_only: the behaviour exists in one arm only"
        )


class TestBundledDataset:
    """The sentences quoted in the docs, on ``load_ab_test()``."""

    def test_overall_conversion_is_inconclusive(self, data) -> None:
        stream, assignment = data
        out = stream.get_ab_test(
            ("exp_group", "treatment", "control"),
            [
                {
                    "name": "conversion",
                    "metric": "has_event",
                    "metric_args": {"event": "order_success"},
                }
            ],
            assignment=assignment,
        )
        assert out["reading"].iloc[0] == (
            "over the whole observation period: "
            "interval [-1.07, +2.28] pp includes 0 (p 0.48); effects outside it are "
            "unlikely; the design detects 2.39 pp at alpha 0.05 — to detect 1 pp, "
            "about 5.7× the paths are needed"
        )

    def test_horizon_conversion_is_an_increase(self, data) -> None:
        stream, assignment = data
        out = stream.get_ab_test(
            ("exp_group", "treatment", "control"),
            [
                {
                    "name": "conversion",
                    "metric": "has_event",
                    "metric_args": {"event": "order_success"},
                }
            ],
            assignment=assignment,
            horizon="14D",
        )
        assert out["reading"].iloc[0] == (
            "within 14 days of assignment: "
            "interval [+1.38, +4.12] pp excludes 0 (p < 0.0001); relative +21 %"
        )

    def test_period_rows_name_their_window(self, data) -> None:
        stream, assignment = data
        metrics = [
            {
                "name": "conversion",
                "metric": "has_event",
                "metric_args": {"event": "order_success"},
            }
        ]
        diff = ("exp_group", "treatment", "control")
        weeks = stream.get_ab_test(diff, metrics, assignment=assignment, period="W")
        week = weeks.set_index("period").loc["2026-03-09/2026-03-15", "reading"]
        assert week.startswith("calendar week 2026-03-09: interval [+0.52, +2.92] pp")
        months = stream.get_ab_test(diff, metrics, assignment=assignment, period="M")
        assert months["reading"].iloc[0].startswith("calendar month 2026-03: ")
        since = stream.get_ab_test(
            diff,
            metrics,
            assignment=assignment,
            period="14D",
            period_basis="since_assignment",
            horizon="42D",
        )
        assert list(since["period"]) == ["0-13", "14-27", "28-41"]
        assert (
            since["reading"]
            .iloc[1]
            .startswith("days 14-27 since assignment, within 42 days of assignment: ")
        )

    def test_empty_since_assignment_bins_are_dropped(self, data) -> None:
        stream, assignment = data
        since = stream.get_ab_test(
            ("exp_group", "treatment", "control"),
            [
                {
                    "name": "conversion",
                    "metric": "has_event",
                    "metric_args": {"event": "order_success"},
                }
            ],
            assignment=assignment,
            period="14D",
            period_basis="since_assignment",
        )
        # "42-55" closes after the end of data for every user: no row
        assert list(since["period"]) == ["0-13", "14-27", "28-41"]
        assert (since["group1_paths"] + since["group2_paths"] > 0).all()


# ----------------------------------------------------------------- sample ratio
def _srm_stream(n_t: int, n_c: int, platform=None) -> Eventstream:
    rows = []
    for i in range(n_t + n_c):
        arm = "treatment" if i < n_t else "control"
        day = pd.Timestamp("2024-01-01") + pd.Timedelta(days=i % 3)
        row = {"user_id": f"u{i}", "event": "visit", "timestamp": day, "arm": arm}
        if platform is not None:
            row["platform"] = platform(i, arm)
        rows.append(row)
    segs = ["arm"] + (["platform"] if platform is not None else [])
    return Eventstream(pd.DataFrame(rows), {"segment_cols": segs})


class TestSampleRatioReading:
    def test_no_mismatch(self) -> None:
        result = _srm_stream(4960, 5040).get_sample_ratio("arm")
        assert result["reading"] == (
            f"expected 50/50, observed 5040/4960 (control/treatment), "
            f"p {_p_text(result['p_value'])}: no mismatch"
        )
        assert math.isclose(result["p_value"], 0.42371, rel_tol=1e-3)

    def test_mismatch(self) -> None:
        result = _srm_stream(4700, 5300).get_sample_ratio("arm")
        assert result["mismatch"]
        assert result["reading"] == (
            "mismatch: expected 50/50, observed 5300/4700 (control/treatment), "
            "p < 0.0001 (alpha 0.001) — the test is invalid until the assignment is "
            "fixed"
        )

    def test_unequal_design(self) -> None:
        result = _srm_stream(200, 800).get_sample_ratio(
            "arm", expected_shares={"control": 0.8, "treatment": 0.2}
        )
        assert result["reading"].startswith(
            "expected 80/20, observed 800/200 (control/treatment), p 1: no mismatch"
        )

    def test_multi_valued_paths(self) -> None:
        result = {
            "levels": pd.DataFrame(
                {
                    "level": ["control", "treatment"],
                    "paths": [500, 500],
                    "expected_share": [0.5, 0.5],
                }
            ),
            "p_value": 1.0,
            "alpha": 0.001,
            "multi_valued_paths": 3,
            "unassigned_paths": 2,
        }
        assert sample_ratio_reading(result) == (
            "mismatch: expected 50/50, observed 500/500 (control/treatment), p 1; "
            "3 path(s) carry more than one arm or disagree with the assignment table "
            "— the test is invalid until the assignment is fixed; 2 path(s) with no "
            "arm in the split are not counted"
        )

    def test_nothing_counted(self) -> None:
        result = {
            "levels": pd.DataFrame(
                {"level": ["a", "b"], "paths": [0, 0], "expected_share": [0.5, 0.5]}
            ),
            "p_value": float("nan"),
            "alpha": 0.001,
            "multi_valued_paths": 0,
            "unassigned_paths": 0,
        }
        assert sample_ratio_reading(result) == (
            "expected 50/50, observed 0/0 (a/b): no path counted, the check cannot run"
        )

    def test_names_the_strata_that_mismatch(self) -> None:
        # android loses half of its treatment paths: the overall split still looks
        # plausible at alpha 0.001, the android group does not
        def platform(i, arm):
            return "android" if i % 2 else "ios"

        rows = []
        for i in range(4000):
            arm = "treatment" if i < 2000 else "control"
            plat = platform(i, arm)
            if arm == "treatment" and plat == "android" and i % 4 == 1:
                continue
            rows.append(
                {
                    "user_id": f"u{i}",
                    "event": "visit",
                    "timestamp": pd.Timestamp("2024-01-01"),
                    "arm": arm,
                    "platform": plat,
                }
            )
        stream = Eventstream(pd.DataFrame(rows), {"segment_cols": ["arm", "platform"]})
        result = stream.get_sample_ratio("arm", strata_col="platform")
        assert result["reading"].endswith(
            "by platform: mismatch in android (adjusted p < 0.0001) — the test is "
            "invalid until the assignment is fixed there"
        )
        clean = _srm_stream(500, 500, platform=platform).get_sample_ratio(
            "arm", strata_col="platform"
        )
        assert clean["reading"].endswith("; by platform: no group mismatches")
