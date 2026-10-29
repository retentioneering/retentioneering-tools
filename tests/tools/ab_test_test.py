"""A/B scorecard (``tools/ab_test.py``): metric specs, the metric matrix, the scorecard."""

import math
import warnings

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    DiffConfigError,
    InvalidMetricConfigError,
    InvalidParameterError,
)
from retentioneering.tools._stats import holm as st_holm
from retentioneering.tools.ab_test import ABMetricSpec, ABTest, parse_metrics

DIFF = ("arm", "treatment", "control")
CONV = {"name": "conv", "metric": "has_event", "metric_args": {"event": "order"}}
ORDERS = {"name": "orders", "metric": "event_count", "metric_args": {"event": "order"}}

# ---------------------------------------------------------------- tiny exact fixture
#   user  arm        assigned_at        events
#   t1    treatment  01-03 00:00        view 01-03 10:00, order 01-03 11:00
#   t2    treatment  01-03 06:00        order 01-03 05:00 (before assignment), view 01-04
#   t3    treatment  01-09 00:00        view 01-09 10:00, order 01-10 10:00
#   t4    treatment  01-05 00:00        (silent: assignment table only)
#   c1    control    01-03 00:00        view 01-03 12:00, view 01-12 12:00
#   c2    control    01-08 00:00        order 01-08 12:00, order 01-16 12:00
#   c3    control    01-10 00:00        view 01-10 12:00
#   x1    control    (not in the table) view 01-05
_EVENTS = [
    ("t1", "view", "2024-01-03 10:00"),
    ("t1", "order", "2024-01-03 11:00"),
    ("t2", "order", "2024-01-03 05:00"),
    ("t2", "view", "2024-01-04 10:00"),
    ("t3", "view", "2024-01-09 10:00"),
    ("t3", "order", "2024-01-10 10:00"),
    ("c1", "view", "2024-01-03 12:00"),
    ("c1", "view", "2024-01-12 12:00"),
    ("c2", "order", "2024-01-08 12:00"),
    ("c2", "order", "2024-01-16 12:00"),
    ("c3", "view", "2024-01-10 12:00"),
    ("x1", "view", "2024-01-05 12:00"),
]
_ASSIGNED = {
    "t1": ("treatment", "2024-01-03 00:00"),
    "t2": ("treatment", "2024-01-03 06:00"),
    "t3": ("treatment", "2024-01-09 00:00"),
    "t4": ("treatment", "2024-01-05 00:00"),
    "c1": ("control", "2024-01-03 00:00"),
    "c2": ("control", "2024-01-08 00:00"),
    "c3": ("control", "2024-01-10 00:00"),
}


def _tiny(shuffle: bool = False) -> Eventstream:
    df = pd.DataFrame(_EVENTS, columns=["user_id", "event", "timestamp"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["arm"] = np.where(df["user_id"].str.startswith("t"), "treatment", "control")
    df["session_id"] = df["user_id"] + "_s1"
    df["platform"] = np.where(df["user_id"].isin(["t1", "c1"]), "ios", "web")
    if shuffle:
        df = df.sample(frac=1.0, random_state=3)
    return Eventstream(
        df,
        {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm", "platform"]},
    )


def _table() -> pd.DataFrame:
    table = pd.DataFrame.from_dict(
        _ASSIGNED, orient="index", columns=["arm", "assigned_at"]
    )
    table["assigned_at"] = pd.to_datetime(table["assigned_at"])
    return table


def _fit(stream, metrics=(CONV,), **kwargs) -> pd.DataFrame:
    kwargs.setdefault("min_support", 1)
    return ABTest.from_stream(stream, DIFF, list(metrics), **kwargs).fit()


# ---------------------------------------------------------------- random fixture
def _random(n: int = 1200, seed: int = 0, effect: float = 0.0) -> Eventstream:
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n):
        arm = "treatment" if i % 2 == 0 else "control"
        platform = "ios" if i % 3 else "web"
        p_order = 0.2 + (effect if arm == "treatment" else 0.0)
        start = pd.Timestamp("2024-01-01") + pd.Timedelta(
            hours=int(rng.integers(0, 24 * 21))
        )
        for j in range(int(rng.integers(1, 6))):
            ts = start + pd.Timedelta(hours=int(rng.integers(0, 24 * 14)))
            event = (
                "order" if rng.random() < p_order else str(rng.choice(["view", "cart"]))
            )
            value = float(rng.lognormal(3, 0.6)) if event == "order" else np.nan
            rows.append((f"u{i:05d}", event, ts, arm, platform, value))
    df = pd.DataFrame(
        rows, columns=["user_id", "event", "timestamp", "arm", "platform", "value"]
    )
    return Eventstream(
        df, {"segment_cols": ["arm", "platform"], "custom_cols": ["value"]}
    )


class TestSpecs:
    def test_defaults_and_dispatch(self) -> None:
        specs = parse_metrics(
            [
                {"metric": "has_event", "metric_args": {"event": "order"}},
                {"metric": "event_count", "metric_args": {"event": "order"}},
                {"metric": "duration"},
                {
                    "name": "aov",
                    "metric": "column_sum",
                    "metric_args": {"column": "value"},
                    "denominator": {
                        "metric": "event_count",
                        "metric_args": {"event": "order"},
                    },
                },
            ]
        )
        assert [s.name for s in specs] == [
            "has_event_order",
            "event_count_order",
            "duration",
            "aov",
        ]
        assert [s.kind for s in specs] == ["binary", "count", "value", "ratio"]
        assert [s.test for s in specs] == [
            "z_test",
            "welch_t_test",
            "welch_t_test",
            "bootstrap",
        ]
        assert specs[3].family_key() == "aov"

    def test_global_and_per_metric_test(self) -> None:
        specs = parse_metrics([CONV, dict(ORDERS, test="bootstrap")], "bootstrap")
        assert [s.test for s in specs] == ["bootstrap", "bootstrap"]
        spec = ABMetricSpec.from_user(dict(CONV, test="welch_t_test"))
        assert spec.test == "welch_t_test"

    @pytest.mark.parametrize(
        "entry",
        [
            {"metric": "has_event_bulk", "metric_args": {"events": ["order"]}},
            {"metric": "event_count_bulk"},
            {"metric": "no_such_metric"},
            {"metric": "first_event_time"},
            {
                "metric": "in_segment",
                "metric_args": {"segment_name": "arm", "segment_level": ["a", "b"]},
            },
            {"metric": "in_segment", "metric_args": {"segment_name": "arm"}},
            dict(CONV, denominator={"metric": "length"}),
            dict(ORDERS, test="z_test"),
            dict(ORDERS, denominator={"metric": "length"}, test="welch_t_test"),
            dict(CONV, test="permutation"),
            dict(CONV, conditions=[]),
            dict(ORDERS, denominator={"metric": "has_event_bulk"}),
            "has_event",
        ],
    )
    def test_rejected_entries(self, entry) -> None:
        with pytest.raises(InvalidMetricConfigError):
            parse_metrics([entry])

    def test_list_level_errors(self) -> None:
        with pytest.raises(InvalidMetricConfigError):
            parse_metrics([])
        with pytest.raises(InvalidMetricConfigError, match="duplicate"):
            parse_metrics([CONV, dict(ORDERS, name="conv")])

    def test_unknown_event_is_reported_with_the_metric(self) -> None:
        with pytest.raises(InvalidMetricConfigError, match="'typo'"):
            _fit(
                _tiny(),
                [
                    {
                        "name": "typo",
                        "metric": "has_event",
                        "metric_args": {"event": "x"},
                    }
                ],
            )


class TestMatrix:
    def test_without_assignment(self) -> None:
        row = _fit(_tiny()).iloc[0]
        # t1, t2, t3 vs c1, c2, c3, x1; t2's order counts without an assignment table
        assert (row.group1_paths, row.group2_paths) == (3, 4)
        assert row.group1_value == pytest.approx(3 / 3)
        assert row.group2_value == pytest.approx(1 / 4)
        assert "group1_silent_paths" not in row.index
        assert row.unassigned_paths == 0

    def test_assignment_silent_paths_and_pre_assignment_events(self) -> None:
        out = _fit(_tiny(), [CONV, ORDERS], assignment=_table()).set_index("metric")
        conv = out.loc["conv"]
        assert (conv.group1_paths, conv.group2_paths) == (4, 3)
        assert conv.group1_value == pytest.approx(2 / 4)  # t2's order is pre-assignment
        assert conv.group2_value == pytest.approx(1 / 3)
        assert (conv.group1_silent_paths, conv.group2_silent_paths) == (1, 0)
        assert conv.unassigned_paths == 1  # x1 is outside the assignment table
        assert out.loc["orders"].group2_value == pytest.approx(2 / 3)

    def test_assignment_series_keeps_pre_assignment_events(self) -> None:
        conv = _fit(_tiny(), assignment=_table()["arm"]).iloc[0]
        assert conv.group1_value == pytest.approx(3 / 4)

    def test_event_order_does_not_matter(self) -> None:
        a = _fit(_tiny(), [CONV, ORDERS], assignment=_table(), period="W")
        b = _fit(_tiny(shuffle=True), [CONV, ORDERS], assignment=_table(), period="W")
        pd.testing.assert_frame_equal(a, b)

    def test_calendar_periods_itt(self) -> None:
        out = _fit(_tiny(), assignment=_table(), period="W", correction="none")
        assert list(out["period"]) == [
            "2024-01-01/2024-01-07",
            "2024-01-08/2024-01-14",
            "2024-01-15/2024-01-21",
        ]
        assert list(out["period_start"]) == list(
            pd.to_datetime(["2024-01-01", "2024-01-08", "2024-01-15"])
        )
        assert list(out["period_end"]) == list(
            pd.to_datetime(["2024-01-08", "2024-01-15", "2024-01-22"])
        )
        # the stream covers 01-03 .. 01-16: only the middle week is whole
        assert list(out["period_complete"]) == [False, True, False]
        # ITT: paths assigned before the period's end, silent ones included
        assert list(out["group1_paths"]) == [3, 4, 4]
        assert list(out["group2_paths"]) == [1, 3, 3]
        np.testing.assert_allclose(out["group1_value"], [1 / 3, 1 / 4, 0.0])
        np.testing.assert_allclose(out["group2_value"], [0.0, 1 / 3, 1 / 3])

    def test_calendar_periods_without_assigned_at(self) -> None:
        out = _fit(_tiny(), assignment=_table()["arm"], period="W")
        # entry = first event; the silent t4 is in every period
        assert list(out["group1_paths"]) == [3, 4, 4]
        assert list(out["group2_paths"]) == [1, 3, 3]

    def test_active_population_is_descriptive(self) -> None:
        out = _fit(_tiny(), assignment=_table(), period="W", period_population="active")
        assert list(out["group1_paths"]) == [2, 1, 0]
        assert list(out["group2_paths"]) == [1, 3, 1]
        assert set(out["verdict"]) == {"descriptive"}
        assert out["p_value"].isna().all() and out["mde"].isna().all()

    def test_since_assignment_periods(self) -> None:
        out = _fit(
            _tiny(),
            [CONV, ORDERS],
            assignment=_table(),
            period="7D",
            period_basis="since_assignment",
        )
        conv = out[out.metric == "conv"]
        # the data end at the last event (01-16 12:00): c3's first window (from
        # 01-10) closes after it, and no path's second window closes in time, so
        # the bin "7-13" is empty in both arms and is not a row
        assert list(conv["period"]) == ["0-6"]
        assert list(conv["period_start"]) == [pd.Timedelta(0)]
        assert list(conv["period_end"]) == [pd.Timedelta(days=7)]
        assert list(conv["immature_paths"]) == [1]
        assert list(conv["period_complete"]) == [False]
        assert list(conv["group1_paths"]) == [4]
        assert list(conv["group2_paths"]) == [2]
        orders = out[out.metric == "orders"]
        np.testing.assert_allclose(orders["group2_value"], [1 / 2])

    def test_horizon(self) -> None:
        out = _fit(_tiny(), [CONV, ORDERS], assignment=_table(), horizon="7D")
        out = out.set_index("metric")
        # c3's window (01-10 + 7 days) ends after the last event (01-16 12:00)
        assert out.loc["conv", "immature_paths"] == 1
        # c2's second order is 8 days after assignment
        assert out.loc["orders", "group2_value"] == pytest.approx(1 / 2)
        out = _fit(_tiny(), assignment=_table(), horizon="10D").iloc[0]
        assert out.immature_paths == 3  # t3, c2, c3 were assigned too late
        assert (out.group1_paths, out.group2_paths) == (3, 1)

    def test_horizon_and_since_assignment_need_assigned_at(self) -> None:
        for kwargs in (
            {"horizon": "7D"},
            {"period": "7D", "period_basis": "since_assignment"},
            {"period_basis": "since_assignment"},
        ):
            with pytest.raises(InvalidParameterError):
                _fit(_tiny(), **kwargs)
            with pytest.raises(InvalidParameterError):
                _fit(_tiny(), assignment=_table()["arm"], **kwargs)

    def test_unit_guard(self) -> None:
        with pytest.raises(InvalidParameterError, match="too narrow"):
            _fit(_tiny(), assignment=_table(), path_col="session_id")

    def test_period_needs_primary_path_col(self) -> None:
        with pytest.raises(InvalidParameterError, match="primary"):
            _fit(_tiny(), period="W", path_col="session_id")

    def test_session_grain_without_assignment(self) -> None:
        out = _fit(_tiny(), path_col="session_id").iloc[0]
        assert (out.group1_paths, out.group2_paths) == (3, 4)

    def test_assignment_must_agree_with_the_segment(self) -> None:
        table = _table()
        table.loc["t1", "arm"] = "control"
        with pytest.raises(DiffConfigError):
            _fit(_tiny(), assignment=table)

    def test_strata_validation(self) -> None:
        with pytest.raises(InvalidParameterError):
            _fit(_tiny(), strata_col="event")
        df = _tiny().to_dataframe()
        df["platform"] = df["platform"].astype(str)
        df.loc[df.index[0], "platform"] = "android"  # t1 now has two platforms
        stream = Eventstream(
            df,
            {
                "path_cols": ["user_id", "session_id"],
                "segment_cols": ["arm", "platform"],
            },
        )
        with pytest.raises(InvalidParameterError, match="changes within"):
            _fit(stream, strata_col="platform")

    def test_strata_labels_and_silent_paths(self) -> None:
        out = _fit(_tiny(), assignment=_table(), strata_col="platform")
        assert list(out["stratum"]) == ["<MISSING>", "ios", "web"]
        assert list(out["group1_paths"]) == [1, 1, 2]  # t4 has no events, no platform
        assert list(out["group2_paths"]) == [0, 1, 2]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"test": "anova"},
            {"correction": "sidak"},
            {"period_basis": "weekly"},
            {"period_population": "all"},
            {"bootstrap_weights": "bayes"},
            {"ci_level": 1.5},
            {"power": 0},
            {"min_support": 0},
            {"n_bootstrap": "many"},
            {"n_bootstrap": 50},
            {"period": "fortnightly"},
            {"horizon": "-3D"},
        ],
    )
    def test_enum_and_range_errors(self, kwargs) -> None:
        if "horizon" in kwargs:
            kwargs["assignment"] = _table()
        with pytest.raises(InvalidParameterError):
            _fit(_tiny(), **kwargs)

    def test_tick_periods_are_anchored_at_the_first_day(self) -> None:
        out = _fit(_tiny(), period="7D")
        assert list(out["period"]) == ["2024-01-03/2024-01-09", "2024-01-10/2024-01-16"]
        # the data end at the last event (01-16 12:00), inside the second bin
        assert list(out["period_complete"]) == [True, False]
        out = _fit(_tiny(), period="D")
        assert out["period"].iloc[0] == "2024-01-03"


# ---------------------------------------------------------------- scorecard
def _counts_stream(cells: dict) -> Eventstream:
    """Users built from ``{(stratum, arm): (paths, converted)}``; one visit each, an order
    for the converted ones."""
    rows = []
    start = pd.Timestamp("2024-01-01")
    i = 0
    for (stratum, arm), (paths, converted) in cells.items():
        for j in range(paths):
            uid = f"u{i:05d}"
            i += 1
            rows.append((uid, "visit", start, arm, stratum))
            if j < converted:
                rows.append(
                    (uid, "order", start + pd.Timedelta(minutes=5), arm, stratum)
                )
    df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm", "group"])
    return Eventstream(df, {"segment_cols": ["arm", "group"]})


_THREE = {
    ("a", "treatment"): (200, 60),
    ("a", "control"): (200, 40),
    ("b", "treatment"): (200, 50),
    ("b", "control"): (200, 50),
    ("c", "treatment"): (200, 30),
    ("c", "control"): (200, 31),
}
AOV = {
    "name": "aov",
    "metric": "column_sum",
    "metric_args": {"column": "value"},
    "denominator": {"metric": "event_count", "metric_args": {"event": "order"}},
}
REVENUE = {
    "name": "revenue",
    "metric": "column_sum",
    "metric_args": {"column": "value"},
}


class TestScorecard:
    def test_columns_of_a_plain_call(self) -> None:
        out = ABTest.from_stream(_random(), DIFF, [CONV]).fit()
        assert list(out.columns) == [
            "metric",
            "population",
            "group1_paths",
            "group2_paths",
            "group1_support",
            "group2_support",
            "conditional",
            "unassigned_paths",
            "group1_value",
            "group1_ci_low",
            "group1_ci_high",
            "group2_value",
            "group2_ci_low",
            "group2_ci_high",
            "delta",
            "delta_se",
            "delta_ci_low",
            "delta_ci_high",
            "rel_delta",
            "rel_delta_ci_low",
            "rel_delta_ci_high",
            "p_value",
            "p_value_adjusted",
            "correction_family",
            "mde_alpha",
            "mde",
            "rel_mde",
            "verdict",
            "test",
            "ci_basis",
            "p_basis",
            "valid_replicates",
            "support_warning",
            "reading",
        ]

    def test_z_test_matches_the_stats_core(self) -> None:
        from retentioneering.tools import _stats as st

        stream = _random(effect=0.1)
        row = ABTest.from_stream(stream, DIFF, [CONV]).fit().iloc[0]
        flags = stream.get_metrics(
            [{"metric": "has_event", "metric_args": {"event": "order"}}]
        )
        arm = stream.get_path_segments("arm").reindex(flags.index)
        y = flags.iloc[:, 0]
        k1, n1 = y[arm == "treatment"].sum(), (arm == "treatment").sum()
        k2, n2 = y[arm == "control"].sum(), (arm == "control").sum()
        assert row.group1_value == pytest.approx(k1 / n1)
        assert (row.delta_ci_low, row.delta_ci_high) == pytest.approx(
            st.newcombe_ci(k1, n1, k2, n2)
        )
        assert row.p_value == pytest.approx(st.two_proportion_z(k1, n1, k2, n2)[1])
        assert (row.rel_delta_ci_low, row.rel_delta_ci_high) == pytest.approx(
            st.delta_log_ratio_ci(k1, n1, k2, n2)
        )
        assert row.mde == pytest.approx(
            st.mde_proportion((k1 + k2) / (n1 + n2), n1, n2)
        )
        assert row.verdict == "increase"
        assert (row.test, row.ci_basis, row.p_basis) == ("z_test", "wilson", "pooled_z")

    def test_welch_matches_scipy(self) -> None:
        from scipy import stats

        stream = _random()
        row = ABTest.from_stream(stream, DIFF, [ORDERS]).fit().iloc[0]
        counts = stream.get_metrics(
            [{"metric": "event_count", "metric_args": {"event": "order"}}]
        ).iloc[:, 0]
        arm = stream.get_path_segments("arm").reindex(counts.index)
        x1, x2 = counts[arm == "treatment"], counts[arm == "control"]
        ref = stats.ttest_ind(x1, x2, equal_var=False)
        assert row.p_value == pytest.approx(ref.pvalue, rel=1e-9)
        ci = ref.confidence_interval(0.95)
        assert (row.delta_ci_low, row.delta_ci_high) == pytest.approx((ci.low, ci.high))
        assert (row.test, row.ci_basis, row.p_basis) == (
            "welch_t_test",
            "welch",
            "welch",
        )
        assert np.isfinite(
            row.rel_delta_ci_low
        )  # joint-bootstrap percentile of the ratio

    def test_ratio_metric_uses_the_bootstrap(self) -> None:
        out = ABTest.from_stream(_random(), DIFF, [AOV, REVENUE], n_bootstrap=400).fit()
        aov = out.set_index("metric").loc["aov"]
        assert (aov.test, aov.ci_basis, aov.p_basis) == (
            "bootstrap",
            "path_bootstrap",
            "bootstrap_se",
        )
        assert aov.valid_replicates == 400
        assert aov.support_warning != "invalid_replicates"
        assert aov.delta_ci_low < aov.delta < aov.delta_ci_high
        assert out.set_index("metric").loc["revenue", "valid_replicates"] is pd.NA

    def test_verdict_comes_from_the_adjusted_p(self) -> None:
        stream = _counts_stream(_THREE)
        plain = ABTest.from_stream(
            stream, DIFF, [CONV], strata_col="group", correction="none"
        ).fit()
        holm = ABTest.from_stream(stream, DIFF, [CONV], strata_col="group").fit()
        a_plain, a_holm = plain.iloc[0], holm.iloc[0]
        assert a_plain.p_value < 0.05 and a_plain.verdict == "increase"
        assert a_holm.p_value == pytest.approx(a_plain.p_value)
        assert a_holm.p_value_adjusted > 0.05 and a_holm.verdict == "inconclusive"
        # the per-comparison interval still excludes 0: it is not what the verdict reads
        assert a_holm.delta_ci_low > 0
        assert list(holm["correction_family"].unique()) == ["metric=conv/strata"]
        assert list(plain["correction_family"].unique()) == ["none"]

    def test_mde_at_the_corrected_alpha(self) -> None:
        from retentioneering.tools import _stats as st

        out = ABTest.from_stream(
            _counts_stream(_THREE), DIFF, [CONV], strata_col="group"
        ).fit()
        row = out.iloc[0]
        assert row.mde_alpha == pytest.approx(0.05 / 3)
        assert row.mde == pytest.approx(
            st.mde_proportion(100 / 400, 200, 200, 0.05 / 3)
        )
        assert row.rel_mde == pytest.approx(row.mde / row.group2_value)

    def test_bonferroni_adds_simultaneous_intervals(self) -> None:
        from retentioneering.tools import _stats as st

        out = ABTest.from_stream(
            _counts_stream(_THREE),
            DIFF,
            [CONV],
            strata_col="group",
            correction="bonferroni",
        ).fit()
        row = out.iloc[0]
        assert (row.delta_ci_low_adj, row.delta_ci_high_adj) == pytest.approx(
            st.newcombe_ci(60, 200, 40, 200, 1 - 0.05 / 3)
        )
        assert row.delta_ci_low_adj < row.delta_ci_low
        assert row.p_value_adjusted == pytest.approx(min(1.0, 3 * row.p_value))

    def test_benjamini_hochberg(self) -> None:
        from retentioneering.tools import _stats as st

        out = ABTest.from_stream(
            _counts_stream(_THREE), DIFF, [CONV], strata_col="group", correction="bh"
        ).fit()
        np.testing.assert_allclose(
            out["p_value_adjusted"], st.benjamini_hochberg(out["p_value"].to_numpy())
        )
        assert "delta_ci_low_adj" not in out.columns

    def test_nan_table(self) -> None:
        cells = {
            ("all", "treatment"): (30, 30),  # everyone converts in both arms
            ("all", "control"): (30, 30),
            ("none", "treatment"): (40, 15),  # control never converts
            ("none", "control"): (40, 0),
        }
        out = (
            ABTest.from_stream(
                _counts_stream(cells), DIFF, [CONV], strata_col="group", min_support=10
            )
            .fit()
            .set_index("stratum")
        )
        both = out.loc["all"]
        assert np.isnan(both.p_value) and np.isnan(both.p_value_adjusted)
        # no path without the event in either arm: below the pooled support rule
        assert both.verdict == "insufficient_data"
        assert both.delta == 0.0 and both.support_warning == ""
        zero = out.loc["none"]
        assert np.isnan(zero.rel_delta) and np.isnan(zero.rel_delta_ci_low)
        # pooled support (15 events): a behaviour of one arm only is tested
        assert zero.verdict == "increase" and zero.p_value < 1e-4
        assert zero.support_warning == "one_arm_only"
        assert zero.group2_value == 0.0

    def test_min_support(self) -> None:
        cells = {("x", "treatment"): (100, 9), ("x", "control"): (100, 30)}
        stream = _counts_stream(cells)
        # the rule is pooled: 39 paths with the event, 161 without
        row = ABTest.from_stream(stream, DIFF, [CONV]).fit().iloc[0]
        assert row.verdict == "decrease" and row.p_value < 0.05
        row = ABTest.from_stream(stream, DIFF, [CONV], min_support=40).fit().iloc[0]
        assert row.verdict == "insufficient_data" and np.isnan(row.p_value)
        assert not np.isnan(row.delta)  # the estimates stay

    def test_strata_columns_and_heterogeneity_guard(self) -> None:
        cells = dict(_THREE)
        cells[("d", "treatment")] = (40, 3)  # fewer than 5 events: not used in Q
        cells[("d", "control")] = (40, 4)
        out = ABTest.from_stream(
            _counts_stream(cells), DIFF, [CONV], strata_col="group", min_support=1
        ).fit()
        assert list(out["stratum"]) == ["a", "b", "c", "d"]
        np.testing.assert_allclose(out["group1_share"], [200 / 640] * 3 + [40 / 640])
        assert out["balance_p_value"].iloc[0] == pytest.approx(1.0)
        assert set(out["heterogeneity_strata_used"]) == {3}
        assert set(out["heterogeneity_dof"]) == {2}
        assert out["heterogeneity_p_value"].nunique() == 1
        # a source column: the recipe cannot show it was fixed before exposure
        assert not out["strata_pre_exposure"].any()
        assert set(out["strata_pre_exposure_source"]) == {"not_pre_exposure"}
        assert out["exploratory"].all()

    def test_stratum_vs_rest(self) -> None:
        cells = {k: v for k, v in _THREE.items() if k[0] in ("a", "b")}
        out = ABTest.from_stream(
            _counts_stream(cells), DIFF, [CONV], strata_col="group"
        ).fit()
        # with two strata the complement of one is the other: the same test as Q
        np.testing.assert_allclose(
            out["stratum_vs_rest_p_value"], out["heterogeneity_p_value"], rtol=1e-9
        )
        a, b = out.iloc[0], out.iloc[1]
        z = (a.delta - b.delta) / np.hypot(a.delta_se, b.delta_se)
        from scipy import stats

        assert a.stratum_vs_rest_p_value == pytest.approx(2 * stats.norm.sf(abs(z)))

    def test_post_exposure_strata_warn(self) -> None:
        stream = _random().add_segment(
            "length_bin", metric_bins={"metric": {"metric": "length"}, "edges": [3]}
        )
        with pytest.warns(UserWarning, match="post-exposure"):
            out = ABTest.from_stream(
                stream, DIFF, [CONV], strata_col="length_bin"
            ).fit()
        assert (out["strata_pre_exposure"] == False).all()  # noqa: E712
        assert out["exploratory"].all()

    def test_pre_exposure_strata_carried_by_path_values(self, recwarn) -> None:
        stream = _random()
        pre = stream.truncate_paths(
            "path_start", [{"pattern": "order", "offset": -1}, "path_end"]
        )
        pre = pre.add_segment(
            "length_bin", metric_bins={"metric": {"metric": "length"}, "edges": [2]}
        )
        labels = pre.get_path_segments("length_bin")
        assert labels.attrs["pre_exposure"] is True
        full = stream.add_segment("length_bin", path_values=labels)
        out = ABTest.from_stream(full, DIFF, [CONV], strata_col="length_bin").fit()
        assert out["strata_pre_exposure"].all()
        assert not out["exploratory"].any()
        assert not [w for w in recwarn if "post-exposure" in str(w.message)]

    def test_invalid_replicates_bootstrap(self) -> None:
        rows = []
        for i in range(60):
            arm = "treatment" if i < 30 else "control"
            rows.append((f"u{i}", "visit", pd.Timestamp("2024-01-01"), arm, np.nan))
            if i in (0, 30, 31, 32, 33, 34, 35):  # one order in treatment
                rows.append(
                    (f"u{i}", "order", pd.Timestamp("2024-01-02"), arm, 10.0 + i)
                )
        df = pd.DataFrame(
            rows, columns=["user_id", "event", "timestamp", "arm", "value"]
        )
        stream = Eventstream(df, {"segment_cols": ["arm"], "custom_cols": ["value"]})
        row = ABTest.from_stream(stream, DIFF, [AOV], min_support=1).fit().iloc[0]
        assert row.support_warning == "invalid_replicates"
        assert "had no value in an arm" in row.reading
        assert row.valid_replicates < 990
        assert np.isnan(row.delta_ci_low) and np.isnan(row.p_value)
        # an absent estimate is NaN: no SE and no MDE either
        assert np.isnan(row.delta_se) and np.isnan(row.mde) and np.isnan(row.rel_mde)
        assert row.group1_value == pytest.approx(10.0)  # the point estimate stays
        assert row.verdict == "inconclusive"

    def test_poisson_weights_and_auto_replicates(self) -> None:
        test = ABTest.from_stream(
            _counts_stream(_THREE),
            DIFF,
            [CONV],
            strata_col="group",
            n_bootstrap="auto",
            bootstrap_weights="poisson",
            test="bootstrap",
        )
        assert test.resolved_n_bootstrap() == 1200
        out = test.fit()
        assert set(out["valid_replicates"]) == {1200}


class TestBootstrapTable:
    def test_long_table_shares_the_scorecard_replicates(self) -> None:
        test = ABTest.from_stream(_random(), DIFF, [CONV, AOV], n_bootstrap=300)
        table = test.bootstrap()
        assert list(table.columns) == [
            "replicate",
            "metric",
            "group1_value",
            "group2_value",
            "delta",
        ]
        assert len(table) == 600
        aov = table[table.metric == "aov"]["delta"].to_numpy()
        row = test.fit().set_index("metric").loc["aov"]
        assert row.delta_ci_low == pytest.approx(np.quantile(aov, 0.025))
        assert row.delta_ci_high == pytest.approx(np.quantile(aov, 0.975))

    def test_contrast_rows(self) -> None:
        test = ABTest.from_stream(
            _random(),
            DIFF,
            [CONV],
            assignment=_random_assignment(),
            period="7D",
            period_basis="since_assignment",
            n_bootstrap=200,
        )
        table = test.bootstrap({"novelty": (["0-6"], ["7-13"])})
        assert list(table.columns) == [
            "replicate",
            "metric",
            "period",
            "contrast",
            "group1_value",
            "group2_value",
            "delta",
        ]
        contrast = table[table.contrast == "novelty"].set_index("replicate")
        assert contrast["period"].isna().all()
        np.testing.assert_allclose(
            contrast["delta"], contrast["group1_value"] - contrast["group2_value"]
        )
        # both sides run on the same paths: those whose 0-6 and 7-13 windows both
        # close before the data end; recomputed by hand from the events
        num, den, sides = test._contrast_columns({"novelty": (["0-6"], ["7-13"])})
        treated = test.matrix.arm == 0
        got = num[treated].sum(axis=0) / den[treated].sum(axis=0)
        df = _random().to_dataframe()
        at = _random_assignment()["assigned_at"]
        end = df["timestamp"].max()
        common = at[(at + pd.Timedelta(days=14) <= end)]
        common = common[
            _random_assignment()["arm"].reindex(common.index) == "treatment"
        ]
        orders = df[(df.event == "order") & df.user_id.isin(common.index)]
        since = orders["timestamp"] - orders["user_id"].map(common)
        early = orders.loc[since < pd.Timedelta(days=7), "user_id"].nunique()
        late = orders.loc[
            (since >= pd.Timedelta(days=7)) & (since < pd.Timedelta(days=14)),
            "user_id",
        ].nunique()
        assert list(sides["side"]) == ["a", "b"]
        np.testing.assert_allclose(got, [early / len(common), late / len(common)])

    def test_contrast_errors(self) -> None:
        with pytest.raises(InvalidParameterError):
            ABTest.from_stream(_random(), DIFF, [CONV]).bootstrap(
                {"x": (["0-6"], ["7-13"])}
            )
        test = ABTest.from_stream(_random(), DIFF, [CONV], period="W")
        with pytest.raises(InvalidParameterError):
            test.bootstrap({"x": (["0-6"], ["7-13"])})

    def test_strata_and_periods(self) -> None:
        table = ABTest.from_stream(
            _random(), DIFF, [CONV], strata_col="platform", period="W", n_bootstrap=100
        ).bootstrap()
        assert {"stratum", "period"} <= set(table.columns)
        assert table.groupby(["stratum", "period"]).size().eq(100).all()


def _random_assignment() -> pd.DataFrame:
    stream = _random()
    first = stream.df.groupby("user_id", observed=True)["timestamp"].min()
    arm = stream.get_path_segments("arm")
    return pd.DataFrame({"arm": arm, "assigned_at": first.reindex(arm.index)})


# ---------------------------------------------------------------- review fixes
def _with_ids(stream: Eventstream, ids) -> Eventstream:
    """The same stream with ``user_id`` replaced through ``ids`` (a callable)."""
    df = stream.to_dataframe()
    df = df[[c for c in df.columns if c not in ("event_index", "event_subindex")]]
    df["user_id"] = ids(df["user_id"])
    return Eventstream(
        df, {"segment_cols": ["arm", "platform"], "custom_cols": ["value"]}
    )


def _tz_tiny(tz: str) -> Eventstream:
    df = _tiny().to_dataframe()
    df = df[["user_id", "event", "timestamp", "arm", "session_id", "platform"]]
    df["timestamp"] = df["timestamp"].dt.tz_localize(tz)
    return Eventstream(
        df,
        {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm", "platform"]},
    )


class TestCalendarPeriodAliases:
    @pytest.mark.parametrize("alias", ["ME", "MS", pd.offsets.MonthEnd()])
    def test_month_spellings_match_m(self, alias) -> None:
        ref = _fit(_random(), period="M", correction="none")
        assert list(ref["period"]) == ["2024-01", "2024-02"]
        assert list(ref["period_start"]) == list(
            pd.to_datetime(["2024-01-01", "2024-02-01"])
        )
        assert list(ref["period_complete"]) == [True, False]
        pd.testing.assert_frame_equal(
            _fit(_random(), period=alias, correction="none"), ref
        )

    @pytest.mark.parametrize("alias", ["Q", "QE", "QS"])
    def test_quarter(self, alias) -> None:
        out = _fit(_random(), period=alias)
        assert list(out["period"]) == ["2024Q1"]
        assert out["period_start"].iloc[0] == pd.Timestamp("2024-01-01")
        assert out["period_end"].iloc[0] == pd.Timestamp("2024-04-01")
        assert not out["period_complete"].iloc[0]

    def test_two_weeks_bin_two_weeks_itt(self) -> None:
        out = _fit(_tiny(), assignment=_table(), period="2W", correction="none")
        # two Monday-to-Sunday weeks per bin, from the week of the first day
        assert list(out["period"]) == ["2024-01-01/2024-01-14", "2024-01-15/2024-01-28"]
        assert list(out["period_end"]) == list(
            pd.to_datetime(["2024-01-15", "2024-01-29"])
        )
        # ITT: paths assigned before the bin's end, silent ones included
        assert list(out["group1_paths"]) == [4, 4]
        assert list(out["group2_paths"]) == [3, 3]
        np.testing.assert_allclose(out["group1_value"], [2 / 4, 0.0])
        np.testing.assert_allclose(out["group2_value"], [1 / 3, 1 / 3])

    def test_multiple_of_another_calendar_unit_is_refused(self) -> None:
        with pytest.raises(InvalidParameterError, match="multiple"):
            _fit(_random(), period="2B")

    @pytest.mark.parametrize("value", [14, 14.0, "14", " 7 ", "1.5"])
    def test_bare_numbers_are_refused(self, value) -> None:
        with pytest.raises(InvalidParameterError, match="14D"):
            _fit(_tiny(), period=value)
        with pytest.raises(InvalidParameterError, match="14D"):
            _fit(_tiny(), assignment=_table(), horizon=value)
        with pytest.raises(InvalidParameterError, match="14D"):
            _fit(
                _tiny(),
                assignment=_table(),
                period=value,
                period_basis="since_assignment",
            )

    def test_horizon_still_takes_a_spelled_out_duration(self) -> None:
        a = _fit(_tiny(), assignment=_table(), horizon="10 days")
        b = _fit(_tiny(), assignment=_table(), horizon="10D")
        pd.testing.assert_frame_equal(a, b)


class TestModeSwitchesAndLevels:
    @pytest.mark.parametrize(
        "kwargs",
        [{"period_population": "active"}, {"period_basis": "since_assignment"}],
    )
    def test_period_modes_need_period(self, kwargs) -> None:
        with pytest.raises(InvalidParameterError, match="without period"):
            _fit(_tiny(), assignment=_table(), **kwargs)

    @pytest.mark.parametrize("level", [0.3, 0.0, 1.0, 1e-6])
    def test_ci_level_range(self, level) -> None:
        with pytest.raises(InvalidParameterError, match="0.5"):
            _fit(_tiny(), ci_level=level)
        assert len(_fit(_tiny(), ci_level=0.5)) == 1

    def test_identical_arms_are_refused(self) -> None:
        same = ("arm", "treatment", "treatment")
        with pytest.raises(DiffConfigError, match="itself"):
            _tiny().get_ab_test(same, [CONV])
        with pytest.raises(DiffConfigError, match="itself"):
            _tiny().get_ab_test(same, [CONV], assignment=_table())


class TestAssignmentIds:
    def test_int_ids_match_a_table_of_strings(self) -> None:
        stream = _with_ids(_random(), lambda ids: ids.str[1:].astype(int))
        table = _random_assignment()
        ref = _fit(_random(), assignment=table)
        table.index = table.index.str[1:].astype(int).astype(str)
        out = _fit(stream, assignment=table)
        cols = ["group1_paths", "group2_paths", "group1_value", "group2_value"]
        pd.testing.assert_frame_equal(out[cols], ref[cols])
        assert (out["group1_silent_paths"] == 0).all()

    def test_no_matching_id_raises(self) -> None:
        table = _random_assignment()
        table.index = table.index.str.upper()
        with pytest.raises(InvalidParameterError, match="only 0 of 1200"):
            _fit(_random(), assignment=table)

    def test_mostly_unmatched_ids_warn_with_the_counts(self) -> None:
        table = _random_assignment()
        ids = list(table.index)
        ids[:700] = [i.upper() for i in ids[:700]]
        table.index = ids
        with pytest.warns(UserWarning, match="only 500 of 1200"):
            _random().get_ab_test(DIFF, [CONV], assignment=table)
        ids[:700] = [i.lower() for i in ids[:700]]
        ids[:500] = [i.upper() for i in ids[:500]]
        table.index = ids
        # 700 of 1200 match: the other 500 are silent assigned paths
        row = _fit(_random(), assignment=table).iloc[0]
        assert row.group1_silent_paths + row.group2_silent_paths == 500

    def test_unit_named_by_the_table_index(self) -> None:
        # session ids equal user ids: the overlap check alone cannot tell the unit
        df = _tiny().to_dataframe()[["user_id", "event", "timestamp", "arm"]]
        df["session_id"] = df["user_id"]
        stream = Eventstream(
            df, {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm"]}
        )
        table = _table().rename_axis("user_id")
        with pytest.raises(InvalidParameterError, match="too narrow"):
            _fit(stream, assignment=table, path_col="session_id")
        # unnamed, the counts tie: ambiguous, so refused too
        with pytest.raises(InvalidParameterError, match="name its index"):
            _fit(stream, assignment=table.rename_axis(None), path_col="session_id")
        # a table declared per session is taken at its word
        _fit(stream, assignment=table.rename_axis("session_id"), path_col="session_id")


class TestTimezones:
    def test_utc_stream_and_utc_table_match_the_naive_run(self) -> None:
        table = _table()
        table["assigned_at"] = table["assigned_at"].dt.tz_localize("UTC")
        for kwargs in (
            {"period": "W"},
            {"horizon": "7D"},
            {"period": "7D", "period_basis": "since_assignment"},
        ):
            ref = _fit(_tiny(), [CONV, ORDERS], assignment=_table(), **kwargs)
            out = _fit(_tz_tiny("UTC"), [CONV, ORDERS], assignment=table, **kwargs)
            for col in ("group1_paths", "group2_paths", "group1_value", "delta"):
                np.testing.assert_array_equal(out[col], ref[col])
        # a naive table on a tz-aware stream is read in the stream's timezone
        ref = _fit(_tiny(), [CONV, ORDERS], assignment=_table(), period="W")
        out = _fit(_tz_tiny("UTC"), [CONV, ORDERS], assignment=_table(), period="W")
        np.testing.assert_array_equal(out["group1_value"], ref["group1_value"])

    def test_calendar_weeks_in_the_stream_timezone(self) -> None:
        ref = _fit(_tiny(), period="W")
        out = _fit(_tz_tiny("Europe/Berlin"), period="W")
        assert list(out["period"]) == list(ref["period"])
        assert list(out["period_start"]) == list(
            ref["period_start"].dt.tz_localize("Europe/Berlin")
        )
        np.testing.assert_array_equal(out["group1_paths"], ref["group1_paths"])

    def test_tz_aware_table_on_a_naive_stream_is_refused(self) -> None:
        table = _table()
        table["assigned_at"] = table["assigned_at"].dt.tz_localize("UTC")
        with pytest.raises(InvalidParameterError, match="naive"):
            _fit(_tiny(), assignment=table)


class TestHorizonCutBins:
    def test_bin_cut_by_the_horizon_is_incomplete(self) -> None:
        rows = []
        for user, arm in (("t1", "treatment"), ("c1", "control")):
            for day in ("01", "09", "20"):
                rows.append((user, "view", pd.Timestamp(f"2024-01-{day}"), arm))
        df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp", "arm"])
        stream = Eventstream(df, {"segment_cols": ["arm"]})
        table = pd.DataFrame(
            {
                "arm": ["treatment", "control"],
                "assigned_at": pd.Timestamp("2024-01-01"),
            },
            index=["t1", "c1"],
        )
        metric = {"name": "views", "metric": "length"}

        def fit(horizon):
            return _fit(
                stream,
                [metric],
                assignment=table,
                period="7D",
                period_basis="since_assignment",
                horizon=horizon,
            )

        # every window of the 7-13 bin closes before the stream's end (01-21), but
        # the 10-day horizon cuts the bin at day 10
        out = fit("10D")
        assert list(out["period"]) == ["0-6", "7-13"]
        assert list(out["immature_paths"]) == [0, 0]
        assert list(out["period_complete"]) == [True, False]
        assert list(fit("14D")["period_complete"]) == [True, True]


class TestWarningLocation:
    def test_post_exposure_warning_points_at_the_caller(self) -> None:
        stream = _random().add_segment(
            "length_bin", metric_bins={"metric": {"metric": "length"}, "edges": [3]}
        )
        with pytest.warns(UserWarning, match="post-exposure") as record:
            stream.get_ab_test(DIFF, [CONV], strata_col="length_bin")
        assert {w.filename for w in record} == {__file__}


# ---------------------------------------------------------------- round 2
class TestPooledSupport:
    def test_a_treatment_that_wipes_out_an_outcome_is_tested(self) -> None:
        cells = {("x", "treatment"): (400, 0), ("x", "control"): (400, 95)}
        row = ABTest.from_stream(_counts_stream(cells), DIFF, [CONV]).fit().iloc[0]
        assert row.verdict == "decrease" and row.p_value < 1e-20
        assert row.support_warning == "one_arm_only"
        assert np.isnan(row.rel_delta_ci_low)  # log ratio undefined at k = 0

    def test_bootstrap_rows_with_few_supporting_paths_are_flagged(self) -> None:
        out = ABTest.from_stream(_random(n=120), DIFF, [AOV], min_support=1).fit()
        assert out.iloc[0].support_warning == "low_support"
        out = ABTest.from_stream(_random(), DIFF, [AOV, CONV]).fit().set_index("metric")
        assert out.loc["aov", "support_warning"] == ""
        assert out.loc["conv", "support_warning"] == ""  # not a bootstrap row

    def test_zero_variance_bootstrap_row_has_no_mde(self) -> None:
        rows = []
        for i in range(60):
            arm = "treatment" if i % 2 else "control"
            rows.append((f"u{i}", "order", pd.Timestamp("2024-01-01"), arm, 10.0))
        df = pd.DataFrame(
            rows, columns=["user_id", "event", "timestamp", "arm", "value"]
        )
        stream = Eventstream(df, {"segment_cols": ["arm"], "custom_cols": ["value"]})
        row = ABTest.from_stream(stream, DIFF, [AOV], min_support=1).fit().iloc[0]
        assert row.delta == 0.0
        assert np.isnan(row.mde) and np.isnan(row.rel_mde)


class TestStrataDeclaration:
    def _length_bins(self, stream: Eventstream) -> Eventstream:
        return stream.add_segment(
            "length_bin", metric_bins={"metric": {"metric": "length"}, "edges": [2]}
        )

    def test_no_op_truncation_is_not_a_pre_exposure_cut(self) -> None:
        noop = self._length_bins(_random().truncate_paths("path_start", "path_end"))
        with pytest.warns(UserWarning, match="post-exposure"):
            out = _fit(noop, strata_col="length_bin")
        assert not out["strata_pre_exposure"].any()
        assert set(out["strata_pre_exposure_source"]) == {"not_pre_exposure"}

    def test_a_cut_at_an_event_reads_as_pre_exposure(self, recwarn) -> None:
        cut = _random().truncate_paths(
            "path_start", [{"pattern": "order", "offset": -1}, "path_end"]
        )
        out = _fit(self._length_bins(cut), strata_col="length_bin")
        assert out["strata_pre_exposure"].all()
        assert set(out["strata_pre_exposure_source"]) == {"recipe"}
        assert not [w for w in recwarn if "strata_col" in str(w.message)]

    def test_declared_strata(self, recwarn) -> None:
        out = _fit(_random(), strata_col="platform", strata_pre_exposure=True)
        assert out["strata_pre_exposure"].all() and not out["exploratory"].any()
        assert set(out["strata_pre_exposure_source"]) == {"declared"}
        assert not [w for w in recwarn if "strata_col" in str(w.message)]
        out = _fit(_random(), strata_col="platform", strata_pre_exposure=False)
        assert set(out["strata_pre_exposure_source"]) == {"not_pre_exposure"}
        assert out["exploratory"].all()

    def test_source_column_warns_and_points_at_the_declaration(self) -> None:
        with pytest.warns(UserWarning, match="strata_pre_exposure=True"):
            out = _fit(_random(), strata_col="platform")
        assert not out["strata_pre_exposure"].any()

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"strata_pre_exposure": "yes", "strata_col": "platform"},
            {"strata_pre_exposure": True},
        ],
    )
    def test_declaration_validated(self, kwargs) -> None:
        with pytest.raises(InvalidParameterError, match="strata_pre_exposure"):
            _fit(_random(), **kwargs)

    def test_strata_from_the_assignment_table(self) -> None:
        table = _table()
        table["signup"] = ["ios", "web", "web", "ios", "ios", "web", "web"]
        out = _fit(
            _tiny(),
            assignment=table,
            strata_col="signup",
            strata_pre_exposure=True,
        )
        # the silent t4 keeps its stratum: no <MISSING> row
        assert list(out["stratum"]) == ["ios", "web"]
        assert list(out["group1_paths"]) == [2, 2]
        assert list(out["group2_paths"]) == [1, 2]
        # the table column is used first even when the stream has the same name
        table = table.rename(columns={"signup": "platform"})
        out = _fit(
            _tiny(), assignment=table, strata_col="platform", strata_pre_exposure=True
        )
        assert list(out["stratum"]) == ["ios", "web"]

    def test_recipe_with_path_values_is_json_and_keeps_the_flag(self) -> None:
        import json

        stream = _random()
        cut = stream.truncate_paths(
            "path_start", [{"pattern": "order", "offset": -1}, "path_end"]
        )
        labels = self._length_bins(cut).get_path_segments("length_bin")
        carried = stream.add_segment("length_bin", path_values=labels)
        recipe = carried.recipe()
        json.dumps(recipe)
        assert recipe[-1]["path_values"]["u00000"] == labels["u00000"]
        assert recipe[-1]["_path_values_pre_exposure"] is True
        out = _fit(carried, strata_col="length_bin")
        assert set(out["strata_pre_exposure_source"]) == {"recipe"}


class TestIntegerIds:
    def test_session_table_at_user_grain_and_ties_are_refused(self) -> None:
        rows = []
        for user in range(1, 41):
            arm = "treatment" if user % 2 else "control"
            for s in range(3):
                session = (user - 1) * 3 + s + 1
                ts = pd.Timestamp("2024-01-01") + pd.Timedelta(hours=user + s)
                rows.append((user, session, "visit", ts, arm))
                if (user + s) % 4 == 0:
                    rows.append(
                        (user, session, "order", ts + pd.Timedelta(1, "m"), arm)
                    )
        df = pd.DataFrame(
            rows, columns=["user_id", "session_id", "event", "timestamp", "arm"]
        )
        stream = Eventstream(
            df, {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm"]}
        )
        users = pd.Series(
            {u: ("treatment" if u % 2 else "control") for u in range(1, 41)}
        )
        # per-user table, per-session analysis: every user id is also a session id
        with pytest.raises(InvalidParameterError, match="too narrow"):
            _fit(stream, assignment=users, path_col="session_id")
        # per-session table read at the user grain: a session_id index raises (R2-01)
        sessions = df.groupby("session_id")["arm"].first()
        with pytest.raises(InvalidParameterError, match="finer path column"):
            _fit(stream, assignment=sessions)
        # unnamed: most ids are not users -> the match-share and the finer-grain
        # warnings, and the 40 that do match disagree with the stream's arm
        with pytest.warns(UserWarning, match="only 40 of 120"):
            with pytest.raises(DiffConfigError, match="disagrees"):
                _fit(stream, assignment=sessions.rename_axis(None))
        # the right pairing runs
        assert len(_fit(stream, assignment=users)) == 1
        named = sessions.rename_axis("session_id")
        assert len(_fit(stream, assignment=named, path_col="session_id")) == 1


@pytest.mark.slow
def test_scorecard_coverage_of_welch_and_bootstrap_rows() -> None:
    """Under a true null, 95 % intervals of a Welch row (orders per user) and a
    bootstrap row (AOV) cover 0 at about the nominal rate (300 paths per arm)."""
    rng = np.random.default_rng(7)
    sims, covered = 200, {"orders": 0, "aov": 0}
    for _ in range(sims):
        n = 600
        users = np.repeat(np.arange(n), 1)
        n_orders = rng.poisson(0.8, n)
        user_rows = np.repeat(users, n_orders)
        df = pd.DataFrame(
            {
                "user_id": np.concatenate([users, user_rows]).astype(str),
                "event": ["visit"] * n + ["order"] * len(user_rows),
                "timestamp": pd.Timestamp("2024-01-01"),
                "value": np.concatenate(
                    [np.full(n, np.nan), rng.lognormal(3, 0.6, len(user_rows))]
                ),
            }
        )
        df["arm"] = np.where(df["user_id"].astype(int) % 2, "treatment", "control")
        stream = Eventstream(df, {"segment_cols": ["arm"], "custom_cols": ["value"]})
        out = (
            ABTest.from_stream(
                stream, DIFF, [ORDERS, AOV], n_bootstrap=200, random_state=int(_)
            )
            .fit()
            .set_index("metric")
        )
        for metric in covered:
            row = out.loc[metric]
            covered[metric] += row.delta_ci_low <= 0 <= row.delta_ci_high
    for metric, hits in covered.items():
        assert 0.91 <= hits / sims <= 0.99, (metric, hits / sims)


class TestUnitGuard:
    """R2-01 / U4: a table or a segment of a different grain than ``path_col``."""

    @staticmethod
    def _stream(n_users: int = 1000, seed: int = 3):
        # integer users 1..1000 and sessions 1..~1500: most session ids are user ids
        rng = np.random.default_rng(seed)
        rows, session = [], 0
        arm_of = {}
        for user in range(1, n_users + 1):
            for _ in range(1 + int(rng.random() < 0.5)):
                session += 1
                arm = "treatment" if rng.random() < 0.5 else "control"
                arm_of[session] = arm
                ts = pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=session)
                rows.append((user, session, "visit", ts))
                buy = 0.25 if arm == "treatment" else 0.10
                if rng.random() < buy:
                    rows.append((user, session, "order", ts + pd.Timedelta(1, "s")))
        df = pd.DataFrame(rows, columns=["user_id", "session_id", "event", "timestamp"])
        stream = Eventstream(df, {"path_cols": ["user_id", "session_id"]})
        return stream, pd.Series(arm_of)

    def test_session_table_at_the_user_grain(self) -> None:
        stream, sessions = self._stream()
        users = stream.df["user_id"].unique()
        overlap = sessions.index.isin(users).mean()
        assert 0.6 < overlap < 0.7  # the round-2 review's 66 %
        diff = ("arm", "treatment", "control")
        named = sessions.rename_axis("session_id")
        with pytest.raises(InvalidParameterError, match="finer path column"):
            ABTest.from_stream(stream, diff, [CONV], assignment=named)
        with pytest.warns(UserWarning, match="are session_id ids and only"):
            ABTest.from_stream(stream, diff, [CONV], assignment=sessions)
        # at its own grain the table runs without a warning
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            out = ABTest.from_stream(
                stream, diff, [CONV], assignment=named, path_col="session_id"
            ).fit()
        assert out["delta"].iloc[0] > 0.1

    def test_segment_constant_per_user_at_the_session_grain(self) -> None:
        rows = []
        for user in range(40):
            arm = "treatment" if user % 2 else "control"
            for s in range(2):
                ts = pd.Timestamp("2024-01-01") + pd.Timedelta(hours=user + s)
                rows.append((f"u{user}", f"u{user}s{s}", "visit", ts, arm))
                if (user + s) % 3 == 0:
                    rows.append(
                        (
                            f"u{user}",
                            f"u{user}s{s}",
                            "order",
                            ts + pd.Timedelta(1, "m"),
                            arm,
                        )
                    )
        df = pd.DataFrame(
            rows, columns=["user_id", "session_id", "event", "timestamp", "arm"]
        )
        stream = Eventstream(
            df, {"path_cols": ["user_id", "session_id"], "segment_cols": ["arm"]}
        )
        diff = ("arm", "treatment", "control")
        with pytest.raises(
            InvalidParameterError, match="constant within every user_id"
        ):
            ABTest.from_stream(stream, diff, [CONV], path_col="session_id")
        with pytest.raises(InvalidParameterError, match="too narrow"):
            stream.get_sample_ratio("arm", path_col="session_id")
        # the user grain runs; a session table named after its grain is taken at
        # its word
        assert len(ABTest.from_stream(stream, diff, [CONV]).fit()) == 1
        table = df.drop_duplicates("session_id").set_index("session_id")["arm"]
        out = ABTest.from_stream(
            stream, diff, [CONV], assignment=table, path_col="session_id"
        ).fit()
        assert out["group1_paths"].iloc[0] == 40


class TestDaylightSaving:
    """R2-02: a naive assigned_at on a tz-aware stream across the autumn change."""

    @staticmethod
    def _stream_and_table(at: str):
        tz = "Europe/Berlin"
        rows, table = [], {}
        for i in range(40):
            arm = "treatment" if i % 2 else "control"
            start = pd.Timestamp("2026-10-20 12:00", tz=tz) + pd.Timedelta(hours=i)
            rows.append((f"u{i}", "visit", start))
            if i % 3 == 0:
                rows.append((f"u{i}", "order", start + pd.Timedelta(minutes=5)))
            table[f"u{i}"] = (arm, start.tz_localize(None))
        table["u0"] = (table["u0"][0], pd.Timestamp(at))
        df = pd.DataFrame(rows, columns=["user_id", "event", "timestamp"])
        frame = pd.DataFrame.from_dict(
            table, orient="index", columns=["arm", "assigned_at"]
        )
        return Eventstream(df), frame

    def test_ambiguous_wall_time_raises_a_clear_error(self) -> None:
        # 02:30 happens twice in Berlin on 2026-10-25
        stream, table = self._stream_and_table("2026-10-25 02:30")
        with pytest.raises(InvalidParameterError, match="occur twice"):
            ABTest.from_stream(stream, DIFF, [CONV], assignment=table, horizon="1D")
        with pytest.raises(InvalidParameterError, match="tz-aware assigned_at"):
            stream.get_sample_ratio(assignment=table)

    def test_skipped_wall_time_moves_forward_and_aware_tables_work(self) -> None:
        # 02:30 does not exist on 2026-03-29: read as 03:00
        stream, table = self._stream_and_table("2026-03-29 02:30")
        out = ABTest.from_stream(stream, DIFF, [CONV], assignment=table).fit()
        assert out["group1_paths"].iloc[0] + out["group2_paths"].iloc[0] == 40
        stream, table = self._stream_and_table("2026-10-25 02:30")
        table["assigned_at"] = table["assigned_at"].dt.tz_localize("UTC")
        assert (
            len(ABTest.from_stream(stream, DIFF, [CONV], assignment=table).fit()) == 1
        )


def test_metric_key_errors_point_at_metric_args() -> None:
    with pytest.raises(InvalidMetricConfigError, match="under 'metric_args'") as info:
        ABMetricSpec.from_user({"metric": "has_event", "event": "order"})
    assert "{'metric': 'has_event', 'metric_args': {'event': 'order'}}" in str(
        info.value
    )
    with pytest.raises(InvalidMetricConfigError, match="'metric_args': "):
        ABMetricSpec.from_user({"has_event": "order"})


# ---------------------------------------------------------------- wave 5 usability
MEAN_VALUE = {
    "name": "mean_value",
    "metric": "column_mean",
    "metric_args": {"column": "value"},
}


class TestPopulationAndSupport:
    """U1 / U8: the population of a row and how many of its paths have a value."""

    def test_population_column(self) -> None:
        stream = _random()
        assert set(_fit(stream)["population"]) == {"stream"}
        out = _fit(stream, assignment=_random_assignment())
        assert set(out["population"]) == {"assignment"}

    def test_support_and_conditional(self) -> None:
        stream = _random()
        out = ABTest.from_stream(stream, DIFF, [CONV, ORDERS, MEAN_VALUE, AOV]).fit()
        out = out.set_index("metric")
        for name in ("conv", "orders"):
            row = out.loc[name]
            assert row.group1_support == row.group1_paths
            assert row.group2_support == row.group2_paths
            assert not row.conditional
            assert "conditional" not in row.reading
        metrics = stream.get_metrics(
            [{"metric": "column_mean", "metric_args": {"column": "value"}}]
        ).iloc[:, 0]
        arm = stream.get_path_segments("arm")
        has = metrics.notna()
        for name in ("mean_value", "aov"):
            row = out.loc[name]
            assert row.group1_support == int((has & (arm == "treatment")).sum())
            assert row.group2_support == int((has & (arm == "control")).sum())
            assert row.conditional
            assert (
                f"conditional: a value for {row.group1_support} of {row.group1_paths}"
                in row.reading
            )


class TestEmptyArmAndConstantRows:
    """R2-03 / S-13: no estimate in an arm, and no spread at all."""

    @staticmethod
    def _no_orders_in_treatment() -> Eventstream:
        df = _random().to_dataframe()
        df = df[~((df["arm"] == "treatment") & (df["event"] == "order"))]
        df = df[["user_id", "event", "timestamp", "arm", "platform", "value"]]
        return Eventstream(
            df, {"segment_cols": ["arm", "platform"], "custom_cols": ["value"]}
        )

    def test_value_and_ratio_with_an_empty_arm(self) -> None:
        stream = self._no_orders_in_treatment()
        out = ABTest.from_stream(stream, DIFF, [MEAN_VALUE, AOV, CONV]).fit()
        out = out.set_index("metric")
        for name in ("mean_value", "aov"):
            row = out.loc[name]
            assert row.verdict == "insufficient_data", name
            assert row.support_warning == "no_data"
            assert np.isnan(row.group1_value) and np.isnan(row.p_value)
            assert "no value in group1" in row.reading
        # a 0/1 metric is still tested: the behaviour exists in one arm only
        conv = out.loc["conv"]
        assert conv.verdict == "decrease"
        assert conv.support_warning == "one_arm_only"

    def test_constant_metric_has_no_standard_error(self) -> None:
        stream = _random()
        ratio = {
            "name": "views_per_view",
            "metric": "event_count",
            "metric_args": {"event": "view"},
            "denominator": {"metric": "event_count", "metric_args": {"event": "view"}},
        }
        out = ABTest.from_stream(stream, DIFF, [ratio], n_bootstrap=200).fit()
        row = out.iloc[0]
        assert row.delta == 0
        assert np.isnan(row.delta_se) and np.isnan(row.mde)
        assert np.isnan(row.delta_ci_low) and np.isnan(row.delta_ci_high)
        assert np.isnan(row.p_value)
        assert "constant in both arms" in row.reading


class TestCorrectionScope:
    """U2: one family per metric (default) or over every metric."""

    def test_all_metrics_in_one_family(self) -> None:
        stream = _random(effect=0.05)
        metrics = [CONV, ORDERS, REVENUE]
        per_metric = ABTest.from_stream(stream, DIFF, metrics).fit()
        joint = ABTest.from_stream(stream, DIFF, metrics, correction_scope="all").fit()
        assert (per_metric["p_value_adjusted"] == per_metric["p_value"]).all()
        expected = st_holm(joint["p_value"].to_numpy())
        np.testing.assert_allclose(joint["p_value_adjusted"], expected)
        assert set(joint["correction_family"]) == {"all_metrics"}
        assert joint["mde_alpha"].tolist() == pytest.approx([0.05 / 3] * 3)
        assert all(
            r.endswith("corrected across all 3 metrics") for r in joint["reading"]
        )
        assert all(
            r.endswith("corrected within this metric only; name one decision metric")
            for r in per_metric["reading"]
        )

    def test_strata_rows_and_auto_replicates(self) -> None:
        stream = _random()
        test = ABTest.from_stream(
            stream,
            DIFF,
            [CONV, ORDERS],
            strata_col="platform",
            strata_pre_exposure=True,
            correction_scope="all",
            n_bootstrap="auto",
        )
        out = test.fit()
        assert set(out["correction_family"]) == {"all_metrics/strata"}
        assert test.resolved_n_bootstrap() == max(1000, math.ceil(20 * 4 / 0.05))

    def test_invalid_scope(self) -> None:
        with pytest.raises(InvalidParameterError, match="correction_scope"):
            ABTest.from_stream(_random(), DIFF, [CONV], correction_scope="metrics")
