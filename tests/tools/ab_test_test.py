"""A/B scorecard (``tools/ab_test.py``): metric specs, the metric matrix, the scorecard."""

import numpy as np
import pandas as pd
import pytest

from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import (
    DiffConfigError,
    InvalidMetricConfigError,
    InvalidParameterError,
)
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
    kwargs.setdefault("min_events", 1)
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
        assert "group1_paths_no_events" not in row.index
        assert row.excluded_paths == 0

    def test_assignment_silent_paths_and_pre_assignment_events(self) -> None:
        out = _fit(_tiny(), [CONV, ORDERS], assignment=_table()).set_index("metric")
        conv = out.loc["conv"]
        assert (conv.group1_paths, conv.group2_paths) == (4, 3)
        assert conv.group1_value == pytest.approx(2 / 4)  # t2's order is pre-assignment
        assert conv.group2_value == pytest.approx(1 / 3)
        assert (conv.group1_paths_no_events, conv.group2_paths_no_events) == (1, 0)
        assert conv.excluded_paths == 1  # x1 is outside the assignment table
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
        assert list(conv["period"]) == ["0-6", "7-13"]
        assert list(conv["period_start"]) == [pd.Timedelta(0), pd.Timedelta(days=7)]
        assert list(conv["period_end"]) == [pd.Timedelta(days=7), pd.Timedelta(days=14)]
        # the second window closes after the stream's last day for 5 of 7 paths
        assert list(conv["immature_paths"]) == [0, 5]
        assert list(conv["period_complete"]) == [True, False]
        assert list(conv["group1_paths"]) == [4, 1]
        assert list(conv["group2_paths"]) == [3, 1]
        orders = out[out.metric == "orders"]
        np.testing.assert_allclose(orders["group2_value"], [1 / 3, 0.0])

    def test_horizon(self) -> None:
        out = _fit(_tiny(), [CONV, ORDERS], assignment=_table(), horizon="7D")
        out = out.set_index("metric")
        assert out.loc["conv", "immature_paths"] == 0
        # c2's second order is 8 days after assignment
        assert out.loc["orders", "group2_value"] == pytest.approx(1 / 3)
        out = _fit(_tiny(), assignment=_table(), horizon="10D").iloc[0]
        assert out.immature_paths == 3  # t3, c2, c3 were assigned too late
        assert (out.group1_paths, out.group2_paths) == (3, 1)

    def test_horizon_and_since_assignment_need_assigned_at(self) -> None:
        for kwargs in (
            {"horizon": "7D"},
            {"period": "7D", "period_basis": "since_assignment"},
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
            {"min_events": 0},
            {"n_bootstrap": "many"},
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
        assert out["period_complete"].all()
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
            "group1_paths",
            "group2_paths",
            "excluded_paths",
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
            "alpha_used",
            "mde",
            "rel_mde",
            "verdict",
            "test",
            "ci_basis",
            "p_basis",
            "valid_replicates",
            "low_support",
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
        assert aov.valid_replicates == 400 and not aov.low_support
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
        assert row.alpha_used == pytest.approx(0.05 / 3)
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
                _counts_stream(cells), DIFF, [CONV], strata_col="group", min_events=10
            )
            .fit()
            .set_index("stratum")
        )
        both = out.loc["all"]
        assert np.isnan(both.p_value) and np.isnan(both.p_value_adjusted)
        assert both.verdict == "inconclusive"
        assert both.delta == 0.0
        zero = out.loc["none"]
        assert np.isnan(zero.rel_delta) and np.isnan(zero.rel_delta_ci_low)
        assert zero.verdict == "insufficient_data" and np.isnan(zero.p_value)
        assert zero.group2_value == 0.0  # the estimate itself is still reported

    def test_min_events(self) -> None:
        cells = {("x", "treatment"): (100, 9), ("x", "control"): (100, 30)}
        stream = _counts_stream(cells)
        assert ABTest.from_stream(stream, DIFF, [CONV]).fit().iloc[0].verdict == (
            "insufficient_data"
        )
        row = ABTest.from_stream(stream, DIFF, [CONV], min_events=5).fit().iloc[0]
        assert row.verdict == "decrease" and row.p_value < 0.05

    def test_strata_columns_and_heterogeneity_guard(self) -> None:
        cells = dict(_THREE)
        cells[("d", "treatment")] = (40, 3)  # fewer than 5 events: not used in Q
        cells[("d", "control")] = (40, 4)
        out = ABTest.from_stream(
            _counts_stream(cells), DIFF, [CONV], strata_col="group", min_events=1
        ).fit()
        assert list(out["stratum"]) == ["a", "b", "c", "d"]
        np.testing.assert_allclose(out["group1_share"], [200 / 640] * 3 + [40 / 640])
        assert out["balance_p_value"].iloc[0] == pytest.approx(1.0)
        assert set(out["heterogeneity_strata_used"]) == {3}
        assert set(out["heterogeneity_df"]) == {2}
        assert out["heterogeneity_p_value"].nunique() == 1
        assert out["strata_pre_exposure"].isna().all()  # a source column: unknown
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

    def test_low_support_bootstrap(self) -> None:
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
        row = ABTest.from_stream(stream, DIFF, [AOV], min_events=1).fit().iloc[0]
        assert row.low_support
        assert row.valid_replicates < 990
        assert np.isnan(row.delta_ci_low) and np.isnan(row.p_value)
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
        plain = table[table.contrast.isna()].set_index(["period", "replicate"])
        contrast = table[table.contrast == "novelty"].set_index("replicate")
        assert contrast["period"].isna().all()
        expected = plain.loc["0-6", "group1_value"] - plain.loc["7-13", "group1_value"]
        np.testing.assert_allclose(contrast["group1_value"], expected)
        np.testing.assert_allclose(
            contrast["delta"], contrast["group1_value"] - contrast["group2_value"]
        )

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
