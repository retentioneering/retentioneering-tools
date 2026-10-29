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
