"""Acceptance tests of the bundled A/B datasets other than Northwind Home.

Every planted story of a dataset's "Solutions (spoilers)" section is asserted here
through the library's public tools (``get_ab_test``, ``get_sample_ratio``,
``get_ab_bootstrap``, ``truncate_paths``, ``add_segment``), so a generator cannot
drift away from its docstring. One hand-written helper stays on purpose: the
user-clustered standard error of a session-grain delta, which no library tool
reports (that is the ``session_assignment`` lesson).

Each dataset has a ``check_<name>`` function returning ``{planted verdict: held}``;
the story tests assert all of them at the default seed, and the slow seed sweep
asserts that each holds in at least 90 % of 20 other seeds.
"""

from __future__ import annotations

import math
import time
import warnings

import numpy as np
import pandas as pd
import pytest

from retentioneering.datasets import (
    AB_TESTS,
    list_ab_tests,
    load_ab_test,
    load_ab_test_assignment,
)
from retentioneering.datasets.ab_cases import (
    aa_calibration,
    lumeo_smart_cart,
    push_campaign,
    session_assignment,
    srm_platform_bug,
    trial_conversion,
)
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import DiffConfigError, InvalidParameterError

DIFF = ("exp_group", "treatment", "control")
CASES = {
    case.NAME: case
    for case in (
        lumeo_smart_cart,
        srm_platform_bug,
        session_assignment,
        aa_calibration,
        trial_conversion,
        push_campaign,
    )
}
SWEEP_SEEDS = range(1, 41)


def _has(event: str, name: str | None = None) -> dict:
    return {
        "name": name or event,
        "metric": "has_event",
        "metric_args": {"event": event},
    }


def _quiet(func, *args, **kwargs):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return func(*args, **kwargs)


def _row(frame: pd.DataFrame, key: str = "metric") -> dict:
    return {k: v for k, v in frame.set_index(key).iterrows()}


# ---------------------------------------------------------------------------
# Story checks: {planted verdict: held}, one function per dataset
# ---------------------------------------------------------------------------


LOOP = "smart_cart_open->smart_cart_open->smart_cart_open"
PRE_LUMEO = [{"pattern": "add_to_cart", "offset": -1}, "path_end"]


def lumeo_builders(stream: Eventstream) -> pd.Series:
    """Pre-exposure rule: collections and a wishlist add before the first add."""
    pre = stream.truncate_paths("path_start", PRE_LUMEO)
    flag = pre.get_metrics(
        [
            {
                "metric": "has_all_events",
                "metric_args": {"events": ["collections", "wishlist_add"]},
            }
        ]
    ).iloc[:, 0]
    return pd.Series(np.where(flag.astype(bool), "builder", "rest"), index=flag.index)


def check_lumeo_smart_cart(stream: Eventstream, assignment: pd.DataFrame) -> dict:
    conv = _has("order_success", "conversion")
    overall = stream.get_ab_test(DIFF, [conv], assignment=assignment).iloc[0]
    strata = stream.add_segment(
        "builder", path_values=lumeo_builders(stream)
    ).get_ab_test(
        DIFF,
        [conv],
        assignment=assignment,
        strata_col="builder",
        strata_pre_exposure=True,
    )
    rows = strata.set_index("stratum")
    srm = stream.get_sample_ratio("exp_group", assignment=assignment)
    _, treatment, control = stream.transition_graph_data(edge_weight="count", diff=DIFF)
    reopen = treatment.loc["smart_cart_open", "smart_cart_open"]
    return {
        "sample_ratio_fine": not srm["mismatch"],
        "top_line_inconclusive": overall["verdict"] == "inconclusive",
        "builders_increase": rows.loc["builder", "verdict"] == "increase",
        "reopen_loop_in_treatment_only": reopen > 0
        and control.get("smart_cart_open", pd.Series(dtype=float)).sum() == 0,
    }


def check_srm_platform_bug(stream: Eventstream, assignment: pd.DataFrame) -> dict:
    conv = _has("order_success", "conversion")
    srm = stream.get_sample_ratio("exp_group", assignment=assignment, by="platform")
    by_platform = srm["by_levels"].set_index("by")["mismatch"]
    bug_day = srm_platform_bug.START + pd.Timedelta(days=srm_platform_bug.BUG_DAY)
    early = assignment[assignment["assigned_at"] < bug_day]
    late = assignment[assignment["assigned_at"] >= bug_day]
    srm_early = stream.get_sample_ratio("exp_group", assignment=early)
    srm_late = stream.get_sample_ratio("exp_group", assignment=late)
    naive = stream.get_ab_test(DIFF, [conv], assignment=assignment).iloc[0]
    rows = stream.get_ab_test(
        DIFF,
        [conv],
        assignment=assignment,
        strata_col="platform",
        strata_pre_exposure=True,
    ).set_index("stratum")
    before = stream.get_ab_test(DIFF, [conv], assignment=early).iloc[0]
    return {
        "mismatch": bool(srm["mismatch"]),
        "mismatch_on_android_only": bool(by_platform["android"])
        and not by_platform["ios"]
        and not by_platform["web"],
        "mismatch_from_day_9": bool(srm_late["mismatch"]) and not srm_early["mismatch"],
        "naive_lift": naive["verdict"] == "increase",
        "lift_on_android": rows.loc["android", "verdict"] == "increase",
        "ios_and_web_inconclusive": (
            rows.loc[["ios", "web"], "verdict"] == "inconclusive"
        ).all(),
        "before_day_9_inconclusive": before["verdict"] == "inconclusive",
    }


def clustered_se(stream: Eventstream, event: str) -> tuple[float, float]:
    """Session-grain delta of "has ``event``" and its user-clustered standard error.

    Linearisation of a difference of two means with users as clusters (the CR1
    sandwich): every session's residual over its arm's size, summed per user.
    """
    df = stream.to_dataframe()
    df["y"] = df["event"] == event
    sessions = df.groupby("session_id").agg(
        user=("user_id", "first"), arm=("exp_group", "first"), y=("y", "max")
    )
    t = sessions[sessions["arm"] == "treatment"]
    c = sessions[sessions["arm"] == "control"]
    mt, mc = t["y"].mean(), c["y"].mean()
    contrib = (
        ((t["y"] - mt) / len(t))
        .groupby(t["user"])
        .sum()
        .sub(((c["y"] - mc) / len(c)).groupby(c["user"]).sum(), fill_value=0)
    )
    g = len(contrib)
    return mt - mc, math.sqrt((contrib**2).sum() * g / (g - 1))


FIRST_SESSION_END = [
    {"pattern": "session_start->.*->session_start", "at": 1, "offset": -1},
    "path_end",
]


def check_session_assignment(stream: Eventstream, assignment: pd.DataFrame) -> dict:
    adds = _has("add_to_cart", "adds")
    srm_user = stream.get_sample_ratio("exp_group")
    srm_session = stream.get_sample_ratio(
        "exp_group", assignment=assignment, path_col="session_id"
    )
    try:
        stream.get_ab_test(DIFF, [adds])
        refuses = False
    except DiffConfigError:
        refuses = True
    session = stream.get_ab_test(
        DIFF, [adds], assignment=assignment, path_col="session_id"
    ).iloc[0]
    _, se = clustered_se(stream, "add_to_cart")
    first = stream.truncate_paths("path_start", FIRST_SESSION_END)
    first_row = first.get_ab_test(DIFF, [adds]).iloc[0]
    return {
        "users_in_both_arms": srm_user["multi_valued_paths"] > 0
        and bool(srm_user["mismatch"]),
        "session_split_fine": not srm_session["mismatch"],
        "user_grain_refuses": refuses,
        "session_grain_increase": session["verdict"] == "increase",
        "session_interval_too_narrow": se / session["delta_se"] >= 1.3,
        "first_session_increase": first_row["verdict"] == "increase",
    }


ORDERS = {"metric": "event_count", "metric_args": {"event": "order_success"}}


def aa_metrics(column: str = "order_value") -> list:
    revenue = {"metric": "column_sum", "metric_args": {"column": column}}
    return [
        _has("order_success", "conversion"),
        {"name": "orders_per_user", **ORDERS},
        {"name": "revenue_per_user", **revenue},
        {"name": "revenue_bootstrap", **revenue, "test": "bootstrap"},
        {"name": "aov", **revenue, "denominator": ORDERS},
    ]


def winsorised(stream: Eventstream, quantile: float = 0.99) -> Eventstream:
    df = stream.to_dataframe()
    cap = df["order_value"].quantile(quantile)
    df["order_value_capped"] = df["order_value"].clip(upper=cap)
    schema = {
        "path_cols": stream.schema.path_cols,
        "segment_cols": stream.schema.segment_cols,
        "custom_cols": [*stream.schema.custom_cols, "order_value_capped"],
    }
    return Eventstream(df, schema)


def check_aa_calibration(stream: Eventstream, assignment: pd.DataFrame) -> dict:
    rows = _row(stream.get_ab_test(DIFF, aa_metrics(), assignment=assignment))
    robust = _row(
        winsorised(stream).get_ab_test(
            DIFF, aa_metrics("order_value_capped")[2:], assignment=assignment
        )
    )
    conv = [_has("order_success", "conversion")]
    daily = stream.get_ab_test(DIFF, conv, assignment=assignment, period="D")
    weekly = stream.get_ab_test(DIFF, conv, assignment=assignment, period="W")
    srm = stream.get_sample_ratio("exp_group", assignment=assignment)
    revenue = stream.get_metrics(
        [{"metric": "column_sum", "metric_args": {"column": "order_value"}}]
    ).iloc[:, 0]
    top = revenue.nlargest(6).index
    arm = assignment["arm"].reindex(top)
    return {
        "sample_ratio_fine": not srm["mismatch"],
        "conversion_inconclusive": rows["conversion"]["verdict"] == "inconclusive",
        "orders_inconclusive": rows["orders_per_user"]["verdict"] == "inconclusive",
        "revenue_welch_increase": rows["revenue_per_user"]["verdict"] == "increase",
        "revenue_bootstrap_increase": rows["revenue_bootstrap"]["verdict"]
        == "increase",
        "aov_increase": rows["aov"]["verdict"] == "increase",
        "winsorised_revenue_inconclusive": robust["revenue_per_user"]["verdict"]
        == "inconclusive",
        "winsorised_aov_inconclusive": robust["aov"]["verdict"] == "inconclusive",
        "daily_rows_not_significant": not daily["verdict"]
        .isin(["increase", "decrease"])
        .any(),
        "weekly_rows_inconclusive": (weekly["verdict"] == "inconclusive").all(),
        "six_largest_paths_in_treatment": (arm == "treatment").all(),
    }


def check_trial_conversion(stream: Eventstream, assignment: pd.DataFrame) -> dict:
    sub = [_has("subscribe", "subscribed")]
    whole = stream.get_ab_test(DIFF, sub, assignment=assignment).iloc[0]
    horizon = stream.get_ab_test(DIFF, sub, assignment=assignment, horizon="21D").iloc[
        0
    ]
    bins = stream.get_ab_test(
        DIFF,
        sub,
        assignment=assignment,
        period="7D",
        period_basis="since_assignment",
    ).set_index("period")
    srm = stream.get_sample_ratio("exp_group", assignment=assignment)
    return {
        "sample_ratio_fine": not srm["mismatch"],
        "whole_window_increase": whole["verdict"] == "increase",
        "horizon_21d_inconclusive": horizon["verdict"] == "inconclusive",
        "days_0_6_increase": bins.loc["0-6", "verdict"] == "increase",
        "days_14_20_decrease": bins.loc["14-20", "verdict"] == "decrease",
    }


def check_push_campaign(stream: Eventstream, assignment: pd.DataFrame) -> dict:
    buyers = [_has("purchase", "buyers")]
    itt = stream.get_ab_test(DIFF, buyers, assignment=assignment).iloc[0]
    active = stream.get_ab_test(DIFF, buyers).iloc[0]
    srm_table = stream.get_sample_ratio("exp_group", assignment=assignment)
    srm_stream = stream.get_sample_ratio("exp_group")
    return {
        "table_ratio_fine": not srm_table["mismatch"],
        "stream_ratio_mismatch": bool(srm_stream["mismatch"]),
        "itt_increase": itt["verdict"] == "increase",
        "stream_only_delta_negative": active["delta"] < 0,
        "stream_only_not_increase": active["verdict"] != "increase",
    }


CHECKS = {
    "lumeo_smart_cart": check_lumeo_smart_cart,
    "srm_platform_bug": check_srm_platform_bug,
    "session_assignment": check_session_assignment,
    "aa_calibration": check_aa_calibration,
    "trial_conversion": check_trial_conversion,
    "push_campaign": check_push_campaign,
}


def run_check(name: str, seed: int | None = None) -> dict:
    stream = load_ab_test(name, seed=seed)
    assignment = load_ab_test_assignment(name, seed=seed)
    return _quiet(CHECKS[name], stream, assignment)


# ---------------------------------------------------------------------------
# Fixtures: the default call of every dataset, once per module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", params=list(CASES))
def case(request):
    name = request.param
    stream = load_ab_test(name, with_truth=True)
    return name, stream, load_ab_test_assignment(name)


def _loaded(name: str):
    return load_ab_test(name, with_truth=True), load_ab_test_assignment(name)


@pytest.fixture(scope="module")
def lumeo():
    return _loaded("lumeo_smart_cart")


@pytest.fixture(scope="module")
def srm_bug():
    return _loaded("srm_platform_bug")


@pytest.fixture(scope="module")
def per_session():
    return _loaded("session_assignment")


@pytest.fixture(scope="module")
def aa():
    return _loaded("aa_calibration")


@pytest.fixture(scope="module")
def trial():
    return _loaded("trial_conversion")


@pytest.fixture(scope="module")
def push():
    return _loaded("push_campaign")


# ---------------------------------------------------------------------------
# Registry and loader contract
# ---------------------------------------------------------------------------


class TestRegistry:
    def test__names_and_order(self) -> None:
        assert list(AB_TESTS) == ["northwind_home", *CASES]
        assert list_ab_tests() == dict(AB_TESTS)

    def test__descriptions_are_one_line(self) -> None:
        for description in AB_TESTS.values():
            assert "\n" not in description
            assert 20 < len(description) < 160

    def test__registry_is_read_only(self) -> None:
        with pytest.raises(TypeError):
            AB_TESTS["mine"] = "x"  # type: ignore[index]
        listed = list_ab_tests()
        listed["mine"] = "x"
        assert "mine" not in AB_TESTS

    def test__default_is_northwind_home(self) -> None:
        default = load_ab_test(as_dataframe=True, n_users=300)
        by_none = load_ab_test(None, as_dataframe=True, n_users=300)
        by_name = load_ab_test("northwind_home", as_dataframe=True, n_users=300)

        pd.testing.assert_frame_equal(default, by_none)
        pd.testing.assert_frame_equal(default, by_name)
        pd.testing.assert_frame_equal(
            load_ab_test_assignment(n_users=300),
            load_ab_test_assignment("northwind_home", n_users=300),
        )

    @pytest.mark.parametrize("loader", [load_ab_test, load_ab_test_assignment])
    @pytest.mark.parametrize("name", ["northwind", "", 3, ["push_campaign"]])
    def test__unknown_name_lists_the_names(self, loader, name) -> None:
        with pytest.raises(InvalidParameterError) as error:
            loader(name)

        for known in AB_TESTS:
            assert known in str(error.value)

    @pytest.mark.parametrize("name", list(CASES))
    @pytest.mark.parametrize(
        "kwargs", [{"n_users": 0}, {"n_users": 2.5}, {"seed": -1}, {"seed": "1"}]
    )
    def test__rejects_bad_size_and_seed(self, name, kwargs) -> None:
        with pytest.raises(InvalidParameterError):
            load_ab_test(name, **kwargs)
        with pytest.raises(InvalidParameterError):
            load_ab_test_assignment(name, **kwargs)

    def test__options_are_keyword_only(self) -> None:
        with pytest.raises(TypeError):
            load_ab_test("push_campaign", True)  # type: ignore[misc]


class TestContract:
    @pytest.mark.parametrize("name", list(CASES))
    def test__frame_columns_and_schema(self, name) -> None:
        module = CASES[name]
        df = load_ab_test(name, as_dataframe=True, n_users=500)
        stream = load_ab_test(name, n_users=500)
        truth = load_ab_test(name, as_dataframe=True, with_truth=True, n_users=500)
        segments = module.SCHEMA["segment_cols"]
        custom = module.SCHEMA["custom_cols"]

        assert list(df.columns) == [
            "user_id",
            "session_id",
            "event",
            "timestamp",
            *segments,
            *custom,
        ]
        assert list(truth.columns) == [*df.columns, *module.TRUTH_COLS]
        assert set(df["event"]) <= set(module.EVENTS)
        assert stream.schema.path_cols == ["user_id", "session_id"]
        assert stream.schema.segment_cols == segments
        assert stream.schema.custom_cols == custom
        with_truth = load_ab_test(name, with_truth=True, n_users=500)
        assert with_truth.schema.custom_cols == [*custom, *module.TRUTH_COLS]
        assert with_truth.schema.segment_cols == segments

    @pytest.mark.parametrize("name", list(CASES))
    def test__deterministic_and_seed_sensitive(self, name) -> None:
        a = load_ab_test(name, as_dataframe=True, n_users=600, seed=7)
        b = load_ab_test(name, as_dataframe=True, n_users=600, seed=7)
        c = load_ab_test(name, as_dataframe=True, n_users=600, seed=8)

        pd.testing.assert_frame_equal(a, b)
        assert not a.equals(c)
        pd.testing.assert_frame_equal(
            load_ab_test_assignment(name, n_users=600, seed=7),
            load_ab_test_assignment(name, n_users=600, seed=7),
        )

    @pytest.mark.parametrize("name", list(CASES))
    def test__default_size_generates_quickly(self, name) -> None:
        started = time.perf_counter()
        stream = load_ab_test(name)
        load_ab_test_assignment(name)
        elapsed = time.perf_counter() - started

        assert elapsed < 4.0  # both calls; each about a second or less
        assert stream.df["user_id"].nunique() > 0.25 * CASES[name].DEFAULT_N_USERS

    def test__user_id_ranges_do_not_overlap(self, case) -> None:
        name, stream, assignment = case
        first = CASES[name].FIRST_USER_ID
        ids = stream.df["user_id"]

        assert ids.min() >= first
        assert ids.max() < first + 100_000
        assert first % 100_000 == 1
        assert len({c.FIRST_USER_ID for c in CASES.values()} | {100_001}) == 7

    def test__assignment_table(self, case) -> None:
        name, stream, assignment = case
        unit = "session_id" if name == "session_assignment" else "user_id"
        frame = stream.to_dataframe()
        arm = frame.groupby(unit)["exp_group"].first()

        assert assignment.index.name == unit
        assert assignment.index.is_unique
        assert list(assignment.columns[:2]) == ["arm", "assigned_at"]
        assert set(assignment["arm"]) == {"treatment", "control"}
        # every path on the stream is in the table, with the same arm
        assert arm.index.isin(assignment.index).all()
        assert (assignment["arm"].reindex(arm.index) == arm).all()
        if "platform" in assignment.columns:
            platform = frame.groupby(unit)["platform"].first()
            assert (assignment["platform"].reindex(platform.index) == platform).all()

    def test__design_is_a_50_50_split(self, case) -> None:
        name, stream, assignment = case
        if name == "srm_platform_bug":  # the planted bug: see its story tests
            return
        shares = assignment["arm"].value_counts(normalize=True)

        assert (shares - 0.5).abs().max() < 0.015

    def test__events_follow_assignment(self, case) -> None:
        name, stream, assignment = case
        unit = "session_id" if name == "session_assignment" else "user_id"
        first = stream.to_dataframe().groupby(unit)["timestamp"].min()
        early = first < assignment["assigned_at"].reindex(first.index)

        if name == "push_campaign":  # planted: about 5 % active before the push
            assert 0.035 < early.sum() / len(assignment) < 0.065
        else:
            assert not early.any()

    def test__segments_constant_per_unit(self, case) -> None:
        name, stream, _ = case
        frame = stream.to_dataframe()
        unit = "session_id" if name == "session_assignment" else "user_id"
        columns = [*CASES[name].SCHEMA["segment_cols"], *CASES[name].TRUTH_COLS]
        per_unit = frame.groupby(unit)[columns].nunique(dropna=False)

        assert (per_unit == 1).all().all()

    def test__sessions_within_users(self, case) -> None:
        _, stream, _ = case
        frame = stream.to_dataframe()
        prefix = frame["session_id"].str.rsplit("_", n=1).str[0]

        assert (prefix == frame["user_id"].astype(str)).all()


# ---------------------------------------------------------------------------
# Planted stories at the default seed
# ---------------------------------------------------------------------------


def test_planted_verdicts_hold_at_the_default_seed(case) -> None:
    name, stream, assignment = case
    held = _quiet(CHECKS[name], stream, assignment)

    assert held and all(held.values()), {k: v for k, v in held.items() if not v}


class TestLumeoSmartCart:
    def test__builder_rule_recovers_the_planted_segment(self, lumeo) -> None:
        stream, _ = lumeo
        rule = lumeo_builders(stream)
        truth = stream.to_dataframe().groupby("user_id")["user_type"].first()
        truth = truth.reindex(rule.index) == "collection_builder"
        found = rule == "builder"

        assert (found & truth).sum() / truth.sum() > 0.95  # recall
        assert (found & truth).sum() / found.sum() > 0.85  # precision

    def test__planted_effects(self, lumeo) -> None:
        stream, assignment = lumeo
        rows = _quiet(
            stream.add_segment("user_type").get_ab_test,
            DIFF,
            [_has("order_success", "conversion")],
            assignment=assignment,
            strata_col="user_type",
            strata_pre_exposure=True,
        ).set_index("stratum")

        assert 0.06 < rows.loc["collection_builder", "delta"] < 0.18
        assert -0.04 < rows.loc["other", "delta"] < 0.015

    def test__loopers_are_builders_who_convert_less(self, lumeo) -> None:
        stream, _ = lumeo
        frame = stream.to_dataframe()
        loop = {
            "metric": "matches_pattern",
            "metric_args": {"pattern": LOOP},
        }
        metrics = stream.get_metrics([loop, _has("order_success")])
        users = frame.groupby("user_id")[["user_type", "exp_group", "platform"]].first()
        users = users.join(metrics.set_axis(["loop", "order"], axis=1))
        builders = users[
            (users["user_type"] == "collection_builder")
            & (users["exp_group"] == "treatment")
        ]
        loopers = users[users["loop"].astype(bool)]

        assert (loopers["user_type"] == "collection_builder").mean() > 0.8
        assert loopers["platform"].isin(["ios", "android"]).mean() > 0.75
        # descriptive, post-exposure: loopers convert below the builders who do not loop
        by_loop = builders.groupby(builders["loop"].astype(bool))["order"].mean()
        assert by_loop[True] < by_loop[False] - 0.08

    def test__platform_fixed_at_signup(self, lumeo) -> None:
        stream, assignment = lumeo
        rows = stream.get_ab_test(
            DIFF,
            [_has("order_success", "conversion")],
            assignment=assignment,
            strata_col="platform",
            strata_pre_exposure=True,
        )

        assert set(rows["stratum"]) == {"ios", "android", "web"}
        assert set(rows["strata_pre_exposure_source"]) == {"declared"}
        assert not rows["exploratory"].any()


class TestSrmPlatformBug:
    def test__later_days_lose_treatment_users(self, srm_bug) -> None:
        stream, assignment = srm_bug
        by_day = stream.get_sample_ratio("exp_group", assignment=assignment, by="day")
        levels = by_day["levels"]
        share = levels[
            (levels["level"] == "treatment") & (levels["by"] != "<ALL>")
        ].set_index("by")["share"]
        day = pd.to_datetime(share.index)
        bug = srm_platform_bug.START + pd.Timedelta(days=srm_platform_bug.BUG_DAY)

        assert abs(share[day < bug].mean() - 0.5) < 0.02
        assert share[day >= bug].mean() < 0.46

    def test__naive_lift_size(self, srm_bug) -> None:
        stream, assignment = srm_bug
        row = stream.get_ab_test(
            DIFF, [_has("order_success", "conversion")], assignment=assignment
        ).iloc[0]

        assert 0.015 < row["delta"] < 0.05

    def test__lost_users_are_low_intent(self, srm_bug) -> None:
        stream, assignment = srm_bug
        users = (
            stream.to_dataframe()
            .groupby("user_id")[["exp_group", "platform", "intent"]]
            .first()
        )
        android = users[users["platform"] == "android"]
        high = android.groupby("exp_group")["intent"].apply(
            lambda s: (s == "high").mean()
        )

        assert high["treatment"] > high["control"] + 0.1


class TestSessionAssignment:
    def test__users_in_both_arms(self, per_session) -> None:
        stream, _ = per_session
        arms = stream.get_path_segments("exp_group")
        n_sessions = stream.to_dataframe().groupby("user_id")["session_id"].nunique()

        assert (arms == "<MULTI>").sum() == stream.get_sample_ratio("exp_group")[
            "multi_valued_paths"
        ]
        assert 0.3 < (n_sessions > 1).mean() < 0.6
        assert n_sessions.max() > 100

    def test__session_table_refused_at_user_grain(self, per_session) -> None:
        stream, assignment = per_session
        with pytest.raises(DiffConfigError):
            stream.get_ab_test(DIFF, [_has("add_to_cart")], assignment=assignment)

    def test__clustered_se_ratio(self, per_session) -> None:
        stream, assignment = per_session
        row = stream.get_ab_test(
            DIFF, [_has("add_to_cart")], assignment=assignment, path_col="session_id"
        ).iloc[0]
        delta, se = clustered_se(stream, "add_to_cart")

        assert delta == pytest.approx(row["delta"])
        assert 1.3 < se / row["delta_se"] < 2.5

    def test__first_session_is_one_session_per_user(self, per_session) -> None:
        stream, _ = per_session
        first = stream.truncate_paths("path_start", FIRST_SESSION_END)
        frame = first.to_dataframe()

        assert frame.groupby("user_id")["session_id"].nunique().max() == 1
        assert frame["user_id"].nunique() == stream.df["user_id"].nunique()
        assert (first.get_path_segments("exp_group") != "<MULTI>").all()


class TestAaCalibration:
    def test__whales_are_the_planted_accounts(self, aa) -> None:
        stream, assignment = aa
        frame = stream.to_dataframe()
        revenue = frame.groupby("user_id")["order_value"].sum()
        whales = frame.loc[frame["whale"], "user_id"].unique()

        assert sorted(revenue.nlargest(6).index) == sorted(whales)
        assert (assignment["arm"].reindex(whales) == "treatment").all()

    def test__weekday_pattern_in_levels(self, aa) -> None:
        stream, _ = aa
        frame = stream.to_dataframe()
        sessions = frame.groupby("session_id").agg(
            day=("timestamp", "min"),
            order=("event", lambda e: (e == "order_success").any()),
        )
        weekend = sessions["day"].dt.dayofweek >= 5
        rate = sessions.groupby(weekend)["order"].mean()

        assert 1.3 < rate[True] / rate[False] < 2.0

    def test__bootstrap_does_not_fix_the_whales(self, aa) -> None:
        stream, assignment = aa
        rows = stream.get_ab_test(DIFF, aa_metrics()[2:4], assignment=assignment)
        welch, boot = rows.iloc[0], rows.iloc[1]

        assert boot["delta"] == pytest.approx(welch["delta"])
        assert boot["delta_se"] == pytest.approx(welch["delta_se"], rel=0.25)


class TestTrialConversion:
    def test__whole_window_versus_horizon(self, trial) -> None:
        stream, assignment = trial
        sub = [_has("subscribe", "subscribed")]
        whole = stream.get_ab_test(DIFF, sub, assignment=assignment).iloc[0]
        h21 = stream.get_ab_test(DIFF, sub, assignment=assignment, horizon="21D").iloc[
            0
        ]

        assert 0.015 < whole["delta"] < 0.045
        assert abs(h21["delta"]) < 0.025
        assert h21["immature_paths"] > 0
        assert h21["delta_ci_low"] < 0 < h21["delta_ci_high"]

    def test__planted_subscribe_days(self, trial) -> None:
        stream, _ = trial
        users = (
            stream.to_dataframe()
            .groupby("user_id")[["exp_group", "planted_subscribe_day"]]
            .first()
        )
        day = users["planted_subscribe_day"]
        share = day.notna().groupby(users["exp_group"]).mean()

        assert day.max() < 21
        assert (share - 0.18).abs().max() < 0.015
        early = (day < 7).groupby(users["exp_group"]).sum()
        assert early["control"] == 0 and early["treatment"] > 0

    def test__offer_is_treatment_only_from_day_3(self, trial) -> None:
        stream, assignment = trial
        frame = stream.to_dataframe()
        offers = frame[frame["event"] == "annual_offer_view"]
        since = (
            offers["timestamp"]
            - assignment["assigned_at"].reindex(offers["user_id"]).to_numpy()
        )

        assert set(offers["exp_group"]) == {"treatment"}
        assert since.min() >= pd.Timedelta(days=3)


class TestPushCampaign:
    def test__most_of_the_table_is_silent(self, push) -> None:
        stream, assignment = push
        row = _quiet(
            stream.get_ab_test,
            DIFF,
            [_has("purchase", "buyers")],
            assignment=assignment,
        ).iloc[0]
        silent = row["group1_paths_no_events"] + row["group2_paths_no_events"]

        assert 0.66 < silent / len(assignment) < 0.74

    def test__itt_effect_size(self, push) -> None:
        stream, assignment = push
        row = _quiet(
            stream.get_ab_test,
            DIFF,
            [_has("purchase", "buyers")],
            assignment=assignment,
        ).iloc[0]

        assert 0.008 < row["delta"] < 0.025

    def test__push_brings_users_onto_the_stream(self, push) -> None:
        stream, _ = push
        levels = stream.get_sample_ratio("exp_group")["levels"].set_index("level")

        assert 0.55 < levels.loc["treatment", "share"] < 0.65
        assert set(
            stream.to_dataframe().loc[
                lambda f: f["event"].isin(["push_open", "promo_view"]), "exp_group"
            ]
        ) == {"treatment"}


# ---------------------------------------------------------------------------
# Seed sweep (slow): every planted verdict holds in at least 90 % of seeds
# ---------------------------------------------------------------------------


# Verdicts on a planted null: "inconclusive" is a true negative, so a 95 % test
# returns it in about 95 % of seeds and no design can do better. Over 40 seeds a
# rate of 0.85 or more is what a calibrated test gives (binomial tail ~1.5 %).
NULL_VERDICTS = {
    "top_line_inconclusive",
    "ios_and_web_inconclusive",
    "before_day_9_inconclusive",
    "conversion_inconclusive",
    "orders_inconclusive",
    "winsorised_revenue_inconclusive",
    "winsorised_aov_inconclusive",
    "daily_rows_not_significant",
    "weekly_rows_inconclusive",
    "horizon_21d_inconclusive",
}


@pytest.mark.slow
@pytest.mark.parametrize("name", list(CHECKS))
def test_seed_sweep_planted_verdicts_hold(name) -> None:
    held: dict[str, list[bool]] = {}
    for seed in SWEEP_SEEDS:
        for verdict, ok in run_check(name, seed).items():
            held.setdefault(verdict, []).append(bool(ok))
    rates = {verdict: sum(oks) / len(oks) for verdict, oks in held.items()}
    low = {
        verdict: rate
        for verdict, rate in rates.items()
        if rate < (0.85 if verdict in NULL_VERDICTS else 0.9)
    }

    assert not low, rates
