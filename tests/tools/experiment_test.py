import copy
import json

import numpy as np
import pandas as pd
import pytest
from retentioneering.tools.experiment import (
    adjust_pvalues,
    experiment_data,
    ordered_reach,
    session_funnel_data,
)
from retentioneering.stats.experiments import _bootstrap_config, _draw_sums
from retentioneering import Eventstream
from scipy import stats

OBS = {
    "start": "2026-01-02",
    "end": "2026-01-16",
    "assignment_time_col": "assigned_at",
    "complete": True,
}
BOOT = {"n_resamples": 999, "random_state": 42, "batch_size": 64}
ORDER = {"metric": "has_event", "metric_args": {"event": "buy"}}
COUNT = {"metric": "event_count", "metric_args": {"event": "buy"}}
REVENUE = {
    "metric": "event_value_sum",
    "metric_args": {"event": "buy", "value_col": "amount"},
}
METRICS = [
    {"name": "conversion", **ORDER, "agg": "mean"},
    {"name": "revenue", **REVENUE, "agg": "mean"},
    {"name": "aov", "agg": "ratio_of_sums", "numerator": REVENUE, "denominator": COUNT},
    {
        "name": "personal_aov",
        "agg": "mean_of_ratios",
        "numerator": REVENUE,
        "denominator": COUNT,
        "eligibility": "denominator_positive",
    },
]


def stream_of(rows):
    df = pd.DataFrame(
        rows,
        columns=["user_id", "session_id", "event", "timestamp", "amount", "variant"],
    )
    df.timestamp = pd.to_datetime(df.timestamp, format="mixed")
    return Eventstream(
        df,
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["variant"],
            "custom_cols": ["amount"],
        },
    )


@pytest.fixture
def case():
    # 4 assigned per arm, 1 silent each. Control revenue100, treatment120.
    data = {
        "c1": [[40], []],
        "c2": [[]],
        "c4": [[30, 30]],
        "t1": [[50], [50], []],
        "t2": [[20]],
        "t4": [[]],
    }
    rows = []
    for user, sessions in data.items():
        for j, orders in enumerate(sessions):
            ts = pd.Timestamp("2026-01-03") + pd.Timedelta(hours=j)
            rows.extend(
                [
                    (user, f"{user}_{j}", "open", ts, np.nan, user[0]),
                    (
                        user,
                        f"{user}_{j}",
                        "cart",
                        ts + pd.Timedelta(seconds=1),
                        np.nan,
                        user[0],
                    ),
                ]
            )
            rows.extend(
                (
                    user,
                    f"{user}_{j}",
                    "buy",
                    ts + pd.Timedelta(seconds=k + 2),
                    val,
                    user[0],
                )
                for k, val in enumerate(orders)
            )
    a = pd.DataFrame(
        {
            "user_id": [f"{g}{i}" for g in "ct" for i in range(1, 5)],
            "variant": ["c"] * 4 + ["t"] * 4,
            "assigned_at": pd.Timestamp("2026-01-01"),
        }
    )
    return stream_of(rows), a


def run(case, **kwargs):
    s, a = case
    opts = {
        "assignments": a,
        "metrics": METRICS,
        "observation": OBS,
        "variant_col": "variant",
        "control": "c",
        "treatment": "t",
    }
    opts.update(kwargs)
    return experiment_data(s, **opts)


def test_manual_points_and_silent_roster(case):
    r = run(case)
    d = r.summary.set_index("metric")
    assert list(r.diagnostics.n_assigned) == [4, 4]
    assert list(r.diagnostics.n_without_events) == [1, 1]
    assert d.loc["conversion", "control"] == d.loc["conversion", "treatment"] == 0.5
    assert d.loc["revenue", "absolute_effect"] == 5
    assert d.loc["aov", "absolute_effect"] == pytest.approx(20 / 3)
    assert d.loc["personal_aov", "control"] == 35
    assert d.loc["personal_aov", "treatment"] == 35
    assert r.metadata["eligibility"][0]["excluded_units"] == 4


def test_session_denominators_and_user_counts(case):
    s, _ = case
    r = session_funnel_data(
        s,
        steps=["open", "buy"],
        segment_col="variant",
        control="c",
        treatment="t",
        bootstrap=BOOT,
    )
    row = r.summary.query("family == 'funnel_cumulative' and step_index == 2").iloc[0]
    assert row.control == 0.5 and row.treatment == 0.6
    assert row.control_units == row.treatment_units == 3
    assert list(r.diagnostics.n_paths) == [4, 5]


@pytest.mark.parametrize(
    "steps", [["open", "cart", "buy"], ["buy", "buy"], ["cart", "open"], ["open"]]
)
def test_reach_matches_retentioneering(case, steps):
    s, _ = case
    for path in ["user_id", "session_id"]:
        reach = ordered_reach(s, steps, path_col=path)
        expected = s.funnel_data(steps=steps, path_col=path)["steps"]
        assert list(reach.sum().astype(int)) == [x["unique_paths"] for x in expected]


def test_marker_order_and_duplicate_steps():
    s = stream_of(
        [
            ("u", "s", "a", "2026-01-03", np.nan, "c"),
            ("u", "s", "a", "2026-01-03", np.nan, "c"),
            ("v", "v", "b", "2026-01-03", np.nan, "t"),
        ]
    ).add_start_end_events()
    for steps in [["path_start", "a", "a", "path_end"], ["a", "a", "a"]]:
        got = ordered_reach(s, steps)
        expected = s.funnel_data(steps=steps)["steps"]
        assert list(got.sum().astype(int)) == [v["unique_paths"] for v in expected]
    assert ordered_reach(s, ["a", "a"]).loc["u", "reach_2"] == 1


def test_start_inclusive_end_exclusive(case):
    s, a = case
    df = s.to_dataframe()
    df.loc[df.event.eq("buy"), "timestamp"] = pd.Timestamp(OBS["end"])
    s = Eventstream(
        df.drop(columns=["index", "subindex", "event_type"]),
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["variant"],
            "custom_cols": ["amount"],
        },
    )
    r = run((s, a))
    assert r.summary.iloc[0].control == r.summary.iloc[0].treatment == 0
    df.loc[df.event.eq("buy"), "timestamp"] = pd.Timestamp(OBS["start"])
    s = Eventstream(
        df.drop(columns=["index", "subindex", "event_type"]),
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["variant"],
            "custom_cols": ["amount"],
        },
    )
    assert run((s, a)).summary.iloc[0].control == 0.5


def test_empty_outcome_window_keeps_roster(case):
    r = run(
        case,
        observation={**OBS, "start": "2026-02-01", "end": "2026-02-02"},
        bootstrap=BOOT,
        funnel_steps=["open", "buy"],
    )
    assert r.diagnostics.n_without_events.tolist() == [4, 4]
    assert r.summary.iloc[0].control == 0
    assert r.summary.iloc[0].absolute_effect_status == "degenerate_distribution"
    aov = r.summary.query("metric == 'aov'").iloc[0]
    assert aov.control_status == "undefined_denominator"
    assert aov.control_invalid_resamples == BOOT["n_resamples"]
    json.dumps(r.to_dict(), allow_nan=False)


def test_sparse_denominator_suppresses_ci(case):
    r = run(case, bootstrap=BOOT)
    row = r.summary.set_index("metric").loc["aov"]
    assert row.absolute_effect_status == "unstable_denominator"
    assert row.absolute_effect_invalid_resamples > 0
    assert pd.isna(row.absolute_effect_ci_low)


def test_bootstrap_batch_invariant(case):
    r1 = run(case, bootstrap={**BOOT, "batch_size": 1})
    r2 = run(case, bootstrap={**BOOT, "batch_size": 211})
    pd.testing.assert_frame_equal(r1.summary, r2.summary)


def test_shared_cluster_weights_keep_linear_relationship():
    config = _bootstrap_config(BOOT)
    values = np.array([[1, 1, 7, 1], [2, 1, 14, 1], [5, 1, 35, 1]], float)
    draws = _draw_sums(values, config, np.random.default_rng(42))
    assert np.array_equal(draws[:, 2], 7 * draws[:, 0])
    assert np.all(draws[:, 1] == 3)
    assert len(np.unique(draws[:, 0])) > 1


def test_overlapping_descriptive_groups_share_draws():
    rows = []
    for u, buy in [("u1", True), ("u2", False), ("u3", False)]:
        for arm in ["c", "t"]:
            rows.append((u, u + arm, "open", "2026-01-03", np.nan, arm))
            if buy:
                rows.append((u, u + arm, "buy", "2026-01-03 00:00:01", 10.0, arm))
    r = session_funnel_data(
        stream_of(rows),
        steps=["open", "buy"],
        segment_col="variant",
        control="c",
        treatment="t",
        bootstrap=BOOT,
    )
    assert r.metadata["n_overlapping_units"] == 3
    assert (r.summary.absolute_effect == 0).all()
    assert (r.summary.absolute_effect_status == "degenerate_distribution").all()


def test_welch_two_sided_matches_scipy_and_holm(case):
    r = run(
        case,
        tests={
            "methods": {"conversion": "fisher_exact", "revenue": "welch"},
            "adjustment": "holm",
        },
    )
    d = r.summary.set_index("metric")
    assert d.loc["conversion", "p_value"] == 1
    expected = stats.ttest_ind(
        [100, 20, 0, 0], [40, 0, 0, 60], equal_var=False, alternative="two-sided"
    ).pvalue
    assert d.loc["revenue", "p_value"] == pytest.approx(expected)
    assert d.loc["revenue", "p_value_adjusted"] >= expected
    assert np.isnan(d.loc["aov", "p_value"])


def test_holm_manual_and_missing_family_member():
    np.testing.assert_allclose(adjust_pvalues([0.01, 0.04, 0.03]), [0.03, 0.06, 0.06])
    np.testing.assert_allclose(
        adjust_pvalues([0.01, np.nan, 0.04]), [0.03, np.nan, 0.08], equal_nan=True
    )


@pytest.mark.parametrize("bad", [True, -1, 1.5])
def test_bad_bootstrap_seed(case, bad):
    with pytest.raises(ValueError):
        run(case, bootstrap={**BOOT, "random_state": bad})


@pytest.mark.parametrize(
    "mutation,match",
    [
        ("duplicate", "one assignment"),
        ("late", "window start"),
        ("unknown_arm", "undeclared"),
        ("missing_user", "missing from"),
        ("arm_conflict", "conflicts"),
        ("missing_time", "nonnull"),
    ],
)
def test_invalid_assignments(case, mutation, match):
    s, a = case
    a = a.copy()
    if mutation == "duplicate":
        a = pd.concat([a, a.iloc[:1]])
    elif mutation == "late":
        a.loc[0, "assigned_at"] = pd.Timestamp("2026-01-03")
    elif mutation == "unknown_arm":
        a.loc[0, "variant"] = "x"
    elif mutation == "missing_user":
        a = a[a.user_id != "c1"]
    elif mutation == "arm_conflict":
        a.loc[0, "variant"] = "t"
    else:
        a.loc[0, "assigned_at"] = pd.NaT
    with pytest.raises(ValueError, match=match):
        run((s, a))


def test_missing_amount_fails_and_inputs_unchanged(case):
    s, a = case
    before = s.to_dataframe().copy(deep=True)
    before_a = a.copy(deep=True)
    run(case, bootstrap=BOOT)
    pd.testing.assert_frame_equal(before, s.to_dataframe())
    pd.testing.assert_frame_equal(before_a, a)
    df = before.copy()
    df.loc[df.event.eq("buy"), "amount"] = np.nan
    new = Eventstream(
        df.drop(columns=["index", "subindex", "event_type"]),
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["variant"],
            "custom_cols": ["amount"],
        },
    )
    with pytest.raises(ValueError, match="amount"):
        run((new, a))


def test_unknown_metric_event_and_typo_fail(case):
    for metric in [
        {"name": "x", "metric": "duration", "metric_args": {}, "agg": "mean"},
        {
            "name": "x",
            "metric": "has_event",
            "metric_args": {"event": "typo"},
            "agg": "mean",
        },
        {"name": "x", **ORDER, "agg": "meen"},
    ]:
        with pytest.raises(ValueError):
            run(case, metrics=[metric])


def test_zero_control_keeps_absolute_effect(case):
    s, a = case
    df = s.to_dataframe()
    df = df[~(df.variant.eq("c") & df.event.eq("buy"))]
    s = Eventstream(
        df.drop(columns=["index", "subindex", "event_type"]),
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["variant"],
            "custom_cols": ["amount"],
        },
    )
    r = run((s, a), bootstrap=BOOT)
    row = r.summary.iloc[0]
    assert (
        row.absolute_effect == 0.5
        and row.absolute_effect_status == "degenerate_component"
    )
    assert pd.isna(row.absolute_effect_ci_low)
    assert (
        np.isnan(row.relative_effect)
        and row.relative_effect_status == "undefined_baseline"
    )


def test_no_implicit_complete_or_test_for_ratio(case):
    with pytest.raises(ValueError):
        run(case, observation={**OBS, "complete": False})
    with pytest.raises(ValueError, match="not ratios"):
        run(case, tests={"methods": {"aov": "welch"}, "adjustment": "holm"})


def test_fisher_matches_exact_table_and_retains_silent_units(case):
    s, a = case
    df = s.to_dataframe()
    df = df[~(df.variant.eq("c") & df.event.eq("buy"))]
    s = Eventstream(
        df.drop(columns=["index", "subindex", "event_type"]),
        schema={
            "path_cols": ["user_id", "session_id"],
            "segment_cols": ["variant"],
            "custom_cols": ["amount"],
        },
    )
    r = run(
        (s, a),
        tests={
            "methods": {"conversion": "fisher_exact", "revenue": "welch"},
            "adjustment": "holm",
        },
    )
    d = r.summary.set_index("metric")
    expected = stats.fisher_exact([[2, 2], [0, 4]], alternative="two-sided").pvalue
    assert d.loc["conversion", "p_value"] == pytest.approx(expected)
    assert d.loc["conversion", "test_method"] == "fisher_exact_two_sided"
    assert d.loc["conversion", "p_value_adjusted"] >= expected


def test_fisher_requires_declared_binary_outcome(case):
    with pytest.raises(ValueError, match="binary"):
        run(case, tests={"methods": {"revenue": "fisher_exact"}, "adjustment": "holm"})


def test_fisher_zero_success_is_valid_while_bootstrap_degenerate(case):
    r = run(
        case,
        observation={**OBS, "start": "2026-02-01", "end": "2026-02-02"},
        bootstrap=BOOT,
        tests={"methods": {"conversion": "fisher_exact"}, "adjustment": "holm"},
    )
    row = r.summary.set_index("metric").loc["conversion"]
    assert row.p_value == 1 and row.test_status == "ok"
    assert row.absolute_effect_status == "degenerate_distribution"


def test_invalid_config_is_not_silently_ignored(case):
    with pytest.raises(ValueError):
        run(case, bootstrap={**BOOT, "typo": True})
    with pytest.raises(ValueError):
        run(case, metrics=copy.deepcopy(METRICS) + [METRICS[0]])


def test_srm_exact_binomial(case):
    r = run(case, expected_allocation=0.5)
    assert r.metadata["srm"]["p_value"] == 1
