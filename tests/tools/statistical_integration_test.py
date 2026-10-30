"""Independent contracts shared by the strict and flexible A/B adapters."""

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from retentioneering import Eventstream
from retentioneering.stats import experiments, primitives
from retentioneering.stats.resampling import (
    BOOTSTRAP_ALGORITHM,
    POOLED_BOOTSTRAP_ALGORITHM,
)
from retentioneering.tools import _bootstrap, _stats
from retentioneering.tools.experiment import experiment_data, session_funnel_data


def test_statistical_adapters_share_formulas_and_declared_family():
    assert _stats.wilson_ci is primitives.wilson_ci
    assert _stats.newcombe_ci is primitives.newcombe_ci
    assert _stats.holm is primitives.holm
    p = [0.01, np.nan, 0.04]
    # Treat an unavailable planned test as p=1 for adjustment purposes, retaining
    # its missing output. It must not make the other tests easier to reject.
    expected = np.array([0.03, np.nan, 0.08])
    np.testing.assert_allclose(experiments.adjust_pvalues(p), expected, equal_nan=True)
    np.testing.assert_allclose(_stats.holm(p), expected, equal_nan=True)
    for k, n in [(0, 250), (250, 250), (37, 120)]:
        np.testing.assert_allclose(
            experiments._wilson(k, n, 0.95), _stats.wilson_ci(k, n)
        )


@pytest.mark.parametrize("batch", [1, 17, 128])
def test_equivalent_unit_contributions_receive_identical_draws(batch):
    control = np.array([[0.0, 1.0], [1.0, 2.0], [12.0, 3.0], [2.0, 1.0]])
    treatment = np.array([[1.0, 1.0], [14.0, 3.0], [0.0, 1.0]])
    config = experiments._bootstrap_config(
        {"n_resamples": 199, "random_state": 42, "batch_size": batch}
    )
    children = np.random.SeedSequence(42).spawn(2)
    strict = [
        experiments._draw_sums(v, config, np.random.default_rng(seed))
        for v, seed in zip([control, treatment], children)
    ]
    stacked = np.vstack([control, treatment])
    flex = _bootstrap.bootstrap_paths(
        stacked[:, [0]],
        stacked[:, [1]],
        np.array([1] * 4 + [0] * 3),
        n_bootstrap=199,
        random_state=42,
        batch=batch,
    )
    np.testing.assert_allclose(
        flex.group2[:, 0], strict[0][:, 0] / strict[0][:, 1], rtol=0, atol=0
    )
    np.testing.assert_allclose(
        flex.group1[:, 0], strict[1][:, 0] / strict[1][:, 1], rtol=0, atol=0
    )
    assert flex.algorithm == BOOTSTRAP_ALGORITHM


def test_one_invalid_draw_suppresses_interval_and_standard_error():
    draws = np.linspace(0, 1, 1000)
    draws[13] = np.nan
    lo, hi = _bootstrap.percentile_ci(draws)
    assert np.isnan(lo[0]) and np.isnan(hi[0])
    assert np.isnan(_bootstrap.bootstrap_se(draws)[0])
    assert np.isnan(_bootstrap.bootstrap_p([0.1], draws)[0])
    config = experiments._bootstrap_config({"n_resamples": 1000})
    lo, hi, status, invalid = experiments._interval(draws, 0.1, 50, config)
    assert np.isnan(lo) and np.isnan(hi)
    assert status == "unstable_denominator" and invalid == 1


@pytest.mark.parametrize("scale", [1e-200, 1.0, 1e200])
def test_raw_welch_and_mean_intervals_preserve_measurement_scale(scale):
    a = np.array([1.0, 2.0, 3.0, 4.0])
    b = np.array([2.0, 3.0, 5.0])
    reference = stats.ttest_ind(a, b, equal_var=False)
    t, df, p, se, status = _stats.welch_samples(a * scale, b * scale)
    assert status == "ok"
    assert t == pytest.approx(reference.statistic)
    assert df == pytest.approx(reference.df)
    assert p == pytest.approx(reference.pvalue)
    assert se / scale == pytest.approx(
        np.sqrt(a.var(ddof=1) / len(a) + b.var(ddof=1) / len(b))
    )
    mean, lo, hi = _stats.sample_mean_ci(a * scale)
    expected = stats.t.interval(0.95, len(a) - 1, loc=a.mean(), scale=stats.sem(a))
    assert mean / scale == pytest.approx(a.mean())
    assert lo / scale == pytest.approx(expected[0])
    assert hi / scale == pytest.approx(expected[1])


def test_raw_decimal_constancy_remains_unavailable_across_adapters():
    c, t = np.repeat(0.1, 30), np.repeat(0.2, 30)
    assert _stats.welch_samples(t, c)[-1] == "degenerate_distribution"
    assert experiments._welch(c, t)[1] == "degenerate_distribution"
    mean, lo, hi = _stats.sample_mean_ci(c)
    assert mean == 0.1 and np.isnan(lo) and np.isnan(hi)


def test_multiplicity_never_discounts_a_missing_planned_hypothesis():
    for adjust in [_stats.holm, _stats.bonferroni, _stats.benjamini_hochberg]:
        missing = adjust([0.02, np.nan, 0.04])
        conservative = adjust([0.02, 1.0, 0.04])
        np.testing.assert_allclose(missing[[0, 2]], conservative[[0, 2]])
        assert np.isnan(missing[1])


@pytest.mark.parametrize("scale", [1e-200, 1.0, 1e200])
def test_bootstrap_standard_error_does_not_square_raw_magnitudes(scale):
    draws = np.linspace(-2, 3, 500)
    observed = _bootstrap.bootstrap_se(draws * scale)[0]
    assert observed / scale == pytest.approx(draws.std(ddof=1))
    low, high = _bootstrap.percentile_ci(draws * scale)
    np.testing.assert_allclose(
        np.array([low[0], high[0]]) / scale, np.quantile(draws, [0.025, 0.975])
    )


def test_one_constant_arm_does_not_remove_the_other_arms_uncertainty():
    control = np.full(12, 0.1)
    treatment = np.linspace(0.1, 0.5, 12)
    result = primitives.welch_samples(treatment, control)
    reference = stats.ttest_1samp(treatment, 0.1)
    assert result[-1] == "ok"
    assert result[0] == pytest.approx(reference.statistic)
    assert result[2] == pytest.approx(reference.pvalue)
    assert primitives.sample_mean_ci(control)[0] == 0.1
    assert np.isnan(primitives.sample_mean_ci(control)[1])


@pytest.mark.parametrize("scale", [1e-200, 1.0, 1e200])
def test_cochran_q_is_invariant_to_measurement_units(scale):
    deltas = np.array([1.0, 2.0, 4.0])
    errors = np.array([0.5, 0.75, 1.25])
    weights = 1 / errors**2
    center = np.average(deltas, weights=weights)
    expected = np.sum(weights * (deltas - center) ** 2)
    q, p, df, used = primitives.cochran_q(
        deltas * scale, errors * scale, [True, True, True]
    )
    assert q == pytest.approx(expected)
    assert p == pytest.approx(stats.chi2.sf(expected, 2))
    assert df == 2 and used == 3


def test_cochran_q_equal_effects_with_tiny_errors_are_not_heterogeneous():
    q, p, df, used = primitives.cochran_q([1.0, 1.0, 1.0], [1e-200] * 3, [True] * 3)
    assert (q, p, df, used) == (0, 1, 2, 3)


@pytest.mark.parametrize("resample", [False, True])
def test_strict_metadata_identifies_the_actual_draw_design(resample):
    assignments = pd.DataFrame(
        {
            "user_id": ["c1", "c2", "t1", "t2"],
            "exp_group": ["control"] * 2 + ["treatment"] * 2,
            "assigned_at": pd.Timestamp("2026-01-01"),
        }
    )
    events = assignments[["user_id", "exp_group"]].assign(
        session_id=["s1", "s2", "s3", "s4"],
        event="view",
        timestamp=pd.Timestamp("2026-01-02"),
    )
    stream = Eventstream(
        events,
        {"path_cols": ["user_id", "session_id"], "segment_cols": ["exp_group"]},
    )
    bootstrap = {"n_resamples": 100, "random_state": 42} if resample else None
    independent = experiment_data(
        stream,
        assignments=assignments,
        metrics=[
            {
                "name": "views",
                "metric": "event_count",
                "metric_args": {"event": "view"},
                "agg": "mean",
            }
        ],
        observation={
            "start": "2026-01-02",
            "end": "2026-01-03",
            "assignment_time_col": "assigned_at",
            "complete": True,
        },
        bootstrap=bootstrap,
    )
    pooled = session_funnel_data(stream, steps=["view"], bootstrap=bootstrap)
    for result, algorithm in [
        (independent, BOOTSTRAP_ALGORITHM),
        (pooled, POOLED_BOOTSTRAP_ALGORITHM),
    ]:
        assert result.metadata["bootstrap_algorithm"] == (
            algorithm if resample else None
        )
        assert result.to_dict()["metadata"]["bootstrap_algorithm"] == (
            algorithm if resample else None
        )


@pytest.mark.parametrize("scale", [1e-220, 1.0, 1e200])
@pytest.mark.parametrize("varying_denominator", [False, True])
def test_paired_decimal_changes_are_constant_at_source_resolution(
    scale, varying_denominator
):
    early = np.arange(1, 31) / 10
    denominator = np.arange(1, 31) if varying_denominator else np.ones(30)
    late = early + 0.2
    assert primitives.paired_constant_statistic(
        late * scale * denominator, early * scale * denominator, denominator
    )


@pytest.mark.parametrize("scale", [1e-220, 1.0, 1e200])
def test_paired_resolution_guard_preserves_resolved_variation(scale):
    early = np.array([1.0, 2.0, 3.0, 4.0]) * scale
    change = np.array([0.2, 0.3, 0.2, 0.3]) * scale
    assert not primitives.paired_constant_statistic(early + change, early, np.ones(4))


def test_paired_resolution_guard_preserves_resolved_subnormal_variation():
    tiny = np.finfo(float).smallest_subnormal
    assert not primitives.paired_constant_statistic([0, 10 * tiny], [0, 0], [1, 1])


def test_paired_resolution_guard_does_not_classify_invalid_ratios_as_constants():
    assert not primitives.paired_constant_statistic([1, 2], [0, 1], [0, 0])
    assert not primitives.paired_constant_statistic([1, 2], [0, 1], [0, 1])
    assert not primitives.paired_constant_statistic([np.nan, 2], [0, 1], [1, 1])
