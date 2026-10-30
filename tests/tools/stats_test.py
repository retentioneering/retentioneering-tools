"""Golden values and edge cases for ``tools/_stats.py``.

Provenance of the frozen numbers:

* Wilson 37/120 and Newcombe 56/70 vs 48/80: Newcombe R. G. (1998), "Interval estimation
  for the difference between independent proportions", Statistics in Medicine 17,
  Table I / Table II example (a); the Wilson digits also equal
  ``scipy.stats.binomtest(37, 120).proportion_ci(method="wilson")``.
* Pooled z p-value 56/70 vs 48/80: ``2 * norm.sf(|z|)`` with the pooled SE, cross-checked
  with statsmodels ``proportions_ztest`` during the review (no statsmodels dependency here).
* Everything else is checked against scipy reference implementations in the test itself.
"""

import numpy as np
import pytest
from scipy import stats

from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _stats as st


class TestProportions:
    def test_wilson_golden(self) -> None:
        low, high = st.wilson_ci(37, 120)
        assert low == pytest.approx(0.23272736988916298, rel=1e-12)
        assert high == pytest.approx(0.39582997723800684, rel=1e-12)

    def test_wilson_matches_scipy(self) -> None:
        for k, n in [(0, 10), (10, 10), (1, 1), (3, 17), (500, 1000)]:
            ref = stats.binomtest(k, n).proportion_ci(0.9, method="wilson")
            low, high = st.wilson_ci(k, n, 0.9)
            assert low == pytest.approx(ref.low, abs=1e-12)
            assert high == pytest.approx(ref.high, abs=1e-12)

    def test_wilson_edges(self) -> None:
        low, high = st.wilson_ci(0, 10)
        assert low == 0.0 and 0 < high < 1
        low, high = st.wilson_ci(10, 10)
        assert high == 1.0 and 0 < low < 1
        low, high = st.wilson_ci(1, 1)
        assert 0 < low < high == 1.0
        low, high = st.wilson_ci(0, 0)
        assert np.isnan(low) and np.isnan(high)

    def test_wilson_vectorised(self) -> None:
        low, high = st.wilson_ci([37, 0, 5], [120, 0, 5])
        assert low.shape == (3,)
        assert np.isnan(low[1]) and np.isnan(high[1])
        assert low[0] == pytest.approx(0.23272736988916298)

    def test_newcombe_golden(self) -> None:
        low, high = st.newcombe_ci(56, 70, 48, 80)
        assert round(float(low), 4) == 0.0524
        assert round(float(high), 4) == 0.3339

    def test_newcombe_empty_group_is_nan(self) -> None:
        low, high = st.newcombe_ci(5, 10, 0, 0)
        assert np.isnan(low) and np.isnan(high)

    def test_newcombe_extremes_are_finite(self) -> None:
        low, high = st.newcombe_ci(0, 50, 0, 50)
        assert low < 0 < high
        low, high = st.newcombe_ci(50, 50, 0, 50)
        assert 0 < low < high <= 1

    def test_pooled_z_golden(self) -> None:
        z, p, se = st.two_proportion_z(56, 70, 48, 80)
        assert p == pytest.approx(0.00804508136819656, rel=1e-10)
        assert z > 0
        p1, p2 = 56 / 70, 48 / 80
        assert se == pytest.approx(np.sqrt(p1 * (1 - p1) / 70 + p2 * (1 - p2) / 80))

    def test_pooled_z_constant_arms_is_nan(self) -> None:
        for k1, k2 in [(0, 0), (10, 20)]:
            z, p, _ = st.two_proportion_z(k1, 10 if k1 else 10, k2, 20 if k2 else 20)
            assert np.isnan(z) and np.isnan(p)

    def test_pooled_z_empty_arm_is_nan(self) -> None:
        z, p, se = st.two_proportion_z(3, 10, 0, 0)
        assert np.isnan(z) and np.isnan(p) and np.isnan(se)

    def test_delta_log_ratio(self) -> None:
        low, high = st.delta_log_ratio_ci(56, 70, 48, 80)
        p1, p2 = 56 / 70, 48 / 80
        half = stats.norm.ppf(0.975) * np.sqrt((1 - p1) / 56 + (1 - p2) / 48)
        assert low == pytest.approx(np.exp(np.log(p1 / p2) - half) - 1)
        assert high == pytest.approx(np.exp(np.log(p1 / p2) + half) - 1)
        assert low < p1 / p2 - 1 < high

    def test_delta_log_ratio_zero_counts_are_nan(self) -> None:
        for args in [(5, 10, 0, 10), (0, 10, 5, 10), (5, 10, 5, 0)]:
            low, high = st.delta_log_ratio_ci(*args)
            assert np.isnan(low) and np.isnan(high)

    def test_delta_log_ratio_all_converted_is_nan(self) -> None:
        # k == n zeroes that arm's variance term: the interval would be a point
        for args in [(30, 30, 30, 30), (30, 30, 1, 1), (5, 10, 10, 10)]:
            low, high = st.delta_log_ratio_ci(*args)
            assert np.isnan(low) and np.isnan(high)

    def test_exact_binomial_matches_scipy(self) -> None:
        for k, n in [(0, 10), (10, 10), (1, 1), (37, 120)]:
            ref = stats.binomtest(k, n).proportion_ci(0.95, method="exact")
            low, high = st.exact_binomial_ci(k, n)
            assert low == pytest.approx(ref.low, abs=1e-10)
            assert high == pytest.approx(ref.high, abs=1e-10)
        low, high = st.exact_binomial_ci(0, 0)
        assert np.isnan(low) and np.isnan(high)

    def test_ci_level_validated(self) -> None:
        with pytest.raises(InvalidParameterError):
            st.wilson_ci(1, 2, 1.5)
        with pytest.raises(InvalidParameterError):
            st.newcombe_ci(1, 2, 1, 2, 0)


class TestWelch:
    def test_matches_scipy(self) -> None:
        rng = np.random.default_rng(1)
        a, b = rng.normal(1.0, 2.0, 40), rng.normal(0.5, 1.0, 25)
        t, df, p, se = st.welch_t(
            a.mean(), a.var(ddof=1), a.size, b.mean(), b.var(ddof=1), b.size
        )
        ref = stats.ttest_ind(a, b, equal_var=False)
        assert t == pytest.approx(ref.statistic, rel=1e-12)
        assert p == pytest.approx(ref.pvalue, rel=1e-10)
        assert se == pytest.approx(np.sqrt(a.var(ddof=1) / 40 + b.var(ddof=1) / 25))
        assert df == pytest.approx(ref.df, rel=1e-12)

    def test_one_constant_arm_still_tests(self) -> None:
        t, df, p, se = st.welch_t(1.0, 0.0, 10, 0.0, 1.0, 10)
        assert np.isfinite(p) and df == pytest.approx(9.0)

    @pytest.mark.parametrize(
        "args", [(1.0, 0.0, 10, 0.0, 0.0, 10), (1.0, 1.0, 1, 0.0, 1.0, 10)]
    )
    def test_undefined_is_nan(self, args) -> None:
        assert all(np.isnan(x) for x in st.welch_t(*args))


class TestMde:
    def test_mde_proportion(self) -> None:
        f = stats.norm.ppf(0.975) + stats.norm.ppf(0.8)
        expected = f * np.sqrt(0.1 * 0.9 * (1 / 1000 + 1 / 1000))
        assert st.mde_proportion(0.1, 1000, 1000) == pytest.approx(expected)
        assert np.isnan(st.mde_proportion(0.0, 1000, 1000))
        assert np.isnan(st.mde_proportion(0.5, 0, 1000))

    def test_mde_shrinks_with_alpha(self) -> None:
        assert st.mde_proportion(0.1, 1000, 1000, alpha=0.01) > st.mde_proportion(
            0.1, 1000, 1000
        )

    def test_mde_mean_and_from_se_agree(self) -> None:
        se = 2.0 * np.sqrt(1 / 50 + 1 / 80)
        assert st.mde_mean(2.0, 50, 80) == pytest.approx(st.mde_from_se(se))
        assert np.isnan(st.mde_from_se(np.nan))
        assert np.isnan(st.mde_from_se(0.0))  # a constant metric detects nothing

    def test_power_validated(self) -> None:
        with pytest.raises(InvalidParameterError):
            st.mde_from_se(1.0, power=1.0)


class TestMultiplicity:
    def test_holm_hand_example(self) -> None:
        p = [0.01, 0.04, 0.03, 0.005]
        # sorted: 0.005*4=0.02, 0.01*3=0.03, 0.03*2=0.06, 0.04*1=0.04 -> monotone 0.06
        assert st.holm(p) == pytest.approx([0.03, 0.06, 0.06, 0.02])

    def test_holm_ties_and_nan(self) -> None:
        out = st.holm([0.02, np.nan, 0.02, 0.5])
        assert np.isnan(out[1])
        # m = 3 finite values
        assert out[[0, 2, 3]] == pytest.approx([0.06, 0.06, 0.5])

    def test_holm_clipped(self) -> None:
        assert st.holm([0.6, 0.7]) == pytest.approx([1.0, 1.0])

    def test_bonferroni_nan(self) -> None:
        out = st.bonferroni([0.01, np.nan, 0.4])
        assert out[0] == pytest.approx(0.02)
        assert np.isnan(out[1])
        assert out[2] == pytest.approx(0.8)

    def test_benjamini_hochberg_hand_example(self) -> None:
        p = [0.01, 0.04, 0.03, 0.005]
        # sorted 0.005, 0.01, 0.03, 0.04 -> *4/1, *4/2, *4/3, *4/4
        # = 0.02, 0.02, 0.04, 0.04 (step-up minimum already monotone)
        assert st.benjamini_hochberg(p) == pytest.approx([0.02, 0.04, 0.04, 0.02])

    def test_benjamini_hochberg_matches_scipy(self) -> None:
        rng = np.random.default_rng(3)
        p = rng.uniform(0, 0.2, 25)
        ref = stats.false_discovery_control(p, method="bh")
        assert st.benjamini_hochberg(p) == pytest.approx(ref, rel=1e-12)

    def test_benjamini_hochberg_nan_and_ties(self) -> None:
        out = st.benjamini_hochberg([0.02, np.nan, 0.02, 0.5])
        assert np.isnan(out[1])
        assert out[[0, 2, 3]] == pytest.approx([0.03, 0.03, 0.5])
        assert np.isnan(st.benjamini_hochberg([np.nan])).all()

    def test_all_nan(self) -> None:
        assert np.isnan(st.holm([np.nan, np.nan])).all()
        assert np.isnan(st.bonferroni([np.nan])).all()
        assert st.holm([]).size == 0


class TestHeterogeneityAndChi2:
    def test_cochran_q_hand_example(self) -> None:
        # w = 100 each, d_bar = 0.2, Q = 100 * 0.01 * 2 = 2
        q, p, df, used = st.cochran_q([0.1, 0.3], [0.1, 0.1], [True, True])
        assert q == pytest.approx(2.0)
        assert df == 1 and used == 2
        assert p == pytest.approx(stats.chi2.sf(2.0, 1))

    def test_cochran_q_support_guard(self) -> None:
        q, p, df, used = st.cochran_q(
            [0.1, 0.3, 5.0, 0.2], [0.1, 0.1, 0.1, 0.0], [True, True, False, True]
        )
        assert used == 2 and df == 1 and q == pytest.approx(2.0)
        q, p, df, used = st.cochran_q([0.1, 0.3], [0.1, 0.1], [True, False])
        assert np.isnan(q) and np.isnan(p) and df == 0 and used == 1

    def test_chi2_balance_matches_scipy(self) -> None:
        table = np.array([[30, 45], [70, 55], [0, 0]])
        chi2, p, dof = st.chi2_balance(table)
        ref = stats.chi2_contingency(table[:2], correction=False)
        assert chi2 == pytest.approx(ref.statistic)
        assert p == pytest.approx(ref.pvalue)
        assert dof == 1

    def test_chi2_balance_single_row(self) -> None:
        chi2, p, dof = st.chi2_balance([[10, 12]])
        assert np.isnan(chi2) and np.isnan(p) and dof == 0

    def test_srm_matches_scipy(self) -> None:
        chi2, p, dof = st.srm_chi2([5100, 4900])
        ref = stats.chisquare([5100, 4900])
        assert chi2 == pytest.approx(ref.statistic) and p == pytest.approx(ref.pvalue)
        assert dof == 1
        chi2, p, dof = st.srm_chi2([700, 300], [0.7, 0.3])
        assert chi2 == pytest.approx(0.0) and p == pytest.approx(1.0)

    def test_srm_validation_and_edges(self) -> None:
        with pytest.raises(InvalidParameterError):
            st.srm_chi2([10, 10], [0.6, 0.6])
        with pytest.raises(InvalidParameterError):
            st.srm_chi2([10, 10], [1.0])
        chi2, p, dof = st.srm_chi2([0, 0])
        assert np.isnan(chi2) and np.isnan(p) and dof == 1
        chi2, p, _ = st.srm_chi2([5, 5], [1.0, 0.0])
        assert np.isinf(chi2) and p == 0.0


class TestCoverage:
    def test_newcombe_and_pooled_z_coverage(self) -> None:
        # 4000 simulated experiments at n = 700 per arm: the 95% Newcombe interval
        # covers the true delta and the pooled z-test keeps H0 in [0.94, 0.96] of runs.
        rng = np.random.default_rng(20261029)
        n, sims = 700, 4000
        k1 = rng.binomial(n, 0.12, sims)
        k2 = rng.binomial(n, 0.10, sims)
        low, high = st.newcombe_ci(k1, n, k2, n)
        coverage = np.mean((low <= 0.02) & (0.02 <= high))
        assert 0.94 <= coverage <= 0.96

        k1 = rng.binomial(n, 0.1, sims)
        k2 = rng.binomial(n, 0.1, sims)
        _, p, _ = st.two_proportion_z(k1, n, k2, n)
        assert 0.94 <= np.mean(p >= 0.05) <= 0.96
