"""Joint path bootstrap engine (``tools/_bootstrap.py``)."""

import time

import numpy as np
import pandas as pd
import pytest

from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _bootstrap as bs


def _toy(n: int = 400, cells: int = 3, seed: int = 0):
    rng = np.random.default_rng(seed)
    num = rng.poisson(1.0, size=(n, cells)).astype(float)
    den = (rng.random((n, cells)) < 0.8).astype(float)
    arm = (np.arange(n) % 2).astype(int)
    return num, den, arm


class TestEngine:
    @pytest.mark.parametrize("weights", ["multinomial", "poisson"])
    def test_batch_size_does_not_change_replicates(self, weights) -> None:
        num, den, arm = _toy()
        ref = bs.bootstrap_paths(num, den, arm, 300, weights, batch=300)
        for batch in (1, 7, 50, 200, 1000):
            other = bs.bootstrap_paths(num, den, arm, 300, weights, batch=batch)
            np.testing.assert_allclose(other.group1, ref.group1, rtol=1e-12)
            np.testing.assert_allclose(other.group2, ref.group2, rtol=1e-12)

    @pytest.mark.parametrize("weights", ["multinomial", "poisson"])
    def test_first_replicates_of_a_longer_run_are_equal(self, weights) -> None:
        num, den, arm = _toy()
        short = bs.bootstrap_paths(num, den, arm, 120, weights, batch=50)
        long = bs.bootstrap_paths(num, den, arm, 500, weights, batch=200)
        np.testing.assert_allclose(long.group1[:120], short.group1, rtol=1e-12)
        np.testing.assert_allclose(long.group2[:120], short.group2, rtol=1e-12)

    @pytest.mark.parametrize("weights", ["multinomial", "poisson"])
    @pytest.mark.parametrize("with_den", [True, False])
    def test_sparse_input_gives_the_dense_replicates(self, weights, with_den) -> None:
        from scipy import sparse

        num, den, arm = _toy(cells=5)
        num[np.random.default_rng(1).random(num.shape) < 0.6] = 0.0
        num[3, 1] = np.nan  # a missing value is no value in both forms
        dense = bs.bootstrap_paths(
            num, den if with_den else None, arm, 250, weights, batch=60
        )
        sparse_den = sparse.csr_matrix(den) if with_den else None
        sparse_res = bs.bootstrap_paths(
            sparse.csr_matrix(num), sparse_den, arm, 250, weights, batch=60
        )
        np.testing.assert_array_equal(sparse_res.group1, dense.group1)
        np.testing.assert_array_equal(sparse_res.group2, dense.group2)
        np.testing.assert_array_equal(sparse_res.valid, dense.valid)

    def test_sparse_input_is_not_changed(self) -> None:
        from scipy import sparse

        num = sparse.csr_matrix(np.array([[1.0, np.nan], [0, 2.0], [3.0, 1.0], [1, 1]]))
        den = sparse.csr_matrix(np.array([[np.inf, 1.0], [1, 1], [1, 1], [1, 1]]))
        before = (num.data.copy(), den.data.copy())
        bs.bootstrap_paths(num, None, np.array([0, 0, 1, 1]), 100)
        bs.bootstrap_paths(num, den, np.array([0, 0, 1, 1]), 100)
        np.testing.assert_array_equal(num.data, before[0])
        np.testing.assert_array_equal(den.data, before[1])

    @pytest.mark.parametrize("weights", ["multinomial", "poisson"])
    def test_shared_denominators_equal_each_cell_alone(self, weights) -> None:
        from scipy import sparse

        rng = np.random.default_rng(3)
        num = rng.poisson(0.4, size=(500, 6)).astype(float)
        den = num[:, [0, 1, 2]] + num[:, [3, 4, 5]] + rng.poisson(0.5, (500, 3))
        den_index = np.array([0, 1, 2, 0, 1, 2])
        arm = (np.arange(500) % 2).astype(int)
        shared = bs.bootstrap_paths(
            sparse.csr_matrix(num),
            sparse.csr_matrix(den),
            arm,
            300,
            weights,
            batch=70,
            den_index=den_index,
        )
        for j, k in enumerate(den_index):
            alone = bs.bootstrap_paths(num[:, j], den[:, k], arm, 300, weights)
            np.testing.assert_array_equal(shared.group1[:, j], alone.group1[:, 0])
            np.testing.assert_array_equal(shared.group2[:, j], alone.group2[:, 0])
            assert shared.valid[j] == alone.valid[0]
        with pytest.raises(InvalidParameterError):
            bs.bootstrap_paths(num, den, arm, 10, den_index=np.array([0, 3]))

    def test_replicates_do_not_depend_on_other_cells(self) -> None:
        num, den, arm = _toy(cells=4)
        full = bs.bootstrap_paths(num, den, arm, 200)
        one = bs.bootstrap_paths(num[:, [2]], den[:, [2]], arm, 200)
        np.testing.assert_allclose(full.group1[:, 2], one.group1[:, 0], rtol=1e-12)

    def test_seed_changes_replicates(self) -> None:
        num, den, arm = _toy()
        a = bs.bootstrap_paths(num, den, arm, 50, random_state=1)
        b = bs.bootstrap_paths(num, den, arm, 50, random_state=2)
        assert not np.allclose(a.group1, b.group1)

    def test_multinomial_keeps_arm_sizes(self) -> None:
        n = 101
        arm = (np.arange(n) >= 40).astype(int)
        ones = np.ones((n, 1))
        # value = sum(w) / sum(w) = 1, and the weighted count of paths is fixed:
        res = bs.bootstrap_paths(ones, None, arm, 50)
        assert np.all(res.group1 == 1.0)
        counts = bs._draw(np.random.default_rng(0), "multinomial", 5, 40)
        assert np.all(counts.sum(axis=1) == 40)

    def test_ratio_of_sums_point_estimate_without_resampling(self) -> None:
        # one path per arm: every multinomial replicate is the path itself
        num = np.array([[6.0], [2.0]])
        den = np.array([[3.0], [4.0]])
        res = bs.bootstrap_paths(num, den, np.array([0, 1]), 20)
        assert np.all(res.group1 == 2.0) and np.all(res.group2 == 0.5)
        assert res.valid.tolist() == [20]

    def test_nan_share_on_zero_denominator(self) -> None:
        # one path of ten carries the denominator: P(not drawn) = (9/10)^10
        n = 10
        num = np.zeros((2 * n, 1))
        den = np.zeros((2 * n, 1))
        den[0, 0] = 1.0
        den[n:, 0] = 1.0
        arm = np.repeat([0, 1], n)
        res = bs.bootstrap_paths(num, den, arm, 20000, batch=5000)
        share = np.isnan(res.group1[:, 0]).mean()
        assert share == pytest.approx(0.9**10, abs=0.01)
        assert res.valid[0] == np.isfinite(res.group1[:, 0]).sum()
        assert not np.isnan(res.group2).any()

    def test_non_finite_inputs_are_no_value(self) -> None:
        num = np.array([[1.0], [np.nan], [3.0], [5.0]])
        res = bs.bootstrap_paths(num, None, np.array([0, 0, 1, 1]), 300)
        # group 1 has one valid path (1.0) and one path without a value
        finite = res.group1[np.isfinite(res.group1)]
        assert np.all(finite == 1.0)

    def test_se_matches_binomial_se(self) -> None:
        rng = np.random.default_rng(7)
        n = 2000
        y = np.r_[rng.random(n) < 0.3, rng.random(n) < 0.25].astype(float)
        arm = np.repeat([0, 1], n)
        res = bs.bootstrap_paths(y[:, None], None, arm, 4000, batch=500)
        sd = bs.bootstrap_se(res.delta)[0]
        p1, p2 = y[:n].mean(), y[n:].mean()
        analytic = np.sqrt(p1 * (1 - p1) / n + p2 * (1 - p2) / n)
        assert sd / analytic == pytest.approx(1.0, abs=0.02)

    def test_validation(self) -> None:
        num, den, arm = _toy()
        with pytest.raises(InvalidParameterError):
            bs.bootstrap_paths(num, den, arm, 10, weights="bayes")
        with pytest.raises(InvalidParameterError):
            bs.bootstrap_paths(num, den, arm, 0)
        with pytest.raises(InvalidParameterError):
            bs.bootstrap_paths(num, den[:, :1], arm, 10)


class TestSummaries:
    def test_percentile_ci_and_min_replicates(self) -> None:
        reps = np.arange(1000, dtype=float)[:, None]
        low, high = bs.percentile_ci(reps, 0.9)
        assert low[0] == pytest.approx(np.quantile(reps, 0.05))
        assert high[0] == pytest.approx(np.quantile(reps, 0.95))
        few = np.full((1000, 1), np.nan)
        few[:99, 0] = 1.0
        assert np.isnan(bs.percentile_ci(few)[0][0])

    def test_bootstrap_p_normal_approximation(self) -> None:
        rng = np.random.default_rng(3)
        reps = rng.normal(0.0, 2.0, size=(5000, 1))
        p = bs.bootstrap_p(np.array([3.92]), reps)
        sd = np.std(reps, ddof=1)
        expected = 2 * (1 - __import__("scipy").stats.norm.cdf(3.92 / sd))
        assert p[0] == pytest.approx(expected, rel=1e-9)
        assert np.isnan(bs.bootstrap_p(np.array([1.0]), np.zeros((500, 1)))[0])

    def test_contrast_rows(self) -> None:
        g1 = np.array([[0.5, 0.2], [0.6, 0.1]])
        g2 = np.array([[0.4, 0.3], [0.4, 0.2]])
        res = bs.BootstrapResult(g1, g2, np.array([2, 2]), 2, "multinomial")
        cells = pd.DataFrame(
            {
                "metric": ["m", "m"],
                "contrast": ["novelty", "novelty"],
                "side": ["a", "b"],
            }
        )
        out = bs.contrast_rows(res, cells, {"novelty": (["0-6"], ["7-13"])})
        assert list(out.columns) == [
            "replicate",
            "metric",
            "contrast",
            "group1_value",
            "group2_value",
            "delta",
        ]
        np.testing.assert_allclose(out["group1_value"], [0.3, 0.5])
        np.testing.assert_allclose(out["group2_value"], [0.1, 0.2])
        np.testing.assert_allclose(out["delta"], [0.2, 0.3])


@pytest.mark.slow
class TestScale:
    def test_lumeo_scale_timing(self) -> None:
        rng = np.random.default_rng(0)
        n = 40_000
        orders = rng.poisson(0.06, n).astype(float)
        revenue = orders * rng.lognormal(4, 0.5, n)
        num = np.c_[orders > 0, rng.random(n) < 0.3, orders, revenue, revenue]
        den = np.c_[np.ones((n, 4)), orders]
        arm = rng.integers(0, 2, n)
        start = time.perf_counter()
        bs.bootstrap_paths(num, den, arm, 1000)
        assert time.perf_counter() - start < 3.0

    def test_percentile_coverage(self) -> None:
        # mean of a skewed metric, n = 300 per arm, true delta 0
        rng = np.random.default_rng(11)
        n, sims, hits = 300, 1000, 0
        arm = np.repeat([0, 1], n)
        for sim in range(sims):
            y = rng.exponential(1.0, 2 * n)
            res = bs.bootstrap_paths(y[:, None], None, arm, 500, random_state=sim)
            low, high = bs.percentile_ci(res.delta)
            hits += low[0] <= 0 <= high[0]
        assert 0.92 <= hits / sims <= 0.97
