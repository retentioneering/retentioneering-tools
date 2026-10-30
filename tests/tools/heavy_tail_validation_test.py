"""Fast independent references for the opt-in research harness; no MC thresholds."""

import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

import numpy as np
from numpy.testing import assert_allclose
from scipy import stats

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples/experiments"
sys.path.insert(0, str(EXAMPLES))
spec = importlib.util.spec_from_file_location(
    "heavy_tail_validation", EXAMPLES / "heavy_tail_validation.py"
)
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)
sys.path.pop(0)


class HeavyTailValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = h.audit.load_stats(ROOT / "src/retentioneering/stats/experiments.py")

    def test_moments_match_literal_resampling(self):
        rng = np.random.default_rng(786)
        for ratio in (False, True):
            x = rng.lognormal(size=23)
            d = rng.integers(1, 5, len(x)) if ratio else np.ones(len(x))
            v = np.column_stack((x, d))
            for size in (7, 23):
                with self.subTest(ratio=ratio, size=size):
                    p, var, sums = h.draw_moments(
                        v, 83, np.random.default_rng(15), size
                    )
                    weights = np.random.default_rng(15).multinomial(
                        size, np.full(len(v), 1 / len(v)), size=83
                    )
                    expected = [h.arm_point(np.repeat(v, w, axis=0)) for w in weights]
                    assert_allclose(p, np.array(expected)[:, 0], rtol=1e-13)
                    assert_allclose(var, np.array(expected)[:, 1], rtol=1e-12)
                    assert_allclose(sums, weights @ v, rtol=1e-13)

    def test_bca_matches_scipy_unequal_samples(self):
        rng = np.random.default_rng(412)
        c, t = rng.lognormal(size=19), rng.lognormal(size=71)
        values = [np.column_stack((v, np.ones(len(v)))) for v in (c, t)]
        dc, _, _ = h.draw_moments(values[0], 999, np.random.default_rng(321))
        dt, _, _ = h.draw_moments(values[1], 999, np.random.default_rng(123))
        draws = h.estimands(dc, dt)
        point = h.estimands(c.mean(), t.mean())
        acceleration = h.acceleration(values)
        for j in range(4):
            with self.subTest(estimand=h.LABELS[j]):

                def statistic(a, b, axis=-1):
                    return h.estimands(a.mean(axis=axis), b.mean(axis=axis))[..., j]

                reference = stats.bootstrap(
                    (c, t),
                    statistic,
                    method="BCa",
                    n_resamples=0,
                    bootstrap_result=SimpleNamespace(
                        bootstrap_distribution=draws[:, j]
                    ),
                )
                lo, hi, status = h.bca_bounds(draws[:, j], point[j], acceleration[j])
                self.assertEqual(status, "ok")
                assert_allclose([lo, hi], reference.confidence_interval, rtol=1e-12)

    def test_baseline_matches_actual_pr216_inference(self):
        for index, s in enumerate(h.audit.scenarios()):
            with self.subTest(scenario=s["name"]):
                values, _ = h.audit.generate(s, np.random.default_rng(index))
                expected = h.audit.calculate(self.m, s, values, 415, 199)["percentile"]
                actual = h.calculate(self.m, s, values, 415, 199)[0]["percentile"]
                for label, (lo, hi, status) in zip(h.LABELS, actual):
                    self.assertEqual(status, expected[label + "_status"])
                    assert_allclose(
                        [lo, hi],
                        [expected[label + "_ci_low"], expected[label + "_ci_high"]],
                        rtol=1e-11,
                        atol=1e-12,
                    )

    def test_welch_matches_scipy_reference_and_test(self):
        s = h.audit.scenarios()[9]
        values, _ = h.audit.generate(s, np.random.default_rng(41))
        actual, _, pvalue, status = h.calculate(self.m, s, values, 42, 199)
        reference = stats.ttest_ind(values[1][:, 0], values[0][:, 0], equal_var=False)
        assert_allclose(
            actual["welch"][2][:2], reference.confidence_interval(), rtol=1e-13
        )
        assert_allclose(pvalue, reference.pvalue, rtol=1e-13)
        self.assertEqual(status, "ok")

    def test_studentizer_catches_sparse_resamples(self):
        s = dict(kind="money")
        c = np.column_stack(([0.0, 0.0, 0.0, 1.0], np.ones(4)))
        actual = h.calculate(self.m, s, [c, c.copy()], 42, 999)[0]
        self.assertEqual(actual["percentile"][2][2], "ok")
        self.assertEqual(actual["bootstrap_t"][2][2], "invalid_studentizer")
        self.assertEqual(actual["bootstrap_t_symmetric"][2][2], "invalid_studentizer")

    def test_studentized_inversion_matches_literal_reference(self):
        rng = np.random.default_rng(13)
        arrays = [rng.lognormal(size=19), rng.lognormal(size=47)]
        values = [np.column_stack((x, np.ones(len(x)))) for x in arrays]
        actual = h.calculate(self.m, dict(kind="money"), values, 87, 199)[0]
        means, variances = [], []
        for x, seed in zip(arrays, np.random.SeedSequence(87).spawn(2)):
            weights = np.random.default_rng(seed).multinomial(
                len(x), np.full(len(x), 1 / len(x)), size=199
            )
            literal = [np.repeat(x, w) for w in weights]
            means.append(np.array([a.mean() for a in literal]))
            variances.append(np.array([a.var(ddof=1) / len(a) for a in literal]))
        point = arrays[1].mean() - arrays[0].mean()
        se = np.sqrt(sum(x.var(ddof=1) / len(x) for x in arrays))
        pivot = (means[1] - means[0] - point) / np.sqrt(sum(variances))
        qlo, qhi = np.quantile(pivot, [0.025, 0.975])
        assert_allclose(
            actual["bootstrap_t"][2][:2],
            [point - qhi * se, point - qlo * se],
            rtol=1e-12,
        )
        q = np.quantile(np.abs(pivot), 0.95)
        assert_allclose(
            actual["bootstrap_t_symmetric"][2][:2],
            [point - q * se, point + q * se],
            rtol=1e-12,
        )

    def test_m_equals_n_reduces_to_basic_for_absolute_effects(self):
        s = h.audit.scenarios()[9]
        values, _ = h.audit.generate(s, np.random.default_rng(91))
        actual = h.calculate(self.m, s, values, 111, 199, m_power=1)[0]
        for j in range(3):
            assert_allclose(
                actual["basic"][j][:2], actual["m_out_of_n_basic"][j][:2], rtol=1e-12
            )

    def test_positive_rescaling_preserves_decisions(self):
        s = h.audit.scenarios()[9]
        values, _ = h.audit.generate(s, np.random.default_rng(59))
        first = h.calculate(self.m, s, values, 123, 199)[0]
        for scale in (1e-6, 1e6):
            transformed = [v * np.array([scale, 1.0]) for v in values]
            second = h.calculate(self.m, s, transformed, 123, 199)[0]
            for method in first:
                for j in range(4):
                    with self.subTest(scale=scale, method=method, estimand=h.LABELS[j]):
                        self.assertEqual(first[method][j][2], second[method][j][2])
                        if first[method][j][2] == "ok":
                            factor = scale if j < 3 else 1.0
                            assert_allclose(
                                np.array(first[method][j][:2]) * factor,
                                second[method][j][:2],
                                rtol=1e-10,
                                atol=1e-12,
                            )


if __name__ == "__main__":
    unittest.main()
