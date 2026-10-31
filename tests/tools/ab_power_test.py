"""``get_ab_transitions_power`` / ``get_ab_steps_power``: how much data a screen needs
(``tools/ab_power.py``).

At ``scale=1`` every projected number must be the getter's (``mde``, the tested rows,
the family size); the MDE of a z-test or Welch row must scale as ``1 / sqrt(scale)``
at a fixed level; ``paths_needed`` must bring the MDE to ``target_delta``; and the
curve must summarise the rows.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from retentioneering import datasets
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.exceptions import InvalidParameterError
from retentioneering.tools import _stats
from retentioneering.tools.ab_power import DEFAULT_SCALE, check_scale

DIFF = ("arm", "treatment", "control")


@pytest.fixture(scope="module")
def stream() -> Eventstream:
    return datasets.load_ab_test()


@pytest.fixture(scope="module")
def assignment() -> pd.DataFrame:
    return datasets.load_ab_test_assignment()


def _at_scale_one(power: pd.DataFrame, frame: pd.DataFrame, keys) -> pd.DataFrame:
    one = power[power["scale"] == 1].set_index(list(keys))
    return one.join(frame.set_index(list(keys)), rsuffix="_getter")


class TestScaleOneIsTheGetter:
    @pytest.mark.parametrize(
        "weight", ["unique_paths", "avg_per_path", "proba_out", "share_of_total"]
    )
    def test_transitions(self, stream, assignment, weight):
        args = dict(assignment=assignment, n_bootstrap=200)
        if weight in ("unique_paths", "avg_per_path"):
            args.pop("n_bootstrap")
        power = stream.get_ab_transitions_power(DIFF, weight, **args)
        frame = stream.get_ab_transitions(DIFF, weight, **args)
        joined = _at_scale_one(power, frame, ("source", "target"))
        assert len(joined) == len(frame)
        np.testing.assert_allclose(joined["mde"], joined["mde_getter"], equal_nan=True)
        np.testing.assert_allclose(
            joined["mde_alpha"], joined["mde_alpha_getter"], equal_nan=True
        )
        tested = np.isfinite(joined["p_value_adjusted"].to_numpy(dtype=float))
        assert (joined["testable"].to_numpy() == tested).all()
        assert int(joined["family_size"].iloc[0]) == frame.attrs["n_tested"]
        assert (joined["group1_paths"] == joined["group1_paths_getter"]).all()
        support = joined["group1_edge_paths"] + joined["group2_edge_paths"]
        np.testing.assert_allclose(joined["pooled_support"], support)

    def test_steps(self, stream, assignment):
        power = stream.get_ab_steps_power(DIFF, 8, assignment=assignment)
        frame = stream.get_ab_steps(DIFF, 8, assignment=assignment)
        joined = _at_scale_one(power, frame, ("event", "step"))
        np.testing.assert_allclose(joined["mde"], joined["mde_getter"], equal_nan=True)
        tested = np.isfinite(joined["p_value_adjusted"].to_numpy(dtype=float))
        assert (joined["testable"].to_numpy() == tested).all()
        assert int(joined["family_size"].iloc[0]) == frame.attrs["n_tested"]
        curve = {c["scale"]: c for c in power.attrs["curve"]}
        assert curve[1.0]["testable"] == frame.attrs["n_tested"]
        assert curve[1.0]["cells"] == len(frame)

    def test_attrs_keep_the_getters(self, stream, assignment):
        power = stream.get_ab_transitions_power(DIFF, assignment=assignment)
        assert power.attrs["tool"] == "get_ab_transitions_power"
        assert power.attrs["edge_weight"] == "unique_paths"
        assert power.attrs["scale"] == [float(s) for s in DEFAULT_SCALE]
        assert "same path composition" in power.attrs["assumption"]


class TestProjection:
    def test_mde_scales_as_one_over_sqrt_scale(self, stream, assignment):
        # a fixed level: without a correction the family does not move the level
        for weight in ("unique_paths", "avg_per_path"):
            power = stream.get_ab_transitions_power(
                DIFF, weight, assignment=assignment, correction="none"
            )
            wide = power.pivot_table(
                index=["source", "target"], columns="scale", values="mde"
            )
            for factor in (0.25, 4.0, 16.0):
                ratio = (wide[factor] / wide[1.0]).dropna()
                np.testing.assert_allclose(ratio, 1 / math.sqrt(factor), rtol=1e-9)

    def test_family_level_follows_the_testable_rows(self, stream, assignment):
        power = stream.get_ab_steps_power(DIFF, 10, assignment=assignment)
        for factor, part in power.groupby("scale"):
            m = int(part["testable"].sum())
            assert (part["family_size"] == m).all()
            assert part["mde_alpha"].iloc[0] == pytest.approx(0.05 / m)

    def test_support_grows_with_scale_and_testable_is_monotone(
        self, stream, assignment
    ):
        power = stream.get_ab_transitions_power(DIFF, assignment=assignment)
        wide = power.pivot_table(
            index=["source", "target"], columns="scale", values="testable"
        ).astype(bool)
        scales = sorted(wide.columns)
        for low, high in zip(scales, scales[1:]):
            assert (wide[high] | ~wide[low]).all()
        curve = pd.DataFrame(power.attrs["curve"])
        assert curve["testable"].is_monotonic_increasing
        assert curve["mde_median"].is_monotonic_decreasing

    def test_scale_to_test(self, stream, assignment):
        power = stream.get_ab_transitions_power(DIFF, assignment=assignment)
        one = power[power["scale"] == 1]
        reach = one["scale_to_test"].to_numpy(dtype=float)
        testable = one["testable"].to_numpy(dtype=bool)
        assert (reach[testable] <= 1 + 1e-12).all()
        assert (~(reach[~testable] <= 1)).all()

    def test_paths_needed_reach_the_target(self, stream, assignment):
        target = 0.01
        power = stream.get_ab_transitions_power(
            DIFF, assignment=assignment, target_delta=target
        )
        at_one = power[power["scale"] == 1]
        # never fewer paths than the support rule needs; none for a row that no
        # multiple makes testable
        assert (at_one["scale_needed"] >= at_one["scale_to_test"] - 1e-12).all()
        assert at_one.loc[at_one["scale_to_test"].isna(), "scale_needed"].isna().all()
        one = at_one[at_one["testable"]].dropna(subset=["scale_needed"])
        # where the MDE, not the support rule, sets the paths needed
        one = one[one["scale_needed"] > one["scale_to_test"] + 1e-9]
        assert len(one)
        # the getter's own MDE at the needed paths, at the family level of scale 1
        level = float(one["mde_alpha"].iloc[0])
        pooled = one["pooled_support"] / (one["group1_paths"] + one["group2_paths"])
        mde = _stats.mde_proportion(
            pooled.to_numpy(),
            one["group1_paths_needed"].to_numpy(dtype=float),
            one["group2_paths_needed"].to_numpy(dtype=float),
            level,
            0.8,
        )
        # within rounding: paths are whole, so the MDE is at most the target and
        # one path fewer per arm would not reach it
        assert (mde <= target + 1e-12).all()
        fewer = _stats.mde_proportion(
            pooled.to_numpy(),
            one["group1_paths_needed"].to_numpy(dtype=float) - 2,
            one["group2_paths_needed"].to_numpy(dtype=float) - 2,
            level,
            0.8,
        )
        assert (fewer > target - 1e-4).all()
        curve = {c["scale"]: c for c in power.attrs["curve"]}
        testable = at_one[at_one["testable"]]
        reached = (testable["mde"] <= target).mean()
        assert curve[1.0]["share_reaching_target"] == pytest.approx(reached)

    def test_bootstrap_rows_scale_the_bootstrap_se(self, stream, assignment):
        power = stream.get_ab_transitions_power(
            DIFF, "proba_out", assignment=assignment, n_bootstrap=200, correction="none"
        )
        frame = stream.get_ab_transitions(
            DIFF, "proba_out", assignment=assignment, n_bootstrap=200, correction="none"
        )
        four = power[power["scale"] == 4].set_index(["source", "target"])
        se = frame.set_index(["source", "target"])["delta_se"]
        expected = _stats.mde_from_se(se.loc[four.index] / 2, 0.05, 0.8)
        np.testing.assert_allclose(four["mde"], expected, equal_nan=True)


class TestValidation:
    def test_scale(self, stream):
        assert check_scale([4, 0.5]) == [0.5, 1.0, 4.0]
        for bad in ([], [0], [-1], ["2"], [True], "x", [float("nan")]):
            with pytest.raises(InvalidParameterError):
                stream.get_ab_transitions_power(
                    ("exp_group", "treatment", "control"), scale=bad
                )

    def test_target_delta(self, stream):
        for bad in (0, -0.1, "0.1", True, float("inf")):
            with pytest.raises(InvalidParameterError):
                stream.get_ab_steps_power(
                    ("exp_group", "treatment", "control"), 3, target_delta=bad
                )

    def test_getter_errors_pass_through(self, stream):
        with pytest.raises(InvalidParameterError):
            stream.get_ab_transitions_power(
                ("exp_group", "treatment", "control"), "count"
            )
        with pytest.raises(InvalidParameterError):
            stream.get_ab_steps_power(
                ("exp_group", "treatment", "control"), path_pattern="cart"
            )

    def test_signatures_extend_the_getters(self):
        import inspect

        for power, getter in (
            ("get_ab_transitions_power", "get_ab_transitions"),
            ("get_ab_steps_power", "get_ab_steps"),
        ):
            p = list(inspect.signature(getattr(Eventstream, power)).parameters)
            g = list(inspect.signature(getattr(Eventstream, getter)).parameters)
            assert p[: len(g)] == g
            assert p[len(g) :] == ["scale", "target_delta"]
            doc = getattr(Eventstream, power).__doc__
            assert "target_delta" in doc and "conservative for Benjamini" in doc
