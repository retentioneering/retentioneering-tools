from __future__ import annotations

import math

import pytest

from retentioneering.eventstream.types import EventstreamType
from tests.eventstream.tooling.fixtures.stattests import test_stream


class TestEventstreamStattests:
    def test_stattests_eventstream__simple(self, test_stream: EventstreamType) -> None:
        params = {
            "groups": ([1, 2, 3, 4], [5, 6, 7, 8]),
            "func": lambda x: x.shape[0],
            "group_names": ("group_1", "group_2"),
            "test": "ttest",
        }
        correct = 0.13545
        result = test_stream.stattests(**params).values["p_val"]
        assert math.isclose(result, correct, abs_tol=0.001)

    def test_stattests_eventstream__refit(self, test_stream: EventstreamType) -> None:
        params_1 = {
            "groups": ([1, 2, 3, 4], [5, 6, 7, 8]),
            "func": lambda x: x.shape[0],
            "group_names": ("group_1", "group_2"),
            "test": "ttest",
        }
        params_2 = {
            "groups": ([1, 2, 3, 4], [5, 6, 7, 8]),
            "func": lambda x: x.shape[0],
            "group_names": ("group_1", "group_2"),
            "test": "mannwhitneyu",
        }

        correct_res_1 = 0.13545
        correct_res_2 = 0.1859

        res_1 = test_stream.stattests(**params_1).values["p_val"]
        res_2 = test_stream.stattests(**params_2).values["p_val"]
        assert math.isclose(res_1, correct_res_1, abs_tol=0.001), "First calculation"
        assert math.isclose(res_2, correct_res_2, abs_tol=0.001), "Refit"

    def test_ttest_correctness(self, test_stream: EventstreamType) -> None:
        st = test_stream.stattests(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
        )

        res_p_val = st.values["p_val"]
        assert math.isclose(res_p_val, 0.13545, abs_tol=0.0001)
        assert st.values["is_group_one_greatest"]
