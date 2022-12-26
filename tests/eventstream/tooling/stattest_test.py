from __future__ import annotations

import math

import pandas as pd
import pytest

from tests.eventstream.tooling.fixtures.stattests import test_stream


class TestEventstreamStattests:
    def test_stattests_eventstream__simple(self, test_stream):
        params = {
            "groups": ([1, 2, 3, 4], [5, 6, 7, 8]),
            "func": lambda x: x.shape[0],
            "group_names": ("group_1", "group_2"),
            "test": "ttest",
        }
        correct = 0.13545
        result = test_stream.stattests(**params).values["p_val"]
        assert math.isclose(result, correct, abs_tol=0.001)

    def test_stattests_eventstream__refit(self, test_stream):
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

    def test_stattests_eventstream__fit_hash_check(self, test_stream):
        params = {
            "groups": ([1, 2, 3, 4], [5, 6, 7, 8]),
            "func": lambda x: x.shape[0],
            "group_names": ("group_1", "group_2"),
            "test": "ttest",
        }

        st = test_stream.stattests(**params)
        hash1 = hash(st)
        st.values
        hash2 = hash(st)

        assert hash1 == hash2
