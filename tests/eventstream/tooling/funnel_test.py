from __future__ import annotations

import pandas as pd
import pytest

from tests.eventstream.tooling.fixtures.funnel import test_stream


class TestEventstreamFunnel:
    def test_funnel_eventstream__simple(self, test_stream):
        params = {"stages": ["catalog", ["product1", "product2"], "cart", "payment_done"]}

        idx = pd.MultiIndex.from_product(
            [["all users"], ["catalog", "product1 | product2", "cart", "payment_done"]],
            names=["segment_name", "stages"],
        )
        correct_res = pd.DataFrame(
            [[6.0, 100.00, 100.00], [8.0, 133.33, 133.33], [8.0, 100.00, 133.33], [8.0, 100.00, 133.33]],
            index=idx,
            columns=["unique_users", "%_of_initial", "%_of_total"],
        )

        res = test_stream.funnel(**params).values
        assert correct_res.compare(res).shape == (0, 0)

    def test_cohorts_eventstream__refit(self, test_stream):
        params_1 = {"stages": ["catalog", ["product1", "product2"], "cart", "payment_done"]}

        params_2 = {"stages": ["catalog", ["product1", "product2"], "cart", "payment_done"], "funnel_type": "closed"}

        idx = pd.MultiIndex.from_product(
            [["all users"], ["catalog", "product1 | product2", "cart", "payment_done"]],
            names=["segment_name", "stages"],
        )
        correct_res_1 = pd.DataFrame(
            [[6.0, 100.00, 100.00], [8.0, 133.33, 133.33], [8.0, 100.00, 133.33], [8.0, 100.00, 133.33]],
            index=idx,
            columns=["unique_users", "%_of_initial", "%_of_total"],
        )

        correct_res_2 = pd.DataFrame(
            [[6.0, 100.00, 100.00], [4.0, 66.67, 66.67], [4.0, 100.00, 66.67], [4.0, 100.00, 66.67]],
            index=idx,
            columns=["unique_users", "%_of_initial", "%_of_total"],
        )

        res_1 = test_stream.funnel(**params_1).values
        res_2 = test_stream.funnel(**params_2).values
        calc_is_correct = correct_res_1.round(2).compare(res_1).shape == (0, 0)
        recalc_is_correct = correct_res_2.round(2).compare(res_2).shape == (0, 0)

        assert calc_is_correct and recalc_is_correct

    def test_funnel_eventstream__fit_hash_check(self, test_stream):
        params = {"stages": ["catalog", ["product1", "product2"], "cart", "payment_done"]}

        cc = test_stream.funnel(**params)
        hash1 = hash(cc)
        cc.values
        hash2 = hash(cc)

        assert hash1 == hash2
