from __future__ import annotations

import pandas as pd
import pytest

from tests.eventstream.tooling.fixtures.funnel import test_stream


class TestEventstreamFunnel:
    def test_funnel_eventstream__simple(self, test_stream):
        params = {"funnel_type": "open", "stages": ["catalog", ["product1", "product2"], "cart", "payment_done"]}

        idx = pd.MultiIndex.from_product(
            [["all users"], ["catalog", "product1 | product2", "cart", "payment_done"]],
            names=["segment_name", "stages"],
        )
        correct_res = pd.DataFrame(
            [[6.0, 100.00, 100.00], [8.0, 133.33, 133.33], [8.0, 100.00, 133.33], [8.0, 100.00, 133.33]],
            index=idx,
            columns=["unique_users", "%_of_previous", "%_of_initial"],
        )

        res = test_stream.funnel(**params, show_plot=False).values
        assert pd.testing.assert_frame_equal(res[correct_res.columns], correct_res, check_dtype=False) is None

    def test_funnel_eventstream__refit(self, test_stream):
        params_1 = {"funnel_type": "open", "stages": ["catalog", ["product1", "product2"], "cart", "payment_done"]}

        params_2 = {"stages": ["catalog", ["product1", "product2"], "cart", "payment_done"], "funnel_type": "hybrid"}

        idx = pd.MultiIndex.from_product(
            [["all users"], ["catalog", "product1 | product2", "cart", "payment_done"]],
            names=["segment_name", "stages"],
        )
        correct_res_1 = pd.DataFrame(
            [[6.0, 100.00, 100.00], [8.0, 133.33, 133.33], [8.0, 100.00, 133.33], [8.0, 100.00, 133.33]],
            index=idx,
            columns=["unique_users", "%_of_previous", "%_of_initial"],
        )

        correct_res_2 = pd.DataFrame(
            [[6.0, 100.00, 100.00], [4.0, 66.67, 66.67], [4.0, 100.00, 66.67], [4.0, 100.00, 66.67]],
            index=idx,
            columns=["unique_users", "%_of_previous", "%_of_initial"],
        )

        res_1 = test_stream.funnel(**params_1, show_plot=False).values
        res_2 = test_stream.funnel(**params_2, show_plot=False).values
        assert (
            pd.testing.assert_frame_equal(res_1[correct_res_1.columns], correct_res_1, check_dtype=False) is None
        ), "First calculation"
        assert (
            pd.testing.assert_frame_equal(res_2[correct_res_2.columns], correct_res_2, check_dtype=False) is None
        ), "Refit"
