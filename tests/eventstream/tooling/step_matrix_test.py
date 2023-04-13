from __future__ import annotations

import pandas as pd
import pytest

from tests.eventstream.tooling.fixtures.step_matrix import test_stream

FLOAT_PRECISION = 2


class TestEventstreamStepMatrix:
    def test_step_matrix_eventstream__simple(self, test_stream):
        params = {"max_steps": 5}

        correct_res = pd.DataFrame(
            [
                [1.0, 0.5, 0.5, 0.25, 0.25],
                [0.0, 0.5, 0.25, 0.0, 0.0],
                [0.0, 0.0, 0.25, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.75, 0.75],
            ],
            index=["event1", "event2", "event4", "ENDED"],
            columns=[1, 2, 3, 4, 5],
        )
        res, _ = test_stream.step_matrix(**params, show_plot=False).values

        assert pd.testing.assert_frame_equal(res[correct_res.columns], correct_res) is None

    def test_step_matrix_eventstream__refit(self, test_stream):
        params_1 = {"max_steps": 5}

        params_2 = {"max_steps": 5, "threshold": 0.3}

        correct_res_1 = pd.DataFrame(
            [
                [1.0, 0.5, 0.5, 0.25, 0.25],
                [0.0, 0.5, 0.25, 0.0, 0.0],
                [0.0, 0.0, 0.25, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.75, 0.75],
            ],
            index=["event1", "event2", "event4", "ENDED"],
            columns=[1, 2, 3, 4, 5],
        )
        correct_res_2 = pd.DataFrame(
            [
                [1.0, 0.5, 0.5, 0.25, 0.25],
                [0.0, 0.5, 0.25, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.75, 0.75],
                [0.0, 0.0, 0.25, 0.0, 0.0],
            ],
            index=["event1", "event2", "ENDED", "THRESHOLDED_1"],
            columns=[1, 2, 3, 4, 5],
        )

        res_1, _ = test_stream.step_matrix(**params_1, show_plot=False).values
        res_2, _ = test_stream.step_matrix(**params_2, show_plot=False).values

        assert pd.testing.assert_frame_equal(res_1[correct_res_1.columns], correct_res_1) is None, "First calculation"
        assert pd.testing.assert_frame_equal(res_2[correct_res_2.columns], correct_res_2) is None, "Refit"
