from __future__ import annotations

import pandas as pd
import pytest

from tests.eventstream.tooling.fixtures.step_matrix import test_stream

FLOAT_PRECISION = 2


class TestEventstreamStepMatrix:
    def test_step_matrix_eventstream__simple(self, test_stream):
        params = {"max_steps": 5}

        correct_res = pd.DataFrame(
            [[1.0, 0.5, 0.5, 0.25, 0.25], [0.0, 0.5, 0.25, 0.0, 0.0], [0.0, 0.0, 0.25, 0.0, 0.0]],
            index=["event1", "event2", "event4"],
            columns=[1, 2, 3, 4, 5],
        )
        res, _ = test_stream.step_matrix(**params, show_plot=False).values

        assert correct_res.round(FLOAT_PRECISION).compare(res).shape == (0, 0)

    def test_step_matrix_eventstream__refit(self, test_stream):
        params_1 = {"max_steps": 5}

        params_2 = {"max_steps": 5, "thresh": 0.3}

        correct_res_1 = pd.DataFrame(
            [[1.0, 0.5, 0.5, 0.25, 0.25], [0.0, 0.5, 0.25, 0.0, 0.0], [0.0, 0.0, 0.25, 0.0, 0.0]],
            index=["event1", "event2", "event4"],
            columns=[1, 2, 3, 4, 5],
        )
        correct_res_2 = pd.DataFrame(
            [[1.0, 0.5, 0.5, 0.25, 0.25], [0.0, 0.5, 0.25, 0.0, 0.0], [0.0, 0.0, 0.25, 0.0, 0.0]],
            index=["event1", "event2", "THRESHOLDED_1"],
            columns=[1, 2, 3, 4, 5],
        )

        res_1, _ = test_stream.step_matrix(**params_1, show_plot=False).values
        res_2, _ = test_stream.step_matrix(**params_2, show_plot=False).values

        assert correct_res_1.round(FLOAT_PRECISION).compare(res_1).shape == (0, 0), "First calculation"
        assert correct_res_2.round(FLOAT_PRECISION).compare(res_2).shape == (0, 0), "Refit"

    def test_step_matrix_eventstream__fit_hash_check(self, test_stream):
        params = {"max_steps": 5}

        cc = test_stream.step_matrix(**params, show_plot=False)
        hash1 = hash(cc)
        hash2 = hash(cc)

        assert hash1 == hash2
