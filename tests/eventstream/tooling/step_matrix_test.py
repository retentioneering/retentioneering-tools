from __future__ import annotations

import pandas as pd
import pytest

from tests.eventstream.tooling.fixtures.step_matrix import stream_simple

FLOAT_PRECISION = 2

# def correct_res_test(test_prefix):
#
#     current_dir = os.path.dirname(os.path.realpath(__file__))
#     test_data_dir = os.path.join(current_dir, "../../datasets/tooling/step_matrix")
#     correct_res_df = pd.read_csv(os.path.join(test_data_dir, f"{test_prefix}.csv"))
#     correct_res_df.columns = correct_res_df.columns.astype(int)
#
#     return correct_res_df.round(FLOAT_PRECISION)


class TestEventstreamStepMatrix:
    def test_step_matrix_eventstream__simple(self, stream_simple):
        params = {"max_steps": 5}

        correct_res = pd.DataFrame(
            [[1.0, 0.5, 0.5, 0.25, 0.25], [0.0, 0.5, 0.25, 0.0, 0.0], [0.0, 0.0, 0.25, 0.0, 0.0]],
            index=["event1", "event2", "event4"],
            columns=[1, 2, 3, 4, 5],
        )
        res, _ = stream_simple.step_matrix(**params).values

        assert correct_res.round(FLOAT_PRECISION).compare(res).shape == (0, 0)

    def test_step_matrix_eventstream__refit(self, stream_simple):
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

        res_1, _ = stream_simple.step_matrix(**params_1).values
        res_2, _ = stream_simple.step_matrix(**params_2).values

        calc_is_correct = correct_res_1.round(FLOAT_PRECISION).compare(res_1).shape == (0, 0)
        recalc_is_correct = correct_res_2.round(FLOAT_PRECISION).compare(res_2).shape == (0, 0)

        assert calc_is_correct and recalc_is_correct

    def test_step_matrix_eventstream__fit_hash_check(self, stream_simple):
        params = {"max_steps": 5}

        cc = stream_simple.step_matrix(**params)
        hash1 = hash(cc)
        cc.values
        hash2 = hash(cc)

        assert hash1 == hash2
