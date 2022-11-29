import os

import pandas as pd

from src.tooling.step_matrix import StepMatrix
from tests.tooling.fixtures.step_matrix import stream_simple, stream_simple_shop

FLOAT_PRECISION = 6


def read_test_data(filename):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../datasets/tooling/step_matrix")
    filepath = os.path.join(test_data_dir, filename)
    df = pd.read_csv(filepath, index_col=0).round(FLOAT_PRECISION)
    df.columns = df.columns.astype(int)
    return df


def run_test(stream, filename, **kwargs):
    sm = StepMatrix(eventstream=stream, **kwargs)
    sm.fit()
    result, _ = sm.values
    result = result.round(FLOAT_PRECISION)
    result_correct = read_test_data(filename)
    test_is_correct = result.compare(result_correct).shape == (0, 0)
    return test_is_correct


class TestStepMatrix:
    def test_step_matrix__simple(self, stream_simple):
        sm = StepMatrix(eventstream=stream_simple, max_steps=5)
        sm.fit()
        result, _ = sm.values

        correct_result = pd.DataFrame(
            [[1.0, 0.5, 0.5, 0.25, 0.25], [0.0, 0.5, 0.25, 0.0, 0.0], [0.0, 0.0, 0.25, 0.0, 0.0]],
            index=["event1", "event2", "event4"],
            columns=[1, 2, 3, 4, 5],
        )
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__simple_thresh(self, stream_simple):
        sm = StepMatrix(eventstream=stream_simple, max_steps=5, thresh=0.3)
        sm.fit()
        result, _ = sm.values

        correct_result = pd.DataFrame(
            [[1.0, 0.5, 0.5, 0.25, 0.25], [0.0, 0.5, 0.25, 0.0, 0.0], [0.0, 0.0, 0.25, 0.0, 0.0]],
            index=["event1", "event2", "THRESHOLDED_1"],
            columns=[1, 2, 3, 4, 5],
        )
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__simple_target(self, stream_simple):
        sm = StepMatrix(eventstream=stream_simple, max_steps=5, targets=["event3"])
        sm.fit()
        result, targets_result = sm.values
        result = pd.concat([result, targets_result])

        correct_result = pd.DataFrame(
            [
                [1.0, 0.5, 0.5, 0.25, 0.25],
                [0.0, 0.5, 0.25, 0.0, 0.0],
                [0.0, 0.0, 0.25, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 0.0],
            ],
            index=["event1", "event2", "event4", "event3"],
            columns=[1, 2, 3, 4, 5],
        )
        assert result.compare(correct_result).shape == (0, 0)

    def test_step_matrix__basic(self, stream_simple_shop):
        assert run_test(stream_simple_shop, "01_basic.csv")

    def test_step_matrix__one_step(self, stream_simple_shop):
        assert run_test(stream_simple_shop, "02_one_step.csv", max_steps=1)

    def test_step_matrix__100_steps(self, stream_simple_shop):
        assert run_test(stream_simple_shop, "03_100_steps.csv", max_steps=100, precision=3)
