import os

import pandas as pd
import pytest

from src.tooling.step_matrix import StepMatrix
from tests.tooling.fixtures.step_matrix import (
    stream_simple_shop,
    test_stream,
    test_stream_end_path,
)

FLOAT_PRECISION = 3


def read_test_data(filename):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../datasets/tooling/step_matrix")
    filepath = os.path.join(test_data_dir, filename)
    df = pd.read_csv(filepath, index_col=0).round(FLOAT_PRECISION)
    df.columns = df.columns.astype(int)
    return df


def run_test(stream, correct, output, **kwargs):
    sm = StepMatrix(eventstream=stream, **kwargs)
    result = None
    if isinstance(correct, str):
        try:
            result_correct = read_test_data(correct)
        except:
            result_correct = correct
    else:
        result_correct = correct
    if output == "matrix":
        result = sm._get_plot_data()[0].round(FLOAT_PRECISION)
    elif output == "targets_table":
        result = sm._get_plot_data()[1].round(FLOAT_PRECISION)
    elif output == "users_fraction":
        result = sm._get_plot_data()[2]
    elif output == "targets_list":
        result = pd.DataFrame(sm._get_plot_data()[3])
    if isinstance(result, str):
        test_is_correct = result == result_correct
    else:
        test_is_correct = result.compare(result_correct).shape == (0, 0)
    return test_is_correct


class TestStepMatrix:
    def test_step_matrix_simple(self, test_stream):
        correct_result = pd.DataFrame(
            [
                [1.0, 0.667, 0.333, 0.167, 0.167],
                [0.0, 0.333, 0.5, 0.167, 0.0],
                [0.0, 0.0, 0.0, 0.167, 0.167],
                [0.0, 0.0, 0.0, 0.167, 0.0],
            ],
            index=["event1", "event2", "event3", "event5"],
            columns=[1, 2, 3, 4, 5],
        )
        assert run_test(test_stream, correct_result, "matrix", max_steps=5)

    def test_step_matrix__max_steps_100(self, stream_simple_shop):
        assert run_test(stream_simple_shop, "03_100_steps.csv", "matrix", max_steps=100, precision=3)

    def test_step_matrix__max_steps_one(self, test_stream):
        correct_result = pd.DataFrame([[1.0]], index=["event1"], columns=[1])
        run_test(test_stream, correct_result, "matrix", max_steps=1)

    def test_step_matrix__thresh(self, test_stream):
        correct_result = pd.DataFrame(
            [
                [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],
                [0.0, 0.333, 0.5, 0.167, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.333, 0.167, 0.167],
            ],
            index=["event1", "event2", "THRESHOLDED_2"],
            columns=[1, 2, 3, 4, 5, 6],
        )
        assert run_test(test_stream, correct_result, "matrix", max_steps=6, thresh=0.3)

    def test_step_matrix__thresh_1(self, test_stream):
        correct_result = pd.DataFrame(
            [
                [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],
                [0.0, 0.333, 0.5, 0.5, 0.167, 0.167],
            ],
            index=["event1", "THRESHOLDED_3"],
            columns=[1, 2, 3, 4, 5, 6],
        )
        assert run_test(test_stream, correct_result, "matrix", max_steps=6, thresh=1.0)

    def test_step_matrix__targets_plot(self, test_stream):
        correct_result = pd.DataFrame(
            [[0.0, 0.0, 0.0, 0.167, 0.167, 0.167]],
            index=["event3"],
            columns=[1, 2, 3, 4, 5, 6],
        )
        assert run_test(test_stream, correct_result, "targets_table", max_steps=6, targets=["event3"])

    def test_step_matrix__targets_thresh_plot(self, test_stream):
        correct_result = pd.DataFrame(
            [[0.0, 0.0, 0.0, 0.167, 0.167, 0.167], [0.0, 0.0, 0.0, 0.167, 0.0, 0.0]],
            index=["event3", "event5"],
            columns=[1, 2, 3, 4, 5, 6],
        )
        assert run_test(
            test_stream, correct_result, "targets_table", max_steps=6, targets=["event3", "event5"], thresh=0.5
        )

    def test_step_matrix__targets_grouping(self, test_stream):
        correct_result = pd.DataFrame([["event3", "event5"]])
        assert run_test(test_stream, correct_result, "targets_list", max_steps=6, targets=[["event3", "event5"]])

    def test_step_matrix__accumulated_only_targets_plot(self, test_stream):
        correct_result = pd.DataFrame(
            [[0.0, 0.0, 0.0, 0.167, 0.167, 0.167, 0.167, 0.333, 0.333, 0.5]],
            index=["ACC_event5"],
            columns=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        )
        assert run_test(
            test_stream, correct_result, "targets_table", max_steps=10, targets=["event5"], accumulated="only"
        )

    def test_step_matrix__accumulated_both_targets_plot(self, test_stream):
        correct_result = pd.DataFrame(
            [
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.333, 0.0, 0.167, 0.0],
                [0.0, 0.0, 0.0, 0.167, 0.0, 0.0, 0.0, 0.167, 0.0, 0.167],
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.333, 0.333, 0.5, 0.5],
                [0.0, 0.0, 0.0, 0.167, 0.167, 0.167, 0.167, 0.333, 0.333, 0.5],
            ],
            index=["event4", "event5", "ACC_event4", "ACC_event5"],
            columns=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        )

        assert run_test(
            test_stream, correct_result, "targets_table", max_steps=10, targets=["event4", "event5"], accumulated="both"
        )

    def test_step_matrix__centered(self, test_stream):
        correct_result = pd.DataFrame(
            [
                [0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.5, 0.5, 0.5, 0.0, 0.5, 0.5, 0.0, 0.0, 0.0],
                [0.0, 0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.5, 0.0, 0.5, 0.0, 0.5, 0.0, 0.0],
                [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.5, 0.0, 0.0, 0.0],
            ],
            index=(["event2", "event1", "event3", "event4", "event5"]),
            columns=[-4, -3, -2, -1, 0, 1, 2, 3, 4, 5],
        )

        correct_result.columns.name = None
        correct_result.index.name = None

        assert run_test(
            test_stream,
            correct_result,
            "matrix",
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
        )

    def test_step_matrix__centered_target_thresh(self, test_stream):

        correct_result = pd.DataFrame(
            [
                [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.5, 0.0, 0.0, 0.0],
                [0.5, 1.0, 1.0, 1.0, 0.0, 1.0, 0.5, 0.5, 0.0, 0.0],
            ],
            index=(["event5", "THRESHOLDED_4"]),
            columns=[-4, -3, -2, -1, 0, 1, 2, 3, 4, 5],
        )

        assert run_test(
            test_stream,
            correct_result,
            "matrix",
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
            targets=["event4"],
            thresh=0.6,
        )

    def test_step_matrix__centered_target_thresh_plot(self, test_stream):
        correct_result = pd.DataFrame(
            [
                [0.0, 0.0, 0.0, 0.5, 0.0, 0.5, 0.0, 0.5, 0.0, 0.0],
            ],
            index=(["event4"]),
            columns=[-4, -3, -2, -1, 0, 1, 2, 3, 4, 5],
        )

        assert run_test(
            test_stream,
            correct_result,
            "targets_table",
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
            targets=["event4"],
            thresh=0.6,
        )

    def test_step_matrix__centered_name(self, test_stream):
        correct_result = "(33.3% of total records)"
        assert run_test(
            test_stream,
            correct_result,
            "users_fraction",
            max_steps=10,
            centered={"event": "event5", "left_gap": 4, "occurrence": 1},
            thresh=0.6,
        )

    def test_step_matrix__events_sorting(self, test_stream):
        correct_result = pd.DataFrame(
            [
                [0.0, 0.0, 0.0, 0.167, 0.0, 0.0],  # event5
                [0.0, 0.0, 0.0, 0.167, 0.167, 0.167],  # event3
                [0.0, 0.333, 0.50, 0.167, 0.0, 0.0],  # event2
                [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],  # event1
            ],
            index=(["event5", "event3", "event2", "event1"]),
            columns=[1, 2, 3, 4, 5, 6],
        )
        new_order = ["event5", "event3", "event2", "event1"]
        assert run_test(test_stream, correct_result, "matrix", sorting=new_order, max_steps=6)

    def test_step_matrix__differential(self, test_stream):
        g1 = [3, 6]
        g2 = [1, 2, 5]
        correct_result = pd.DataFrame(
            [
                [0, 1.0, 0.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
                [0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0],
                [0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0],
            ],
            index=(["event1", "event2", "event3", "event4", "event5"]),
            columns=[-5, -4, -3, -2, -1, 0, 1, 2, 3, 4],
        )
        assert run_test(
            test_stream,
            correct_result,
            "matrix",
            max_steps=10,
            centered={"event": "event3", "left_gap": 5, "occurrence": 1},
            groups=(g1, g2),
        )

    def test_step_matrix__differential_name(self, test_stream):
        g1 = [3, 6]
        g2 = [1, 2, 5]
        correct_result = "(33.3% of total records)"
        assert run_test(
            test_stream,
            correct_result,
            "users_fraction",
            max_steps=10,
            centered={"event": "event3", "left_gap": 5, "occurrence": 1},
            groups=(g1, g2),
        )

    def test_step_matrix__path_end(self, test_stream_end_path):

        correct_result = pd.DataFrame(
            [
                [1.0, 0.5, 0.333, 0.167, 0.167],
                [0, 0.5, 0.333, 0.0, 0.0],
                [0, 0.0, 0.167, 0.167, 0.0],
                [0, 0.0, 0.0, 0.167, 0.167],
                [0, 0.0, 0.0, 0.167, 0.0],
                [0, 0.0, 0.167, 0.333, 0.667],
            ],
            index=(["event1", "event2", "event3", "event5", "event4", "path_end"]),
            columns=[1, 2, 3, 4, 5],
        )
        assert run_test(test_stream_end_path, correct_result, "matrix", max_steps=5)
