import os

import pandas as pd
import pytest

FLOAT_PRECISION = 3


@pytest.fixture
def max_steps_cor():
    correct_result = pd.DataFrame(
        [
            [1.0, 0.667, 0.333, 0.167, 0.167],
            [0.0, 0.333, 0.5, 0.167, 0.0],
            [0.0, 0.0, 0.0, 0.167, 0.167],
            [0.0, 0.0, 0.0, 0.167, 0.0],
            [0.0, 0.0, 0.167, 0.333, 0.667],
        ],
        index=["event1", "event2", "event3", "event5", "ENDED"],
        columns=[1, 2, 3, 4, 5],
    )
    return correct_result


@pytest.fixture
def max_steps_100_cor():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/tooling/step_matrix")
    filepath = os.path.join(test_data_dir, "03_100_steps.csv")
    df = pd.read_csv(filepath, index_col=0).round(FLOAT_PRECISION)
    df.columns = df.columns.astype(int)
    return df


@pytest.fixture
def max_steps_one_cor():
    correct_result = pd.DataFrame([[1.0]], index=["event1"], columns=[1])
    return correct_result


@pytest.fixture
def thresh_cor():
    correct_result = pd.DataFrame(
        [
            [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],
            [0.0, 0.333, 0.5, 0.167, 0.0, 0.0],
            [0.0, 0.0, 0.167, 0.333, 0.667, 0.667],
            [0.0, 0.0, 0.0, 0.333, 0.167, 0.167],
        ],
        index=["event1", "event2", "ENDED", "THRESHOLDED_2"],
        columns=[1, 2, 3, 4, 5, 6],
    )
    return correct_result


@pytest.fixture
def thresh_1_cor():
    correct_result = pd.DataFrame(
        [
            [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],
            [0.0, 0.0, 0.167, 0.333, 0.667, 0.667],
            [0.0, 0.333, 0.5, 0.5, 0.167, 0.167],
        ],
        index=["event1", "ENDED", "THRESHOLDED_3"],
        columns=[1, 2, 3, 4, 5, 6],
    )
    return correct_result


@pytest.fixture
def targets_plot_cor():
    correct_result = pd.DataFrame(
        [[0.0, 0.0, 0.0, 0.167, 0.167, 0.167]],
        index=["event3"],
        columns=[1, 2, 3, 4, 5, 6],
    )
    return correct_result


@pytest.fixture
def targets_thresh_plot_cor():
    correct_result = pd.DataFrame(
        [[0.0, 0.0, 0.0, 0.167, 0.167, 0.167], [0.0, 0.0, 0.0, 0.167, 0.0, 0.0]],
        index=["event3", "event5"],
        columns=[1, 2, 3, 4, 5, 6],
    )
    return correct_result


@pytest.fixture
def targets_grouping_cor():
    correct_result = [["event3", "event5"]]
    return correct_result


@pytest.fixture
def accumulated_only_targets_plot_cor():
    correct_result = pd.DataFrame(
        [[0.0, 0.0, 0.0, 0.167, 0.167, 0.167, 0.167, 0.333, 0.333, 0.5]],
        index=["ACC_event5"],
        columns=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    )
    return correct_result


@pytest.fixture
def accumulated_both_targets_plot_cor():
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
    return correct_result


@pytest.fixture
def centered_cor():
    correct_result = pd.DataFrame(
        [
            [0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.5, 0.5, 0.5, 0.0, 0.5, 0.5, 0.0, 0.0, 0.0],
            [0.0, 0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.5, 0.0, 0.5, 0.0, 0.5, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.5, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 1.0],
        ],
        index=(["event2", "event1", "event3", "event4", "event5", "ENDED"]),
        columns=["-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5"],
    )
    return correct_result


@pytest.fixture
def centered_target_thresh_cor():
    correct_result = pd.DataFrame(
        [
            [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.5, 0.0, 0.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 1.0],
            [0.5, 1.0, 1.0, 1.0, 0.0, 1.0, 0.5, 0.5, 0.0, 0.0],
        ],
        index=(["event5", "ENDED", "THRESHOLDED_4"]),
        columns=["-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5"],
    )
    return correct_result


@pytest.fixture
def centered_target_thresh_plot_cor():
    correct_result = pd.DataFrame(
        [
            [0.0, 0.0, 0.0, 0.5, 0.0, 0.5, 0.0, 0.5, 0.0, 0.0],
        ],
        index=(["event4"]),
        columns=["-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5"],
    )
    return correct_result


@pytest.fixture
def centered_name_cor():
    correct_result = "(33.3% of total records)"
    return correct_result


@pytest.fixture
def events_sorting_cor():
    correct_result = pd.DataFrame(
        [
            [0.0, 0.0, 0.0, 0.167, 0.0, 0.0],  # event5
            [0.0, 0.0, 0.0, 0.167, 0.167, 0.167],  # event3
            [0.0, 0.333, 0.50, 0.167, 0.0, 0.0],  # event2
            [1.0, 0.667, 0.333, 0.167, 0.167, 0.167],  # event1
            [0.0, 0.0, 0.167, 0.333, 0.667, 0.667],  # ENDED
        ],
        index=(["event5", "event3", "event2", "event1", "ENDED"]),
        columns=[1, 2, 3, 4, 5, 6],
    )
    return correct_result


@pytest.fixture
def differential_cor():
    correct_result = pd.DataFrame(
        [
            [0, 1.0, 0.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
            [0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0],
            [0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0],
            [0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0, -1.0, -1.0, -1.0],
        ],
        index=(["event1", "event2", "event3", "event4", "event5", "ENDED"]),
        columns=["-5", "-4", "-3", "-2", "-1", "0", "1", "2", "3", "4"],
    )
    return correct_result


@pytest.fixture
def differential_name_cor():
    correct_result = "(33.3% of total records)"
    return correct_result


@pytest.fixture
def path_end_cor():
    correct_result = pd.DataFrame(
        [
            [1.0, 0.5, 0.333, 0.167, 0.167],
            [0, 0.5, 0.333, 0.0, 0.0],
            [0, 0.0, 0.167, 0.167, 0.0],
            [0, 0.0, 0.0, 0.167, 0.167],
            [0, 0.0, 0.0, 0.167, 0.0],
            [0, 0.0, 0.167, 0.333, 0.667],
        ],
        index=(["event1", "event2", "event3", "event5", "event4", "ENDED"]),
        columns=[1, 2, 3, 4, 5],
    )
    return correct_result


@pytest.fixture
def weight_col_cor():
    correct_result = pd.DataFrame(
        [
            [0.667, 0.0, 0.0, 1.0, 0.333],
            [0.333, 0.333, 0.0, 0.0, 0.667],
            [0, 0.0, 0.667, 0.0, 0.0],
            [0, 0.333, 0.0, 0.0, 0.0],
            [0, 0.333, 0.333, 0.0, 0.0],
        ],
        index=(["event1", "event2", "event5", "event3", "event4"]),
        columns=[1, 2, 3, 4, 5],
    )
    return correct_result
