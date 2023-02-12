import os

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream


def read_test_data(filename):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../../datasets/eventstream/tooling/user_lifetime_hist")
    filepath = os.path.join(test_data_dir, filename)
    source_df = pd.read_csv(filepath)
    return source_df


@pytest.fixture
def test_stream():
    source_df = read_test_data("input.csv")
    source_stream = Eventstream(source_df)
    return source_stream


@pytest.fixture
def source_stream_for_log_scale():
    source_df = read_test_data("input_for_log_scale.csv")
    source_stream = Eventstream(source_df)
    return source_stream
