import os

import pandas as pd
import pytest

from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema


@pytest.fixture
def test_df():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_data_dir = os.path.join(current_dir, "../../datasets/transition_graph")
    filepath = os.path.join(test_data_dir, "input.csv")

    df = pd.read_csv(filepath)

    return df
