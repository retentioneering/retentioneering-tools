import os

import pandas as pd
import pytest

from src.eventstream import Eventstream


class TestTest:
    def test_test__first(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        test_data_dir = os.path.join(current_dir, "../datasets/tooling/stattests")
        source_df = pd.read_csv(os.path.join(test_data_dir, "01_simple_data.csv"))

        source = Eventstream(source_df)
        try:
            source.stattests(
                groups=([1, 2, 3, 4], [5, 6, 7, 8]),
                func=lambda x: x.shape[0],
                group_names=("group_1", "group_2"),
                test="ttest",
            )
        except Exception as e:
            pytest.fail("Runtime error in Eventstream.stattests. " + str(e))
        try:
            source.stattests(
                groups=([1, 2, 3, 4], [5, 6, 7, 8]),
                func=lambda x: x.shape[0],
                group_names=("group_1", "group_2"),
                test="chi2_contingency",
            )
        except Exception as e:
            pytest.fail("Runtime error in Eventstream.stattests. " + str(e))
