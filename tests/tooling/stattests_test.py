import math
import os

import numpy as np
import pandas as pd

from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema


class TestTest:
    def test_test__first(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        test_data_dir = os.path.join(current_dir, "../datasets/tooling/stattests")
        source_df = pd.read_csv(os.path.join(test_data_dir, "01_simple_data.csv"))

        source = Eventstream(source_df)
        test_results = source.stattests(
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            objective=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
        )
        res_p_val = test_results.values()["p_val"]
        assert math.isclose(res_p_val, 0.13545, abs_tol=0.0001)
        assert test_results.values()["is_group_one_greatest"]
