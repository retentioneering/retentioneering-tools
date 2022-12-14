import math
import os

import pandas as pd

from src.eventstream import Eventstream
from src.tooling.stattests import StatTests


class TestTest:
    def test_test__first(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        test_data_dir = os.path.join(current_dir, "../datasets/tooling/stattests")
        source_df = pd.read_csv(os.path.join(test_data_dir, "01_simple_data.csv"))

        source = Eventstream(source_df)
        st = StatTests(
            eventstream=source,
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            function=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
        )
        st.fit()
        res_p_val = st.values["p_val"]
        assert math.isclose(res_p_val, 0.13545, abs_tol=0.0001)
        assert st.values["is_group_one_greatest"]
