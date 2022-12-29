import math

import pandas as pd

from src.eventstream import Eventstream
from src.tooling.stattests import StatTests
from tests.eventstream.fixtures.eventstream import test_stattests_stream_1


class TestEventstreamStattests:
    def test_ttest_correctness(self, test_stattests_stream_1):
        source = test_stattests_stream_1
        st = StatTests(
            eventstream=source,
            groups=([1, 2, 3, 4], [5, 6, 7, 8]),
            func=lambda x: x.shape[0],
            group_names=("group_1", "group_2"),
            test="ttest",
        )
        st.fit()
        res_p_val = st.values["p_val"]
        assert math.isclose(res_p_val, 0.13545, abs_tol=0.0001)
        assert st.values["is_group_one_greatest"]
