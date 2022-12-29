import pandas as pd
import pytest

from src.eventstream import Eventstream
from tests.eventstream.fixtures.eventstream import test_stattests_stream_1


class TestEventstreamStattests:
    def test_stattests_working(self, test_stattests_stream_1):
        source = test_stattests_stream_1
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
