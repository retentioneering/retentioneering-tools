import os
from time import sleep

import pandas as pd
import pytest

from .fixtures.utils import set_local_tracker


class TestSimpleShop:
    @pytest.mark.usefixtures("set_local_tracker")
    def test_simple_shop_stream(self) -> None:
        from retentioneering.datasets import load_simple_shop

        args = {"as_dataframe": False}

        stream = load_simple_shop(**args)
        sleep(3)

        from retentioneering.utils.tracker_analytics_tools import process_data

        logs = process_data(pd.read_csv(os.environ["RETE_TRACKER_CSV_PATH"]))
        log = logs[logs["event_custom_name"] == "load_simple_shop_end"].iloc[0]

        assert log["args"] == args
        assert log["performance_before"] == {}
        assert log["performance_after"] == {}

    @pytest.mark.usefixtures("set_local_tracker")
    def test_simple_shop_dataframe(self) -> None:
        from retentioneering.datasets import load_simple_shop

        args = {"as_dataframe": True}

        stream = load_simple_shop(**args)
        sleep(2)

        from retentioneering.utils.tracker_analytics_tools import process_data

        logs = process_data(pd.read_csv(os.environ["RETE_TRACKER_CSV_PATH"]))
        log = logs[logs["event_custom_name"] == "load_simple_shop_end"].iloc[0]

        assert log["args"] == args
        assert log["performance_before"] == {}
        assert log["performance_after"] == {}
