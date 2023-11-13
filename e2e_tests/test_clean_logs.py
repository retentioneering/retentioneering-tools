import os
from time import sleep

import pandas as pd
import pytest

from retentioneering.datasets import load_simple_shop
from retentioneering.utils.tracker_analytics_tools import process_data

from .fixtures.utils import set_local_tracker


class TestCleanLogs:
    @pytest.mark.usefixtures("set_local_tracker")
    def test_simple_shop_stream(self) -> None:
        args = {"as_dataframe": False, "add_start_end_events": False}

        stream = load_simple_shop(**args)
        sleep(3)

        logs = process_data(pd.read_csv(os.environ["RETE_TRACKER_CSV_PATH"]))
        log = logs[logs["event_custom_name"] == "load_simple_shop_end"].iloc[0]

        assert log["args"] == args
        assert log["performance_before"] == {}
        assert log["performance_after"] == {}

    def test_empty_logs(self) -> None:
        assert not os.path.exists(os.environ["RETE_TRACKER_CSV_PATH"])
