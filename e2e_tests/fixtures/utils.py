from __future__ import annotations

import os
from typing import Iterable

import pandas as pd
import pytest
from _pytest.fixtures import FixtureRequest

from retentioneering.backend import counter


@pytest.fixture(scope="function")
def set_local_tracker(request: FixtureRequest) -> Iterable:
    columns = [
        "user_id",
        "event_custom_name",
        "event_name",
        "event_value",
        "params",
        "scope",
        "event_time",
        "jupyter_kernel_id",
        "colab",
        "event_date_local",
        "event_day_week",
        "event_timestamp",
        "event_timestamp_ms",
        "source",
        "version",
        "os",
        "index",
        "browser",
        "eventstream_index",
        "parent_eventstream_index",
        "child_eventstream_index",
        "account_id",
    ]
    with open(os.environ["RETE_TRACKER_CSV_PATH"], "w") as log_file:
        # get path to f
        os.environ["RETE_TRACKER_ENABLED"] = "true"

        pd.DataFrame(columns=columns).to_csv(log_file, index=False)

    yield
    os.environ["RETE_TRACKER_ENABLED"] = "false"
    os.remove(os.environ["RETE_TRACKER_CSV_PATH"])
    counter.reload()
