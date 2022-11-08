from __future__ import annotations

import pandas as pd

from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema


class TestSankey:
    def test_funnel__open(self):
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:01:00"],
                [1, "event2", "2022-01-01 00:01:02"],
                [1, "event1", "2022-01-01 00:02:00"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event1", "2022-01-01 00:03:00"],
                [1, "event3", "2022-01-01 00:03:30"],
                [1, "event1", "2022-01-01 00:04:00"],
                [1, "event3", "2022-01-01 00:04:30"],
                [1, "event1", "2022-01-01 00:05:00"],
                [1, "path_end", "2022-01-01 00:05:00"],
                [2, "event1", "2022-01-02 00:00:00"],
                [2, "event3", "2022-01-02 00:00:05"],
                [2, "event2", "2022-01-02 00:01:05"],
                [2, "path_end", "2022-01-02 00:01:05"],
                [3, "event1", "2022-01-02 00:01:10"],
                [3, "event3", "2022-01-02 00:02:05"],
                [3, "event4", "2022-01-02 00:03:05"],
                [3, "path_end", "2022-01-02 00:03:05"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        stream = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(event_name="event", event_timestamp="timestamp", user_id="user_id"),
            schema=EventstreamSchema(),
        )
        res_data, res_nodes, res_edges = stream.step_sankey(as_data_graph=True)
