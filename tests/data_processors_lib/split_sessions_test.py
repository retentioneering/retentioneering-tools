from __future__ import annotations

import pandas as pd

from src.data_processors_lib.rete import SplitSessions, SplitSessionsParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema


class TestSplitSessions:
    def test_split_session(self):
        source_df = pd.DataFrame(
            [
                {"event_name": "pageview", "event_type": "raw", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {
                    "event_name": "cart_btn_click",
                    "event_type": "raw",
                    "event_timestamp": "2021-10-26 12:02",
                    "user_id": "1",
                },
                {"event_name": "pageview", "event_type": "raw", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {
                    "event_name": "plus_icon_click",
                    "event_type": "raw",
                    "event_timestamp": "2021-10-26 12:04",
                    "user_id": "1",
                },
            ]
        )

        source = Eventstream(
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
            raw_data=source_df,
            schema=EventstreamSchema(),
        )

        events = SplitSessions(
            params=SplitSessionsParams(
                session_cutoff=(100, "s"),
                mark_truncated=True,
                session_col="session_id",
            )
        )

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)
        events_names: list[str] = result_df[result.schema.event_name].to_list()
        # @TODO: add correct test after example of needed work. Vladimir Makhanov
        assert True
