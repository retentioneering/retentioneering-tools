import pandas as pd

from src.data_processors_lib.rete import PositiveTarget, PositiveTargetParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema


class TestPositiveTarget:
    def test_positive_target(self):
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
            raw_data=source_df,
            schema=EventstreamSchema(),
            raw_data_schema=RawDataSchema(
                event_name="event_name", event_timestamp="event_timestamp", user_id="user_id"
            ),
        )

        params = {
            "positive_target_events": ["cart_btn_click"],
        }
        events = PositiveTarget(params=PositiveTargetParams(**params))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)
        events_names = result_df[result.schema.event_name].to_list()
        assert "positive_target_cart_btn_click" in events_names
