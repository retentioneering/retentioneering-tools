import pandas as pd

from src.data_processors_lib.rete import CutPathBeforeEvent, CutPathBeforeEventParams
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema


class TestCutPathBefore:
    def test_cut_path_before(self):
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
            "cutoff_events": ["pageview"],
            "cut_shift": 1,
            "min_cjm": 8,
        }
        events = CutPathBeforeEvent(params=CutPathBeforeEventParams(**params))

        result = events.apply(source)
        result_df = result.to_dataframe(show_deleted=True)
        events_names = result_df[result.schema.event_name].to_list()
        # @TODO: add correct test after example of needed work. Vladimir Makhanov
        assert True
