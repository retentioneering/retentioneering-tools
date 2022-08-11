from typing import Callable, Any, TypedDict

from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.data_processor.params_model import ParamsModel, Func
from src.eventstream.eventstream import Eventstream

from src.eventstream.schema import EventstreamSchema

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


class DeleteEventsParams(TypedDict):
    filter: EventstreamFilter


class DeleteEvents(DataProcessor[DeleteEventsParams]):
    def __init__(self, params: DeleteEventsParams):
        self.params = ParamsModel(
            fields=params,
            fields_schema={
                "filter": Func()
            }
        )

    def apply(self, eventstream: Eventstream) -> Eventstream:
        filter = self.params.fields["filter"]
        events = eventstream.to_dataframe()
        mathed_events_q = filter(events, eventstream.schema)
        matched_events = events[mathed_events_q].copy()

        matched_events["ref"] = matched_events[eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data=matched_events,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": eventstream}],
        )
        eventstream.soft_delete(eventstream.to_dataframe())
        return eventstream
