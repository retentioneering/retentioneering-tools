from typing import Any, Callable, TypedDict

from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.data_processor.params_model import Func, ParamsModel, String
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema

EventstreamFilter = Callable[[DataFrame, EventstreamSchema], Any]


class SimpleGroupParams(TypedDict, total=True):
    event_name: str
    filter: EventstreamFilter
    event_type: str


class SimpleGroup(DataProcessor[SimpleGroupParams]):
    def __init__(self, params: SimpleGroupParams) -> None:
        self.params = ParamsModel(
            fields=params,
            fields_schema={
                "event_name": String(),
                "filter": Func(),
                "event_type": String(optional=True, default="group_alias"),
            },
        )

    def apply(self, eventstream: Eventstream) -> Eventstream:
        event_name = self.params.fields.get("event_name")
        filter = self.params.fields.get("filter", None)
        event_type = self.params.fields.get("event_type", None)

        events: DataFrame = eventstream.to_dataframe()
        mathed_events_q = filter(events, eventstream.schema)
        matched_events: DataFrame = events[mathed_events_q].copy()  # type: ignore

        if event_type is not None:
            matched_events[eventstream.schema.event_type] = event_type

        matched_events[eventstream.schema.event_name] = event_name
        matched_events["ref"] = matched_events[eventstream.schema.event_id]
        return Eventstream(
            raw_data=matched_events,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": eventstream}],
        )


class DeleteEventsParams(TypedDict):
    filter: EventstreamFilter


class DeleteEvents(DataProcessor[DeleteEventsParams]):
    def __init__(self, params: DeleteEventsParams) -> None:
        self.params = ParamsModel(fields=params, fields_schema={"filter": Func()})

    def apply(self, eventstream: Eventstream) -> Eventstream:
        filter = self.params.fields["filter"]
        events = eventstream.to_dataframe()
        mathed_events_q = filter(events, eventstream.schema)
        matched_events: DataFrame = events[mathed_events_q].copy()  # type: ignore

        matched_events["ref"] = matched_events[eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data=matched_events,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            relations=[{"raw_col": "ref", "evenstream": eventstream}],
        )
        eventstream.soft_delete(eventstream.to_dataframe())
        return eventstream
