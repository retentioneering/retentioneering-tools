from typing import Any, Callable, Optional

import pandas as pd

from src.data_processor.data_processor import DataProcessor
from src.eventstream.types import EventstreamSchemaType, EventstreamType
from src.params_model import ParamsModel

EventstreamFilter = Callable[[pd.DataFrame, EventstreamSchemaType], Any]


class GroupEventsParams(ParamsModel):
    """
    Class with parameters for class :py:func:`GroupEvents`
    """

    event_name: str
    func: EventstreamFilter
    event_type: Optional[str] = "group_alias"


class GroupEvents(DataProcessor):
    """
    Filters specified events from input ``eventstream`` and creates on their basis
    new synthetic events with new name.

    Parameters
    ----------
    event_name : str
        Name of new, grouped event
    func : Callable[[DataFrame, EventstreamSchema], Any]
        Custom function which returns boolean mask the same length as input Eventstream
        If ``True`` - events, that will be grouped
        If ``False`` - events, that will be remained
    event_type : str, default="group_alias"
        Event_type name for the grouped events
        If custom event_type is created - it is important to add it to the ``DEFAULT_INDEX_ORDER``

    Returns
    -------
    Eventstream
        ``Eventstream`` with:

         - new synthetic events with ``group_alias`` or custom type
         - raw events marked ``_deleted=True``

        +-----------------+----------------+-------------------+----------------+
        | **event_name**  | **event_type** | **timestamp**     |  **_deleted**  |
        +-----------------+----------------+-------------------+----------------+
        | raw_event_name  | raw            | raw_event         |  True          |
        +-----------------+----------------+-------------------+----------------+
        | new_event_name  | group_alias    | raw_event         |  False         |
        +-----------------+----------------+-------------------+----------------+

    """

    params: GroupEventsParams

    def __init__(self, params: GroupEventsParams) -> None:
        super().__init__(params=params)

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from src.eventstream.eventstream import Eventstream

        event_name = self.params.event_name
        func: Callable = self.params.func
        event_type = self.params.event_type

        events = eventstream.to_dataframe()
        mask = func(events, eventstream.schema)
        matched_events = events[mask]

        with pd.option_context("mode.chained_assignment", None):
            if event_type is not None:
                matched_events[eventstream.schema.event_type] = event_type

            matched_events[eventstream.schema.event_name] = event_name
            matched_events["ref"] = matched_events[eventstream.schema.event_id]

        return Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=matched_events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
