from typing import Any, Callable, Optional

import pandas as pd

from retentioneering.backend.tracker import track
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType, EventstreamType
from retentioneering.params_model import ParamsModel
from retentioneering.widget.widgets import ReteFunction

EventstreamFilter = Callable[[pd.DataFrame, EventstreamSchemaType], Any]


class GroupEventsParams(ParamsModel):
    """
    A class with parameters for :py:class:`.GroupEvents` class.
    """

    event_name: str
    func: EventstreamFilter
    event_type: Optional[str] = "group_alias"

    _widgets = {
        "func": ReteFunction(),
    }


class GroupEvents(DataProcessor):
    """
    Filter the specified events from the input ``eventstream`` and create
    new synthetic events, with names based on the old events' names.

    Parameters
    ----------
    event_name : str
        Name of the created event.
    func : Callable[[DataFrame, EventstreamSchema], Any]
        Custom function that returns boolean mask with the same length as input eventstream.

        - If ``True`` - events will be grouped.
        - If ``False`` - events will be remained.
    event_type : str, default "group_alias"
        Event_type name for the grouped events.
        If custom event_type is created, it should be added to the ``DEFAULT_INDEX_ORDER``.

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

    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.
    """

    params: GroupEventsParams

    @track(  # type: ignore
        tracking_info={"event_name": "init"},
        scope="group_events",
        allowed_params=[],
    )
    def __init__(self, params: GroupEventsParams) -> None:
        super().__init__(params=params)

    @track(  # type: ignore
        tracking_info={"event_name": "apply"},
        scope="group_events",
        allowed_params=[],
    )
    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from retentioneering.eventstream.eventstream import Eventstream

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
