from typing import Callable

import pandas as pd
from pandas import DataFrame

from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.eventstream.types import EventstreamSchemaType, EventstreamType
from retentioneering.params_model import ParamsModel
from retentioneering.widget.widgets import ReteFunction


class FilterEventsParams(ParamsModel):
    """
    A class with parameters for :py:class:`.FilterEvents` class.

    """

    func: Callable[[DataFrame, EventstreamSchema], bool]

    _widgets = {
        "func": ReteFunction(),
    }


class FilterEvents(DataProcessor):
    """
    Filters input ``eventstream`` on the basis of custom conditions.

    Parameters
    ----------
    func : Callable[[DataFrame, EventstreamSchema], bool]
        Custom function that returns boolean mask the same length as input ``eventstream``.

        - If ``True`` - the row will be left in the eventstream.
        - If ``False`` - the row will be deleted from the eventstream.

    Returns
    -------
    Eventstream
        ``Eventstream`` with events that should be deleted from input ``eventstream``.

    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.

    """

    params: FilterEventsParams

    def __init__(self, params: FilterEventsParams):
        super().__init__(params=params)

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from retentioneering.eventstream.eventstream import Eventstream

        func: Callable[[DataFrame, EventstreamSchemaType], bool] = self.params.func  # type: ignore
        events: pd.DataFrame = eventstream.to_dataframe()
        mask = func(events, eventstream.schema)
        events_to_delete = events[~mask]

        with pd.option_context("mode.chained_assignment", None):
            events_to_delete["ref"] = events_to_delete[eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=events_to_delete,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        if not events_to_delete.empty:
            eventstream._soft_delete(events=eventstream.to_dataframe())

        return eventstream
