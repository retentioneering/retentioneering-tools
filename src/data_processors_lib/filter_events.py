from typing import Callable

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.schema import EventstreamSchema
from src.eventstream.types import EventstreamSchemaType, EventstreamType
from src.params_model import ParamsModel
from src.widget.widgets import ReteFunction


class FilterEventsParams(ParamsModel):
    """
    Class with parameters for class :py:func:`FilterEvents`

    """

    func: Callable[[DataFrame, EventstreamSchema], bool]

    _widgets = {
        "filter": ReteFunction,
    }


class FilterEvents(DataProcessor):
    """
    Filters input ``eventstream`` on the basis of custom conditions.

    Parameters
    ----------
    func : Callable[[DataFrame, EventstreamSchema], bool]
        Custom function which returns boolean mask the same length as input ``eventstream``.

        - If ``True`` - row will be remained
        - If ``False`` - row will be deleted

    Returns
    -------
    Eventstream
        ``Eventstream`` with events that should be deleted from input ``eventstream``.


    """

    params: FilterEventsParams

    def __init__(self, params: FilterEventsParams):
        super().__init__(params=params)

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from src.eventstream.eventstream import Eventstream

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
