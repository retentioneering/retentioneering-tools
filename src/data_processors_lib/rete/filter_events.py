from typing import Callable

import pandas as pd
from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.eventstream import Eventstream
from src.eventstream.schema import EventstreamSchema
from src.params_model import ParamsModel


class FilterEventsParams(ParamsModel):
    """
    Class with parameters for class :py:func:`FilterEvents`

    """

    filter: Callable[[DataFrame, EventstreamSchema], bool]


class FilterEvents(DataProcessor):
    """
    Filters input Eventstream on the basis of custom conditions

    Parameters
    ----------
    filter : Callable[[DataFrame, EventstreamSchema], bool]
        Custom function which returns boolean mask the same length as input Eventstream
        If ``True`` - row will be remained
        If ``False`` - row will be deleted

    Returns
    -------
    Eventstream
        Eventstream with events that should be deleted from input Eventstream.

    """

    params: FilterEventsParams

    def __init__(self, params: FilterEventsParams):
        super().__init__(params=params)

    def apply(self, eventstream: Eventstream) -> Eventstream:
        filter_: Callable[[DataFrame, EventstreamSchema], bool] = self.params.filter  # type: ignore
        events: pd.DataFrame = eventstream.to_dataframe()
        mask = filter_(events, eventstream.schema)
        events_to_delete = events[~mask]

        with pd.option_context("mode.chained_assignment", None):
            events_to_delete["ref"] = events_to_delete[eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=events_to_delete,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        if not events_to_delete.empty:
            eventstream.soft_delete(events=eventstream.to_dataframe())

        return eventstream
