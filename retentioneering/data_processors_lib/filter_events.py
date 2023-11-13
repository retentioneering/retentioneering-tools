from inspect import signature
from typing import Callable, Optional

import pandas as pd
from pandas import DataFrame, Series

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe
from retentioneering.widget.widgets import ReteFunction


class FilterEventsParams(ParamsModel):
    """
    A class with parameters for :py:class:`.FilterEvents` class.

    """

    func: Callable[[DataFrame, Optional[EventstreamSchema]], Series]

    _widgets = {
        "func": ReteFunction(),
    }


@docstrings.get_sections(base="FilterEvents")  # type: ignore
class FilterEvents(DataProcessor):
    """
    Filters input ``eventstream`` on the basis of custom conditions.

    Parameters
    ----------
    func : Callable[[DataFrame, Optional[EventstreamSchema]], bool]
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

    @time_performance(
        scope="filter_events",
        event_name="init",
    )
    def __init__(self, params: FilterEventsParams):
        super().__init__(params=params)

    @time_performance(
        scope="filter_events",
        event_name="apply",
    )
    def apply(self, df: DataFrame, schema: EventstreamSchemaType) -> DataFrame:
        func: Callable[[DataFrame, Optional[EventstreamSchemaType]], Series] = self.params.func  # type: ignore

        expected_args_count = len(signature(func).parameters)
        if expected_args_count == 1:
            mask = func(df)  # type: ignore
        else:
            mask = func(df, schema)

        result = df[mask]

        collect_data_performance(
            scope="filter_events",
            event_name="metadata",
            called_params=self.to_dict()["values"],
            performance_data={
                "parent": {
                    "shape": df.shape,
                    "hash": hash_dataframe(df),
                },
                "child": {
                    "shape": result.shape,
                    "hash": hash_dataframe(result),
                },
            },
        )

        return result
