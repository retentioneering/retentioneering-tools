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


class PipeParams(ParamsModel):
    func: Callable[[DataFrame, Optional[EventstreamSchema]], DataFrame]

    _widgets = {
        "func": ReteFunction(),
    }


@docstrings.get_sections(base="Pipe")  # type: ignore
class Pipe(DataProcessor):
    """
    Modify an input eventstream in an arbitrary way by applying given function.
    The function must accept a DataFrame associated with the input eventstream
    and return a new state of the modified eventstream.

    Parameters
    ----------
    func : Callable[[DataFrame], DataFrame]
        A function that is applied to the DataFrame underlying the eventstream.
        Must accept DataFrame as input and return DataFrame as output

    Returns
    -------
    Eventstream
        Resulting eventstream
    """

    params: PipeParams

    @time_performance(
        scope="pipe",
        event_name="init",
    )
    def __init__(self, params: PipeParams):
        super().__init__(params=params)

    @time_performance(
        scope="pipe",
        event_name="apply",
    )
    def apply(self, df: DataFrame, schema: EventstreamSchemaType) -> DataFrame:
        func: Callable[[DataFrame, Optional[EventstreamSchemaType]], DataFrame] = self.params.func  # type: ignore

        expected_args_count = len(signature(func).parameters)
        if expected_args_count == 1:
            result = func(df)  # type: ignore
        else:
            result = func(df, schema)

        collect_data_performance(
            scope="pipe",
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
