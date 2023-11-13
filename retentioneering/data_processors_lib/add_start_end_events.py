from __future__ import annotations

import pandas as pd
from pandas import DataFrame

from retentioneering.backend.tracker import collect_data_performance, time_performance
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe


class AddStartEndEventsParams(ParamsModel):
    pass


@docstrings.get_sections(base="AddStartEndEvents")  # type: ignore
class AddStartEndEvents(DataProcessor):
    """
    Create two synthetic events in each user's path: ``path_start`` and ``path_end``.
    If ``path_start`` or ``path_end`` already exists in a path, a new one is not added.

    Returns
    -------
    Eventstream
        ``Eventstream`` with new synthetic events only - two for each user:

        +----------------+----------------+----------------+
        | **event_name** | **event_type** | **timestamp**  |
        +----------------+----------------+----------------+
        | path_start     | path_start     | first_event    |
        +----------------+----------------+----------------+
        | path_end       | path_end       | last_event     |
        +----------------+----------------+----------------+

    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.
    """

    params: AddStartEndEventsParams

    @time_performance(scope="add_start_end_events", event_name="init")
    def __init__(self, params: AddStartEndEventsParams) -> None:
        super().__init__(params=params)

    @time_performance(
        scope="add_start_end_events",
        event_name="apply",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        user_col = schema.user_id
        type_col = schema.event_type
        event_col = schema.event_name

        matched_events_start: DataFrame = df.groupby(user_col, as_index=False).first()  # type: ignore
        matched_events_start = matched_events_start[matched_events_start[type_col] != "path_start"]
        matched_events_start[type_col] = "path_start"
        matched_events_start[event_col] = "path_start"

        matched_events_end: DataFrame = df.groupby(user_col, as_index=False).last()  # type: ignore
        matched_events_end = matched_events_end[matched_events_end[type_col] != "path_end"]
        matched_events_end[type_col] = "path_end"
        matched_events_end[event_col] = "path_end"

        matched_events = pd.concat([matched_events_start, matched_events_end])

        result = pd.concat([df, matched_events])
        collect_data_performance(
            scope="add_start_end_events",
            event_name="metadata",
            called_params={},
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
