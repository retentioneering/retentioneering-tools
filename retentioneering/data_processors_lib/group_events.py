from inspect import signature
from typing import Any, Callable, Optional

import pandas as pd

from retentioneering.backend.tracker import collect_data_performance, time_performance
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe
from retentioneering.widget.widgets import ReteFunction

EventstreamFilter = Callable[[pd.DataFrame, Optional[EventstreamSchemaType]], Any]


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


@docstrings.get_sections(base="GroupEvents")  # type: ignore
class GroupEvents(DataProcessor):
    """
    Filter the specified events from the input ``eventstream`` and create
    new synthetic events, with names based on the old events' names.

    Parameters
    ----------
    event_name : str
        Name of the created event.
    func : Callable[[DataFrame, Optional[EventstreamSchema]], Any]
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

         - new synthetic events with ``group_alias`` or custom type`

        +-----------------+----------------+-------------------+
        | **event_name**  | **event_type** | **timestamp**     |
        +-----------------+----------------+-------------------+
        | raw_event_name  | raw            | raw_event         |
        +-----------------+----------------+-------------------+
        | new_event_name  | group_alias    | raw_event         |
        +-----------------+----------------+-------------------+

    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.
    """

    params: GroupEventsParams

    @time_performance(
        scope="group_events",
        event_name="init",
    )
    def __init__(self, params: GroupEventsParams) -> None:
        super().__init__(params=params)

    @time_performance(
        scope="group_events",
        event_name="apply",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        event_name = self.params.event_name
        func: Callable = self.params.func
        event_type = self.params.event_type

        parent_info = {
            "shape": df.shape,
            "hash": hash_dataframe(df),
        }

        expected_args_count = len(signature(func).parameters)
        if expected_args_count == 1:
            mask = func(df)  # type: ignore
        else:
            mask = func(df, schema)

        with pd.option_context("mode.chained_assignment", None):
            if event_type is not None:
                df.loc[mask, schema.event_type] = event_type
            df.loc[mask, schema.event_name] = event_name

        collect_data_performance(
            scope="group_events",
            event_name="metadata",
            called_params=self.to_dict()["values"],
            performance_data={
                "parent": parent_info,
                "child": {
                    "shape": df.shape,
                    "hash": hash_dataframe(df),
                },
            },
        )

        return df
