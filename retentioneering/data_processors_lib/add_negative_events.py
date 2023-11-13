from __future__ import annotations

from typing import Any, Callable, List

import pandas as pd

from retentioneering.backend.tracker import collect_data_performance, time_performance
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.eventstream.types import EventstreamSchemaType, EventstreamType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe
from retentioneering.widget.widgets import ListOfString, ReteFunction

EventstreamFilter = Callable[[pd.DataFrame, EventstreamSchema], Any]


def _default_func(eventstream: EventstreamType, targets: List[str]) -> pd.DataFrame:
    """
    Filter rows with target events from the input eventstream.

    Parameters
    ----------
    eventstream : Eventstream
        Source eventstream or output from previous nodes.

    targets : list of str
        Each event from that list is associated with the bad result (scenario)
        of user's behaviour (experience) in the product.
        If there are several target events in user path - the event with minimum timestamp is taken.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with targets and its timestamps.
    """
    user_col = eventstream.schema.user_id
    time_col = eventstream.schema.event_timestamp
    event_col = eventstream.schema.event_name
    df = eventstream.to_dataframe()

    targets_index = df[df[event_col].isin(targets)].groupby(user_col)[time_col].idxmin()  # type: ignore

    return df.loc[targets_index]  # type: ignore


class AddNegativeEventsParams(ParamsModel):
    """
    A class with parameters for :py:class:`.AddNegativeEvents` class.
    """

    targets: List[str]
    # @TODO: remove eventstream from the "func" signature in a future major release. Aleksei Avramenko
    func: Callable = _default_func

    _widgets = {"func": ReteFunction(), "targets": ListOfString()}


@docstrings.get_sections(base="AddNegativeEvents")  # type: ignore
class AddNegativeEvents(DataProcessor):
    """
    Create new synthetic events in paths of all users having the specified event(s):
    ``negative_target_RAW_EVENT_NAME``.

    Parameters
    ----------
    targets : list of str
        Define the list of events that we consider negative.
        If there are several target events in the user path, the event with the minimum timestamp is taken.
    func : Callable, default _default_func_negative
        Filter rows with target events from the input eventstream.

    Returns
    -------
    Eventstream
        ``Eventstream`` with new synthetic events only added to the users who fit the conditions.

        +--------------------------------+-----------------+-----------------------------+
        | **event_name**                 | **event_type**  | **timestamp**               |
        +--------------------------------+-----------------+-----------------------------+
        | negative_target_RAW_EVENT_NAME | negative_target | min(targets)                |
        +--------------------------------+-----------------+-----------------------------+

    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.


    """

    params: AddNegativeEventsParams

    @time_performance(
        scope="add_negative_events",
        event_name="init",
    )
    def __init__(self, params: AddNegativeEventsParams):
        super().__init__(params=params)

    @time_performance(
        scope="add_negative_events",
        event_name="apply",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        from retentioneering.eventstream.eventstream import Eventstream

        type_col = schema.event_type
        event_col = schema.event_name

        func = self.params.func
        targets = self.params.targets

        eventstream = Eventstream(
            raw_data_schema=schema.to_raw_data_schema(event_index=True),
            raw_data=df,
            add_start_end_events=False,
        )

        # @TODO: remove eventstream from the "func" signature in a future major release. Aleksei Avramenko
        negative_targets: pd.DataFrame = func(eventstream, targets)
        negative_targets[type_col] = "negative_target"
        negative_targets[event_col] = "negative_target_" + negative_targets[event_col]

        result = pd.concat([eventstream.to_dataframe(), negative_targets])

        collect_data_performance(
            scope="add_negative_events",
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
