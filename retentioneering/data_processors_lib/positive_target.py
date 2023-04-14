from __future__ import annotations

from typing import Callable, List

import pandas as pd

from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamType
from retentioneering.params_model import ParamsModel
from retentioneering.widget.widgets import ListOfString, ReteFunction


def _default_func(eventstream: EventstreamType, targets: list[str]) -> pd.DataFrame:
    """
    Filter rows with target events from the input eventstream.

    Parameters
    ----------
    eventstream : Eventstream
        Source eventstream or output from previous nodes.

    targets : list of str
        Condition for eventstream filtering.
        Each event from that list is associated with a conversion goal of the user behaviour in the product.
        If there are several target events in user path - the event with minimum timestamp is taken.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with positive_events and its timestamps.
    """
    user_col = eventstream.schema.user_id
    time_col = eventstream.schema.event_timestamp
    event_col = eventstream.schema.event_name
    df = eventstream.to_dataframe()

    positive_events_index = df[df[event_col].isin(targets)].groupby(user_col)[time_col].idxmin()  # type: ignore

    return df.loc[positive_events_index]  # type: ignore


class PositiveTargetParams(ParamsModel):
    """
    A class with parameters for :py:class:`.PositiveTarget` class.
    """

    targets: List[str]
    func: Callable = _default_func

    _widgets = {"func": ReteFunction(), "targets": ListOfString()}


class PositiveTarget(DataProcessor):
    """
    Create new synthetic events in paths of all users having the specified events:
    ``positive_target_RAW_EVENT_NAME``

    Parameters
    ----------
    targets : list of str
        Define the list of events we consider positive.
        If there are several target events in a user path, the event with the minimum timestamp is taken.

    func : Callable, default _default_func
        Filter rows with target events from the input eventstream.

    Returns
    -------
    Eventstream
        ``Eventstream`` with new synthetic events only added to users who fit the conditions.

        +--------------------------------+-----------------+-----------------------------+
        | **event_name**                 | **event_type**  | **timestamp**               |
        +--------------------------------+-----------------+-----------------------------+
        | positive_target_RAW_EVENT_NAME | positive_target | min(targets)                |
        +--------------------------------+-----------------+-----------------------------+

    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.
    """

    params: PositiveTargetParams

    def __init__(self, params: PositiveTargetParams):
        super().__init__(params=params)

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from retentioneering.eventstream.eventstream import Eventstream

        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        func: Callable[[EventstreamType, list[str]], pd.DataFrame] = self.params.func
        targets = self.params.targets

        positive_targets = func(eventstream, targets)
        positive_targets[type_col] = "positive_target"
        positive_targets[event_col] = "positive_target_" + positive_targets[event_col]
        positive_targets["ref"] = None

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=positive_targets,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
