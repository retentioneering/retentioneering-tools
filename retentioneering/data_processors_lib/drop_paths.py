from __future__ import annotations

from typing import Optional, Tuple

import numpy as np

from retentioneering.backend.tracker import track
from retentioneering.constants import DATETIME_UNITS
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamType
from retentioneering.params_model import ParamsModel
from retentioneering.widget.widgets import ReteTimeWidget


class DropPathsParams(ParamsModel):
    """
    A class with parameters for :py:class:`.DropPaths` class.
    """

    min_steps: Optional[int]
    min_time: Optional[Tuple[float, DATETIME_UNITS]]

    _widgets = {
        "min_time": ReteTimeWidget(),
    }


class DropPaths(DataProcessor):
    """
    Filter user paths based on the path length, removing the paths that are shorter than the
    specified number of events or time.

    Parameters
    ----------
    min_steps : int, optional
        Minimum user path length measured in the number of events.

    min_time : Tuple(float, :numpy_link:`DATETIME_UNITS<>`), optional
        Minimum user path length and its unit of measure.

    Returns
    -------
    Eventstream
        ``Eventstream`` with events that should be deleted from input ``eventstream`` marked ``_deleted=True``.

    Raises
    ------
    ValueError
        If both of ``min_steps`` and ``min_time`` are empty or both are given.

    See Also
    --------
    .TimedeltaHist : Plot the distribution of the time deltas between two events.
    .UserLifetimeHist : Plot the distribution of user lifetimes.

    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.
    """

    params: DropPathsParams

    @track(  # type: ignore
        tracking_info={"event_name": "init"},
        scope="drop_paths",
        allowed_params=[],
    )
    def __init__(self, params: DropPathsParams):
        super().__init__(params=params)

    @track(  # type: ignore
        tracking_info={"event_name": "apply"},
        scope="drop_paths",
        allowed_params=[],
    )
    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from retentioneering.eventstream.eventstream import Eventstream

        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp

        min_time, time_unit = None, None
        min_steps = self.params.min_steps

        if self.params.min_time:
            min_time, time_unit = self.params.min_time

        if min_steps and min_time:
            raise ValueError("min_steps and min_time parameters cannot be used simultaneously!")

        if not min_steps and not min_time:
            raise ValueError("Either min_steps or min_time must be specified!")

        events = eventstream.to_dataframe(copy=True)

        if min_time and time_unit:
            userpath = (
                events.groupby(user_col)[time_col]
                .agg([np.min, np.max])
                .rename(columns={"amin": "start", "amax": "end"})
            )
            mask_ = (userpath["end"] - userpath["start"]) / np.timedelta64(1, time_unit) < min_time  # type: ignore

        else:
            userpath = events.groupby([user_col])[[time_col]].nunique().rename(columns={time_col: "length"})
            mask_ = userpath["length"] < min_steps

        users_to_delete = userpath[mask_].index
        events = events[events[user_col].isin(users_to_delete)]
        events["ref"] = events.loc[:, eventstream.schema.event_id]

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )

        if not events.empty:
            eventstream._soft_delete(eventstream.to_dataframe())

        return eventstream
