from __future__ import annotations

from typing import Optional, Tuple

import numpy as np
import pandas as pd
from pandas import DataFrame

from retentioneering.backend.tracker import track
from retentioneering.constants import DATETIME_UNITS
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamType
from retentioneering.params_model import ParamsModel
from retentioneering.widget.widgets import ReteTimeWidget


class LabelCroppedPathsParams(ParamsModel):
    """
    A class with parameters for :py:class:`.LabelCroppedPaths` class.
    """

    left_cutoff: Optional[Tuple[float, DATETIME_UNITS]]
    right_cutoff: Optional[Tuple[float, DATETIME_UNITS]]

    _widgets = {
        "left_cutoff": ReteTimeWidget(),
        "right_cutoff": ReteTimeWidget(),
    }


class LabelCroppedPaths(DataProcessor):
    """
    Create new synthetic event(s) for each user based on the timeout threshold:
    ``cropped_left`` or ``cropped_right``

    Parameters
    ----------
    left_cutoff : Tuple(float, :numpy_link:`DATETIME_UNITS<>`), optional
        Threshold value with its unit of measure.
        The timedelta between the last event in each user's path and the first event in the whole eventstream
        is calculated. For users with the timedelta less than the selected ``left_cutoff``,
        the new ``cropped_left`` event is added.

    right_cutoff : Tuple(float, :numpy_link:`DATETIME_UNITS<>`), optional
        Threshold value with its unit of measure.
        The timedelta between the first event in each user's path and the last event in the whole eventstream
        is calculated. For users with timedelta less than the selected ``right_cutoff``,
        the new ``cropped_right`` event is added.

    Returns
    -------
    Eventstream
        Eventstream containing only the generated synthetic events, for users whose paths
        satisfy the specified cut-offs.

        +-------------------+-------------------+------------------+
        | **event_name**    | **event_type**    |  **timestamp**   |
        +-------------------+-------------------+------------------+
        | cropped_left      | cropped_left      |  first_event     |
        +-------------------+-------------------+------------------+
        | cropped_right     | cropped_right     |  last_event      |
        +-------------------+-------------------+------------------+

    See Also
    --------
    .TimedeltaHist : Plot the distribution of the time deltas between two events.
    .UserLifetimeHist : Plot the distribution of user lifetimes.

    Raises
    ------
    ValueError
        If both of ``left_cutoff`` and ``right_cutoff`` are empty.

    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.
    """

    params: LabelCroppedPathsParams

    @track(  # type: ignore
        tracking_info={"event_name": "init"},
        scope="label_cropped_paths",
        allowed_params=[],
    )
    def __init__(self, params: LabelCroppedPathsParams):
        super().__init__(params=params)

    @track(  # type: ignore
        tracking_info={"event_name": "apply"},
        scope="label_cropped_paths",
        allowed_params=[],
    )
    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from retentioneering.eventstream.eventstream import Eventstream

        events: DataFrame = eventstream.to_dataframe(copy=True)
        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name

        left_cutoff, left_truncated_unit = None, None
        right_cutoff, right_truncated_unit = None, None

        if self.params.left_cutoff:
            left_cutoff, left_truncated_unit = self.params.left_cutoff

        if self.params.right_cutoff:
            right_cutoff, right_truncated_unit = self.params.right_cutoff
        labeled_events = pd.DataFrame()

        if not left_cutoff and not right_cutoff:
            raise ValueError("Either left_cutoff or right_cutoff must be specified!")

        userpath = (
            events.groupby(user_col)[time_col].agg([np.min, np.max]).rename(columns={"amin": "start", "amax": "end"})
        )

        if left_cutoff:
            timedelta = (userpath["end"] - events[time_col].min()) / np.timedelta64(
                1, left_truncated_unit  # type: ignore
            )
            cropped_users_index = userpath[timedelta < left_cutoff].index
            left_labeled_events = events.groupby(user_col).first().reindex(cropped_users_index).reset_index()

            left_labeled_events[event_col] = "cropped_left"
            left_labeled_events[type_col] = "cropped_left"
            left_labeled_events["ref"] = None
            labeled_events = pd.concat([labeled_events, left_labeled_events])

        if right_cutoff:
            timedelta = (events[time_col].max() - userpath["start"]) / np.timedelta64(
                1, right_truncated_unit  # type: ignore
            )
            cropped_users_index = userpath[timedelta < right_cutoff].index
            right_labeled_events = events.groupby(user_col).last().reindex(cropped_users_index).reset_index()

            right_labeled_events[event_col] = "cropped_right"
            right_labeled_events[type_col] = "cropped_right"
            right_labeled_events["ref"] = None
            labeled_events = pd.concat([labeled_events, right_labeled_events])

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=labeled_events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
