from __future__ import annotations

from typing import Optional, Tuple

import numpy as np
import pandas as pd
from pandas import DataFrame

from retentioneering.backend.tracker import collect_data_performance, time_performance
from retentioneering.constants import DATETIME_UNITS
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe
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


@docstrings.get_sections(base="LabelCroppedPaths")  # type: ignore
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

    @time_performance(
        scope="label_cropped_paths",
        event_name="init",
    )
    def __init__(self, params: LabelCroppedPathsParams):
        super().__init__(params=params)

    @time_performance(  # type: ignore
        scope="label_cropped_paths",
        event_name="apply",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        user_col = schema.user_id
        time_col = schema.event_timestamp
        type_col = schema.event_type
        event_col = schema.event_name

        left_cutoff, left_truncated_unit = None, None
        right_cutoff, right_truncated_unit = None, None

        if self.params.left_cutoff:
            left_cutoff, left_truncated_unit = self.params.left_cutoff

        if self.params.right_cutoff:
            right_cutoff, right_truncated_unit = self.params.right_cutoff

        labeled_events = pd.DataFrame()

        if not left_cutoff and not right_cutoff:
            raise ValueError("Either left_cutoff or right_cutoff must be specified!")

        userpath = df.groupby(user_col)[time_col].agg(start=np.min, end=np.max)  # type: ignore

        if left_cutoff:
            timedelta = (userpath["end"] - df[time_col].min()) / np.timedelta64(1, left_truncated_unit)  # type: ignore
            cropped_users_index = userpath[timedelta < left_cutoff].index
            left_labeled_events = df.groupby(user_col).first().reindex(cropped_users_index).reset_index()

            left_labeled_events[event_col] = "cropped_left"
            left_labeled_events[type_col] = "cropped_left"
            labeled_events = pd.concat([labeled_events, left_labeled_events])

        if right_cutoff:
            timedelta = (df[time_col].max() - userpath["start"]) / np.timedelta64(
                1, right_truncated_unit  # type: ignore
            )
            cropped_users_index = userpath[timedelta < right_cutoff].index
            right_labeled_events = df.groupby(user_col).last().reindex(cropped_users_index).reset_index()

            right_labeled_events[event_col] = "cropped_right"
            right_labeled_events[type_col] = "cropped_right"
            labeled_events = pd.concat([labeled_events, right_labeled_events])

        result = pd.concat([df, labeled_events])

        collect_data_performance(
            scope="label_cropped_paths",
            event_name="metadata",
            called_params=self.to_dict()["values"],
            not_hash_values=["left_cutoff", "right_cutoff"],
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
