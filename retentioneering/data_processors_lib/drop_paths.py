from __future__ import annotations

from typing import Optional, Tuple

import numpy as np
import pandas as pd

from retentioneering.backend.tracker import collect_data_performance, time_performance
from retentioneering.constants import DATETIME_UNITS
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe
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


@docstrings.get_sections(base="DropPaths")  # type: ignore
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

    @time_performance(
        scope="drop_paths",
        event_name="init",
    )
    def __init__(self, params: DropPathsParams):
        super().__init__(params=params)

    @time_performance(
        scope="drop_paths",
        event_name="apply",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        user_col = schema.user_id
        time_col = schema.event_timestamp

        min_time, time_unit = None, None
        min_steps = self.params.min_steps

        if self.params.min_time:
            min_time, time_unit = self.params.min_time

        if min_steps and min_time:
            raise ValueError("min_steps and min_time parameters cannot be used simultaneously!")

        if not min_steps and not min_time:
            raise ValueError("Either min_steps or min_time must be specified!")

        if min_time and time_unit:
            userpath = (
                df.groupby(user_col)[time_col].agg([np.min, np.max]).rename(columns={"amin": "start", "amax": "end"})
            )
            mask_ = (userpath["end"] - userpath["start"]) / np.timedelta64(1, time_unit) < min_time  # type: ignore

        else:
            userpath = df.groupby([user_col])[[time_col]].nunique().rename(columns={time_col: "length"})
            mask_ = userpath["length"] < min_steps

        users_to_delete = userpath[mask_].index
        result = df[~df[user_col].isin(users_to_delete)]

        collect_data_performance(
            scope="drop_paths",
            event_name="metadata",
            called_params=self.to_dict()["values"],
            not_hash_values=["min_time"],
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
