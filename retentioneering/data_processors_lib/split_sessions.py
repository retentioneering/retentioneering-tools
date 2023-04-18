from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd

from retentioneering.constants import DATETIME_UNITS
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.eventstream.types import EventstreamType
from retentioneering.params_model import ParamsModel
from retentioneering.widget.widgets import ReteTimeWidget


class SplitSessionsParams(ParamsModel):
    """
    A class with parameters for :py:class:`.SplitSessions` class.

    """

    timeout: Tuple[float, DATETIME_UNITS]
    mark_truncated: bool = False
    session_col: str = "session_id"

    _widgets = {"timeout": ReteTimeWidget()}


class SplitSessions(DataProcessor):
    """
    Create new synthetic events, that divide users' paths on sessions:
    ``session_start`` (or ``session_start_cropped``) and ``session_end`` (or ``session_end_cropped``).
    Also create a new column that contains session number for each event in input eventstream.
    Session number will take the form: ``{user_id}_{session_number through one user path}``.

    Parameters
    ----------
    timeout : Tuple(float, :numpy_link:`DATETIME_UNITS<>`)
        Threshold value and its unit of measure.
        ``session_start`` and ``session_end`` events are always placed before the first and after the last event
        in each user's path.
        Because user can have more than one session, it calculates timedelta between every two consecutive events in
        each user's path.
        If the calculated timedelta is more than selected timeout,
        new synthetic events - ``session_start`` and ``session_end`` are created inside the user path,
        marking session starting and ending points.

    mark_truncated : bool, default False
        If ``True`` - calculates timedelta between:

        - first event in each user's path and first event in the whole eventstream.
        - last event in each user's path and last event in the whole eventstream.

        For users with timedelta less than selected ``timeout``,
        a new synthetic event - ``session_start_cropped`` or ``session_end_cropped`` will be added.

    session_col : str, default "session_id"
        The name of the ``session_col``.

    Returns
    -------
    Eventstream
        ``Eventstream`` with new synthetic events and ``session_col``.

        +-----------------------------+----------------------------+-----------------+
        | **event_name**              | **event_type**             | **timestamp**   |
        +-----------------------------+----------------------------+-----------------+
        | session_start               | session_start              | first_event     |
        +-----------------------------+----------------------------+-----------------+
        | session_end                 | session_end                | last_event      |
        +-----------------------------+----------------------------+-----------------+
        | session_start_cropped       | session_start_cropped      | first_event     |
        +-----------------------------+----------------------------+-----------------+
        | session_end_cropped         | session_end_cropped        | last_event      |
        +-----------------------------+----------------------------+-----------------+

        If the delta between timestamps of two consecutive events
        (raw_event_n and raw_event_n+1) is greater than the selected ``timeout``
        the user will have more than one session:

        +--------------+-------------------+------------------+-------------------+------------------+
        |  **user_id** | **event_name**    | **event_type**   | **timestamp**     | **session_col**  |
        +--------------+-------------------+------------------+-------------------+------------------+
        |     1        | session_start     | session_start    | first_event       |     1_0          |
        +--------------+-------------------+------------------+-------------------+------------------+
        |     1        | session_end       | session_end      | raw_event_n       |     1_0          |
        +--------------+-------------------+------------------+-------------------+------------------+
        |     1        | session_start     | session_start    | raw_event_n+1     |     1_1          |
        +--------------+-------------------+------------------+-------------------+------------------+
        |     1        | session_end       | session_end      | last_event        |     1_1          |
        +--------------+-------------------+------------------+-------------------+------------------+

    See Also
    --------
    .TimedeltaHist : Plot the distribution of the time deltas between two events.
    .Eventstream.describe : Show general eventstream statistics.
    .Eventstream.describe_events : Show general eventstream events statistics.


    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.

    """

    params: SplitSessionsParams

    def __init__(self, params: SplitSessionsParams) -> None:
        super().__init__(params=params)

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from retentioneering.eventstream.eventstream import Eventstream

        user_col = eventstream.schema.user_id
        time_col = eventstream.schema.event_timestamp
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name
        session_col = self.params.session_col
        timeout, timeout_unit = self.params.timeout
        mark_truncated = self.params.mark_truncated

        df = eventstream.to_dataframe(copy=True)
        df["ref"] = df[eventstream.schema.event_id]

        df["prev_timedelta"] = df[time_col] - df.groupby(user_col)[time_col].shift(1)
        df["next_timedelta"] = df.groupby(user_col)[time_col].shift(-1) - df[time_col]
        df["prev_timedelta"] /= np.timedelta64(1, timeout_unit)  # type: ignore
        df["next_timedelta"] /= np.timedelta64(1, timeout_unit)  # type: ignore

        session_starts_mask = (df["prev_timedelta"] > timeout) | (df["prev_timedelta"].isnull())
        session_ends_mask = (df["next_timedelta"] > timeout) | (df["next_timedelta"].isnull())

        df["is_session_start"] = session_starts_mask
        df[session_col] = df.groupby(user_col)["is_session_start"].transform(np.cumsum)
        df[session_col] = df[user_col].astype(str) + "_" + df[session_col].astype(str)

        session_starts = df[session_starts_mask].copy()
        session_ends = df[session_ends_mask].copy()

        session_starts[event_col] = "session_start"
        session_starts[type_col] = "session_start"
        session_starts["ref"] = None

        session_ends[event_col] = "session_end"
        session_ends[type_col] = "session_end"
        session_ends["ref"] = None

        df = df.drop(["prev_timedelta", "next_timedelta", "is_session_start"], axis=1)

        if mark_truncated:
            dataset_start = df[time_col].min()
            dataset_end = df[time_col].max()
            start_to_start = (session_starts[time_col] - dataset_start) / np.timedelta64(1, timeout_unit)
            end_to_end = (dataset_end - session_ends[time_col]) / np.timedelta64(1, timeout_unit)

            session_starts_truncated = session_starts[start_to_start < timeout].reset_index()
            session_ends_truncated = session_ends[end_to_end < timeout].reset_index()

            session_starts_truncated[event_col] = "session_start_cropped"
            session_starts_truncated[type_col] = "session_start_cropped"

            session_ends_truncated[event_col] = "session_end_cropped"
            session_ends_truncated[type_col] = "session_end_cropped"

            session_starts = pd.concat([session_starts, session_starts_truncated])
            session_ends = pd.concat([session_ends, session_ends_truncated])

        df = pd.concat([df, session_starts, session_ends])

        raw_data_schema = eventstream.schema.to_raw_data_schema()
        raw_data_schema.custom_cols.append({"custom_col": session_col, "raw_data_col": session_col})

        eventstream = Eventstream(
            schema=EventstreamSchema(custom_cols=[session_col]),
            raw_data_schema=raw_data_schema,
            raw_data=df,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )

        return eventstream
