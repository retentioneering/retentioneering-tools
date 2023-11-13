from __future__ import annotations

import uuid
import warnings
from typing import Final, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from retentioneering.backend.tracker import collect_data_performance, time_performance
from retentioneering.constants import DATETIME_UNITS
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe
from retentioneering.widget.widgets import ListOfString, ReteTimeWidget


class SplitSessionsParams(ParamsModel):
    """
    A class with parameters for :py:class:`.SplitSessions` class.

    """

    timeout: Optional[Tuple[float, DATETIME_UNITS]]
    delimiter_events: Optional[List[str]]
    delimiter_col: Optional[str]
    mark_truncated: bool = False
    session_col: str = "session_id"

    _widgets = {"timeout": ReteTimeWidget(), "delimiter_events": ListOfString()}


@docstrings.get_sections(base="SplitSessions")  # type: ignore
class SplitSessions(DataProcessor):
    """
    Create new synthetic events, that divide users' paths on sessions:
    ``session_start`` (or ``session_start_cropped``) and ``session_end`` (or ``session_end_cropped``).
    Also create a new column that contains session number for each event in input eventstream.
    Session number will take the form: ``{user_id}_{session_number through one user path}``.

    Parameters
    ----------
    timeout : Tuple(float, :numpy_link:`DATETIME_UNITS<>`), optional
        Threshold value and its unit of measure.
        ``session_start`` and ``session_end`` events are always placed before the first and after the last event
        in each user's path.
        Because user can have more than one session, it calculates timedelta between every two consecutive events in
        each user's path.
        If the calculated timedelta is more than selected timeout,
        new synthetic events - ``session_start`` and ``session_end`` are created inside the user path,
        marking session starting and ending points.
    delimiter_events : list of str, optional
        Delimiters define special events in the eventstream that indicate the start and the end of a session.

        - If a single delimiter is defined, it is associated with the session start and the end simultaneously.
          Delimiting events will be replaced with "session_start" event.
        - If a list of two delimiters is defined, the first and second events are associated with session start
          and end correspondingly. Delimiting events will be replaced with "session_start" and "session_end" events.

    delimiter_col : list, optional
        Determines a column that already contains custom session identifiers.

    mark_truncated : bool, default False
        Works with ``timeout`` argument only. If ``True`` - calculates timedelta between:

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

    Examples
    --------
    Splitting with a single delimiting event.

    .. code-block:: python

        df = pd.DataFrame(
            [
                [111, "session_delimiter", "2023-01-01 00:00:00"],
                [111, "A", "2023-01-01 00:00:01"],
                [111, "B", "2023-01-01 00:00:02"],
                [111, "session_delimiter", "2023-01-01 00:00:04"],
                [111, "C", "2023-01-01 00:00:04"],
            ],
            columns=["user_id", "event", "timestamp"]
        )
        Eventstream(df)\\
            .split_sessions(delimiter_events=["session_delimiter"])\\
            .to_dataframe()\\
            .sort_values(["user_id", "event_index"])\\
            [["user_id", "event", "timestamp", "session_id"]]

           user_id          event           timestamp session_id
        0      111  session_start 2023-01-01 00:00:00      111_1
        1      111              A 2023-01-01 00:00:01      111_1
        2      111              B 2023-01-01 00:00:02      111_1
        3      111    session_end 2023-01-01 00:00:02      111_1
        4      111  session_start 2023-01-01 00:00:04      111_2
        5      111              C 2023-01-01 00:00:04      111_2
        6      111    session_end 2023-01-01 00:00:04      111_2

    Splitting with a couple of delimiters indicating session start and session end.

    .. code-block:: python

        df = pd.DataFrame(
            [
                [111, "custom_start", "2023-01-01 00:00:00"],
                [111, "A", "2023-01-01 00:00:01"],
                [111, "B", "2023-01-01 00:00:02"],
                [111, "custom_end", "2023-01-01 00:00:02"],
                [111, "custom_start", "2023-01-01 00:00:04"],
                [111, "C", "2023-01-01 00:00:04"],
                [111, "custom_end", "2023-01-01 00:00:04"]
            ],
            columns=["user_id", "event", "timestamp"]
        )
        stream = Eventstream(df)
        stream.split_sessions(delimiter_events=["custom_start", "custom_end"])\\
            .to_dataframe()\\
            .sort_values(["user_id", "event_index"])\\
            [["user_id", "event", "timestamp", "session_id"]]

           user_id          event           timestamp session_id
        0      111  session_start 2023-01-01 00:00:00      111_1
        1      111              A 2023-01-01 00:00:01      111_1
        2      111              B 2023-01-01 00:00:02      111_1
        3      111    session_end 2023-01-01 00:00:02      111_1
        4      111  session_start 2023-01-01 00:00:04      111_2
        5      111              C 2023-01-01 00:00:04      111_2
        6      111    session_end 2023-01-01 00:00:04      111_2

    Splitting by a 'delimiter_col'.

    .. code-block:: python

        df = pd.DataFrame(
            [
                [111, "A", "2023-01-01 00:00:01", "session_1"],
                [111, "B", "2023-01-01 00:00:02", "session_1"],
                [111, "C", "2023-01-01 00:00:03", "session_2"],
                [111, "D", "2023-01-01 00:00:04", "session_2"],
            ],
            columns=["user_id", "event", "timestamp", "custom_ses_id"]
        )
        raw_data_schema = {"custom_cols": [{"raw_data_col": "custom_ses_id", "custom_col": "custom_ses_id"}]}
        stream = Eventstream(df, raw_data_schema=raw_data_schema)
        stream.split_sessions(delimiter_col="custom_ses_id")\\
            .to_dataframe()\\
            .sort_values(["user_id", "event_index"])\\
            [["user_id", "event", "timestamp", "session_id", "custom_ses_id"]]

           user_id          event           timestamp session_id custom_ses_id
        0      111  session_start 2023-01-01 00:00:01      111_1     session_1
        1      111              A 2023-01-01 00:00:01      111_1     session_1
        2      111              B 2023-01-01 00:00:02      111_1     session_1
        3      111    session_end 2023-01-01 00:00:02      111_1     session_1
        4      111  session_start 2023-01-01 00:00:03      111_2     session_2
        5      111              C 2023-01-01 00:00:03      111_2     session_2
        6      111              D 2023-01-01 00:00:04      111_2     session_2
        7      111    session_end 2023-01-01 00:00:04      111_2     session_2

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

    @time_performance(
        scope="split_sessions",
        event_name="init",
    )
    def __init__(self, params: SplitSessionsParams) -> None:
        super().__init__(params=params)

        self.IS_SESSION_START_COL: Final = "is_session_start"

    def __get_session_borders_mask_by_timeout(
        self, df: pd.DataFrame, schema: EventstreamSchemaType
    ) -> Tuple[pd.Series, pd.Series]:
        timeout, timeout_unit = self.params.timeout or (None, None)
        user_col = schema.user_id
        timestamp_col = schema.event_timestamp

        prev_timedelta = df[timestamp_col] - df.groupby(user_col)[timestamp_col].shift(1)
        next_timedelta = df.groupby(user_col)[timestamp_col].shift(-1) - df[timestamp_col]
        prev_timedelta /= np.timedelta64(1, timeout_unit)  # type: ignore
        next_timedelta /= np.timedelta64(1, timeout_unit)  # type: ignore

        session_starts_mask = (prev_timedelta > timeout) | (prev_timedelta.isnull())
        session_ends_mask = (next_timedelta > timeout) | (next_timedelta.isnull())

        return session_starts_mask, session_ends_mask

    def __get_session_borders_mask_by_delimiting_events(
        self, df: pd.DataFrame, schema: EventstreamSchemaType
    ) -> Tuple[pd.Series, pd.Series, str, Union[str, None]]:
        delimiter_events = self.params.delimiter_events or [""]
        event_col = schema.event_name
        starting_event, ending_event = None, None

        if len(delimiter_events) == 1:
            starting_event = delimiter_events[0]
            session_starts_mask = df[event_col] == starting_event
            session_ends_mask = (df[event_col] == starting_event).shift(-1).fillna(True)
        elif len(delimiter_events) == 2:
            starting_event, ending_event = delimiter_events
            session_starts_mask = df[event_col] == starting_event
            session_ends_mask = df[event_col] == ending_event
        else:
            raise ValueError("The length of the 'delimiter_events' list must be 1 or 2.")

        return session_starts_mask, session_ends_mask, starting_event, ending_event

    def __get_session_borders_mask_by_delimiting_col(
        self, df: pd.DataFrame, schema: EventstreamSchemaType
    ) -> Tuple[pd.Series, pd.Series]:
        delimiter_col = self.params.delimiter_col
        user_col = schema.user_id

        if delimiter_col == self.params.session_col:
            warnings.warn("'delimiter_col' and 'session_col' are the same. The output will re-write delimiter_col.")

        session_starts_mask = df.groupby(user_col)[delimiter_col].shift(1) != df[delimiter_col]  # type: ignore
        session_ends_mask = df.groupby(user_col)[delimiter_col].shift(-1) != df[delimiter_col]  # type: ignore

        return session_starts_mask, session_ends_mask

    @time_performance(  # type: ignore
        scope="split_sessions",
        event_name="apply",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        id_col = schema.event_id
        user_col = schema.user_id
        time_col = schema.event_timestamp
        type_col = schema.event_type
        event_col = schema.event_name
        event_index_col = schema.event_index
        session_col = self.params.session_col
        # relate to 'delimiter_events' parameter
        starting_event, ending_event = None, None

        delimiter_params = [self.params.timeout, self.params.delimiter_events, self.params.delimiter_col]
        delimiter_params_count = sum([x is not None for x in delimiter_params])

        if delimiter_params_count > 1:
            text_message = "Only one of 'timeout' or 'delimiter_events' or 'delimiter_col' parameter must be defined."
            raise ValueError(text_message)
        if delimiter_params_count == 0:
            text_message = "Either one of 'timeout' or 'delimiter_events' or 'delimiter_col' parameter must be defined."
            raise ValueError(text_message)

        df = df.sort_values([user_col, event_index_col])

        if self.params.timeout:
            session_starts_mask, session_ends_mask = self.__get_session_borders_mask_by_timeout(df, schema)
        elif self.params.delimiter_events:
            (
                session_starts_mask,
                session_ends_mask,
                starting_event,
                ending_event,
            ) = self.__get_session_borders_mask_by_delimiting_events(df, schema)
        elif self.params.delimiter_col:
            session_starts_mask, session_ends_mask = self.__get_session_borders_mask_by_delimiting_col(df, schema)
        else:
            raise ValueError("Either 'timeout' or 'delimiter_events' or 'delimiter_col' parameter must be defined.")

        df[self.IS_SESSION_START_COL] = session_starts_mask
        df[session_col] = df.groupby(user_col)[self.IS_SESSION_START_COL].transform(np.cumsum)
        df[session_col] = df[user_col].astype(str) + "_" + df[session_col].astype(str)

        session_starts = df[session_starts_mask].copy()
        session_ends = df[session_ends_mask].copy()

        session_starts[event_col] = "session_start"
        session_starts[type_col] = "session_start"
        session_starts[id_col] = [uuid.uuid4() for x in range(len(session_starts))]

        session_ends[event_col] = "session_end"
        session_ends[type_col] = "session_end"
        session_ends[id_col] = [uuid.uuid4() for x in range(len(session_ends))]

        if self.params.timeout and self.params.mark_truncated:
            timeout, timeout_unit = self.params.timeout
            dataset_start = df[time_col].min()
            dataset_end = df[time_col].max()
            start_to_start = (session_starts[time_col] - dataset_start) / np.timedelta64(1, timeout_unit)
            end_to_end = (dataset_end - session_ends[time_col]) / np.timedelta64(1, timeout_unit)

            session_starts_truncated = session_starts[start_to_start < timeout]
            session_ends_truncated = session_ends[end_to_end < timeout]

            session_starts_truncated[event_col] = "session_start_cropped"
            session_starts_truncated[type_col] = "session_start_cropped"

            session_ends_truncated[event_col] = "session_end_cropped"
            session_ends_truncated[type_col] = "session_end_cropped"

            session_starts = pd.concat([session_starts, session_starts_truncated])
            session_ends = pd.concat([session_ends, session_ends_truncated])

        result = pd.concat([df, session_starts, session_ends])
        result = result.drop([self.IS_SESSION_START_COL], axis=1)
        df = df.drop([self.IS_SESSION_START_COL], axis=1)

        collect_data_performance(
            scope="split_sessions",
            event_name="metadata",
            called_params=self.to_dict()["values"],
            not_hash_values=["timeout"],
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

        if self.params.delimiter_events:
            result = result[result[event_col] != starting_event]
            if ending_event:
                result = result[result[event_col] != ending_event]

        return result
