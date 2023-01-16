from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
from IPython.display import display

from src.eventstream.types import EventstreamType


class Describe:
    """
    Display general eventstream information. If ``session_col`` is present in eventstream columns, also
    output session statistics, assuming ``session_col`` is the session identifier column.

    Parameters
    ----------
    session_col : str, default 'session_id'
        Specify name of the session column. If present in the eventstream, output session statistics.

    """

    def __init__(self, eventstream: EventstreamType, session_col: Optional[str] = "session_id") -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.session_col = session_col
        self.type_col = self.__eventstream.schema.event_type

    def display(self) -> None:
        user_col, event_col, time_col, type_col, session_col = (
            self.user_col,
            self.event_col,
            self.time_col,
            self.type_col,
            self.session_col,
        )

        df = self.__eventstream.to_dataframe()
        has_sessions = session_col in df.columns

        df = df[df[type_col].isin(["raw"])]
        max_time = df[time_col].max()
        min_time = df[time_col].min()

        print("-" * 30)
        print("\033[1mBasic statistics\033[0m")
        print()

        out_rows = [
            ["unique users", df[user_col].nunique()],
            ["unique events", df[event_col].nunique()],
            ["eventstream start", df[time_col].min()],
            ["eventstream end", df[time_col].max()],
            ["eventstream length", max_time - min_time],
        ]
        out_columns = ["metric", "value"]
        if has_sessions:
            out_rows.insert(2, ["unique sessions", df[session_col].nunique()])  # type: ignore
        out_df = pd.DataFrame(out_rows, columns=out_columns).set_index("metric")
        display(out_df)

        user_agg = df.groupby(user_col).agg({time_col: ["min", "max"], event_col: ["count"]}).reset_index()
        time_diff_user = user_agg[(time_col, "max")] - user_agg[(time_col, "min")]
        mean_time_user = time_diff_user.mean()
        median_time_user = time_diff_user.median()
        std_time_user = time_diff_user.std()
        min_length_time_user = time_diff_user.min()
        max_length_time_user = time_diff_user.max()
        print("-" * 30)
        session_agg, session_stats = None, []
        if has_sessions:
            session_agg = df.groupby(session_col).agg({time_col: ["min", "max"], event_col: ["count"]}).reset_index()
            time_diff_session = session_agg[(time_col, "max")] - session_agg[(time_col, "min")]
            mean_time_session = time_diff_session.mean()
            median_time_session = time_diff_session.median()
            std_time_session = time_diff_session.std()
            min_length_time_session = time_diff_session.min()
            max_length_time_session = time_diff_session.max()
            session_stats = [
                [mean_time_session],
                [std_time_session],
                [median_time_session],
                [min_length_time_session],
                [max_length_time_session],
            ]
        out_rows = [
            ["mean time", mean_time_user],
            ["std time", std_time_user],
            ["median time", median_time_user],
            ["min time", min_length_time_user],
            ["max time", max_length_time_user],
        ]
        out_columns = ["metric", "per user path"]
        if has_sessions:
            out_rows = [out_rows[i] + session_stats[i] for i in range(len(out_rows))]
            out_columns.append("per session")
            print("\033[1mUser path/session time length\033[0m")
        else:
            print("\033[1mUser path time length\033[0m")
        out_df = pd.DataFrame(out_rows, columns=out_columns).set_index("metric")
        display(out_df)

        event_count_user = user_agg[(event_col, "count")]
        mean_user = round(event_count_user.mean(), 2)  # type: ignore
        median_user = event_count_user.median()
        std_user = round(event_count_user.std(), 2)  # type: ignore
        min_length_user = event_count_user.min()
        max_length_user = event_count_user.max()
        print("-" * 30)
        if has_sessions:
            if session_agg is None:
                raise ValueError("session_agg is None")
            event_count_session = session_agg[(event_col, "count")]
            mean_session = round(event_count_session.mean(), 2)  # type: ignore
            median_session = event_count_session.median()
            std_session = round(event_count_session.std(), 2)  # type: ignore
            min_length_session = event_count_session.min()
            max_length_session = event_count_session.max()
            session_stats = [
                [mean_session],
                [std_session],
                [median_session],
                [min_length_session],
                [max_length_session],
            ]
        out_rows = [
            ["mean events", mean_user],
            ["std events", std_user],
            ["median events", median_user],
            ["min events", min_length_user],
            ["max events", max_length_user],
        ]
        out_columns = ["metric", "per user path"]
        if has_sessions:
            out_rows = [out_rows[i] + session_stats[i] for i in range(len(out_rows))]
            out_columns.append("per session")
            print("\033[1mNumber of events per user path/session\033[0m")
        else:
            print("\033[1mNumber of events per user path\033[0m")
        out_df = pd.DataFrame(out_rows, columns=out_columns).set_index("metric")
        display(out_df)
