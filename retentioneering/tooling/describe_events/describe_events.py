from __future__ import annotations

from typing import List, Optional

import numpy as np
import pandas as pd
from IPython.display import display

from retentioneering.eventstream.types import EventstreamType


class DescribeEvents:
    """
    Display general information on the eventstream events. If ``session_col`` is present in eventstream, also
    output session statistics, assuming ``session_col`` is the session identifier column.

    Parameters
    ----------
    session_col : str, default 'session_id'
        Specify name of the session column. If present in the eventstream, output session statistics.

    event_list : List of str or 'all', default 'all'
        Specify the events to be plotted. If ``all``, describe all events.

    """

    def __init__(
        self,
        eventstream: EventstreamType,
        session_col: Optional[str] = "session_id",
        event_list: Optional[List[str] | str] = "all",
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.session_col = session_col
        self.type_col = self.__eventstream.schema.event_type
        self.event_list = event_list

    def display(self) -> None:
        user_col, event_col, time_col, type_col, session_col, event_list = (
            self.user_col,
            self.event_col,
            self.time_col,
            self.type_col,
            self.session_col,
            self.event_list,
        )

        df = self.__eventstream.to_dataframe()
        if event_list != "all":
            if type(event_list) is not list:
                raise TypeError('event_list should either be "all", or a list of event names to include.')
            unique_events = df[df[event_col].isin(event_list)][event_col].unique()
        else:
            unique_events = df[event_col].unique()

        has_sessions = session_col in df.columns

        df["__event_trajectory_idx"] = df.groupby(user_col).cumcount()
        df["__event_trajectory_timedelta"] = df[time_col] - df.groupby(user_col)[time_col].transform("first")
        total_events = df.shape[0]
        unique_users = df[user_col].nunique()

        unique_sessions = None
        if has_sessions:
            df["__event_session_idx"] = df.groupby(session_col).cumcount()
            df["__event_session_timedelta"] = df[time_col] - df.groupby(session_col)[time_col].transform("first")
            unique_sessions = df[session_col].nunique()  # type: ignore

        for i, event_name in enumerate(unique_events):
            if i != 0:
                print("=" * 30, end="\n")

            event_data = df[df[event_col] == event_name]

            print(f'\033[1m"{event_name}" event statistics:\033[0m')
            print()

            print("-" * 30)
            print("\033[1mBasic statistics\033[0m")
            print()

            event_share = round(event_data.shape[0] / total_events, 4)
            unique_users_event = event_data[user_col].nunique()
            user_event_share = round(unique_users_event / unique_users, 4)
            out_rows = [
                ["first appearance", event_data[time_col].min()],
                ["last appearance", event_data[time_col].max()],
                ["number of occurrences", event_data.shape[0]],
                ["share of all events", str(event_share * 100) + "%"],
                ["users with the event", unique_users_event],
                ["share of users with the event", str(user_event_share * 100) + "%"],
            ]
            out_columns = ["metric", "value"]
            if has_sessions:
                unique_sessions_event = event_data[session_col].nunique()  # type: ignore
                session_event_share = round(unique_sessions_event / unique_sessions, 4)
                out_rows.append(["sessions with the event", unique_sessions_event])
                out_rows.append(["share of sessions with the event", str(session_event_share * 100) + "%"])
            out_df = pd.DataFrame(out_rows, columns=out_columns).set_index("metric")
            display(out_df)

            user_agg = event_data.groupby(user_col)[event_col].agg("count")
            mean_events_user, std_events_user, median_events_user = user_agg.mean(), user_agg.std(), user_agg.median()
            min_events_user, max_events_user = user_agg.min(), user_agg.max()
            session_agg, session_stats = None, []
            if has_sessions:
                session_agg = event_data.groupby(session_col)[event_col].agg("count")  # type: ignore
                mean_events_session, std_events_session, median_events_session = (
                    session_agg.mean(),
                    session_agg.std(),
                    session_agg.median(),
                )
                min_events_session, max_events_session = session_agg.min(), session_agg.max()
                session_stats = [
                    [mean_events_session],
                    [std_events_session],
                    [median_events_session],
                    [min_events_session],
                    [max_events_session],
                ]
            print("-" * 30)
            out_rows = [
                ["mean appearances", mean_events_user],
                ["std appearances", std_events_user],
                ["median appearances", median_events_user],
                ["min appearances", min_events_user],
                ["max appearances", max_events_user],
            ]
            out_columns = ["metric", "per user path"]
            if has_sessions:
                out_rows = [out_rows[i] + session_stats[i] for i in range(len(out_rows))]
                out_columns.append("per session")
                print("\033[1mAppearances per user path/session\033[0m")
            else:
                print("\033[1mAppearances per user path\033[0m")
            out_df = pd.DataFrame(out_rows, columns=out_columns).set_index("metric")
            display(out_df)

            user_agg = event_data.groupby(user_col)["__event_trajectory_timedelta"].min()
            mean_time_user, std_time_user, median_time_user = user_agg.mean(), user_agg.std(), user_agg.median()
            min_time_user, max_time_user = user_agg.min(), user_agg.max()
            session_agg, session_stats = None, []
            if has_sessions:
                session_agg = event_data.groupby(session_col)["__event_session_timedelta"].min()  # type: ignore
                mean_time_session, std_time_session, median_time_session = (
                    session_agg.mean(),
                    session_agg.std(),
                    session_agg.median(),
                )
                min_time_session, max_time_session = session_agg.min(), session_agg.max()
                session_stats = [
                    [mean_time_session],
                    [std_time_session],
                    [median_time_session],
                    [min_time_session],
                    [max_time_session],
                ]
            print("-" * 30)
            out_rows = [
                ["mean appearances", mean_time_user],
                ["std appearances", std_time_user],
                ["median appearances", median_time_user],
                ["min appearances", min_time_user],
                ["max appearances", max_time_user],
            ]
            out_columns = ["metric", "per user path"]
            if has_sessions:
                out_rows = [out_rows[i] + session_stats[i] for i in range(len(out_rows))]
                out_columns.append("per session")
                print("\033[1mTime before first appearance since user path/session start\033[0m")
            else:
                print("\033[1mTime before first appearance since user path start\033[0m")
            out_df = pd.DataFrame(out_rows, columns=out_columns).set_index("metric")
            display(out_df)

            user_agg = event_data.groupby(user_col)["__event_trajectory_idx"].min()
            mean_events_user, std_events_user, median_events_user = user_agg.mean(), user_agg.std(), user_agg.median()
            min_events_user, max_events_user = user_agg.min(), user_agg.max()
            session_agg, session_stats = None, []
            if has_sessions:
                session_agg = event_data.groupby(session_col)["__event_session_idx"].min()  # type: ignore
                mean_events_session, std_events_session, median_events_session = (
                    session_agg.mean(),
                    session_agg.std(),
                    session_agg.median(),
                )
                min_events_session, max_events_session = session_agg.min(), session_agg.max()
                session_stats = [
                    [mean_events_session],
                    [std_events_session],
                    [median_events_session],
                    [min_events_session],
                    [max_events_session],
                ]
            print("-" * 30)
            out_rows = [
                ["mean events", mean_events_user],
                ["std events", std_events_user],
                ["median events", median_events_user],
                ["min events", min_events_user],
                ["max events", max_events_user],
            ]
            out_columns = ["metric", "per user path"]
            if has_sessions:
                out_rows = [out_rows[i] + session_stats[i] for i in range(len(out_rows))]
                out_columns.append("per session")
                print("\033[1mEvents before first appearance since user path/session start\033[0m")
            else:
                print("\033[1mEvents before first appearance since user path start\033[0m")
            out_df = pd.DataFrame(out_rows, columns=out_columns).set_index("metric")
            display(out_df)
