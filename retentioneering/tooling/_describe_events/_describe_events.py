from __future__ import annotations

import pandas as pd

from retentioneering.eventstream.types import EventstreamType


class _DescribeEvents:
    TIME_ROUND_UNIT = "s"
    STATS_ORDER = ["mean", "std", "median", "min", "max"]
    UNIQUE_SESS = ("basic_statistics", "unique_sessions")
    SHARE_SESS = ("basic_statistics", "unique_sessions_shared")

    def __init__(
        self,
        eventstream: EventstreamType,
        session_col: str = "session_id",
        raw_events_only: bool = False,
        event_list: list[str] | None = None,
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.session_col = session_col
        self.type_col = self.__eventstream.schema.event_type
        self.event_list = event_list
        self.raw_events_only = raw_events_only
        self.df = self.__eventstream.to_dataframe(copy=True)
        self.has_session_col: bool = False
        if self.raw_events_only:
            self.df = self.df[self.df[self.type_col].isin(["raw"])]

        self.total_events_base: int = len(self.df)
        self.unique_users_base: int = self.df[self.user_col].nunique()
        if self.session_col in self.df.columns:
            self.has_session_col = True
            self.total_sessions_base: int = self.df[self.session_col].nunique()

    def _agg_min_time(self, df: pd.DataFrame, agg_col: str, prefix: str) -> pd.DataFrame:
        df[f"__event_{prefix}_idx"] = df.groupby(agg_col).cumcount()
        df[f"__event_{prefix}_timedelta"] = df[self.time_col] - df.groupby(agg_col)[self.time_col].transform("first")
        return df

    def _agg_time_events(self, df: pd.DataFrame, agg_col: str, prefix: str) -> pd.DataFrame:
        first_time = f"time_to_FO_{prefix}_wise"
        first_event = f"steps_to_FO_{prefix}_wise"

        df_agg_event = (
            df.groupby([self.event_col, agg_col])
            .agg(time_to_FO=(f"__event_{prefix}_timedelta", "first"), steps_to_FO=(f"__event_{prefix}_idx", "first"))
            .reset_index()
        )
        df_agg_event.columns = [self.event_col, self.user_col, first_time, first_event]  # type: ignore
        df_agg_event[first_time] = df_agg_event[first_time].dt.total_seconds()
        df_agg_event = df_agg_event.groupby([self.event_col])[[first_time, first_event]].agg(
            self.STATS_ORDER  # type: ignore
        )
        df_agg_event[(first_event, "mean")] = df_agg_event[(first_event, "mean")].round(2)
        df_agg_event[(first_event, "std")] = df_agg_event[(first_event, "std")].round(2)

        for stat in self.STATS_ORDER:
            mult_ind = (first_time, stat)
            df_agg_event[mult_ind] = pd.to_timedelta(df_agg_event[mult_ind], unit="s").round(self.TIME_ROUND_UNIT)  # type: ignore

        return df_agg_event

    def _create_basic_info_df(self, df: pd.DataFrame) -> pd.DataFrame:
        basic_info = df.groupby("event").agg(
            number_of_occurrences=("event_id", "count"), unique_users=("user_id", "nunique")
        )

        basic_info["number_of_occurrences_shared"] = (
            basic_info["number_of_occurrences"] / self.total_events_base
        ).round(2)
        basic_info["unique_users_shared"] = (basic_info["unique_users"] / self.unique_users_base).round(2)

        basic_info.columns = pd.MultiIndex.from_product([["basic_statistics"], basic_info.columns])

        if self.has_session_col:
            basic_info[self.UNIQUE_SESS] = self.df.groupby("event")[self.session_col].agg("nunique")
            basic_info[self.SHARE_SESS] = (basic_info[self.UNIQUE_SESS] / self.total_sessions_base).round(2)

        return basic_info

    def _values(self) -> pd.DataFrame:
        df = self._agg_min_time(df=self.df, agg_col=self.user_col, prefix="user")

        if self.has_session_col:
            df = self._agg_min_time(df=df, agg_col=self.session_col, prefix="session")

        if self.event_list:
            df = df[df[self.event_col].isin(self.event_list)]

        basic_info = self._create_basic_info_df(df=df)
        df_agg_event = self._agg_time_events(df=df, agg_col=self.user_col, prefix="user")

        if self.has_session_col:
            df_agg_sess = self._agg_time_events(df=df, agg_col=self.session_col, prefix="session")
            df_agg_event = df_agg_event.merge(df_agg_sess, left_index=True, right_index=True)

        res = basic_info.merge(df_agg_event, left_index=True, right_index=True)
        if self.has_session_col:
            res.insert(2, self.UNIQUE_SESS, res.pop(self.UNIQUE_SESS))  # type: ignore
        return res


__all__ = ("_DescribeEvents",)
