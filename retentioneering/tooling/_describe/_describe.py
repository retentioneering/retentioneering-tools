from __future__ import annotations

import numpy as np
import pandas as pd
from numpy import timedelta64

from retentioneering.eventstream.types import EventstreamType


class _Describe:
    OUT_COLS = ("value",)
    INDEX_NAMES = ("category", "metric")
    TIME_ROUND_UNIT = "s"

    def __init__(
        self, eventstream: EventstreamType, session_col: str = "session_id", raw_events_only: bool = False
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.type_col = self.__eventstream.schema.event_type
        self.session_col = session_col

        self.overall_stats = [
            ["overall"],
            ["unique_users", "unique_events", "eventstream_start", "eventstream_end", "eventstream_length"],
        ]
        self.time_events_stats = [["path_length_time", "path_length_steps"], ["mean", "std", "median", "min", "max"]]

        self.df = self.__eventstream.to_dataframe(copy=True)
        self.has_session_col: bool = False
        if self.session_col in self.df.columns:
            self.has_session_col = True

        if raw_events_only:
            self.df = self.df[self.df[self.type_col].isin(["raw"])]

    def _calc_statistics(self, agg_col: str) -> list[np.timedelta64 | int | float]:
        df_agg = self.df.groupby(agg_col).agg({self.time_col: ["min", "max"], self.event_col: ["count"]}).reset_index()
        time_diff_user = df_agg[(self.time_col, "max")] - df_agg[(self.time_col, "min")]
        mean_time_agg_col = time_diff_user.mean().round(self.TIME_ROUND_UNIT)  # type: ignore
        median_time_agg_col = time_diff_user.median().round(self.TIME_ROUND_UNIT)  # type: ignore
        std_time_agg_col = time_diff_user.std().round(self.TIME_ROUND_UNIT)  # type: ignore
        min_length_time_agg_col = time_diff_user.min().round(self.TIME_ROUND_UNIT)  # type: ignore
        max_length_time_agg_col = time_diff_user.max().round(self.TIME_ROUND_UNIT)  # type: ignore

        event_count_agg_col = df_agg[(self.event_col, "count")]
        mean_event_agg_col = round(event_count_agg_col.mean(), 2)  # type: ignore
        median_event_agg_col = event_count_agg_col.median()
        std_event_agg_col = round(event_count_agg_col.std(), 2)  # type: ignore
        min_event_length_agg_col = event_count_agg_col.min()
        max_event_length_agg_col = event_count_agg_col.max()

        values_time_events = [
            mean_time_agg_col,
            std_time_agg_col,
            median_time_agg_col,
            min_length_time_agg_col,
            max_length_time_agg_col,
            mean_event_agg_col,
            std_event_agg_col,
            median_event_agg_col,
            min_event_length_agg_col,
            max_event_length_agg_col,
        ]
        return values_time_events  # type: ignore

    def _output_df_construction(
        self, values_overall: list[timedelta64 | int | float], values_time_events: list[timedelta64 | int | float]
    ) -> pd.DataFrame:
        overall_index = pd.MultiIndex.from_product(self.overall_stats, names=self.INDEX_NAMES)
        time_events_index = pd.MultiIndex.from_product(self.time_events_stats, names=self.INDEX_NAMES)

        # TODO внезапно в старом коде появилась ошибка типов, разобраться
        df_overall = pd.DataFrame(data=values_overall, index=overall_index, columns=self.OUT_COLS)  # type: ignore
        df_time_events = pd.DataFrame(data=values_time_events, index=time_events_index, columns=self.OUT_COLS)  # type: ignore

        return pd.concat([df_overall, df_time_events])

    def _values(self) -> pd.DataFrame:
        max_time = self.df[self.time_col].max()
        min_time = self.df[self.time_col].min()

        values_overall = [
            self.df[self.user_col].nunique(),
            self.df[self.event_col].nunique(),
            min_time.round(self.TIME_ROUND_UNIT),
            max_time.round(self.TIME_ROUND_UNIT),
            (max_time - min_time).round(self.TIME_ROUND_UNIT),
        ]

        values_time_events = self._calc_statistics(self.user_col)

        if self.has_session_col:
            self.time_events_stats[0] += ["session_length_time", "session_length_steps"]
            self.overall_stats[1].insert(2, "unique_sessions")

            values_overall.insert(2, self.df[self.session_col].nunique())
            values_time_events += self._calc_statistics(self.session_col)

        return self._output_df_construction(values_overall, values_time_events)  # type: ignore


__all__ = ("_Describe",)
