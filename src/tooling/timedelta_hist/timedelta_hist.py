# write and run version 1 of class contents
# add tests

from __future__ import annotations

from typing import List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns

from src.constants import DATETIME_UNITS
from src.eventstream.types import EventstreamType
from src.tooling.timedelta_hist.constants import AGGREGATION_NAMES


class TimedeltaHist:
    """
    Plots the distribution of the time deltas between two events. Supports various
    distribution types, such as distribution of time for adjacent consecutive events, or
    for a pair of pre-defined events, or median transition time from event to event per user/session.

    Parameters
    ----------
    event_pair: tuple | list of length 2 (optional, default None)
        Specifies an event pair to plot the time distance between. The first
        item corresponds to chronologically first event, the second item corresponds to the second event. If
        event_pair=None, plots distribution of timedelta for all adjacent events.
        Examples: ('login', 'purchase'); ['start', 'cabinet']
    only_adjacent_event_pairs: bool, default True
        Is used only when event_pair is not None; specifies whether events need to be adjacent to be included.
        For example, if event_pair=("login", "purchase") and only_adjacent_event_pairs=False, then the sequence
        ("login", "main", "trading", "purchase") will contain a valid pair(which is not the case with
        only_adjacent_event_pairs=True)
    weight_col: str, default None
        Specifies a unit of observation, inside which time differences will be computed. For example, if weight_col is
        set to session id, will only compute time deltas for events inside each session. If None, selects user_id
        column.
    aggregation: {None, "mean", "median"}, default None
        Specifies the aggregation policy for the time distances. Aggregates based on passed weight_col. None means no
        aggregation; "mean" and "median" plots distributions of weight_col unit mean and weight_col unit median
        timedeltas. For example, if session id is specified in weight_col, one observation per
        session(for example, session median) will be provided for the histogram.
    timedelta_units: :numpy_link:`DATETIME_UNITS<>`, default "s"
        Specifies the units of the time differences the histogram should use. Use "s" for seconds, "m" for minutes,
        "h" for hours and "D" for days.
    log_scale: bool, default False
        Applies log scaling to the x axis.
    lower_cutoff_quantile: float (optional, default None)
        Specifies the time distance quantile as the lower boundary. The values below the boundary are truncated.
    upper_cutoff_quantile: float (optional, default None)
        Specifies the time distance quantile as the upper boundary. The values above the boundary are truncated.
    bins: int, default 20
        Specifies the amount of histogram bins.

    """

    def __init__(
        self,
        eventstream: EventstreamType,
        event_pair: Optional[Tuple[str, str] | List[str]] = None,
        only_adjacent_event_pairs: bool = True,
        weight_col: Optional[str] = None,
        aggregation: Optional[AGGREGATION_NAMES] = None,
        timedelta_units: DATETIME_UNITS = "s",
        log_scale: bool = False,
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int = 20,
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        if event_pair is not None:
            if type(event_pair) not in (list, tuple):
                raise TypeError("event_pair should be a tuple or a list of length 2.")
            if len(event_pair) != 2:
                raise ValueError("event_pair should be a tuple or a list of length 2.")
        self.event_pair = event_pair
        self.only_adjacent_event_pairs = only_adjacent_event_pairs
        self.weight_col = weight_col
        if weight_col is None:
            self.agg_col = self.user_col
        else:
            self.agg_col = weight_col
        self.aggregation = aggregation
        self.timedelta_units = timedelta_units
        if lower_cutoff_quantile is not None:
            if not 0 < lower_cutoff_quantile < 1:
                raise ValueError("lower_cutoff_quantile should be a fraction between 0 and 1.")
        self.lower_cutoff_quantile = lower_cutoff_quantile
        if upper_cutoff_quantile is not None:
            if not 0 < upper_cutoff_quantile < 1:
                raise ValueError("upper_cutoff_quantile should be a fraction between 0 and 1.")
        self.upper_cutoff_quantile = upper_cutoff_quantile
        self.log_scale = log_scale
        self.bins = bins

    def _prepare_event_pair_data(self, data: pd.DataFrame) -> pd.DataFrame:
        if not self.only_adjacent_event_pairs:
            data = data[data[self.event_col].isin(self.event_pair)]  # type: ignore
        select_ids = (data[self.event_col] == self.event_pair[1]) & (data[self.event_col].shift() == self.event_pair[0])  # type: ignore
        data = data[select_ids]
        return data

    def _exclude_multiunit_events(self, data: pd.DataFrame) -> pd.DataFrame:
        # removes all diffs where the timedelta is computed between different grouping units
        # e.g. if grouping unit is user, removes all cases where timedelta is between events of different users
        # example: ['user1_action', 'user1_action', 'user2_action'] - diff between middle and last event is excluded
        return data[data[self.agg_col] == data[self.agg_col].shift()]  # type: ignore

    def _aggregate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.aggregation is not None:
            data = data.groupby(self.agg_col)["time_passed"].agg(self.aggregation).reset_index()
        return data

    def _remove_cutoff_values(self, series: pd.Series) -> pd.Series:
        if self.upper_cutoff_quantile is not None and self.lower_cutoff_quantile is not None:
            return series[
                (series <= series.quantile(self.upper_cutoff_quantile))
                & (series >= series.quantile(self.lower_cutoff_quantile))
            ]
        elif self.upper_cutoff_quantile is not None:
            return series[series <= series.quantile(self.upper_cutoff_quantile)]
        elif self.lower_cutoff_quantile is not None:
            return series[series >= series.quantile(self.lower_cutoff_quantile)]
        return series

    def plot(self) -> go.Figure:
        data = self.__eventstream.to_dataframe().sort_values([self.agg_col, self.time_col])
        if self.event_pair is not None:
            data = self._prepare_event_pair_data(data)
        data["time_passed"] = data[self.time_col].diff() / np.timedelta64(1, self.timedelta_units)
        # the next line removes "invalid" events(events not inside one unit(user/session))
        data = self._exclude_multiunit_events(data)
        data = self._aggregate_data(data)
        values_to_plot = data["time_passed"].reset_index(drop=True)
        values_to_plot = self._remove_cutoff_values(values_to_plot)
        plt.title(
            f"Timedelta histogram, event pair {self.event_pair}, weight column {self.weight_col}"
            f"{', group ' + self.aggregation if self.aggregation is not None else ''}"
        )
        plt.xlabel(f"Time units: {self.timedelta_units}")
        if self.log_scale:
            logbins = np.logspace(np.log10(values_to_plot.min()), np.log10(values_to_plot.max()), self.bins)
            plt.xscale("log")
            return plt.hist(values_to_plot, bins=logbins, rwidth=0.9)
        return plt.hist(values_to_plot, bins=self.bins, rwidth=0.9)
