# write and run version 1 of class contents
# add tests

from __future__ import annotations

from typing import List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns

from src.eventstream.types import EventstreamType
from src.tooling.transition_time_hist.constants import (
    AGGREGATION_NAMES,
    HIST_TYPE_NAMES,
    TIMEDELTA_UNIT_NAMES,
)


class TransitionTimeHist:
    """
    Plots histogram for distribution of time between events. Supports various
    distribution types, such as distribution of time for adjacent consecutive events, or
    for a pair of pre-defined events, or median transition time from event to event per user.

    Parameters
    ----------
    hist_type: {"adjacent", "event_pair"}, default "adjacent"
        Defines the histogram type. "adjacent" plots time between all adjacent events across all users;
        "event_pair" plots time between all event pairs, where the events are specified in event_pair argument,
        and only_adjacent_event_pairs argument specifies whether the events must be adjacent.
    event_pair: tuple | list of length 2 (optional, default None)
        Is used only for hist_type="event_pair"; specifies event pair to plot transition time for. The first
        item corresponds to chronologically first event, the second item corresponds to the second event.
        Examples: ('login', 'purchase'); ['start', 'cabinet']
    only_adjacent_event_pairs: bool, default True
        Is used only for hist_type="event_pair"; specifies whether events need to be adjacent to be included.
        For example, if event_pair=("login", "purchase") and only_adjacent_event_pairs=False, then the sequence
        ("login", "main", "trading", "purchase") will contain a valid pair(which is not the case with
        only_adjacent_event_pairs=True)
    aggregation: {None, "user_mean", "user_median"}, default None
        Specifies aggregation policy for histogram data. None means no aggregation; "user_mean" and
        "user_median" plots distributions of user mean and user median time between adjacent events.
    timedelta_units: {"seconds", "minutes", "hours", "days"}, default "seconds"
        Specifies units of time difference the histogram should use.
    log_scale: bool, default False
        Applies log scaling to the horizontal axis.
    lower_cutoff_quantile: float (optional, default None)
        Specifies quantile data below which is excluded. Can be useful for removing small values from the histogram.
    upper_cutoff_quantile: float (optional, default None)
        Specifies quantile data above which is excluded. Can be useful for removing outliers from the histogram.
    bins: int, default 20
        Specifies the amount of histogram bins.

    """

    def __init__(
        self,
        eventstream: EventstreamType,
        hist_type: HIST_TYPE_NAMES = "adjacent",
        event_pair: Optional[Tuple[str, str] | List[str]] = None,
        only_adjacent_event_pairs: bool = True,
        aggregation: Optional[AGGREGATION_NAMES] = None,
        timedelta_units: TIMEDELTA_UNIT_NAMES = "seconds",
        log_scale: bool = False,
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int = 20,
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.hist_type = hist_type
        if hist_type == "event_pair":
            if not event_pair:
                raise ValueError('For hist_type="event_pair", argument event_pair should be specified.')
            if type(event_pair) not in (list, tuple):
                raise TypeError("event_pair should be a tuple or a list of length 2.")
            if len(event_pair) != 2:
                raise ValueError("event_pair should be a tuple or a list of length 2.")
        self.event_pair = event_pair
        self.only_adjacent_event_pairs = only_adjacent_event_pairs
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

    def _convert_to_timeunits(self, series: pd.Series) -> pd.Series:
        res = series.dt.total_seconds()
        if self.timedelta_units == "minutes":
            res = res / 60
        elif self.timedelta_units == "hours":
            res = res / 3600
        elif self.timedelta_units == "days":
            res = res / 86400
        return res

    def _aggregate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.aggregation == "user_mean":
            data = data.groupby(self.user_col)["time_passed"].mean().reset_index()
        elif self.aggregation == "user_mean":
            data = data.groupby(self.user_col)["time_passed"].median().reset_index()
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
        data = self.__eventstream.to_dataframe().sort_values([self.user_col, self.time_col])
        if self.hist_type == "event_pair":
            data = self._prepare_event_pair_data(data)
        data["time_passed"] = self._convert_to_timeunits(data[self.time_col].diff())
        data = data[data[self.user_col] == data[self.user_col].shift()]
        data = self._aggregate_data(data)
        values_to_plot = data["time_passed"].reset_index(drop=True)
        values_to_plot = self._remove_cutoff_values(values_to_plot)
        if self.log_scale:
            logbins = np.logspace(np.log10(values_to_plot.min()), np.log10(values_to_plot.max()), self.bins)
            plt.xscale("log")
            return plt.hist(values_to_plot, bins=logbins)
        return plt.hist(values_to_plot, bins=self.bins)
