from __future__ import annotations

import warnings
from typing import List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns

from src.constants import DATETIME_UNITS
from src.eventstream.types import EventstreamType


class EventTimestampHist:
    """
    Plots the distribution of events over time. Can be useful for detecting time-based anomalies, and visualising
    general timespan of the eventstream.

    Parameters
    ----------
    event_list: List[str] | "all" (optional, default "all")
        Specifies the events to be plotted. If "all"(by default), plots all events.
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
        event_list: Optional[List[str] | str] = "all",
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int = 20,
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        if event_list != "all":
            if type(event_list) is not list:
                raise TypeError('event_list should either be "all", or a list of event names to include.')
        self.event_list = event_list
        if lower_cutoff_quantile is not None:
            if not 0 < lower_cutoff_quantile < 1:
                raise ValueError("lower_cutoff_quantile should be a fraction between 0 and 1.")
        self.lower_cutoff_quantile = lower_cutoff_quantile
        if upper_cutoff_quantile is not None:
            if not 0 < upper_cutoff_quantile < 1:
                raise ValueError("upper_cutoff_quantile should be a fraction between 0 and 1.")
        self.upper_cutoff_quantile = upper_cutoff_quantile
        if lower_cutoff_quantile is not None and upper_cutoff_quantile is not None:
            if lower_cutoff_quantile > upper_cutoff_quantile:
                warnings.warn("lower_cutoff_quantile exceeds upper_cutoff_quantile; no data passed to the histogram")
        self.bins = bins

    def _remove_cutoff_values(self, series: pd.Series) -> pd.Series:
        idx = [True] * len(series)
        if self.upper_cutoff_quantile is not None:
            idx &= series <= series.quantile(self.upper_cutoff_quantile)
        if self.lower_cutoff_quantile is not None:
            idx &= series >= series.quantile(self.lower_cutoff_quantile)
        return series[idx]

    @property
    def values(self) -> tuple[np.ndarray, np.ndarray | int]:
        data = self.__eventstream.to_dataframe()
        if self.event_list != "all":
            data = data[data[self.event_col].isin(self.event_list)]
        values = data[self.time_col]
        idx = [True] * len(values)
        if self.upper_cutoff_quantile is not None:
            idx &= values <= values.quantile(self.upper_cutoff_quantile)
        if self.lower_cutoff_quantile is not None:
            idx &= values >= values.quantile(self.lower_cutoff_quantile)
        values_to_plot = values[idx].to_numpy()
        bins_to_plot = self.bins
        return values_to_plot, bins_to_plot

    def plot(self) -> None:
        out_hist = self.values
        plt.title("Event timestamp histogram")
        plt.hist(out_hist[0], bins=out_hist[1])
        plt.show()
