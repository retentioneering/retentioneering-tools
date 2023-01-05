from __future__ import annotations

import warnings
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns

from src.constants import DATETIME_UNITS
from src.eventstream.types import EventstreamType


class UserLifetimeHist:
    """
    Plots the distribution of user lifetimes. A users' lifetime is the timedelta between the first and the last events
    of the user. Can be useful for finding suitable parameters of various data processors, such as
    DeleteUsersByPathLength or TruncatedEvents.

    Parameters
    ----------
    timedelta_unit: :numpy_link:`DATETIME_UNITS<>`, default "s"
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
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: bool = False,
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int = 20,
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.timedelta_unit = timedelta_unit
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
        self.log_scale = log_scale
        self.bins = bins

    def _remove_cutoff_values(self, series: pd.Series) -> pd.Series:
        idx = [True] * len(series)
        if self.upper_cutoff_quantile is not None:
            idx &= series <= series.quantile(self.upper_cutoff_quantile)
        if self.lower_cutoff_quantile is not None:
            idx &= series >= series.quantile(self.lower_cutoff_quantile)
        return series[idx]

    def values(self) -> tuple[np.ndarray, np.ndarray | int]:
        data = self.__eventstream.to_dataframe().groupby(self.user_col)[self.time_col].agg(["min", "max"])
        data["time_passed"] = data["max"] - data["min"]
        values_to_plot = (data["time_passed"] / np.timedelta64(1, self.timedelta_unit)).reset_index(drop=True)
        values_to_plot = self._remove_cutoff_values(values_to_plot).to_numpy()
        if self.log_scale:
            bins_to_plot = np.logspace(np.log10(values_to_plot.min()), np.log10(values_to_plot.max()), self.bins)
        else:
            bins_to_plot = self.bins
        return values_to_plot, bins_to_plot

    def plot(self) -> None:
        out_hist = self.values()
        if self.log_scale:
            plt.xscale("log")
        plt.title("User lifetime histogram")
        plt.xlabel(f"Time units: {self.timedelta_unit}")
        plt.hist(out_hist[0], bins=out_hist[1], rwidth=0.9)
        plt.show()
