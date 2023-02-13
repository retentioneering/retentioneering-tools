from __future__ import annotations

import warnings
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from retentioneering.constants import DATETIME_UNITS
from retentioneering.eventstream.types import EventstreamType


class UserLifetimeHist:
    """
    A class for visualize a ``users' lifetime``.

    Parameters
    ----------
    timedelta_unit : :numpy_link:`DATETIME_UNITS<>`, default 's'
        Specifies the units of the time differences the histogram should use. Use "s" for seconds, "m" for minutes,
        "h" for hours and "D" for days.
    log_scale : tuple of bool, default (False, False)
        Apply log scaling to the (``x``, ``y``) axes.
    lower_cutoff_quantile : float, optional
        Specifies the time distance quantile as the lower boundary. The values below the boundary are truncated.
    upper_cutoff_quantile : float, optional
        Specifies the time distance quantile as the upper boundary. The values above the boundary are truncated.
    bins : int or str, default 20
        Generic bin parameter that can be the name of a reference rule or
        the number of bins. Passed to :numpy_bins_link:`numpy.histogram_bin_edges<>`
    figsize : tuple of float, default (12.0, 7.0)
        Width, height in inches.

    """

    def __init__(
        self,
        eventstream: EventstreamType,
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: tuple[bool, bool] = (False, False),
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int | str = 20,
        figsize: tuple[float, float] = (12.0, 7.0),
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
        self.figsize = figsize

    def _remove_cutoff_values(self, series: pd.Series) -> pd.Series:
        idx = [True] * len(series)
        if self.upper_cutoff_quantile is not None:
            idx &= series <= series.quantile(self.upper_cutoff_quantile)
        if self.lower_cutoff_quantile is not None:
            idx &= series >= series.quantile(self.lower_cutoff_quantile)
        return series[idx]

    @property
    def values(self) -> tuple[np.ndarray, np.ndarray]:
        """
        Calculate values for the histplot.

        Returns
        -------
        tuple(np.ndarray, np.ndarray)

            1. The first array contains the values for histogram
            2. The first array contains the bin edges

        """
        data = self.__eventstream.to_dataframe().groupby(self.user_col)[self.time_col].agg(["min", "max"])
        data["time_passed"] = data["max"] - data["min"]
        values_to_plot = (data["time_passed"] / np.timedelta64(1, self.timedelta_unit)).reset_index(  # type: ignore
            drop=True
        )

        if self._remove_cutoff_values:  # type: ignore
            values_to_plot = self._remove_cutoff_values(values_to_plot).to_numpy()
        if self.log_scale[0]:
            log_adjustment = np.timedelta64(100, "ms") / np.timedelta64(1, self.timedelta_unit)
            values_to_plot = np.where(
                values_to_plot != 0, values_to_plot, values_to_plot + log_adjustment
            )  # type: ignore
            bins_to_show = np.power(10, np.histogram_bin_edges(np.log10(values_to_plot), bins=self.bins))
        else:
            bins_to_show = np.histogram_bin_edges(values_to_plot, bins=self.bins)
        if len(values_to_plot) == 0:
            bins_to_show = np.array([])
        return values_to_plot, bins_to_show  # type: ignore

    def plot(self) -> None:
        """
        Create a sns.histplot based on the calculated values.
        """
        out_hist = self.values[0]
        plt.figure(figsize=self.figsize)

        plt.title("User lifetime histogram")
        plt.xlabel(f"Time units: {self.timedelta_unit}")
        sns.histplot(out_hist, bins=self.bins, log_scale=self.log_scale)
        plt.show()
