from __future__ import annotations

import warnings
from typing import Literal, Optional

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.constants import BINS_ESTIMATORS


class EventTimestampHist:
    """
    Class for visualizing distribution of events over time.

    Parameters
    ----------
    raw_events_only : bool, default False
        If ``True`` - statistics will only be shown for raw events.
        If ``False`` - statistics will be shown for all events presented in your data.
    event_list : list of str, optional
        Specify events to be displayed.
    lower_cutoff_quantile : float, optional
        Specify time distance quantile as the lower boundary. The values below the boundary are truncated.
    upper_cutoff_quantile : float, optional
        Specify time distance quantile as the upper boundary. The values above the boundary are truncated.
    bins : int or str, default 20
        Generic bin parameter that can be the name of a reference rule or
        the number of bins. Passed to :numpy_bins_link:`numpy.histogram_bin_edges<>`.
    figsize : tuple of float, default (12.0, 7.0)
        Width, height in inches.



    See Also
    --------
    .Cohorts :
    .TruncatedEvents : Can be useful for finding suitable values of parameters for this data processor.
    .TimedeltaHist
    .UserLifetimeHist :
    .Eventstream.describe
    .Eventstream.describe_events

    Notes
    -----
    See :ref:`Eventstream user guide<eventstream_events_timestamp>` for the details.
    """

    def __init__(
        self,
        eventstream: EventstreamType,
        raw_events_only: bool = False,
        event_list: list[str] | None = None,
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int | Literal[BINS_ESTIMATORS] = 20,
        figsize: tuple[float, float] = (12.0, 7.0),
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp

        self.event_list = event_list
        self.raw_events_only = raw_events_only

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
        self.figsize = figsize
        self.bins_to_show: np.ndarray = np.array([])
        self.values_to_plot: np.ndarray = np.array([])

    def _remove_cutoff_values(self, series: pd.Series) -> pd.Series:
        idx = [True] * len(series)
        if self.upper_cutoff_quantile is not None:
            idx &= series <= series.quantile(self.upper_cutoff_quantile)
        if self.lower_cutoff_quantile is not None:
            idx &= series >= series.quantile(self.lower_cutoff_quantile)
        return series[idx]

    def fit(self) -> None:
        """
        Calculate values for the histplot.

        Returns
        -------
        None

        """
        data = self.__eventstream.to_dataframe()

        if self.raw_events_only:
            data = data[data["event_type"].isin(["raw"])]
        if self.event_list:
            data = data[data[self.event_col].isin(self.event_list)]

        values_to_plot = data[self.time_col]
        if self._remove_cutoff_values:  # type: ignore
            values_to_plot = self._remove_cutoff_values(values_to_plot).to_numpy()

        bins_to_show = np.histogram_bin_edges(pd.to_numeric(values_to_plot), bins=self.bins)
        bins_to_show = pd.to_datetime(bins_to_show).round("s")
        if len(values_to_plot) == 0:
            bins_to_show = np.array([])

        self.bins_to_show = bins_to_show  # type: ignore
        self.values_to_plot = values_to_plot  # type: ignore

    @property
    def values(self) -> tuple[np.ndarray, np.ndarray]:
        """

        Returns
        -------
        tuple(np.ndarray, np.ndarray)

            1. The first array contains the values for histogram.
            2. The first array contains the bin edges.

        """
        return self.values_to_plot, self.bins_to_show

    def plot(self) -> matplotlib.axes.Axesne:
        """
        Create a sns.histplot based on the calculated values.

        Returns
        -------
        :matplotlib_axes:`matplotlib.axes.Axes<>`
            The matplotlib axes containing the plot.

        """

        plt.figure(figsize=self.figsize)

        hist = sns.histplot(self.values_to_plot, bins=self.bins)
        hist.set_title("Event timestamp histogram")

        return hist
