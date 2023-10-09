from __future__ import annotations

import warnings
from typing import Literal

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.constants import BINS_ESTIMATORS


class EventTimestampHist:
    """
    Plot the distribution of events over time. Can be useful for detecting
    time-based anomalies, and visualising general timespan of the eventstream.

    Parameters
    ----------
    eventstream : EventstreamType

    See Also
    --------
    .TimedeltaHist : Plot the distribution of the time deltas between two events.
    .UserLifetimeHist : Plot the distribution of user lifetimes.
    .Eventstream.describe : Show general eventstream statistics.
    .Eventstream.describe_events : Show general eventstream events statistics.

    Notes
    -----
    See :ref:`Eventstream user guide<eventstream_events_timestamp>` for the details.
    """

    __eventstream: EventstreamType
    raw_events_only: bool
    event_list: list[str] | None
    lower_cutoff_quantile: float | None
    upper_cutoff_quantile: float | None
    bins: int | Literal[BINS_ESTIMATORS]
    bins_to_show: np.ndarray
    values_to_plot: np.ndarray

    @time_performance(
        scope="event_timestamp_hist",
        event_name="init",
    )
    def __init__(self, eventstream: EventstreamType) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp

        self.bins_to_show = np.array([])
        self.values_to_plot = np.array([])

    def _remove_cutoff_values(self, series: pd.Series) -> pd.Series:
        idx = [True] * len(series)
        if self.upper_cutoff_quantile is not None:
            idx &= series <= series.quantile(self.upper_cutoff_quantile)
        if self.lower_cutoff_quantile is not None:
            idx &= series >= series.quantile(self.lower_cutoff_quantile)
        return series[idx]

    def __validate_input(
        self,
        lower_cutoff_quantile: float | None = None,
        upper_cutoff_quantile: float | None = None,
    ) -> tuple[float | None, float | None]:
        if lower_cutoff_quantile is not None:
            if not 0 < lower_cutoff_quantile < 1:
                raise ValueError("lower_cutoff_quantile should be a fraction between 0 and 1.")

        if upper_cutoff_quantile is not None:
            if not 0 < upper_cutoff_quantile < 1:
                raise ValueError("upper_cutoff_quantile should be a fraction between 0 and 1.")

        if lower_cutoff_quantile is not None and upper_cutoff_quantile is not None:
            if lower_cutoff_quantile > upper_cutoff_quantile:
                warnings.warn("lower_cutoff_quantile exceeds upper_cutoff_quantile; no data passed to the histogram")

        return upper_cutoff_quantile, lower_cutoff_quantile

    @time_performance(
        scope="event_timestamp_hist",
        event_name="fit",
    )
    def fit(
        self,
        raw_events_only: bool = False,
        event_list: list[str] | None = None,
        lower_cutoff_quantile: float | None = None,
        upper_cutoff_quantile: float | None = None,
        bins: int | Literal[BINS_ESTIMATORS] = 20,
    ) -> None:
        """
        Calculate values for the histplot.

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

        Returns
        -------
        None

        """
        called_params = {
            "raw_events_only": raw_events_only,
            "event_list": event_list,
            "lower_cutoff_quantile": lower_cutoff_quantile,
            "upper_cutoff_quantile": upper_cutoff_quantile,
            "bins": bins,
        }

        self.upper_cutoff_quantile, self.lower_cutoff_quantile = self.__validate_input(
            lower_cutoff_quantile,
            upper_cutoff_quantile,
        )

        self.event_list = event_list
        self.raw_events_only = raw_events_only
        self.bins = bins

        data = self.__eventstream.to_dataframe(copy=True)

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

        collect_data_performance(
            scope="event_timestamp_hist",
            event_name="metadata",
            called_params=called_params,
            performance_data={},
            eventstream_index=self.__eventstream._eventstream_index,
        )

    @property
    @time_performance(  # type: ignore
        scope="event_timestamp_hist",
        event_name="values",
    )
    def values(self) -> tuple[np.ndarray, np.ndarray]:
        """

        Returns
        -------
        tuple(np.ndarray, np.ndarray)

            1. The first array contains the values for histogram.
            2. The first array contains the bin edges.

        """
        return self.values_to_plot, self.bins_to_show

    @time_performance(
        scope="event_timestamp_hist",
        event_name="plot",
    )
    def plot(self, width: float = 6.0, height: float = 4.5) -> matplotlib.axes.Axesne:
        """
        Create a sns.histplot based on the calculated values.

        Parameters
        ----------
        width : float, default 6.0
            Width in inches.
        height : float, default 4.5
            Height in inches.

        Returns
        -------
        :matplotlib_axes:`matplotlib.axes.Axes<>`
            The matplotlib axes containing the plot.

        """

        figsize = (width, height)
        plt.figure(figsize=figsize)

        hist = sns.histplot(self.values_to_plot, bins=self.bins)
        hist.set_title("Event timestamp histogram")

        collect_data_performance(
            scope="event_timestamp_hist",
            event_name="metadata",
            called_params={"width": width, "height": height},
            performance_data={},
            eventstream_index=self.__eventstream._eventstream_index,
        )

        return hist
