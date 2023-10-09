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
from retentioneering.constants import DATETIME_UNITS
from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.constants import BINS_ESTIMATORS


class UserLifetimeHist:
    """

    Plot the distribution of user lifetimes. A ``users lifetime`` is the timedelta between
    the first and the last events of the user.

    Parameters
    ----------
    eventstream : EventstreamType

    See Also
    --------
    .EventTimestampHist : Plot the distribution of events over time.
    .TimedeltaHist : Plot the distribution of the time deltas between two events.
    .Eventstream.describe : Show general eventstream statistics.
    .Eventstream.describe_events : Show general eventstream events statistics.
    .DropPaths : Filter user paths based on the path length, removing the paths that are shorter than the
                               specified number of events or cut_off.

    Notes
    -----
    See :ref:`Eventstream user guide<eventstream_user_lifetime>` for the details.

    """

    __eventstream: EventstreamType
    timedelta_unit: DATETIME_UNITS
    log_scale: bool | tuple[bool, bool] | None
    lower_cutoff_quantile: float | None
    upper_cutoff_quantile: float | None
    bins: int | Literal[BINS_ESTIMATORS]
    bins_to_show: np.ndarray
    values_to_plot: np.ndarray

    @time_performance(
        scope="user_lifetime_hist",
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
        log_scale: bool | tuple[bool, bool] | None = None,
        lower_cutoff_quantile: float | None = None,
        upper_cutoff_quantile: float | None = None,
    ) -> tuple[tuple[bool, bool], float | None, float | None]:
        if lower_cutoff_quantile is not None:
            if not 0 < lower_cutoff_quantile < 1:
                raise ValueError("lower_cutoff_quantile should be a fraction between 0 and 1.")

        if upper_cutoff_quantile is not None:
            if not 0 < upper_cutoff_quantile < 1:
                raise ValueError("upper_cutoff_quantile should be a fraction between 0 and 1.")

        if lower_cutoff_quantile is not None and upper_cutoff_quantile is not None:
            if lower_cutoff_quantile > upper_cutoff_quantile:
                warnings.warn("lower_cutoff_quantile exceeds upper_cutoff_quantile; no data passed to the histogram")

        if log_scale:
            if isinstance(log_scale, bool):
                log_scale = (log_scale, False)
            else:
                log_scale = log_scale
        else:
            log_scale = (False, False)

        return log_scale, upper_cutoff_quantile, lower_cutoff_quantile

    @time_performance(
        scope="user_lifetime_hist",
        event_name="fit",
    )
    def fit(
        self,
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: bool | tuple[bool, bool] | None = None,
        lower_cutoff_quantile: float | None = None,
        upper_cutoff_quantile: float | None = None,
        bins: int | Literal[BINS_ESTIMATORS] = 20,
    ) -> None:
        """
        Calculate values for the histplot.

        Parameters
        ----------
        timedelta_unit : :numpy_link:`DATETIME_UNITS<>`, default 's'
            Specify units of time differences the histogram should use. Use "s" for seconds, "m" for minutes,
            "h" for hours and "D" for days.
        log_scale : bool or tuple of bool, optional

            - If ``True`` - apply log scaling to the ``x`` axis.
            - If tuple of bool - apply log scaling to the (``x``,``y``) axes correspondingly.
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
            "timedelta_unit": timedelta_unit,
            "log_scale": log_scale,
            "lower_cutoff_quantile": lower_cutoff_quantile,
            "upper_cutoff_quantile": upper_cutoff_quantile,
            "bins": bins,
        }
        not_hash_values = ["timedelta_unit"]

        self.log_scale, self.upper_cutoff_quantile, self.lower_cutoff_quantile = self.__validate_input(
            log_scale,
            lower_cutoff_quantile,
            upper_cutoff_quantile,
        )

        self.timedelta_unit = timedelta_unit
        self.bins = bins

        data = self.__eventstream.to_dataframe(copy=True).groupby(self.user_col)[self.time_col].agg(["min", "max"])
        data["time_passed"] = data["max"] - data["min"]
        values_to_plot = (data["time_passed"] / np.timedelta64(1, self.timedelta_unit)).reset_index(  # type: ignore
            drop=True
        )

        if self._remove_cutoff_values:  # type: ignore
            values_to_plot = self._remove_cutoff_values(values_to_plot).to_numpy()
        if self.log_scale[0]:
            log_adjustment = np.timedelta64(100, "ms") / np.timedelta64(1, self.timedelta_unit)
            values_to_plot = np.where(values_to_plot != 0, values_to_plot, values_to_plot + log_adjustment)  # type: ignore
            bins_to_show = np.power(10, np.histogram_bin_edges(np.log10(values_to_plot), bins=self.bins))
        else:
            bins_to_show = np.histogram_bin_edges(values_to_plot, bins=self.bins)
        if len(values_to_plot) == 0:
            bins_to_show = np.array([])

        self.bins_to_show = bins_to_show
        self.values_to_plot = values_to_plot  # type: ignore
        collect_data_performance(
            scope="user_lifetime_hist",
            event_name="metadata",
            called_params=called_params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self.__eventstream._eventstream_index,
        )

    @property
    @time_performance(
        scope="user_lifetime_hist",
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
        scope="user_lifetime_hist",
        event_name="plot",
    )
    def plot(self, width: float = 6.0, height: float = 4.5) -> matplotlib.axes.Axes:
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
        plt.subplots(figsize=figsize)

        hist = sns.histplot(self.values_to_plot, bins=self.bins, log_scale=self.log_scale)
        hist.set_title("User lifetime histogram")
        hist.set_xlabel(f"Time units: {self.timedelta_unit}")

        collect_data_performance(
            scope="event_timestamp_hist",
            event_name="metadata",
            called_params={"width": width, "height": height},
            performance_data={},
            eventstream_index=self.__eventstream._eventstream_index,
        )

        return hist
