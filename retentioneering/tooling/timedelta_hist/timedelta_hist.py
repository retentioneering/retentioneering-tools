from __future__ import annotations

import warnings
from typing import Literal, Optional

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from retentioneering.constants import DATETIME_UNITS
from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.constants import BINS_ESTIMATORS
from retentioneering.tooling.timedelta_hist.constants import (
    AGGREGATION_NAMES,
    EVENTSTREAM_GLOBAL_EVENTS,
)


class TimedeltaHist:
    """
    Plot the distribution of the time deltas between two events. Support various
    distribution types, such as distribution of time for adjacent consecutive events, or
    for a pair of pre-defined events, or median transition time from event to event per user/session.

    Parameters
    ----------
    raw_events_only : bool, default True
        If ``True`` - statistics will be shown only for raw events.
        If ``False`` - statistics will be shown for all events presented in your data.
    event_pair : tuple of str, optional
        Specify an event pair to plot the time distance between. The first
        item corresponds to chronologically first event, the second item corresponds to the second event. If
        ``event_pair=None``, plot distribution of timedelta for all adjacent events.

        Examples: ('login', 'purchase'); ['start', 'cabinet']

        Besides the generic eventstream events, ``event_pair`` can accept special ``eventstream_start`` and
        ``eventstream_end`` events which denote the first and the last event in the entire eventstream correspondingly.

        Note that the sequence of events and ``weight_col`` is important.

    only_adjacent_event_pairs : bool, default True
        Is used only when ``event_pair`` is not ``None``; specifies whether events need to be
        adjacent to be included.

        For example, if ``event_pair=("login", "purchase")`` and ``only_adjacent_event_pairs=False``,
        then the sequence ("login", "main", "trading", "purchase") will contain a valid pair
        (which is not the case with only_adjacent_event_pairs=True).

    weight_col : str, default 'user_id'
        Specify a unit of observation, inside which time differences will be computed.
        For example:

        - If ``user_id`` - time deltas will be computed only for events inside each user path.
        - If ``session_id`` - the same, but inside each session.

    aggregation : {None, "mean", "median"}, default None
        Specify the aggregation policy for the time distances. Aggregate based on passed ``weight_col``.

        - If ``None`` - no aggregation;
        - ``mean`` and ``median`` plot distributions of ``weight_col`` unit mean or unit ``median`` timedeltas.

        For example, if session id is specified in ``weight_col``, one observation per
        session (for example, session median) will be provided for the histogram.
    timedelta_unit : :numpy_link:`DATETIME_UNITS<>`, default 's'
        Specify units of time differences the histogram should use. Use "s" for seconds, "m" for minutes,
        "h" for hours and "D" for days.
    log_scale: bool | tuple of bool | None, optional

         - If ``True`` - apply log scaling to the ``x`` axis.
         - If tuple of bool - apply log scaling to the (``x``,``y``) axes correspondingly.

    lower_cutoff_quantile : float, optional
        Specify time distance quantile as the lower boundary. The values below the boundary are truncated.
    upper_cutoff_quantile : float, optional
        Specify time distance quantile as the upper boundary. The values above the boundary are truncated.
    bins : int or {"auto", "fd", "doane", "scott", "stone", "rice", "sturges", "sqrt"}, default 20
        Generic bin parameter that can be the name of a reference rule or
        the number of bins. Passed to :numpy_bins_link:`numpy.histogram_bin_edges<>`.
    figsize : tuple of float, default (6.0, 4.5)
        Width, height in inches.

    See Also
    --------
    .UserLifetimeHist : Plot the distribution of user lifetimes.
    .EventTimestampHist : Plot the distribution of events over time.
    .Eventstream.describe : Show general eventstream statistics.
    .Eventstream.describe_events : Show general eventstream events statistics.
    .AddStartEndEvents : Create new synthetic events ``path_start`` and ``path_end`` to each user trajectory.
    .SplitSessions : Create new synthetic events, that divide users’ paths on sessions.
    .LabelCroppedPaths : Create new synthetic event(s) for each user based on the timeout threshold.
    .DropPaths : Filter user paths based on the path length, removing the paths that are shorter than the
                                specified number of events or cut_off.


    Notes
    -----
    See :ref:`Eventstream user guide<eventstream_timedelta_hist>` for the details.
    """

    EVENTSTREAM_START = "eventstream_start"
    EVENTSTREAM_END = "eventstream_end"

    def __init__(
        self,
        eventstream: EventstreamType,
        raw_events_only: bool = False,
        event_pair: Optional[list[str | EVENTSTREAM_GLOBAL_EVENTS]] = None,
        only_adjacent_event_pairs: bool = True,
        weight_col: str = "user_id",
        aggregation: Optional[AGGREGATION_NAMES] = None,
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: bool | tuple[bool, bool] | None = None,
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int | Literal[BINS_ESTIMATORS] = 20,
        figsize: tuple[float, float] = (6.0, 4.5),
    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.type_col = self.__eventstream.schema.event_type
        self.raw_events_only = raw_events_only

        if event_pair is not None:
            if type(event_pair) not in (list, tuple):
                raise TypeError("event_pair should be a tuple or a list of length 2.")
            if len(event_pair) != 2:
                raise ValueError("event_pair should be a tuple or a list of length 2.")

            if set(event_pair) == {self.EVENTSTREAM_START, self.EVENTSTREAM_END}:
                raise ValueError(
                    f"event_pair = ['{self.EVENTSTREAM_START}', '{self.EVENTSTREAM_END}'] "
                    f"is invalid. Only one event of these two events can be a member of the event_pair."
                )
            if set(event_pair) in [{self.EVENTSTREAM_START}, {self.EVENTSTREAM_END}]:
                raise ValueError(
                    f"event_pair = ['{self.EVENTSTREAM_START}', '{self.EVENTSTREAM_END}'] and "
                    f"event_pair = ['{self.EVENTSTREAM_START}', '{self.EVENTSTREAM_END}'] "
                    f"are invalid. Events '{self.EVENTSTREAM_START}' "
                    f"and '{self.EVENTSTREAM_END}' couldn't be doubled."
                )

        self.event_pair = event_pair
        self.only_adjacent_event_pairs = only_adjacent_event_pairs
        self.weight_col = weight_col

        self.aggregation = aggregation
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

        if log_scale:
            if isinstance(log_scale, bool):
                self.log_scale = (log_scale, False)
            else:
                self.log_scale = log_scale
        else:
            self.log_scale = (False, False)
        self.bins = bins
        self.figsize = figsize
        self.bins_to_show: np.ndarray = np.array([])
        self.values_to_plot: np.ndarray = np.array([])

    def _prepare_time_diff(self, data: pd.DataFrame) -> pd.DataFrame:
        if not self.only_adjacent_event_pairs:
            data = data[data[self.event_col].isin(self.event_pair)]  # type: ignore

        weight_col_group = data.groupby([self.weight_col])
        with pd.option_context("mode.chained_assignment", None):
            data["time_passed"] = weight_col_group[self.time_col].diff() / np.timedelta64(1, self.timedelta_unit)  # type: ignore
            if self.event_pair:
                data["prev_event"] = weight_col_group[self.event_col].shift()
                data = data[(data[self.event_col] == self.event_pair[1]) & (data["prev_event"] == self.event_pair[0])]

        return data.dropna(subset="time_passed")  # type: ignore

    def _aggregate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.aggregation is not None:
            data = data.groupby(self.weight_col)["time_passed"].agg(self.aggregation).reset_index()
        return data

    def _remove_cutoff_values(self, series: pd.Series) -> pd.Series:
        idx = [True] * len(series)
        if self.upper_cutoff_quantile is not None:
            idx &= series <= series.quantile(self.upper_cutoff_quantile)
        if self.lower_cutoff_quantile is not None:
            idx &= series >= series.quantile(self.lower_cutoff_quantile)
        return series[idx]

    def _prepare_global_events_diff(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.EVENTSTREAM_START in self.event_pair:  # type: ignore
            global_event_time = data[self.time_col].min()
            global_event = self.EVENTSTREAM_START
        else:
            global_event_time = data[self.time_col].max()
            global_event = self.EVENTSTREAM_END

        global_events = data.groupby([self.weight_col]).first().reset_index().copy()
        global_events[self.time_col] = global_event_time
        global_events[self.event_col] = global_event

        data = data[data[self.event_col].isin(self.event_pair)].copy()  # type: ignore
        data = pd.concat([data, global_events]).sort_values([self.weight_col, self.time_col]).reset_index(drop=True)
        return data

    def fit(self) -> None:
        """
        Calculate values and bins for the histplot.

        Returns
        -------
        None
        """

        data = self.__eventstream.to_dataframe(copy=True)

        if self.raw_events_only:
            data = data[data[self.type_col].isin(["raw"])]
        data = data.sort_values([self.weight_col, self.time_col])

        if self.event_pair is not None and set([self.EVENTSTREAM_START, self.EVENTSTREAM_END]).intersection(
            self.event_pair
        ):
            data = self._prepare_global_events_diff(data)

        data = self._prepare_time_diff(data)

        data = self._aggregate_data(data)
        values_to_plot = data["time_passed"].reset_index(drop=True)
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

    def plot(self) -> matplotlib.axes.Axes:
        """
        Create a sns.histplot based on the calculated values.

        Returns
        -------
        :matplotlib_axes:`matplotlib.axes.Axes<>`
            The matplotlib axes containing the plot.
        """

        plt.subplots(figsize=self.figsize)

        hist = sns.histplot(self.values_to_plot, bins=self.bins, log_scale=self.log_scale)
        hist.set_title(
            f"Timedelta histogram, event pair - {self.event_pair}, weight column - {self.weight_col}"
            f"{', group - ' + self.aggregation if self.aggregation is not None else ''}"
        )
        hist.set_xlabel(f"Time units: {self.timedelta_unit}")
        return hist
