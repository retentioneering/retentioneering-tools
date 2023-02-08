from __future__ import annotations

import warnings
from typing import Literal, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from retentioneering.constants import DATETIME_UNITS
from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.timedelta_hist.constants import (
    AGGREGATION_NAMES,
    EVENTSTREAM_EVENTS,
)


class TimedeltaHist:
    """
    Plot the distribution of the time deltas between two events. Support various
    distribution types, such as distribution of time for adjacent consecutive events, or
    for a pair of pre-defined events, or median transition time from event to event per user/session.

    Parameters
    ----------
    event_pair : tuple of str or {"eventstream_start", "eventstream_end"}, optional
        Specify an event pair to plot the time distance between. The first
        item corresponds to chronologically first event, the second item corresponds to the second event. If
        ``event_pair=None``, plot distribution of timedelta for all adjacent events.

        Examples: ('login', 'purchase'); ['start', 'cabinet']

        If one of elements in ``event_pair`` is ``eventstream_start`` or ``eventstream_end``:

        - | ``eventstream_start`` - timedelta between first event of whole dataset and first specified
          | event inside selected unit of observation will be calculated.
        - | ``eventstream_end`` - timedelta between last event of whole dataset and first specified
          | event inside selected unit of observation will be calculated.

        Note that the sequence of events and ``weight_col`` is important.

    only_adjacent_event_pairs : bool, default True
        Is used only when ``event_pair`` is not ``None``; specifies whether events need to be
        adjacent to be included.

        For example, if ``event_pair=("login", "purchase")`` and ``only_adjacent_event_pairs=False``,
        then the sequence ("login", "main", "trading", "purchase") will contain a valid pair
        (which is not the case with only_adjacent_event_pairs=True)

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
        Specify the units of the time differences the histogram should use. Use "s" for seconds, "m" for minutes,
        "h" for hours and "D" for days.
    log_scale : tuple of bool, default (False, False)
        Apply log scaling to the (``x``, ``y``) axises.
    lower_cutoff_quantile : float, optional
        Specify the time distance quantile as the lower boundary. The values below the boundary are truncated.
    upper_cutoff_quantile : float, optional
        Specify the time distance quantile as the upper boundary. The values above the boundary are truncated.
    bins : int, default "auto"
        Specify the amount of histogram bins.
    """

    EVENTSTREAM_EVENTS_LIST = ["eventstream_start", "eventstream_end"]

    def __init__(
        self,
        eventstream: EventstreamType,
        event_pair: Optional[list[str | Literal[EVENTSTREAM_EVENTS]]] = None,
        only_adjacent_event_pairs: bool = True,
        weight_col: str = "user_id",
        aggregation: Optional[Literal[AGGREGATION_NAMES]] = None,
        timedelta_unit: DATETIME_UNITS = "s",
        log_scale: tuple[bool, bool] = (False, False),
        lower_cutoff_quantile: Optional[float] = None,
        upper_cutoff_quantile: Optional[float] = None,
        bins: int | Literal["auto"] = "auto",
        figsize: tuple[int, int] = (15, 10),
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
            if len(set(event_pair).intersection(TimedeltaHist.EVENTSTREAM_EVENTS_LIST)) == 2:
                raise ValueError("Both events can't be of eventstream_<> type")
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
        self.log_scale = log_scale
        self.bins = bins
        self.figsize = figsize

    def _prepare_event_pair_data(self, data: pd.DataFrame) -> pd.DataFrame:
        if not self.only_adjacent_event_pairs:
            data = data[data[self.event_col].isin(self.event_pair)].copy()  # type: ignore

        data["time_passed"] = data[self.time_col].diff() / np.timedelta64(1, self.timedelta_unit)  # type: ignore
        first_user_id_value = data.loc[data.index[0], "user_id"]  # type: ignore
        shift_series = data[self.weight_col].shift(fill_value=first_user_id_value).copy()
        data["shift_weight_col"] = shift_series
        select_ids = (data[self.event_col] == self.event_pair[1]) & (data[self.event_col].shift() == self.event_pair[0])  # type: ignore
        data = data[select_ids]
        return data

    def _exclude_multiunit_events(self, data: pd.DataFrame) -> pd.DataFrame:
        # removes all diffs where the timedelta is computed between different grouping units
        # e.g. if grouping unit is user, removes all cases where timedelta is between events of different users
        # example: ['user1_action', 'user1_action', 'user2_action'] - diff between middle and last event is excluded
        if self.event_pair:
            data = data[data[self.weight_col] == data["shift_weight_col"]]  # type: ignore
        else:
            data = data[data[self.weight_col] == data[self.weight_col].shift()]  # type: ignore
        return data

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
        if "eventstream_start" in self.event_pair:  # type: ignore
            global_event_time = data[self.time_col].min()
            global_event = "eventstream_start"

        else:
            global_event_time = data[self.time_col].max()
            global_event = "eventstream_end"

        global_events = data.groupby([self.weight_col]).first().reset_index().copy()
        global_events[self.time_col] = global_event_time
        global_events[self.event_col] = global_event

        data = data[data[self.event_col].isin(self.event_pair)].copy()  # type: ignore
        data = pd.concat([data, global_events]).sort_values([self.weight_col, self.time_col]).reset_index(drop=True)
        return data

    @property
    def values(self) -> tuple[np.ndarray, np.ndarray]:
        """

        Returns
        -------
        tuple(np.ndarray, np.ndarray)
            1. Contains the values for histogram
            2. Contains the bin edges
        """
        data = self.__eventstream.to_dataframe().sort_values([self.weight_col, self.time_col])

        if self.event_pair is not None:
            if set(TimedeltaHist.EVENTSTREAM_EVENTS_LIST).intersection(self.event_pair):
                data = self._prepare_global_events_diff(data)
            data = self._prepare_event_pair_data(data)
        else:
            data["time_passed"] = data[self.time_col].diff() / np.timedelta64(1, self.timedelta_unit)  # type: ignore
        data = self._exclude_multiunit_events(data)

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

        return values_to_plot, bins_to_show  # type: ignore

    def plot(self) -> None:
        """
        Creates a sns.histplot based on the calculated values.

        """
        out_hist = self.values[0]
        plt.figure(figsize=self.figsize)
        if self.log_scale[0]:
            plt.xscale("log")
        if self.log_scale[1]:
            plt.yscale("log")
        plt.title(
            f"Timedelta histogram, event pair {self.event_pair}, weight column {self.weight_col}"
            f"{', group ' + self.aggregation if self.aggregation is not None else ''}"
        )
        plt.xlabel(f"Time units: {self.timedelta_unit}")
        sns.histplot(out_hist, bins=self.bins, log_scale=self.log_scale)

        plt.show()
