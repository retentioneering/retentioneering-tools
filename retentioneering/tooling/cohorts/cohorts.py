from __future__ import annotations

from typing import Literal, Tuple

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from retentioneering.constants import DATETIME_UNITS, DATETIME_UNITS_LIST
from retentioneering.eventstream.types import EventstreamType

# @TODO Подумать над сокращением списка поддерживаемых типов для когорт? dpanina


class Cohorts:
    """
    A class that provides methods for cohort analysis. The users are split into groups
    depending on the time of their first appearance in the eventstream; thus each user is
    associated with some ``cohort_group``. Retention rates of the active users
    belonging to each ``cohort_group`` are  calculated within each ``cohort_period``.

    Parameters
    ----------
    eventstream : EventstreamType


    See Also
    --------
    .Eventstream.cohorts : Call Cohorts tool as an eventstream method.
    .EventTimestampHist : Plot the distribution of events over time.
    .UserLifetimeHist : Plot the distribution of user lifetimes.

    Notes
    -----
    See :doc:`Cohorts user guide</user_guides/cohorts>` for the details.

    """

    __eventstream: EventstreamType
    cohort_start_unit: DATETIME_UNITS
    cohort_period: int
    cohort_period_unit: DATETIME_UNITS

    average: bool
    cut_bottom: int
    cut_right: int
    cut_diagonal: int

    def __init__(self, eventstream: EventstreamType):
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp

        self._cohort_matrix_result: pd.DataFrame = pd.DataFrame()

    def _add_min_date(
        self,
        data: pd.DataFrame,
        cohort_start_unit: DATETIME_UNITS,
        cohort_period: int,
        cohort_period_unit: DATETIME_UNITS,
    ) -> pd.DataFrame:
        freq = cohort_start_unit
        data["user_min_date_gr"] = data.groupby(self.user_col)[self.time_col].transform(min)
        min_cohort_date = data["user_min_date_gr"].min().to_period(freq).start_time
        max_cohort_date = data["user_min_date_gr"].max()
        if DATETIME_UNITS_LIST.index(cohort_start_unit) < DATETIME_UNITS_LIST.index(cohort_period_unit):
            freq = cohort_period_unit

        if freq == "W":
            freq = "D"

        data["user_min_date_gr"] = data["user_min_date_gr"].dt.to_period(freq)

        step = np.timedelta64(cohort_period, cohort_period_unit)
        start_point = np.datetime64(min_cohort_date, freq)
        end_point = np.datetime64(max_cohort_date, freq) + np.timedelta64(cohort_period, cohort_period_unit)

        coh_groups_start_dates = np.arange(start_point, end_point, step)
        coh_groups_start_dates = pd.to_datetime(coh_groups_start_dates).to_period(freq)
        if max_cohort_date < coh_groups_start_dates[-1].start_time:  # type: ignore
            coh_groups_start_dates = coh_groups_start_dates[:-1]  # type: ignore

        cohorts_list = pd.DataFrame(
            data=coh_groups_start_dates, index=None, columns=["CohortGroup"]  # type: ignore
        ).reset_index()
        cohorts_list.columns = ["CohortGroupNum", "CohortGroup"]  # type: ignore
        cohorts_list["CohortGroupNum"] += 1

        data["OrderPeriod"] = data[self.time_col].dt.to_period(freq)
        start_int = pd.Series(min_cohort_date.to_period(freq=freq)).astype(int)[0]

        converter_freq = np.timedelta64(cohort_period, cohort_period_unit)
        converter_freq_ = converter_freq.astype(f"timedelta64[{freq}]").astype(int)
        data["CohortGroupNum"] = (data["user_min_date_gr"].astype(int) - start_int + converter_freq_) // converter_freq_

        data = data.merge(cohorts_list, on="CohortGroupNum", how="left")

        data["CohortPeriod"] = (
            (data["OrderPeriod"].astype(int) - (data["CohortGroup"].astype(int) + converter_freq_))  # type: ignore
            // converter_freq_
        ) + 1

        return data

    @staticmethod
    def _cut_cohort_matrix(
        df: pd.DataFrame, cut_bottom: int = 0, cut_right: int = 0, cut_diagonal: int = 0
    ) -> pd.DataFrame:
        for row in df.index:
            df.loc[row, max(0, df.loc[row].notna()[::-1].idxmax() + 1 - cut_diagonal) :] = None  # type: ignore

        return df.iloc[: len(df) - cut_bottom, : len(df.columns) - cut_right]

    def fit(
        self,
        cohort_start_unit: DATETIME_UNITS,
        cohort_period: Tuple[int, DATETIME_UNITS],
        average: bool = True,
        cut_bottom: int = 0,
        cut_right: int = 0,
        cut_diagonal: int = 0,
    ) -> None:
        """
        Calculates the cohort internal values with the defined parameters.
        Applying ``fit`` method is necessary for the following usage
        of any visualization or descriptive ``Cohorts`` methods.

        Parameters
        ----------
        cohort_start_unit : :numpy_link:`DATETIME_UNITS<>`
            The way of rounding and formatting of the moment from which the cohort count begins.
            The minimum timestamp is rounded down to the selected datetime unit.

            For example:
            assume we have an eventstream with the following minimum timestamp - "2021-12-28 09:08:34.432456".
            The result of roundings with different ``DATETIME_UNITS`` is shown in the table below:

            +------------------------+-------------------------+
            | **cohort_start_unit**  | **cohort_start_moment** |
            +------------------------+-------------------------+
            | Y                      |  2021-01-01 00:00:00    |
            +------------------------+-------------------------+
            | M                      |  2021-12-01 00:00:00    |
            +------------------------+-------------------------+
            | W                      |  2021-12-27 00:00:00    |
            +------------------------+-------------------------+
            | D                      |  2021-08-28 00:00:00    |
            +------------------------+-------------------------+

        cohort_period : Tuple(int, :numpy_link:`DATETIME_UNITS<>`)
            The cohort_period size and its ``DATETIME_UNIT``. This parameter is used in calculating:

            - Start moments for each cohort from the moment specified with the ``cohort_start_unit`` parameter
            - Cohort periods for each cohort from its start moment.
        average : bool, default True
            - If ``True`` - calculating average for each cohort period.
            - If ``False`` - averaged values aren't calculated.
        cut_bottom : int, default 0
            Drop 'n' rows from the bottom of the cohort matrix.
            Average is recalculated.
        cut_right : int, default 0
            Drop 'n' columns from the right side of the cohort matrix.
            Average is recalculated.
        cut_diagonal : int, default 0
            Replace values in 'n' diagonals (last period-group cells) with ``np.nan``.
            Average is recalculated.

        Notes
        -----
        Parameters ``cohort_start_unit`` and ``cohort_period`` should be consistent.
        Due to "Y" and "M" being non-fixed types, it can be used only with each other
        or if ``cohort_period_unit`` is more detailed than ``cohort_start_unit``.
        More information - :numpy_timedelta_link:`about numpy timedelta<>`

        Only cohorts with at least 1 user in some period are shown.

        See :doc:`Cohorts user guide</user_guides/cohorts>` for the details.

        """
        data = self.__eventstream.to_dataframe()
        self.average = average
        self.cohort_start_unit = cohort_start_unit
        self.cohort_period, self.cohort_period_unit = cohort_period
        self.cut_bottom = cut_bottom
        self.cut_right = cut_right
        self.cut_diagonal = cut_diagonal

        if self.cohort_period <= 0:
            raise ValueError("cohort_period should be positive integer!")

        # @TODO добавить ссылку на numpy с объяснением. dpanina
        if self.cohort_period_unit in ["Y", "M"] and self.cohort_start_unit not in ["Y", "M"]:
            raise ValueError(
                """Parameters ``cohort_start_unit`` and ``cohort_period`` should be consistent.
                                 Due to "Y" and "M" are non-fixed types it can be used only with each other
                                 or if ``cohort_period_unit`` is more detailed than ``cohort_start_unit``!"""
            )

        df = self._add_min_date(
            data=data,
            cohort_start_unit=self.cohort_start_unit,
            cohort_period=self.cohort_period,
            cohort_period_unit=self.cohort_period_unit,
        )

        cohorts = df.groupby(["CohortGroup", "CohortPeriod"])[[self.user_col]].nunique()
        cohorts.reset_index(inplace=True)

        cohorts.rename(columns={self.user_col: "TotalUsers"}, inplace=True)
        cohorts.set_index(["CohortGroup", "CohortPeriod"], inplace=True)
        cohort_group_size = cohorts["TotalUsers"].groupby(level=0).first()
        cohorts.reset_index(inplace=True)
        user_retention = (
            cohorts.pivot(index="CohortPeriod", columns="CohortGroup", values="TotalUsers").divide(
                cohort_group_size, axis=1
            )
        ).T

        user_retention = self._cut_cohort_matrix(
            df=user_retention, cut_diagonal=self.cut_diagonal, cut_bottom=self.cut_bottom, cut_right=self.cut_right
        )
        user_retention.index = user_retention.index.astype(str)
        if self.average:
            user_retention.loc["Average"] = user_retention.mean()

        self._cohort_matrix_result = user_retention

    def heatmap(self, width: float = 5.0, height: float = 5.0) -> matplotlib.axes.Axes:
        """
        Builds a heatmap based on the calculated cohort matrix values.
        Should be used after :py:func:`fit`.

        Parameters
        ----------
        width : float, default 5.0
            Width of the figure in inches.
        height : float, default 5.0
            Height of the figure in inches.


        Returns
        -------
        matplotlib.axes.Axes
        """

        df = self._cohort_matrix_result
        figsize = (width, height)
        figure, ax = plt.subplots(figsize=figsize)
        sns.heatmap(df, annot=True, fmt=".1%", linewidths=1, linecolor="gray", ax=ax)
        return ax

    def lineplot(
        self, plot_type: Literal["cohorts", "average", "all"] = "cohorts", width: float = 7.0, height: float = 5.0
    ) -> matplotlib.axes.Axes:
        """
        Create a chart representing each cohort dynamics over time.
        Should be used after :py:func:`fit`.

        Parameters
        ----------
        plot_type: 'cohorts', 'average' or 'all'
            - if ``cohorts`` - shows a lineplot for each cohort,
            - if ``average`` - shows a lineplot only for the average values over all the cohorts,
            - if ``all`` - shows a lineplot for each cohort and also for their average values.
        width : float, default 7.0
            Width of the figure in inches.
        height : float, default 5.0
            Height of the figure in inches.

        Returns
        -------
        matplotlib.axes.Axes

        """
        if plot_type not in ["cohorts", "average", "all"]:
            raise ValueError("plot_type parameter should be 'cohorts', 'average' or 'all'!")
        figsize = (width, height)
        df_matrix = self._cohort_matrix_result
        df_wo_average = df_matrix[df_matrix.index != "Average"]  # type: ignore
        if plot_type in ["all", "average"] and "Average" not in df_matrix.index:  # type: ignore
            df_matrix.loc["Average"] = df_matrix.mean()  # type: ignore
        df_average = df_matrix[df_matrix.index == "Average"]  # type: ignore
        figure, ax = plt.subplots(figsize=figsize)
        if plot_type == "all":
            sns.lineplot(df_wo_average.T, lw=1.5, ax=ax)
            sns.lineplot(df_average.T, lw=2.5, palette=["red"], marker="X", markersize=8, alpha=0.6, ax=ax)

        if plot_type == "average":
            sns.lineplot(df_average.T, lw=2.5, palette=["red"], marker="X", markersize=8, alpha=0.6, ax=ax)

        if plot_type == "cohorts":
            sns.lineplot(df_wo_average.T, lw=1.5, ax=ax)

        ax.legend(loc="upper left", bbox_to_anchor=(1, 1))
        ax.set_xlabel("Period from the start of observation")
        ax.set_ylabel("Share of active users")
        return ax

    @property
    def values(self) -> pd.DataFrame:
        """
        Returns a pd.DataFrame representing the calculated cohort matrix values.
        Should be used after :py:func:`fit`.

        Returns
        -------
        pd.DataFrame

        """
        return self._cohort_matrix_result

    @property
    def params(self) -> dict[str, DATETIME_UNITS | tuple | bool | int | None]:
        """
        Returns the parameters used for the last fitting.
        Should be used after :py:func:`fit`.

        """
        return {
            "cohort_start_unit": self.cohort_start_unit,
            "cohort_period": (self.cohort_period, self.cohort_period_unit),
            "average": self.average,
            "cut_bottom": self.cut_bottom,
            "cut_right": self.cut_right,
            "cut_diagonal": self.cut_diagonal,
        }
