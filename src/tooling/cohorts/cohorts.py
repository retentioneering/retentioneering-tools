from __future__ import annotations

from typing import Literal, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from src.constants import DATETIME_UNITS
from src.eventstream.types import EventstreamType

# @TODO Подумать над сокращением списка поддерживаемых типов для когорт? dpanina


class Cohorts:
    """
    A class which provides methods for cohort analysis. The users are spit into groups
    depending on the time of their first appearance in the eventstream so each user is
    associated with some ``cohort_group``. The retention rate of the active users
    belonging to each ``cohort_group`` is  calculated within each ``cohort_period``.

    Parameters
    ----------
    eventstream : EventstreamType
    cohort_start_unit : :numpy_link:`DATETIME_UNITS<>`
        The way of rounding and format of the moment from which the cohort count begins.
        Minimum timestamp rounding down to the selected datetime unit.

        For example:
        We have eventstream with minimum timestamp - "2021-12-28 09:08:34.432456"
        The result of roundings with different ``DATETIME_UNITS`` is in the table below:

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

        - Start moments for each cohort from the moment defined with the ``cohort_start_unit`` parameter
        - Cohort periods for each cohort from ifs start moment.
    average : bool, default True
        - If ``True`` - calculating average for each cohort period.
        - If ``False`` - averaged values don't calculated.
    cut_bottom : int
        Drop 'n' rows from the bottom of the cohort matrix.
        Average is recalculated.
    cut_right : int
        Drop 'n' columns from the right side of the cohort matrix.
        Average is recalculated.
    cut_diagonal : int
        Replace values in 'n' diagonals (last period-group cells) with ``np.nan``.
        Average is recalculated.

    Notes
    -----
    Parameters ``cohort_start_unit`` and ``cohort_period`` should be consistent.
    Due to "Y" and "M" are non-fixed types it can be used only with each other
    or if ``cohort_period_unit`` is more detailed than ``cohort_start_unit``.
    More information - :numpy_timedelta_link:`about numpy timedelta<>`


    Only cohorts with at least 1 user in any period - are shown.

    See Also
    --------
    :py:func:`src.eventstream.eventstream.Eventstream.cohorts`
    """

    __eventstream: EventstreamType
    cohort_period: int
    cohort_period_unit: DATETIME_UNITS
    cohort_start_unit: DATETIME_UNITS
    DATETIME_UNITS_LIST = ["Y", "M", "W", "D", "h", "m", "s", "ms", "us", "μs", "ns", "ps", "fs", "as"]

    def __init__(
        self,
        eventstream: EventstreamType,
        cohort_start_unit: DATETIME_UNITS,
        cohort_period: Tuple[int, DATETIME_UNITS],
        average: bool = True,
        cut_bottom: int = 0,
        cut_right: int = 0,
        cut_diagonal: int = 0,
    ):

        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.average = average
        self.cohort_start_unit = cohort_start_unit
        self.cohort_period, self.cohort_period_unit = cohort_period
        self.cut_bottom = cut_bottom
        self.cut_right = cut_right
        self.cut_diagonal = cut_diagonal

        data = self.__eventstream.to_dataframe()
        self.data = data
        self._cohort_matrix_result: pd.DataFrame = pd.DataFrame()

        if self.cohort_period <= 0:
            raise ValueError("cohort_period should be positive integer!")

        # @TODO добавить ссылку на numpy с объяснением. dpanina
        if self.cohort_period_unit in ["Y", "M"] and self.cohort_start_unit not in ["Y", "M"]:
            raise ValueError(
                """Parameters ``cohort_start_unit`` and ``cohort_period`` should be consistent.
                                 Due to "Y" and "M" are non-fixed types it can be used only with each other
                                 or if ``cohort_period_unit`` is more detailed than ``cohort_start_unit``!"""
            )

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
        if Cohorts.DATETIME_UNITS_LIST.index(cohort_start_unit) < Cohorts.DATETIME_UNITS_LIST.index(cohort_period_unit):
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

    def fit(self) -> None:
        """
        Calculates the cohort internal values with the defined parameters.
        Applying ``fit`` method is mandatory for the following usage
        of any visualization or descriptive ``Cohorts`` methods.

        """

        df = self._add_min_date(
            data=self.data,
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

    def heatmap(self, figsize: Tuple[float, float] = (10, 10)) -> sns.heatmap:
        """
        Builds a heatmap based on the calculated cohort matrix values.
        Should be used after :py:func:`fit`.

        Parameters
        ----------
        figsize: Tuple[float, float], default (10, 10)
            Is a tuple of the width and height of the figure in inches.

        Returns
        -------
        sns.heatmap
        """

        df = self._cohort_matrix_result

        figure = plt.figure(figsize=figsize)
        sns.heatmap(df, annot=True, fmt=".1%", linewidths=1, linecolor="gray")
        return figure

    def lineplot(
        self,
        show_plot: Literal["cohorts", "average", "all"] = "cohorts",
        figsize: Tuple[float, float] = (10, 10),
    ) -> sns.lineplot:
        """
        Creates a chart representing each cohort dynamics over time.
        Should be used after :py:func:`fit`.

        Parameters
        ----------
        show_plot: 'cohorts', 'average' or 'all'
            - if ``cohorts`` - shows a lineplot for each cohort,
            - if ``average`` - shows a lineplot only for the average values over all the cohorts,
            - if ``all`` - shows a lineplot for each cohort and also for their average values.
        figsize: Tuple[float, float], default (10, 10)
            Is a tuple of the width and height of the figure in inches.

        Returns
        -------
        sns.lineplot

        """
        if show_plot not in ["cohorts", "average", "all"]:
            raise ValueError("show_plot parameter should be 'cohorts', 'average' or 'all'!")

        df_matrix = self._cohort_matrix_result
        df_wo_average = df_matrix[df_matrix.index != "Average"]  # type: ignore
        if show_plot in ["all", "average"] and "Average" not in df_matrix.index:  # type: ignore
            df_matrix.loc["Average"] = df_matrix.mean()  # type: ignore
        df_average = df_matrix[df_matrix.index == "Average"]  # type: ignore
        figure = plt.figure(figsize=figsize)
        if show_plot == "all":
            sns.lineplot(df_wo_average.T, lw=1.5)
            sns.lineplot(df_average.T, lw=2.5, palette=["red"], marker="X", markersize=8, alpha=0.6)

        if show_plot == "average":
            sns.lineplot(df_average.T, lw=2.5, palette=["red"], marker="X", markersize=8, alpha=0.6)

        if show_plot == "cohorts":
            sns.lineplot(df_wo_average.T, lw=1.5)

        plt.legend(loc="upper left", bbox_to_anchor=(1, 1))
        plt.xlabel("Period from the start of observation")
        plt.ylabel("Share of active users")
        return figure

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
