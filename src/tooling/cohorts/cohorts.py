from __future__ import annotations

from typing import Literal

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from dateutil.relativedelta import relativedelta

from src.eventstream.types import EventstreamType


class Cohorts:
    __eventstream: EventstreamType

    def __init__(
            self,
            eventstream: EventstreamType,
            average: Literal['all', 'only'] | None = 'all',
            cohort_measure: Literal["Y", 'M', "D"] = "M",
            cohort_group: int = 1,
            cohort_period: int = 1,
            drop_groups_periods: int = 0,
            cut_bottom: int = 0,
            cut_right: int = 0,

    ) -> None:
        self.__eventstream = eventstream
        self.user_col = self.__eventstream.schema.user_id
        self.event_col = self.__eventstream.schema.event_name
        self.time_col = self.__eventstream.schema.event_timestamp
        self.average = average
        self.cohort_measure = cohort_measure
        self.cohort_group = cohort_group
        self.cohort_period = cohort_period
        self.drop_groups_periods = drop_groups_periods
        self.cut_bottom = cut_bottom
        self.cut_right = cut_right
        self.startdates_groups_dict = {}

        data = self.__eventstream.to_dataframe()
        self.data = data

        if self.cohort_group <= 0:
            raise ValueError("cohort_group should be !")

        if self.cohort_period <= 0:
            raise ValueError("cohort_period should be !")

        if self.average not in ["all", "only", None]:
            raise ValueError("average should be 'all', 'only' or None!")

    def cohort_matrix(self) -> pd.DataFrame:

        df = self._add_min_date(data=self.data,
                                cohort_measure=self.cohort_measure,
                                cohort_group=self.cohort_group,
                                cohort_period=self.cohort_period
                                )

        cohorts = df.groupby(['CohortGroup', 'CohortPeriod'])[[self.user_col]].nunique()
        cohorts.reset_index(inplace=True)

        cohorts.rename(columns={self.user_col: 'TotalUsers'}, inplace=True)
        cohorts.set_index(['CohortGroup', 'CohortPeriod'], inplace=True)
        cohort_group_size = cohorts['TotalUsers'].groupby(level=0).first()
        cohorts.reset_index(inplace=True)
        user_retention = (cohorts.pivot('CohortPeriod', 'CohortGroup', 'TotalUsers')
                          .divide(cohort_group_size, axis=1)).T

        user_retention = self._cut_cohort_matrix(df=user_retention,
                                                 drop_groups_periods=self.drop_groups_periods,
                                                 cut_bottom=self.cut_bottom,
                                                 cut_right=self.cut_right)

        if self.average:
            user_retention.loc['Average'] = user_retention.mean()

        return user_retention

    def _add_min_date(self,
                      data: pd.DataFrame,
                      cohort_measure: Literal["Y", 'M', "D"],
                      cohort_group: int,
                      cohort_period: int,
                      ) -> pd.DataFrame:

        periods_dict = {'D': ['days', '%Y-%m-%d'],
                        'M': ['months', '%Y-%m'],
                        'Y': ['years', '%Y']}

        data['user_min_date'] = data.groupby(self.user_col)[self.time_col].transform(min) \
            .apply(lambda x: datetime.strptime(x.strftime(periods_dict[cohort_measure][1]),
                                               periods_dict[cohort_measure][1]))

        data['OrderPeriod'] = data[self.time_col] \
            .apply(lambda x: datetime.strptime(x.strftime(periods_dict[cohort_measure][1]),
                                               periods_dict[cohort_measure][1]))

        self.startdates_groups_dict = self._create_startdates_groups_dict(tmp=data,
                                                                          cohort_measure=self.cohort_measure,
                                                                          cohort_group=self.cohort_group)

        data['CohortGroup'] = data.groupby('user_min_date')['user_min_date'].transform(self._find_coh_group)
        # data['CohortGroup_date'] = data['CohortGroup'] \
        #     .apply(lambda x: datetime.strptime(x,
        #                                        periods_dict[cohort_measure][1]))

        data['CohortPeriod'] = ((data['OrderPeriod'].dt.to_period(cohort_measure).view(int)
                                - (data['user_min_date'].dt.to_period(cohort_measure).view(int)
                                   + cohort_group)) // cohort_period) + 1

        data.loc[data['CohortPeriod'] < 0, 'CohortPeriod'] = 0

        return data

    def _create_startdates_groups_dict(self,
                                       tmp: pd.DataFrame,
                                       cohort_measure: Literal["Y", 'M', "D"],
                                       cohort_group: int
                                       ):

        periods_dict = {'D': ['days', '%Y-%m-%d'],
                        'M': ['months', '%Y-%m'],
                        'Y': ['years', '%Y']}

        end_date = tmp['user_min_date'].max()
        start_date = tmp['user_min_date'].min()

        coh_num = 1
        uom_inside_coh_count = 1
        start_date_coh = start_date.strftime(periods_dict[cohort_measure][1])

        while end_date >= start_date:

            self.startdates_groups_dict[start_date] = [coh_num, start_date_coh]

            start_date = start_date + relativedelta(**{periods_dict[cohort_measure][0]: 1})
            uom_inside_coh_count += 1

            if uom_inside_coh_count > cohort_group:
                coh_num += 1
                uom_inside_coh_count = 1
                start_date_coh = start_date.strftime(periods_dict[cohort_measure][1])

        return self.startdates_groups_dict

    def _cut_cohort_matrix(self,
                           df: pd.DataFrame,
                           drop_groups_periods: int,
                           cut_bottom: int,
                           cut_right: int) -> pd.DataFrame:

        for i in range(len(df)):
            df.iloc[i, max(0, df.iloc[i, ].notna()[::-1].idxmax() + 1 - drop_groups_periods):] = None

        return df.iloc[:len(df) - cut_bottom, :len(df.columns) - cut_right]

    def _find_coh_group(self, x):
        x = x.min()
        return self.startdates_groups_dict.get(x)[1]

    def cohort_heatmap(self, figsize=(10, 10)):

        df = self.cohort_matrix()

        plt.figure(figsize=figsize)
        return sns.heatmap(df,
                           annot=True,
                           fmt='.1%',
                           linewidths=1,
                           linecolor='gray')

    def cohort_lineplot(self, figsize=(10, 10)):

        df_matrix = self.cohort_matrix()

        coh_periods = len(df_matrix.columns)
        plt.figure(figsize=figsize)

        if self.average == 'all' or self.average is None:
            for i in range(coh_periods):
                sns.lineplot(x=df_matrix.columns[:coh_periods - i],
                             y=df_matrix.iloc[i, :coh_periods - i],
                             lw=2, marker="o", markersize=8, palette='pastel')

        if self.average == 'all' or self.average == 'only':
            self.cohort_average_lineplot(df_matrix)

        plt.legend(labels=[i for i in df_matrix.index], loc='upper left', bbox_to_anchor=(1, 1))
        plt.xlabel('Period from the start of observation')
        plt.ylabel('Share of active users')

    def cohort_average_lineplot(self, df_matrix) -> sns.lineplot:
        active_users_share = df_matrix.iloc[-1]

        sns.lineplot(x=df_matrix.columns,
                     y=active_users_share,
                     lw=3,
                     marker="X",
                     markersize=10,
                     color='b')

        plt.xlabel('Period from the start of observation')
        plt.ylabel('Share of active users')
        plt.legend(labels=['average'], loc='upper left', bbox_to_anchor=(1, 1))

    def join_bars(self, figsize=(12, 7)) -> sns.histplot:

        df = self._add_min_date(data=self.data,
                                cohort_measure=self.cohort_measure,
                                cohort_group=self.cohort_group,
                                cohort_period=self.cohort_period
                                )

        df_grouped = df.groupby(self.user_col).first().sort_values('CohortGroup')
        plt.figure(figsize=figsize)
        plt.xticks(rotation=45)
        sns.histplot(df_grouped, x='CohortGroup', hue=None)

        plt.xlabel('Join date')
        plt.ylabel('Number of new users')
