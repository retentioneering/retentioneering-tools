from __future__ import annotations

import pandas as pd
from pydantic import ValidationError

from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from src.tooling.cohorts import Cohorts


class TestCohorts:
    def test_cohorts__matrix_MM(self):
        source_df = pd.DataFrame(
            [
                [1, "event", "raw", "2021-12-28 00:01:00"],
                [2, "event", "raw", "2021-12-28 00:01:00"],
                [1, "event", "raw", "2022-01-03 00:01:00"],
                [2, "event", "raw", "2022-01-04 00:01:00"],
                [3, "event", "raw", "2022-01-05 00:02:00"],
                [4, "event", "raw", "2022-01-06 00:03:00"],
                [5, "event", "raw", "2022-01-07 00:07:00"],
                [6, "event", "raw", "2022-01-08 00:08:00"],
                [1, "event", "raw", "2022-01-13 00:01:00"],
                [2, "event", "raw", "2022-01-15 00:01:00"],
                [1, "event", "raw", "2022-02-03 00:01:00"],
                [2, "event", "raw", "2022-02-04 00:01:00"],
                [3, "event", "raw", "2022-02-05 00:02:00"],
                [4, "event", "raw", "2022-02-06 00:03:00"],
                [5, "event", "raw", "2022-02-07 00:07:00"],
                [6, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-02-06 00:03:00"],
                [8, "event", "raw", "2022-02-07 00:07:00"],
                [9, "event", "raw", "2022-02-08 00:08:00"],
                [10, "event", "raw", "2022-03-03 00:01:00"],
                [2, "event", "raw", "2022-04-04 00:01:00"],
                [3, "event", "raw", "2022-04-05 00:02:00"],
                [7, "event", "raw", "2022-03-06 00:03:00"],
                [8, "event", "raw", "2022-04-07 00:07:00"],
                [9, "event", "raw", "2022-03-08 00:08:00"],
                [10, "event", "raw", "2022-04-03 00:01:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            schema=EventstreamSchema(),
        )

        cohorts = Cohorts(eventstream=source, cohort_start_unit="M", cohort_period=(1, "M"), average=False)
        res = cohorts.cohort_matrix().fillna(-999).reset_index()
        indx = ["2021-12", "2022-01", "2022-02", "2022-03"]

        cols = [0, 1, 2, 3, 4]
        correct_res = [
            [1.0, 1.0, 1.0, -999.0, 0.5],
            [1.0, 1.0, -999.0, 0.25, -999.0],
            [1.0, 0.666667, 0.333333, -999.00, -999.0],
            [1.0, 1.0, -999.0, -999.00, -999.0],
        ]

        correct_res_df = pd.DataFrame(data=correct_res, index=indx, columns=cols)
        correct_res_df.index.name = "CohortGroup"
        correct_res_df.columns.name = "CohortPeriod"

        assert correct_res_df.reset_index().round(2).compare(res.round(2)).shape == (0, 0)

    def test_cohorts__matrix_M2M(self):
        source_df = pd.DataFrame(
            [
                [1, "event", "raw", "2021-12-28 00:01:00"],
                [2, "event", "raw", "2021-12-28 00:01:00"],
                [1, "event", "raw", "2022-01-03 00:01:00"],
                [2, "event", "raw", "2022-01-04 00:01:00"],
                [3, "event", "raw", "2022-01-05 00:02:00"],
                [4, "event", "raw", "2022-01-06 00:03:00"],
                [5, "event", "raw", "2022-01-07 00:07:00"],
                [6, "event", "raw", "2022-01-08 00:08:00"],
                [1, "event", "raw", "2022-01-13 00:01:00"],
                [2, "event", "raw", "2022-01-15 00:01:00"],
                [1, "event", "raw", "2022-02-03 00:01:00"],
                [2, "event", "raw", "2022-02-04 00:01:00"],
                [3, "event", "raw", "2022-02-05 00:02:00"],
                [4, "event", "raw", "2022-02-06 00:03:00"],
                [5, "event", "raw", "2022-02-07 00:07:00"],
                [6, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-02-06 00:03:00"],
                [8, "event", "raw", "2022-02-07 00:07:00"],
                [9, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-03-06 00:03:00"],
                [9, "event", "raw", "2022-03-08 00:08:00"],
                [10, "event", "raw", "2022-03-03 00:01:00"],
                [2, "event", "raw", "2022-04-04 00:01:00"],
                [3, "event", "raw", "2022-04-05 00:02:00"],
                [8, "event", "raw", "2022-04-07 00:07:00"],
                [10, "event", "raw", "2022-04-03 00:01:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            schema=EventstreamSchema(),
        )

        cohorts = Cohorts(eventstream=source, cohort_start_unit="M", cohort_period=(2, "M"), average=False)
        res = cohorts.cohort_matrix().fillna(-999).reset_index()
        indx = ["2021-12", "2022-02"]

        cols = [0, 1, 2]
        correct_res = [[1.0, 1.0, 0.3333], [1.0, 0.5, -999.0]]

        correct_res_df = pd.DataFrame(data=correct_res, index=indx, columns=cols)
        correct_res_df.index.name = "CohortGroup"
        correct_res_df.columns.name = "CohortPeriod"

        assert correct_res_df.reset_index().round(2).compare(res.round(2)).shape == (0, 0)

    def test_cohorts__matrix_D1M(self):
        # @TODO - нужен ли тест на более информативную ошибку? dpanina
        pass

    def test_cohorts__matrix_D30D(self):
        source_df = pd.DataFrame(
            [
                [1, "event", "raw", "2021-12-28 00:01:00"],
                [2, "event", "raw", "2021-12-28 00:01:00"],
                [1, "event", "raw", "2022-01-03 00:01:00"],
                [2, "event", "raw", "2022-01-04 00:01:00"],
                [3, "event", "raw", "2022-01-05 00:02:00"],
                [4, "event", "raw", "2022-01-06 00:03:00"],
                [5, "event", "raw", "2022-01-07 00:07:00"],
                [6, "event", "raw", "2022-01-08 00:08:00"],
                [1, "event", "raw", "2022-01-13 00:01:00"],
                [2, "event", "raw", "2022-01-15 00:01:00"],
                [1, "event", "raw", "2022-02-03 00:01:00"],
                [2, "event", "raw", "2022-02-04 00:01:00"],
                [3, "event", "raw", "2022-02-05 00:02:00"],
                [4, "event", "raw", "2022-02-06 00:03:00"],
                [5, "event", "raw", "2022-02-07 00:07:00"],
                [6, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-02-06 00:03:00"],
                [8, "event", "raw", "2022-02-07 00:07:00"],
                [9, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-03-06 00:03:00"],
                [9, "event", "raw", "2022-03-08 00:08:00"],
                [10, "event", "raw", "2022-03-03 00:01:00"],
                [2, "event", "raw", "2022-04-04 00:01:00"],
                [3, "event", "raw", "2022-04-05 00:02:00"],
                [8, "event", "raw", "2022-04-07 00:07:00"],
                [10, "event", "raw", "2022-04-03 00:01:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            schema=EventstreamSchema(),
        )

        cohorts = Cohorts(eventstream=source, cohort_start_unit="D", cohort_period=(30, "D"), average=False)
        res = cohorts.cohort_matrix().fillna(-999).reset_index()
        indx = ["2021-12-28", "2022-01-27", "2022-02-26"]

        cols = [0, 1, 2, 3]
        correct_res = [[1.0, 1.0, -999.0, 0.3333], [1.0, 0.6667, 0.3333, -999.0], [1.0, 1.0, -999.0, -999.0]]

        correct_res_df = pd.DataFrame(data=correct_res, index=indx, columns=cols)
        correct_res_df.index.name = "CohortGroup"
        correct_res_df.columns.name = "CohortPeriod"

        assert correct_res_df.reset_index().round(2).compare(res.round(2)).shape == (0, 0)

    def test_cohorts__matrix_W4W(self):
        source_df = pd.DataFrame(
            [
                [1, "event", "raw", "2021-12-28 00:01:00"],
                [2, "event", "raw", "2021-12-28 00:01:00"],
                [1, "event", "raw", "2022-01-03 00:01:00"],
                [2, "event", "raw", "2022-01-04 00:01:00"],
                [3, "event", "raw", "2022-01-05 00:02:00"],
                [4, "event", "raw", "2022-01-06 00:03:00"],
                [5, "event", "raw", "2022-01-07 00:07:00"],
                [6, "event", "raw", "2022-01-08 00:08:00"],
                [1, "event", "raw", "2022-01-13 00:01:00"],
                [2, "event", "raw", "2022-01-15 00:01:00"],
                [1, "event", "raw", "2022-02-03 00:01:00"],
                [2, "event", "raw", "2022-02-04 00:01:00"],
                [3, "event", "raw", "2022-02-05 00:02:00"],
                [4, "event", "raw", "2022-02-06 00:03:00"],
                [5, "event", "raw", "2022-02-07 00:07:00"],
                [6, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-02-06 00:03:00"],
                [8, "event", "raw", "2022-02-07 00:07:00"],
                [9, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-03-06 00:03:00"],
                [9, "event", "raw", "2022-03-08 00:08:00"],
                [10, "event", "raw", "2022-03-03 00:01:00"],
                [2, "event", "raw", "2022-04-04 00:01:00"],
                [3, "event", "raw", "2022-04-05 00:02:00"],
                [8, "event", "raw", "2022-04-07 00:07:00"],
                [10, "event", "raw", "2022-04-03 00:01:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            schema=EventstreamSchema(),
        )

        cohorts = Cohorts(eventstream=source, cohort_start_unit="W", cohort_period=(4, "W"), average=False)
        res = cohorts.cohort_matrix().fillna(-999).reset_index()
        indx = ["2021-12-27", "2022-01-24", "2022-02-21"]

        cols = [0, 1, 2, 3]
        correct_res = [[1.0, 1.0, -999.0, 0.3333], [1.0, 0.6667, 0.3333, -999.0], [1.0, 1.0, -999.0, -999.0]]

        correct_res_df = pd.DataFrame(data=correct_res, index=indx, columns=cols)
        correct_res_df.index.name = "CohortGroup"
        correct_res_df.columns.name = "CohortPeriod"

        assert correct_res_df.reset_index().round(2).compare(res.round(2)).shape == (0, 0)

    def test_cohorts__matrix_D4W(self):
        source_df = pd.DataFrame(
            [
                [1, "event", "raw", "2021-12-28 00:01:00"],
                [2, "event", "raw", "2021-12-28 00:01:00"],
                [1, "event", "raw", "2022-01-03 00:01:00"],
                [2, "event", "raw", "2022-01-04 00:01:00"],
                [3, "event", "raw", "2022-01-05 00:02:00"],
                [4, "event", "raw", "2022-01-06 00:03:00"],
                [5, "event", "raw", "2022-01-07 00:07:00"],
                [6, "event", "raw", "2022-01-08 00:08:00"],
                [1, "event", "raw", "2022-01-13 00:01:00"],
                [2, "event", "raw", "2022-01-15 00:01:00"],
                [1, "event", "raw", "2022-02-03 00:01:00"],
                [2, "event", "raw", "2022-02-04 00:01:00"],
                [3, "event", "raw", "2022-02-05 00:02:00"],
                [4, "event", "raw", "2022-02-06 00:03:00"],
                [5, "event", "raw", "2022-02-07 00:07:00"],
                [6, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-02-06 00:03:00"],
                [8, "event", "raw", "2022-02-07 00:07:00"],
                [9, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-03-06 00:03:00"],
                [9, "event", "raw", "2022-03-08 00:08:00"],
                [10, "event", "raw", "2022-03-03 00:01:00"],
                [2, "event", "raw", "2022-04-04 00:01:00"],
                [3, "event", "raw", "2022-04-05 00:02:00"],
                [8, "event", "raw", "2022-04-07 00:07:00"],
                [10, "event", "raw", "2022-04-03 00:01:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            schema=EventstreamSchema(),
        )

        cohorts = Cohorts(eventstream=source, cohort_start_unit="D", cohort_period=(4, "W"), average=False)
        res = cohorts.cohort_matrix().fillna(-999).reset_index()
        indx = ["2021-12-28", "2022-01-25", "2022-02-22"]

        cols = [0, 1, 2, 3]
        correct_res = [[1.0, 1.0, -999.0, 0.3333], [1.0, 0.6667, 0.3333, -999.0], [1.0, 1.0, -999.0, -999.0]]

        correct_res_df = pd.DataFrame(data=correct_res, index=indx, columns=cols)
        correct_res_df.index.name = "CohortGroup"
        correct_res_df.columns.name = "CohortPeriod"

        assert correct_res_df.reset_index().round(2).compare(res.round(2)).shape == (0, 0)

    def test_cohorts__matrix_W30D(self):
        source_df = pd.DataFrame(
            [
                [1, "event", "raw", "2021-12-28 00:01:00"],
                [2, "event", "raw", "2021-12-28 00:01:00"],
                [1, "event", "raw", "2022-01-03 00:01:00"],
                [2, "event", "raw", "2022-01-04 00:01:00"],
                [3, "event", "raw", "2022-01-05 00:02:00"],
                [4, "event", "raw", "2022-01-06 00:03:00"],
                [5, "event", "raw", "2022-01-07 00:07:00"],
                [6, "event", "raw", "2022-01-08 00:08:00"],
                [1, "event", "raw", "2022-01-13 00:01:00"],
                [2, "event", "raw", "2022-01-15 00:01:00"],
                [1, "event", "raw", "2022-02-03 00:01:00"],
                [2, "event", "raw", "2022-02-04 00:01:00"],
                [3, "event", "raw", "2022-02-05 00:02:00"],
                [4, "event", "raw", "2022-02-06 00:03:00"],
                [5, "event", "raw", "2022-02-07 00:07:00"],
                [6, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-02-06 00:03:00"],
                [8, "event", "raw", "2022-02-07 00:07:00"],
                [9, "event", "raw", "2022-02-08 00:08:00"],
                [7, "event", "raw", "2022-03-06 00:03:00"],
                [9, "event", "raw", "2022-03-08 00:08:00"],
                [10, "event", "raw", "2022-03-03 00:01:00"],
                [2, "event", "raw", "2022-04-04 00:01:00"],
                [3, "event", "raw", "2022-04-05 00:02:00"],
                [8, "event", "raw", "2022-04-07 00:07:00"],
                [10, "event", "raw", "2022-04-03 00:01:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(
            raw_data=source_df,
            raw_data_schema=RawDataSchema(
                event_name="event", event_timestamp="timestamp", user_id="user_id", event_type="event_type"
            ),
            schema=EventstreamSchema(),
        )

        cohorts = Cohorts(eventstream=source, cohort_start_unit="W", cohort_period=(30, "D"), average=False)
        res = cohorts.cohort_matrix().fillna(-999).reset_index()
        indx = ["2021-12-27", "2022-01-26", "2022-02-25"]

        cols = [0, 1, 2, 3]
        correct_res = [[1.0, 1.0, -999.0, 0.3333], [1.0, 0.6667, 0.3333, -999.0], [1.0, 1.0, -999.0, -999.0]]

        correct_res_df = pd.DataFrame(data=correct_res, index=indx, columns=cols)
        correct_res_df.index.name = "CohortGroup"
        correct_res_df.columns.name = "CohortPeriod"

        assert correct_res_df.reset_index().round(2).compare(res.round(2)).shape == (0, 0)
