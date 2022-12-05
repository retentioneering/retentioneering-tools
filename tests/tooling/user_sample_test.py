from __future__ import annotations

import math
import unittest

import pandas as pd

from src.eventstream.eventstream import DELETE_COL_NAME, Eventstream
from src.eventstream.schema import EventstreamSchema, RawDataSchema
from src.utils import shuffle_df


class EventstreamTest(unittest.TestCase):
    __raw_data: pd.DataFrame
    __raw_data_schema: RawDataSchema

    def setUp(self):
        self.__raw_data = pd.DataFrame(
            [
                {"name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "1"},
                {"name": "click_1", "event_timestamp": "2021-10-26 12:02", "user_id": "1"},
                {"name": "click_2", "event_timestamp": "2021-10-26 12:03", "user_id": "1"},
                {"name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "2"},
                {"name": "click_1", "event_timestamp": "2021-10-26 12:02", "user_id": "2"},
                {"name": "click_2", "event_timestamp": "2021-10-26 12:03", "user_id": "3"},
                {"name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "3"},
                {"name": "click_1", "event_timestamp": "2021-10-26 12:02", "user_id": "3"},
                {"name": "click_2", "event_timestamp": "2021-10-26 12:03", "user_id": "3"},
                {"name": "pageview", "event_timestamp": "2021-10-26 12:00", "user_id": "4"},
                {"name": "click_1", "event_timestamp": "2021-10-26 12:02", "user_id": "5"},
                {"name": "click_2", "event_timestamp": "2021-10-26 12:03", "user_id": "5"},
            ]
        )
        self.__raw_data_schema = RawDataSchema(event_name="name", event_timestamp="event_timestamp", user_id="user_id")

    def test_sample_users(self):
        user_sample_share = 0.8
        es = Eventstream(raw_data_schema=self.__raw_data_schema, raw_data=self.__raw_data, schema=EventstreamSchema())
        es_s = Eventstream(
            raw_data_schema=self.__raw_data_schema,
            raw_data=self.__raw_data,
            schema=EventstreamSchema(),
            user_sample_share=user_sample_share,
        )
        df = es.to_dataframe()
        df_s = es_s.to_dataframe()
        user_cnt = len(df[self.__raw_data_schema.user_id].unique())
        user_cnt_s = len(df_s[self.__raw_data_schema.user_id].unique())
        self.assertTrue(math.isclose(user_cnt * user_sample_share, user_cnt_s, abs_tol=0.51))
