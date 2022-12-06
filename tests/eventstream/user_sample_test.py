from __future__ import annotations

import math
import os

import pandas as pd

from src.eventstream.eventstream import Eventstream


class UserSampleTest:
    def test_sample_users(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        test_data_dir = os.path.join(current_dir, "../datasets/eventstream/user_sample")
        source_df = pd.read_csv(os.path.join(test_data_dir, "01_five_users_data.csv"))
        user_sample_share = 0.8
        user_sample_size = 3
        es = Eventstream(source_df)
        es_sampled_1 = Eventstream(source_df, user_sample_size=user_sample_share)
        es_sampled_2 = Eventstream(source_df, user_sample_size=user_sample_size)
        df, df_sampled_1, df_sampled_2 = es.to_dataframe(), es_sampled_1.to_dataframe(), es_sampled_2.to_dataframe()
        user_cnt = len(df["user_id"].unique())
        user_cnt_sampled_1 = len(df_sampled_1["user_id"].unique())
        user_cnt_sampled_2 = len(df_sampled_2["user_id"].unique())

        assert math.isclose(user_cnt * user_sample_share, user_cnt_sampled_1, abs_tol=0.51)
        assert math.isclose(user_sample_size, user_cnt_sampled_2, abs_tol=0.51)
