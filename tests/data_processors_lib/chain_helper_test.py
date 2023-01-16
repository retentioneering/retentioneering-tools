from __future__ import annotations

import pandas as pd

from retentioneering.eventstream.eventstream import Eventstream


class TestChainHelper:
    def test_chain_helper(self) -> None:
        source_df = pd.DataFrame(
            [
                [1, "event1", "2022-01-01 00:00:00"],
                [1, "event2", "2022-01-01 00:00:01"],
                [1, "event3", "2022-01-01 00:00:02"],
                [2, "event4", "2022-01-02 00:00:00"],
            ],
            columns=["user_id", "event", "timestamp"],
        )

        correct_result_columns = ["user_id", "event", "event_type", "timestamp"]
        correct_result = pd.DataFrame(
            [
                [1, "path_start", "path_start", "2022-01-01 00:00:00"],
                [1, "new_user", "new_user", "2022-01-01 00:00:00"],
                [1, "event1", "raw", "2022-01-01 00:00:00"],
                [1, "event2", "raw", "2022-01-01 00:00:01"],
                [1, "event3", "raw", "2022-01-01 00:00:02"],
                [1, "path_end", "path_end", "2022-01-01 00:00:02"],
                [2, "path_start", "path_start", "2022-01-02 00:00:00"],
                [2, "new_user", "new_user", "2022-01-02 00:00:00"],
                [2, "event4", "raw", "2022-01-02 00:00:00"],
                [2, "path_end", "path_end", "2022-01-02 00:00:00"],
            ],
            columns=correct_result_columns,
        )

        stream = Eventstream(source_df)

        result = stream.add_start_end().add_new_users(new_users_list="all")
        result_df = result.to_dataframe()[correct_result_columns].reset_index(drop=True)

        assert result_df.compare(correct_result).shape == (0, 0)
