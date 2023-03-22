from __future__ import annotations

import pandas as pd
import pytest
from pandas.core.common import flatten
from pydantic import ValidationError

from retentioneering.eventstream import Eventstream, EventstreamSchema, RawDataSchema
from retentioneering.tooling.funnel import Funnel


class TestFunnel:
    def test_funnel__open(self):
        source_df = pd.DataFrame(
            [
                # открытая, закрытая, закрытая+
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "catalog", "raw", "2022-01-01 00:01:00"],
                [1, "product1", "raw", "2022-01-01 00:02:00"],
                [1, "product2", "raw", "2022-01-01 00:03:00"],
                [1, "cart", "raw", "2022-01-01 00:07:00"],
                [1, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [2, "start", "start", "2022-02-01 00:01:00"],
                [2, "product1", "raw", "2022-02-01 00:01:00"],
                [2, "product2", "raw", "2022-02-01 00:02:00"],
                [2, "cart", "raw", "2022-02-01 00:07:00"],
                [2, "payment_done", "raw", "2022-02-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [3, "start", "start", "2022-01-01 00:01:00"],
                [3, "product1", "raw", "2022-01-01 00:01:00"],
                [3, "product2", "raw", "2022-01-01 00:02:00"],
                [3, "catalog", "raw", "2022-01-01 00:03:00"],
                [3, "catalog", "raw", "2022-01-01 00:04:00"],
                [3, "product18", "raw", "2022-01-01 00:06:30"],
                [3, "cart", "raw", "2022-01-01 00:07:00"],
                [3, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая, закрытая+
                [4, "start", "start", "2022-01-01 00:01:00"],
                [4, "catalog", "raw", "2022-01-01 00:01:00"],
                [4, "product1", "raw", "2022-01-01 00:02:00"],
                [4, "product2", "raw", "2022-01-01 00:03:00"],
                [4, "catalog", "raw", "2022-01-01 00:04:00"],
                [4, "product1", "raw", "2022-01-01 00:06:00"],
                [4, "product18", "raw", "2022-01-01 00:06:30"],
                [4, "cart", "raw", "2022-01-01 00:07:00"],
                [4, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [5, "start", "start", "2022-01-01 00:01:00"],
                [5, "product1", "raw", "2022-01-01 00:02:00"],
                [5, "product2", "raw", "2022-01-01 00:03:00"],
                [5, "product1", "raw", "2022-01-01 00:06:00"],
                [5, "product18", "raw", "2022-01-01 00:06:30"],
                [5, "cart", "raw", "2022-01-01 00:07:00"],
                [5, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [6, "start", "start", "2022-01-01 00:01:00"],
                [6, "product1", "raw", "2022-01-01 00:01:00"],
                [6, "product2", "raw", "2022-01-01 00:02:00"],
                [6, "catalog", "raw", "2022-01-01 00:03:00"],
                [6, "catalog", "raw", "2022-01-01 00:04:00"],
                [6, "product18", "raw", "2022-01-01 00:06:30"],
                [6, "cart", "raw", "2022-01-01 00:07:00"],
                [6, "payment_done", "raw", "2022-01-01 00:08:00"],
                #
                [7, "start", "start", "2022-01-01 00:01:00"],
                [7, "catalog", "raw", "2022-01-01 00:01:00"],
                [7, "product1", "raw", "2022-01-01 00:02:00"],
                [7, "product2", "raw", "2022-01-01 00:03:00"],
                [7, "catalog", "raw", "2022-01-01 00:04:00"],
                [7, "product1", "raw", "2022-01-01 00:06:00"],
                [7, "product18", "raw", "2022-01-01 00:06:30"],
                [7, "payment_done", "raw", "2022-01-01 00:07:00"],
                [7, "cart", "raw", "2022-01-01 00:08:00"],
                #
                [8, "start", "start", "2022-01-01 00:01:00"],
                [8, "catalog", "raw", "2022-01-01 00:01:00"],
                [8, "product1", "raw", "2022-01-01 00:02:00"],
                [8, "product2", "raw", "2022-01-01 00:03:00"],
                [8, "catalog", "raw", "2022-01-01 00:04:00"],
                [8, "product1", "raw", "2022-01-01 00:06:00"],
                [8, "product18", "raw", "2022-01-01 00:06:30"],
                [8, "payment_done", "raw", "2022-01-01 00:07:00"],
                [8, "cart", "raw", "2022-01-01 00:08:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(source_df)

        funnel = Funnel(
            eventstream=source, funnel_type="open", stages=["catalog", ["product1", "product2"], "cart", "payment_done"]
        )
        funnel.fit()
        res_dict = funnel.res_dict

        correct_result = {
            "all users": {"stages": ["catalog", "product1 | product2", "cart", "payment_done"], "values": [6, 8, 8, 8]}
        }

        assert correct_result == res_dict

    def test_funnel__open_stage_names(self):
        source_df = pd.DataFrame(
            [
                # открытая, закрытая, закрытая+
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "catalog", "raw", "2022-01-01 00:01:00"],
                [1, "product1", "raw", "2022-01-01 00:02:00"],
                [1, "product2", "raw", "2022-01-01 00:03:00"],
                [1, "cart", "raw", "2022-01-01 00:07:00"],
                [1, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [2, "start", "start", "2022-02-01 00:01:00"],
                [2, "product1", "raw", "2022-02-01 00:01:00"],
                [2, "product2", "raw", "2022-02-01 00:02:00"],
                [2, "cart", "raw", "2022-02-01 00:07:00"],
                [2, "payment_done", "raw", "2022-02-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [3, "start", "start", "2022-01-01 00:01:00"],
                [3, "product1", "raw", "2022-01-01 00:01:00"],
                [3, "product2", "raw", "2022-01-01 00:02:00"],
                [3, "catalog", "raw", "2022-01-01 00:03:00"],
                [3, "catalog", "raw", "2022-01-01 00:04:00"],
                [3, "product18", "raw", "2022-01-01 00:06:30"],
                [3, "cart", "raw", "2022-01-01 00:07:00"],
                [3, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая, закрытая+
                [4, "start", "start", "2022-01-01 00:01:00"],
                [4, "catalog", "raw", "2022-01-01 00:01:00"],
                [4, "product1", "raw", "2022-01-01 00:02:00"],
                [4, "product2", "raw", "2022-01-01 00:03:00"],
                [4, "catalog", "raw", "2022-01-01 00:04:00"],
                [4, "product1", "raw", "2022-01-01 00:06:00"],
                [4, "product18", "raw", "2022-01-01 00:06:30"],
                [4, "cart", "raw", "2022-01-01 00:07:00"],
                [4, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [5, "start", "start", "2022-01-01 00:01:00"],
                [5, "product1", "raw", "2022-01-01 00:02:00"],
                [5, "product2", "raw", "2022-01-01 00:03:00"],
                [5, "product1", "raw", "2022-01-01 00:06:00"],
                [5, "product18", "raw", "2022-01-01 00:06:30"],
                [5, "cart", "raw", "2022-01-01 00:07:00"],
                [5, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [6, "start", "start", "2022-01-01 00:01:00"],
                [6, "product1", "raw", "2022-01-01 00:01:00"],
                [6, "product2", "raw", "2022-01-01 00:02:00"],
                [6, "catalog", "raw", "2022-01-01 00:03:00"],
                [6, "catalog", "raw", "2022-01-01 00:04:00"],
                [6, "product18", "raw", "2022-01-01 00:06:30"],
                [6, "cart", "raw", "2022-01-01 00:07:00"],
                [6, "payment_done", "raw", "2022-01-01 00:08:00"],
                #
                [7, "start", "start", "2022-01-01 00:01:00"],
                [7, "catalog", "raw", "2022-01-01 00:01:00"],
                [7, "product1", "raw", "2022-01-01 00:02:00"],
                [7, "product2", "raw", "2022-01-01 00:03:00"],
                [7, "catalog", "raw", "2022-01-01 00:04:00"],
                [7, "product1", "raw", "2022-01-01 00:06:00"],
                [7, "product18", "raw", "2022-01-01 00:06:30"],
                [7, "payment_done", "raw", "2022-01-01 00:07:00"],
                [7, "cart", "raw", "2022-01-01 00:08:00"],
                #
                [8, "start", "start", "2022-01-01 00:01:00"],
                [8, "catalog", "raw", "2022-01-01 00:01:00"],
                [8, "product1", "raw", "2022-01-01 00:02:00"],
                [8, "product2", "raw", "2022-01-01 00:03:00"],
                [8, "catalog", "raw", "2022-01-01 00:04:00"],
                [8, "product1", "raw", "2022-01-01 00:06:00"],
                [8, "product18", "raw", "2022-01-01 00:06:30"],
                [8, "payment_done", "raw", "2022-01-01 00:07:00"],
                [8, "cart", "raw", "2022-01-01 00:08:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(source_df)

        funnel = Funnel(
            eventstream=source,
            funnel_type="open",
            stages=["catalog", ["product1", "product2"], "cart", "payment_done"],
            stage_names=["catalog", "product", "cart", "payment_done"],
        )

        funnel.fit()
        res_dict = funnel.res_dict

        correct_result = {
            "all users": {"stages": ["catalog", "product", "cart", "payment_done"], "values": [6, 8, 8, 8]}
        }

        assert correct_result == res_dict

    def test_funnel__closed(self):
        source_df = pd.DataFrame(
            [
                # открытая, закрытая, закрытая+
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "catalog", "raw", "2022-01-01 00:01:00"],
                [1, "product1", "raw", "2022-01-01 00:02:00"],
                [1, "product2", "raw", "2022-01-01 00:03:00"],
                [1, "cart", "raw", "2022-01-01 00:07:00"],
                [1, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [2, "start", "start", "2022-02-01 00:01:00"],
                [2, "product1", "raw", "2022-02-01 00:01:00"],
                [2, "product2", "raw", "2022-02-01 00:02:00"],
                [2, "cart", "raw", "2022-02-01 00:07:00"],
                [2, "payment_done", "raw", "2022-02-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [3, "start", "start", "2022-01-01 00:01:00"],
                [3, "product1", "raw", "2022-01-01 00:01:00"],
                [3, "product2", "raw", "2022-01-01 00:02:00"],
                [3, "catalog", "raw", "2022-01-01 00:03:00"],
                [3, "catalog", "raw", "2022-01-01 00:04:00"],
                [3, "product18", "raw", "2022-01-01 00:06:30"],
                [3, "cart", "raw", "2022-01-01 00:07:00"],
                [3, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая, закрытая+
                [4, "start", "start", "2022-01-01 00:01:00"],
                [4, "catalog", "raw", "2022-01-01 00:01:00"],
                [4, "product1", "raw", "2022-01-01 00:02:00"],
                [4, "product2", "raw", "2022-01-01 00:03:00"],
                [4, "catalog", "raw", "2022-01-01 00:04:00"],
                [4, "product1", "raw", "2022-01-01 00:06:00"],
                [4, "product18", "raw", "2022-01-01 00:06:30"],
                [4, "cart", "raw", "2022-01-01 00:07:00"],
                [4, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [5, "start", "start", "2022-01-01 00:01:00"],
                [5, "product1", "raw", "2022-01-01 00:02:00"],
                [5, "product2", "raw", "2022-01-01 00:03:00"],
                [5, "product1", "raw", "2022-01-01 00:06:00"],
                [5, "product18", "raw", "2022-01-01 00:06:30"],
                [5, "cart", "raw", "2022-01-01 00:07:00"],
                [5, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [6, "start", "start", "2022-01-01 00:01:00"],
                [6, "product1", "raw", "2022-01-01 00:01:00"],
                [6, "product2", "raw", "2022-01-01 00:02:00"],
                [6, "catalog", "raw", "2022-01-01 00:03:00"],
                [6, "catalog", "raw", "2022-01-01 00:04:00"],
                [6, "product18", "raw", "2022-01-01 00:06:30"],
                [6, "cart", "raw", "2022-01-01 00:07:00"],
                [6, "payment_done", "raw", "2022-01-01 00:08:00"],
                #
                [7, "start", "start", "2022-01-01 00:01:00"],
                [7, "catalog", "raw", "2022-01-01 00:01:00"],
                [7, "product1", "raw", "2022-01-01 00:02:00"],
                [7, "product2", "raw", "2022-01-01 00:03:00"],
                [7, "catalog", "raw", "2022-01-01 00:04:00"],
                [7, "product1", "raw", "2022-01-01 00:06:00"],
                [7, "product18", "raw", "2022-01-01 00:06:30"],
                [7, "payment_done", "raw", "2022-01-01 00:07:00"],
                [7, "cart", "raw", "2022-01-01 00:08:00"],
                #
                [8, "start", "start", "2022-01-01 00:01:00"],
                [8, "catalog", "raw", "2022-01-01 00:01:00"],
                [8, "product1", "raw", "2022-01-01 00:02:00"],
                [8, "product2", "raw", "2022-01-01 00:03:00"],
                [8, "catalog", "raw", "2022-01-01 00:04:00"],
                [8, "product1", "raw", "2022-01-01 00:06:00"],
                [8, "product18", "raw", "2022-01-01 00:06:30"],
                [8, "payment_done", "raw", "2022-01-01 00:07:00"],
                [8, "cart", "raw", "2022-01-01 00:08:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(source_df)

        funnel = Funnel(
            eventstream=source,
            stages=["catalog", ["product1", "product2"], "cart", "payment_done"],
            funnel_type="closed",
        )
        funnel.fit()
        res_dict = funnel.res_dict

        correct_result = {
            "all users": {"stages": ["catalog", "product1 | product2", "cart", "payment_done"], "values": [6, 4, 4, 4]}
        }

        assert correct_result == res_dict

    def test_funnel__closed(self):
        source_df = pd.DataFrame(
            [
                # открытая, закрытая, закрытая+
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "catalog", "raw", "2022-01-01 00:01:00"],
                [1, "product1", "raw", "2022-01-01 00:02:00"],
                [1, "product2", "raw", "2022-01-01 00:03:00"],
                [1, "cart", "raw", "2022-01-01 00:07:00"],
                [1, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [2, "start", "start", "2022-02-01 00:01:00"],
                [2, "product1", "raw", "2022-02-01 00:01:00"],
                [2, "product2", "raw", "2022-02-01 00:02:00"],
                [2, "cart", "raw", "2022-02-01 00:07:00"],
                [2, "payment_done", "raw", "2022-02-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [3, "start", "start", "2022-01-01 00:01:00"],
                [3, "product1", "raw", "2022-01-01 00:01:00"],
                [3, "product2", "raw", "2022-01-01 00:02:00"],
                [3, "catalog", "raw", "2022-01-01 00:03:00"],
                [3, "catalog", "raw", "2022-01-01 00:04:00"],
                [3, "product18", "raw", "2022-01-01 00:06:30"],
                [3, "cart", "raw", "2022-01-01 00:07:00"],
                [3, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая, закрытая+
                [4, "start", "start", "2022-01-01 00:01:00"],
                [4, "catalog", "raw", "2022-01-01 00:01:00"],
                [4, "product1", "raw", "2022-01-01 00:02:00"],
                [4, "product2", "raw", "2022-01-01 00:03:00"],
                [4, "catalog", "raw", "2022-01-01 00:04:00"],
                [4, "product1", "raw", "2022-01-01 00:06:00"],
                [4, "product18", "raw", "2022-01-01 00:06:30"],
                [4, "cart", "raw", "2022-01-01 00:07:00"],
                [4, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [5, "start", "start", "2022-01-01 00:01:00"],
                [5, "product1", "raw", "2022-01-01 00:02:00"],
                [5, "product2", "raw", "2022-01-01 00:03:00"],
                [5, "product1", "raw", "2022-01-01 00:06:00"],
                [5, "product18", "raw", "2022-01-01 00:06:30"],
                [5, "cart", "raw", "2022-01-01 00:07:00"],
                [5, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [6, "start", "start", "2022-01-01 00:01:00"],
                [6, "product1", "raw", "2022-01-01 00:01:00"],
                [6, "product2", "raw", "2022-01-01 00:02:00"],
                [6, "catalog", "raw", "2022-01-01 00:03:00"],
                [6, "catalog", "raw", "2022-01-01 00:04:00"],
                [6, "product18", "raw", "2022-01-01 00:06:30"],
                [6, "cart", "raw", "2022-01-01 00:07:00"],
                [6, "payment_done", "raw", "2022-01-01 00:08:00"],
                #
                [7, "start", "start", "2022-01-01 00:01:00"],
                [7, "catalog", "raw", "2022-01-01 00:01:00"],
                [7, "product1", "raw", "2022-01-01 00:02:00"],
                [7, "product2", "raw", "2022-01-01 00:03:00"],
                [7, "catalog", "raw", "2022-01-01 00:04:00"],
                [7, "product1", "raw", "2022-01-01 00:06:00"],
                [7, "product18", "raw", "2022-01-01 00:06:30"],
                [7, "payment_done", "raw", "2022-01-01 00:07:00"],
                [7, "cart", "raw", "2022-01-01 00:08:00"],
                #
                [8, "start", "start", "2022-01-01 00:01:00"],
                [8, "catalog", "raw", "2022-01-01 00:01:00"],
                [8, "product1", "raw", "2022-01-01 00:02:00"],
                [8, "product2", "raw", "2022-01-01 00:03:00"],
                [8, "catalog", "raw", "2022-01-01 00:04:00"],
                [8, "product1", "raw", "2022-01-01 00:06:00"],
                [8, "product18", "raw", "2022-01-01 00:06:30"],
                [8, "payment_done", "raw", "2022-01-01 00:07:00"],
                [8, "cart", "raw", "2022-01-01 00:08:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(source_df)

        funnel = Funnel(
            eventstream=source,
            stages=["catalog", ["product1", "product2"], "cart", "payment_done"],
            stage_names=None,
            funnel_type="closed",
        )

        funnel.fit()
        res_dict = funnel.res_dict

        correct_result = {
            "all users": {"stages": ["catalog", "product1 | product2", "cart", "payment_done"], "values": [6, 4, 4, 2]}
        }

        assert correct_result == res_dict

    def test_funnel__hybrid_segments(self):
        source_df = pd.DataFrame(
            [
                # открытая, закрытая, закрытая+
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "catalog", "raw", "2022-01-01 00:01:00"],
                [1, "product1", "raw", "2022-01-01 00:02:00"],
                [1, "product2", "raw", "2022-01-01 00:03:00"],
                [1, "cart", "raw", "2022-01-01 00:07:00"],
                [1, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [2, "start", "start", "2022-02-01 00:01:00"],
                [2, "product1", "raw", "2022-02-01 00:01:00"],
                [2, "product2", "raw", "2022-02-01 00:02:00"],
                [2, "cart", "raw", "2022-02-01 00:07:00"],
                [2, "payment_done", "raw", "2022-02-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [3, "start", "start", "2022-01-01 00:01:00"],
                [3, "product1", "raw", "2022-01-01 00:01:00"],
                [3, "product2", "raw", "2022-01-01 00:02:00"],
                [3, "catalog", "raw", "2022-01-01 00:03:00"],
                [3, "catalog", "raw", "2022-01-01 00:04:00"],
                [3, "product18", "raw", "2022-01-01 00:06:30"],
                [3, "cart", "raw", "2022-01-01 00:07:00"],
                [3, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая, закрытая+
                [4, "start", "start", "2022-01-01 00:01:00"],
                [4, "catalog", "raw", "2022-01-01 00:01:00"],
                [4, "product1", "raw", "2022-01-01 00:02:00"],
                [4, "product2", "raw", "2022-01-01 00:03:00"],
                [4, "catalog", "raw", "2022-01-01 00:04:00"],
                [4, "product1", "raw", "2022-01-01 00:06:00"],
                [4, "product18", "raw", "2022-01-01 00:06:30"],
                [4, "cart", "raw", "2022-01-01 00:07:00"],
                [4, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [5, "start", "start", "2022-01-01 00:01:00"],
                [5, "product1", "raw", "2022-01-01 00:02:00"],
                [5, "product2", "raw", "2022-01-01 00:03:00"],
                [5, "product1", "raw", "2022-01-01 00:06:00"],
                [5, "product18", "raw", "2022-01-01 00:06:30"],
                [5, "cart", "raw", "2022-01-01 00:07:00"],
                [5, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [6, "start", "start", "2022-01-01 00:01:00"],
                [6, "product1", "raw", "2022-01-01 00:01:00"],
                [6, "product2", "raw", "2022-01-01 00:02:00"],
                [6, "catalog", "raw", "2022-01-01 00:03:00"],
                [6, "catalog", "raw", "2022-01-01 00:04:00"],
                [6, "product18", "raw", "2022-01-01 00:06:30"],
                [6, "cart", "raw", "2022-01-01 00:07:00"],
                [6, "payment_done", "raw", "2022-01-01 00:08:00"],
                #
                [7, "start", "start", "2022-01-01 00:01:00"],
                [7, "catalog", "raw", "2022-01-01 00:01:00"],
                [7, "product1", "raw", "2022-01-01 00:02:00"],
                [7, "product2", "raw", "2022-01-01 00:03:00"],
                [7, "catalog", "raw", "2022-01-01 00:04:00"],
                [7, "product1", "raw", "2022-01-01 00:06:00"],
                [7, "product18", "raw", "2022-01-01 00:06:30"],
                [7, "payment_done", "raw", "2022-01-01 00:07:00"],
                [7, "cart", "raw", "2022-01-01 00:08:00"],
                #
                [8, "start", "start", "2022-01-01 00:01:00"],
                [8, "catalog", "raw", "2022-01-01 00:01:00"],
                [8, "product1", "raw", "2022-01-01 00:02:00"],
                [8, "product2", "raw", "2022-01-01 00:03:00"],
                [8, "catalog", "raw", "2022-01-01 00:04:00"],
                [8, "product1", "raw", "2022-01-01 00:06:00"],
                [8, "product18", "raw", "2022-01-01 00:06:30"],
                [8, "payment_done", "raw", "2022-01-01 00:07:00"],
                [8, "cart", "raw", "2022-01-01 00:08:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(source_df)
        conv_users = [1, 2, 3, 7]
        non_conv_users = [4, 5, 6, 8]

        funnel = Funnel(
            eventstream=source,
            stages=["catalog", ["product1", "product2"], "cart", "payment_done"],
            funnel_type="hybrid",
            segments=(conv_users, non_conv_users),
        )

        funnel.fit()
        res_dict = funnel.res_dict

        correct_result = {
            "group 0": {"stages": ["catalog", "product1 | product2", "cart", "payment_done"], "values": [3, 2, 2, 2]},
            "group 1": {"stages": ["catalog", "product1 | product2", "cart", "payment_done"], "values": [3, 2, 2, 2]},
        }
        assert correct_result == res_dict

    def test_funnel__closed_segment_names(self):
        source_df = pd.DataFrame(
            [
                # открытая, закрытая, закрытая+
                [1, "start", "start", "2022-01-01 00:01:00"],
                [1, "catalog", "raw", "2022-01-01 00:01:00"],
                [1, "product1", "raw", "2022-01-01 00:02:00"],
                [1, "product2", "raw", "2022-01-01 00:03:00"],
                [1, "cart", "raw", "2022-01-01 00:07:00"],
                [1, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [2, "start", "start", "2022-02-01 00:01:00"],
                [2, "product1", "raw", "2022-02-01 00:01:00"],
                [2, "product2", "raw", "2022-02-01 00:02:00"],
                [2, "cart", "raw", "2022-02-01 00:07:00"],
                [2, "payment_done", "raw", "2022-02-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [3, "start", "start", "2022-01-01 00:01:00"],
                [3, "product1", "raw", "2022-01-01 00:01:00"],
                [3, "product2", "raw", "2022-01-01 00:02:00"],
                [3, "catalog", "raw", "2022-01-01 00:03:00"],
                [3, "catalog", "raw", "2022-01-01 00:04:00"],
                [3, "product18", "raw", "2022-01-01 00:06:30"],
                [3, "cart", "raw", "2022-01-01 00:07:00"],
                [3, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая, закрытая+
                [4, "start", "start", "2022-01-01 00:01:00"],
                [4, "catalog", "raw", "2022-01-01 00:01:00"],
                [4, "product1", "raw", "2022-01-01 00:02:00"],
                [4, "product2", "raw", "2022-01-01 00:03:00"],
                [4, "catalog", "raw", "2022-01-01 00:04:00"],
                [4, "product1", "raw", "2022-01-01 00:06:00"],
                [4, "product18", "raw", "2022-01-01 00:06:30"],
                [4, "cart", "raw", "2022-01-01 00:07:00"],
                [4, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая
                [5, "start", "start", "2022-01-01 00:01:00"],
                [5, "product1", "raw", "2022-01-01 00:02:00"],
                [5, "product2", "raw", "2022-01-01 00:03:00"],
                [5, "product1", "raw", "2022-01-01 00:06:00"],
                [5, "product18", "raw", "2022-01-01 00:06:30"],
                [5, "cart", "raw", "2022-01-01 00:07:00"],
                [5, "payment_done", "raw", "2022-01-01 00:08:00"],
                # открытая, закрытая - кроме продукта, закрытая+ только каталог
                [6, "start", "start", "2022-01-01 00:01:00"],
                [6, "product1", "raw", "2022-01-01 00:01:00"],
                [6, "product2", "raw", "2022-01-01 00:02:00"],
                [6, "catalog", "raw", "2022-01-01 00:03:00"],
                [6, "catalog", "raw", "2022-01-01 00:04:00"],
                [6, "product18", "raw", "2022-01-01 00:06:30"],
                [6, "cart", "raw", "2022-01-01 00:07:00"],
                [6, "payment_done", "raw", "2022-01-01 00:08:00"],
                #
                [7, "start", "start", "2022-01-01 00:01:00"],
                [7, "catalog", "raw", "2022-01-01 00:01:00"],
                [7, "product1", "raw", "2022-01-01 00:02:00"],
                [7, "product2", "raw", "2022-01-01 00:03:00"],
                [7, "catalog", "raw", "2022-01-01 00:04:00"],
                [7, "product1", "raw", "2022-01-01 00:06:00"],
                [7, "product18", "raw", "2022-01-01 00:06:30"],
                [7, "payment_done", "raw", "2022-01-01 00:07:00"],
                [7, "cart", "raw", "2022-01-01 00:08:00"],
                #
                [8, "start", "start", "2022-01-01 00:01:00"],
                [8, "catalog", "raw", "2022-01-01 00:01:00"],
                [8, "product1", "raw", "2022-01-01 00:02:00"],
                [8, "product2", "raw", "2022-01-01 00:03:00"],
                [8, "catalog", "raw", "2022-01-01 00:04:00"],
                [8, "product1", "raw", "2022-01-01 00:06:00"],
                [8, "product18", "raw", "2022-01-01 00:06:30"],
                [8, "payment_done", "raw", "2022-01-01 00:07:00"],
                [8, "cart", "raw", "2022-01-01 00:08:00"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )

        source = Eventstream(source_df)

        conv_users = [1, 2, 3, 7]
        non_conv_users = [4, 5, 6, 8]

        funnel = Funnel(
            eventstream=source,
            stages=["catalog", ["product1", "product2"], "cart", "payment_done"],
            funnel_type="closed",
            segments=(conv_users, non_conv_users),
            segment_names=["conv_users", "non_conv_users"],
        )

        funnel.fit()
        res_dict = funnel.res_dict

        correct_result = {
            "conv_users": {
                "stages": ["catalog", "product1 | product2", "cart", "payment_done"],
                "values": [3, 2, 2, 1],
            },
            "non_conv_users": {
                "stages": ["catalog", "product1 | product2", "cart", "payment_done"],
                "values": [3, 2, 2, 1],
            },
        }
        assert correct_result == res_dict

    def test_params_model__incorrect_funnel_type(self):
        with pytest.raises(ValueError):
            source_df = pd.DataFrame(
                [
                    # открытая, закрытая, закрытая+
                    [1, "start", "start", "2022-01-01 00:01:00"],
                    [1, "catalog", "raw", "2022-01-01 00:01:00"],
                    [1, "product1", "raw", "2022-01-01 00:02:00"],
                    [1, "product2", "raw", "2022-01-01 00:03:00"],
                    [1, "cart", "raw", "2022-01-01 00:07:00"],
                    [1, "payment_done", "raw", "2022-01-01 00:08:00"],
                ],
                columns=["user_id", "event", "event_type", "timestamp"],
            )

            source = Eventstream(source_df)

            p = Funnel(eventstream=source, stages=["catalog", "cart", "payment_done"], funnel_type="check_me")
