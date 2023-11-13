from __future__ import annotations

from typing import List

import pandas as pd
import pytest

from retentioneering.data_processors_lib import GroupEventsBulk, GroupEventsBulkParams
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import EventstreamSchema, RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestGroupEventsBulk(ApplyTestBase):
    _Processor = GroupEventsBulk
    _source_df = pd.DataFrame(
        [
            [1, "pageview", "2021-10-26 12:00"],
            [1, "cart_btn_click", "2021-10-26 12:02"],
            [1, "pageview", "2021-10-26 12:03"],
            [2, "plus_icon_click", "2021-10-26 12:04"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def test_group_events_bulk_apply_1(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"])

        grouping_rules: List = [
            {
                "event_name": "add_to_cart",
                "event_type": "group_alias",
                "func": lambda df, schema: df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"]),
            },
            {
                "event_name": "page",
                "event_type": "raw",
                "func": lambda df, schema: df[schema.event_name].isin(["pageview", "pageview"]),
            },
        ]

        actual = self._apply_dataprocessor(
            params=GroupEventsBulkParams(
                grouping_rules=grouping_rules,
            ),
        )
        expected = pd.DataFrame(
            [
                [1, "page", "raw", "2021-10-26 12:00"],
                [1, "add_to_cart", "group_alias", "2021-10-26 12:02"],
                [1, "page", "raw", "2021-10-26 12:03"],
                [2, "add_to_cart", "group_alias", "2021-10-26 12:04"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_group_events_apply_2_none_grouped(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin([])

        grouping_rules: List = [
            # first rules
            {
                "event_name": "add_to_cart",
                "event_type": "group_alias",
                "func": lambda df, schema: df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"]),
            },
            # intersection error, no effect
            {
                "event_name": "add_to_cart",
                "event_type": "raw",
                "func": lambda df, schema: df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"]),
            },
            # rule
            {
                "event_name": "add_to_cart_2",
                "event_type": "raw",
                "func": lambda df, schema: df[schema.event_name].isin(["add_to_cart"]),
            },
        ]

        with pytest.raises(ValueError):
            self._apply_dataprocessor(
                params=GroupEventsBulkParams(
                    grouping_rules=grouping_rules,
                ),
            )

        actual = self._apply_dataprocessor(
            params=GroupEventsBulkParams(
                grouping_rules=grouping_rules,
                ignore_intersections=True,
            ),
        )
        expected = pd.DataFrame(
            [
                [1, "pageview", "raw", "2021-10-26 12:00"],
                [1, "add_to_cart_2", "raw", "2021-10-26 12:02"],
                [1, "pageview", "raw", "2021-10-26 12:03"],
                [2, "add_to_cart_2", "raw", "2021-10-26 12:04"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_group_events_pass_as_dict(self) -> None:
        grouping_rules = {
            "add_to_cart": lambda df, schema: df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"]),
        }

        actual = self._apply_dataprocessor(
            params=GroupEventsBulkParams(
                grouping_rules=grouping_rules,
            ),
        )
        expected = pd.DataFrame(
            [
                [1, "pageview", "raw", "2021-10-26 12:00"],
                [1, "add_to_cart", "group_alias", "2021-10-26 12:02"],
                [1, "pageview", "raw", "2021-10-26 12:03"],
                [2, "add_to_cart", "group_alias", "2021-10-26 12:04"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)
