from __future__ import annotations

import pandas as pd

from retentioneering.data_processors_lib import GroupEvents, GroupEventsParams
from retentioneering.eventstream.eventstream import Eventstream
from retentioneering.eventstream.schema import EventstreamSchema, RawDataSchema
from tests.data_processors_lib.common import ApplyTestBase, GraphTestBase


class TestGroupEvents(ApplyTestBase):
    _Processor = GroupEvents
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

    def test_group_events_apply_1(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"])

        actual = self._apply_dataprocessor(
            params=GroupEventsParams(
                event_name="add_to_cart",
                event_type="group_alias",
                func=_filter,
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

    def test_group_events_apply_1_without_schema(self) -> None:
        def _filter(df: pd.DataFrame):
            return df["event"].isin(["cart_btn_click", "plus_icon_click"])

        actual = self._apply_dataprocessor(
            params=GroupEventsParams(
                event_name="add_to_cart",
                event_type="group_alias",
                func=_filter,
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

    def test_group_events_apply_2_none_grouped(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin([])

        actual = self._apply_dataprocessor(
            params=GroupEventsParams(
                event_name="add_to_cart",
                event_type="group_alias",
                func=_filter,
            ),
        )
        expected = pd.DataFrame(
            [
                [1, "pageview", "raw", "2021-10-26 12:00"],
                [1, "cart_btn_click", "raw", "2021-10-26 12:02"],
                [1, "pageview", "raw", "2021-10-26 12:03"],
                [2, "plus_icon_click", "raw", "2021-10-26 12:04"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_group_events_apply_3_all_grouped(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin(["pageview", "cart_btn_click", "plus_icon_click"])

        actual = self._apply_dataprocessor(
            params=GroupEventsParams(
                event_name="anything",
                event_type="group_alias",
                func=_filter,
            )
        )
        expected = pd.DataFrame(
            [
                [1, "anything", "group_alias", "2021-10-26 12:00"],
                [1, "anything", "group_alias", "2021-10-26 12:02"],
                [1, "anything", "group_alias", "2021-10-26 12:03"],
                [2, "anything", "group_alias", "2021-10-26 12:04"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestGroupEventsGraph(GraphTestBase):
    _Processor = GroupEvents
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

    def test_group_events_graph_1(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return (df[schema.user_id].isin([2])) | (df.event.str.contains("cart_btn_click"))

        original, actual = self._apply(
            GroupEventsParams(
                event_name="event_new",
                event_type="group_alias",
                func=_filter,
            ),
            return_with_original=True,
        )
        expected = pd.DataFrame(
            [
                [1, "pageview", "raw", "2021-10-26 12:00"],
                [1, "event_new", "group_alias", "2021-10-26 12:02"],
                [1, "pageview", "raw", "2021-10-26 12:03"],
                [2, "event_new", "group_alias", "2021-10-26 12:04"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_group_events_graph_2_none_grouped(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin([])

        original, actual = self._apply(
            GroupEventsParams(
                event_name="event_new",
                event_type="group_alias",
                func=_filter,
            ),
            return_with_original=True,
        )
        expected = pd.DataFrame(
            [
                [1, "pageview", "raw", "2021-10-26 12:00"],
                [1, "cart_btn_click", "raw", "2021-10-26 12:02"],
                [1, "pageview", "raw", "2021-10-26 12:03"],
                [2, "plus_icon_click", "raw", "2021-10-26 12:04"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_group_events_graph_3_all_grouped(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin(["pageview", "cart_btn_click", "plus_icon_click"])

        original, actual = self._apply(
            GroupEventsParams(
                event_name="anything",
                event_type="group_alias",
                func=_filter,
            ),
            return_with_original=True,
        )
        expected = pd.DataFrame(
            [
                [1, "anything", "group_alias", "2021-10-26 12:00"],
                [1, "anything", "group_alias", "2021-10-26 12:02"],
                [1, "anything", "group_alias", "2021-10-26 12:03"],
                [2, "anything", "group_alias", "2021-10-26 12:04"],
            ],
            columns=["user_id", "event", "event_type", "timestamp"],
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)
