from __future__ import annotations

import pandas as pd

from src.eventstream.schema import (
    RawDataSchema,
    EventstreamSchema,
)
from src.data_processors_lib.rete import (
    FilterEvents,
    FilterEventsParams,
)
from tests.data_processors_lib.common import (
    apply_processor,
    apply_processor_with_graph,
)


class TestFilterEvents:
    _source_df = pd.DataFrame(
        [
            [1, "pageview",        "2021-10-26 12:00"],
            [1, "cart_btn_click",  "2021-10-26 12:02"],
            [1, "pageview",        "2021-10-26 12:03"],
            [2, "plus_icon_click", "2021-10-26 12:04"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def _apply(self, params: FilterEventsParams) -> (pd.DataFrame, pd.DataFrame):
        return apply_processor(
            FilterEvents(params),
            self._source_df,
            raw_data_schema=self._raw_data_schema,
        )

    def test_filter_events_apply_1_some_filtered(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin(["cart_btn_click", "plus_icon_click"])
        original, actual = self._apply(FilterEventsParams(filter=_filter))
        expected = pd.DataFrame([
            [1,   "pageview", "2021-10-26 12:00:00"],
            [1,   "pageview", "2021-10-26 12:03:00"],
        ], columns=["user_id", "event_name", "event_timestamp"]
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_filter_events_apply_2_none_filtered(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin([])
        original, actual = self._apply(FilterEventsParams(filter=_filter))
        expected = pd.DataFrame([
            [1, "pageview",        "2021-10-26 12:00"],
            [1, "cart_btn_click",  "2021-10-26 12:02"],
            [1, "pageview",        "2021-10-26 12:03"],
            [2, "plus_icon_click", "2021-10-26 12:04"],
        ], columns=["user_id", "event_name", "event_timestamp"]
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_filter_events_apply_3_all_filtered(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin(["pageview", "cart_btn_click", "plus_icon_click"])
        original, actual = self._apply(FilterEventsParams(filter=_filter))
        expected = pd.DataFrame([
        ], columns=["user_id", "event_name", "event_timestamp"]
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)


class TestFilterEventsGraph:
    _source_df = pd.DataFrame(
        [
            [1, "pageview",        "2021-10-26 12:00"],
            [1, "cart_btn_click",  "2021-10-26 12:02"],
            [1, "pageview",        "2021-10-26 12:03"],
            [2, "plus_icon_click", "2021-10-26 12:04"],
        ],
        columns=["user_id", "event", "timestamp"],
    )
    _raw_data_schema = RawDataSchema(
        user_id="user_id",
        event_name="event",
        event_timestamp="timestamp",
    )

    def _apply(self, params: FilterEventsParams) -> (pd.DataFrame, pd.DataFrame):
        return apply_processor_with_graph(
            FilterEvents(params),
            self._source_df,
            raw_data_schema=self._raw_data_schema,
        )

    def test_filter_events_graph_1_some_filtered(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return (df[schema.user_id].isin([2])) | (df.event_name.str.contains("cart_btn_click"))
        original, actual = self._apply(FilterEventsParams(filter=_filter))
        expected = pd.DataFrame([
            [1,   "cart_btn_click",        "raw", "2021-10-26 12:02:00"],
            [2,  "plus_icon_click",        "raw", "2021-10-26 12:04:00"],
        ], columns=["user_id", "event_name", "event_type", "event_timestamp"]
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_filter_events_graph_2_none_filtered(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin([])
        original, actual = self._apply(FilterEventsParams(filter=_filter))
        expected = pd.DataFrame([
            [1, "pageview",        "2021-10-26 12:00"],
            [1, "cart_btn_click",  "2021-10-26 12:02"],
            [1, "pageview",        "2021-10-26 12:03"],
            [2, "plus_icon_click", "2021-10-26 12:04"],
        ], columns=["user_id", "event_name", "event_timestamp"]
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)

    def test_filter_events_graph_3_all_filtered(self) -> None:
        def _filter(df: pd.DataFrame, schema: EventstreamSchema):
            return df[schema.event_name].isin(["pageview", "cart_btn_click", "plus_icon_click"])
        original, actual = self._apply(FilterEventsParams(filter=_filter))
        expected = pd.DataFrame([
        ], columns=["user_id", "event_name", "event_timestamp"]
        )
        assert actual[expected.columns].compare(expected).shape == (0, 0)
