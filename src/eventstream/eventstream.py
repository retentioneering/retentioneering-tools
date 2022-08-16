# TODO fix me
from __future__ import annotations

import uuid
from typing import List, Optional, TypedDict

import numpy as np
import pandas as pd

from src.utils.list import find_index
from src.utils.pandas import get_merged_col

from .schema import EventstreamSchema, RawDataSchema

IndexOrder = List[Optional[str]]

DEFAULT_INDEX_ORDER: IndexOrder = [
    "profile",
    "start",
    "new_user",
    "resume",
    "session_start",
    "group_alias",
    "raw",
    "raw_sleep",
    None,
    "synthetic",
    "synthetic_sleep",
    "positive_target",
    "negative_target",
    "session_end",
    "session_sleep",
    "pause",
    "lost",
    "end",
]

RAW_COL_PREFIX = "raw_"
DELETE_COL_NAME = "_deleted"

# TODO проработать резервирование колонок


class Relation(TypedDict):
    evenstream: Eventstream
    raw_col: Optional[str]


class Eventstream:
    schema: EventstreamSchema
    index_order: IndexOrder
    relations: List[Relation]
    __raw_data_schema: RawDataSchema
    __events: pd.DataFrame

    def __init__(
        self,
        raw_data_schema: RawDataSchema,
        raw_data: pd.DataFrame,
        schema: EventstreamSchema = EventstreamSchema(),
        prepare: bool = True,
        index_order: IndexOrder = DEFAULT_INDEX_ORDER,
        relations: List[Relation] = [],
    ) -> None:
        self.schema = schema
        self.index_order = index_order
        self.relations = relations
        self.__raw_data_schema = raw_data_schema
        self.__events = self.__prepare_events(raw_data) if prepare else raw_data
        self.index_events()

    def copy(self) -> Eventstream:
        return Eventstream(
            raw_data_schema=self.__raw_data_schema.copy(),
            raw_data=self.__events.copy(),
            schema=self.schema.copy(),
            prepare=False,
            index_order=self.index_order.copy(),
            relations=self.relations.copy(),
        )

    def append_eventstream(self, eventstream: Eventstream) -> None:
        if not self.schema.is_equal(eventstream.schema):
            raise ValueError("invalid schema: joined eventstream")

        curr_events = self.to_dataframe(raw_cols=True, show_deleted=True)
        new_events = eventstream.to_dataframe(raw_cols=True, show_deleted=True)

        curr_deleted_events = curr_events[curr_events[DELETE_COL_NAME] == True]  # noqa
        new_deleted_events = new_events[new_events[DELETE_COL_NAME] == True]  # noqa
        deleted_events = curr_deleted_events.append(new_deleted_events).drop_duplicates(  # type: ignore
            subset=[self.schema.event_id]
        )

        merged_events = pd.merge(  # type: ignore
            curr_events,
            new_events,
            left_on=self.schema.event_id,
            right_on=self.schema.event_id,
            how="outer",
            indicator=True,
        )

        left_events = merged_events[(merged_events["_merge"] == "left_only") | (merged_events["_merge"] == "both")]
        right_events = merged_events[(merged_events["_merge"] == "right_only")]

        left_raw_cols = self.get_raw_cols()
        right_raw_cols = eventstream.get_raw_cols()
        cols = self.schema.get_cols()

        result_left_part = pd.DataFrame()
        result_right_part = pd.DataFrame()

        for col in cols:
            result_left_part[col] = get_merged_col(df=left_events, colname=col, suffix="_x")
            result_right_part[col] = get_merged_col(df=right_events, colname=col, suffix="_y")

        for col in left_raw_cols:
            result_left_part[col] = get_merged_col(df=left_events, colname=col, suffix="_x")

        for col in right_raw_cols:
            result_right_part[col] = get_merged_col(df=right_events, colname=col, suffix="_y")

        result_left_part[DELETE_COL_NAME] = get_merged_col(df=left_events, colname=DELETE_COL_NAME, suffix="_x")
        result_right_part[DELETE_COL_NAME] = get_merged_col(df=right_events, colname=DELETE_COL_NAME, suffix="_y")

        self.__events = pd.concat([result_left_part, result_right_part])
        self.soft_delete(deleted_events)
        self.index_events()

    def join_eventstream(self, eventstream: Eventstream) -> None:
        if not self.schema.is_equal(eventstream.schema):
            raise ValueError("invalid schema: joined eventstream")

        relation_i = find_index(
            l=eventstream.relations,
            cond=lambda rel: rel["evenstream"] == self,
        )

        if relation_i == -1:
            raise ValueError("relation not found!")

        relation_col_name = f"ref_{relation_i}"

        curr_events = self.to_dataframe(raw_cols=True, show_deleted=True)
        joined_events = eventstream.to_dataframe(raw_cols=True, show_deleted=True)
        not_related_events = joined_events[joined_events[relation_col_name].isna()]
        not_related_events_ids: pd.Series[str] = not_related_events[self.schema.event_id]

        merged_events = pd.merge(  # type: ignore
            curr_events,
            joined_events,
            left_on=self.schema.event_id,
            right_on=relation_col_name,
            how="outer",
            indicator=True,
        )

        left_id_colname = f"{self.schema.event_id}_y"

        both_events = merged_events[(merged_events["_merge"] == "both")]
        left_events = merged_events[(merged_events["_merge"] == "left_only")]
        right_events = merged_events[
            (merged_events["_merge"] == "both")
            | (merged_events[left_id_colname].isin(not_related_events_ids))  # type: ignore
        ]

        left_raw_cols = self.get_raw_cols()
        right_raw_cols = eventstream.get_raw_cols()
        cols = self.schema.get_cols()

        result_left_part = pd.DataFrame()
        result_right_part = pd.DataFrame()
        result_deleted_events = pd.DataFrame()

        for col in cols:
            result_left_part[col] = get_merged_col(df=left_events, colname=col, suffix="_x")
            result_deleted_events[col] = get_merged_col(df=both_events, colname=col, suffix="_x")
            result_right_part[col] = get_merged_col(df=right_events, colname=col, suffix="_y")

        for col in left_raw_cols:
            result_left_part[col] = get_merged_col(df=left_events, colname=col, suffix="_x")
            result_deleted_events[col] = get_merged_col(df=both_events, colname=col, suffix="_x")

        for col in right_raw_cols:
            result_right_part[col] = get_merged_col(df=right_events, colname=col, suffix="_y")

        result_left_part[DELETE_COL_NAME] = get_merged_col(df=left_events, colname=DELETE_COL_NAME, suffix="_x")

        result_deleted_events[DELETE_COL_NAME] = True

        left_delete_col = f"{DELETE_COL_NAME}_x"
        right_delete_col = f"{DELETE_COL_NAME}_y"
        result_right_part[DELETE_COL_NAME] = right_events[left_delete_col] | right_events[right_delete_col]

        self.__events = result_left_part.append(result_right_part).append(result_deleted_events)  # type: ignore
        self.index_events()

    def to_dataframe(self, raw_cols: bool = False, show_deleted: bool = False) -> pd.DataFrame:
        cols = self.schema.get_cols() + self.get_relation_cols()

        if raw_cols:
            cols += self.get_raw_cols()

        if show_deleted:
            cols.append(DELETE_COL_NAME)

        events = self.__events if show_deleted else self.__get_not_deleted_events()
        view = pd.DataFrame(events, columns=cols)
        return view

    def index_events(self) -> None:
        order_temp_col_name = "order"
        indexed = self.__events

        indexed[order_temp_col_name] = indexed[self.schema.event_type].apply(  # type: ignore
            lambda e: self.__get_event_priority(e)  # type: ignore
        )
        indexed = indexed.sort_values([self.schema.event_timestamp, order_temp_col_name])  # type: ignore
        indexed = indexed.drop([order_temp_col_name], axis=1)
        # indexed[id_col_col_name] = range(1, len(indexed) + 1)
        indexed.reset_index(inplace=True, drop=True)
        indexed[self.schema.event_index] = indexed.index
        self.__events = indexed

    def get_raw_cols(self) -> list[str]:
        cols: pd.Index = self.__events.columns
        raw_cols: list[str] = []
        for col in cols:  # type: ignore
            if col.startswith(RAW_COL_PREFIX):  # type: ignore
                raw_cols.append(col)  # type: ignore
        return raw_cols

    def get_relation_cols(self) -> list[str]:
        cols: pd.Index = self.__events.columns
        relation_cols: list[str] = []
        for col in cols:  # type: ignore
            if col.startswith("ref_"):  # type: ignore
                relation_cols.append(col)  # type: ignore
        return relation_cols

    def soft_delete(self, events: pd.DataFrame) -> None:
        deleted_events = events.copy()
        deleted_events[DELETE_COL_NAME] = True
        merged: pd.DataFrame = pd.merge(  # type: ignore
            left=self.__events,
            right=deleted_events,
            left_on=self.schema.event_id,
            right_on=self.schema.event_id,
            indicator=True,
            how="left",
        )
        self.__events[DELETE_COL_NAME] = self.__events[DELETE_COL_NAME] | merged[f"{DELETE_COL_NAME}_y"] == True  # noqa

    def __get_not_deleted_events(self) -> pd.DataFrame:
        events = self.__events
        return events[events[DELETE_COL_NAME] == False]  # noqa type: ignore

    def __prepare_events(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        events = raw_data.copy()
        # add "raw_" prefix for raw cols
        events.rename(lambda col: f"raw_{col}", axis="columns", inplace=True)

        events[DELETE_COL_NAME] = False
        events[self.schema.event_id] = [uuid.uuid4() for _ in range(len(events))]
        events[self.schema.event_name] = self.__get_col_from_raw_data(
            raw_data=raw_data,
            colname=self.__raw_data_schema.event_name,
        )
        events[self.schema.event_timestamp] = pd.to_datetime(  # type: ignore
            self.__get_col_from_raw_data(  # type: ignore
                raw_data=raw_data,
                colname=self.__raw_data_schema.event_timestamp,
            ),
        )
        events[self.schema.user_id] = self.__get_col_from_raw_data(
            raw_data=raw_data,
            colname=self.__raw_data_schema.user_id,
        )

        if self.__raw_data_schema.event_type is not None:
            events[self.schema.event_type] = self.__get_col_from_raw_data(
                raw_data=raw_data,
                colname=self.__raw_data_schema.event_type,
            )
        else:
            events[self.schema.event_type] = "raw"

        for custom_col_schema in self.__raw_data_schema.custom_cols:
            raw_data_col = custom_col_schema["raw_data_col"]
            custom_col = custom_col_schema["custom_col"]
            if custom_col not in self.schema.custom_cols:
                raise ValueError(f'invald raw data schema. Custom column "{custom_col}" does not exists in schema!')
            events[custom_col] = self.__get_col_from_raw_data(
                raw_data=raw_data,
                colname=raw_data_col,
            )

        for custom_col in self.schema.custom_cols:
            if custom_col in events.columns:
                continue
            events[custom_col] = np.nan

        # add relations
        for i in range(len(self.relations)):
            rel_col_name = f"ref_{i}"
            relation = self.relations[i]
            col = raw_data[relation["raw_col"]] if relation["raw_col"] is not None else np.nan  # type: ignore
            events[rel_col_name] = col

        return events

    def __get_col_from_raw_data(
        self, raw_data: pd.DataFrame, colname: str, create: bool = False
    ) -> pd.Series | float | None:  # type: ignore
        if colname in raw_data.columns:
            return raw_data[colname]  # type: ignore
        else:
            if create:
                return np.nan
            else:
                raise ValueError(f'invald raw data. Column "{colname}" does not exists!')

    def __get_event_priority(self, event_type: Optional[str]) -> int:
        if event_type in self.index_order:
            return self.index_order.index(event_type)
        return 8
