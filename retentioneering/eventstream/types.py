from __future__ import annotations

from abc import abstractmethod
from dataclasses import field
from typing import Any, List, Optional, Protocol, TypedDict, runtime_checkable

import pandas as pd

IndexOrder = List[Optional[str]]


class Relation(TypedDict):
    eventstream: "EventstreamType"
    raw_col: Optional[str]


@runtime_checkable
class EventstreamType(Protocol):
    schema: EventstreamSchemaType
    index_order: IndexOrder
    relations: List[Relation]
    __raw_data_schema: RawDataSchemaType
    __events: pd.DataFrame | pd.Series[Any]

    @abstractmethod
    def copy(self) -> EventstreamType:
        ...

    @abstractmethod
    def append_eventstream(self, eventstream: EventstreamType) -> None:
        ...

    @abstractmethod
    def _join_eventstream(self, eventstream: EventstreamType) -> None:
        ...

    @abstractmethod
    def to_dataframe(self, raw_cols: bool = False, show_deleted: bool = False, copy: bool = False) -> pd.DataFrame:
        ...

    @abstractmethod
    def _get_raw_cols(self) -> list[str]:
        ...

    @abstractmethod
    def _get_relation_cols(self) -> list[str]:
        ...

    @abstractmethod
    def add_custom_col(self, name: str, data: pd.Series[Any] | None) -> None:
        ...

    @abstractmethod
    def _soft_delete(self, events: pd.DataFrame) -> None:
        ...


class EventstreamSchemaType(Protocol):
    custom_cols: List[str] = field(default_factory=list)
    user_id: str = "user_id"
    event_timestamp: str = "event_timestamp"
    event_name: str = "event_name"
    event_index: str = "event_index"
    event_type: str = "event_type"
    event_id: str = "event_id"

    @abstractmethod
    def copy(self) -> EventstreamSchemaType:
        ...

    @abstractmethod
    def is_equal(self, schema: EventstreamSchemaType) -> bool:
        ...

    @abstractmethod
    def get_cols(self) -> list[str]:
        ...

    @abstractmethod
    def to_raw_data_schema(self) -> RawDataSchemaType:
        ...


class RawDataCustomColSchema(TypedDict):
    raw_data_col: str
    custom_col: str


class RawDataSchemaType(Protocol):
    event_name: str = "event"
    event_timestamp: str = "timestamp"
    user_id: str = "user_id"
    event_type: Optional[str] = None
    custom_cols: List[RawDataCustomColSchema] = field(default_factory=list)

    @abstractmethod
    def copy(self) -> RawDataSchemaType:
        ...
