from __future__ import annotations

from abc import abstractmethod
from dataclasses import field
from typing import Any, List, Optional, Protocol, TypedDict

import pandas as pd

IndexOrder = List[Optional[str]]


class EventstreamType(Protocol):
    schema: EventstreamSchemaType
    index_order: IndexOrder
    __raw_data_schema: RawDataSchemaType
    __events: pd.DataFrame | pd.Series[Any]

    @abstractmethod
    def copy(self) -> EventstreamType:
        ...

    @property
    @abstractmethod
    def _eventstream_index(self) -> int:
        ...

    @abstractmethod
    def append_eventstream(self, eventstream: EventstreamType) -> None:
        ...

    @property
    @abstractmethod
    def _hash(self) -> str:
        ...

    @abstractmethod
    def to_dataframe(self, copy: bool = False) -> pd.DataFrame:
        ...

    @abstractmethod
    def add_custom_col(self, name: str, data: pd.Series[Any] | None) -> None:
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
    def get_default_cols(self) -> List[str]:
        ...

    @abstractmethod
    def get_cols(self) -> list[str]:
        ...

    @abstractmethod
    def to_raw_data_schema(self, event_id: bool = False, event_index: bool = False) -> RawDataSchemaType:
        ...


class RawDataCustomColSchema(TypedDict):
    raw_data_col: str
    custom_col: str


class RawDataSchemaType(Protocol):
    event_name: str = "event"
    event_timestamp: str = "timestamp"
    user_id: str = "user_id"
    event_index: Optional[str] = None
    event_type: Optional[str] = None
    event_id: Optional[str] = None
    custom_cols: List[RawDataCustomColSchema] = field(default_factory=list)

    @abstractmethod
    def get_default_cols(self) -> List[str]:
        ...

    @abstractmethod
    def copy(self) -> RawDataSchemaType:
        ...
