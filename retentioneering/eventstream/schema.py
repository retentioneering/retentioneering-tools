# * Copyright (C) 2020 Maxim Godzi, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

# TODO fix me
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from retentioneering.eventstream.types import (
    EventstreamSchemaType,
    RawDataCustomColSchema,
    RawDataSchemaType,
)


@dataclass
class EventstreamSchema(EventstreamSchemaType):
    """
    Define a schema for ``eventstream`` columns names.
    If names of the columns are different from default names, they need to be
    specified.

    Parameters
    ----------
    event_id : str, default "event_id"
    event_type : str, default "event_type"
    event_index : str, default "event_index"
    event_name : str, default "event"
    event_timestamp : str, default "timestamp"
    user_id : str, default "user_id"
    custom_cols : list of str, optional

    Notes
    -----
    See :ref:`Eventstream user guide<eventstream_field_names>` for the details.

    """

    event_id: str = "event_id"
    event_type: str = "event_type"
    event_index: str = "event_index"
    event_name: str = "event"
    event_timestamp: str = "timestamp"
    user_id: str = "user_id"
    custom_cols: List[str] = field(default_factory=list)

    def copy(self) -> EventstreamSchema:
        return EventstreamSchema(
            event_id=self.event_id,
            event_type=self.event_type,
            event_index=self.event_index,
            event_name=self.event_name,
            event_timestamp=self.event_timestamp,
            user_id=self.user_id,
            custom_cols=self.custom_cols.copy(),
        )

    def is_equal(self, schema: EventstreamSchemaType) -> bool:
        return (
            self.event_id == schema.event_id
            and self.event_type == schema.event_type
            and self.event_index == schema.event_index
            and self.event_name == schema.event_name
            and self.event_timestamp == schema.event_timestamp
            and self.user_id == schema.user_id
            and (set(self.custom_cols).issubset(schema.custom_cols))
        )

    def get_default_cols(self) -> List[str]:
        return [
            self.event_id,
            self.event_type,
            self.event_index,
            self.event_name,
            self.event_timestamp,
            self.user_id,
        ]

    def get_cols(self) -> list[str]:
        return [
            self.event_id,
            self.event_type,
            self.event_index,
            self.event_name,
            self.event_timestamp,
            self.user_id,
        ] + self.custom_cols

    def to_raw_data_schema(self, event_id: bool = False, event_index: bool = False) -> RawDataSchema:
        custom_cols: List[RawDataCustomColSchema] = []

        for col in self.custom_cols:
            custom_cols.append({"custom_col": col, "raw_data_col": col})

        return RawDataSchema(
            event_name=self.event_name,
            event_type=self.event_type,
            event_id=self.event_id if event_id else None,
            event_index=self.event_index if event_index else None,
            user_id=self.user_id,
            event_timestamp=self.event_timestamp,
            custom_cols=custom_cols,
        )


@dataclass
class RawDataSchema(RawDataSchemaType):
    """
    Define schema for ``raw_data`` columns names.
    If names of the columns are different from default names, they need to be
    specified.

    Parameters
    ----------
    event_name : str, default "event"
    event_timestamp : str, default "timestamp"
    user_id : str, default "user_id"
    event_type : str, optional
    event_index: str, optional
    custom_cols : list, optional

    Notes
    -----
    See :ref:`Eventstream user guide<eventstream_raw_data_schema>` for the details.

    """

    event_name: str = "event"
    event_timestamp: str = "timestamp"
    user_id: str = "user_id"
    event_index: Optional[str] = None
    event_type: Optional[str] = None
    event_id: Optional[str] = None
    custom_cols: List[RawDataCustomColSchema] = field(default_factory=list)

    def get_default_cols(self) -> List[str]:
        cols: List[str] = [
            self.event_name,
            self.event_timestamp,
            self.user_id,
        ]

        if self.event_index:
            cols.append(self.event_index)

        if self.event_type:
            cols.append(self.event_type)

        if self.event_id:
            cols.append(self.event_id)

        return cols

    def copy(self) -> RawDataSchema:
        return RawDataSchema(
            event_name=self.event_name,
            event_timestamp=self.event_timestamp,
            user_id=self.user_id,
            custom_cols=self.custom_cols,
            event_type=self.event_type,
            event_index=self.event_index,
            event_id=self.event_id,
        )
