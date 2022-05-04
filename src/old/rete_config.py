# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


from typing import Sequence, List, Mapping, Tuple, Callable, Type, Optional, Union, MutableMapping, MutableSequence, cast
from typing_extensions import Literal, TypedDict
from enum import Enum


EventsOrder = List[Union[str, None]]

class EventstreamSchema():
    event_id: str
    event_type: str
    event_index: str
    event_name: str
    event_timestamp: str
    user_id: str
    custom_cols: List[str]

    def __init__(self,
      event_id: str = "event_id", 
      event_type: str = "event_type",
      event_index: str = "event_index",
      event_name: str = "event_name",
      event_timestamp: str = "event_timestamp",
      user_id: str = "user_id",
      custom_cols: List[str] = []
    ):
        self.event_id = event_id
        self.event_type = event_type
        self.event_index = event_index
        self.event_name = event_name
        self.event_timestamp = event_timestamp
        self.user_id = user_id
        self.custom_cols = custom_cols


DEFAULT_EVENTS_ORDER: EventsOrder = [
    'profile',
    'start',
    'new_user',
    'resume',
    'session_start',
    'group_alias',
    'raw',
    'raw_sleep',
    None,
    'synthetic',
    'synthetic_sleep',
    'positive_target',
    'negative_target',
    'session_end',
    'session_sleep',
    'pause',
    'lost',
    'end',
]


class ReteConfig():
    eventstream_schema: EventstreamSchema
    events_order: EventsOrder

    def __init__(
        self,
        eventstream_schema: EventstreamSchema = EventstreamSchema(), 
        events_order: EventsOrder = DEFAULT_EVENTS_ORDER,
    ):
        self.eventstream_schema = eventstream_schema
        self.events_order = events_order

    def copy(self):
        return ReteConfig(
            eventstream_schema=self.eventstream_schema,
            events_order=self.events_order,
        )
