from __future__ import annotations

import platform
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from retentioneering import __version__
from retentioneering.backend import counter


@dataclass
class TrackingInfo:
    user_id: str
    event_custom_name: str
    event_name: str
    event_value: str
    params: dict[str, Any]
    scope: str
    event_time: datetime
    jupyter_kernel_id: str
    colab: int
    # marketing_session_type_3_id: str # @FIXME: what is this? Vladimir Makhanov.

    event_date_local: str = ""
    # event_date_moscow: str    # @TODO: implement. Vladimir Makhanov
    event_day_week: int = 0
    event_timestamp: int = 0
    event_timestamp_ms: int = 0
    source: str = "rete_tools_backend"
    version: str = str(__version__)
    os: str = platform.system()
    index: int = 0
    browser: str = ""
    eventstream_index: int | None = None
    parent_eventstream_index: int | None = None
    child_eventstream_index: int | None = None
    account_id: str = ""

    def __post_init__(self) -> None:
        event_timestamp = self.event_time.timestamp()
        tz_string = self.event_time.astimezone().tzname()
        local_printable_date = self.event_time.strftime("%m/%d/%Y %H:%M:%S")
        self.account_id = self.user_id
        self.event_date_local = f"{local_printable_date} GMT{tz_string}"
        self.event_day_week = self.event_time.weekday()
        self.event_timestamp = int(event_timestamp)
        self.event_timestamp_ms = int(event_timestamp * 1000)
        self.user_id = f"{self.user_id}|none|none|none"
        self.index = counter.get_event_index()
