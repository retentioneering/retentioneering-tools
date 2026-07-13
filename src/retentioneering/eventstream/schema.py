from dataclasses import dataclass, field, fields
from difflib import get_close_matches
from typing import List

from retentioneering.exceptions import SchemaConfigError


@dataclass
class EventstreamSchema:
    path_cols: List[str] = field(default_factory=lambda: ["user_id"])
    event_cols: List[str] = field(default_factory=lambda: ["event"])
    timestamp_col: str = "timestamp"
    event_type: str = "event_type"
    index: str = "index"
    subindex: str = "subindex"
    segment_cols: List[str] = field(default_factory=list)
    custom_cols: List[str] | None = None

    def __post_init__(self) -> None:
        if not self.path_cols:
            raise ValueError("EventstreamSchema: path_cols must not be empty")
        if not self.event_cols:
            raise ValueError("EventstreamSchema: event_cols must not be empty")
        all_cols = (
            self.path_cols
            + self.event_cols
            + self.segment_cols
            + (self.custom_cols or [])
        )
        seen: set[str] = set()
        dups: set[str] = set()
        for c in all_cols:
            (dups if c in seen else seen).add(c)
        if dups:
            raise ValueError(
                f"EventstreamSchema: duplicate column names: {sorted(dups)}"
            )

    @property
    def path_col(self):
        return self.path_cols[0]

    @property
    def event_col(self):
        return self.event_cols[0]

    @property
    def public_cols(self):
        return (
            self.path_cols
            + self.event_cols
            + [self.timestamp_col]
            + self.segment_cols
            + (self.custom_cols or [])
        )

    @property
    def cols(self):
        return (
            self.path_cols
            + self.event_cols
            + [self.timestamp_col]
            + self.segment_cols
            + (self.custom_cols or [])
            + [self.event_type, self.index, self.subindex]
        )

    @classmethod
    def from_dict(cls, schema_dict: dict | None) -> "EventstreamSchema":
        schema_dict = schema_dict or {}
        valid_keys = [f.name for f in fields(cls)]
        unknown_keys = [k for k in schema_dict if k not in valid_keys]
        if unknown_keys:
            key = unknown_keys[0]
            suggestions = get_close_matches(key, valid_keys, n=1)
            hint = f" Did you mean '{suggestions[0]}'?" if suggestions else ""
            raise SchemaConfigError(
                f"Unknown schema key '{key}'.{hint} Valid keys: {sorted(valid_keys)}"
            )
        return cls(**schema_dict)

    def copy(self) -> "EventstreamSchema":
        return EventstreamSchema(
            path_cols=self.path_cols.copy(),
            event_cols=self.event_cols.copy(),
            timestamp_col=self.timestamp_col,
            event_type=self.event_type,
            index=self.index,
            subindex=self.subindex,
            segment_cols=self.segment_cols.copy(),
            custom_cols=self.custom_cols.copy()
            if self.custom_cols is not None
            else None,
        )
