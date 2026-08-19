import warnings
from dataclasses import dataclass, field, fields
from difflib import get_close_matches
from typing import List

from retentioneering.exceptions import SchemaConfigError


@dataclass
class EventstreamSchema:
    """Column roles in an eventstream's frame."""

    path_cols: List[str] = field(default_factory=lambda: ["user_id"])
    event_col: str = "event"
    timestamp_col: str = "timestamp"
    event_type: str = "event_type"
    index: str = "index"
    subindex: str = "subindex"
    segment_cols: List[str] = field(default_factory=list)
    custom_cols: List[str] | None = None

    def __post_init__(self) -> None:
        if not self.path_cols:
            raise ValueError("EventstreamSchema: path_cols must not be empty")
        if not self.event_col:
            raise ValueError("EventstreamSchema: event_col must not be empty")
        all_cols = (
            self.path_cols
            + [self.event_col]
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
    def public_cols(self):
        return (
            self.path_cols
            + [self.event_col]
            + [self.timestamp_col]
            + self.segment_cols
            + (self.custom_cols or [])
        )

    @property
    def cols(self):
        return (
            self.path_cols
            + [self.event_col]
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
        if "event_cols" in schema_dict:
            schema_dict = cls._absorb_event_cols(schema_dict)
            unknown_keys = [k for k in unknown_keys if k != "event_cols"]
        if unknown_keys:
            key = unknown_keys[0]
            suggestions = get_close_matches(key, valid_keys, n=1)
            hint = f" Did you mean '{suggestions[0]}'?" if suggestions else ""
            raise SchemaConfigError(
                f"Unknown schema key '{key}'.{hint} Valid keys: {sorted(valid_keys)}"
            )
        return cls(**schema_dict)

    @staticmethod
    def _absorb_event_cols(schema_dict: dict) -> dict:
        """
        Accept the deprecated `event_cols` list, warning, and fold it into `event_col`.

        A longer list is an error rather than a silent narrowing to its first
        element, which would drop a column the caller declared.
        """
        value = schema_dict["event_cols"]
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, (list, tuple)) or not value:
            raise SchemaConfigError(
                "Schema key 'event_cols' is deprecated in favour of 'event_col' "
                f"(a string); got {value!r}."
            )
        if len(value) > 1:
            raise SchemaConfigError(
                f"Schema key 'event_cols' got {list(value)!r}: an eventstream has "
                f"one event column. Declare it as event_col='{value[0]}' and the "
                f"rest as custom_cols={list(value[1:])!r}."
            )
        if "event_col" in schema_dict and schema_dict["event_col"] != value[0]:
            raise SchemaConfigError(
                f"Schema got both event_col={schema_dict['event_col']!r} and the "
                f"deprecated event_cols={list(value)!r}, naming different columns. "
                f"Keep event_col."
            )
        warnings.warn(
            f"Schema key 'event_cols' is deprecated — use event_col='{value[0]}'.",
            FutureWarning,
            stacklevel=4,
        )
        schema_dict = {k: v for k, v in schema_dict.items() if k != "event_cols"}
        schema_dict["event_col"] = value[0]
        return schema_dict

    @property
    def event_cols(self) -> List[str]:
        """Deprecated alias for `[event_col]`, kept for code that reads the list."""
        warnings.warn(
            "EventstreamSchema.event_cols is deprecated — use .event_col.",
            FutureWarning,
            stacklevel=2,
        )
        return [self.event_col]

    def copy(self) -> "EventstreamSchema":
        return EventstreamSchema(
            path_cols=self.path_cols.copy(),
            event_col=self.event_col,
            timestamp_col=self.timestamp_col,
            event_type=self.event_type,
            index=self.index,
            subindex=self.subindex,
            segment_cols=self.segment_cols.copy(),
            custom_cols=self.custom_cols.copy()
            if self.custom_cols is not None
            else None,
        )
