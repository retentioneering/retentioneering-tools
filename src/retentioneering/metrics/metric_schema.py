"""Declarative registry of per-metric config shapes for `MetricBuilder`.

Single source of truth for what `metric_args` each of the 9 path metrics
accepts (required/optional keys, valid modes, event/segment-existence
checks). Before this module, the same knowledge was encoded twice by hand
in `metric_builder.py`: once in `MetricConfig._parse_dict_config` (an
if/elif chain building the internal config `_build_metric` consumes) and
once in `MetricBuilder.validate_metric_config` (a separate if/elif chain
checking the same config against runtime data — available events/segment
values, which aren't known until a config is validated against a real
eventstream). The two chains had to be kept in sync by hand; this module
is now the only place a metric's shape is described, and both
`metric_builder.py` functions are thin dispatchers into it.

Each `MetricSchema.parse()` returns the exact field names `MetricBuilder`'s
`_build_*` methods already read off the parsed config dict (`event_names`,
`start_event`/`end_event`, `pattern`, `active_events`, `segment_name`/
`segment_values`/`mode`/`threshold`) — introducing this registry doesn't
change what those methods consume, only where the shape is declared.
"""

from dataclasses import dataclass, field
from typing import Any, Callable

from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import InvalidMetricConfigError

IN_SEGMENT_MODES = {
    "any",  # segment_value appears at least once
    "all",  # segment_value is the only value in the segment column
    "event_share",  # segment_value appears in at least N% of events
}

# Special synthetic events that don't need to exist in the eventstream
# (used by time_between's start_event/end_event existence check).
SYNTHETIC_EVENTS = {EventTypes().PATH_START.name, EventTypes().PATH_END.name}


def _normalize_events(events: Any, metric_name: str) -> list[str] | None:
    """
    Normalizes the 'events' metric_args value for event_count/has_event.

    Omitting the key, or passing None/[], means "all events" (returns None,
    resolved to the full event list at build time) — same convention as
    active_days' 'active_events'.
    """
    if not events:
        return None
    if isinstance(events, str):
        return [events]
    if isinstance(events, list):
        return events
    raise InvalidMetricConfigError(
        f"'{metric_name}' metric 'events' must be string or list"
    )


@dataclass(frozen=True)
class ValidationContext:
    """Runtime data `validate_metric_args` needs that isn't known until a
    config is checked against a real eventstream."""

    available_events: set
    segment_cols: list
    segment_values: Callable[
        [str], set
    ]  # segment_col -> its unique values, called lazily


@dataclass(frozen=True)
class MetricSchema:
    name: str
    # metric_args -> parsed fields (raises InvalidMetricConfigError on a bad shape:
    # missing required keys, invalid enum values — anything checkable without
    # touching the eventstream itself).
    parse: Callable[[dict], dict]
    # (metric_args, ValidationContext) -> None; raises InvalidMetricConfigError
    # for anything that needs runtime data (event/segment existence, ranges).
    validate: Callable[[dict, ValidationContext], None]
    # parsed fields -> output column name(s), or None if not known until build time
    # (e.g. in_segment with segment_value=None resolves at build time).
    metric_names: Callable[[dict], list | None]
    # value written into the parsed config's "type" key — what `_build_metric`
    # dispatches on. Equal to `name` except for time_between ("time_from_to").
    build_type: str = field(default="")

    def __post_init__(self):
        if not self.build_type:
            object.__setattr__(self, "build_type", self.name)


def _no_op_validate(metric_args: dict, ctx: ValidationContext) -> None:
    pass


def _zero_arg_schema(name: str) -> MetricSchema:
    """length/duration/first_event_time: no metric_args at all."""
    return MetricSchema(
        name=name,
        parse=lambda metric_args: {},
        validate=_no_op_validate,
        metric_names=lambda fields: [name],
    )


def _event_list_validate(metric_args: dict, ctx: ValidationContext) -> None:
    # Omitted/None/[] means "all events" - nothing to validate.
    events = metric_args.get("events")
    if events:
        events_to_check = [events] if isinstance(events, str) else events
        for e in events_to_check:
            if e not in ctx.available_events:
                raise InvalidMetricConfigError(
                    f"Event '{e}' not found. Available events: {sorted(ctx.available_events)}"
                )


def _event_list_schema(name: str, prefix: str) -> MetricSchema:
    """event_count/has_event: an optional 'events' string-or-list, wildcard if omitted."""

    def parse(metric_args: dict) -> dict:
        return {"event_names": _normalize_events(metric_args.get("events"), name)}

    def metric_names(fields: dict) -> list | None:
        event_names = fields["event_names"]
        return (
            [f"{prefix}_{e}" for e in event_names] if event_names is not None else None
        )

    return MetricSchema(
        name=name, parse=parse, validate=_event_list_validate, metric_names=metric_names
    )


def _active_days_parse(metric_args: dict) -> dict:
    return {"active_events": metric_args.get("active_events")}


def _matches_pattern_parse(metric_args: dict) -> dict:
    pattern = metric_args.get("pattern")
    if not pattern:
        raise InvalidMetricConfigError(
            "'matches_pattern' metric requires 'pattern' in metric_args"
        )
    return {"pattern": pattern}


def _matches_pattern_validate(metric_args: dict, ctx: ValidationContext) -> None:
    pattern = metric_args.get("pattern")
    if not pattern:
        raise InvalidMetricConfigError(
            "'matches_pattern' metric requires 'pattern' in metric_args"
        )


def _time_between_parse(metric_args: dict) -> dict:
    start_event = metric_args.get("start_event")
    end_event = metric_args.get("end_event")
    if not start_event or not end_event:
        raise InvalidMetricConfigError(
            "'time_between' metric requires 'start_event' and 'end_event' in metric_args"
        )
    return {"start_event": start_event, "end_event": end_event}


def _time_between_validate(metric_args: dict, ctx: ValidationContext) -> None:
    start_event = metric_args.get("start_event")
    end_event = metric_args.get("end_event")
    if not start_event or not end_event:
        raise InvalidMetricConfigError(
            "'time_between' metric requires 'start_event' and 'end_event' in metric_args"
        )
    # path_start and path_end are synthetic events, don't validate them
    if start_event not in SYNTHETIC_EVENTS and start_event not in ctx.available_events:
        raise InvalidMetricConfigError(
            f"Event '{start_event}' not found. Available events: {sorted(ctx.available_events)}"
        )
    if end_event not in SYNTHETIC_EVENTS and end_event not in ctx.available_events:
        raise InvalidMetricConfigError(
            f"Event '{end_event}' not found. Available events: {sorted(ctx.available_events)}"
        )


def _in_segment_parse(metric_args: dict) -> dict:
    segment_name = metric_args.get("segment_name")
    segment_value = metric_args.get(
        "segment_value"
    )  # None, scalar (str/int/float/bool), or list
    mode = metric_args.get("mode", "any")

    if not segment_name:
        raise InvalidMetricConfigError(
            "'in_segment' metric requires 'segment_name' in metric_args"
        )
    if mode not in IN_SEGMENT_MODES:
        raise InvalidMetricConfigError(
            f"'in_segment' metric has invalid mode '{mode}'. Valid modes: {sorted(IN_SEGMENT_MODES)}"
        )

    if segment_value is None:
        # Resolved later to all unique values in the segment column
        segment_values = None
    elif isinstance(segment_value, list):
        if len(segment_value) == 0:
            raise InvalidMetricConfigError(
                "'in_segment' metric requires non-empty 'segment_value' list"
            )
        segment_values = segment_value
    elif isinstance(segment_value, (str, int, float, bool)):
        segment_values = [segment_value]
    else:
        raise InvalidMetricConfigError(
            f"'in_segment' metric 'segment_value' must be string, number, boolean, list, or None. "
            f"Got: {type(segment_value).__name__}"
        )

    fields = {
        "segment_name": segment_name,
        "segment_values": segment_values,
        "mode": mode,
    }

    if mode == "event_share":
        threshold = metric_args.get("threshold")
        if threshold is None:
            raise InvalidMetricConfigError(
                "'in_segment' metric with mode 'event_share' requires 'threshold' (e.g., 0.1 for 10%)"
            )
        fields["threshold"] = threshold

    return fields


def _in_segment_metric_names(fields: dict) -> list | None:
    segment_values = fields["segment_values"]
    if segment_values is None:
        return None  # actual columns determined at build time
    segment_name, mode = fields["segment_name"], fields["mode"]
    return [f"in_segment_{segment_name}_{v}_{mode}" for v in segment_values]


def _in_segment_validate(metric_args: dict, ctx: ValidationContext) -> None:
    segment_name = metric_args.get("segment_name")
    segment_value = metric_args.get("segment_value")
    mode = metric_args.get("mode", "any")

    if not segment_name:
        raise InvalidMetricConfigError(
            "'in_segment' metric requires 'segment_name' in metric_args"
        )
    if mode not in IN_SEGMENT_MODES:
        raise InvalidMetricConfigError(
            f"'in_segment' metric has invalid mode '{mode}'. Valid modes: {sorted(IN_SEGMENT_MODES)}"
        )

    if segment_name not in ctx.segment_cols:
        raise InvalidMetricConfigError(
            f"Segment '{segment_name}' not found. Available segments: {ctx.segment_cols}"
        )

    if segment_value is not None:
        values_to_check = (
            segment_value if isinstance(segment_value, list) else [segment_value]
        )
        available_segment_values = ctx.segment_values(segment_name)
        for v in values_to_check:
            if v not in available_segment_values:
                raise InvalidMetricConfigError(
                    f"Segment value '{v}' not found in segment '{segment_name}'. "
                    f"Available values: {sorted(str(x) for x in available_segment_values)}"
                )

    if mode == "event_share":
        threshold = metric_args.get("threshold")
        if threshold is None:
            raise InvalidMetricConfigError(
                "'event_share' mode requires 'threshold' (e.g., 0.1 for 10%)"
            )
        if not isinstance(threshold, (int, float)) or threshold < 0 or threshold > 1:
            raise InvalidMetricConfigError(
                f"'event_share' mode requires 'threshold' between 0 and 1 (got {threshold})"
            )


METRIC_SCHEMAS: dict[str, MetricSchema] = {
    "length": _zero_arg_schema("length"),
    "duration": _zero_arg_schema("duration"),
    "first_event_time": _zero_arg_schema("first_event_time"),
    "active_days": MetricSchema(
        name="active_days",
        parse=_active_days_parse,
        validate=_no_op_validate,  # no existence check today, matching the prior chains
        metric_names=lambda fields: ["active_days"],
    ),
    "has_event": _event_list_schema("has_event", "has_event"),
    "event_count": _event_list_schema("event_count", "event_count"),
    "matches_pattern": MetricSchema(
        name="matches_pattern",
        parse=_matches_pattern_parse,
        validate=_matches_pattern_validate,
        metric_names=lambda fields: [f"matches_pattern_{fields['pattern']}"],
    ),
    "time_between": MetricSchema(
        name="time_between",
        parse=_time_between_parse,
        validate=_time_between_validate,
        metric_names=lambda fields: [
            f"time_from_{fields['start_event']}_to_{fields['end_event']}"
        ],
        build_type="time_from_to",
    ),
    "in_segment": MetricSchema(
        name="in_segment",
        parse=_in_segment_parse,
        validate=_in_segment_validate,
        metric_names=_in_segment_metric_names,
    ),
}


def parse_metric_args(metric_name: str, metric_args: dict) -> dict:
    if metric_name not in METRIC_SCHEMAS:
        raise InvalidMetricConfigError(
            f"Unknown metric type: '{metric_name}'. Valid metrics: {sorted(METRIC_SCHEMAS)}"
        )
    return METRIC_SCHEMAS[metric_name].parse(metric_args or {})


def validate_metric_args(
    metric_name: str, metric_args: dict, ctx: ValidationContext
) -> None:
    if metric_name not in METRIC_SCHEMAS:
        raise InvalidMetricConfigError(
            f"Unknown metric '{metric_name}'. Valid metrics: {sorted(METRIC_SCHEMAS)}"
        )
    METRIC_SCHEMAS[metric_name].validate(metric_args or {}, ctx)
