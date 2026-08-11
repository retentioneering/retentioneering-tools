"""
MetricBuilder - module for building metrics for each path in the eventstream

Supports metrics:
- length - number of steps (events)
- duration - duration in seconds between first and last event
- event_count - event count for a single event (one number per path)
  - event: event name (string, required)
- has_event - event presence (0/1) for a single event (one number per path)
  - event: event name (string, required)
- event_count_bulk - event count for one or more events, expanded into one column per event
  - events: list of event names, or omit/None for every event in the eventstream
    (an explicit empty list is invalid - only omitting/None means "all events")
- has_event_bulk - event presence (0/1) for one or more events, expanded into one column per event
  - events: list of event names, or omit/None for every event in the eventstream
    (an explicit empty list is invalid - only omitting/None means "all events")
- has_all_events - whether ALL of the given events occurred at least once (0/1, AND semantics)
  - events: non-empty list of event names
- has_any_event - whether ANY of the given events occurred at least once (0/1, OR semantics)
  - events: non-empty list of event names
- time_between - time in seconds between first occurrences of two events
  - start_event: event name
  - end_event: event name
- first_event_time - timestamp of first event
- active_days - number of unique days with at least one event
  - active_events: event name (string or list of strings)
- matches_pattern - whether path matches a token pattern (0/1)
  - pattern: string pattern like "login->.*->purchase" (literal events matched as whole
    tokens, not substrings; ".*" matches any run of whole events)
- in_segment - binary metric showing if a path belongs to a segment value (0/1)
  - segment_name: segment column name
  - segment_level: segment value (string, list of strings, or None for all values)
  - mode:
    - any: segment_level appears at least once
    - all: segment_level is the only value in the segment column
    - event_share: segment_level appears in at least N% of events
      - threshold: percentage of events (e.g., 0.1 for 10%)
- in_segment_bulk - the same 0/1 membership check, expanded into one column per
  (segment column, segment level) pair
  - segment_name: segment column name, or omit/None for every segment column
  - segment_levels: list of segment values, or omit/None for every level of the
    selected segment column(s) (an explicit empty list is invalid - only
    omitting/None means "all levels"); requires segment_name
  - mode / threshold: same as in_segment
"""

from typing import Any, Dict, List, Set

import pandas as pd

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import (
    InvalidMetricConfigError,
    InvalidParameterError,
    PatternSyntaxError,
)
from retentioneering.paths import anchors
from retentioneering.utils.sql_quoting import quote_list, quote_literal


# Valid metric names
VALID_METRICS = {
    "length",
    "duration",
    "event_count",
    "has_event",
    "event_count_bulk",
    "has_event_bulk",
    "has_all_events",
    "has_any_event",
    "time_between",
    "first_event_time",
    "active_days",
    "matches_pattern",
    "in_segment",
    "in_segment_bulk",
}

# Valid modes for the in_segment metric
IN_SEGMENT_MODES = {
    "any",  # segment_level appears at least once
    "all",  # segment_level is the only value in the segment column
    "event_share",  # segment_level appears in at least N% of events
}


# Special synthetic events that don't need to exist in the eventstream
SYNTHETIC_EVENTS = {EventTypes().PATH_START.name, EventTypes().PATH_END.name}


def _normalize_single_event(event: Any, metric_name: str) -> str:
    """Normalizes the 'event' metric_args value for has_event/event_count: a single
    event name is required - no list, no wildcard. Use '{metric_name}_bulk' for a
    list of events, or 'has_all_events'/'has_any_event' for AND/OR conditions."""
    if not event or not isinstance(event, str):
        raise InvalidMetricConfigError(
            f"'{metric_name}' metric requires a single 'event' (string) in metric_args"
        )
    return event


def _normalize_events_bulk(events: Any, metric_name: str) -> List[str] | None:
    """Normalizes the 'events' metric_args value for has_event_bulk/event_count_bulk.

    Omitting the key, or passing None, means "all events" (returns None, resolved to
    the full event list at build time). An explicit empty list is rejected - it is
    not a valid spelling of the wildcard and is almost always a caller-side bug.
    """
    if events is None:
        return None
    if isinstance(events, list):
        if len(events) == 0:
            raise InvalidMetricConfigError(
                f"'{metric_name}' metric 'events' must not be an empty list. "
                f"Omit 'events' (or pass None) to select every event."
            )
        return events
    raise InvalidMetricConfigError(
        f"'{metric_name}' metric 'events' must be a list, or omitted/None"
    )


def _normalize_events_required(events: Any, metric_name: str) -> List[str]:
    """Normalizes the 'events' metric_args value for has_all_events/has_any_event:
    a non-empty list is required - there's no wildcard, since AND/OR over "every
    event in the stream" isn't a meaningful condition."""
    if not isinstance(events, list) or len(events) == 0:
        raise InvalidMetricConfigError(
            f"'{metric_name}' metric requires a non-empty 'events' list in metric_args"
        )
    return events


def _normalize_time_between(metric_args: Dict[str, Any]) -> tuple[str, str]:
    """Normalizes 'start_event'/'end_event' metric_args for time_between: both required."""
    start_event = metric_args.get("start_event")
    end_event = metric_args.get("end_event")
    if not start_event or not end_event:
        raise InvalidMetricConfigError(
            "'time_between' metric requires 'start_event' and 'end_event' in metric_args"
        )
    return start_event, end_event


def _normalize_pattern(metric_args: Dict[str, Any]) -> str:
    """Normalizes the 'pattern' metric_args value for matches_pattern: required, non-empty."""
    pattern = metric_args.get("pattern")
    if not pattern:
        raise InvalidMetricConfigError(
            "'matches_pattern' metric requires 'pattern' in metric_args"
        )
    return pattern


def _normalize_in_segment(metric_args: Dict[str, Any]) -> Dict[str, Any]:
    """Normalizes in_segment's metric_args shape: 'segment_name'/'mode' (required,
    valid mode) and 'segment_level' (None | scalar | non-empty list, normalized to
    'segment_levels' - a list, or None meaning "all values", resolved at build time).

    Shared by parse (which also needs segment_name/mode to build metric_names) and
    validate (which layers real-eventstream existence/range checks on top of this
    same shape).
    """
    segment_name = metric_args.get("segment_name")
    segment_level = metric_args.get("segment_level")
    mode = metric_args.get("mode", "any")

    if not segment_name:
        raise InvalidMetricConfigError(
            "'in_segment' metric requires 'segment_name' in metric_args"
        )
    if mode not in IN_SEGMENT_MODES:
        raise InvalidMetricConfigError(
            f"'in_segment' metric has invalid mode '{mode}'. Valid modes: {sorted(IN_SEGMENT_MODES)}"
        )

    if segment_level is None:
        segment_levels = None
    elif isinstance(segment_level, list):
        if len(segment_level) == 0:
            raise InvalidMetricConfigError(
                "'in_segment' metric requires non-empty 'segment_level' list"
            )
        segment_levels = segment_level
    elif isinstance(segment_level, (str, int, float, bool)):
        segment_levels = [segment_level]
    else:
        raise InvalidMetricConfigError(
            f"'in_segment' metric 'segment_level' must be string, number, boolean, list, or None. "
            f"Got: {type(segment_level).__name__}"
        )

    return {
        "segment_name": segment_name,
        "segment_levels": segment_levels,
        "mode": mode,
    }


def _normalize_in_segment_bulk(metric_args: Dict[str, Any]) -> Dict[str, Any]:
    """Normalizes in_segment_bulk's metric_args shape - the wildcard-friendly
    sibling of `_normalize_in_segment`.

    Both 'segment_name' and 'segment_levels' may be omitted/None, each meaning
    "all of them" (all segment columns / all levels of the selected column),
    resolved at build time. As with event_count_bulk's 'events', an explicit
    empty list is rejected rather than read as the wildcard. 'segment_levels'
    without a 'segment_name' is rejected too: a level list only makes sense
    relative to one segment column.
    """
    segment_name = metric_args.get("segment_name")
    segment_levels = metric_args.get("segment_levels")
    mode = metric_args.get("mode", "any")

    if "segment_level" in metric_args:
        raise InvalidMetricConfigError(
            "'in_segment_bulk' metric takes 'segment_levels' (a list), not "
            "'segment_level'. Use the 'in_segment' metric for a single level."
        )
    if mode not in IN_SEGMENT_MODES:
        raise InvalidMetricConfigError(
            f"'in_segment_bulk' metric has invalid mode '{mode}'. Valid modes: {sorted(IN_SEGMENT_MODES)}"
        )

    if segment_levels is not None:
        if not isinstance(segment_levels, list):
            raise InvalidMetricConfigError(
                "'in_segment_bulk' metric 'segment_levels' must be a list, or omitted/None"
            )
        if len(segment_levels) == 0:
            raise InvalidMetricConfigError(
                "'in_segment_bulk' metric 'segment_levels' must not be an empty list. "
                "Omit 'segment_levels' (or pass None) to select every level."
            )
        if not segment_name:
            raise InvalidMetricConfigError(
                "'in_segment_bulk' metric requires 'segment_name' when 'segment_levels' "
                "is given - levels belong to a single segment column."
            )

    return {
        "segment_name": segment_name or None,
        "segment_levels": segment_levels,
        "mode": mode,
    }


def _normalize_in_segment_threshold(
    metric_args: Dict[str, Any], metric_name: str = "in_segment"
) -> Any:
    """Normalizes the 'threshold' metric_args value for in_segment's 'event_share'
    mode: required when present. Range checking (0-1) needs no real-eventstream
    data, but is validate-only (see `MetricBuilder.validate_metric_config`) since
    parse only needs to know a threshold was supplied, not that it's sane."""
    threshold = metric_args.get("threshold")
    if threshold is None:
        raise InvalidMetricConfigError(
            f"'{metric_name}' metric with mode 'event_share' requires 'threshold' (e.g., 0.1 for 10%)"
        )
    return threshold


def combined_metric_name(metric: str, events: List[str]) -> str:
    """Canonical column name for has_all_events/has_any_event: the one piece of
    metric-naming *logic* (as opposed to plain interpolation) shared across
    this module, data_processors/filter_paths.py, and
    data_processors/collapse_events.py - kept here since this module is the
    single path-metrics registry (see ADR-0007) and the other two are equal
    consumers of it, not of each other."""
    joiner = "_and_" if metric == "has_all_events" else "_or_"
    return f"{metric}_{joiner.join(events)}"


class MetricConfig:
    """Parser and validator for metric configurations"""

    def __init__(
        self,
        config_items: List[Dict[str, Any]],
        available_events: Set[str] | List[str] | None = None,
    ):
        """
        Args:
            config_items: Metric configuration dicts.
            available_events: All event names present in the eventstream. When given,
                omitted/None/[] 'events' (has_event/event_count) is eagerly resolved to
                every available event, so 'metric_names'/'cols' reflect the real columns
                MetricBuilder will produce instead of staying None. Callers that only
                need the parsed shape (not the resolved column names) may omit this.
        """
        self.config_items = config_items
        self.available_events = available_events
        self.parsed_configs = self._parse_configs()

    def get_enriched_configs(self) -> List[Dict[str, Any]]:
        """
        Returns original configs enriched with 'cols' key containing column names.

        Example:
            Input: [{"metric": "has_event", "metric_args": {"events": ["A", "B"]}, "agg": "mean"}]
            Output: [{"metric": "has_event", "metric_args": {"events": ["A", "B"]}, "agg": "mean", "cols": ["has_event_A", "has_event_B"]}]

        Note: For in_segment metric with segment_level=None, cols will be empty list
        because the actual columns are determined at build time.
        """
        enriched = []
        for parsed in self.parsed_configs:
            original = parsed["original"].copy()
            # Handle case when metric_names is None (e.g., in_segment with segment_level=None)
            # In this case, actual columns are determined at build time
            metric_names = parsed.get("metric_names")
            original["cols"] = metric_names if metric_names is not None else []
            enriched.append(original)
        return enriched

    def _parse_configs(self) -> List[Dict[str, Any]]:
        """Parses configuration items into structured format"""
        configs = []

        for config_dict in self.config_items:
            if not isinstance(config_dict, dict):
                raise InvalidMetricConfigError(
                    f"Expected dict config, got {type(config_dict).__name__}: {config_dict}"
                )

            config = self._parse_dict_config(config_dict)
            configs.append(config)

        return configs

    def _parse_dict_config(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Parses a dict-based configuration. Raises InvalidMetricConfigError on invalid config."""
        metric = config_dict.get("metric")
        metric_args = config_dict.get("metric_args", {})

        if not metric:
            raise InvalidMetricConfigError(
                f"Config missing 'metric' field: {config_dict}"
            )

        if metric == "length":
            return {
                "type": "length",
                "metric_names": ["length"],
                "original": config_dict,
            }
        elif metric == "duration":
            return {
                "type": "duration",
                "metric_names": ["duration"],
                "original": config_dict,
            }
        elif metric == "first_event_time":
            return {
                "type": "first_event_time",
                "metric_names": ["first_event_time"],
                "original": config_dict,
            }
        elif metric == "active_days":
            active_events = metric_args.get("active_events")
            return {
                "type": "active_days",
                "metric_names": ["active_days"],
                "active_events": active_events,
                "original": config_dict,
            }
        elif metric == "matches_pattern":
            pattern = _normalize_pattern(metric_args)
            return {
                "type": "matches_pattern",
                "pattern": pattern,
                "metric_names": [f"matches_pattern_{pattern}"],
                "original": config_dict,
            }
        elif metric == "has_event":
            event = _normalize_single_event(metric_args.get("event"), "has_event")
            return {
                "type": "has_event",
                "event_names": [event],
                "metric_names": [f"has_event_{event}"],
                "original": config_dict,
            }
        elif metric == "event_count":
            event = _normalize_single_event(metric_args.get("event"), "event_count")
            return {
                "type": "event_count",
                "event_names": [event],
                "metric_names": [f"event_count_{event}"],
                "original": config_dict,
            }
        elif metric == "has_event_bulk":
            event_names = _normalize_events_bulk(
                metric_args.get("events"), "has_event_bulk"
            )
            # Wildcard ('events' omitted/None): eagerly resolve to every available
            # event (when known) so 'metric_names'/'cols' reflect the real columns
            # MetricBuilder will produce instead of staying None — callers that only
            # need the parsed shape (not the resolved column names) may omit
            # available_events.
            if event_names is None and self.available_events is not None:
                event_names = sorted(self.available_events)
            return {
                "type": "has_event_bulk",
                "event_names": event_names,
                "metric_names": [f"has_event_bulk_{e}" for e in event_names]
                if event_names is not None
                else None,
                "original": config_dict,
            }
        elif metric == "event_count_bulk":
            event_names = _normalize_events_bulk(
                metric_args.get("events"), "event_count_bulk"
            )
            if event_names is None and self.available_events is not None:
                event_names = sorted(self.available_events)
            return {
                "type": "event_count_bulk",
                "event_names": event_names,
                "metric_names": [f"event_count_bulk_{e}" for e in event_names]
                if event_names is not None
                else None,
                "original": config_dict,
            }
        elif metric in ("has_all_events", "has_any_event"):
            events = _normalize_events_required(metric_args.get("events"), metric)
            return {
                "type": metric,
                "events": events,
                "metric_names": [combined_metric_name(metric, events)],
                "original": config_dict,
            }
        elif metric == "time_between":
            start_event, end_event = _normalize_time_between(metric_args)
            return {
                "type": "time_from_to",
                "start_event": start_event,
                "end_event": end_event,
                "metric_names": [f"time_from_{start_event}_to_{end_event}"],
                "original": config_dict,
            }
        elif metric == "in_segment":
            norm = _normalize_in_segment(metric_args)
            segment_name = norm["segment_name"]
            segment_levels = norm["segment_levels"]  # None means all values
            mode = norm["mode"]

            # metric_names stays None until build time when segment_levels is None
            # (every value in the segment column - not known without the eventstream).
            metric_names = (
                None
                if segment_levels is None
                else [f"in_segment_{segment_name}_{v}_{mode}" for v in segment_levels]
            )

            result = {
                "type": "in_segment",
                "segment_name": segment_name,
                "segment_levels": segment_levels,
                "mode": mode,
                "metric_names": metric_names,
                "original": config_dict,
            }

            if mode == "event_share":
                result["threshold"] = _normalize_in_segment_threshold(metric_args)

            return result
        elif metric == "in_segment_bulk":
            norm = _normalize_in_segment_bulk(metric_args)
            segment_name = norm["segment_name"]  # None means all segment columns
            segment_levels = norm["segment_levels"]  # None means all levels
            mode = norm["mode"]

            # metric_names stays None while either half is a wildcard - the
            # segment columns and their levels aren't known without the
            # eventstream, so the real column names only exist at build time.
            metric_names = (
                [f"in_segment_bulk_{segment_name}_{v}_{mode}" for v in segment_levels]
                if segment_name is not None and segment_levels is not None
                else None
            )

            result = {
                "type": "in_segment_bulk",
                "segment_name": segment_name,
                "segment_levels": segment_levels,
                "mode": mode,
                "metric_names": metric_names,
                "original": config_dict,
            }

            if mode == "event_share":
                result["threshold"] = _normalize_in_segment_threshold(
                    metric_args, "in_segment_bulk"
                )

            return result
        else:
            raise InvalidMetricConfigError(
                f"Unknown metric type: '{metric}'. Valid metrics: {sorted(VALID_METRICS)}"
            )


class MetricBuilder:
    """Main class for building metrics"""

    def __init__(self, eventstream: Any):
        self.eventstream = eventstream
        self.df = eventstream.df
        self.df_with_start_end = None  # Lazy load for path_pattern metric
        self.schema = eventstream.schema

    def validate_metric_config(
        self,
        metric: Dict[str, Any],
        available_events: Set[str] | List[str] | None = None,
    ) -> None:
        """
        Validate a single metric configuration.

        Args:
            metric: Metric configuration dict with 'metric' and optional 'metric_args'
            available_events: Set of available event names. If None, extracted from eventstream.

        Raises:
            InvalidMetricConfigError: If configuration is invalid

        Shape checks (required fields, valid modes/enums) are shared with
        `_parse_dict_config` via the same `_normalize_*` helpers, so the two
        can't drift apart on what a metric's `metric_args` must look like.
        This method's own job on top of that shared shape is exactly what
        needs real eventstream data and so can't be checked at parse time:
        event/segment-value existence, and the `in_segment` threshold range.
        """
        if available_events is None:
            event_col = self.schema.event_col
            available_events = set(self.df[event_col].unique().tolist())
        else:
            available_events = set(available_events)

        metric_name = metric.get("metric")
        if not metric_name:
            raise InvalidMetricConfigError(
                "Metric configuration must include 'metric' field"
            )

        metric_args = metric.get("metric_args", {})

        if metric_name in ("event_count", "has_event"):
            event = _normalize_single_event(metric_args.get("event"), metric_name)
            if event not in available_events:
                raise InvalidMetricConfigError(
                    f"Event '{event}' not found. Available events: {sorted(available_events)}"
                )

        elif metric_name in ("event_count_bulk", "has_event_bulk"):
            events = _normalize_events_bulk(metric_args.get("events"), metric_name)
            if events is not None:
                for e in events:
                    if e not in available_events:
                        raise InvalidMetricConfigError(
                            f"Event '{e}' not found. Available events: {sorted(available_events)}"
                        )

        elif metric_name in ("has_all_events", "has_any_event"):
            events = _normalize_events_required(metric_args.get("events"), metric_name)
            for e in events:
                if e not in available_events:
                    raise InvalidMetricConfigError(
                        f"Event '{e}' not found. Available events: {sorted(available_events)}"
                    )

        elif metric_name == "time_between":
            start_event, end_event = _normalize_time_between(metric_args)
            # path_start and path_end are synthetic events, don't validate them
            if (
                start_event not in SYNTHETIC_EVENTS
                and start_event not in available_events
            ):
                raise InvalidMetricConfigError(
                    f"Event '{start_event}' not found. Available events: {sorted(available_events)}"
                )
            if end_event not in SYNTHETIC_EVENTS and end_event not in available_events:
                raise InvalidMetricConfigError(
                    f"Event '{end_event}' not found. Available events: {sorted(available_events)}"
                )

        elif metric_name == "matches_pattern":
            # Delegated rather than open-coded: a token is no longer necessarily
            # an event name, and a hand-rolled `tok not in available_events`
            # would reject a valid `[cart|purchase]` as a missing event while
            # letting a typo inside it through as an always-true position.
            pattern = _normalize_pattern(metric_args)
            try:
                anchors.validate_pattern_tokens(
                    anchors.normalize_pattern(pattern, warn=False, param="pattern"),
                    available_events,
                    param="pattern",
                )
            except (InvalidParameterError, PatternSyntaxError) as exc:
                raise InvalidMetricConfigError(
                    f"In pattern '{pattern}': {exc.message}"
                ) from exc

        elif metric_name == "in_segment":
            norm = _normalize_in_segment(metric_args)
            segment_name, segment_levels, mode = (
                norm["segment_name"],
                norm["segment_levels"],
                norm["mode"],
            )

            # Validate that segment column exists
            if segment_name not in self.schema.segment_cols:
                raise InvalidMetricConfigError(
                    f"Segment '{segment_name}' not found. Available segments: {self.schema.segment_cols}"
                )

            # Validate segment_levels if provided (None means "all values" - nothing to check)
            if segment_levels is not None:
                available_segment_levels = set(self.df[segment_name].unique().tolist())
                for v in segment_levels:
                    if v not in available_segment_levels:
                        raise InvalidMetricConfigError(
                            f"Segment value '{v}' not found in segment '{segment_name}'. "
                            f"Available values: {sorted(str(x) for x in available_segment_levels)}"
                        )

            # Validate mode-specific parameters
            if mode == "event_share":
                threshold = _normalize_in_segment_threshold(metric_args)
                if (
                    not isinstance(threshold, (int, float))
                    or threshold < 0
                    or threshold > 1
                ):
                    raise InvalidMetricConfigError(
                        f"'event_share' mode requires 'threshold' between 0 and 1 (got {threshold})"
                    )

        elif metric_name == "in_segment_bulk":
            norm = _normalize_in_segment_bulk(metric_args)
            segment_name, segment_levels, mode = (
                norm["segment_name"],
                norm["segment_levels"],
                norm["mode"],
            )

            if segment_name is None:
                # Wildcard over every segment column - only meaningful if the
                # eventstream has at least one.
                if not self.schema.segment_cols:
                    raise InvalidMetricConfigError(
                        "'in_segment_bulk' metric with no 'segment_name' requires at least "
                        "one segment column in the eventstream, but there are none. "
                        "Add one with add_segment() or add_clusters()."
                    )
            else:
                if segment_name not in self.schema.segment_cols:
                    raise InvalidMetricConfigError(
                        f"Segment '{segment_name}' not found. Available segments: {self.schema.segment_cols}"
                    )
                if segment_levels is not None:
                    available_segment_levels = set(
                        self.df[segment_name].unique().tolist()
                    )
                    for v in segment_levels:
                        if v not in available_segment_levels:
                            raise InvalidMetricConfigError(
                                f"Segment value '{v}' not found in segment '{segment_name}'. "
                                f"Available values: {sorted(str(x) for x in available_segment_levels)}"
                            )

            if mode == "event_share":
                threshold = _normalize_in_segment_threshold(
                    metric_args, "in_segment_bulk"
                )
                if (
                    not isinstance(threshold, (int, float))
                    or threshold < 0
                    or threshold > 1
                ):
                    raise InvalidMetricConfigError(
                        f"'event_share' mode requires 'threshold' between 0 and 1 (got {threshold})"
                    )

    def build_metrics(
        self, config: List[Dict[str, Any]], path_col: str | None = None
    ) -> pd.DataFrame:
        """
        Main method for building metrics

        Args:
            config: List of metric configuration dicts with 'metric' and optional 'metric_args' fields
            path_col: Path ID column (if None, taken from schema)

        Returns:
            DataFrame with path_id as index and metrics as columns

        Raises:
            InvalidMetricConfigError: If any metric configuration references an
                event or segment value that doesn't exist, so typos fail loudly
                instead of silently producing all-zero metrics.
        """
        path_col = path_col or self.schema.path_col
        event_col = self.schema.event_col

        available_events = set(self.df[event_col].unique().tolist())
        metric_config = MetricConfig(config, available_events=available_events)

        for parsed in metric_config.parsed_configs:
            self.validate_metric_config(
                parsed["original"], available_events=available_events
            )

        path_ids = self.df[path_col].unique()

        metric_dfs: List[pd.DataFrame] = []

        # Build metrics for each configuration
        for config_item in metric_config.parsed_configs:
            metric_df = self._build_metric(config_item, path_col, event_col)

            # For time_from_to metrics, keep NaN values (don't fill with 0)
            if config_item["type"] == "time_from_to":
                metric_df = metric_df.reindex(path_ids)
            else:
                metric_df = metric_df.reindex(path_ids, fill_value=0).fillna(0)

            metric_dfs.append(metric_df)

        if metric_dfs:
            result_df = pd.concat(metric_dfs, axis=1)
        else:
            result_df = pd.DataFrame(index=path_ids)

        result_df.index.name = path_col
        return result_df

    def _build_metric(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds specific metric according to configuration"""

        if config["type"] == "event_count":
            return self._build_event_count(config, path_col, event_col)
        elif config["type"] == "has_event":
            return self._build_has(config, path_col, event_col)
        elif config["type"] == "event_count_bulk":
            return self._build_event_count_bulk(config, path_col, event_col)
        elif config["type"] == "has_event_bulk":
            return self._build_has_bulk(config, path_col, event_col)
        elif config["type"] in ("has_all_events", "has_any_event"):
            return self._build_has_all_or_any(config, path_col, event_col)
        elif config["type"] == "time_from_to":
            return self._build_time_from_to(config, path_col, event_col)
        elif config["type"] == "matches_pattern":
            return self._build_matches(config, path_col, event_col)
        elif config["type"] == "length":
            return self._build_length(path_col)
        elif config["type"] == "duration":
            return self._build_duration(path_col)
        elif config["type"] == "first_event_time":
            return self._build_first_event_time(path_col)
        elif config["type"] == "active_days":
            return self._build_active_days(path_col, config.get("active_events"))
        elif config["type"] == "in_segment":
            return self._build_in_segment(config, path_col)
        elif config["type"] == "in_segment_bulk":
            return self._build_in_segment_bulk(config, path_col)
        else:
            raise InvalidMetricConfigError(f"Unknown metric type: '{config['type']}'")

    def _resolve_event_names(self, config: Dict[str, Any], event_col: str) -> List[str]:
        """Resolves a possibly-wildcard (None) 'event_names' config entry to an
        explicit, sorted list of event names."""
        event_names = config["event_names"]
        if event_names is None:
            # Wildcard: 'events' was omitted/None - count every event in the stream.
            event_names = sorted(self.df[event_col].unique().tolist())
        return event_names

    def _build_count_pivot(
        self,
        event_names: List[str],
        path_col: str,
        event_col: str,
        col_prefix: str,
    ) -> pd.DataFrame:
        """Shared primitive: per-path count of each event in event_names, pivoted
        into one column per event named '{col_prefix}{event}'. Used directly by
        event_count/event_count_bulk, and as the basis for has_event/has_event_bulk
        (via >0) and has_all_events/has_any_event (via row-wise all()/any())."""
        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        events_quoted = quote_list(event_names)
        query = f"""
        SELECT
            {path_col_q},
            {event_col_q},
            count(*) as count
        FROM df
        WHERE {event_col_q} IN ({events_quoted})
        GROUP BY {path_col_q}, {event_col_q}
        """

        result = engine.run(query, df=self.df)

        if result.empty:
            return pd.DataFrame(columns=[f"{col_prefix}{e}" for e in event_names])

        # Pivot: path_id as index, events as columns
        metrics_df = (
            result.set_index([path_col, event_col])["count"]
            .unstack(fill_value=0)
            .reindex(columns=event_names, fill_value=0)
        )
        metrics_df.columns = [f"{col_prefix}{c}" for c in metrics_df.columns]

        return metrics_df

    def _build_event_count(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds the event count metric for a single event"""
        event_names = self._resolve_event_names(config, event_col)
        return self._build_count_pivot(event_names, path_col, event_col, "event_count_")

    def _build_has(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds the event presence metric (0/1) for a single event"""
        counts = self._build_event_count(config, path_col, event_col)
        has = (counts > 0).astype(int)
        has.columns = [c.replace("event_count_", "has_event_", 1) for c in has.columns]
        return has

    def _build_event_count_bulk(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds event count metrics for one or more events, one column per event"""
        event_names = self._resolve_event_names(config, event_col)
        return self._build_count_pivot(
            event_names, path_col, event_col, "event_count_bulk_"
        )

    def _build_has_bulk(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds event presence metrics (0/1) for one or more events, one column per event"""
        counts = self._build_event_count_bulk(config, path_col, event_col)
        has = (counts > 0).astype(int)
        has.columns = [
            c.replace("event_count_bulk_", "has_event_bulk_", 1) for c in has.columns
        ]
        return has

    def _build_has_all_or_any(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds has_all_events (AND) / has_any_event (OR) - a single 0/1 column
        per path. Reuses the count pivot with a throwaway internal prefix, then
        reduces row-wise; the per-event columns never surface downstream."""
        events = config["events"]
        metric_name = config["metric_names"][0]
        counts = self._build_count_pivot(events, path_col, event_col, "__evt__")
        present = counts > 0
        if config["type"] == "has_all_events":
            reduced = present.all(axis=1)
        else:
            reduced = present.any(axis=1)
        return reduced.astype(int).to_frame(metric_name)

    def _build_time_from_to(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds time difference metric between two events (in seconds)"""
        start_event = config["start_event"]
        end_event = config["end_event"]
        timestamp_col = self.schema.timestamp_col

        # Lazy load dataframe with start_end events (needed for path_start/path_end)
        if self.df_with_start_end is None:
            self.df_with_start_end = self.eventstream.add_start_end_events(
                path_col=path_col
            ).df

        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        timestamp_col_q = engine.quote_ident(timestamp_col)
        query = f"""
        WITH first_events AS (
            SELECT
                {path_col_q},
                MIN(CASE WHEN {event_col_q} = {quote_literal(start_event)} THEN {timestamp_col_q} END) as time_from,
                MIN(CASE WHEN {event_col_q} = {quote_literal(end_event)} THEN {timestamp_col_q} END) as time_to
            FROM df_with_start_end
            GROUP BY {path_col_q}
        )
        SELECT
            {path_col_q},
            {dialect.epoch("time_to - time_from")} as time_diff_seconds
        FROM first_events
        WHERE time_from IS NOT NULL AND time_to IS NOT NULL
        """

        result = engine.run(query, df_with_start_end=self.df_with_start_end)

        metric_name = f"time_from_{start_event}_to_{end_event}"
        if len(result) > 0:
            return result.set_index(path_col).rename(
                columns={"time_diff_seconds": metric_name}
            )
        else:
            # Empty result - no paths have both events
            return pd.DataFrame(columns=[metric_name])

    def _build_length(self, path_col: str) -> pd.DataFrame:
        """Number of steps (events) per path"""
        path_col_q = engine.quote_ident(path_col)
        query = f"""
        SELECT {path_col_q}, COUNT(*) AS length
        FROM df
        GROUP BY {path_col_q}
        """
        result = engine.run(query, df=self.df)
        return result.set_index(path_col)

    def _build_duration(self, path_col: str) -> pd.DataFrame:
        """Duration in seconds between first and last event per path"""
        timestamp_col = self.schema.timestamp_col
        path_col_q = engine.quote_ident(path_col)
        timestamp_col_q = engine.quote_ident(timestamp_col)
        query = f"""
        SELECT {path_col_q}, {dialect.epoch(f"MAX({timestamp_col_q}) - MIN({timestamp_col_q})")} AS duration
        FROM df
        GROUP BY {path_col_q}
        """
        result = engine.run(query, df=self.df)
        result["duration"] = result["duration"].astype(float)
        return result.set_index(path_col)

    def _build_first_event_time(self, path_col: str) -> pd.DataFrame:
        """Unix timestamp (seconds) of first event per path.
        Stored as float so mean/median/percentile aggregations work correctly."""
        timestamp_col = self.schema.timestamp_col
        path_col_q = engine.quote_ident(path_col)
        timestamp_col_q = engine.quote_ident(timestamp_col)
        query = f"""
        SELECT {path_col_q}, {dialect.epoch(f"MIN({timestamp_col_q})")} AS first_event_time
        FROM df
        GROUP BY {path_col_q}
        """
        result = engine.run(query, df=self.df)
        result["first_event_time"] = result["first_event_time"].astype(float)
        return result.set_index(path_col)

    def _build_active_days(self, path_col: str, active_events=None) -> pd.DataFrame:
        """Number of unique days with at least one (matching) event per path.
        active_events: optional list of events to count; if None, all events count."""
        timestamp_col = self.schema.timestamp_col
        event_col = self.schema.event_col
        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        timestamp_col_q = engine.quote_ident(timestamp_col)
        if active_events:
            ev_list = (
                active_events if isinstance(active_events, list) else [active_events]
            )
            quoted = quote_list(ev_list)
            count_expr = f"COUNT(DISTINCT CASE WHEN {event_col_q} IN ({quoted}) THEN CAST({timestamp_col_q} AS DATE) END)"
        else:
            count_expr = f"COUNT(DISTINCT CAST({timestamp_col_q} AS DATE))"
        query = f"""
        SELECT {path_col_q}, {count_expr} AS active_days
        FROM df
        GROUP BY {path_col_q}
        """
        result = engine.run(query, df=self.df)
        return result.set_index(path_col)

    def _build_matches(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds pattern matching metric (0/1) for each path.

        A degenerate case of positional anchoring: `paths.anchors` locates the
        pattern in every path, and this metric only asks whether it was located
        at all. Before the anchors module existed this was a second, independent
        RE2 implementation of the same matching semantics (see that module's
        docstring for what the two had drifted on).
        """
        pattern = config["pattern"]
        metric_name = config["metric_names"][0]

        # Lazy load dataframe with start_end events (needed for path_start/path_end in patterns)
        if self.df_with_start_end is None:
            self.df_with_start_end = self.eventstream.add_start_end_events(
                path_col=path_col
            ).df

        # Token existence is checked by `validate_metric_config`, which every
        # build goes through; normalizing here only settles the redundant
        # leading/trailing gaps the matcher assumes are already gone.
        pattern = anchors.normalize_pattern(pattern, warn=False, param="pattern")

        match = anchors.resolve_anchors(
            self.df_with_start_end, self.schema, pattern, path_col=path_col
        )
        matched = match.paths()

        all_paths = pd.Index(self.df_with_start_end[path_col].unique(), name=path_col)
        return pd.DataFrame({metric_name: all_paths.isin(matched)}, index=all_paths)

    def _build_in_segment(self, config: Dict[str, Any], path_col: str) -> pd.DataFrame:
        """
        Builds in_segment metric (0/1) for each path.

        Determines if a path belongs to a segment based on different modes:
        - any: segment_level appears at least once
        - all: segment_level is the only value in the segment column
        - event_share: segment_level appears in at least N% of events

        Can handle multiple segment values at once, generating one column per value.
        If segment_levels is None, generates metrics for all unique values in the segment.
        """
        return self._build_in_segment_columns(
            segment_name=config["segment_name"],
            segment_levels=config["segment_levels"],
            mode=config["mode"],
            threshold=config.get("threshold"),
            path_col=path_col,
            col_prefix="in_segment_",
        )

    def _build_in_segment_bulk(
        self, config: Dict[str, Any], path_col: str
    ) -> pd.DataFrame:
        """
        Builds in_segment_bulk metrics (0/1) - the same per-level membership
        columns as in_segment, but with both halves of the selection allowed to
        be wildcards: `segment_name=None` means every segment column in the
        schema, `segment_levels=None` means every level of the selected
        column(s). One column per (segment column, level) pair.
        """
        segment_name = config["segment_name"]
        segment_names = (
            list(self.schema.segment_cols) if segment_name is None else [segment_name]
        )

        frames = [
            self._build_in_segment_columns(
                segment_name=name,
                segment_levels=config["segment_levels"],
                mode=config["mode"],
                threshold=config.get("threshold"),
                path_col=path_col,
                col_prefix="in_segment_bulk_",
            )
            for name in segment_names
        ]

        if not frames:
            return pd.DataFrame()
        if len(frames) == 1:
            return frames[0]
        return pd.concat(frames, axis=1)

    def _resolve_segment_levels(self, segment_name: str) -> List[Any]:
        """Every level actually present in a segment column, sorted by string
        representation for a stable column order. Missing values are skipped -
        NaN can't be compared with `=` in SQL, so a NaN "level" would only ever
        produce an all-zero column."""
        unique_values = self.df[segment_name].unique().tolist()
        return sorted((v for v in unique_values if not pd.isna(v)), key=str)

    def _build_in_segment_columns(
        self,
        segment_name: str,
        segment_levels: List[Any] | None,
        mode: str,
        threshold: Any,
        path_col: str,
        col_prefix: str,
    ) -> pd.DataFrame:
        """Shared primitive behind in_segment and in_segment_bulk: one 0/1
        column per level of a single segment column, named
        '{col_prefix}{segment_name}_{level}_{mode}'. `segment_levels=None`
        resolves to every level present in the column."""
        df = self.df

        if segment_levels is None:
            segment_levels = self._resolve_segment_levels(segment_name)

        metric_names = [
            f"{col_prefix}{segment_name}_{v}_{mode}" for v in segment_levels
        ]

        # Build metrics for each segment value
        result_dfs = []
        path_col_q = engine.quote_ident(path_col)
        segment_name_q = engine.quote_ident(segment_name)

        for segment_level, metric_name in zip(segment_levels, metric_names):
            # Format value for SQL comparison
            sql_value = quote_literal(segment_level)

            if mode == "any":
                # Path belongs if segment_level appears at least once
                query = f"""
                SELECT
                    {path_col_q},
                    MAX(CASE WHEN {segment_name_q} = {sql_value} THEN 1 ELSE 0 END) AS belongs
                FROM df
                GROUP BY {path_col_q}
                """
                result = engine.run(query, df=df)

            elif mode == "all":
                # Path belongs if segment_level is the only value in the segment column
                query = f"""
                WITH path_segment_levels AS (
                    SELECT
                        {path_col_q},
                        COUNT(DISTINCT {segment_name_q}) AS distinct_values,
                        MAX(CASE WHEN {segment_name_q} = {sql_value} THEN 1 ELSE 0 END) AS has_target
                    FROM df
                    GROUP BY {path_col_q}
                )
                SELECT
                    {path_col_q},
                    CASE WHEN distinct_values = 1 AND has_target = 1 THEN 1 ELSE 0 END AS belongs
                FROM path_segment_levels
                """
                result = engine.run(query, df=df)

            elif mode == "event_share":
                # Path belongs if segment_level appears in at least N% of events
                query = f"""
                WITH path_counts AS (
                    SELECT
                        {path_col_q},
                        COUNT(*) AS total_events,
                        SUM(CASE WHEN {segment_name_q} = {sql_value} THEN 1 ELSE 0 END) AS target_events
                    FROM df
                    GROUP BY {path_col_q}
                )
                SELECT
                    {path_col_q},
                    CASE WHEN CAST(target_events AS DOUBLE) / total_events >= {threshold} THEN 1 ELSE 0 END AS belongs
                FROM path_counts
                """
                result = engine.run(query, df=df)
            else:
                continue

            result_dfs.append(
                result.set_index(path_col).rename(columns={"belongs": metric_name})
            )

        if not result_dfs:
            return pd.DataFrame(columns=metric_names)

        return pd.concat(result_dfs, axis=1)
