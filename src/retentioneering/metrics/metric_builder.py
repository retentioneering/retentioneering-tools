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
  - segment_value: segment value (string, list of strings, or None for all values)
  - mode:
    - any: segment_value appears at least once
    - all: segment_value is the only value in the segment column
    - event_share: segment_value appears in at least N% of events
      - threshold: percentage of events (e.g., 0.1 for 10%)
"""

import re
from typing import Any, Dict, List, Set

import pandas as pd

from retentioneering import engine
from retentioneering.engine import dialect
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import InvalidMetricConfigError
from retentioneering.utils.sequences import generate_patterns_with_optional_gaps
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
}

# Valid modes for the in_segment metric
IN_SEGMENT_MODES = {
    "any",  # segment_value appears at least once
    "all",  # segment_value is the only value in the segment column
    "event_share",  # segment_value appears in at least N% of events
}


# Special synthetic events that don't need to exist in the eventstream
SYNTHETIC_EVENTS = {EventTypes().PATH_START.name, EventTypes().PATH_END.name}


def _pattern_variant_to_regex(pattern_variant: str) -> str:
    """Converts one gap-expanded matches_pattern variant (still '->'-joined, with
    literal '.*' tokens for enabled gaps) into a regex that matches whole tokens
    only, not substrings. Every literal token is re.escape()'d; '.*' gap tokens
    are left as raw regex (that part already works correctly - it's only the
    literal tokens that need escaping/anchoring). The whole regex is wrapped in a
    leading/trailing '->' to align with the same padding added to the built path
    string, giving token-boundary anchoring without needing lookaround (DuckDB's
    regexp_matches uses RE2, which doesn't support it)."""
    tokens = pattern_variant.split("->")
    parts = [".*" if tok == ".*" else re.escape(tok) for tok in tokens]
    return "->" + "->".join(parts) + "->"


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

        Note: For in_segment metric with segment_value=None, cols will be empty list
        because the actual columns are determined at build time.
        """
        enriched = []
        for parsed in self.parsed_configs:
            original = parsed["original"].copy()
            # Handle case when metric_names is None (e.g., in_segment with segment_value=None)
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
            pattern = metric_args.get("pattern")
            if not pattern:
                raise InvalidMetricConfigError(
                    "'matches_pattern' metric requires 'pattern' in metric_args"
                )
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
            start_event = metric_args.get("start_event")
            end_event = metric_args.get("end_event")
            if not start_event or not end_event:
                raise InvalidMetricConfigError(
                    "'time_between' metric requires 'start_event' and 'end_event' in metric_args"
                )
            return {
                "type": "time_from_to",
                "start_event": start_event,
                "end_event": end_event,
                "metric_names": [f"time_from_{start_event}_to_{end_event}"],
                "original": config_dict,
            }
        elif metric == "in_segment":
            segment_name = metric_args.get("segment_name")
            segment_value = metric_args.get(
                "segment_value"
            )  # Can be None, scalar (str/int/float/bool), or list
            mode = metric_args.get("mode", "any")

            if not segment_name:
                raise InvalidMetricConfigError(
                    "'in_segment' metric requires 'segment_name' in metric_args"
                )
            if mode not in IN_SEGMENT_MODES:
                raise InvalidMetricConfigError(
                    f"'in_segment' metric has invalid mode '{mode}'. Valid modes: {sorted(IN_SEGMENT_MODES)}"
                )

            # Normalize segment_values to list or None (for all values)
            if segment_value is None:
                # Will be resolved later to all unique values in the segment column
                segment_values = None
                metric_names = None  # Will be set later when we know the actual values
            elif isinstance(segment_value, list):
                if len(segment_value) == 0:
                    raise InvalidMetricConfigError(
                        "'in_segment' metric requires non-empty 'segment_value' list"
                    )
                segment_values = segment_value
                metric_names = [
                    f"in_segment_{segment_name}_{v}_{mode}" for v in segment_values
                ]
            elif isinstance(segment_value, (str, int, float, bool)):
                # Single scalar value (string, number, or boolean)
                segment_values = [segment_value]
                metric_names = [f"in_segment_{segment_name}_{segment_value}_{mode}"]
            else:
                raise InvalidMetricConfigError(
                    f"'in_segment' metric 'segment_value' must be string, number, boolean, list, or None. Got: {type(segment_value).__name__}"
                )

            result = {
                "type": "in_segment",
                "segment_name": segment_name,
                "segment_values": segment_values,  # None means all values
                "mode": mode,
                "metric_names": metric_names,  # None if segment_values is None
                "original": config_dict,
            }

            # Add mode-specific parameters
            if mode == "event_share":
                threshold = metric_args.get("threshold")
                if threshold is None:
                    raise InvalidMetricConfigError(
                        "'in_segment' metric with mode 'event_share' requires 'threshold' (e.g., 0.1 for 10%)"
                    )
                result["threshold"] = threshold

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

        Delegates the actual per-metric checks (required fields, valid modes/
        ranges, event/segment existence) to `metric_schema.py`'s registry —
        this method's only job is the 'metric' field check (shared by every
        metric) and assembling the runtime `ValidationContext`.
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
            event = metric_args.get("event")
            if not event or not isinstance(event, str):
                raise InvalidMetricConfigError(
                    f"'{metric_name}' metric requires a single 'event' (string) in metric_args"
                )
            if event not in available_events:
                raise InvalidMetricConfigError(
                    f"Event '{event}' not found. Available events: {sorted(available_events)}"
                )

        elif metric_name in ("event_count_bulk", "has_event_bulk"):
            # Omitted/None means "all events" (like active_days' active_events) - nothing to validate.
            # An explicit empty list is invalid (see _normalize_events_bulk).
            events = metric_args.get("events")
            if events is not None:
                if not isinstance(events, list) or len(events) == 0:
                    raise InvalidMetricConfigError(
                        f"'{metric_name}' metric 'events' must be a non-empty list, "
                        f"or omitted/None for all events"
                    )
                for e in events:
                    if e not in available_events:
                        raise InvalidMetricConfigError(
                            f"Event '{e}' not found. Available events: {sorted(available_events)}"
                        )

        elif metric_name in ("has_all_events", "has_any_event"):
            events = metric_args.get("events")
            if not isinstance(events, list) or len(events) == 0:
                raise InvalidMetricConfigError(
                    f"'{metric_name}' metric requires a non-empty 'events' list in metric_args"
                )
            for e in events:
                if e not in available_events:
                    raise InvalidMetricConfigError(
                        f"Event '{e}' not found. Available events: {sorted(available_events)}"
                    )

        elif metric_name == "time_between":
            start_event = metric_args.get("start_event")
            end_event = metric_args.get("end_event")
            if not start_event or not end_event:
                raise InvalidMetricConfigError(
                    "'time_between' metric requires 'start_event' and 'end_event' in metric_args"
                )
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
            pattern = metric_args.get("pattern")
            if not pattern:
                raise InvalidMetricConfigError(
                    "'matches_pattern' metric requires 'pattern' in metric_args"
                )
            for tok in pattern.split("->"):
                if tok == ".*" or tok in SYNTHETIC_EVENTS:
                    continue
                if tok not in available_events:
                    raise InvalidMetricConfigError(
                        f"Event '{tok}' in pattern '{pattern}' not found. "
                        f"Available events: {sorted(available_events)}"
                    )

        elif metric_name == "in_segment":
            segment_name = metric_args.get("segment_name")
            segment_value = metric_args.get(
                "segment_value"
            )  # Can be None, string, or list
            mode = metric_args.get("mode", "any")

            if not segment_name:
                raise InvalidMetricConfigError(
                    "'in_segment' metric requires 'segment_name' in metric_args"
                )
            if mode not in IN_SEGMENT_MODES:
                raise InvalidMetricConfigError(
                    f"'in_segment' metric has invalid mode '{mode}'. Valid modes: {sorted(IN_SEGMENT_MODES)}"
                )

            # Validate that segment column exists
            if segment_name not in self.schema.segment_cols:
                raise InvalidMetricConfigError(
                    f"Segment '{segment_name}' not found. Available segments: {self.schema.segment_cols}"
                )

            # Get available segment values
            available_segment_values = set(self.df[segment_name].unique().tolist())

            # Validate segment_value if provided
            if segment_value is not None:
                # Normalize to list for validation
                if isinstance(segment_value, list):
                    values_to_check = segment_value
                else:
                    # Single scalar value (str, int, float, bool)
                    values_to_check = [segment_value]

                for v in values_to_check:
                    if v not in available_segment_values:
                        raise InvalidMetricConfigError(
                            f"Segment value '{v}' not found in segment '{segment_name}'. "
                            f"Available values: {sorted(str(x) for x in available_segment_values)}"
                        )

            # Validate mode-specific parameters
            if mode == "event_share":
                threshold = metric_args.get("threshold")
                if threshold is None:
                    raise InvalidMetricConfigError(
                        "'event_share' mode requires 'threshold' (e.g., 0.1 for 10%)"
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

        Matches whole tokens, not substrings: both the path and every literal
        segment of the pattern are '->'-delimited token sequences, and each
        literal token is re.escape()'d before being compiled into the regex.
        DuckDB's regexp_matches uses RE2, which has no lookaround support, so
        token-boundary anchoring is done by padding both the path and the regex
        with a leading/trailing '->' delimiter (see _pattern_variant_to_regex).
        """
        pattern = config["pattern"]
        metric_name = config["metric_names"][0]

        # Lazy load dataframe with start_end events (needed for path_start/path_end in patterns)
        if self.df_with_start_end is None:
            self.df_with_start_end = self.eventstream.add_start_end_events(
                path_col=path_col
            ).df

        # Build path strings for each path_id, padded with a leading/trailing '->'
        # so every event (including the first/last) is flanked by the delimiter.
        path_col_q = engine.quote_ident(path_col)
        event_col_q = engine.quote_ident(event_col)
        query = f"""
        SELECT {path_col_q}, '->' || {dialect.path_agg(event_col_q)} || '->' as path
        FROM df_with_start_end
        GROUP BY {path_col_q}
        """
        paths = engine.run(query, df_with_start_end=self.df_with_start_end)

        # Generate patterns with optional gaps
        patterns = generate_patterns_with_optional_gaps(pattern)
        patterns_chunk = " OR ".join(
            dialect.regexp_match("path", quote_literal(_pattern_variant_to_regex(p)))
            for p in patterns
        )

        metric_name_q = engine.quote_ident(metric_name)
        query = f"select {path_col_q}, {patterns_chunk} as {metric_name_q} from paths"
        result = engine.run(query, paths=paths)
        return result.set_index(path_col)

    def _build_in_segment(self, config: Dict[str, Any], path_col: str) -> pd.DataFrame:
        """
        Builds in_segment metric (0/1) for each path.

        Determines if a path belongs to a segment based on different modes:
        - any: segment_value appears at least once
        - all: segment_value is the only value in the segment column
        - event_share: segment_value appears in at least N% of events

        Can handle multiple segment values at once, generating one column per value.
        If segment_values is None, generates metrics for all unique values in the segment.
        """
        segment_name = config["segment_name"]
        segment_values = config["segment_values"]
        mode = config["mode"]

        df = self.df

        # Resolve segment_values if None (use all unique values, keep original types)
        if segment_values is None:
            unique_values = df[segment_name].unique().tolist()
            # Sort with string representation for consistent ordering
            segment_values = sorted(unique_values, key=lambda x: str(x))

        # Build metric names
        metric_names = [f"in_segment_{segment_name}_{v}_{mode}" for v in segment_values]

        # Build metrics for each segment value
        result_dfs = []
        path_col_q = engine.quote_ident(path_col)
        segment_name_q = engine.quote_ident(segment_name)

        for segment_value, metric_name in zip(segment_values, metric_names):
            # Format value for SQL comparison
            sql_value = quote_literal(segment_value)

            if mode == "any":
                # Path belongs if segment_value appears at least once
                query = f"""
                SELECT
                    {path_col_q},
                    MAX(CASE WHEN {segment_name_q} = {sql_value} THEN 1 ELSE 0 END) AS belongs
                FROM df
                GROUP BY {path_col_q}
                """
                result = engine.run(query, df=df)

            elif mode == "all":
                # Path belongs if segment_value is the only value in the segment column
                query = f"""
                WITH path_segment_values AS (
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
                FROM path_segment_values
                """
                result = engine.run(query, df=df)

            elif mode == "event_share":
                # Path belongs if segment_value appears in at least N% of events
                threshold = config["threshold"]
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
