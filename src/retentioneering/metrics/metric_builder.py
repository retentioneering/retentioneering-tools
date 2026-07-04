"""
MetricBuilder - module for building metrics for each path in the eventstream

Supports metrics:
- length - number of steps (events)
- duration - duration in seconds between first and last event
- event_count - event count for specific event(s)
  - events: event name (string or list of strings); list of events returns multiple metrics
- has - event presence (0/1) for specific event(s)
  - events: event name (string or list of strings); list of events returns multiple metrics
- time_between - time in seconds between first occurrences of two events
  - event_from: event name
  - event_to: event name
- first_event_dt - timestamp of first event
- active_days - number of unique days with at least one event
  - active_events: event name (string or list of strings)
- matches - whether path matches a regex pattern (0/1)
  - pattern: string pattern like "login->.*->purchase"
- belongs_to - binary metric showing if a path belongs to a segment (0/1)
  - segment_name: segment column name
  - segment_value: segment value (string, list of strings, or None for all values)
  - mode:
    - any: segment_value appears at least once
    - all: segment_value is the only value in the segment column
    - event_share: segment_value appears in at least N% of events
      - threshold: percentage of events (e.g., 0.1 for 10%)
"""

from typing import Any, Dict, List, Set

import duckdb
import pandas as pd

from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import InvalidMetricConfigError
from retentioneering.utils.sequences import generate_patterns_with_optional_gaps


# Valid metric names
VALID_METRICS = {
    "length",
    "duration",
    "event_count",
    "has",
    "time_between",
    "first_event_dt",
    "active_days",
    "matches",
    "belongs_to",
}

# Valid modes for belongs_to metric
BELONGS_TO_MODES = {
    "any",  # segment_value appears at least once
    "all",  # segment_value is the only value in the segment column
    "event_share",  # segment_value appears in at least N% of events
}


def format_value_for_sql(value) -> str:
    """
    Format a Python value for use in SQL query.

    Handles strings, numbers, booleans, and None.
    """
    if value is None:
        return "NULL"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, str):
        # Escape single quotes in strings
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        # Fallback: convert to string
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"


# Special synthetic events that don't need to exist in the eventstream
SYNTHETIC_EVENTS = {EventTypes().PATH_START.name, EventTypes().PATH_END.name}


class MetricConfig:
    """Parser and validator for metric configurations"""

    def __init__(self, config_items: List[Dict[str, Any]]):
        self.config_items = config_items
        self.parsed_configs = self._parse_configs()

    def get_enriched_configs(self) -> List[Dict[str, Any]]:
        """
        Returns original configs enriched with 'cols' key containing column names.

        Example:
            Input: [{"metric": "has", "metric_args": {"events": ["A", "B"]}, "agg": "mean"}]
            Output: [{"metric": "has", "metric_args": {"events": ["A", "B"]}, "agg": "mean", "cols": ["has_A", "has_B"]}]

        Note: For belongs_to metric with segment_value=None, cols will be empty list
        because the actual columns are determined at build time.
        """
        enriched = []
        for parsed in self.parsed_configs:
            original = parsed["original"].copy()
            # Handle case when metric_names is None (e.g., belongs_to with segment_value=None)
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
        elif metric == "first_event_dt":
            return {
                "type": "first_event_dt",
                "metric_names": ["first_event_dt"],
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
        elif metric == "matches":
            pattern = metric_args.get("pattern")
            if not pattern:
                raise InvalidMetricConfigError(
                    "'matches' metric requires 'pattern' in metric_args"
                )
            return {
                "type": "matches",
                "pattern": pattern,
                "metric_names": [f"matches_{pattern}"],
                "original": config_dict,
            }
        elif metric == "has":
            events = metric_args.get("events")
            if not events:
                raise InvalidMetricConfigError(
                    "'has' metric requires 'events' in metric_args"
                )

            if isinstance(events, str):
                event_names = [events]
            elif isinstance(events, list):
                if len(events) == 0:
                    raise InvalidMetricConfigError(
                        "'has' metric requires non-empty 'events' list"
                    )
                event_names = events
            else:
                raise InvalidMetricConfigError(
                    "'has' metric 'events' must be string or list"
                )
            return {
                "type": "has",
                "event_names": event_names,
                "metric_names": [f"has_{e}" for e in event_names],
                "original": config_dict,
            }
        elif metric == "event_count":
            events = metric_args.get("events")
            if not events:
                raise InvalidMetricConfigError(
                    "'event_count' metric requires 'events' in metric_args"
                )

            if isinstance(events, str):
                events = [events]
            elif isinstance(events, list):
                if len(events) == 0:
                    raise InvalidMetricConfigError(
                        "'event_count' metric requires non-empty 'events' list"
                    )
            else:
                raise InvalidMetricConfigError(
                    "'event_count' metric 'events' must be string or list"
                )
            return {
                "type": "event_count",
                "event_names": events,
                "metric_names": [f"event_count_{e}" for e in events],
                "original": config_dict,
            }
        elif metric == "time_between":
            event_from = metric_args.get("event_from")
            event_to = metric_args.get("event_to")
            if not event_from or not event_to:
                raise InvalidMetricConfigError(
                    "'time_between' metric requires 'event_from' and 'event_to' in metric_args"
                )
            return {
                "type": "time_from_to",
                "event_from": event_from,
                "event_to": event_to,
                "metric_names": [f"time_from_{event_from}_to_{event_to}"],
                "original": config_dict,
            }
        elif metric == "belongs_to":
            segment_name = metric_args.get("segment_name")
            segment_value = metric_args.get(
                "segment_value"
            )  # Can be None, scalar (str/int/float/bool), or list
            mode = metric_args.get("mode", "any")

            if not segment_name:
                raise InvalidMetricConfigError(
                    "'belongs_to' metric requires 'segment_name' in metric_args"
                )
            if mode not in BELONGS_TO_MODES:
                raise InvalidMetricConfigError(
                    f"'belongs_to' metric has invalid mode '{mode}'. Valid modes: {sorted(BELONGS_TO_MODES)}"
                )

            # Normalize segment_values to list or None (for all values)
            if segment_value is None:
                # Will be resolved later to all unique values in the segment column
                segment_values = None
                metric_names = None  # Will be set later when we know the actual values
            elif isinstance(segment_value, list):
                if len(segment_value) == 0:
                    raise InvalidMetricConfigError(
                        "'belongs_to' metric requires non-empty 'segment_value' list"
                    )
                segment_values = segment_value
                metric_names = [
                    f"belongs_to_{segment_name}_{v}_{mode}" for v in segment_values
                ]
            elif isinstance(segment_value, (str, int, float, bool)):
                # Single scalar value (string, number, or boolean)
                segment_values = [segment_value]
                metric_names = [f"belongs_to_{segment_name}_{segment_value}_{mode}"]
            else:
                raise InvalidMetricConfigError(
                    f"'belongs_to' metric 'segment_value' must be string, number, boolean, list, or None. Got: {type(segment_value).__name__}"
                )

            result = {
                "type": "belongs_to",
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
                        "'belongs_to' metric with mode 'event_share' requires 'threshold' (e.g., 0.1 for 10%)"
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

        if metric_name not in VALID_METRICS:
            raise InvalidMetricConfigError(
                f"Unknown metric '{metric_name}'. Valid metrics: {sorted(VALID_METRICS)}"
            )

        metric_args = metric.get("metric_args", {})

        if metric_name == "event_count":
            event = metric_args.get("events")
            if not event:
                raise InvalidMetricConfigError(
                    "'event_count' metric requires 'events' in metric_args"
                )
            events_to_check = [event] if isinstance(event, str) else event
            for e in events_to_check:
                if e not in available_events:
                    raise InvalidMetricConfigError(
                        f"Event '{e}' not found. Available events: {sorted(available_events)}"
                    )

        elif metric_name == "has":
            events = metric_args.get("events")
            if not events:
                raise InvalidMetricConfigError(
                    "'has' metric requires 'events' in metric_args"
                )
            events_to_check = [events] if isinstance(events, str) else events
            for e in events_to_check:
                if e not in available_events:
                    raise InvalidMetricConfigError(
                        f"Event '{e}' not found. Available events: {sorted(available_events)}"
                    )

        elif metric_name == "time_between":
            event_from = metric_args.get("event_from")
            event_to = metric_args.get("event_to")
            if not event_from or not event_to:
                raise InvalidMetricConfigError(
                    "'time_between' metric requires 'event_from' and 'event_to' in metric_args"
                )
            # path_start and path_end are synthetic events, don't validate them
            if (
                event_from not in SYNTHETIC_EVENTS
                and event_from not in available_events
            ):
                raise InvalidMetricConfigError(
                    f"Event '{event_from}' not found. Available events: {sorted(available_events)}"
                )
            if event_to not in SYNTHETIC_EVENTS and event_to not in available_events:
                raise InvalidMetricConfigError(
                    f"Event '{event_to}' not found. Available events: {sorted(available_events)}"
                )

        elif metric_name == "matches":
            pattern = metric_args.get("pattern")
            if not pattern:
                raise InvalidMetricConfigError(
                    "'matches' metric requires 'pattern' in metric_args"
                )

        elif metric_name == "belongs_to":
            segment_name = metric_args.get("segment_name")
            segment_value = metric_args.get(
                "segment_value"
            )  # Can be None, string, or list
            mode = metric_args.get("mode", "any")

            if not segment_name:
                raise InvalidMetricConfigError(
                    "'belongs_to' metric requires 'segment_name' in metric_args"
                )
            if mode not in BELONGS_TO_MODES:
                raise InvalidMetricConfigError(
                    f"'belongs_to' metric has invalid mode '{mode}'. Valid modes: {sorted(BELONGS_TO_MODES)}"
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
        self, config: List[Dict[str, Any]], path_id_col: str | None = None
    ) -> pd.DataFrame:
        """
        Main method for building metrics

        Args:
            config: List of metric configuration dicts with 'metric' and optional 'metric_args' fields
            path_id_col: Path ID column (if None, taken from schema)

        Returns:
            DataFrame with path_id as index and metrics as columns
        """
        path_id_col = path_id_col or self.schema.path_col
        event_col = self.schema.event_col

        metric_config = MetricConfig(config)
        path_ids = self.df[path_id_col].unique()

        metric_dfs: List[pd.DataFrame] = []

        # Build metrics for each configuration
        for config_item in metric_config.parsed_configs:
            metric_df = self._build_metric(config_item, path_id_col, event_col)

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

        result_df.index.name = path_id_col
        return result_df

    def _build_metric(
        self, config: Dict[str, Any], path_id_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds specific metric according to configuration"""

        if config["type"] == "event_count":
            return self._build_event_count(config, path_id_col, event_col)
        elif config["type"] == "has":
            return self._build_has(config, path_id_col, event_col)
        elif config["type"] == "time_from_to":
            return self._build_time_from_to(config, path_id_col, event_col)
        elif config["type"] == "matches":
            return self._build_matches(config, path_id_col, event_col)
        elif config["type"] == "length":
            return self._build_length(path_id_col)
        elif config["type"] == "duration":
            return self._build_duration(path_id_col)
        elif config["type"] == "first_event_dt":
            return self._build_first_event_dt(path_id_col)
        elif config["type"] == "active_days":
            return self._build_active_days(path_id_col, config.get("active_events"))
        elif config["type"] == "belongs_to":
            return self._build_belongs_to(config, path_id_col)
        else:
            raise InvalidMetricConfigError(f"Unknown metric type: '{config['type']}'")

    def _build_event_count(
        self, config: Dict[str, Any], path_id_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds event count metrics for one or more events"""
        event_names = config["event_names"]

        events_quoted = ", ".join([f"'{e}'" for e in event_names])
        query = f"""
        SELECT
            {path_id_col},
            {event_col},
            count(*) as count
        FROM df
        WHERE {event_col} IN ({events_quoted})
        GROUP BY {path_id_col}, {event_col}
        """

        df = self.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()

        if result.empty:
            return pd.DataFrame(columns=[f"event_count_{e}" for e in event_names])

        # Pivot: path_id as index, events as columns
        metrics_df = (
            result.set_index([path_id_col, event_col])["count"]
            .unstack(fill_value=0)
            .reindex(columns=event_names, fill_value=0)
        )
        metrics_df.columns = [f"event_count_{e}" for e in metrics_df.columns]

        return metrics_df

    def _build_has(
        self, config: Dict[str, Any], path_id_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds event presence metrics (0/1) for one or more events"""

        metrics_df = self._build_event_count(config, path_id_col, event_col)
        metrics_df = (metrics_df > 0).astype(int)
        metrics_df.columns = [
            col.replace("event_count_", "has_") for col in metrics_df.columns
        ]

        return metrics_df

    def _build_time_from_to(
        self, config: Dict[str, Any], path_id_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds time difference metric between two events (in seconds)"""
        event_from = config["event_from"]
        event_to = config["event_to"]
        timestamp_col = self.schema.timestamp

        # Lazy load dataframe with start_end events (needed for path_start/path_end)
        if self.df_with_start_end is None:
            self.df_with_start_end = self.eventstream.add_start_end_events(
                path_id_col=path_id_col
            ).df

        query = f"""
        WITH first_events AS (
            SELECT
                {path_id_col},
                MIN(CASE WHEN {event_col} = '{event_from}' THEN {timestamp_col} END) as time_from,
                MIN(CASE WHEN {event_col} = '{event_to}' THEN {timestamp_col} END) as time_to
            FROM df_with_start_end
            GROUP BY {path_id_col}
        )
        SELECT
            {path_id_col},
            EPOCH(time_to - time_from) as time_diff_seconds
        FROM first_events
        WHERE time_from IS NOT NULL AND time_to IS NOT NULL
        """

        df_with_start_end = self.df_with_start_end  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()

        metric_name = f"time_from_{event_from}_to_{event_to}"
        if len(result) > 0:
            return result.set_index(path_id_col).rename(
                columns={"time_diff_seconds": metric_name}
            )
        else:
            # Empty result - no paths have both events
            return pd.DataFrame(columns=[metric_name])

    def _build_length(self, path_id_col: str) -> pd.DataFrame:
        """Number of steps (events) per path"""
        query = f"""
        SELECT {path_id_col}, COUNT(*) AS length
        FROM df
        GROUP BY {path_id_col}
        """
        df = self.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()
        return result.set_index(path_id_col)

    def _build_duration(self, path_id_col: str) -> pd.DataFrame:
        """Duration in seconds between first and last event per path"""
        timestamp_col = self.schema.timestamp
        query = f"""
        SELECT {path_id_col}, EPOCH(MAX({timestamp_col}) - MIN({timestamp_col})) AS duration
        FROM df
        GROUP BY {path_id_col}
        """
        df = self.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()
        result["duration"] = result["duration"].astype(float)
        return result.set_index(path_id_col)

    def _build_first_event_dt(self, path_id_col: str) -> pd.DataFrame:
        """Unix timestamp (seconds) of first event per path.
        Stored as float so mean/median/percentile aggregations work correctly."""
        timestamp_col = self.schema.timestamp
        query = f"""
        SELECT {path_id_col}, EPOCH(MIN({timestamp_col})) AS first_event_dt
        FROM df
        GROUP BY {path_id_col}
        """
        df = self.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()
        result["first_event_dt"] = result["first_event_dt"].astype(float)
        return result.set_index(path_id_col)

    def _build_active_days(self, path_id_col: str, active_events=None) -> pd.DataFrame:
        """Number of unique days with at least one (matching) event per path.
        active_events: optional list of events to count; if None, all events count."""
        timestamp_col = self.schema.timestamp
        event_col = self.schema.event_col
        if active_events:
            ev_list = (
                active_events if isinstance(active_events, list) else [active_events]
            )
            quoted = ", ".join(f"'{e}'" for e in ev_list)
            count_expr = f"COUNT(DISTINCT CASE WHEN {event_col} IN ({quoted}) THEN CAST({timestamp_col} AS DATE) END)"
        else:
            count_expr = f"COUNT(DISTINCT CAST({timestamp_col} AS DATE))"
        query = f"""
        SELECT {path_id_col}, {count_expr} AS active_days
        FROM df
        GROUP BY {path_id_col}
        """
        df = self.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()
        return result.set_index(path_id_col)

    def _build_matches(
        self, config: Dict[str, Any], path_id_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds pattern matching metric (0/1) for each path"""
        pattern = config["pattern"]
        metric_name = config["metric_names"][0]

        # Lazy load dataframe with start_end events (needed for path_start/path_end in patterns)
        if self.df_with_start_end is None:
            self.df_with_start_end = self.eventstream.add_start_end_events(
                path_id_col=path_id_col
            ).df

        # Build path strings for each path_id
        query = f"""
        SELECT {path_id_col}, string_agg({event_col}, '->') as path
        FROM df_with_start_end
        GROUP BY {path_id_col}
        """
        # df_with_start_end and paths are needed for DuckDB scripts execution
        df_with_start_end = self.df_with_start_end  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        paths = duckdb.query(query).df()  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string

        # Generate patterns with optional gaps
        patterns = generate_patterns_with_optional_gaps(pattern)
        patterns_chunk = " OR ".join(
            f"regexp_matches(path, {format_value_for_sql(p)})" for p in patterns
        )

        query = f'select {path_id_col}, {patterns_chunk} as "{metric_name}" from paths'
        result = duckdb.sql(query).df()
        return result.set_index(path_id_col)

    def _build_belongs_to(
        self, config: Dict[str, Any], path_id_col: str
    ) -> pd.DataFrame:
        """
        Builds belongs_to metric (0/1) for each path.

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
        metric_names = [f"belongs_to_{segment_name}_{v}_{mode}" for v in segment_values]

        # Build metrics for each segment value
        result_dfs = []

        for segment_value, metric_name in zip(segment_values, metric_names):
            # Format value for SQL comparison
            sql_value = format_value_for_sql(segment_value)

            if mode == "any":
                # Path belongs if segment_value appears at least once
                query = f"""
                SELECT
                    {path_id_col},
                    MAX(CASE WHEN {segment_name} = {sql_value} THEN 1 ELSE 0 END) AS belongs
                FROM df
                GROUP BY {path_id_col}
                """
                result = duckdb.query(query).df()

            elif mode == "all":
                # Path belongs if segment_value is the only value in the segment column
                query = f"""
                WITH path_segment_values AS (
                    SELECT
                        {path_id_col},
                        COUNT(DISTINCT {segment_name}) AS distinct_values,
                        MAX(CASE WHEN {segment_name} = {sql_value} THEN 1 ELSE 0 END) AS has_target
                    FROM df
                    GROUP BY {path_id_col}
                )
                SELECT
                    {path_id_col},
                    CASE WHEN distinct_values = 1 AND has_target = 1 THEN 1 ELSE 0 END AS belongs
                FROM path_segment_values
                """
                result = duckdb.query(query).df()

            elif mode == "event_share":
                # Path belongs if segment_value appears in at least N% of events
                threshold = config["threshold"]
                query = f"""
                WITH path_counts AS (
                    SELECT
                        {path_id_col},
                        COUNT(*) AS total_events,
                        SUM(CASE WHEN {segment_name} = {sql_value} THEN 1 ELSE 0 END) AS target_events
                    FROM df
                    GROUP BY {path_id_col}
                )
                SELECT
                    {path_id_col},
                    CASE WHEN CAST(target_events AS DOUBLE) / total_events >= {threshold} THEN 1 ELSE 0 END AS belongs
                FROM path_counts
                """
                result = duckdb.query(query).df()
            else:
                continue

            result_dfs.append(
                result.set_index(path_id_col).rename(columns={"belongs": metric_name})
            )

        if not result_dfs:
            return pd.DataFrame(columns=metric_names)

        return pd.concat(result_dfs, axis=1)
