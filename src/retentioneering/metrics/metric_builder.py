"""
MetricBuilder - module for building metrics for each path in the eventstream

Supports metrics:
- length - number of steps (events)
- duration - duration in seconds between first and last event
- event_count - event count for specific event(s)
  - events: event name (string or list of strings); list of events returns multiple metrics;
    omit (or pass None/[]) to count every event in the eventstream
- has_event - event presence (0/1) for specific event(s)
  - events: event name (string or list of strings); list of events returns multiple metrics;
    omit (or pass None/[]) to check every event in the eventstream
- time_between - time in seconds between first occurrences of two events
  - start_event: event name
  - end_event: event name
- first_event_time - timestamp of first event
- active_days - number of unique days with at least one event
  - active_events: event name (string or list of strings)
- matches_pattern - whether path matches a regex pattern (0/1)
  - pattern: string pattern like "login->.*->purchase"
- in_segment - binary metric showing if a path belongs to a segment value (0/1)
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

from retentioneering.exceptions import InvalidMetricConfigError
from retentioneering.metrics.metric_schema import (
    METRIC_SCHEMAS,
    ValidationContext,
    parse_metric_args,
    validate_metric_args,
)
from retentioneering.utils.sequences import generate_patterns_with_optional_gaps


# Valid metric names — derived from the schema registry, not hand-maintained.
VALID_METRICS = set(METRIC_SCHEMAS)


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


class MetricConfig:
    """Parser and validator for metric configurations"""

    def __init__(self, config_items: List[Dict[str, Any]]):
        self.config_items = config_items
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
        """Parses a dict-based configuration. Raises InvalidMetricConfigError on invalid config.

        Delegates the actual per-metric shape to `metric_schema.py`'s registry —
        this method's only job is the 'metric' field check (shared by every
        metric) and assembling the result dict `_build_metric` expects.
        """
        metric = config_dict.get("metric")
        metric_args = config_dict.get("metric_args", {})

        if not metric:
            raise InvalidMetricConfigError(
                f"Config missing 'metric' field: {config_dict}"
            )

        fields = parse_metric_args(metric, metric_args)
        schema = METRIC_SCHEMAS[metric]
        return {
            "type": schema.build_type,
            "metric_names": schema.metric_names(fields),
            **fields,
            "original": config_dict,
        }


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
        ctx = ValidationContext(
            available_events=available_events,
            segment_cols=self.schema.segment_cols,
            segment_values=lambda col: set(self.df[col].unique().tolist()),
        )
        validate_metric_args(metric_name, metric_args, ctx)

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

        metric_config = MetricConfig(config)

        available_events = set(self.df[event_col].unique().tolist())
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

    def _build_event_count(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds event count metrics for one or more events"""
        event_names = config["event_names"]
        if event_names is None:
            # Wildcard: 'events' was omitted/None/[] - count every event in the stream.
            event_names = sorted(self.df[event_col].unique().tolist())

        events_quoted = ", ".join([f"'{e}'" for e in event_names])
        query = f"""
        SELECT
            {path_col},
            {event_col},
            count(*) as count
        FROM df
        WHERE {event_col} IN ({events_quoted})
        GROUP BY {path_col}, {event_col}
        """

        df = self.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()

        if result.empty:
            return pd.DataFrame(columns=[f"event_count_{e}" for e in event_names])

        # Pivot: path_id as index, events as columns
        metrics_df = (
            result.set_index([path_col, event_col])["count"]
            .unstack(fill_value=0)
            .reindex(columns=event_names, fill_value=0)
        )
        metrics_df.columns = [f"event_count_{e}" for e in metrics_df.columns]

        return metrics_df

    def _build_has(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds event presence metrics (0/1) for one or more events"""

        metrics_df = self._build_event_count(config, path_col, event_col)
        metrics_df = (metrics_df > 0).astype(int)
        metrics_df.columns = [
            col.replace("event_count_", "has_event_") for col in metrics_df.columns
        ]

        return metrics_df

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

        query = f"""
        WITH first_events AS (
            SELECT
                {path_col},
                MIN(CASE WHEN {event_col} = '{start_event}' THEN {timestamp_col} END) as time_from,
                MIN(CASE WHEN {event_col} = '{end_event}' THEN {timestamp_col} END) as time_to
            FROM df_with_start_end
            GROUP BY {path_col}
        )
        SELECT
            {path_col},
            EPOCH(time_to - time_from) as time_diff_seconds
        FROM first_events
        WHERE time_from IS NOT NULL AND time_to IS NOT NULL
        """

        df_with_start_end = self.df_with_start_end  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()

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
        query = f"""
        SELECT {path_col}, COUNT(*) AS length
        FROM df
        GROUP BY {path_col}
        """
        df = self.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()
        return result.set_index(path_col)

    def _build_duration(self, path_col: str) -> pd.DataFrame:
        """Duration in seconds between first and last event per path"""
        timestamp_col = self.schema.timestamp_col
        query = f"""
        SELECT {path_col}, EPOCH(MAX({timestamp_col}) - MIN({timestamp_col})) AS duration
        FROM df
        GROUP BY {path_col}
        """
        df = self.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()
        result["duration"] = result["duration"].astype(float)
        return result.set_index(path_col)

    def _build_first_event_time(self, path_col: str) -> pd.DataFrame:
        """Unix timestamp (seconds) of first event per path.
        Stored as float so mean/median/percentile aggregations work correctly."""
        timestamp_col = self.schema.timestamp_col
        query = f"""
        SELECT {path_col}, EPOCH(MIN({timestamp_col})) AS first_event_time
        FROM df
        GROUP BY {path_col}
        """
        df = self.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()
        result["first_event_time"] = result["first_event_time"].astype(float)
        return result.set_index(path_col)

    def _build_active_days(self, path_col: str, active_events=None) -> pd.DataFrame:
        """Number of unique days with at least one (matching) event per path.
        active_events: optional list of events to count; if None, all events count."""
        timestamp_col = self.schema.timestamp_col
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
        SELECT {path_col}, {count_expr} AS active_days
        FROM df
        GROUP BY {path_col}
        """
        df = self.df  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        result = duckdb.query(query).df()
        return result.set_index(path_col)

    def _build_matches(
        self, config: Dict[str, Any], path_col: str, event_col: str
    ) -> pd.DataFrame:
        """Builds pattern matching metric (0/1) for each path"""
        pattern = config["pattern"]
        metric_name = config["metric_names"][0]

        # Lazy load dataframe with start_end events (needed for path_start/path_end in patterns)
        if self.df_with_start_end is None:
            self.df_with_start_end = self.eventstream.add_start_end_events(
                path_col=path_col
            ).df

        # Build path strings for each path_id
        query = f"""
        SELECT {path_col}, string_agg({event_col}, '->') as path
        FROM df_with_start_end
        GROUP BY {path_col}
        """
        # df_with_start_end and paths are needed for DuckDB scripts execution
        df_with_start_end = self.df_with_start_end  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string
        paths = duckdb.query(query).df()  # noqa: F841 -- referenced by name via DuckDB replacement scan in the SQL string

        # Generate patterns with optional gaps
        patterns = generate_patterns_with_optional_gaps(pattern)
        patterns_chunk = " OR ".join(
            f"regexp_matches(path, {format_value_for_sql(p)})" for p in patterns
        )

        query = f'select {path_col}, {patterns_chunk} as "{metric_name}" from paths'
        result = duckdb.sql(query).df()
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

        for segment_value, metric_name in zip(segment_values, metric_names):
            # Format value for SQL comparison
            sql_value = format_value_for_sql(segment_value)

            if mode == "any":
                # Path belongs if segment_value appears at least once
                query = f"""
                SELECT
                    {path_col},
                    MAX(CASE WHEN {segment_name} = {sql_value} THEN 1 ELSE 0 END) AS belongs
                FROM df
                GROUP BY {path_col}
                """
                result = duckdb.query(query).df()

            elif mode == "all":
                # Path belongs if segment_value is the only value in the segment column
                query = f"""
                WITH path_segment_values AS (
                    SELECT
                        {path_col},
                        COUNT(DISTINCT {segment_name}) AS distinct_values,
                        MAX(CASE WHEN {segment_name} = {sql_value} THEN 1 ELSE 0 END) AS has_target
                    FROM df
                    GROUP BY {path_col}
                )
                SELECT
                    {path_col},
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
                        {path_col},
                        COUNT(*) AS total_events,
                        SUM(CASE WHEN {segment_name} = {sql_value} THEN 1 ELSE 0 END) AS target_events
                    FROM df
                    GROUP BY {path_col}
                )
                SELECT
                    {path_col},
                    CASE WHEN CAST(target_events AS DOUBLE) / total_events >= {threshold} THEN 1 ELSE 0 END AS belongs
                FROM path_counts
                """
                result = duckdb.query(query).df()
            else:
                continue

            result_dfs.append(
                result.set_index(path_col).rename(columns={"belongs": metric_name})
            )

        if not result_dfs:
            return pd.DataFrame(columns=metric_names)

        return pd.concat(result_dfs, axis=1)
