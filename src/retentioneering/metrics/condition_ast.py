"""
Shared metric-condition AST parsing and SQL translation.

A "condition" is a tree of comparison leaves (`{"op", "metric", "metric_args",
"value"}`) connected by `and`/`or`/`not` branch nodes, evaluated against
precomputed metric columns (named per `metrics/metric_builder.py`'s naming
convention). Two data processors need this: `filter_paths` (keep/drop whole
paths matching a condition) and `collapse_events` (name a collapsed session
based on a condition over the events within it, in `event_groups[].cases`).
Both consumers are equal users of this module - neither owns it.
"""

from typing import Any, Dict, List, Set

from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.metrics.metric_builder import combined_metric_name

# Metrics that produce multiple columns and can never appear in a single
# comparison/IN leaf condition, regardless of operator.
FORBIDDEN_IN_CONDITIONS = {"has_event_bulk", "event_count_bulk"}


def check_not_forbidden_in_condition(metric: str, processor_name: str) -> None:
    if metric in FORBIDDEN_IN_CONDITIONS:
        raise PreprocessingConfigError(
            processor_name,
            f"'{metric}' produces multiple columns and cannot appear in a "
            f"condition. Use the singular '{metric.replace('_bulk', '')}', or "
            f"'has_all_events'/'has_any_event'.",
        )


def build_metric_names(
    metric: str, metric_args: Dict[str, Any] | None, processor_name: str
) -> List[str]:
    """
    Builds full metric name(s) from metric type and arguments.
    Returns a list of metric names (always exactly one, except for in_segment
    with a list segment_value).

    Examples:
        - metric="length", metric_args=None -> ["length"]
        - metric="has_event", metric_args={"event": "purchase"} -> ["has_event_purchase"]
        - metric="event_count", metric_args={"event": "purchase"} -> ["event_count_purchase"]
        - metric="has_all_events", metric_args={"events": ["logout", "cancellation"]}
          -> ["has_all_events_logout_and_cancellation"]
        - metric="has_any_event", metric_args={"events": ["logout", "cancellation"]}
          -> ["has_any_event_logout_or_cancellation"]
        - metric="time_between", metric_args={"start_event": "login", "end_event": "purchase"}
          -> ["time_from_login_to_purchase"]
        - metric="active_days", metric_args={"active_events": ["login", "purchase"]}
          -> ["active_days"]
    """
    metric_args = metric_args or {}

    if metric == "has_event":
        event = metric_args.get("event")
        if not event:
            raise PreprocessingConfigError(
                processor_name, "'has_event' metric requires 'event' in metric_args"
            )
        return [f"has_event_{event}"]

    elif metric == "event_count":
        event = metric_args.get("event")
        if not event:
            raise PreprocessingConfigError(
                processor_name, "'event_count' metric requires 'event' in metric_args"
            )
        return [f"event_count_{event}"]

    elif metric in ("has_event_bulk", "event_count_bulk"):
        raise PreprocessingConfigError(
            processor_name,
            f"'{metric}' produces one column per event and cannot be used in a "
            f"filter_paths/collapse_events condition. Use "
            f"'{metric.replace('_bulk', '')}' for a single event, or "
            f"'has_all_events'/'has_any_event' for a multi-event condition.",
        )

    elif metric in ("has_all_events", "has_any_event"):
        events = metric_args.get("events")
        if not isinstance(events, list) or len(events) == 0:
            raise PreprocessingConfigError(
                processor_name,
                f"'{metric}' metric requires a non-empty 'events' list in metric_args",
            )
        return [combined_metric_name(metric, events)]

    elif not metric_args:
        return [metric]

    elif metric == "time_between":
        start_event = metric_args.get("start_event")
        end_event = metric_args.get("end_event")
        return [f"time_from_{start_event}_to_{end_event}"]

    elif metric == "active_days":
        return ["active_days"]

    elif metric == "matches_pattern":
        pattern = metric_args.get("pattern")
        return [f"matches_pattern_{pattern}"]

    elif metric in {"length", "duration"}:
        return [metric]

    elif metric == "in_segment":
        segment_name = metric_args.get("segment_name")
        segment_value = metric_args.get("segment_value")
        mode = metric_args.get("mode", "any")
        if segment_value is None:
            raise PreprocessingConfigError(
                processor_name,
                "'in_segment' metric with segment_value=None cannot be used in condition "
                "(column names are not known until runtime)",
            )
        if isinstance(segment_value, list):
            return [f"in_segment_{segment_name}_{v}_{mode}" for v in segment_value]
        return [f"in_segment_{segment_name}_{segment_value}_{mode}"]

    else:
        raise PreprocessingConfigError(processor_name, f"Unknown metric type: {metric}")


def extract_metric_configs(
    node: Dict[str, Any], processor_name: str
) -> List[Dict[str, Any]]:
    """
    Extracts all unique metric configurations from the AST.
    Returns a list of dicts with 'metric' and 'metric_args' fields.
    """
    configs: List[Dict[str, Any]] = []
    seen_keys: Set[str] = set()

    def walk(n: Dict[str, Any]):
        op = n.get("op")
        if op.upper() in {"AND", "OR"}:
            for a in n.get("args", []):
                walk(a)
        elif op.upper() == "NOT":
            for a in n.get("args", []):
                walk(a)
        else:
            # IN node or comparison node - both reference a single metric leaf
            metric = n.get("metric")
            if metric:
                check_not_forbidden_in_condition(metric, processor_name)
                metric_args = n.get("metric_args")
                # Create unique key for deduplication
                key = (metric, str(sorted((metric_args or {}).items())))
                if key not in seen_keys:
                    seen_keys.add(key)
                    configs.append({"metric": metric, "metric_args": metric_args or {}})

    walk(node)
    return configs


def _quote_ident(ident: str, processor_name: str) -> str:
    # Simple identifier quoting for DuckDB
    if not isinstance(ident, str):
        raise PreprocessingConfigError(processor_name, "Identifier must be a string")
    escaped = ident.replace('"', '""')
    return f'"{escaped}"'


def _literal_sql(value: Any, processor_name: str) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    raise PreprocessingConfigError(
        processor_name, f"Unsupported literal type in condition: {type(value)}"
    )


def ast_to_sql(node: Dict[str, Any], processor_name: str) -> str:
    op = node.get("op")
    if op == "==":  # Python-style equality is accepted as an alias for "="
        op = "="
    if op.upper() in {"AND", "OR"}:
        args = node.get("args", [])
        if not args:
            raise PreprocessingConfigError(
                processor_name, f"Logical operator '{op}' requires args"
            )
        joiner = " AND " if op.upper() == "AND" else " OR "
        return "(" + joiner.join(ast_to_sql(a, processor_name) for a in args) + ")"
    elif op.upper() == "NOT":
        args = node.get("args", [])
        if len(args) != 1:
            raise PreprocessingConfigError(
                processor_name, "'NOT' operator requires exactly one arg"
            )
        return "(NOT " + ast_to_sql(args[0], processor_name) + ")"
    elif op.upper() == "IN":
        metric = node.get("metric")
        if not metric:
            raise PreprocessingConfigError(
                processor_name, "'IN' operator requires 'metric' field"
            )

        metric_args = node.get("metric_args")
        metric_names = build_metric_names(metric, metric_args, processor_name)

        # Check if we have multiple metrics (list case for in_segment)
        if len(metric_names) > 1:
            raise PreprocessingConfigError(
                processor_name,
                "'IN' operator not supported with multiple metrics (list events)",
            )

        metric_name = metric_names[0]
        values = node.get("value")
        if values is None:
            values = node.get("args")
        if not isinstance(values, list) or len(values) == 0:
            raise PreprocessingConfigError(
                processor_name,
                "'IN' operator requires non-empty list of values in 'value' or 'args'",
            )

        left_sql = _quote_ident(metric_name, processor_name)

        # Map values to SQL literals, with special mapping for has_* boolean to 1/0
        def map_value(v: Any) -> str:
            if metric_name.startswith("has_") and isinstance(v, bool):
                return "1" if v else "0"
            return _literal_sql(v, processor_name)

        values_sql = ", ".join(map_value(v) for v in values)
        return f"({left_sql} IN ({values_sql}))"

    if op not in {"=", "!=", ">", "<", ">=", "<="}:
        raise PreprocessingConfigError(
            processor_name, f"Unsupported comparison operator: {op}"
        )

    # comparison node
    metric = node.get("metric")
    if not metric:
        raise PreprocessingConfigError(
            processor_name, "Comparison nodes must have 'metric' field"
        )

    metric_args = node.get("metric_args")
    metric_names = build_metric_names(metric, metric_args, processor_name)
    value = node.get("value")

    if value is None:
        raise PreprocessingConfigError(
            processor_name, "Comparison nodes must have 'value' field"
        )
    if not isinstance(value, (str, int, float, bool)):
        raise PreprocessingConfigError(
            processor_name, "Comparison nodes must have primitive 'value'"
        )

    # Single metric case. has_event/event_count are strict single-event and
    # has_all_events/has_any_event are single-column, so metric_names always has
    # exactly one entry for those. in_segment with a list segment_value can still
    # yield more than one name here; using only metric_names[0] in that case is a
    # pre-existing, unrelated quirk (out of scope for this refactor).
    metric_name = metric_names[0]
    left_sql = _quote_ident(metric_name, processor_name)

    # Handle boolean values for has_* metrics
    if metric_name.startswith("has_") and isinstance(value, bool):
        right_sql = "1" if value else "0"
    elif isinstance(value, str):
        # String values are treated as literals
        right_sql = _literal_sql(value, processor_name)
    else:
        right_sql = _literal_sql(value, processor_name)

    return f"({left_sql} {op} {right_sql})"
