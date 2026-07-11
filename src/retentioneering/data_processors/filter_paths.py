import pandas as pd
from typing import Any, Dict, List, Set, Tuple

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.utils.sql_quoting import quote_ident, quote_literal

PROCESSOR_NAME = "filter_paths"


class FilterPaths(DataProcessor):
    condition: Dict[str, Any]
    path_col: str | None
    event_col: str | None

    def __init__(
        self,
        condition: Dict[str, Any],
        path_col: str | None,
        event_col: str | None,
    ) -> None:
        self.condition = condition
        self.path_col = path_col
        self.event_col = event_col
        super().__init__()

    def apply(self, df, schema) -> Tuple[pd.DataFrame, EventstreamSchema]:
        # This method should not be called directly anymore
        # All filtering is done via AST conditions in Eventstream.filter_paths()
        raise PreprocessingConfigError(
            PROCESSOR_NAME,
            "FilterPaths.apply() should not be called directly. Use Eventstream.filter_paths() with condition.",
        )

    @staticmethod
    def _build_metric_names(
        metric: str, metric_args: Dict[str, Any] | None
    ) -> List[str]:
        """
        Builds full metric name(s) from metric type and arguments.
        Returns a list of metric names (usually just one, but can be multiple for has/event_count with list).

        Examples:
            - metric="length", metric_args=None -> ["length"]
            - metric="has_event", metric_args={"events": "purchase"} -> ["has_event_purchase"]
            - metric="has_event", metric_args={"events": ["logout", "cancellation"]} -> ["has_event_logout", "has_event_cancellation"]
            - metric="event_count", metric_args={"events": "purchase"} -> ["event_count_purchase"]
            - metric="event_count", metric_args={"events": ["view", "purchase"]} -> ["event_count_view", "event_count_purchase"]
            - metric="time_between", metric_args={"start_event": "login", "end_event": "purchase"}
              -> ["time_from_login_to_purchase"]
            - metric="active_days", metric_args={"active_events": ["login", "purchase"]}
              -> ["active_days"]
        """
        if not metric_args:
            return [metric]

        if metric == "has_event":
            events = metric_args.get("events")
            if isinstance(events, list):
                return [f"has_event_{event}" for event in events]
            return [f"has_event_{events}"]

        elif metric == "event_count":
            event = metric_args.get("events")
            if isinstance(event, list):
                return [f"event_count_{e}" for e in event]
            return [f"event_count_{event}"]

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
                    PROCESSOR_NAME,
                    "'in_segment' metric with segment_value=None cannot be used in condition "
                    "(column names are not known until runtime)",
                )
            if isinstance(segment_value, list):
                return [f"in_segment_{segment_name}_{v}_{mode}" for v in segment_value]
            return [f"in_segment_{segment_name}_{segment_value}_{mode}"]

        else:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"Unknown metric type: {metric}"
            )

    @staticmethod
    def _extract_metric_configs(node: Dict[str, Any]) -> List[Dict[str, Any]]:
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
            elif op.upper() == "IN":
                metric = n.get("metric")
                if metric:
                    metric_args = n.get("metric_args")
                    # Create unique key for deduplication
                    key = (metric, str(sorted((metric_args or {}).items())))
                    if key not in seen_keys:
                        seen_keys.add(key)
                        configs.append(
                            {"metric": metric, "metric_args": metric_args or {}}
                        )
            else:
                # comparison node
                metric = n.get("metric")
                if metric:
                    metric_args = n.get("metric_args")
                    # Create unique key for deduplication
                    key = (metric, str(sorted((metric_args or {}).items())))
                    if key not in seen_keys:
                        seen_keys.add(key)
                        configs.append(
                            {"metric": metric, "metric_args": metric_args or {}}
                        )

        walk(node)
        return configs

    @staticmethod
    def _quote_ident(ident: str) -> str:
        if not isinstance(ident, str):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Identifier must be a string"
            )
        return quote_ident(ident)

    @staticmethod
    def _literal_sql(value: Any) -> str:
        if not isinstance(value, (bool, int, float, str)):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"Unsupported literal type in condition: {type(value)}"
            )
        return quote_literal(value)

    def _ast_to_sql(self, node: Dict[str, Any]) -> str:
        op = node.get("op")
        if op == "==":  # Python-style equality is accepted as an alias for "="
            op = "="
        if op.upper() in {"AND", "OR"}:
            args = node.get("args", [])
            if not args:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, f"Logical operator '{op}' requires args"
                )
            joiner = " AND " if op.upper() == "AND" else " OR "
            return "(" + joiner.join(self._ast_to_sql(a) for a in args) + ")"
        elif op.upper() == "NOT":
            args = node.get("args", [])
            if len(args) != 1:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "'NOT' operator requires exactly one arg"
                )
            return "(NOT " + self._ast_to_sql(args[0]) + ")"
        elif op.upper() == "IN":
            metric = node.get("metric")
            if not metric:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME, "'IN' operator requires 'metric' field"
                )

            metric_args = node.get("metric_args")
            metric_names = self._build_metric_names(metric, metric_args)

            # Check if we have multiple metrics (list case for has)
            if len(metric_names) > 1:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    "'IN' operator not supported with multiple metrics (list events)",
                )

            metric_name = metric_names[0]
            values = node.get("value")
            if values is None:
                values = node.get("args")
            if not isinstance(values, list) or len(values) == 0:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    "'IN' operator requires non-empty list of values in 'value' or 'args'",
                )

            left_sql = self._quote_ident(metric_name)

            # Map values to SQL literals, with special mapping for has_* boolean to 1/0
            def map_value(v: Any) -> str:
                if metric_name.startswith("has_") and isinstance(v, bool):
                    return "1" if v else "0"
                return self._literal_sql(v)

            values_sql = ", ".join(map_value(v) for v in values)
            return f"({left_sql} IN ({values_sql}))"

        if op not in {"=", "!=", ">", "<", ">=", "<="}:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, f"Unsupported comparison operator: {op}"
            )

        # comparison node
        metric = node.get("metric")
        if not metric:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Comparison nodes must have 'metric' field"
            )

        metric_args = node.get("metric_args")
        metric_names = self._build_metric_names(metric, metric_args)
        value = node.get("value")

        if value is None:
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Comparison nodes must have 'value' field"
            )
        if not isinstance(value, (str, int, float, bool)):
            raise PreprocessingConfigError(
                PROCESSOR_NAME, "Comparison nodes must have primitive 'value'"
            )

        # Special handling for has metric with multiple events (list)
        if metric == "has_event" and len(metric_names) > 1:
            # For has with list of events:
            # - If checking "= true": ALL events must be present (AND)
            # - If checking "= false": at least one event must be absent (OR)
            # - Other operators don't make sense for lists
            if op == "=":
                if isinstance(value, bool):
                    joiner = " AND " if value else " OR "
                    right_sql = "1" if value else "0"
                    conditions = [
                        f"{self._quote_ident(name)} {op} {right_sql}"
                        for name in metric_names
                    ]
                    return "(" + joiner.join(conditions) + ")"
                else:
                    raise PreprocessingConfigError(
                        PROCESSOR_NAME,
                        "has metric with list of events only supports boolean values",
                    )
            elif op == "!=":
                if isinstance(value, bool):
                    # != true means at least one should be false (OR)
                    # != false means all should be true (AND)
                    joiner = " OR " if value else " AND "
                    right_sql = "1" if value else "0"
                    conditions = [
                        f"{self._quote_ident(name)} {op} {right_sql}"
                        for name in metric_names
                    ]
                    return "(" + joiner.join(conditions) + ")"
                else:
                    raise PreprocessingConfigError(
                        PROCESSOR_NAME,
                        "has metric with list of events only supports boolean values",
                    )
            else:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"Operator '{op}' not supported for has metric with list of events",
                )

        # Single metric case
        metric_name = metric_names[0]
        left_sql = self._quote_ident(metric_name)

        # Handle boolean values for has_* metrics
        if metric_name.startswith("has_") and isinstance(value, bool):
            right_sql = "1" if value else "0"
        elif isinstance(value, str):
            # String values are treated as literals
            right_sql = self._literal_sql(value)
        else:
            right_sql = self._literal_sql(value)

        return f"({left_sql} {op} {right_sql})"

    # Public helper wrappers expected to be used from Eventstream.filter_paths
    def _get_metric_configs(self, node: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self._extract_metric_configs(node)

    def _get_where_condition(self, node: Dict[str, Any]) -> str:
        return self._ast_to_sql(node)
