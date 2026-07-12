import pandas as pd
from typing import Any, Dict, List, Tuple

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.metrics.condition_ast import ast_to_sql, extract_metric_configs

PROCESSOR_NAME = "filter_paths"


class FilterPaths(DataProcessor):
    """
    Keeps or drops whole paths matching a metric condition tree.

    The condition-AST parsing and SQL translation this needs is shared with
    `collapse_events`' `event_groups[].cases` (see
    `metrics/condition_ast.py`) - this class is a thin, `filter_paths`-scoped
    wrapper over that shared logic, not its owner.
    """

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

    # Public helper wrappers expected to be used from Eventstream.filter_paths
    def _get_metric_configs(self, node: Dict[str, Any]) -> List[Dict[str, Any]]:
        return extract_metric_configs(node, PROCESSOR_NAME)

    def _get_where_condition(self, node: Dict[str, Any]) -> str:
        return ast_to_sql(node, PROCESSOR_NAME)
