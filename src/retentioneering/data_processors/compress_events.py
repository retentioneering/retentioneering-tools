"""Apply a vocabulary plan: drop, rename and merge events in one step.

``compress_events`` is the processor behind ``get_ab_compression``'s plans, and
takes a plain mapping too. It adds no transformation of its own: it is the
composition of ``edit_events`` (delete the dropped events, rename the others) and
``collapse_events(loops=...)`` (collapse each run of a merged label into one event),
so a plan means exactly what those two processors do.
"""

from typing import Any, Tuple

import pandas as pd

from retentioneering.data_processors.collapse_events import CollapseEvents
from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.data_processors.edit_events import EditEvents
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.utils.sequences import find_delimiter_collisions

PROCESSOR_NAME = "compress_events"


def plan_steps(plan: Any) -> Tuple[list, dict, list]:
    """``(delete, rename, collapse)`` of a plan: the arguments of ``edit_events``
    and of ``collapse_events(loops=...)``."""
    from retentioneering.tools.ab_compression import plan_mapping

    mapping, merged, _ = plan_mapping(plan)
    delete = sorted(e for e, label in mapping.items() if label is None)
    rename = {
        e: label for e, label in mapping.items() if label is not None and label != e
    }
    return delete, rename, merged


class CompressEvents(DataProcessor):
    delete: list
    rename: dict
    collapse: list
    path_col: str | None

    def __init__(self, plan: Any, path_col: str | None = None) -> None:
        self.delete, self.rename, self.collapse = plan_steps(plan)
        offenders = find_delimiter_collisions(self.rename.values())
        if offenders:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"New event name(s) {offenders} contain '->', which retentioneering "
                "uses as the path delimiter. Choose different labels.",
            )
        self.path_col = path_col
        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        present = set(map(str, df[schema.event_col].cat.categories.tolist()))
        # a plan lists the events of the population it was built on; an event the
        # stream does not have needs nothing
        delete = [e for e in self.delete if e in present]
        rename = {e: label for e, label in self.rename.items() if e in present}
        if delete or rename:
            df, schema = EditEvents(rename=rename, delete=delete).apply(df, schema)
        labels = set(map(str, df[schema.event_col].cat.categories.tolist()))
        collapse = [label for label in self.collapse if label in labels]
        if collapse:
            df, schema = CollapseEvents(loops=collapse, path_col=self.path_col).apply(
                df, schema
            )
        return df, schema
