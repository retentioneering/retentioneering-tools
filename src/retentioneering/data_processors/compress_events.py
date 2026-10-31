"""Apply a vocabulary plan: drop, rename and merge events in one step.

``compress_events`` is the processor behind ``get_ab_compression``'s plans, and
takes a plain mapping too. It adds no transformation of its own: it is the
composition of ``edit_events`` (delete the dropped events, rename the others) and
``collapse_events(loops=...)`` (collapse each run of a merged label into one event),
so a plan means exactly what those two processors do.

The result is checked against the plan's own reading of the paths: each path in
path order, ``(index, subindex)``, with the plan's labels and each run of a merged
label read as one event. A stream on which the processors read the paths in
another order (events that share a timestamp) would get a result that is not the
plan's, so the processor raises instead of returning it.
"""

from typing import Any, Tuple

import numpy as np
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
        self.named = set(self.delete) | set(self.rename) | set(self.collapse)
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
        grain = self.path_col or schema.path_col
        present = set(map(str, df[schema.event_col].cat.categories.tolist()))
        # a plan lists the events of the population it was built on; an event the
        # stream does not have needs nothing
        delete = [e for e in self.delete if e in present]
        rename = {e: label for e, label in self.rename.items() if e in present}
        # a new label that is already an event the plan does not name would mix
        # that event into the label without the plan saying so
        unnamed = sorted(
            {label for label in rename.values() if label in present} - self.named
        )
        if unnamed:
            raise PreprocessingConfigError(
                PROCESSOR_NAME,
                f"label(s) {unnamed} are also events of the stream that the plan "
                "does not name. Map each of them to its own label too (in a "
                "mapping, {'B': 'B'} makes B a member of the label), or choose "
                "another label.",
            )
        out, out_schema = df, schema
        if delete or rename:
            out, out_schema = EditEvents(rename=rename, delete=delete).apply(
                out, out_schema
            )
        labels = set(map(str, out[out_schema.event_col].cat.categories.tolist()))
        collapse = [label for label in self.collapse if label in labels]
        if collapse:
            out, out_schema = CollapseEvents(loops=collapse, path_col=grain).apply(
                out, out_schema
            )
        self._check(df, out, schema, grain, set(delete), rename, set(collapse))
        return out, out_schema

    @staticmethod
    def _check(
        before: pd.DataFrame,
        after: pd.DataFrame,
        schema: EventstreamSchema,
        grain: str,
        delete: set,
        rename: dict,
        collapse: set,
    ) -> None:
        """Raise unless ``after`` is the plan applied to ``before`` in path order."""
        order = [grain, schema.index, schema.subindex]
        source = before.sort_values(order, kind="stable")
        events = source[schema.event_col].astype(str)
        kept = ~events.isin(delete).to_numpy()
        labels = events.map(rename).fillna(events).to_numpy(dtype=object)[kept]
        paths = source[grain].to_numpy(dtype=object)[kept]
        if len(labels) and collapse:
            repeat = np.r_[
                False, (labels[1:] == labels[:-1]) & (paths[1:] == paths[:-1])
            ] & np.isin(labels, list(collapse))
            labels, paths = labels[~repeat], paths[~repeat]
        result = after.sort_values(order, kind="stable")
        got = result[schema.event_col].astype(str).to_numpy(dtype=object)
        got_paths = result[grain].to_numpy(dtype=object)
        if (
            len(got) == len(labels)
            and (got == labels).all()
            and (got_paths == paths).all()
        ):
            return
        differ = sum(
            a != b for _, a, b in _paths_compared(paths, labels, got_paths, got)
        )
        raise PreprocessingConfigError(
            PROCESSOR_NAME,
            f"the plan does not apply exactly to this stream: in {differ} path(s) "
            "edit_events and collapse_events return other events than the plan's "
            "own reading of the path (its labels, in path order: index, then "
            "subindex). This happens when collapse_events reads events that share a "
            "timestamp in another order, as with markers from add_events at the "
            "timestamp of other events. Nothing is returned rather than a stream "
            "compressed differently from the plan.",
        )


def _paths_compared(paths_a, labels_a, paths_b, labels_b):
    """``(path, events of a, events of b)`` for every path of either side."""
    a = pd.Series(list(labels_a)).groupby(list(paths_a), sort=False).agg(tuple)
    b = pd.Series(list(labels_b)).groupby(list(paths_b), sort=False).agg(tuple)
    for path in a.index.union(b.index):
        yield path, a.get(path), b.get(path)
