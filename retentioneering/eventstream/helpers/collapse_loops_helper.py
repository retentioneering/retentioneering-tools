from __future__ import annotations

from typing import Literal, Union

from retentioneering.backend.tracker import (
    collect_data_performance,
    time_performance,
    track,
)
from retentioneering.utils.doc_substitution import docstrings

from ..types import EventstreamType


class CollapseLoopsHelperMixin:
    @docstrings.with_indent(12)
    @time_performance(
        scope="collapse_loops",
        event_name="helper",
        event_value="combine",
    )
    def collapse_loops(
        self: EventstreamType,
        suffix: Union[Literal["loop", "count"], None] = None,
        time_agg: Literal["max", "min", "mean"] = "min",
    ) -> EventstreamType:
        """
        A method of ``Eventstream`` class that finds ``loops`` and creates new synthetic events
        in paths of all users having such sequences.

        A ``loop`` - is a sequence of repetitive events.
        For example *"event1 -> event1"*

        Parameters
        ----------
            %(CollapseLoops.parameters)s

        Returns
        -------
        Eventstream
             Input ``eventstream`` with ``loops`` replaced by new synthetic events.


        """
        calling_params = {
            "suffix": suffix,
            "time_agg": time_agg,
        }
        not_hash_values = ["suffix", "time_agg"]

        # avoid circular import
        from retentioneering.data_processors_lib import (
            CollapseLoops,
            CollapseLoopsParams,
        )
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=CollapseLoops(params=CollapseLoopsParams(suffix=suffix, time_agg=time_agg))  # type: ignore
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        collect_data_performance(
            scope="collapse_loops",
            event_name="metadata",
            called_params=calling_params,
            not_hash_values=not_hash_values,
            performance_data={},
            eventstream_index=self._eventstream_index,
            parent_eventstream_index=self._eventstream_index,
            child_eventstream_index=result._eventstream_index,
        )

        return result
