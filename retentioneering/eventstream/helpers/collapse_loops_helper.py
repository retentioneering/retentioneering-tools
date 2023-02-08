from __future__ import annotations

from typing import Literal, Union

from ..types import EventstreamType


class CollapseLoopsHelperMixin:
    def collapse_loops(
        self,
        suffix: Union[Literal["loop", "count"], None] = "loop",
        timestamp_aggregation_type: Literal["max", "min", "mean"] = "max",
    ) -> EventstreamType:
        """
        Method of ``Eventstream Class`` which finds ``loops`` and creates new synthetic events
        in each user's path who have such sequences.

        ``Loop`` - is the sequence of repetitive events in user's path.
        For example *"event1 -> event1"*

        Returns
        -------
        Eventstream
             Input ``eventstream`` with ``loops`` replaced by new synthetic events.

        Notes
        -----
        See parameters and details of dataprocessor functionality
        :py:func:`retentioneering.data_processors_lib.collapse_loops.CollapseLoops`
        """

        # avoid circular import
        from retentioneering.data_processors_lib import (
            CollapseLoops,
            CollapseLoopsParams,
        )
        from retentioneering.graph.nodes import EventsNode
        from retentioneering.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=CollapseLoops(
                params=CollapseLoopsParams(
                    suffix=suffix, timestamp_aggregation_type=timestamp_aggregation_type  # type: ignore
                )
            )
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
