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
        A method of ``Eventstream`` class that finds ``loops`` and creates new synthetic events
        in paths of all users having such sequences.

        A ``loop`` - is a sequence of repetitive events.
        For example *"event1 -> event1"*

        Parameters
        ----------
        See parameters description
            :py:class:`.CollapseLoops`

        Returns
        -------
        Eventstream
             Input ``eventstream`` with ``loops`` replaced by new synthetic events.


        """

        # avoid circular import
        from retentioneering.data_processors_lib import (
            CollapseLoops,
            CollapseLoopsParams,
        )
        from retentioneering.graph.nodes import EventsNode
        from retentioneering.graph.preprocessing_graph import PGraph

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
