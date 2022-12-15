from __future__ import annotations

from typing import Callable, List, Optional

from ..types import EventstreamType


class NegativeTargetHelperMixin:
    def negative_target(
        self, negative_target_events: List[str], negative_function: Optional[Callable] = None
    ) -> EventstreamType:
        """
        Method of ``Eventstream Class`` which creates new synthetic events in each user's path
        who have specified event(s) - ``negative_target_RAW_EVENT_NAME``.
        And adds them to the input ``eventstream``.

        Returns
        -------
        Eventstream
            Input ``eventstream`` with new synthetic events.

        Notes
        -----
        See parameters and details of dataprocessor functionality
        :py:func:`src.data_processors_lib.rete.negative_target.PositiveTarget`

        """
        # avoid circular import
        from src.data_processors_lib.rete import NegativeTarget, NegativeTargetParams
        from src.graph.nodes import EventsNode
        from src.graph.p_graph import PGraph

        p = PGraph(source_stream=self)  # type: ignore

        params: dict[str, list[str] | Callable] = {"negative_target_events": negative_target_events}
        if negative_function:
            params["negative_function"] = negative_function

        node = EventsNode(processor=NegativeTarget(params=NegativeTargetParams(**params)))  # type: ignore
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
