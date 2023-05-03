from __future__ import annotations

from typing import Literal, Union

from retentioneering.backend.tracker import track

from ..types import EventstreamType


class CollapseLoopsHelperMixin:
    @track(  # type: ignore
        tracking_info={"event_name": "helper"},
        scope="collapse_loops",
        event_value="combine",
        allowed_params=[
            "suffix",
            "time_agg",
        ],
    )
    def collapse_loops(
        self,
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
        from retentioneering.preprocessing_graph import PreprocessingGraph
        from retentioneering.preprocessing_graph.nodes import EventsNode

        p = PreprocessingGraph(source_stream=self)  # type: ignore

        node = EventsNode(
            processor=CollapseLoops(params=CollapseLoopsParams(suffix=suffix, time_agg=time_agg))  # type: ignore
        )
        p.add_node(node=node, parents=[p.root])
        result = p.combine(node)
        del p
        return result
