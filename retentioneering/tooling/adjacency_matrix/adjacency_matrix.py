from __future__ import annotations

import networkx as nx
import pandas as pd
from IPython.display import display

from retentioneering.edgelist import Edgelist
from retentioneering.eventstream.types import EventstreamType
from retentioneering.nodelist import Nodelist
from retentioneering.tooling.typing.transition_graph import NormType


class AdjacencyMatrix:

    __edgelist: Edgelist

    def __init__(self, eventstream: EventstreamType) -> None:
        self.__eventstream = eventstream
        self.__nodelist = Nodelist(
            nodelist_default_col=eventstream.schema.event_name,
            custom_cols=eventstream.schema.custom_cols,
            time_col=eventstream.schema.event_timestamp,
            event_col=eventstream.schema.event_name,
        )
        self.__nodelist.calculate_nodelist(self.__eventstream.to_dataframe())
        self.__edgelist = Edgelist(
            event_col=eventstream.schema.event_name,
            time_col=eventstream.schema.event_timestamp,
            default_weight_col=eventstream.schema.event_name,
            index_col=eventstream.schema.user_id,
            nodelist=self.__nodelist.nodelist_df,
        )

    def values(self, weights: list[str] | None, norm_type: NormType) -> pd.DataFrame:
        """
        Parameters
        ----------
        weights : list of str or None
        norm_type : {"full", "node", None}

        Returns
        -------
        pd.DataFrame
            Transition matrix
        """

        self.__edgelist.calculate_edgelist(
            data=self.__eventstream.to_dataframe(), norm_type=norm_type, custom_cols=weights
        )
        edgelist: pd.DataFrame = self.__edgelist.edgelist_df
        graph = nx.DiGraph()
        graph.add_weighted_edges_from(edgelist.values)
        return nx.to_pandas_adjacency(G=graph)

    def display(self, weights: list[str] | None, norm_type: NormType) -> None:
        adjacency_matrix = self.values(weights=weights, norm_type=norm_type)
        display(adjacency_matrix)
