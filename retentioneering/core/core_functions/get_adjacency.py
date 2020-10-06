# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

import networkx as nx
import pandas as pd


def get_adjacency(self, *,
                  weight_col=None,
                  norm_type=None):
    """
    Creates edge graph in the matrix format. Row indeces are event_col values,
     from which the transition occured, and columns are events, to
    which the transition occured. The values are weights of the edges defined
    with weight_col and norm_type parameters.

    Parameters
    ----------
    weight_col: str (optional, default=None)
        Aggregation column for transitions weighting. To calculate weights
        as number of transion events use None. To calculate number
        of unique users passed through given transition 'user_id'.
         For any other aggreagtion, like number of sessions, pass the column name.

    norm_type: {None, 'full', 'node'} (optional, default=None)
        Type of normalization. If None return raw number of transtions
        or other selected aggregation column. 'full' - normalized over
        entire dataset. 'node' weight for edge A --> B normalized over
        user in A

    Returns
    -------
    Dataframe with number of columns and rows equal to unique number of
    event_col values.

    Return type
    -----------
    pd.DataFrame
    """
    agg = self.get_edgelist(weight_col=weight_col,
                            norm_type=norm_type)
    graph = nx.DiGraph()
    graph.add_weighted_edges_from(agg.values)
    return nx.to_pandas_adjacency(graph)
