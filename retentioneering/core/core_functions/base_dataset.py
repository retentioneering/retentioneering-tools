# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import networkx as nx
import pandas as pd

from .step_matrix import step_matrix
from .get_clusters import get_clusters, filter_cluster
from .plot_graph import plot_graph
from .extract_features import extract_features

class BaseDataset(object):

    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self.retention_config = {}
        self._embedding_types = ['tfidf']
        self._locals = None
        self.clusters = None

    def _get_shift(self, *,
                   index_col=None,
                   event_col=None):
        index_col = index_col or self.retention_config['index_col']
        event_col = event_col or self.retention_config['event_col']
        time_col = self.retention_config['event_time_col']

        data = self._obj.copy()
        data.sort_values([index_col, time_col], inplace=True)
        shift = data.groupby(index_col).shift(-1)

        data['next_'+event_col] = shift[event_col]
        data['next_'+str(time_col)] = shift[time_col]

        return data

    def get_edgelist(self, *,
                     weight_col=None,
                     norm_type=None,
                     edge_attributes='edge_weight'):
        """
        Creates weighted table of the transitions between events.

        Parameters
        ----------
        weight_col: str, optional, default=None
            Aggregation column for transitions weighting. To calculate weights
            as number of transion events leave as ```None``. To calculate number
            of unique users passed through given transition
            ``edge_attributes='user_id'``. For any other aggreagtion, life
            number of sessions, pass the column name.

        norm_type: {None, 'full', 'node'} str, optional, default=None
            Type of normalization. If ``None`` return raw number of transtions
            or other selected aggregation column.

        edge_attributes: str (optional, default 'edge_weight')

        Returns
        -------
        Dataframe with number of rows equal to all transitions with weight
        non-zero weight (max is squared number of  unique ``event_col`` values)
        and the following column structure: ``source_node``, ``target_node`` and
        ``edge_weight``.

        Return type
        -----------
        pd.DataFrame
        """
        if norm_type not in [None, 'full', 'node']:
            raise ValueError(f'unknown normalization type: {norm_type}')

        event_col = self.retention_config['event_col']
        time_col = self.retention_config['event_time_col']

        cols = [event_col, 'next_'+str(event_col)]

        data = self._get_shift().copy()

        # get aggregation:
        if weight_col is None:
            agg = (data
                   .groupby(cols)[time_col]
                   .count()
                   .reset_index())
            agg.rename(columns={time_col: edge_attributes}, inplace=True)
        else:
            agg = (data
                   .groupby(cols)[weight_col]
                   .nunique()
                   .reset_index())
            agg.rename(columns={weight_col: edge_attributes}, inplace=True)

        # apply normalization:
        if norm_type == 'full':
            if weight_col is None:
                agg[edge_attributes] /= agg[edge_attributes].sum()
            else:
                agg[edge_attributes] /= data[weight_col].nunique()

        if norm_type == 'node':
            if weight_col is None:
                event_transitions_counter = data.groupby(event_col)[cols[1]].count().to_dict()
                agg[edge_attributes] /= agg[cols[0]].map(event_transitions_counter)
            else:
                user_counter = data.groupby(cols[0])[weight_col].nunique().to_dict()
                agg[edge_attributes] /= agg[cols[0]].map(user_counter)

        return agg

    def get_adjacency(self, *,
                      weight_col=None,
                      norm_type=None):
        """
        Creates edge graph in the matrix format. Basically this method
        is similar to ``BaseTrajectory.rete.get_edgelist()`` but in different
        format. Row indeces are ``event_col`` values, from which the
        transition occured, while the row names are ``event_col`` values, to
        which the transition occured. The values are weights of the edges defined
        with ``weight_col``, ``edge_attributes`` and ``norm`` parameters.

        Parameters
        ----------
        weight_col: str, optional, default=None
            Aggregation column for transitions weighting. To calculate weights
            as number of transion events leave as ```None``. To calculate number
            of unique users passed through given transition
            ``edge_attributes='user_id'``. For any other aggreagtion, life
            number of sessions, pass the column name.

        norm_type: {None, 'full', 'node'} str, optional, default=None
            Type of normalization. If ``None`` return raw number of transtions
            or other selected aggregation column. If ``norm_type='full'`` normalization

        Returns
        -------
        Dataframe with number of columns and rows equal to unique number of
        ``event_col`` values.

        Return type
        -----------
        pd.DataFrame
        """
        agg = self.get_edgelist(weight_col=weight_col,
                                norm_type=norm_type)
        graph = nx.DiGraph()
        graph.add_weighted_edges_from(agg.values)
        return nx.to_pandas_adjacency(graph)

    def split_sessions(self, *,
                       by_event=None,
                       thresh,
                       eos_event=None,
                       session_col='session_id'):
        """
        Generates ``session`_id` column with session rank for each ``index_col``
        based on time difference between events. Sessions are automatically defined
        with time diffrence between events.

        Parameters
        ----------
        session_col
        by_event
        thresh: int
            Minimal threshold in seconds between two sessions. Default: ``1800`` or 30 min
        eos_event:
            If not ``None`` specified event name will be added at the and of each session

        Returns
        -------
        Original Dataframe with ``session_id`` column in dataset.

        Return type
        -----------
        pd.DataFrame
        """

        session_col_arg = session_col or 'session_id'

        index_col = self.retention_config['index_col']
        event_col = self.retention_config['event_col']
        time_col = self.retention_config['event_time_col']

        res = self._obj.copy()

        if by_event is None:
            res[time_col] = pd.to_datetime(res[time_col])
            if thresh is None:
                # add end_of_session event at the end of each string
                res.sort_values(by=time_col, inplace=True, ascending=False)
                res[hash('session')] = res.groupby(index_col).cumcount()
                res_session_ends = res[(res[hash('session')] == 0)].copy()
                res_session_ends[event_col] = eos_event
                res_session_ends[time_col] = res_session_ends[time_col] + pd.Timedelta(seconds=1)

                res = pd.concat([res, res_session_ends])

                res.sort_values(by=time_col, inplace=True)

            else:
                # split sessions by time thresh:
                # drop end_of_session events if already present:
                if eos_event is not None:
                    res = res[res[event_col] != eos_event].copy()

                res.sort_values(by=time_col, inplace=True)
                shift_res = res.groupby(index_col).shift(-1)

                time_delta = pd.to_datetime(shift_res[time_col]) - pd.to_datetime(res[time_col])
                time_delta = time_delta.dt.total_seconds()

                # get boolean mapper for end_of_session occurrences
                eos_mask = time_delta > thresh

                # add session column:
                res[hash('session')] = eos_mask
                res[hash('session')] = res.groupby(index_col)[hash('session')].cumsum()
                res[hash('session')] = res.groupby(index_col)[hash('session')].shift(1).fillna(0).map(int).map(str)

                # add end_of_session event if specified:
                if eos_event is not None:
                    tmp = res.loc[eos_mask].copy()
                    tmp[event_col] = eos_event
                    tmp[time_col] += pd.Timedelta(seconds=1)

                    res = pd.concat([res, tmp], ignore_index=True)
                    res = res.sort_values(time_col).reset_index(drop=True)

                res[session_col_arg] = res[index_col].map(str) + '_' + res[hash('session')]

        else:
            # split sessions by event:
            res[hash('session')] = res[event_col] == by_event
            res[hash('session')] = res.groupby(index_col)[hash('session')].cumsum().fillna(0).map(int).map(str)
            res[session_col_arg] = res[index_col].map(str) + '_' + res[hash('session')]

        res.drop(columns=[hash('session')], inplace=True)
        if session_col is None and session_col_arg in res.columns:
            res.drop(columns=[session_col_arg], inplace=True)
        return res


BaseDataset.step_matrix = step_matrix
BaseDataset.get_clusters = get_clusters
BaseDataset.filter_cluster = filter_cluster
BaseDataset.plot_graph = plot_graph
BaseDataset.extract_features = extract_features
