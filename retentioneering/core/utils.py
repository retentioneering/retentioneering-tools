# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import os
import pandas as pd
import numpy as np
import networkx as nx
from datetime import timedelta
from retentioneering.core import feature_extraction
from retentioneering.core import clustering
from retentioneering.visualization import plot
from sklearn.linear_model import LogisticRegression
from retentioneering.core.model import ModelDescriptor
from retentioneering.core import node_metrics
from retentioneering.core import preprocessing


def init_config(**config):
    """
    Initialize config and pandas accessors

    :param positive_target_event: name of positive target event
    :param negative_target_event: name of negative target event
    :param index_col: name of index column, e.g. `user_pseudo_id` in our examples
    :param event_col: name of event column, e.g. `event_name` in our examples
    :param event_time_col: name of event timestamp column, e.g. `event_timestamp` in our examples
    :param pos_target_definition: optional, if exists then add target event with described logic:
        If empty dict, then adds `positive_target_event` for users, who have not it in the end of track.
        If contains `time_limit`, then adds event to session after time_limit seconds of inactivity.
        If contains `event_list`, then replace events from list with `positive_target_event`
    :param neg_target_definition: optional, similar to `pos_target_definition`
    :param experiments_folder: optional, where to save results of analysis.
        If unexists then folder named with current timestamp will be created.
    :param source_event: optional, name of session / user trajectory starting event
    :return: Nothing
    """
    if 'experiments_folder' not in config:
        config.update({'experiments_folder': '{}'.format(pd.datetime.now()).replace(':', '-').split('.')[0]})
    if 'target_event_list' not in config:
        config.update({
            'target_event_list': [
                config.get('negative_target_event'),
                config.get('positive_target_event'),
            ]
        })
    if 'columns_map' not in config:
        config['columns_map'] = {
            'user_pseudo_id': config.get('index_col'),
            'event_name': config.get('event_col'),
            'event_timestamp':  config.get('event_time_col'),
        }
    if not os.path.exists(config['experiments_folder']):
        os.mkdir(config['experiments_folder'])

    @pd.api.extensions.register_dataframe_accessor("trajectory")
    class RetentioneeringTrajectory(BaseTrajectory):

        def __init__(self, pandas_obj):
            super(RetentioneeringTrajectory, self).__init__(pandas_obj)
            self.retention_config = config

    @pd.api.extensions.register_dataframe_accessor("retention")
    class RetentioneeringDataset(BaseDataset):

        def __init__(self, pandas_obj):
            super(RetentioneeringDataset, self).__init__(pandas_obj)
            self.retention_config = config


class BaseTrajectory(object):

    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self._accessor_type = 'trajectory'
        self.retention_config = {
            'columns_map': {
                'user_pseudo_id': 'user_pseudo_id',
                'event_name': 'event_name',
                'event_timestamp': 'event_timestamp',
            }}

    def _get_shift(self, index_col=None, event_col=None, shift_name='next_event', **kwargs):
        if 'next_event' not in self._obj.columns:
            # TODO indexation when init
            colmap = self.retention_config['columns_map']
            (self._obj
             .sort_values([index_col or colmap['user_pseudo_id'], colmap['event_timestamp']], inplace=True))
            shift = self._obj.groupby(index_col or colmap['user_pseudo_id']).shift(-1)
            if shift_name not in self._obj.columns:
                self._obj[shift_name] = shift[event_col or colmap['event_name']]
            self._obj['next_timestamp'] = shift[colmap['event_timestamp']]

    def get_edgelist(self, cols=None, edge_col=None, edge_attributes='event_count', norm=True, **kwargs):
        """
        Creates graph in the following representation: `source_node, target_node, edge_weight`

        :param cols: list of source and target columns, e.g. `event_name`, `next_event` correspondingly
        :param edge_col: aggregation column for edge weighting,
            e.g. set it to `index_col` and `edge_attributes='unique'` to calculate unique users passed through edge
        :param edge_attributes: name of edge weighting,
            second part after `_` should be a valid pandas.groupby.agg() parameter, e.g. `count`, `mean`. `sum` and etc.
        :param norm: normalize over number of users
        :return: pd.DataFrame with graph in edgelist format
        """
        if cols is None:
            cols = [
                self.retention_config['event_col'],
                'next_event'
            ]
        self._get_shift(event_col=cols[0], shift_name=cols[1], **kwargs)
        agg = (self._obj
               .groupby(cols)[edge_col or self.retention_config['event_time_col']]
               .agg(edge_attributes.split('_')[1])
               .reset_index())
        agg.columns = cols + [edge_attributes]
        if norm:
            agg[edge_attributes] /= self._obj[self.retention_config['index_col']].nunique()
        return agg

    def get_adjacency(self, cols=None, edge_attributes='event_count', norm=True, **kwargs):
        """
        Creates graph in the matrix format

        :param cols: list of source and target columns, e.g. `event_name`, `next_event` correspondingly
        :param edge_attributes: name of edge weighting,
            second part after `_` should be a valid pandas.groupby.agg() parameter, e.g. `count`, `mean`. `sum` and etc.
        :param norm: normalize over number of users
        :param kwargs: params from edgelist
        :return: pd.DataFrame with graph in m   atrix format
        """
        agg = self.get_edgelist(cols=cols, edge_attributes=edge_attributes, norm=norm, **kwargs)
        G = nx.DiGraph()
        G.add_weighted_edges_from(agg.values)
        return nx.to_pandas_adjacency(G).round(2)

    def _add_event_rank(self, index_col=None, **kwargs):
        self._obj['event_rank'] = 1
        self._obj['event_rank'] = self._obj.groupby(
            index_col or self.retention_config['index_col'])['event_rank'].cumsum()

    @staticmethod
    def _add_accums(agg, name):
        """
        Creates Accumulator Variables

        :param agg: Counts of events by step
        :param name: Name of Accumulator
        :return: Accumulator Variable
        """
        if name not in agg.index:
            return pd.Series([0] * agg.shape[1], index=agg.columns, name='Accumulated ' + name)
        return agg.loc[name].cumsum().rename('Accumulated ' + name)

    def get_step_matrix(self, max_steps=None, plot_type=True, **kwargs):
        """
        Plots heatmap with distribution of events over event steps (ordering in the session by event time)

        :param max_steps: maximum number of steps to show
        :param plot_type: if True, then plot in interactive session (jupyter notebook)
        :return: pd.DataFrame with distribution of events over event order
        """
        target_event_list = self.retention_config['target_event_list']
        # TODO give filter, return to desc tables ???
        self._add_event_rank(**kwargs)
        agg = self.get_edgelist(cols=['event_rank', self.retention_config['event_col']], norm=False)
        if max_steps:
            agg = agg[agg.event_rank <= max_steps]
        agg.columns = ['event_rank', 'event_name', 'freq']
        tot_cnt = agg[agg['event_rank'] == 1].freq.sum()
        agg['freq'] = agg['freq'] / tot_cnt
        piv = agg.pivot(index='event_name', columns='event_rank', values='freq').fillna(0)
        piv.columns.name = None
        piv.index.name = None
        for i in target_event_list:
            piv = piv.append(self._add_accums(piv, i))
        piv = piv.round(2)
        if plot_type:
            plot.step_matrix(piv)
        return piv

    @staticmethod
    def _create_diff_index(desc_old, desc_new):
        old_id = set(desc_old.index)
        new_id = set(desc_new.index)

        if old_id != new_id:
            for idx in new_id - old_id:
                row = pd.Series([0] * desc_old.shape[1], name=idx)
                row.index += 1
                desc_old = desc_old.append(row, sort=True)
            for idx in old_id - new_id:
                row = pd.Series([0] * desc_new.shape[1], name=idx)
                row.index += 1
                desc_new = desc_new.append(row, sort=True)
        return desc_old, desc_new

    @staticmethod
    def _diff_step_allign(desc_old, desc_new):
        max_old = desc_old.shape[1]
        max_new = desc_new.shape[1]
        if max_old < max_new:
            for i in range(max_old, max_new + 1):
                desc_old[i] = desc_old[i - 1]
        elif max_old > max_new:
            for i in range(max_new, max_old + 1):
                desc_new[i] = desc_new[i - 1]
        return desc_old, desc_new

    def split_sessions(self, by_event=None, minimal_thresh=30):
        """
        Creates column session with session rank

        :param by_event: if not None, then split sessions by specific event,
            else sessions are automatically defined from time diffrence between events
        :param minimal_thresh: minimal time distance between sessions for case of automatic definition
        :return: Nothing
        """
        if by_event is None:
            preprocessing.split_sessions(self._obj, minimal_thresh=minimal_thresh)
        else:
            self._obj['session'] = self._obj[self.retention_config['event_col']] == by_event
            self._obj['session'] = self._obj.groupby(self.retention_config['index_col']).session.cumsum()

    def weight_by_mechanics(self, main_event_map, **kwargs):
        """
        Calculates weights of mechanics over index_col

        :param main_event_map: mapping of main events for mechanics
        :param kwargs: keyword arguments for feature_extraction and sklearn LDA model
        :return: weights of mechanics for each user and mechanics description
        """
        mechs, mech_desc = preprocessing.weight_by_mechanics(self._obj, main_event_map, **kwargs)
        return mechs, mech_desc

    def plot_graph(self, user_based=True, node_params=None, **kwargs):
        """
        Create interactive graph visualization

        :param user_based: if True, then edge weights is calculated as unique rate of users who go through them
        :param node_params: mapping describes which node should be highlighted by target or source type
            Node param should be represented in the following form
            ```{
                    'lost': 'bad_target',
                    'passed': 'nice_target',
                    'onboarding_welcome_screen': 'source',
                }```
            If mapping is not given, it will be constracted from config
        :param kwargs: other parameters for visualization
        :return: Nothing
        """
        if user_based:
            kwargs.update({
                'edge_col': self.retention_config['index_col'],
                'edge_attributes': '_nunique',
                'norm': True,
            })
        if node_params is None:
            _node_params = {
                'positive_target_event': 'nice_target',
                'negative_target_event': 'bad_target',
                'source_event': 'source',
            }
            node_params = {}
            for key, val in _node_params.items():
                name = self.retention_config.get(key)
                if name is None:
                    continue
                node_params.update({name: val})
        plot.graph(self._obj.trajectory.get_edgelist(**kwargs), node_params, **kwargs)

    @staticmethod
    def calculate_node_metrics(metric_type='centrality'):
        """
        Calculate metrics on graph

        :param metric_type: type of metrics, e.g. node centrality
        :return:
        """
        raise NotImplementedError('Sorry! This function is not ready now')
        func = getattr(node_metrics, metric_type)
        return func


class BaseDataset(BaseTrajectory):

    def __init__(self, pandas_obj):
        super(BaseDataset, self).__init__(pandas_obj)
        self._embedding_types = ['tfidf', 'counts', 'frequency']

    def extract_features(self, feature_type='tfidf', drop_targets=True, **kwargs):
        """
        Vectorize users`s trajectories
        Available vectorization methods is `Tf-Idf` (feature_type='tfidf'),
        `Event Frequencies` (feature_type='frequency') and `Event Counts` (feature_type='count').

        :param feature_type: type of vectorizer
        :param drop_targets: if True, then targets will be removed from feature generation
        :param kwargs: keyword arguments for vectorizer
        :return: encoded users trajectories
        :rtype: pd.DataFrame of (number of users, number of unique events | event n-grams)
        """

        if feature_type not in self._embedding_types:
            raise ValueError("Unknown feature type: {}.\nPlease choose one from {}".format(
                feature_type,
                ' '.join(self._embedding_types)
            ))

        func = getattr(feature_extraction, feature_type + '_embedder')
        if drop_targets:
            tmp = self._obj[
                ~self._obj[self.retention_config['event_col']].isin(self.retention_config['target_event_list'])
            ].copy()
        else:
            tmp = self._obj
        return func(tmp, **kwargs)

    def extract_features_from_test(self, test, train=None, **kwargs):
        if train is None:
            train = self.extract_features(**kwargs)
        test = test.retention.extract_features(**kwargs)
        test = test.loc[:, train.columns.tolist()]
        return test.fillna(0)

    def _make_target(self):
        target = (self._obj
                  .groupby(self.retention_config['index_col'])
                  .apply(lambda x: self.retention_config['positive_target_event'] in x))
        return target

    def get_clusters(self, plot_type=None, refit_cluster=False, **kwargs):
        """
        Finds cluster of users in data.

        :param plot_type: type of clustering visualization.
            Available methods are (`cluster_heatmap`, `cluster_tsne`, `cluster_pie`).
            Please, see examples to understand different visualizations
        :param kwargs: keyword arguments for clusterer
        :return: np.array of clusters
        """
        if hasattr(self, 'datatype') and self.datatype == 'features':
            features = self._obj.copy()
        else:
            features = self.extract_features(**kwargs)
        if not hasattr(self, 'clusters') or refit_cluster:
            self.clusters = clustering.simple_cluster(features, **kwargs)
            self._create_cluster_mapping(features.index.values)

        target = self.get_positive_users()
        target = features.index.isin(target)
        if hasattr(self, '_tsne'):
            features.retention._tsne = self._tsne
        if plot_type:
            func = getattr(plot, plot_type)
            res = func(features, self.clusters, target, **kwargs)
            if res is not None:
                self._tsne = res
        return self.clusters

    def _create_cluster_mapping(self, ids):
        self.cluster_mapping = {}
        for cluster in set(self.clusters):
            self.cluster_mapping[cluster] = ids[self.clusters == cluster].tolist()

    def filter_cluster(self, cluster_name, index_col=None):
        ids = self.cluster_mapping[cluster_name]
        return self._obj[self._obj[
            index_col or self.retention_config['index_col']].isin(ids)].copy().reset_index(drop=True)

    def create_model(self, model_type=LogisticRegression, regression_targets=None, **kwargs):
        """
        Creates model explainer for given model.

        :param model_type: model class in sklearn-api style (should have methods `fit`, `predict_proba`
        :param regression_targets: mapping from index_col to regression target e.g. LTV of user
        :param kwargs: params for model and explainer
        :return:
        """
        if hasattr(self, 'datatype') and self.datatype == 'features':
            features = self._obj.copy()
        else:
            if 'ngram_range' not in kwargs:
                kwargs.update({'ngram_range': (1, 2)})
            features = self.extract_features(**kwargs)
        if regression_targets is not None:
            target = self.make_regression_targets(features, regression_targets)
        else:
            target = features.index.isin(self.get_positive_users())
        kwargs.pop('ngram_range')
        mod = ModelDescriptor(model_type, features, target, **kwargs)
        return mod

    @staticmethod
    def make_regression_targets(features, regression_targets):
        """
        Creates target vector for given features

        :param features: feature matrix
        :param regression_targets: mapping from index_col to regression target e.g. LTV of user
        :return: list of targets alligned to feature matrix indices
        """
        return [regression_targets.get(i) for i in features.index]

    def get_step_matrix_difference(self, groups, plot_type=True, max_steps=30, **kwargs):
        """
        Plots heatmap with difference of events distributions over steps between two given groups

        :param groups: boolean vector that splits data in to groups
        :param plot_type: if True, then heatmap plot will be shown in interactive mode
        :param max_steps: maximum number of steps to show
        :return: pd.DataFrame with step matrix
        """
        desc_old = self._obj[~groups].copy().trajectory.get_step_matrix(plot_type=False, max_steps=max_steps, **kwargs)
        desc_new = self._obj[groups].copy().trajectory.get_step_matrix(plot_type=False, max_steps=max_steps, **kwargs)
        desc_old, desc_new = self._create_diff_index(desc_old, desc_new)
        desc_old, desc_new = self._diff_step_allign(desc_old, desc_new)
        diff = desc_new - desc_old
        diff = diff.sort_index(axis=1)
        if plot_type:
            plot.step_matrix(diff)
        return diff

    def _process_target_config(self, data, cfg, target):
        target = 'positive_event_name' if target.startswith('pos_') else 'negative_event_name'
        target = self.retention_config.get(target)
        for key, val in cfg.items():
            func = getattr(self, f'_process_{key}')
            data = func(data, val, target)
        return data

    def _process_time_limit(self, data, threshold, name):
        if 'next_timestamp' in data:
            col = 'next_timestamp'
            change_next = True
            data[self.retention_config.get('event_time_col')] \
                = pd.to_datetime(data[self.retention_config.get('event_time_col')])
        else:
            col = self.retention_config.get('event_time_col')
            change_next = False
        data[col] = pd.to_datetime(data[col])
        thresh = pd.to_datetime(data[col].max()) - timedelta(seconds=threshold)
        bads = set(data[data[col] >= thresh][self.retention_config.get('index_col')])
        goods = set(data[self.retention_config.get('index_col')]) - bads
        tmp = data[data[self.retention_config.get('index_col')].isin(goods)]
        tmp = tmp.groupby(self.retention_config.get('index_col')).tail(1)
        if change_next:
            tmp[self.retention_config.get('index_col')] = tmp.next_event.values
            tmp.next_event = name
            tmp[self.retention_config.get('event_time_col')] += timedelta(seconds=1)
            tmp['next_timestamp'] += timedelta(seconds=1)
        else:
            tmp[self.retention_config.get('index_col')] = name
            tmp[self.retention_config.get('event_time_col')] += timedelta(seconds=1)
        data.reset_index(drop=True, inplace=True)

        return data.append(tmp, ignore_index=True).reset_index(drop=True)

    def _process_event_list(self, data, event_list, name):
        if 'next_event_name' in data:
            col = 'next_event_name'
        else:
            col = self.retention_config.get('event_col')
        data[col] = np.where(data[col].isin(event_list), name, data[col])
        return data

    def _process_empty(self, data, other, name):
        if 'next_event_name' in data:
            col = 'next_event_name'
            change_next = True
            data['next_timestamp'] \
                = pd.to_datetime(data[self.retention_config.get('event_time_col')])
        else:
            col = self.retention_config.get('event_col')
            change_next = False
        data[self.retention_config.get('event_time_col')] \
            = pd.to_datetime(data[self.retention_config.get('event_time_col')])
        bads = set(data[data[col] == other][self.retention_config.get('index_col')])
        goods = set(data[self.retention_config.get('index_col')]) - bads
        tmp = data[data[self.retention_config.get('index_col')].isin(goods)]
        tmp = tmp.groupby(self.retention_config.get('index_col')).tail(1)
        if change_next:
            tmp[self.retention_config.get('event_col')] = tmp.next_event.values
            tmp.next_event = name
            tmp[self.retention_config.get('event_time_col')] += timedelta(seconds=1)
            tmp['next_timestamp'] += timedelta(seconds=1)
        else:
            tmp[self.retention_config.get('event_col')] = name
            tmp[self.retention_config.get('event_time_col')] += timedelta(seconds=1)
        data.reset_index(drop=True, inplace=True)
        return data.append(tmp, ignore_index=True).reset_index(drop=True)

    def _add_first_event(self, first_event):
        top1 = self._obj.groupby(self.retention_config['index_col']).head(1)
        if 'next_event' in top1:
            top1.next_event = top1[self.retention_config['event_col']].values
        top1[self.retention_config['event_col']] = first_event
        top1[self.retention_config['event_time_col']] -= 1
        return top1.append(self._obj, ignore_index=True).reset_index(drop=True)

    def _convert_timestamp(self, time_col=None):
        timestamp = self._obj[time_col or self.retention_config['event_time_col']].iloc[0]
        if type(timestamp) != str:
            l = len(str(timestamp))
            self._obj[time_col or self.retention_config['event_time_col']] *= 10 ** (19 - l)
        self._obj[
            time_col or self.retention_config['event_time_col']
        ] = pd.to_datetime(self._obj[time_col or self.retention_config['event_time_col']])

    def prepare(self, first_event=None):
        """
        Add target events based on target event description in config

        :param first_event: if not None, then adds `first_event` prevously others to clickstream
        :return: input data updated with target events
        """
        self._convert_timestamp()
        if first_event is not None:
            data = self._add_first_event(first_event)
        else:
            data = self._obj.copy()
        targets = {
            'pos_target_definition',
            'neg_target_definition'
        }
        if (self.retention_config.get('positive_target_event') in set(self._obj[self.retention_config.get('event_col')])
                or self.retention_config.get('pos_target_definition') is None):
            targets = targets - {'pos_target_definition'}
        if (self.retention_config.get('negative_target_event') in set(self._obj[self.retention_config.get('event_col')])
                or self.retention_config.get('neg_target_definition') is None):
            targets = targets - {'neg_target_definition'}
        empty_definition = []
        for target in targets:
            tmp = self.retention_config.get(target)
            if len(tmp) == 0:
                empty_definition.append(target)
                continue
            data = self._process_target_config(data, tmp, target)
        if len(empty_definition) == 2:
            return data
        for target in empty_definition:
            other = (self.retention_config['positive_target_event']
                     if target.startswith('neg_')
                     else self.retention_config['negative_target_event'])
            target = (self.retention_config['positive_target_event']
                      if target.startswith('pos_')
                      else self.retention_config['negative_target_event'])
            data = self._process_empty(data, other, target)
        return data

    def get_positive_users(self, index_col=None):
        """
        Finds users, who have positive_target_event

        :param index_col: name of index by which to create targets.
            if None, then `index_col` from config is used.
        :return: np.array of users with positive_target_event in trajectory
        """
        pos_users = (
            self._obj[self._obj
                      [self.retention_config['event_col']] == self.retention_config['positive_target_event']]
            [index_col or self.retention_config['index_col']]
        )
        return pos_users

    def create_filter(self, index_col=None):
        """
        Creates positive users filter for get_step_matrix_difference method

        :param index_col: name of index by which to create filter.
            if None, then `index_col` from config is used.
        :return: pd.Series with filter
        """
        pos_users = self.get_positive_users(index_col)
        return self._obj[index_col or self.retention_config['index_col']].isin(pos_users)

    def remove_events(self, event_list, mode='equal'):
        """
        Removes events from event_list using different modes:
            `equal` -- full event name match with element from event_list,
            `startswith` -- event name starts with element from event_list,
            `contains` -- event name contains element from event_list,

        :param event_list: list of events or elements,
            that should be contained in `event_col`
        :param mode: type of comparison: `equal`, `startswith` or `contains`
        :return: pd.DataFrame with filtered clickstream
        """
        data = self._obj.copy()
        func = getattr(preprocessing, '_event_filter_' + mode)

        for event in event_list:
            data = data.loc[func(data[self.retention_config['event_col']], event)]
        return data.reset_index(drop=True)

    def learn_tsne(self, targets=None, plot_type=None, refit=False, regression_targets=None, **kwargs):
        """
        Learns TSNE projection for selected feature space (`feature_type` in kwargs)
        and visualize it with chosen visualization type

        :param targets: vector of targets for users
            if None, then calculates automatically based on `positive-` and `negative_target_event`
        :param plot_type: type of visualization: `clusters` or `targets`
            if None, then only calculates tsne without vis
        :param refit: if True, then tsne will be refitted e.g. it is needed if you perform hyperparam selection
        :param regression_targets: mapping from index_col to regression target e.g. LTV of user
        :param kwargs: parameters for feature extraction, sklearn.manifold.TSNE and sklearn.cluster.KMeans
        :return: np.array with TSNE transform for user tracks
        """
        if hasattr(self, 'datatype') and self.datatype == 'features':
            features = self._obj.copy()
        else:
            features = self.extract_features(**kwargs)
            if targets is None:
                if regression_targets is not None:
                    targets = self.make_regression_targets(features, regression_targets)
                else:
                    targets = features.index.isin(self.get_positive_users())
                    targets = np.where(targets, self.retention_config['positive_target_event'],
                                       self.retention_config['negative_target_event'])
            self._tsne_targets = targets
        if not (hasattr(self, '_tsne') and not refit):
            self._tsne = feature_extraction.learn_tsne(features, **kwargs)
        if plot_type == 'clusters':
            targets = self.get_clusters(plot_type=None, **kwargs)
        elif plot_type == 'targets':
            targets = self._tsne_targets
        else:
            return self._tsne

        plot.cluster_tsne(self._obj, targets, targets)
        return self._tsne

    def select_bbox_from_tsne(self, bbox, plotting=True, **kwargs):
        """
        Selects data filtered by cordinates of tsne plot

        :param bbox: list of lists that contains angles of bbox
            ```bbox = [
                [0, 0], # [min x, max x]
                [10, 10] # [min y, max y]
            ]```
        :param plotting: if True, then visualize graph of selected users
        :return: pd.DataFrame with filtered clickstream of users in bbox
        """
        if not hasattr(self, '_tsne'):
            raise ValueError('Please, use `learn_tsne` before selection of specific bbox')

        f = self._tsne.index.values[(self._tsne.iloc[:, 0] >= bbox[0][0])
                                    & (self._tsne.iloc[:, 0] <= bbox[0][1])
                                    & (self._tsne.iloc[:, 1] >= bbox[1][0])
                                    & (self._tsne.iloc[:, 1] <= bbox[1][1])]

        filtered = self._obj[self._obj[self.retention_config['index_col']].isin(f)]
        if plotting:
            filtered.retention.plot_graph(**kwargs)
        return filtered.reset_index(drop=True)

    def show_tree_selector(self, **kwargs):
        """
        Shows tree selector, based on your event names.
        It uses `_` for splitting event names for group aggregation

        :param kwargs: event_col and params for iframe: `width` and `height`
        :return:
        """
        from retentioneering.core.tree_selector import show_tree_filter
        show_tree_filter(kwargs.get('event_col') or self._obj[self.retention_config['event_col']], **kwargs)

    def use_tree_filter(self, path, **kwargs):
        """
        Filters and aggregate data based on given config

        :param path: path to aggregation config
        :param kwargs: do nothing
        :return: filtered clickstream
        """
        from retentioneering.core.tree_selector import use_tree_filter
        res = use_tree_filter(self._obj, path, **kwargs)
        return res

    def _create_bins(self, data, time_step, index_col):

        tmp = data.join(
            data.groupby(index_col or self.retention_config['index_col'])
            [self.retention_config['event_time_col']].min(),
            on=index_col or self.retention_config['index_col'], rsuffix='_min')

        data['bins'] = (
                data[self.retention_config['event_time_col']] - tmp[self.retention_config['event_time_col'] + '_min']
        )
        data['bins'] = np.floor(data['bins'] / np.timedelta64(1, time_step))

    def survival_curves(self, groups, spec_event=None, time_min=None, time_max=None, event_col=None, index_col=None,
                        target_event=None, time_step='D', plotting=True, **kwargs):
        """
        Plot survival curves for given grouping

        :param groups: np.array of clickstream shape, that splits data into different groups
        :param spec_event: event specific for test,
            e.g. we change auth flow, so we need to compare only users, who have started authorization,
            in this case `spec_event='auth_start'`
        :param time_min: time when a/b test was started. if None, then whole dataset is used
        :param time_max: time when a/b test was ended. if None, then whole dataset is used
        :param event_col: name of event column, e.g. `event_name` in our examples.
            If None, init_config is used.
        :param index_col: name of index column, e.g. `user_pseudo_id` in our examples.
            If None, init_config is used.
        :param target_event: name of target event. If None, init_config is used.
        :param time_step: time step for calculation of survival rate at specific time.
            Default is day (`'D'`).
            Possible options:
                (`'D'` -- day, `'M'` -- month, `'h'` -- hour, `'m'` -- minute, `'Y'` -- year,
                 `'W'` -- week, `'s'` -- seconds, `'ms'` -- milliseconds.
        :param plotting: if True, then plots survival curves
        :param kwargs: do nothing
        :return: pd.DataFrame with points at survival curves and prints chi-squared LogRank test for equality statistics
        """

        data = self._obj.copy()
        if spec_event is not None:
            users = (data
                     [data[event_col or self.retention_config['event_col']]
                      == spec_event]
                     [index_col or self.retention_config['index_col']]).unique()
            f = data[index_col or self.retention_config['index_col']].isin(users)
            data = data[f].copy()
            groups = groups[f].copy()
        if type(data[self.retention_config['event_time_col']].iloc[0]) not in (int, float, object, str):
            data[self.retention_config['event_time_col']] = pd.to_datetime(
                data[self.retention_config['event_time_col']])
        if time_min is not None:
            f = data[self.retention_config['event_time_col']] >= pd.to_datetime(time_min)
            data = data[f].copy()
            groups = groups[f].copy()
        if time_max is not None:
            f = data[self.retention_config['event_time_col']] <= pd.to_datetime(time_max)
            data = data[f].copy()
            groups = groups[f].copy()
        self._create_bins(data, time_step, index_col)

        data['metric_col'] = (data
                              [event_col or self.retention_config['event_col']]
                              == (target_event or self.retention_config['positive_target_event']))
        tmp = data[data.metric_col == 1]
        curves = tmp.groupby(
            [groups, 'bins']
        )[index_col or self.retention_config['index_col']].nunique().rename('metric').reset_index()
        curves = curves.sort_values('bins', ascending=False)
        curves['metric'] = curves.groupby(groups.name).metric.cumsum()
        curves = curves.sort_values('bins')
        res = (curves
               .merge(curves
                      .groupby(groups.name)
                      .head(1)[[groups.name, 'metric']],
                      on=groups.name, suffixes=('', '_max')))
        self._logrank_test(res, groups.name)
        res['metric'] = res.metric / res.metric_max
        if plotting:
            plot.sns.lineplot(data=res, x='bins', y='metric', hue=groups.name)
        return res

    @staticmethod
    def _logrank_test(x, group_col):
        x['next_metric'] = x.groupby(group_col).metric.shift(-1)
        x['o'] = (x['metric'] - x['next_metric'])
        oj = x.groupby('bins').o.sum()
        nj = x.groupby('bins').metric.sum()
        exp = (oj / nj).rename('exp')
        x = x.join(exp, on='bins')
        x1 = x[x[group_col]]
        x1.index = x1.bins
        up = (x1.o - x1.exp * x1.metric).sum()
        var = ((oj * (x1.metric / nj) * (1 - x1.metric / nj) * (nj - oj)) / (nj - 1)).sum()
        z = up ** 2 / var

        from scipy.stats import chi2
        pval = 1 - chi2.cdf(z, df=1)
        print(f"""
        There is {'' if pval <= 0.05 else 'no '}significant difference
        log-rank chisq: {z}
        P-value: {pval}
        """)

    def index_based_split(self, index_col=None, test_size=0.2, seed=0):
        """
        Split dataset based on index

        :param index_col: index col, if None, than use config
        :param test_size: rate of test subsample
        :param seed: random seed
        :return: train, test
        """
        np.random.seed(seed)
        ids = np.random.permutation(self._obj[index_col or self.retention_config['index_col']].unique())
        f = self._obj[index_col or self.retention_config['index_col']].isin(ids[int(ids.shape[0] * test_size):])
        return self._obj[f].copy(), self._obj[~f].copy()
