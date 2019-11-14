# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score, accuracy_score


@pd.api.extensions.register_dataframe_accessor("attribution")
class MultiChannelAttribution(object):
    def __init__(self, obj):
        self._obj = obj
        self.retention_config = obj.retention.retention_config

    def _prepare_data(self, target_col=None, target_event=None, event_col=None, index_col=None, **kwargs):
        data = self._obj
        tmp = self._obj.copy()
        if target_col is not None:
            tmp = tmp.rename({target_col: 'target'}, 1)
        else:
            f = tmp[
                index_col or self.retention_config['index_col']
                ][tmp[event_col or self.retention_config['event_col']] == (
                        target_event or self.retention_config['positive_target_event'])].unique()
            tmp = tmp[~tmp[event_col or self.retention_config['event_col']].isin(
                [(target_event or self.retention_config['positive_target_event']),
                 self.retention_config['negative_target_event']])]
            tmp['target'] = data[index_col or self.retention_config['index_col']].isin(f)
        return tmp

    def _calc_unique_crs(self, tmp, event_col, index_col, **kwargs):
        return (tmp[tmp.target]
                .groupby(event_col or self.retention_config['event_col'])[
                    index_col or self.retention_config['index_col']]
                .nunique())

    def _calc_unique_usrs(self, tmp, event_col, index_col, **kwargs):
        return (tmp
                .groupby(event_col or self.retention_config['event_col'])[
                    index_col or self.retention_config['index_col']]
                .nunique())

    def removal_effect(self, target_col=None, target_event=None, event_col=None, index_col=None, tmp=None, **kwargs):
        """
        Removal effect is a rate of missing conversions from all conversions
        """
        if tmp is None:
            tmp = self._prepare_data(target_col, target_event, event_col, index_col)
        rem_effect = (self._calc_unique_crs(tmp, event_col, index_col)
                      / tmp[tmp.target][index_col or self.retention_config['index_col']].nunique()).reset_index()
        rem_effect.columns = [event_col or self.retention_config['event_col'], 'removal_effect']
        return rem_effect.sort_values('removal_effect', ascending=False)

    @staticmethod
    def _abs_sorting(data, col):
        data['sorting'] = data[col].abs()
        return data.sort_values('sorting', ascending=False).drop('sorting', 1)

    def causal_effect(self, target_col=None, target_event=None, event_col=None, index_col=None, **kwargs):
        """
        Causal effect for channel is a difference between overall conversion rate
        and conversion rate without the channel
        """
        tmp = self._prepare_data(target_col, target_event, event_col, index_col)
        all_cnt = tmp[index_col or self.retention_config['index_col']].nunique()
        pos_cnt = tmp[tmp.target][index_col or self.retention_config['index_col']].nunique()
        overall_cr = pos_cnt / all_cnt
        all_channel = self._calc_unique_usrs(tmp, event_col, index_col)
        pos_channel = self._calc_unique_crs(tmp, event_col, index_col)
        cr_wo_channel = (pos_cnt - pos_channel) / (all_cnt - all_channel)
        causal = (overall_cr - cr_wo_channel).rename('causal_effect').reset_index()
        return self._abs_sorting(causal, 'causal_effect')

    @staticmethod
    def _subset_calc(x):
        return ' '.join(list(set(x)))

    def _find_unique_subsets(self, tmp, event_col=None,
                             index_col=None, chain_length=2, thresh=10, relative_weighting=False, **kwargs):
        user_subsets = tmp.groupby(
            index_col or self.retention_config['index_col']
        ).agg({
            (event_col or self.retention_config['event_col']): self._subset_calc,
            'target': 'max'
        })
        user_subsets['chain_length'] = user_subsets[event_col or self.retention_config['event_col']].str.split(' ').map(
            len)
        res = user_subsets[user_subsets.chain_length <= chain_length].groupby(
            event_col or self.retention_config['event_col']).agg({
                'target': 'mean',
                'chain_length': 'count'
            })
        res = res[res.chain_length >= thresh]
        if relative_weighting:
            overall = res.chain_length.sum()
        else:
            overall = user_subsets.shape[0]
        res['freq'] = res.chain_length / overall
        return res

    @staticmethod
    def _calc_shapley(res):
        channel_attr = {}
        for sub in res.index:
            _sub = set(sub.split())
            for channel in _sub:
                ch = res.loc[sub]
                if len(_sub) == 1:
                    if channel not in channel_attr:
                        channel_attr[channel] = 0
                    channel_attr[channel] += ch.target * ch.freq
                else:
                    woch = res.reindex([' '.join(_sub - {channel})]).fillna(0)
                    if channel not in channel_attr:
                        channel_attr[channel] = 0
                    channel_attr[channel] += (ch.target - woch.target.item()) * ch.freq
        return channel_attr

    def _filter_first_touch(self, tmp, **kwargs):
        return tmp.groupby(kwargs.get('index_col', self.retention_config['index_col'])).head(kwargs.get('n', 3))

    def _filter_last_touch(self, tmp, **kwargs):
        return tmp.groupby(kwargs.get('index_col', self.retention_config['index_col'])).tail(kwargs.get('n', 3))

    def _filter_channels(self, tmp, channel_filter, **kwargs):
        if channel_filter:
            func = getattr(self, '_filter_' + channel_filter['type'])
            tmp = func(tmp, **channel_filter)
        return tmp

    def shapley_value(self, target_col=None, target_event=None, event_col=None, index_col=None,
                      chain_length=3, thresh=10, relative_weighting=False, channel_filter=None, **kwargs):
        """
        Shapley values is a weighted sum of additional value of each channel into each channel sets
        """
        tmp = self._prepare_data(target_col, target_event, event_col, index_col)
        tmp = self._filter_channels(tmp, channel_filter)
        res = self._find_unique_subsets(tmp, event_col, index_col, chain_length, thresh, relative_weighting)
        channel_attr = self._calc_shapley(res)
        attr = pd.Series(channel_attr).reset_index()
        attr.columns = [event_col or self.retention_config['event_col'], 'shapley_value']
        return self._abs_sorting(attr, 'shapley_value')

    def first_touch(self, target_col=None, target_event=None, event_col=None, index_col=None, **kwargs):
        tmp = self._prepare_data(target_col, target_event, event_col, index_col)
        tmp = self._filter_first_touch(tmp, n=1)
        attr = tmp.groupby([event_col or self.retention_config['event_col']]).target.mean().reset_index()
        attr.columns = [event_col or self.retention_config['event_col'], 'first_touch']
        return attr.sort_values('first_touch', ascending=False)

    def last_touch(self, target_col=None, target_event=None, event_col=None, index_col=None, **kwargs):
        tmp = self._prepare_data(target_col, target_event, event_col, index_col)
        tmp = self._filter_last_touch(tmp, n=1)
        attr = tmp.groupby([event_col or self.retention_config['event_col']]).target.mean().reset_index()
        attr.columns = [event_col or self.retention_config['event_col'], 'last_touch']
        return attr.sort_values('last_touch', ascending=False)

    def linear(self, target_col=None, target_event=None, event_col=None, index_col=None, **kwargs):
        tmp1 = self._prepare_data(target_col, target_event, event_col, index_col)
        tmp = tmp1[tmp1.target].copy()
        tmp['cnt'] = tmp.groupby([index_col or self.retention_config['index_col']]).target.apply(lambda x: x / x.sum())
        attr = (
                tmp.groupby([event_col or self.retention_config['event_col']]).cnt.sum() /
                tmp1.groupby([event_col or self.retention_config['event_col']]).size()
        ).reset_index()
        attr.columns = [event_col or self.retention_config['event_col'], 'linear']
        return attr.sort_values('linear', ascending=False)

    def position_based(self, weight=0.8, target_col=None, target_event=None, event_col=None, index_col=None, **kwargs):
        tmp1 = self._prepare_data(target_col, target_event, event_col, index_col)
        tmp = tmp1[tmp1.target].copy()
        tmp['cnt'] = (tmp
                      .groupby([index_col or self.retention_config['index_col']])
                      .target
                      .apply(lambda x: x * np.array(
                            [weight / 2] + (
                                ([(1 - weight) / (x.shape[0] - 2)] * (x.shape[0] - 2) + [weight / 2])
                                if x.shape[0] > 2 else [weight / 2] if x.shape[0] == 2 else [])
        )))
        tmp['cnt'] = tmp.groupby([index_col or self.retention_config['index_col']]).cnt.apply(lambda x: x / x.sum())
        attr = (
                tmp.groupby([event_col or self.retention_config['event_col']]).cnt.sum() /
                tmp1.groupby([event_col or self.retention_config['event_col']]).size()
        ).reset_index()
        attr.columns = [event_col or self.retention_config['event_col'], 'position_based']
        return attr.sort_values('position_based', ascending=False)

    def time_decay(self, target_col=None, target_event=None, event_col=None, index_col=None, **kwargs):
        tmp1 = self._prepare_data(target_col, target_event, event_col, index_col)
        tmp = tmp1[tmp1.target].copy()
        tmp['cnt'] = tmp.groupby([index_col or self.retention_config['index_col']]).target.cumsum()
        tmp['cnt'] = tmp.groupby([index_col or self.retention_config['index_col']]).cnt.apply(lambda x: x / x.sum())
        attr = (
                tmp.groupby([event_col or self.retention_config['event_col']]).cnt.sum() /
                tmp1.groupby([event_col or self.retention_config['event_col']]).size()
        ).reset_index()
        attr.columns = [event_col or self.retention_config['event_col'], 'time_decay']
        return attr.sort_values('time_decay', ascending=False)

    def check_openning(self, target_col=None, target_event=None, event_col=None, index_col=None, **kwargs):
        ft = self.first_touch(target_col, target_event, event_col, index_col)
        lt = self.last_touch(target_col, target_event, event_col, index_col)
        cmp = ft.merge(lt, how='outer', on=(event_col or self.retention_config['event_col'])).fillna(0)
        cmp['is_open'] = cmp.first_touch > cmp.last_touch
        cmp['openning_uplift'] = cmp.first_touch / (cmp.last_touch + 1e-20)
        cmp.openning_uplift = np.where((cmp.last_touch == 0) & (cmp.first_touch != 0),
                                       np.inf,
                                       np.where(cmp.first_touch == 0, 0, cmp.openning_uplift))
        return cmp.loc[:,
               [event_col or self.retention_config['event_col'], 'is_open', 'openning_uplift']
               ].sort_values('openning_uplift', ascending=False)

    def score_all(self, index_col=None, event_col=None, **kwargs):
        scores = {}
        res = {}
        tmp = self._prepare_data(**kwargs)
        for i in ['causal_effect', 'removal_effect', 'shapley_value', 'first_touch', 'last_touch', 'linear',
                  'time_decay', 'position_based', 'rete_attr']:
            scores[i] = {}
            scores[i]['roc'], scores[i]['accuracy'], res[i] = self._calc_score(i, tmp, **kwargs)
        acc = (
                tmp[tmp.target][index_col or self.retention_config['index_col']].nunique() /
                tmp[index_col or self.retention_config['index_col']].nunique()
        )
        scores['constant'] = {
            'roc': 0.5,
            'accuracy': acc if acc > 0.5 else 1 - acc
        }
        return pd.DataFrame(scores).T.sort_values('accuracy', ascending=False), self._merge_df(res)

    def _merge_df(self, res, event_col=None):
        df = pd.DataFrame(columns=[event_col or self.retention_config['event_col']])
        for i, j in res.items():
            df = df.merge(j, how='outer', on=(event_col or self.retention_config['event_col']))
        return df.fillna(0)

    @staticmethod
    def _opt_split(gt, probs):
        res = []
        for i in range(1, 100):
            res.append(accuracy_score(gt, probs > i / 100))
        return accuracy_score(gt, probs > ((np.array(res).argmax() + 1) / 100))

    def _calc_score(self, atmetric, tmp=None, index_col=None, event_col=None, **kwargs):
        if type(atmetric) == str:
            func = getattr(self, atmetric)
            at = func(**kwargs)
        else:
            at = atmetric
            atmetric = atmetric.columns[1]
        if tmp is None:
            tmp = self._prepare_data(**kwargs)
        tmp = tmp.merge(at, how='left', on=event_col or self.retention_config['event_col'])
        tmp[atmetric] = tmp[atmetric].fillna(0)
        x = tmp.groupby(index_col or self.retention_config['index_col'])[atmetric].sum()
        prob = 1 / (1 + np.exp(-x))
        return (roc_auc_score(tmp.groupby(index_col or self.retention_config['index_col']).target.max().loc[prob.index].values, prob.values),
                self._opt_split(tmp.groupby(index_col or self.retention_config['index_col']).target.max().loc[prob.index].values, prob.values), at)

    def find_attr_weights(self, mod, event_col=None, **kwargs):
        res = {}
        if hasattr(mod.mod, 'coef_'):
            weights = dict(zip(mod.data.columns, mod.mod.coef_[0]))
        else:
            weights = dict(zip(mod.data.columns, mod.mod.feature_importances_))
        for i in set(self._obj[event_col or self.retention_config['event_col']]) - {self.retention_config['positive_target_event'],
                                                                                    self.retention_config['negative_target_event']}:
            ids = [j for j in mod.data.columns if i in j]
            res[i] = mod.data.loc[:, ids].dot(np.array([weights.get(j) for j in ids])).mean()
        x = pd.Series(res).reset_index()
        x.columns = [event_col or self.retention_config['event_col'], 'rete_attr']
        return x.sort_values('rete_attr', ascending=False)

    def rete_attr(self, **kwargs):
        mod = self._obj.retention.create_model(**kwargs)
        return self.find_attr_weights(mod, **kwargs)

    def sample_users(self, ch, dist, event_col=None, index_col=None):
        users = (
            self._obj[self._obj[event_col or self.retention_config['event_col']] == ch][
                self._obj[event_col or self.retention_config['index_col']]
            ].unique()
        )

    def calc_distribution(self, at):
        return self._obj.shape[0] * at

    def attributive_sampling(self, atmetric, tmp=None, **kwargs):
        if type(atmetric) == str:
            func = getattr(self, atmetric)
            at = func(**kwargs)
        else:
            at = atmetric
            atmetric = atmetric.columns[1]
        if tmp is None:
            tmp = self._prepare_data(**kwargs)

