# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

import pandas as pd


class BaseDataset(object):
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self.retention_config = {}

    def _get_shift(self, *,
                   index_col=None,
                   event_col=None):
        index_col = index_col or self.retention_config['user_col']
        event_col = event_col or self.retention_config['event_col']
        time_col = self.retention_config['event_time_col']

        data = self._obj.copy()
        data.sort_values([index_col, time_col], inplace=True)
        shift = data.groupby(index_col).shift(-1)

        data['next_'+event_col] = shift[event_col]
        data['next_'+str(time_col)] = shift[time_col]

        return data

    def split_sessions(self, *,
                       by_event=None,
                       thresh,
                       eos_event=None,
                       session_col='session_id'):

        session_col_arg = session_col or 'session_id'

        index_col = self.retention_config['user_col']
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


