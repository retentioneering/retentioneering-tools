# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


__all__ = ['keep_one_user',
           'keep_one_event']


def keep_one_user(dataset):
    index_col = dataset.rete.retention_config['user_col']

    df = dataset.copy()

    user_to_keep = df[index_col][0]
    df = df[df[index_col] == user_to_keep].copy()

    return df


def keep_one_event(dataset):
    event_col = dataset.rete.retention_config['event_col']

    df = dataset.copy()

    event_to_keep = df[event_col][0]
    df = df[df[event_col] == event_to_keep].copy()

    return df
