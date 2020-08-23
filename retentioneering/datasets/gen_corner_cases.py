import pandas as pd

__all__ = ['keep_one_user',
           'keep_one_event']

def keep_one_user(dataset, dataset_config):
    df = dataset.copy()

    user_to_keep = df[dataset_config.index_col][0]
    df = df[df[dataset_config.index_col]==user_to_keep].copy()
    return df


def keep_one_event(dataset, dataset_config):
    df = dataset.copy()

    event_to_keep = df[dataset_config.event][0]
    df = df[df[dataset_config.event]==event_to_keep].copy()
    return df