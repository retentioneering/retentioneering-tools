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
