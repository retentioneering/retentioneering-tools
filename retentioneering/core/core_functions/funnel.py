# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

import numpy as np

from ...visualization.plot_funnel import plot_stacked_funnel


def funnel(self, *,
           targets,
           funnel_type=None,
           groups=None,
           group_names=None):
    """
    Plots simple convertion funnel with stages as specified in targets parameter.

    Parameters
    ----------
    targets: list of str
        List of events used as stages for the funnel. Absolute and relative
        number of users who reached specified events at least once will be
        plotted. Multiple events can be grouped together as individual state
        by combining them as sub list.
    funnel_type: None or 'closed'
        if None all users will be counted on each stage
        if closed - dataset will be changed. Each stage will include only users,
        who was on all previous stages.
    groups: list of collectibles (optional, default None)
        List of user_ids collections. Funnel for each user_id collection
        will be plotted. If None all users from dataset will be plotted
    group_names: list of strings (optional, default None)
        Names for specified user groups to place in a legend. If specified
         len(group_names) must be equal to len(groups).

    Returns
    -------
    Funnel plot

    """
    if not np.all([(isinstance(targets, list))]):
        raise ValueError('targets must be list!')

    if not np.all([isinstance(i, (list, str)) for i in targets]):
        raise ValueError('elements of targets must be string!')

    data = self._obj
    event_col = self.retention_config['event_col']
    index_col = self.retention_config['user_col']

    if funnel_type=='closed':
        data = prepare_data_for_closed_funnel(self, data, targets, groups=groups)

    # if group not specified select all users
    if groups is None:
        groups = [data[index_col].unique()]
        group_names = ['all users']
    elif group_names is None:
        group_names = [f"group {i}" for i in range(len(groups))]

    # pre-process targets

    # format targets to list of lists:
    targets_new = []
    for i in targets:
        if type(i) != list:
            targets_new.append([i])
    # for n, i in enumerate(targets):
    #     if type(i) != list:
            #target[n] = [i]

    # generate target_names:
    target_names = []
    for t in targets_new:
        # get name
        target_names.append(' | '.join(t).strip(' | '))

    res_dict = {}
    for group, group_name in zip(groups, group_names):

        # isolate users from group
        group_data = data[data[index_col].isin(group)]

        vals = []
        for target in targets_new:
            # define how many users have particular target:
            vals.append(group_data[group_data[event_col].isin(target)][index_col].nunique())
        res_dict[group_name] = {'targets': target_names, 'values': vals}

    return plot_stacked_funnel(res_dict)

def prepare_data_for_closed_funnel(self,
                                   data,
                                   targets,
                                   groups):

    """
    Prepare dataset to put it in def funnel(). On each next stage remain only users
    who were at all previous

    Parameters
    ----------
    data: list of str
    targets: list of str
        List of events used as stages for the funnel. Absolute and relative
        number of users who reached specified events at least once will be
        plotted. Multiple events can be grouped together as individual state
        by combining them as sub list.
    groups: list of collectibles (optional, default None)
        List of user_ids collections. Funnel for each user_id collection
        will be plotted. If None all users from dataset will be plotted

    Returns
    -------
    Cropped full DataFrame

    """
    index_col = self.retention_config['user_col']

    df = data.copy()
    if groups:
        groups_ = list(groups)
        for gr in groups_:
            df = crop_df(self, df, targets, gr=gr)
    #если не указаны сегменты
    else:
        gr = df[index_col].unique()
        df = crop_df(self, df, targets, gr=gr)

    return df

def crop_df(self, df, targets, gr):

    """
    Plots simple convertion funnel with stages as specified in targets parameter.

    Parameters
    ----------
    df: DataFrame
        Copy of original DataFrame 
    targets: list of str
        List of events used as stages for the funnel. Absolute and relative
        number of users who reached specified events at least once will be
        plotted. Multiple events can be grouped together as individual state
        by combining them as sub list.
    gr: np.array or set()
        iterable object with users ids from one segment(group)

    Returns
    -------
    Cropped DataFrame in single group (segment) of users

    """
    event_col = self.retention_config['event_col']
    index_col = self.retention_config['user_col']

    users_0 = set(df[(df[event_col]==targets[0])&(df[index_col].isin(gr))][index_col])


    prev_users_target = users_0

    for target in targets[1:]:
        user_target = set(df[(df[event_col]==target)&(df[index_col].isin(users_0))][index_col])
        user_target = user_target - (user_target - prev_users_target)

        prev_users_target = user_target
        #удаляются юзеры, которых не было на предыдущем этапе

        df = df.drop(df[(df[event_col] == target)&\
                    (df[index_col].isin(gr))&\
                    (~df[index_col].isin(user_target))].index)

    return df
