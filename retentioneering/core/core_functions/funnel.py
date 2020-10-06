# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

from ...visualization.plot_funnel import plot_stacked_funnel


def funnel(self, *,
           targets,
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

    data = self._obj
    event_col = self.retention_config['event_col']
    index_col = self.retention_config['user_col']

    # if group not specified select all users
    if groups is None:
        groups = [data[index_col].unique()]
        group_names = ['all users']
    elif group_names is None:
        group_names = [f"group {i}" for i in range(len(groups))]

    # pre-process targets

    # format targets to list of lists:
    for n, i in enumerate(targets):
        if type(i) != list:
            targets[n] = [i]

    # generate target_names:
    target_names = []
    for t in targets:
        # get name
        target_names.append(' | '.join(t).strip(' | '))

    res_dict = {}
    for group, group_name in zip(groups, group_names):

        # isolate users from group
        group_data = data[data[index_col].isin(group)]

        vals = []
        for target in targets:
            # define how many users have particular target:
            vals.append(group_data[group_data[event_col].isin(target)][index_col].nunique())
        res_dict[group_name] = {'targets': target_names, 'values': vals}

    return plot_stacked_funnel(res_dict)