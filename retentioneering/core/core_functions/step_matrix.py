# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


import pandas as pd

from retentioneering.visualization import plot_step_matrix


def step_matrix(self, *,
                max_steps=20,
                weight_col=None,
                precision=2,
                targets=None,
                accumulated=None,
                sorting=None,
                thresh=0,
                centered=None,
                groups=None,
                show_plot=True):
    """
    Plots heatmap with distribution of users over trajectory steps ordered by
    event name. Matrix rows are event names, columns are aligned user trajectory
    step numbers and the values are shares of users. A given entry X at column i
    and event j means at i'th step fraction of users X  have specific event j.

    Parameters
    ----------
    max_steps: int (optional, default 20)
        Maximum number of steps in trajectory to include.

    weight_col: str (optional, default None)
        Aggregation column for edge weighting. If None, specified index_col
        from retentioneering.config will be used as column name. For example,
        can be specified as `session_id` if dataframe has such column.

    precision: int (optional, default 2)
        Number of decimal digits after 0 to show as fractions in the heatmap.

    thresh: float (optional, default 0)
        Used to remove rare events. Aggregates all rows where all values are
        less then specified threshold.

    targets: list (optional, default None)
        List of events names (as str) to include in the bottom of
        step_matrix as individual rows. Each specified target will have
        separate color-coding space for clear visualization. Example:
        ['product_page', 'cart', 'payment']. If multiple targets need to
        be compared and plotted using same color-coding scale, such targets
        must be combined in sub-list.
        Examples: ['product_page', ['cart', 'payment']]

    accumulated: string (optional, default None)
        Option to include accumulated values for targets. Valid values are
        None (do not show accumulated tartes), 'both' (show step values and
        accumulated values), 'only' (show targets only as accumulated).

    centered: dict (optional, default None)
        Parameter used to align user trajectories at specific event at specific
        step. Has to contain three keys:
            'event': str, name of event to align
            'left_gap': int, number of events to include before specified event
            'occurrence': int which occurance of event to align (typical 1)
        When this parameter is not None only users which have specified i'th
        'occurance' of selected event preset in their trajectories will
        be included. Fraction of such remaining users is specified in the title of
        centered step_matrix. Example:
        {'event': 'cart', 'left_gap': 8, 'occurrence': 1}

    sorting: list (optional, default None)
        List of events_names (as string) can be passed to plot step_matrix with
        specified ordering of events. If None rows will be ordered according
        to i`th value (first row, where 1st element is max, second row, where
        second element is max, etc)

    groups: tuple (optional, default None)
        Can be specified to plot step differential step_matrix. Must contain
        tuple of two elements (g_1, g_2): where g_1 and g_2 are collections
        of user_id`s (list, tuple or set). Two separate step_matrixes M1 and M2
        will be calculated for users from g_1 and g_2, respectively. Resulting
        matrix will be the matrix M = M1-M2. Note, that values in each column
        in differential step matrix will sum up to 0 (since columns in both M1
        and M2 always sum up to 1).

    show_plot: bool (optional, default True)
        whether to show resulting heatmap or not.

    Returns
    -------
    Dataframe with max_steps number of columns and len(event_col.unique)
    number of rows at max, or less if used thr > 0.

    Return type
    -----------
    pd.DataFrame
    """

    event_col = self.retention_config['event_col']
    weight_col = weight_col or self.retention_config['user_col']
    time_col = self.retention_config['event_time_col']

    data = self._obj.copy()

    # add termination event to each history:
    data = data.rete.split_sessions(thresh=None,
                                    eos_event='ENDED',
                                    session_col=None)

    data['event_rank'] = 1
    data['event_rank'] = data.groupby(weight_col)['event_rank'].cumsum()

    # BY HERE WE NEED TO OBTAIN FINAL DIFF piv and piv_targets before sorting, thresholding and plotting:

    if groups:
        data_pos = data[data[weight_col].isin(groups[0])].copy()
        if len(data_pos) == 0:
            raise IndexError('Users from positive group are not present in dataset')
        piv_pos, piv_targets_pos, fraction_title, window, targets_plot = \
            _step_matrix_values(data=data_pos,
                                weight_col=weight_col,
                                event_col=event_col,
                                time_col=time_col,
                                targets=targets,
                                accumulated=accumulated,
                                centered=centered,
                                max_steps=max_steps)

        data_neg = data[data[weight_col].isin(groups[1])].copy()
        if len(data_pos) == 0:
            raise IndexError('Users from negative group are not present in dataset')
        piv_neg, piv_targets_neg, fraction_title, window, targets_plot = \
            _step_matrix_values(data=data_neg,
                                weight_col=weight_col,
                                event_col=event_col,
                                time_col=time_col,
                                targets=targets,
                                accumulated=accumulated,
                                centered=centered,
                                max_steps=max_steps)

        def join_index(df1, df2):
            full_list = set(df1.index) | set(df2.index)
            for i in full_list:
                if i not in df1.index:
                    df1.loc[i] = 0
                if i not in df2.index:
                    df2.loc[i] = 0

        join_index(piv_pos, piv_neg)
        piv = piv_pos - piv_neg

        if targets:
            join_index(piv_targets_pos, piv_targets_neg)
            piv_targets = piv_targets_pos - piv_targets_neg
        else:
            piv_targets = None

    else:
        piv, piv_targets, fraction_title, window, targets_plot = \
            _step_matrix_values(data=data,
                                weight_col=weight_col,
                                event_col=event_col,
                                time_col=time_col,
                                targets=targets,
                                accumulated=accumulated,
                                centered=centered,
                                max_steps=max_steps)

    thresh_index = 'THRESHOLDED_'
    if thresh != 0:
        # find if there are any rows to threshold:
        thresholded = piv.loc[(piv.abs() < thresh).all(1)].copy()
        if len(thresholded) > 0:
            piv = piv.loc[(piv.abs() >= thresh).any(1)].copy()
            thresh_index = f'THRESHOLDED_{len(thresholded)}'
            piv.loc[thresh_index] = thresholded.sum()

    if sorting is None:
        piv = _sort_matrix(piv)

        keep_in_the_end = []
        keep_in_the_end.append('ENDED') if ('ENDED' in piv.index) else None
        keep_in_the_end.append(thresh_index) if (thresh_index in piv.index) else None

        events_order = [*(i for i in piv.index if i not in keep_in_the_end), *keep_in_the_end]
        piv = piv.loc[events_order]

        # add sorting list to config
        self.retention_config['step_matrix'] = {'sorting': events_order}

    else:
        # if custom sorting was provided:
        if not isinstance(sorting, list):
            raise TypeError('parameter `sorting` must be a list of event names')
        if {*sorting} != {*piv.index}:
            raise ValueError(
                'provided sorting list does not match list of events. Run with `sorting` = None to get the actual list')

        piv = piv.loc[sorting]

    if centered:
        piv.columns = [f'{int(i) - window - 1}' for i in piv.columns]
        if targets:
            piv_targets.columns = [f'{int(i) - window - 1}' for i in piv_targets.columns]

    if show_plot:
        plot_step_matrix.step_matrix(piv, piv_targets,
                                     targets_list=targets_plot,
                                     title=f'{"centered" if centered else ""} {"differential " if groups else ""}step matrix {fraction_title}',
                                     centered_position=window,
                                     precision=precision)

    return piv


def _step_matrix_values(*, data, weight_col, event_col, time_col,
                       targets, accumulated, centered, max_steps):
    """
    Supplemental function to calculate values for step_matrix

    Parameters same as in step_matrix

    Parameters
    ----------
    data
    weight_col
    event_col
    time_col
    targets
    accumulated
    centered
    max_steps

    Returns
    -------

    pandas Dataframe

    """

    def pad_cols(df, max_steps):
        """
        Parameters
        ----------
        df - dataframe
        max_steps - number of cols
        Returns
        -------
        returns Dataframe with columns from 0 to max_steps
        """
        df = df.copy()
        if max(df.columns) < max_steps:
            for col in range(max(df.columns) + 1, max_steps + 1):
                df[col] = 0
        # add missing cols if needed:
        if min(df.columns) > 1:
            for col in range(1, min(df.columns)):
                df[col] = 0
        # sort cols
        return df[list(range(1, max_steps + 1))]

    from copy import deepcopy

    data = data.copy()
    targets = deepcopy(targets)

    # ALIGN DATA IF CENTRAL
    fraction_title = ''
    window = None
    if centered is not None:
        # CHECKS
        if (not isinstance(centered, dict) or
                ({'event', 'left_gap', 'occurrence'} - {*centered.keys()})):
            raise ValueError("Parameter centered must be dict with following keys: 'event', 'left_gap', 'occurrence'")

        center_event = centered.get('event')
        window = centered.get('left_gap')
        occurrence = centered.get('occurrence')
        if occurrence < 1 or not isinstance(occurrence, int):
            raise ValueError("key value 'occurrence' must be Int >=1")
        if window < 0 or not isinstance(window, int):
            raise ValueError("key value 'left_gap' must be Int >=0")
        if center_event not in data[event_col].unique():
            raise ValueError(f'Event "{center_event}" not found in the column: "{event_col}"')

        # keep only users who have center_event at least N = occurrence times
        data['occurrence'] = data[event_col] == center_event
        data['occurance_counter'] = data.groupby(weight_col)['occurrence'].cumsum() * data['occurrence']
        users_to_keep = data[data['occurance_counter'] == occurrence][weight_col].unique()

        if len(users_to_keep) == 0:
            raise ValueError(f'no records found with event "{center_event}" occuring N={occurrence} times')

        fraction_used = len(users_to_keep) / data[weight_col].nunique() * 100
        if fraction_used < 100:
            fraction_title = f'({fraction_used:.1f}% of total records)'
        data = data[data[weight_col].isin(users_to_keep)].copy()

        def pad_to_center(x):
            position = x.loc[(x[event_col] == center_event) &
                             (x['occurance_counter'] == occurrence)]['event_rank'].min()
            shift = position - window - 1
            x['event_rank'] = x['event_rank'] - shift
            return x
        data = data.groupby(weight_col).apply(pad_to_center)
        data = data[data['event_rank'] > 0].copy()

    # calculate step matrix elements:
    agg = (data
           .groupby(['event_rank', event_col])[weight_col]
           .nunique()
           .reset_index())
    agg[weight_col] /= data[weight_col].nunique()

    agg = agg[agg['event_rank'] <= max_steps]
    agg.columns = ['event_rank', 'event_name', 'freq']

    piv = agg.pivot(index='event_name', columns='event_rank', values='freq').fillna(0)

    # add missing cols if number of events < max_steps:
    piv = pad_cols(piv, max_steps)

    piv.columns.name = None
    piv.index.name = None

    # MAKE TERMINATED STATE ACCUMULATED:
    if 'ENDED' in piv.index:
        piv.loc['ENDED'] = piv.loc['ENDED'].cumsum().fillna(0)

    # add NOT_STARTED events for centered matrix
    if centered:
        piv.loc['NOT_STARTED'] = 1 - piv.sum()

    # ADD ROWS FOR TARGETS:
    piv_targets = None
    if targets:
        # obtain flatten list of targets:
        targets_flatten = list(pd.core.common.flatten(targets))

        # format targets to list of lists:
        for n, i in enumerate(targets):
            if type(i) != list:
                targets[n] = [i]

        agg_targets = (data
                       .groupby(['event_rank', event_col])[time_col]
                       .count()
                       .reset_index())
        agg_targets[time_col] /= data[weight_col].nunique()
        agg_targets.columns = ['event_rank', 'event_name', 'freq']

        agg_targets = agg_targets[agg_targets['event_rank'] <= max_steps]

        piv_targets = agg_targets.pivot(index='event_name', columns='event_rank', values='freq').fillna(0)
        piv_targets = pad_cols(piv_targets, max_steps)

        # if target is not present in dataset add zeros:
        for i in targets_flatten:
            if i not in piv_targets.index:
                piv_targets.loc[i] = 0

        piv_targets = piv_targets.loc[targets_flatten].copy()

        piv_targets.columns.name = None
        piv_targets.index.name = None

        if accumulated == 'only':
            piv_targets.index = map(lambda x: 'ACC_' + x, piv_targets.index)
            for i in piv_targets.index:
                piv_targets.loc[i] = piv_targets.loc[i].cumsum().fillna(0)

            # change names is targets list:
            for target in targets:
                for j, item in enumerate(target):
                    target[j] = 'ACC_'+item

        if accumulated == 'both':
            for i in piv_targets.index:
                piv_targets.loc['ACC_'+i] = piv_targets.loc[i].cumsum().fillna(0)

            # add accumulated targets to the list:
            targets_not_acc = deepcopy(targets)
            for target in targets:
                for j, item in enumerate(target):
                    target[j] = 'ACC_'+item
            targets = targets_not_acc + targets

    return piv, piv_targets, fraction_title, window, targets


def _sort_matrix(step_matrix):
    x = step_matrix.copy()
    order = []
    for i in x.columns:
        new_r = x[i].idxmax()
        order.append(new_r)
        x = x.drop(new_r)
        if x.shape[0] == 0:
            break
    order.extend(list(set(step_matrix.index) - set(order)))
    return step_matrix.loc[order]
