def create_model(self, model_type=LogisticRegression, regression_targets=None, **kwargs):
    """
    Creates model explainer for a given model.

    Parameters
    --------
    model_type: sklearn class
        Model class in sklearn-api style (should have methods `fit`, `predict_proba`).
    regression_targets: dict, optional
        Mapping from ``index_col`` to regression target e.g. LTV of user. Default: ``None``
    kwargs:
        Parameters for model class that you use, also contains parameters for ``retention.extract_features()``.

    Returns
    --------
    Creates ModelDescriptor
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
        target = features.index.isin(self.get_positive_users(**kwargs))
    feature_range = kwargs.pop('ngram_range')
    mod = ModelDescriptor(model_type, features, target, feature_range=feature_range, **kwargs)
    return mod


@staticmethod
def make_regression_targets(features, regression_targets):
    """
    Creates target vector for given features.

    Parameters
    --------
    features: pd.DataFrame
        Feature matrix.
    regression_targets: dict
        Mapping from ``index_col`` to regression target, e.g. LTV of user.

    Returns
    --------
    List of targets alligned to feature matrix indices

    Return type
    -------

    """
    return [regression_targets.get(i) for i in features.index]


def _process_thr(self, data, thr, max_steps=30, mod=lambda x: x, **kwargs):
    f = data.index.str.startswith('Accumulated')
    if kwargs.get('targets', True):
        f |= data.index.isin(self.retention_config['target_event_list'])
    print("""
    Unused events on first {} steps:
        {}
    """.format(max_steps, '\n\t'.join(data[f].index.tolist())))
    return data.loc[(mod(data) >= thr).any(1) | f]


def _process_target_config(self, data, cfg, target):
    target = 'positive_target_event' if target.startswith('pos_') else 'negative_target_event'
    target = self.retention_config.get(target)
    for key, val in cfg.items():
        func = getattr(self, f'_process_{key}')
        data = func(data, val, target)
    return data


def _process_time_limit(self, data, threshold, name):
    self._init_cols(locals())
    if 'next_timestamp' in data:
        col = 'next_timestamp'
        change_next = True
        data[self._event_time_col()] \
            = pd.to_datetime(data[self._event_time_col()])
    else:
        col = self._event_time_col()
        change_next = False
    data[col] = pd.to_datetime(data[col])
    max_time = data[col].max()
    tmp = data.groupby(self._index_col()).tail(1)
    tmp = tmp[(max_time - tmp[col]).dt.total_seconds() > threshold]

    if change_next:
        tmp[self._event_col()] = tmp.next_event.values
        tmp.next_event = name
        tmp[self._event_time_col()] += timedelta(seconds=1)
        tmp['next_timestamp'] += timedelta(seconds=1)
    else:
        tmp[self._event_col()] = name
        tmp[self._event_time_col()] += timedelta(seconds=1)
    data.reset_index(drop=True, inplace=True)

    return data.append(tmp, ignore_index=True).reset_index(drop=True)


def _process_event_list(self, data, event_list, name):
    self._init_cols(locals())
    if 'next_event' in data:
        col = 'next_event'
    else:
        col = self._event_col()
    data[col] = np.where(data[col].isin(event_list), name, data[col])
    return data


def _process_empty(self, data, other, name):
    self._init_cols(locals())
    if 'next_event' in data:
        col = 'next_event'
        change_next = True
        data['next_timestamp'] \
            = pd.to_datetime(data[self._event_time_col()])
    else:
        col = self._event_col()
        change_next = False
    data[self._event_time_col()] \
        = pd.to_datetime(data[self._event_time_col()])
    bads = set(data[data[col] == other][self._index_col()])
    goods = set(data[self._index_col()]) - bads
    tmp = data[data[self._index_col()].isin(goods)]
    tmp = tmp.groupby(self._index_col()).tail(1)
    if change_next:
        tmp[self._event_col()] = tmp.next_event.values
        tmp.next_event = name
        tmp[self._event_time_col()] += timedelta(seconds=1)
        tmp['next_timestamp'] += timedelta(seconds=1)
    else:
        tmp[self._event_col()] = name
        tmp[self._event_time_col()] += timedelta(seconds=1)
    data.reset_index(drop=True, inplace=True)
    return data.append(tmp, ignore_index=True).reset_index(drop=True)


def _add_first_event(self, first_event):
    self._init_cols(locals())
    top1 = self._obj.groupby(self._index_col()).head(1)
    if 'next_event' in top1:
        top1.next_event = top1[self._event_col()].values
    top1[self._event_col()] = first_event
    top1[self._event_time_col()] -= timedelta(seconds=1)
    return top1.append(self._obj, ignore_index=True).reset_index(drop=True)


def _convert_timestamp(self, time_col=None):
    self._init_cols(locals())
    timestamp = self._obj[self._event_time_col()].iloc[0]
    if hasattr(timestamp, 'second'):
        return
    if type(timestamp) != str:
        l = len(str(int(timestamp)))
        self._obj[self._event_time_col()] *= 10 ** (19 - l)
    self._obj[self._event_time_col()] = pd.to_datetime(self._obj[self._event_time_col()])


def calculate_delays(self, plotting=True, time_col=None, index_col=None, event_col=None, bins=15, **kwargs):
    """
    Displays the logarithm of delay between ``time_col`` with the next value in nanoseconds as a histogram.

    Parameters
    --------
    plotting: bool, optional
        If ``True``, then histogram is plotted as a graph. Default: ``True``
    time_col: str, optional
        Name of custom time column for more information refer to ``init_config``. For instance, if in config you have defined ``event_time_col`` as ``server_timestamp``, but want to use function over ``user_timestamp``. By default the column defined in ``init_config`` will be used as ``time_col``.
    index_col: str, optional
        Name of custom index column, for more information refer to ``init_config``. For instance, if in config you have defined ``index_col`` as ``user_id``, but want to use function over sessions. By default the column defined in ``init_config`` will be used as ``index_col``.
    event_col: str, optional
        Name of custom event column, for more information refer to ``init_config``. For instance, you may want to aggregate some events or rename and use it as new event column. By default the column defined in ``init_config`` will be used as ``event_col``.
    bins: int, optional
        Number of bins for visualisation. Default: ``50``

    Returns
    -------
    Delays in seconds for each ``time_col``. Index is preserved as in original dataset.

    Return type
    -------
    List
    """
    self._init_cols(locals())
    data = self._get_shift(index_col=self._index_col(),
                           event_col=self._event_col()).copy()

    delays = np.log((data['next_timestamp'] - data[self._event_time_col()]) // pd.Timedelta('1s'))

    if plotting:
        fig, ax = plot.sns.mpl.pyplot.subplots(
            figsize=kwargs.get('figsize', (15, 7)))  # control figsize for proper display on large bin numbers
        _, bins, _ = plt.hist(delays[~np.isnan(delays) & ~np.isinf(delays)], bins=bins, log=True)
        if not kwargs.get('logvals', False):  # test & compare with logarithmic and normal
            plt.xticks(bins, np.around(np.exp(bins), 1))
        plt.show()

    return np.exp(delays)


def insert_sleep_events(self, events, delays=None, time_col=None, index_col=None, event_col=None):
    """
    Populates given dataset with sleep events representing time difference between occuring events. Note that this method is not inplace.

    Parameters
    --------
    events: dict
        Event name and log nanosecond ranges in the following structure: ``'event_name' : ['from_logtime', 'to_logtime']``. Keys of the dictionary are custom event names, while values are lists of two floats indicating start and end of time difference in lorarithm nanoseconds.
    delays: list
        Timestamp differences of each event with the next one. If ``None``, then uses ``BaseDataset.rete.calculate_delays()``. Default: ``None``
    time_col: str, optional
        Name of custom time column for more information refer to ``init_config``. For instance, if in config you have defined ``event_time_col`` as ``server_timestamp``, but want to use function over ``user_timestamp``. By default the column defined in ``init_config`` will be used as ``time_col``.
    index_col: str, optional
        Name of custom index column, for more information refer to ``init_config``. For instance, if in config you have defined ``index_col`` as ``user_id``, but want to use function over sessions. By default the column defined in ``init_config`` will be used as ``index_col``.
    event_col:  str, optional
        Name of custom event column, for more information refer to ``init_config``. For instance, you may want to aggregate some events or rename and use it as new event column. By default the column defined in ``init_config`` will be used as ``event_col``.

    Returns
    --------
    Original dataframe with inserted sleep events.

    Return type
    -------
    pd.DataFrame
    """

    self._init_cols(locals())

    if delays is None:
        delays = self.calculate_delays(False, self._event_time_col(), self._index_col(), self._event_col())

    data = self._obj.copy()
    to_add = []

    for event_name, (t_min, t_max) in events.items():
        tmp = data.loc[(delays >= t_min) & (delays < t_max)]
        tmp[self._event_col()] = event_name
        tmp[self._event_time_col()] += pd.Timedelta((np.e ** t_min) / 2)
        to_add.append(tmp)
        data['next_event'] = np.where((delays >= t_min) & (delays < t_max), event_name, data['next_event'])
        data['next_timestamp'] = np.where((delays >= t_min) & (delays < t_max),
                                          data[self._event_time_col()] + pd.Timedelta((np.e ** t_min) / 2),
                                          data['next_timestamp'])
    to_add.append(data)
    to_add = pd.concat(to_add)
    return to_add.sort_values(self._event_col()).reset_index(drop=True)


def remove_events(self, event_list, mode='equal'):
    """
    Removes events from dataset.

    Parameters
    --------
    event_list: list or str
        Events or other elements of ``event_col`` that should be filtered out.
    mode: str, optional
        Type of comparison:
            - `equal`: full event name match with element from ``event_list``;
            - `startswith`: event name starts with element from ``event_list``;
            - `contains`: event name contains element from ``event_list``.

    Returns
    --------
    Filtered dataframe based on event names.

    Return type
    -------
    pd.DataFrame
    """
    self._init_cols(locals())
    data = self._obj.copy()
    func = getattr(preprocessing, '_event_filter_' + mode)

    for event in event_list:
        data = data.loc[func(data[self._event_col()], event)]
    return data.reset_index(drop=True)


def select_bbox_from_tsne(self, bbox, plotting=True, **kwargs):
    """
    Selects data filtered by cordinates of TSNE plot.

    Parameters
    ---------
    bbox: list
        List of lists that contains angles of bbox.
            ```bbox = [
                [0, 0], # [min x, max x]
                [10, 10] # [min y, max y]
            ]```
    plotting: bool, optional
        If ``True``, then visualize graph of selected users.

    Returns
    --------
    Dataframe with filtered clickstream of users in bbox.

    Return type
    -------
    pd.DataFrame
    """
    self._init_cols(locals())
    if not hasattr(self, '_tsne'):
        raise ValueError('Please, use `learn_tsne` before selection of specific bbox')

    f = self._tsne.index.values[(self._tsne.iloc[:, 0] >= bbox[0][0])
                                & (self._tsne.iloc[:, 0] <= bbox[0][1])
                                & (self._tsne.iloc[:, 1] >= bbox[1][0])
                                & (self._tsne.iloc[:, 1] <= bbox[1][1])]

    filtered = self._obj[self._obj[self._index_col()].isin(f)]
    if plotting:
        filtered.rete.plot_graph(**kwargs)
    return filtered.reset_index(drop=True)


def show_tree_selector(self, **kwargs):
    """
    Shows tree selector for event filtering, based on values in ``event_col`` column. It uses `_` for event splitting and aggregation, so ideally the event name structure in the dataset should include underscores, e.g. ``[section]_[page]_[action]``. In this case event names are separated into levels, so that all the events with the same ``[section]`` will be placed under the same section, etc.
    There two kind of checkboxes in IFrame: large blue and small white. The former are used to include or exclude event from original dataset. The latter are used for event aggregation: toggle on a checkbox to aggregate all the underlying events to this level.
    Tree filter has a download button in the end of event list, which downloads a JSON config file, which you then need to use to filter and aggregate events with ``BaseDataset.rete.use_tree_filter()`` method.

    Parameters
    --------
    event_col: str, optional
        Name of custom event column, for more information refer to ``init_config``. For instance, you may want to aggregate some events or rename and use it as new event column. By default the column defined in ``init_config`` will be used as ``event_col``.
    width: int, optional
        Width of IFrame object with filters.
    height: int, optional
        Height of IFrame object with filters.

    Returns
    --------
    Renders events tree selector

    Return type
    --------
    IFrame
    """
    self._init_cols(locals())
    from retentioneering.core.tree_selector import show_tree_filter
    show_tree_filter(self._obj[self._event_col()], **kwargs)


def use_tree_filter(self, path, **kwargs):
    """
    Uses generated with ``show_tree_filter()`` JSON config to filter and aggregate ``event_col`` values of dataset.

    Parameters
    --------
    path: str
        Path to JSON config file generated with ``show_tree_filter()`` method.

    Returns
    --------
    Filtered and aggregated dataset

    Return type
    --------
    pd.DataFrame
    """
    from retentioneering.core.tree_selector import use_tree_filter
    res = use_tree_filter(self._obj, path, **kwargs)
    return res


def _create_bins(self, data, time_step, index_col=None):
    self._init_cols(locals())
    tmp = data.join(
        data.groupby(self._index_col())
        [self._event_time_col()].min(),
        on=self._index_col(), rsuffix='_min')

    data['bins'] = (
            data[self._event_time_col()] - tmp[self._event_time_col() + '_min']
    )
    data['bins'] = np.floor(data['bins'] / np.timedelta64(1, time_step))


def survival_curves(self, groups, spec_event=None, time_min=None, time_max=None, event_col=None, index_col=None,
                    target_event=None, time_step='D', plotting=True, **kwargs):
    """
    Plot survival curves for given grouping.

    Parameters
    --------
    groups: np.array
        Array of clickstream shape that splits data into different groups.
    spec_event: str, optional
        Event specific for test, e.g. we change auth flow, so we need to compare only users, who have started authorization, in this case `spec_event='auth_start'`. Default: ``None``
    time_min: int, optional
        Time when A/B test was started. If ``None``, then whole dataset is used. Defaul: ``None``
    time_max: int, optional
        Time when A/B test was ended. If ``None``, then whole dataset is used. Default: ``None``
    index_col: str, optional
        Name of custom index column, for more information refer to ``init_config``. For instance, if in config you have defined ``index_col`` as ``user_id``, but want to use function over sessions. By default the column defined in ``init_config`` will be used as ``index_col``.
    event_col: str, optional
        Name of custom event column, for more information refer to ``init_config``. For instance, you may want to aggregate some events or rename and use it as new event column. By default the column defined in ``init_config`` will be used as ``event_col``.
    target_event: str, optional
        Name of target event. If ``None``, then taken from ``retention_config``. Default: ``None``
    time_step: str, optional
        Time step for calculation of survival rate at specific time.
            Possible options:
                (`'D'` -- day, `'M'` -- month, `'h'` -- hour, `'m'` -- minute, `'Y'` -- year,
                 `'W'` -- week, `'s'` -- seconds, `'ms'` -- milliseconds).
        Default is day (`'D'`).
    plotting: bool, optional
        If ``True``, then plots survival curves.

    Returns
    --------
    Dataframe with points at survival curves and prints chi-squared LogRank test for equality statistics.

    Return type
    -------
    pd.DataFrame
    """
    self._init_cols(locals())
    data = self._obj.copy()
    if spec_event is not None:
        users = (data[data[self._event_col()] == spec_event][self._index_col()]).unique()
        f = data[self._index_col()].isin(users)
        data = data[f].copy()
        groups = groups[f].copy()
    if type(data[self._event_time_col()].iloc[0]) not in (int, float, object, str):
        data[self._event_time_col()] = pd.to_datetime(
            data[self._event_time_col()])
    if time_min is not None:
        f = data[self._event_time_col()] >= pd.to_datetime(time_min)
        data = data[f].copy()
        groups = groups[f].copy()
    if time_max is not None:
        f = data[self._event_time_col()] <= pd.to_datetime(time_max)
        data = data[f].copy()
        groups = groups[f].copy()
    self._create_bins(data, time_step, index_col)

    data['metric_col'] = (data
                          [self._event_col()]
                          == (target_event or self.retention_config['positive_target_event']))
    tmp = data[data.metric_col == 1]
    curves = tmp.groupby(
        [groups, 'bins']
    )[self._index_col()].nunique().rename('metric').reset_index()
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
    Splits dataset between train and test based on ``index_col``.

    Parameters
    -------
    index_col: str, optional
        Name of custom index column, for more information refer to ``init_config``. For instance, if in config you have defined ``index_col`` as ``user_id``, but want to use function over sessions. By default the column defined in ``init_config`` will be used as ``index_col``.
    test_size: float, optional
        Rate of test subsample from 0 to 1. Default: ``0.2``
    seed: int, optional
        Random seed number. Default: ``0``

    Returns
    -------
    Two dataframes: train and test.

    Return type
    -------
    pd.DataFrame
    """
    self._init_cols(locals())
    np.random.seed(seed)
    ids = np.random.permutation(self._obj[self._index_col()].unique())
    f = self._obj[self._index_col()].isin(ids[int(ids.shape[0] * test_size):])
    return self._obj[f].copy(), self._obj[~f].copy()


def step_matrix_bootstrap(self, n_samples=10, sample_size=None, sample_rate=1, random_state=0, **kwargs):
    """
    Estimates means and standard deviations of step matrix values with bootstrap.

    Parameters
    --------
    n_samples: int, optional
        Number of samples for bootstrap. Default: ``10``
    sample_size: int, optional
        Size of each subsample. Default: ``None``
    sample_rate: float, optional
        Rate of each subsample. Note that it cannot be used with ``sample_size``. Default: ``1``
    random_state: int, optional
        Random state for sampling. Default: ``0``
    kwargs: optional
        Arguments of ``BaseDataset.rete.get_step_matrix()``

    Returns
    --------
    Two dataframes: with mean and standard deviation values.

    Return type
    --------
    pd.DataFrame
    """
    self._init_cols(locals())
    res = []
    base = pd.DataFrame(0,
                        index=self._obj[self._event_col()].unique(),
                        columns=range(1, kwargs.get('max_steps') or 31)
                        )
    thr = kwargs.pop('thr', None)
    plot_type = kwargs.pop('plot_type', None)
    for i in range(n_samples):
        tmp = self._obj.sample(n=sample_size, frac=sample_rate, replace=True, random_state=random_state + i)
        tmp = (tmp.rete.get_step_matrix(plot_type=False, **kwargs) + base).fillna(0)
        tmp = tmp.loc[base.index.tolist()]
        res.append(tmp.values[:, :, np.newaxis])
    kwargs.update({'thr': thr})
    res = np.concatenate(res, axis=2)
    piv = pd.DataFrame(res.mean(2), index=base.index, columns=base.columns)
    stds = pd.DataFrame(res.std(2), index=base.index, columns=base.columns)

    if not kwargs.get('reverse'):
        for i in self.retention_config['target_event_list']:
            piv = piv.append(self._add_accums(piv, i))
    if kwargs.get('thr'):
        thr = kwargs.pop('thr')
        piv = self._process_thr(piv, thr, kwargs.get('max_steps' or 30), **kwargs)
    if kwargs.get('sorting'):
        piv = self._sort_matrix(piv)
    if not kwargs.get('for_diff'):
        if kwargs.get('reverse'):
            piv.columns = ['n'] + ['n - {}'.format(i - 1) for i in piv.columns[1:]]
    if plot_type:
        plot.step_matrix(
            piv.round(2),
            title=kwargs.get('title',
                             'Step matrix {}'
                             .format('reversed' if kwargs.get('reverse') else '')), **kwargs)
        plot.step_matrix(
            stds.round(3),
            title=kwargs.get('title',
                             'Step matrix std'), **kwargs)
    if kwargs.get('dt_means') is not None:
        means = np.array(self._obj.groupby('event_rank').apply(
            lambda x: (x.next_timestamp - x.event_timestamp).dt.total_seconds().mean()
        ))
        piv = pd.concat([piv, pd.DataFrame([means[:kwargs.get('max_steps' or 30)]],
                                           columns=piv.columns, index=['dt_mean'])])
    return piv, stds


def core_event_distribution(self, core_events, index_col=None, event_col=None,
                            thresh=None, plotting=True, use_greater=True, **kwargs):
    self._init_cols(locals())
    if type(core_events) == str:
        core_events = [core_events]
    self._obj['is_core_event'] = self._obj[self._event_col()].isin(core_events)
    rates = self._obj.groupby(self._index_col()).is_core_event.mean()
    if plotting:
        plot.core_event_dist(rates, thresh, **kwargs)
    if use_greater:
        f = set(rates[rates >= thresh].index.values)
    else:
        f = set(rates[rates < thresh].index.values)
    return self._obj[self._obj[self._index_col()].isin(f)].reset_index(drop=True)


def pairwise_time_distribution(self, event_order, time_col=None, index_col=None,
                               event_col=None, bins=100, limit=180, topk=3):
    self._init_cols(locals())
    if 'next_event' not in self._obj.columns:
        data = self._get_shift(index_col=index_col,
                               event_col=event_col).copy()

    data['time_diff'] = (data['next_timestamp'] - data[
        time_col or self.retention_config['event_time_col']]).dt.total_seconds()
    f_cur = data[self._event_col()] == event_order[0]
    f_next = data['next_event'] == event_order[1]
    s_next = data[f_cur & f_next].copy()
    s_cur = data[f_cur & (~f_next)].copy()

    s_cur.time_diff[s_cur.time_diff < limit].hist(alpha=0.5, log=True,
                                                  bins=bins, label='Others {:.2f}'.format(
            (s_cur.time_diff < limit).sum() / f_cur.sum()
        ))
    s_next.time_diff[s_next.time_diff < limit].hist(alpha=0.7, log=True,
                                                    bins=bins,
                                                    label='Selected event order {:.2f}'.format(
                                                        (s_next.time_diff < limit).sum() / f_cur.sum()
                                                    ))
    plot.sns.mpl.pyplot.legend()
    plot.sns.mpl.pyplot.show()
    (s_cur.next_event.value_counts() / f_cur.sum()).iloc[:topk].plot.bar()


@staticmethod
def _find_traj(x, event_list, event_col):
    res = np.ones_like(x[event_col]).astype(bool)
    for elem in event_list:
        res &= ((x[event_col] == elem).cumsum() > 0).values
    return res.max()


def create_trajectory_filter(self, event_list, index_col=None, event_col=None, **kwargs):
    self._init_cols(locals())
    df_stat = (self
               ._obj
               .groupby(self._index_col())
               .apply(self._find_traj,
                      event_list=event_list,
                      event_col=self._event_col()))
    return df_stat


def apply_trajectory_filter(self, event_list, index_col=None, event_col=None, **kwargs):
    self._init_cols(locals())
    f = self.create_trajectory_filter(event_list, index_col, event_col, **kwargs)
    f = self._obj[self._index_col()].isin(f[f].index.tolist())
    return self._obj[f].copy().reset_index(drop=True)


def _is_cycle(self, data):
    """
        Utilite for cycle search
    """
    temp = data.split('~~')
    return True if temp[0] == temp[-1] and len(set(temp)) > 1 else False


def _is_loop(self, data):
    """
        Utilite for loop search
    """
    temp = data.split('~~')
    return True if len(set(temp)) == 1 else False


def get_equal_fraction(self, fraction=1, random_state=42):
    """
        Selects fraction of good users and the same number of bad users

        Parameters
        --------
        fraction: float, optional
            Fraction of users. Should be in interval of (0,1]
        random_state: int, optional
            random state for numpy choice function

        Returns
        --------
        Two dataframes: with good and bad users

        Return type
        --------
        tuple of pd.DataFrame
    """
    if fraction <= 0 or fraction > 1:
        raise ValueError('The fraction is <= 0 or > 1')
    self._init_cols(locals())

    np.random.seed(random_state)
    good_users = self.get_positive_users()
    bad_users = self.get_negative_users()

    sample_size = min(int(len(good_users) * fraction), len(bad_users))
    good_users_sample = set(np.random.choice(good_users, sample_size, replace=False))
    bad_users_sample = set(np.random.choice(bad_users, sample_size, replace=False))

    return (self._obj[self._obj[self._index_col()].isin(good_users_sample)],
            self._obj[self._obj[self._index_col()].isin(bad_users_sample)])


def _remove_duplicates(self, data):
    """
    Removing same events, that are going one after another
    ('ev1 -> ev1 -> ev2 -> ev1 -> ev3 -> ev3   --------> ev1 -> ev2 -> ev1 -> ev3').
    This utilite is used in a find_sequences function

    """
    t = data.split('~~')
    t = '~~'.join([t[0]] + ['~~'.join(word for ind, word in enumerate(t[1:]) if t[ind] != t[ind + 1])])
    return t[:-2] if t[-1] == '~' else t


def find_sequences(self, ngram_range=(1, 1), fraction=1, random_state=42, exclude_cycles=False, exclude_loops=False,
                   exclude_repetitions=False, threshold=0, coefficient=0):
    """
        Finds all subsequences of length lying in interval

        Parameters
        --------
        fraction: float, optional
            Fraction of users. Should be in interval of (0,1]
        random_state: int, optional
            random state for numpy choice function

        Returns
        --------
        Two dataframes: with good and bad users

        Return type
        --------
        tuple of pd.DataFrame
    """
    self._init_cols(locals())
    sequences = dict()
    good, bad = self.get_equal_fraction(fraction, random_state)
    countvect = CountVectorizer(ngram_range=ngram_range, token_pattern='[^~]+')
    good_corpus = good.groupby(self._index_col())[self._event_col()].apply(
        lambda x: '~~'.join([l.lower() for l in x if l != 'pass' and l != 'lost']))
    good_count = countvect.fit_transform(good_corpus.values)
    good_frame = pd.DataFrame(columns=['~~'.join(x.split(' ')) for x in countvect.get_feature_names()],
                              data=good_count.todense())
    bad_corpus = bad.groupby(self._index_col())[self._event_col()].apply(
        lambda x: '~~'.join([l.lower() for l in x if l != 'pass' and l != 'lost']))
    bad_count = countvect.fit_transform(bad_corpus.values)
    bad_frame = pd.DataFrame(columns=['~~'.join(x.split(' ')) for x in countvect.get_feature_names()],
                             data=bad_count.todense())

    res = pd.concat([good_frame.sum(), bad_frame.sum()], axis=1).fillna(0).reset_index()
    res.columns = ['Sequence', 'Good', 'Lost']

    if exclude_cycles:
        res = res[~res.Sequence.apply(lambda x: self._is_cycle(x))]
    if exclude_loops:
        temp = res[~res.Sequence.apply(lambda x: self._is_loop(x))]
    if exclude_repetitions:
        res.Sequence = res.Sequence.apply(lambda x: self._remove_duplicates(x))
        res = res.groupby(res.Sequence)[['Good', 'Lost']].sum().reset_index()
        res = res[res.Sequence.apply(lambda x: len(x.split('~~')) in range(ngram_range[0], ngram_range[1] + 1))]

    res['Lost2Good'] = res['Lost'] / res['Good']
    return res[(abs(res['Lost2Good'] - 1) > coefficient) & (res.Good + res.Lost > threshold)] \
        .sort_values('Lost', ascending=False).reset_index(drop=True)


def find_cycles(self, interval, fraction=1, random_state=42, exclude_loops=False, exclude_repetitions=False):
    """

    Parameters
    ----------
    interval - interval of lengths for search. Any int number
    fraction - fraction of good users. Any float in (0,1]
    random_state - random_state for numpy random seed

    Returns pd.DataFrame with cycles
    -------

    """
    self._init_cols(locals())
    temp = self.find_sequences(interval, fraction, random_state, exclude_loops=exclude_loops,
                               exclude_repetitions=exclude_repetitions).reset_index(drop=True)
    return temp[temp['Sequence'].apply(lambda x: self._is_cycle(x))].reset_index(drop=True)


def find_loops(self, fraction=1, random_state=42):
    """
    Function for loop searching
    Parameters
    ----------
    fraction - fraction of good users. Any float in (0,1]
    random_state - random_state for numpy random seed

    Returns pd.DataFrame with loops. Good, Lost columns are for all occurences,
    (Good/Lost)_no_duplicates are for counting each cycle only once for user in which they occur
    -------

    """

    def loop_search(data, self_loops, event_list, is_bad):
        self._init_cols(locals())
        event_list = {k: 0 for k in event_list}
        for ind, url in enumerate(data[1:]):
            if data[ind] == data[ind + 1]:
                if url in self_loops.keys():
                    self_loops[url][is_bad] += 1
                    if event_list[url] == 0:
                        self_loops[url][is_bad + 3] += 1
                        event_list[url] = 1
                else:
                    self_loops[url] = [0, 0, 0, 0, 0, 0]
                    self_loops[url][is_bad] = 1
                    if event_list[url] == 0:
                        self_loops[url][is_bad + 3] += 1
                        event_list[url] = 1

    self._init_cols(locals())
    self_loops = dict()
    event_list = self._obj[self._event_col()].unique()
    good, bad = self.get_equal_fraction(fraction, random_state)
    for el in good.groupby(self._index_col()):
        loop_search(el[1][self._event_col()].values, self_loops, event_list, 0)

    for el in bad.groupby(self._index_col()):
        loop_search(el[1][self._event_col()].values, self_loops, event_list, 1)

    for key, val in self_loops.items():
        if val[0] != 0:
            self_loops[key][2] = val[1] / val[0]
        if val[3] != 0:
            self_loops[key][5] = val[4] / val[3]

    return pd.DataFrame(data=[[a[0]] + a[1] for a in self_loops.items()],
                        columns=['Sequence', 'Good', 'Lost', 'Lost2Good', 'GoodUnique',
                                 'LostUnique', 'UniqueLost2Good']) \
        .sort_values('Lost', ascending=False).reset_index(drop=True)
