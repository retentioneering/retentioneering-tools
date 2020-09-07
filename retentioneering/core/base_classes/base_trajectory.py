import pandas as pd
import numpy as np
import networkx as nx
from retentioneering.visualization import plot


class BaseTrajectory(object):
    """
    Trajectory is the basic object in Retentioneering. It is a dataframe consisting of at least three columns which
    should be reflected in ``init_cofig``: index, event and timestamp.
    """
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self._accessor_type = 'trajectory'
        self.retention_config = {}
        self._locals = None

    def get_shift(self, *, index_col=None, event_col=None):
        """

        Parameters
        ----------
        index_col
        event_col

        Returns
        -------

        """
        index_col = index_col or self.retention_config['index_col']
        event_col = event_col or self.retention_config['event_col']
        time_col = self.retention_config['event_time_col']

        data = self._obj.copy()
        data.sort_values([index_col, time_col], inplace=True)
        shift = data.groupby(index_col).shift(-1)

        data['next_'+event_col] = shift[event_col]
        data['next_'+str(time_col)] = shift[time_col]

        return data

    def get_edgelist(self, *, weight_col=None, norm_type=None, edge_attributes=None):
        """
        Creates weighted table of the transitions between events.

        Parameters
        -------
        edge_attributes
        weight_col: str, optional, default=None
            Aggregation column for transitions weighting. To calculate weights as number of transion events leave as
            ```None``. To calculate number of unique users passed through given transition
            ``edge_attributes='user_id'``. For any other aggreagtion, life number of sessions, pass the column name.

        norm_type: {None, 'full', 'node'} str, optional, default=None
            Type of normalization. If ``None`` return raw number of transtions or other selected aggregation column.
            If ``norm_type='full'`` normalization

        Returns
        -------
        Dataframe with number of rows equal to all transitions with weight non-zero weight (max is squared number of
         unique ``event_col`` values) and the following column structure: ``source_node``, ``target_node`` and
         ``edge_weight``.

        Return type
        -------
        pd.DataFrame
        """
        if norm_type not in [None, 'full', 'node']:
            raise ValueError(f'unknown normalization type: {norm_type}')

        event_col = self.retention_config['event_col']
        time_col = self.retention_config['event_time_col']
        edge_attributes = edge_attributes or 'edge_weight'

        cols = [event_col, 'next_'+str(event_col)]

        data = self.get_shift().copy()

        # get aggregation:
        if weight_col is None:
            agg = (data
                   .groupby(cols)[time_col]
                   .count()
                   .reset_index())
            agg.rename(columns={time_col: edge_attributes}, inplace=True)
        else:
            agg = (data
                   .groupby(cols)[weight_col]
                   .nunique()
                   .reset_index())
            agg.rename(columns={weight_col: edge_attributes}, inplace=True)

        # apply normalization:
        if norm_type == 'full':
            if weight_col is None:
                agg[edge_attributes] /= agg[edge_attributes].sum()
            else:
                agg[edge_attributes] /= data[weight_col].nunique()

        if norm_type == 'node':
            if weight_col is None:
                event_transitions_counter = data.groupby(event_col)[cols[1]].count().to_dict()
                agg[edge_attributes] /= agg[cols[0]].map(event_transitions_counter)
            else:
                user_counter = data.groupby(cols[0])[weight_col].nunique().to_dict()
                agg[edge_attributes] /= agg[cols[0]].map(user_counter)

        return agg

    def get_adjacency(self, *, weight_col=None, norm_type=None):
        """
        Creates edge graph in the matrix format. Basically this method is similar to
        ``BaseTrajectory.rete.get_edgelist()`` but in different format. Row indeces are ``event_col`` values, from
         which the transition occured, while the row names are ``event_col`` values, to which the transition occured.
         The values are weights of the edges defined with ``weight_col``, ``edge_attributes`` and ``norm`` parameters.

        Parameters
        -------
        weight_col: str, optional, default=None
            Aggregation column for transitions weighting. To calculate weights as number of transion events leave as
            ```None``. To calculate number of unique users passed through given transition
            ``edge_attributes='user_id'``. For any other aggreagtion, life number of sessions, pass the column name.

        norm_type: {None, 'full', 'node'} str, optional, default=None
            Type of normalization. If ``None`` return raw number of transtions or other selected aggregation column.
            If ``norm_type='full'`` normalization

        Returns
        -------
        Dataframe with number of columns and rows equal to unique number of ``event_col`` values.

        Return type
        -------
        pd.DataFrame
        """
        agg = self.get_edgelist(weight_col=weight_col,
                                norm_type=norm_type)
        graph = nx.DiGraph()
        graph.add_weighted_edges_from(agg.values)
        return nx.to_pandas_adjacency(graph)

    @staticmethod
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

    def get_step_matrix(self, *, max_steps=30, weight_col=None, index_col=None, reverse=None,
                        plot_type=True, sorting=True,  thr=0, **kwargs):
        """
        *** DEPRICATED ***
        *** WILL BE REMOVED IN NEXT UPDATES ***
        *** USE rete.step_matrix instead ***

        Plots heatmap with distribution of users over session steps ordered by event name. Matrix rows are event names,
        columns are aligned user trajectory step numbers and the values are shares of users. A given entry means that at
         a particular number step x% of users encountered a specific event.

        Parameters
        --------
        index_col
        max_steps: int, optional
            Maximum number of steps in trajectory to include. Depending on ``reverse`` parameter value, the steps are
            counted from the beginning of trajectories if ``reverse=False``, or from the end otherwise.
        plot_type: bool, optional
            If ``True``, then plots step matrix in interactive session (Jupyter notebook). Default: ``True``
        thr: float, optional
            Used to prune matrix and display only the rows with at least one value >= ``thr``. Default: ``None``
        reverse: str or list, optional
            This parameter is used to display reversed matrix from target events towards the beginning of trajectories.
            Range of possible values:
                - ``None``: displays default step matrix from the start of trajectories. Uses all the user trajectories.
                - ``'pos'``: displays reverse step matrix in such a way that the first column is the
                ``positive_target_event`` share, which is always 1, and the following columns reflect the share of users
                 on final steps before reaching the target. Uses only those trajectories, which ended up having at least
                  one ``positive_target_event`` in trajectory.
                - ``'neg'``: same as ``pos`` but for ``negative_target_event``. Uses only those trajectories, which
                ended up having at least one ``negative_target_event`` in trajectory.
                - ``['pos', 'neg']``: combination of ``pos`` and ``neg`` options, first column has only target events.
                Uses all the trajectories with target events inside.
            Default: ``None``
        sorting: bool, optional
            If ``True``, then automatically places elements with highest values in top. Rows are sorted in such a way
            that the first one has highest first column value, second row has the highest second column value,besides
            already used first value, etc. With this sorting you may see a dominant trajectory as a diagonal.
            Default: ``True``
        weight_col: str, optional
            Aggregation column for edge weighting. For instance, you may set it to the same value as in ``index_col``
            and define ``edge_attributes='users_unique'`` to calculate unique users passed through edge.
            Default: ``None``

        Returns
        -------
        Dataframe with ``max_steps`` number of columns and len(event_col.unique) number of rows at max, or less if used
        ``thr`` > 0.

        Return type
        -------
        pd.DataFrame
        """

        def _add_accums(agg, name):
            if name not in agg.index:
                return pd.Series([0] * agg.shape[1], index=agg.columns, name='Accumulated ' + name)
            return agg.loc[name].cumsum().shift(1).fillna(0).rename('Accumulated ' + name)


        event_col = self.retention_config['event_col']
        target_event_list = self.retention_config['target_event_list']
        weight_col = weight_col or index_col or self.retention_config['index_col']

        data = self._obj.copy()

        data['event_rank'] = 1
        data['event_rank'] = data.groupby(weight_col)['event_rank'].cumsum()

        if reverse:
            d = {
                'pos': self.retention_config['positive_target_event'],
                'neg': self.retention_config['negative_target_event']
            }
            if type(reverse) == list:
                targets = [d[i] for i in reverse]
            else:
                targets = [d[reverse]]

            data['convpoint'] = data[event_col].isin(targets).astype(int)
            data['convpoint'] = (
                    data
                    .groupby(weight_col)['convpoint']
                    .cumsum() - data['convpoint']
            )
            data['event_rank'] = (
                    data
                    .groupby([weight_col, 'convpoint'])['event_rank']
                    .apply(lambda x: x.max() - x + 1)
            )
            data['non-detriment'] = (
                    data
                    .groupby([weight_col, 'convpoint'])[event_col]
                    .apply(lambda x: pd.Series([x.iloc[-1] in targets] * x.shape[0], index=x.index))
            )
            if not data['non-detriment'].any():
                raise ValueError('There is not {} event in this group'.format(targets[0]))

            data = data[data['non-detriment'].fillna(False)]
            data.drop('non-detriment', axis=1, inplace=True)

        # calculate step matrix elements:
        agg = (data
               .groupby(['event_rank', event_col])[weight_col]
               .nunique()
               .reset_index())
        agg[weight_col] /= data[weight_col].nunique()

        if max_steps:
            agg = agg[agg['event_rank'] <= max_steps]
        agg.columns = ['event_rank', 'event_name', 'freq']

        piv = agg.pivot(index='event_name', columns='event_rank', values='freq').fillna(0)
        piv.columns.name = None
        piv.index.name = None

        if not reverse:
            for i in target_event_list:
                piv = piv.append(_add_accums(piv, i))

        if thr != 0:
            keep = piv.index.str.startswith('Accumulated')
            keep |= piv.index.isin(target_event_list)
            piv = piv.loc[(piv >= thr).any(1) | keep].copy()

        if sorting:
            piv = BaseTrajectory._sort_matrix(piv)

        if not kwargs.get('for_diff'):
            if reverse:
                piv.columns = ['n'] + ['n - {}'.format(int(i) - 1) for i in piv.columns[1:]]

        # TODO: need to remove forced normalization and implement one of the solutions:
        # 1) accumulated counts only users after trajectory terminates (columns will be always normalize)
        # 2) accumulated cumulatively counts all target events (no longer sums to 1)
        for indices in piv.columns.values:
            piv[indices] = piv[indices] / piv[indices].sum()

        if plot_type:
            plot.step_matrix(piv,None,
                                 title=kwargs.get('title',
                                              'Step matrix {}'
                                              .format('reversed' if kwargs.get('reverse') else '')),
                                 **kwargs)


        if kwargs.get('dt_means') is not None:
            means = np.array(data.groupby('event_rank').apply(
                lambda x: (x.next_timestamp - x.event_timestamp).dt.total_seconds().mean()
            ))
            piv = pd.concat([piv, pd.DataFrame([means[:max_steps]], columns=piv.columns, index=['dt_mean'])])

        return piv

    def split_sessions(self, *, by_event=None, thresh, eos_event=None, session_col='session_id'):
        """
        Generates ``session`_id` column with session rank for each ``index_col`` based on time difference between
        events. Sessions are automatically defined with time diffrence between events.
        Parameters
        --------
        session_col
        by_event
        thresh: int
            Minimal threshold in seconds between two sessions. Default: ``1800`` or 30 min
        eos_event:
            If not ``None`` specified event name will be added at the and of each session
        Returns
        -------
        Original Dataframe with ``session_id`` column in dataset.
        Return type
        -------
        pd.DataFrame
        """

        session_col_arg = session_col or 'session_id'

        index_col = self.retention_config['index_col']
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

    def plot_graph(self, *, node_params=None, weight_col=None, event_col=None,
                   node_weights=None, norm_type='full', **kwargs):
        """
        *** DEPRICATED ***
        *** WILL BE REMOVED IN NEXT UPDATES ***
        *** USE rete.graph instead ***

        Create interactive graph visualization. Each node is a unique ``event_col`` value, edges are transitions between
         events and edge weights are calculated metrics. By default, it is a percentage of unique users that have passed
          though a particular edge visualized with the edge thickness. Node sizes are  Graph loop is a transition to the
           same node, which may happen if users encountered multiple errors or made any action at least twice.
        Graph nodes are movable on canvas which helps to visualize user trajectories but is also a cumbersome process to
         place all the nodes so it forms a story.
        That is why IFrame object also has a download button. By pressing it, a JSON configuration file with all the
        node parameters is downloaded. It contains node names, their positions, relative sizes and types. It it used as
        ``layout_dump`` parameter for layout configuration. Finally, show weights toggle shows and hides edge weights.
        Parameters
        --------
        event_col
        norm_type
        node_weights
        weight_col
        node_params: dict, optional
            Event mapping describing which nodes or edges should be highlighted by different colors for better
            visualisation. Dictionary keys are ``event_col`` values, while keys have the following possible values:
                - ``bad_target``: highlights node and all incoming edges with red color;
                - ``nice_target``: highlights node and all incoming edges with green color;
                - ``bad_node``: highlights node with red color;
                - ``nice_node``: highlights node with green color;
                - ``source``: highlights node and all outgoing edges with yellow color.
            Example ``node_params`` is shown below:
            ```
            {
                'lost': 'bad_target',
                'purchased': 'nice_target',
                'onboarding_welcome_screen': 'source',
                'choose_login_type': 'nice_node',
                'accept_privacy_policy': 'bad_node',
            }
            ```
            If ``node_params=None``, it will be constructed from ``retention_config`` variable, so that:
            ```
            {
                'positive_target_event': 'nice_target',
                'negative_target_event': 'bad_target',
                'source_event': 'source',
            }
            ```
            Default: ``None``
        thresh: float, optional
            Minimal edge weight value to be rendered on a graph. If a node has no edges of the weight >= ``thresh``,
            then it is not shown on a graph. It is used to filter out rare event and not to clutter visualization. If
            you want to preserve some of the nodes regardless of ``thresh`` value, please use ``targets`` parameter.
            Default: ``0.05``
        targets: list, optional
            List of nodes which will ignore ``thresh`` parameter.
        show_percent: bool, optional
            If ``True``, then all edge weights are converted to percents by multiplying by 100 and adding percentage
            sign. Default: ``True``
        interactive: bool, optional
            If ``True``, then plots graph visualization in interactive session (Jupyter notebook). Default: ``True``
        layout_dump: str, optional
            Path to layout configuration file relative to current directory. If defined, uses configuration file as a
            graph layout. Default: ``None``
        width: int, optional
            Width of plot in pixels. Default: ``500``
        height: int, optional
            Height of plot in pixels. Default: ``500``
        kwargs: optional
            Other parameters for ``BaseTrajectory.rete.get_edgelist()``
        Returns
        --------
        Plots IFrame graph of ``width`` and ``height`` size.
        Saves webpage with JS graph visualization to ``retention_config.experiments_folder``.
        Return type
        -------
        Renders IFrame object in case of ``interactive=True`` and saves graph visualization as HTML in
        ``experiments_folder`` of ``retention_config``.
        """
        event_col = event_col or self.retention_config['event_col']

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
        node_weights = node_weights or self._obj[event_col].value_counts().to_dict()
        path = plot.graph(self._obj.trajectory.get_edgelist(weight_col=weight_col,
                                                            norm_type=norm_type),
                          node_params, node_weights=node_weights, **kwargs)
        return path

    def graph(self, *, targets=None, weight_col=None, event_col=None,
                   node_weights=None, norm_type='full', thresh=0.01,  **kwargs):
        """
        Create interactive graph visualization. Each node is a unique ``event_col`` value, edges are transitions between
         events and edge weights are calculated metrics. By default, it is a percentage of unique users that have passed
          though a particular edge visualized with the edge thickness. Node sizes are  Graph loop is a transition to the
           same node, which may happen if users encountered multiple errors or made any action at least twice.
        Graph nodes are movable on canvas which helps to visualize user trajectories but is also a cumbersome process to
         place all the nodes so it forms a story.
        That is why IFrame object also has a download button. By pressing it, a JSON configuration file with all the
        node parameters is downloaded. It contains node names, their positions, relative sizes and types. It it used as
        ``layout_dump`` parameter for layout configuration. Finally, show weights toggle shows and hides edge weights.

        Parameters
        --------
        event_col
        norm_type
        node_weights
        weight_col
        node_params: dict, optional
            Event mapping describing which nodes or edges should be highlighted by different colors for better
            visualisation. Dictionary keys are ``event_col`` values, while keys have the following possible values:
                - ``bad_target``: highlights node and all incoming edges with red color;
                - ``nice_target``: highlights node and all incoming edges with green color;
                - ``bad_node``: highlights node with red color;
                - ``nice_node``: highlights node with green color;
                - ``source``: highlights node and all outgoing edges with yellow color.
            Example ``node_params`` is shown below:
            ```
            {
                'lost': 'bad_target',
                'purchased': 'nice_target',
                'onboarding_welcome_screen': 'source',
                'choose_login_type': 'nice_node',
                'accept_privacy_policy': 'bad_node',
            }
            ```
            If ``node_params=None``, it will be constructed from ``retention_config`` variable, so that:
            ```
            {
                'positive_target_event': 'nice_target',
                'negative_target_event': 'bad_target',
                'source_event': 'source',
            }
            ```
            Default: ``None``
        thresh: float, optional
            Minimal edge weight value to be rendered on a graph. If a node has no edges of the weight >= ``thresh``,
            then it is not shown on a graph. It is used to filter out rare event and not to clutter visualization. If
            you want to preserve some of the nodes regardless of ``thresh`` value, please use ``targets`` parameter.
            Default: ``0.05``
        targets: list, optional
            List of nodes which will ignore ``thresh`` parameter.
        show_percent: bool, optional
            If ``True``, then all edge weights are converted to percents by multiplying by 100 and adding percentage
            sign. Default: ``True``
        interactive: bool, optional
            If ``True``, then plots graph visualization in interactive session (Jupyter notebook). Default: ``True``
        layout_dump: str, optional
            Path to layout configuration file relative to current directory. If defined, uses configuration file as a
            graph layout. Default: ``None``
        width: int, optional
            Width of plot in pixels. Default: ``500``
        height: int, optional
            Height of plot in pixels. Default: ``500``
        kwargs: optional
            Other parameters for ``BaseTrajectory.rete.get_edgelist()``

        Returns
        --------
        Plots IFrame graph of ``width`` and ``height`` size.
        Saves webpage with JS graph visualization to ``retention_config.experiments_folder``.

        Return type
        -------
        Renders IFrame object in case of ``interactive=True`` and saves graph visualization as HTML in
        ``experiments_folder`` of ``retention_config``.
        """
        event_col = event_col or self.retention_config['event_col']

        node_weights = node_weights or self._obj[event_col].value_counts().to_dict()
        path = plot.graph(self._obj.trajectory.get_edgelist(weight_col=weight_col,
                                                            norm_type=norm_type),
                          targets, node_weights=node_weights, thresh=thresh, **kwargs)
        return path

    def step_matrix(self, *, max_steps=30, weight_col=None,
                    targets=None, accumulated=None,
                    sorting=True, thresh=0,
                    centered=None):
        """
        Plots heatmap with distribution of users over session steps ordered by event name. Matrix rows are event names,
        columns are aligned user trajectory step numbers and the values are shares of users. A given entry means that at
         a particular number step x% of users encountered a specific event.

        Parameters
        --------
        centered
        accumulated
        thresh
        accumulate_targets
        targets
        index_col
        max_steps: int, optional
            Maximum number of steps in trajectory to include. Depending on ``reverse`` parameter value, the steps are
            counted from the beginning of trajectories if ``reverse=False``, or from the end otherwise.
        plot_type: bool, optional
            If ``True``, then plots step matrix in interactive session (Jupyter notebook). Default: ``True``
        thr: float, optional
            Used to prune matrix and display only the rows with at least one value >= ``thr``. Default: ``None``
        reverse: str or list, optional
            This parameter is used to display reversed matrix from target events towards the beginning of trajectories.
            Range of possible values:
                - ``None``: displays default step matrix from the start of trajectories. Uses all the user trajectories.
                - ``'pos'``: displays reverse step matrix in such a way that the first column is the
                ``positive_target_event`` share, which is always 1, and the following columns reflect the share of users
                 on final steps before reaching the target. Uses only those trajectories, which ended up having at least
                  one ``positive_target_event`` in trajectory.
                - ``'neg'``: same as ``pos`` but for ``negative_target_event``. Uses only those trajectories, which
                ended up having at least one ``negative_target_event`` in trajectory.
                - ``['pos', 'neg']``: combination of ``pos`` and ``neg`` options, first column has only target events.
                Uses all the trajectories with target events inside.
            Default: ``None``
        sorting: bool, optional
            If ``True``, then automatically places elements with highest values in top. Rows are sorted in such a way
            that the first one has highest first column value, second row has the highest second column value,besides
            already used first value, etc. With this sorting you may see a dominant trajectory as a diagonal.
            Default: ``True``
        weight_col: str, optional
            Aggregation column for edge weighting. For instance, you may set it to the same value as in ``index_col``
            and define ``edge_attributes='users_unique'`` to calculate unique users passed through edge.
            Default: ``None``

        Returns
        -------
        Dataframe with ``max_steps`` number of columns and len(event_col.unique) number of rows at max, or less if used
        ``thr`` > 0.

        Return type
        -------
        pd.DataFrame
        """
        from pandas.core.common import flatten
        from copy import deepcopy

        event_col = self.retention_config['event_col']
        weight_col = weight_col or self.retention_config['index_col']
        time_col = self.retention_config['event_time_col']

        data = self._obj.copy()

        # add termination event to each history:
        data = data.rete.split_sessions(thresh=None,
                                        eos_event='ENDED',
                                        session_col=None)

        data['event_rank'] = 1
        data['event_rank'] = data.groupby(weight_col)['event_rank'].cumsum()

        # ALIGN DATA IF CENTRAL
        fraction_title = ''
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

            if len(users_to_keep)==0:
                raise ValueError(f'no records found with event "{center_event}" occuring N={occurrence} times')

            fraction_used = round(len(users_to_keep) / data[weight_col].nunique() * 100)
            if fraction_used != 100:
                fraction_title = f'({fraction_used}% of total records)'
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

        if max_steps:
            agg = agg[agg['event_rank'] <= max_steps]
        agg.columns = ['event_rank', 'event_name', 'freq']

        piv = agg.pivot(index='event_name', columns='event_rank', values='freq').fillna(0)
        piv.columns.name = None
        piv.index.name = None

        # MAKE TERMINATED STATE ACCUMULATED:
        if 'ENDED' in piv.index:
            piv.loc['ENDED'] = piv.loc['ENDED'].cumsum().fillna(0)

        # add NOT_STARTED events for centered matrix
        if centered:
            piv.loc['NOT_STARTED'] = 1 - piv.sum()

        if sorting:
            piv = BaseTrajectory._sort_matrix(piv)
            if 'ENDED' in piv.index:
                keep_in_the_end = ['ENDED']
                piv = piv.loc[[*(i for i in piv.index if i not in keep_in_the_end), *keep_in_the_end]]

        if thresh != 0:
            # find if there are any rows to threshold:
            thresholded = piv.loc[(piv < thresh).all(1)].copy()
            if len(thresholded) > 0:
                piv = piv.loc[(piv >= thresh).any(1)].copy()
                piv.loc[f'THRESHOLDED_{len(thresholded)}'] = thresholded.sum()

        # ADD ROWS FOR TARGETS:
        piv_targets = None
        if targets:
            # obtain flatten list of targets:
            targets_flatten = list(flatten(targets))

            # format targets to list of lists:
            for n, i in enumerate(targets):
                if type(i) != list:
                    targets[n] = [i]

            if len(set(targets_flatten) - set(data[event_col])) != 0:
                raise ValueError(f'Some of the events from "targets" are not found in the column: "{event_col}"')
            else:
                agg_targets = (data
                               .groupby(['event_rank', event_col])[time_col]
                               .count()
                               .reset_index())
                agg_targets[time_col] /= data[weight_col].nunique()
                agg_targets.columns = ['event_rank', 'event_name', 'freq']

                if max_steps:
                    agg_targets = agg_targets[agg_targets['event_rank'] <= max_steps]

                piv_targets = agg_targets.pivot(index='event_name', columns='event_rank', values='freq').fillna(0)
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

        if centered:
            piv.columns = [f'{int(i)-window-1}' for i in piv.columns]
            if targets:
                piv_targets.columns = [f'{int(i) - window - 1}' for i in piv_targets.columns]

        plot.step_matrix(piv, piv_targets,
                         targets_list=targets,
                         title=f'{"centered" if centered else ""} step matrix {fraction_title}',
                         centered_position=(window if centered else None))

        return piv
