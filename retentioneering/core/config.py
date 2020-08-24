# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import os
import json
import pandas as pd
from .base_classes.base_trajectory import BaseTrajectory
from .base_classes.base_dataset import BaseDataset


def init_config(**config):
    """
    Primary method that defines the configuration of the analysis with Retentioneering library. It is mandatory to initialize or update config and pandas accessors with this method before moving on to any other method.

    Parameters
    -------
    index_col: str
        Dataframe index column name (e.g. `user_pseudo_id` in our examples). Index column is the ID of trajectory which is a series of events or states happening at given times. Index column separates your dataset by users or sessions, etc. The most common ones are user or session columns. If you have a default dataset with user_id, event_name and timestamp columns, and want to use session as an index column, please use ``retentioneering.core.preprocessing.split_sessions`` method to generate pseudosessions.
    event_col: str
        Dataframe column name in which all the event names are stored (e.g. `event_name` in our examples).
    event_time_col: str
        Dataframe column name with timestamp values of the corresponding events (e.g. `event_timestamp` in our examples). The values in this column should be integer or float types (in the latter case, they will be rounded to integer).
    positive_target_event: str
        Name of positive target event. There are two options to define this parameter:
            - Real value which exists explicitly at least once in ``event_col`` of the dataframe. In this case there is no need to define ``pos_target_event``. For instance, you have a ``purchase`` value inside ``event_col`` which you would like to use as positive target. In this case, you should define ``positive_target_event = 'purchase'`` and not define ``pos_target_definition``.
            - Preferred alias of positive target which is implicitly generated according to logic stated in ``pos_target_definition``. In this case it is obligatory to define ``pos_target_definition``. For instance, you have ``thank_you`` and ``order_received`` events inside ``event_col`` which both mean successful purchase. In this case, you define ``positive_target_event = 'purchase'`` and ``pos_target_definition={event_list=['thank_you', 'order_received']}``.
    negative_target_event: str
        Name of negative target event. Similar to ``positive_target_event``.
    pos_target_definition: dict, optional
        If not defined:
            ``positive_target_event`` is taken as positive target and it should be present in ``event_col`` at least once. If it is not present, the future analysis will be carried on without positive target events.
        If defined:
            - As empty dict, then adds `positive_target_event` for users or sessions depending on ``index_col``, who do not have ``negative_target_event`` anywhere in the trajectory.
            - If contains ``time_limit`` (int, optional), then adds event to session after ``time_limit`` seconds of inactivity. Positive target event will be populated for each ``index_col`` whose last event happened at least ``time_limit`` seconds before ``max(event_time_col)`` and in absence of ``neg_target_event`` explicitly present in ``event_col``.
            - If contains ``event_list`` (str or dict, optional), then replaces events in list with ``positive_target_event``. It is used when you want to combine several positive target events with one name.
            - Note that either ``time_limit`` or ``event_list`` should be defined, not both.
    neg_target_definition: dict, optional
        Similar to ``pos_target_definition``, but applied to ``negative_target_event``. If both ``pos_target_definition`` and ``neg_target_definition`` are used, then in order to successfully populate the dataset, the definitions should be noncontradictory. Firstly, non-empty dictionaries are executed, then empty.
    experiments_folder: str, optional
        Folder name in current working directory where the attachments will be saved. If not stated, then folder with current timestamp as a name will be created.
    source_event: str, optional
        Name of starting event of user session or trajectory. Used in graph visuatization by accenting color of the edges starting from ``source_event`` with yellow.

    Returns
    -------
    Updates or creates ``retention_config`` dict variable with defined values.
    """
    if 'experiments_folder' not in config:
        config.update({'experiments_folder': '{}'.format(pd.datetime.now()).replace(':', '-').split('.')[0]})
    if 'target_event_list' not in config:
        config.update({
            'target_event_list': [
                config.get('negative_target_event'),
                config.get('positive_target_event'),
            ]
        })
    if 'columns_map' not in config:
        config['columns_map'] = {
            'user_pseudo_id': config.get('index_col'),
            'event_name': config.get('event_col'),
            'event_timestamp':  config.get('event_time_col'),
        }
    if not os.path.exists(config['experiments_folder']):
        os.mkdir(config['experiments_folder'])

    with open(os.path.join(config['experiments_folder'], "config.json"), "w") as f:
        json.dump(config, f)

    @pd.api.extensions.register_dataframe_accessor("trajectory")
    class RetentioneeringTrajectory(BaseTrajectory):

        def __init__(self, pandas_obj):
            super(RetentioneeringTrajectory, self).__init__(pandas_obj)
            with open(os.path.join(config['experiments_folder'], "config.json")) as f:
                self.retention_config = json.load(f)

    @pd.api.extensions.register_dataframe_accessor("retention")
    class RetentioneeringDataset(BaseDataset):

        def __init__(self, pandas_obj):
            super(RetentioneeringDataset, self).__init__(pandas_obj)
            with open(os.path.join(config['experiments_folder'], "config.json")) as f:
                self.retention_config = json.load(f)
