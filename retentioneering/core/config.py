# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


import os
import pandas as pd
from .core_functions.base_dataset import BaseDataset

config = {'experiments_folder': 'experiments'}

if not os.path.exists(config['experiments_folder']):
    os.mkdir(config['experiments_folder'])


@pd.api.extensions.register_dataframe_accessor("rete")
class RetentioneeringDataset(BaseDataset):

    def __init__(self, pandas_obj):
        super(RetentioneeringDataset, self).__init__(pandas_obj)
        self.retention_config = config


# inform user to update their old notebooks from v1.0.x:
def init_config(*args, **kwargs):
    class VersionError(Exception):
        pass

    raise VersionError('this function was depricated from v2.0.0. ' +
                       'Please refer to documentaion at https://github.com/retentioneering/retentioneering-tools')