from __future__ import annotations

from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from retentioneering.backend.tracker import collect_data_performance, time_performance
from retentioneering.constants import DATETIME_UNITS
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe
from retentioneering.widget.widgets import ListOfIds, ReteTimeWidget


class LabelLostUsersParams(ParamsModel):
    """
    A class with parameters for :py:class:`.LabelLostUsers` class.
    """

    timeout: Optional[Tuple[float, DATETIME_UNITS]]
    lost_users_list: Optional[Union[List[int], List[str]]]

    _widgets = {
        "timeout": ReteTimeWidget(),
        "lost_users_list": ListOfIds(),
    }


@docstrings.get_sections(base="LabelLostUsers")  # type: ignore
class LabelLostUsers(DataProcessor):
    """
    Create one of synthetic events in each user's path:
    ``lost_user`` or ``absent_user``.


    Parameters
    ----------
    Only one of parameters could be used at the same time
    timeout : Tuple(float, :numpy_link:`DATETIME_UNITS<>`), optional
        Threshold value and its unit of measure.
        Calculate timedelta between the last event in each user's path and the last event in the whole eventstream.
        For users with timedelta greater or equal to selected ``timeout``, a new synthetic event - ``lost_user``
        will be added.
        For other users paths a new synthetic event - ``absent_user`` will be added.
    lost_users_list : list of int or list of str, optional
        If the `list of user_ids` is given new synthetic event - ``lost_user`` will be added to each user from the list.
        For other user's paths will be added new synthetic event - ``absent_user``.

    Returns
    -------
    Eventstream
        ``Eventstream`` with new synthetic events only - one for each user:

        +-----------------+-----------------+------------------+
        | **event_name**  | **event_type**  |  **timestamp**   |
        +-----------------+-----------------+------------------+
        | lost_user       | lost_user       | last_event       |
        +-----------------+-----------------+------------------+
        | absent_user     | absent_user     | last_event       |
        +-----------------+-----------------+------------------+

    Raises
    ------
    ValueError
        Raised when both ``timeout`` and ``lost_users_list`` are either empty or given.

    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.
    """

    params: LabelLostUsersParams

    @time_performance(
        scope="label_lost_users",
        event_name="init",
    )
    def __init__(self, params: LabelLostUsersParams):
        super().__init__(params=params)

    @time_performance(  # type: ignore
        scope="label_lost_users",
        event_name="apply",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        user_col = schema.user_id
        time_col = schema.event_timestamp
        type_col = schema.event_type
        event_col = schema.event_name

        timeout, timeout_unit = None, None
        lost_users_list = self.params.lost_users_list
        data_lost = pd.DataFrame()

        if self.params.timeout:
            timeout, timeout_unit = self.params.timeout

        if timeout and lost_users_list:
            raise ValueError("timeout and lost_users_list parameters cannot be used simultaneously!")

        if not timeout and not lost_users_list:
            raise ValueError("Either timeout or lost_users_list must be specified!")

        if timeout and timeout_unit:
            data_lost = df.groupby(user_col, as_index=False).last()
            data_lost["diff_end_to_end"] = data_lost[time_col].max() - data_lost[time_col]

            data_lost["diff_end_to_end"] /= np.timedelta64(1, timeout_unit)  # type: ignore

            data_lost[type_col] = np.where(data_lost["diff_end_to_end"] < timeout, "absent_user", "lost_user")
            data_lost[event_col] = data_lost[type_col]
            del data_lost["diff_end_to_end"]

        if lost_users_list:
            data_lost = df.groupby(user_col, as_index=False).last()
            data_lost[type_col] = np.where(data_lost["user_id"].isin(lost_users_list), "lost_user", "absent_user")
            data_lost[event_col] = data_lost[type_col]

        result = pd.concat([df, data_lost])

        collect_data_performance(
            scope="label_lost_users",
            event_name="metadata",
            called_params=self.to_dict()["values"],
            not_hash_values=["timeout"],
            performance_data={
                "parent": {
                    "shape": df.shape,
                    "hash": hash_dataframe(df),
                },
                "child": {
                    "shape": result.shape,
                    "hash": hash_dataframe(result),
                },
            },
        )

        return result
