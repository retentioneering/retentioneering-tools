from __future__ import annotations

from typing import List, Literal, Union

import pandas as pd

from retentioneering.backend.tracker import collect_data_performance, time_performance
from retentioneering.data_processor import DataProcessor
from retentioneering.eventstream.types import EventstreamSchemaType
from retentioneering.params_model import ParamsModel
from retentioneering.utils.doc_substitution import docstrings
from retentioneering.utils.hash_object import hash_dataframe
from retentioneering.widget.widgets import ListOfUsers


class LabelNewUsersParams(ParamsModel):
    """
    A class with parameters for :py:class:`.LabelNewUsers` class.
    """

    new_users_list: Union[List[int], List[str], Literal["all"]]
    _widgets = {"new_users_list": ListOfUsers()}


@docstrings.get_sections(
    base="LabelNewUsers",
)  # type: ignore
class LabelNewUsers(DataProcessor):
    """
    Create a new synthetic event for each user:
    ``new_user`` or ``existing_user``.

    Parameters
    ----------
    new_users_list : list of int or list of str or `all`
        If the `list of user_ids` is given - ``new_user`` event will be created for each user from the list.
        Event ``existing_user`` will be added to the rest of the users.

        If ``all`` - ``new_user`` synthetic event will be created for all users from the input ``eventstream``.

    Returns
    -------
    Eventstream
        Eventstream with new synthetic events, one for each user:

        +-----------------+-----------------+------------------------+
        | **event_name**  | **event_type**  | **timestamp**          |
        +-----------------+-----------------+------------------------+
        | new_user        | new_user        | first_event            |
        +-----------------+-----------------+------------------------+
        | existing_user   | existing_user   | first_event            |
        +-----------------+-----------------+------------------------+

    Notes
    -----
    See :doc:`Data processors user guide</user_guides/dataprocessors>` for the details.

    """

    params: LabelNewUsersParams

    @time_performance(
        scope="label_new_users",
        event_name="init",
    )
    def __init__(self, params: LabelNewUsersParams):
        super().__init__(params=params)

    @time_performance(  # type: ignore
        scope="label_new_users",
        event_name="apply",
    )
    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        user_col = schema.user_id
        type_col = schema.event_type
        event_col = schema.event_name
        new_users_list = self.params.new_users_list

        matched_events = df.groupby(user_col, as_index=False).first()

        if new_users_list == "all":
            matched_events[type_col] = "new_user"
            matched_events[event_col] = "new_user"
        else:
            new_user_mask = matched_events[user_col].isin(new_users_list)
            matched_events.loc[new_user_mask, type_col] = "new_user"  # type: ignore
            matched_events.loc[~new_user_mask, type_col] = "existing_user"  # type: ignore
            matched_events[event_col] = matched_events[type_col]

        result = pd.concat([df, matched_events])

        collect_data_performance(
            scope="label_new_users",
            event_name="metadata",
            called_params={"new_users_list": new_users_list},
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
