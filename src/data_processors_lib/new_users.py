from __future__ import annotations

from typing import List, Literal, Union

from pandas import DataFrame

from src.data_processor.data_processor import DataProcessor
from src.eventstream.types import EventstreamType
from src.params_model import ParamsModel
from src.widget.widgets import ListOfIntNewUsers


class NewUsersParams(ParamsModel):
    """
    Class with parameters for class :py:func:`NewUsersEvents`
    """

    new_users_list: Union[List[int], List[str], Literal["all"]]
    _widgets = {"new_users_list": ListOfIntNewUsers}


class NewUsersEvents(DataProcessor):
    """
    Creates new synthetic event for each user:
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
        Eventstream with new synthetic events one for each user:

        +-----------------+-----------------+------------------------+
        | **event_name**  | **event_type**  | **timestamp**          |
        +-----------------+-----------------+------------------------+
        | new_user        | new_user        | first_event            |
        +-----------------+-----------------+------------------------+
        | existing_user   | existing_user   | first_event            |
        +-----------------+-----------------+------------------------+


    """

    params: NewUsersParams

    def __init__(self, params: NewUsersParams):
        super().__init__(params=params)

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
        from src.eventstream.eventstream import Eventstream

        events: DataFrame = eventstream.to_dataframe(copy=True)
        user_col = eventstream.schema.user_id
        type_col = eventstream.schema.event_type
        event_col = eventstream.schema.event_name
        new_users_list = self.params.new_users_list

        matched_events = events.groupby(user_col, as_index=False).first()

        if new_users_list == "all":
            matched_events[type_col] = "new_user"
            matched_events[event_col] = "new_user"
        else:
            new_user_mask = matched_events[user_col].isin(new_users_list)
            matched_events.loc[new_user_mask, type_col] = "new_user"  # type: ignore
            matched_events.loc[~new_user_mask, type_col] = "existing_user"  # type: ignore
            matched_events[event_col] = matched_events[type_col]

        matched_events["ref"] = None

        eventstream = Eventstream(
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            raw_data=matched_events,
            relations=[{"raw_col": "ref", "eventstream": eventstream}],
        )
        return eventstream
