from __future__ import annotations

from typing import Any, Union

import pandas as pd

from src.eventstream.types import EventstreamType


COUNT_COL_NAME = "count"
UserClass = Union[str, int]


class UserList:
    # readonly
    __users: pd.DataFrame
    __eventstream: EventstreamType

    def __init__(
            self,
            eventstream: EventstreamType
    ) -> None:
        self.__eventstream = eventstream
        self.__users = self.__make_userlist()

    def get_eventstream(self):
        return self.__eventstream

    def to_dataframe(self):
        return self.__users.copy()

    def add_classes(
            self,
            colname: str,
            classes: pd.DataFrame
    ) -> None:
        user_col = self.__eventstream.schema.user_id
        self.__users.reset_index(inplace=True, drop=True)
        merged = self.__users.merge(classes, on=user_col, how="left")
        merged.reset_index(inplace=True, drop=True)
        self.__users[colname] = merged[colname]

    def assign(
            self,
            colname: str,
            value: UserClass,
            users: pd.Series[Any] | list[Any]
    ) -> None:
        user_col = self.__eventstream.schema.user_id
        matched = self.__users[user_col].isin(users)
        matched_users = self.__users[matched].copy()
        matched_users[colname] = value
        source_col = self.__users[colname]
        source_col.update(matched_users[colname])

    def get_count(
            self,
            colname: str
    ) -> int:
        usercol = self.__eventstream.schema.user_id
        r = self.__users.groupby([colname])[usercol].count().reset_index()
        return r

    def mark_eventstream(
            self,
            colname: str,
            inplace: bool = False
    ):
        eventstream = self.__eventstream if inplace else self.__eventstream.copy()

        usercol = eventstream.schema.user_id
        eventstream_df = eventstream.to_dataframe()

        users = self.__users[[usercol, colname]]
        merged = eventstream_df.merge(users, how='left', on=usercol)
        marked_col = merged[colname]
        eventstream.add_custom_col(
            name=colname,
            data=marked_col
        )
        return eventstream

    def get_eventstream_subset(
            self,
            colname: str,
            values: list[UserClass] = None
    ) -> pd.Series[Any]:
        usercol = self.__eventstream.schema.user_id
        matched = self.__users[colname].isin(values=values)
        users_subset = self.__users[matched]
        eventstream_dataframe = self.__eventstream.to_dataframe(copy=True)
        _df = eventstream_dataframe[eventstream_dataframe[usercol].isin(users_subset[usercol])]
        return _df

    def __make_userlist(self) -> pd.Series[Any]:
        user_col = self.__eventstream.schema.user_id
        id_col = self.__eventstream.schema.event_id

        events = self.__eventstream.to_dataframe()
        users = events.groupby([user_col])[id_col].count().reset_index()

        users = users.sort_values(by=id_col, ascending=False)

        users.reset_index(inplace=True, drop=True)
        users.rename(columns={id_col: COUNT_COL_NAME}, inplace=True)

        return users


class Segments:
    # readonly
    __userlist: UserList
    __eventstream: EventstreamType


class Clusters:
    __eventstream: EventstreamType
    __clusters_list: list[int]
    segments: Segments | None

    def __init__(self, eventstream: EventstreamType):
        self.__eventstream = eventstream
        self.segments = None
