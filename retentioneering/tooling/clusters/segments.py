from __future__ import annotations

from typing import List, Union

import numpy
import pandas as pd
from numpy import ndarray

from retentioneering.eventstream.types import EventstreamType
from retentioneering.tooling.clusters.userlist import UserList

SEGMENTS_COLNAME = "segment"
SegmentVal = Union[int, str]


class Segments:
    # readonly
    __userlist: UserList
    __eventstream: EventstreamType

    def __init__(
        self,
        eventstream: EventstreamType,
        segments_df: pd.DataFrame | None = None,
    ):
        self.__userlist = UserList(eventstream=eventstream)
        self.__eventstream = eventstream

        if segments_df is not None:
            self.__userlist.add_classes(SEGMENTS_COLNAME, segments_df)
        else:
            # add empty segments
            userlist_df = self.__userlist.to_dataframe()
            userlist_df[SEGMENTS_COLNAME] = numpy.nan
            self.__userlist.add_classes(SEGMENTS_COLNAME, userlist_df)

    def show_segments(self) -> pd.DataFrame | pd.Series:
        user_id = self.__eventstream.schema.user_id
        return self.__userlist.to_dataframe()[[user_id, SEGMENTS_COLNAME]]

    def add_segment(self, segment: SegmentVal, users: Union[pd.Series, List]) -> None:
        self.__userlist.assign(SEGMENTS_COLNAME, value=segment, users=users)

    def get_users(self, segment: SegmentVal) -> pd.Series:
        user_id = self.__eventstream.schema.user_id
        userlist_df = self.__userlist.to_dataframe()
        return userlist_df[userlist_df[SEGMENTS_COLNAME] == segment][user_id]

    def get_all_users(self) -> pd.Series:
        user_id = self.__eventstream.schema.user_id
        return self.__userlist.to_dataframe()[user_id]

    def get_all_segments(self) -> pd.DataFrame | pd.Series | ndarray:
        userlist_df = self.__userlist.to_dataframe()
        return userlist_df[SEGMENTS_COLNAME].unique()

    def get_segment_list(self) -> pd.DataFrame | int:
        userlist_df = self.__userlist.get_count(SEGMENTS_COLNAME)
        return userlist_df
