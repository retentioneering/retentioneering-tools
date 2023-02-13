import numpy as np
import pandas as pd

from retentioneering.eventstream.types import EventstreamSchemaType


class EndedEventsMixin:
    def __init__(self) -> None:
        pass

    @staticmethod
    def _add_ended_events(data: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        """
        Adds artificial ``ENDED`` event in the end of a path. If a path already
        contains ``path_end`` event, it will be replaced with ``ENDED`` event.
        Otherwise, ``ENDED`` event will be placed into the end of the path.
        """
        data[schema.event_name] = data[schema.event_name].str.replace("path_end", "ENDED")
        users_with_ended = data[data[schema.event_name] == "ENDED"][schema.user_id].unique()

        paths_with_ended = data[data[schema.user_id].isin(users_with_ended)]
        paths_without_ended = data[~data[schema.user_id].isin(users_with_ended)]

        additional_ended_events = (
            paths_without_ended.groupby(schema.user_id, as_index=False)
            .last()
            .assign(
                **{
                    schema.event_name: "ENDED",
                    schema.event_index: lambda df_: df_[schema.event_index]
                    + (np.where(df_[schema.event_name] == "ENDED", 0.5, 0)),
                }
            )
        )

        new_data = pd.concat([paths_with_ended, paths_without_ended, additional_ended_events])
        return new_data
