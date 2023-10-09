from __future__ import annotations

import typing
import uuid
from typing import Any, Type

import pandas as pd

from retentioneering.data_processor.registry import register_dataprocessor
from retentioneering.params_model import ParamsModel
from retentioneering.utils.classes import call_if_implemented

if typing.TYPE_CHECKING:
    from retentioneering.eventstream.types import EventstreamSchemaType, EventstreamType


class DataProcessor:
    params: ParamsModel

    @classmethod
    def __init_subclass__(cls: Type[DataProcessor], **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        register_dataprocessor(cls)

    def __init__(self, params: ParamsModel | Any) -> None:
        if not issubclass(type(params), ParamsModel):
            raise TypeError("params is not subclass of ParamsModel")

        self.params = params
        self.pk = uuid.uuid4()

    def __call__(self, params: ParamsModel) -> DataProcessor:
        DataProcessor.__init__(self, params=params)
        return self

    def _define_new_schema(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> EventstreamSchemaType:
        cols = schema.get_cols()
        data_cols = list(df.columns)

        new_schema = schema.copy()

        if lost_cols := set(cols).difference(set(data_cols)):
            raise ValueError(
                f"Dataprocessor error, the following columns are missing from the returned dataframe:  {', '.join(lost_cols)}"
            )

        if len(data_cols) > len(cols):
            new_custom_cols = list(set(data_cols).difference(cols))
            new_schema.custom_cols.extend(new_custom_cols)

        return new_schema

    def _get_new_data(self, eventstream: EventstreamType) -> EventstreamType:
        from retentioneering.eventstream.eventstream import Eventstream

        # TODO: legacy fallback, удалить после переноса датапроцессоров
        copy = eventstream.copy()

        diff: EventstreamType | None = call_if_implemented(self, "apply_diff", [copy])
        if diff is not None:
            copy._join_eventstream(diff)
            return copy

        curr_events = copy.to_dataframe()
        new_events = self.apply(df=curr_events, schema=copy.schema)
        new_schema = self._define_new_schema(df=new_events, schema=copy.schema)

        new_stream = Eventstream(
            raw_data=new_events,
            raw_data_schema=new_schema.to_raw_data_schema(event_id=True, event_index=True),
            schema=new_schema,  # type: ignore
            index_order=copy.index_order,
        )

        new_stream.drop_soft_deleted_events()
        return new_stream

    def apply(self, df: pd.DataFrame, schema: EventstreamSchemaType) -> pd.DataFrame:
        raise NotImplementedError

    def apply_diff(self, eventstream: EventstreamType) -> EventstreamType:
        raise NotImplementedError

    def export(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        widgets: dict[str, Any] = self.params.get_widgets()
        data["name"] = self.__class__.__name__
        data["pk"] = str(self.pk)
        data["schema"] = self.params.schema()
        data["widgets"] = widgets
        return data

    @classmethod
    def get_view(cls) -> dict[str, str | list | dict]:
        data: dict[str, str | list | dict] = dict()
        data["name"] = cls.__name__
        from retentioneering.params_model.registry import params_model_registry

        params_models = params_model_registry.get_registry()
        params_model_name = cls.__annotations__["params"]
        if type(params_model_name) is str:
            params = params_models[params_model_name]
        else:
            params = params_model_name
        view = params.get_widgets()
        view_data = []
        for key in view:
            view_data.append(view[key])

        data["params"] = view_data
        return data

    def to_dict(self) -> dict:
        data = {
            "values": self.params.dict(),
            "name": self.__class__.__name__,
        }
        return data

    def copy(self) -> DataProcessor:
        return self.__copy__()

    def __copy__(self) -> DataProcessor:
        return self.__class__(params=self.params.copy())
