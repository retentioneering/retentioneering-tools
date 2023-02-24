from __future__ import annotations

import typing
import uuid
from typing import Any

from retentioneering.data_processor.registry import register_dataprocessor
from retentioneering.params_model import ParamsModel

if typing.TYPE_CHECKING:
    from retentioneering.eventstream.types import EventstreamType


class DataProcessor:
    params: ParamsModel

    @classmethod
    def __init_subclass__(cls: type[DataProcessor], **kwargs: Any) -> None:
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

    def apply(self, eventstream: EventstreamType) -> EventstreamType:
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
