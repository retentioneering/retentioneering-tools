from __future__ import annotations

import uuid
from typing import Any

from src.params_model.registry import register_params_model
from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class DataProcessor:
    params: ParamsModel

    # @classmethod
    def __init_subclass__(cls, **kwargs):
        register_params_model(cls)

    def __init__(self, params: ParamsModel | Any) -> None:
        if not issubclass(type(params), ParamsModel):
            raise TypeError("params is not subclass of ParamsModel")

        self.params = params
        self.pk = uuid.uuid4()

    def apply(self, eventstream: Eventstream) -> Eventstream:
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
    def get_view(cls) -> dict[str, str]:
        data: dict[str, str | list[dict]] = dict()
        data["name"] = cls.__class__.__name__
        print(cls)
        print(dir(cls))
        # view = cls.params.get_view()
        # data["params"] = view

        return data
