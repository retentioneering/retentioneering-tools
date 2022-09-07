from __future__ import annotations

from typing import Any

from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class DataProcessor:
    params: ParamsModel

    def __init__(self, params: ParamsModel | Any) -> None:
        if not issubclass(type(params), ParamsModel):
            raise TypeError("params is not subclass of ParamsModel")

        self.params = params

    def apply(self, eventstream: Eventstream) -> Eventstream:
        raise NotImplementedError

    def export(self) -> dict[str, Any]:
        data = {}
        params_schema = self.params.schema()

        return data