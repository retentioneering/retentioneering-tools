from typing import TypeVar, TypedDict

from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel

P = TypeVar("P", bound=TypedDict)


class DataProcessor:
    params: ParamsModel = None

    def __init__(self, params: ParamsModel) -> None:
        if not issubclass(type(params), ParamsModel):
            raise TypeError('params is not subclass of ParamsModel')

        self.params = params

    def apply(self, eventstream: Eventstream) -> Eventstream:
        raise NotImplementedError

    def __repr__(self) -> str:
        data = {
            'schema': self.params.schema(),
            'values': self.params.dict(),
        }
        return f'{data}'
