from typing import TypeVar, TypedDict

from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel

P = TypeVar("P", bound=TypedDict)


class DataProcessor:
    params: ParamsModel = None

    def __init__(self, params: ParamsModel) -> None:
        self.params = params

    def apply(self, eventstream: Eventstream) -> Eventstream:
        raise NotImplementedError
