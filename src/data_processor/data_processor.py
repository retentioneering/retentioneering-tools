from abc import abstractmethod
from typing import Generic, TypeVar, TypedDict, Any

from src.eventstream.eventstream import Eventstream
from src.params_model import ReteParamsModel
from .params_model import ParamsModel

P = TypeVar("P", bound=TypedDict)


class DataProcessor(Generic[P]):
    params: ParamsModel[P]

    @abstractmethod
    def apply(self, eventstream: Eventstream) -> Eventstream:
        raise NotImplementedError()


class ReteDataProcessor:
    params: ReteParamsModel = None

    def __init__(self, params: ReteParamsModel) -> None:
        self.params = params

    def apply(self, eventstream: Eventstream) -> Eventstream:
        raise NotImplementedError
