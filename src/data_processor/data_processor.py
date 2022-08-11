from abc import abstractmethod
from typing import Generic, TypeVar, TypedDict, Any
from .params_model import ParamsModel

from src.eventstream.eventstream import Eventstream

P = TypeVar("P", bound=TypedDict)


class DataProcessor(Generic[P]):
    params: ParamsModel[P]

    def __init__(self, params: ParamsModel):
        pass

    @abstractmethod
    def apply(self, eventstream: Eventstream) -> Eventstream:
        pass


from src.params_model import ReteParamsModel


class ReteDataProcessor:
    params: ReteParamsModel = None

    def __init__(self, params: ReteParamsModel) -> Any:
        self.params = params

    def apply(self, eventstream: Eventstream) -> Eventstream:
        raise NotImplementedError
