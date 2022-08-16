from abc import abstractmethod
from typing import Generic, TypedDict, TypeVar

from src.eventstream import Eventstream

from .params_model import ParamsModel

P = TypeVar("P", bound=TypedDict)


class DataProcessor(Generic[P]):
    params: ParamsModel[P]

    @abstractmethod
    def apply(self, eventstream: Eventstream) -> Eventstream:
        raise NotImplementedError()
