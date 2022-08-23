from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class DataProcessor:
    params: ParamsModel

    def __init__(self, params: ParamsModel) -> None:
        if not issubclass(type(params), ParamsModel):  # type: ignore
            raise TypeError("params is not subclass of ParamsModel")

        self.params = params

    def apply(self, eventstream: Eventstream) -> Eventstream:
        raise NotImplementedError
