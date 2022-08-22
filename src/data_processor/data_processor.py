from src.eventstream.eventstream import Eventstream
from src.params_model import ParamsModel


class DataProcessor:
    params: ParamsModel = None

    def __init__(self, params: ParamsModel) -> None:
        print(f"{issubclass(type(params), ParamsModel)=}")
        if not issubclass(type(params), ParamsModel):
            raise TypeError("params is not subclass of ParamsModel")

        self.params = params

    def apply(self, eventstream: Eventstream) -> Eventstream:
        raise NotImplementedError
