from abc import ABCMeta, abstractmethod
from typing import Any


class ConnectorProtocol(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def send_message(self, data) -> None:
        ...


class TrackerMainConnector(ConnectorProtocol):

    def send_message(self, data: dict[str, Any]) -> None:
        pass
