from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import asdict

import requests

from .tracking_info import TrackingInfo


class ConnectorProtocol(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def send_message(self, data: TrackingInfo) -> None:
        ...


class TrackerMainConnector(ConnectorProtocol):
    def __init__(self) -> None:
        super().__init__()
        self.url = "https://t.trsbf.com/endpoint/event"
        self.source = "rete_tools"
        self.session = requests.Session()

    def _post(self, data: dict) -> dict:
        return self.session.post(self.url, data=data).json()

    def send_message(self, data: TrackingInfo) -> None:
        self._post(asdict(data))
