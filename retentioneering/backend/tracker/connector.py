from __future__ import annotations

import json
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
        self.source = "rete_transition_graph"
        self.session = requests.Session()

    def _post(self, data: dict) -> requests.Response:
        data["source"] = self.source
        data["params"] = str(data["params"])
        req = requests.Request("POST", self.url, data=json.dumps(data))
        prepped = req.prepare()
        response = self.session.send(prepped)
        return response

    def send_message(self, data: TrackingInfo) -> None:
        self._post(asdict(data))
