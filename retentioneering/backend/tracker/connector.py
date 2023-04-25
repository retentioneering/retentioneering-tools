from __future__ import annotations

import json
import threading
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
    url: str
    source: str
    session: requests.Session

    def __init__(self) -> None:
        super().__init__()
        self.url = "https://t.trsbf.com/endpoint/event"
        self.source = "rete_transition_graph"
        self.session = requests.Session()

    def _post(self, data: dict) -> None:
        # run _post_job in a separate thread using multi-threading
        try:
            post_thread = threading.Thread(target=self._post_job, args=(data,))
            post_thread.start()
        except Exception:
            # supress any exceptions in tracking
            # @TODO: store exceptions to tracking base or sentry. Vladimir Makhanov
            pass

    def _post_job(self, data: dict) -> requests.Response | None:
        try:
            data["source"] = self.source
            with self.session.post(self.url, data=json.dumps(data)) as response:
                return response
        except Exception:
            # supress any exceptions in tracking. Vladimir Makhanov
            return None

    def _prepare_data(self, data: TrackingInfo) -> dict:
        prepared_data = asdict(data)
        prepared_data["source"] = self.source
        prepared_data["params"] = str(prepared_data["params"])
        return prepared_data

    def send_message(self, data: TrackingInfo) -> None:
        prepared_data = self._prepare_data(data=data)
        self._post(prepared_data)
