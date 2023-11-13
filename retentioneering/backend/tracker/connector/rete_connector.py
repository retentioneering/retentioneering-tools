from __future__ import annotations

import json
import threading
from dataclasses import asdict

import requests
from requests.adapters import HTTPAdapter

from retentioneering.backend.tracker.connector import ConnectorProtocol
from retentioneering.backend.tracker.tracking_info import TrackingInfo


class TrackerMainConnector(ConnectorProtocol):
    url: str
    source: str
    session: requests.Session

    def __init__(self) -> None:
        super().__init__()
        self.url = "https://ftrack-1.server.retentioneering.com/endpoint/event"
        self.source = "rete_tools_backend"
        adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

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
        prepared_data["params"] = json.dumps(prepared_data["params"])
        del prepared_data["event_time"]
        return prepared_data

    def send_message(self, data: TrackingInfo) -> None:
        prepared_data = self._prepare_data(data=data)
        self._post(prepared_data)
