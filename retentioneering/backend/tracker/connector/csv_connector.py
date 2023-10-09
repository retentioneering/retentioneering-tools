import json
import os
import threading
from dataclasses import asdict

import pandas as pd

from ..tracking_info import TrackingInfo
from .protocol import ConnectorProtocol


class CSVConnector(ConnectorProtocol):
    def __init__(self, csv_name: str):
        super().__init__()
        # validate csv path writable

        csv_path = f"{os.getcwd()}/{csv_name}"
        if os.access(csv_path, os.F_OK):
            raise Exception(f"CSV file {csv_path} already exists")
        else:
            self.csv_path = csv_path
            os.environ["RETE_TRACKER_CSV_PATH"] = csv_path

        self.source = "rete_tools_backend"
        self.columns = [
            "user_id",
            "event_custom_name",
            "event_name",
            "event_value",
            "params",
            "scope",
            "event_time",
            "jupyter_kernel_id",
            "colab",
            "event_date_local",
            "event_day_week",
            "event_timestamp",
            "event_timestamp_ms",
            "source",
            "version",
            "os",
            "index",
            "browser",
            "eventstream_index",
            "parent_eventstream_index",
            "child_eventstream_index",
            "account_id",
        ]

    def _post(self, data: dict) -> None:
        # run _post_job in a separate thread using multi-threading
        try:
            post_thread = threading.Thread(target=self._post_job, args=(data,))
            post_thread.start()
        except Exception as e:
            pass

    def _post_job(self, data: dict) -> None:
        with open(self.csv_path, "a") as log_file:
            pd.DataFrame([data], columns=self.columns).to_csv(log_file, index=False, header=False)

    def _prepare_data(self, data: TrackingInfo) -> dict:
        prepared_data = asdict(data)
        prepared_data["source"] = self.source
        prepared_data["params"] = json.dumps(prepared_data["params"])
        del prepared_data["event_time"]
        return prepared_data

    def send_message(self, data: TrackingInfo) -> None:
        prepared_data = self._prepare_data(data=data)
        self._post(prepared_data)
