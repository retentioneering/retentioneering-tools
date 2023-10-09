import os

from retentioneering import RETE_CONFIG

from .connector.csv_connector import CSVConnector
from .connector.rete_connector import TrackerMainConnector
from .tracker import Tracker

if csv_name := os.getenv("RETE_TRACKER_CSV_NAME", None):
    tracker = Tracker(connector=CSVConnector(csv_name=csv_name))
else:
    tracker = Tracker(connector=TrackerMainConnector(), enabled=RETE_CONFIG.tracking.is_tracking_allowed)

track = tracker.track
time_performance = tracker.time_performance
collect_data_performance = tracker.collect_data_performance

__all__ = [
    "track",
    "tracker",
    "time_performance",
    "collect_data_performance",
]
