from .connector import TrackerMainConnector
from .tracker import Tracker

tracker = Tracker(connector=TrackerMainConnector())

track = tracker.track
