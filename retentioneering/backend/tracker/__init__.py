from .connector import TrackerMainConnector
from .tracker import Tracker

# @TODO: read data from config file. Vladimir Makhanov
ENABLED = False

tracker = Tracker(connector=TrackerMainConnector(), enabled=ENABLED)

track = tracker.track
