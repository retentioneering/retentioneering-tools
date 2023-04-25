from retentioneering import RETE_CONFIG

from .connector import TrackerMainConnector
from .tracker import Tracker

tracker = Tracker(connector=TrackerMainConnector(), enabled=RETE_CONFIG)

track = tracker.track
