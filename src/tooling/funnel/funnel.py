from src.eventstream import Eventstream


class Funnel:
    __eventstream: Eventstream

    def __init__(self, eventstream: Eventstream):
        self.__eventstream = eventstream
