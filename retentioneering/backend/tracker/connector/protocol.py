from __future__ import annotations

from abc import ABCMeta, abstractmethod

from retentioneering.backend.tracker.tracking_info import TrackingInfo


class ConnectorProtocol(metaclass=ABCMeta):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def send_message(self, data: TrackingInfo) -> None:
        ...
