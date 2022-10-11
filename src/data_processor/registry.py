from __future__ import annotations

import typing

from src.utils.registry import ReteRegistry

if typing.TYPE_CHECKING:
    from .data_processor import DataProcessor


class DataprocessorRegistry(ReteRegistry):
    objects = "Dataprocessor"
    pass


dataprocessor_registry = DataprocessorRegistry()


def register_dataprocessor(cls: DataProcessor):
    dataprocessor_registry[cls.__class__.__name__] = cls.get_view()
