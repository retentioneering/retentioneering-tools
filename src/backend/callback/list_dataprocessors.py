from __future__ import annotations

from typing import Any

from src.data_processor.registry import dataprocessor_view_registry

# import for filling registry
from src.data_processors_lib.rete import PositiveTargetParams  # noqa


def list_dataprocessor(payload: dict[str, Any]) -> list[dict[str, str]]:
    registry: list[dict[str, str]] = dataprocessor_view_registry.get_registry()
    return registry
