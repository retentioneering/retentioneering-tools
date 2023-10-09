from __future__ import annotations

from typing import Any

from retentioneering.data_processor.registry import dataprocessor_view_registry

# import for filling registry
from retentioneering.data_processors_lib import AddPositiveEventsParams  # noqa


def list_dataprocessor(payload: dict[str, Any]) -> list[dict[str, str]]:
    registry: list[dict[str, str]] = dataprocessor_view_registry.get_export_registry()
    registry = sorted(registry, key=lambda x: x["name"])
    return registry
