from __future__ import annotations

from typing import Any

# import for filling registry
from src.data_processors_lib.rete import PositiveTargetParams  # noqa
from src.params_model.registry import params_model_registry


def list_dataprocessor(payload: dict[str, Any]) -> list[dict[str, str]]:
    registry: list[dict[str, str]] = params_model_registry.get_registry()
    return registry
