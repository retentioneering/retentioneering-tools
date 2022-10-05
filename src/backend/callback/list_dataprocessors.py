from __future__ import annotations

import json

# import for filling registry
from src.data_processors_lib.rete import PositiveTargetParams  # noqa
from src.params_model.registry import params_model_registry


def list_dataprocessor() -> str:
    registry: dict = params_model_registry.get_registry()
    response: str = json.dumps({"dataprocessor": registry})
    return response
