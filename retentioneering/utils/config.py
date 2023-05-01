from __future__ import annotations

import json
import os
import uuid
import warnings
from dataclasses import asdict
from pathlib import Path

from pydantic import BaseConfig
from pydantic.dataclasses import dataclass

from retentioneering.utils.hwid import get_hwid  # type: ignore

DEFAULT_CONFIG = {
    "transition_graph": {
        "width": 960,
        "height": 900,
        "show_weights": True,
        "show_percents": False,
        "show_nodes_names": True,
        "show_all_edges_for_targets": True,
        "show_nodes_without_links": False,
        "url": "https://static.server.retentioneering.com/viztools/transition-graph/v3/transition-graph.umd.js",
    },
    "preprocessing_graph": {
        "width": 960,
        "height": 900,
    },
}


@dataclass
class TrackingConfig(BaseConfig):
    """
    Configuration for tracking.
    """

    is_tracking_allowed: bool = True
    tracking_id: str = ""

    def __post_init__(self) -> None:
        if self.tracking_id == "":
            self.tracking_id = get_hwid() or str(uuid.uuid4())


@dataclass
class TransitionGraphConfig(BaseConfig):
    """
    Configuration for the transition graph.
    """

    url: str = "https://static.server.retentioneering.com/viztools/transition-graph/v3/transition-graph.umd.js"
    width: int = 960
    height: int = 900
    show_weights: bool = True
    show_percents: bool = False
    show_nodes_names: bool = True
    show_all_edges_for_targets: bool = True
    show_nodes_without_links: bool = False


@dataclass
class PreprocessiongGraphConfig(BaseConfig):
    """
    Configuration for the preprocessing graph.
    """

    width: int = 960
    height: int = 900


@dataclass
class Config(BaseConfig):
    """
    Configuration for the application.
    """

    tracking: TrackingConfig = TrackingConfig()
    transition_graph: TransitionGraphConfig = TransitionGraphConfig()
    preprocessing_graph: PreprocessiongGraphConfig = PreprocessiongGraphConfig()

    def _get_path_for_config(self) -> str | None:
        config_filename = ".retentioneering_config.json"
        home = str(Path.home())
        if os.access(f"{home}", os.W_OK):
            return f"{home}/{config_filename}"
        elif os.access(".", os.W_OK):
            print(config_filename)
            return config_filename
        else:
            return None

    def __post_init__(self) -> None:
        config_path = self._get_path_for_config()
        if config_path is None:
            return None
        # check if file exists
        if not os.path.isfile(config_path):
            with open(config_path, "w") as f:
                json.dump(DEFAULT_CONFIG, f)
        else:
            with open(config_path, "r") as f:
                try:
                    config = json.load(f)
                    self.tracking = TrackingConfig(**config.get("tracking", {}))
                    self.transition_graph = TransitionGraphConfig(**config.get("transition_graph", {}))
                    self.preprocessing_graph = PreprocessiongGraphConfig(**config.get("preprocessing_graph", {}))
                except json.decoder.JSONDecodeError:
                    warnings.warn("Invalid config file. Please fix it and try again. Current config file is default.")

    def save(self) -> None:
        config_path = self._get_path_for_config()
        if config_path is None:
            return None
        # get current data as dict
        config_data = {
            "tracking": asdict(self.tracking),
            "transition_graph": asdict(self.transition_graph),
            "preprocessing_graph": asdict(self.preprocessing_graph),
        }
        with open(config_path, "w") as f:
            json.dump(config_data, f)


RETE_CONFIG = Config()
