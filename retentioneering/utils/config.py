import json
import os
import uuid

from pydantic import BaseConfig
from pydantic.dataclasses import dataclass

from retentioneering.backend.tracker.hwid import get_hwid  # type: ignore


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

    def __post_init__(self) -> None:
        # check if file exists
        if not os.path.isfile("retentioneering_config.json"):
            with open("retentioneering_config.json", "w") as f:
                json.dump(
                    {
                        "tracking": {
                            "is_tracking_allowed": True,
                            "tracking_id": "",
                        },
                        "transition_graph": {
                            "width": 960,
                            "height": 900,
                            "show_weights": True,
                            "show_percents": False,
                            "show_nodes_names": True,
                            "show_all_edges_for_targets": True,
                            "show_nodes_without_links": False,
                        },
                        "preprocessing_graph": {
                            "width": 960,
                            "height": 900,
                        },
                    },
                    f,
                )
        else:
            with open("retentioneering_config.json", "r") as f:
                config = json.load(f)
                self.tracking = TrackingConfig(**config["tracking"])
                self.transition_graph = TransitionGraphConfig(**config["transition_graph"])
                self.preprocessing_graph = PreprocessiongGraphConfig(**config["preprocessing_graph"])


RETE_CONFIG = Config()
