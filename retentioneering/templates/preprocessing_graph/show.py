import uuid
from typing import Optional

from jinja2 import Environment, FileSystemLoader, PackageLoader, Template


class PreprocessingGraphRenderer:
    __template: Template
    __environment: Environment

    def __init__(self) -> None:
        self.__environment = Environment(
            loader=PackageLoader(package_name="retentioneering", package_path="templates"),
        )
        self.__template = self.__environment.get_template("preprocessing_graph/template.html")

    def show(
        self,
        server_id: str,
        env: str,
        width: int,
        height: int,
        graph_url: str,
        graph_style_url: str,
        tracking_hardware_id: str,
        tracking_eventstream_index: int,
        tracking_scope: str,
        kernel_id: Optional[str] = None,
    ) -> str:
        return self.__template.render(
            server_id=server_id,
            env=env,
            block_id=str(uuid.uuid4()),
            width=width,
            height=height,
            graph_url=graph_url,
            graph_style_url=graph_style_url,
            tracking_hardware_id=tracking_hardware_id,
            kernel_id=kernel_id,
            tracking_eventstream_index=tracking_eventstream_index,
            tracking_scope=tracking_scope,
        )
