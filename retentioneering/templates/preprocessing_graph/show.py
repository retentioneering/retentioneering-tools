import uuid

from jinja2 import Environment, FileSystemLoader, PackageLoader, Template


class PreprocessingGraphRenderer:
    __template: Template
    __environment: Environment

    def __init__(self) -> None:
        self.__environment = Environment(
            loader=PackageLoader(package_name="retentioneering", package_path="templates/preprocessing_graph"),
        )
        self.__template = self.__environment.get_template("preprocessing_graph.html")

    def show(self, server_id: str, env: str, width: int, height: int) -> str:
        return self.__template.render(
            server_id=server_id,
            env=env,
            block_id=str(uuid.uuid4()),
            width=width,
            height=height,
        )
