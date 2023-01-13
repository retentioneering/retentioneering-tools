from jinja2 import Environment, FileSystemLoader, PackageLoader, Template


class PGraphRenderer:
    __template: Template
    __environment: Environment

    def __init__(self) -> None:
        self.__environment = Environment(
            loader=PackageLoader(package_name="retentioneering", package_path="templates/p_graph"),
        )
        self.__template = self.__environment.get_template("p_graph.html")

    def show(self, server_id: str, env: str) -> str:
        template: str = self.__template.render(server_id=server_id, env=env)
        return template
