from jinja2 import Environment, FileSystemLoader, Template


class PGraphRenderer:
    __template: Template
    __environment: Environment

    def __init__(self) -> None:
        self.__environment = Environment(loader=FileSystemLoader("./src/templates/"))
        self.__template = self.__environment.get_template("p_graph/p_graph.html")

    def show(self, server_id: str, env: str) -> str:
        return self.__template.render(server_id=server_id, env=env)
