import uuid

from jinja2 import Environment, FileSystemLoader, Template


class PGraphRenderer:
    __template: Template
    __environment: Environment

    def __init__(self) -> None:
        # little workaround for notebooks. @TODO: think how to avoid that. Vladimir Makhanov
        import sys

        if any("retentioneering-tools-new-arch/examples" in x for x in sys.path):
            self.__environment = Environment(loader=FileSystemLoader("../src/templates"))
        else:
            self.__environment = Environment(loader=FileSystemLoader("src/templates"))

        self.__template = self.__environment.get_template("p_graph/p_graph.html")

    def show(self, server_id: str, env: str, width: int, height: int) -> str:
        return self.__template.render(
            server_id=server_id,
            env=env,
            block_id=str(uuid.uuid4()),
            width=width,
            height=height,
        )
