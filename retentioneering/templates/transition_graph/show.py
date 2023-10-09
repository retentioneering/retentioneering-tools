from __future__ import annotations

from jinja2 import Environment, PackageLoader, Template


class TransitionGraphRenderer:
    __template: Template
    __environment: Environment

    def __init__(self) -> None:
        self.__environment = Environment(
            loader=PackageLoader(package_name="retentioneering", package_path="templates"),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )

        self.__template = self.__environment.get_template("transition_graph/template.html")

    def show(self, widget_id: str, script_url: str, style: str, state: dict | str) -> str:
        return self.__template.render(state=state, id=widget_id, script_url=script_url, style=style)
