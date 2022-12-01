from typing import Any

from jinja2 import Environment, FileSystemLoader, Template

from docs.source.conf import extensions


class TransitionGraphRenderer:
    __template: Template
    __environment: Environment

    def __init__(self) -> None:
        # little workaround for notebooks. @TODO: think how to avoid that. Vladimir Makhanov
        import sys

        if any("retentioneering-tools-new-arch/examples" in x for x in sys.path):
            self.__environment = Environment(
                loader=FileSystemLoader("../src/templates/translition_graph"),
                autoescape=False,
                trim_blocks=True,
                lstrip_blocks=True,
            )
        else:
            self.__environment = Environment(
                loader=FileSystemLoader("src/templates/translition_graph"),
                autoescape=False,
                trim_blocks=True,
                lstrip_blocks=True,
            )

        self.__body_template = self.__environment.get_template("body.html")
        self.__full = self.__environment.get_template("full.html")
        self.__init = self.__environment.get_template("init.html")
        self.__inner_iframe = self.__environment.get_template("inner_iframe.html")
        self.__all_in_one = self.__environment.get_template("all_in_one.html")

    def body(self, **kwargs: Any) -> str:
        return self.__body_template.render(**kwargs)

    def all_in_one(self, **kwargs: Any) -> str:
        return self.__all_in_one.render(**kwargs)

    def full(self, **kwargs: Any) -> str:
        return self.__full.render(**kwargs)

    def init(self, **kwargs: Any) -> str:
        return self.__init.render(**kwargs)

    def inner_iframe(self, **kwargs: Any) -> str:
        return self.__inner_iframe.render(**kwargs)

    def init_code(self, **kwargs: Any) -> str:
        return """
            initialize({{
                serverId: \\\"{server_id}\\\",
                env: \\\"{env}\\\",
                configNodes: \\\"{nodes}\\\",
                configLinks: \\\"{links}\\\",
                nodesColsNames: \\\"{node_cols_names}\\\",
                linksWeightsNames: \\\"{links_weights_names}\\\",
                nodesThreshold: \\\"{nodes_threshold}\\\",
                linksThreshold: \\\"{links_threshold}\\\",
                showWeights: \\\"{show_weights}\\\",
                showPercents: \\\"{show_percents}\\\",
                showNodesNames: \\\"{show_nodes_names}\\\",
                showAllEdgesForTargets: \\\"{show_all_edges_for_targets}\\\",
                showNodesWithoutLinks: \\\"{show_nodes_without_links}\\\",
                useLayoutDump: \\\"Boolean({layout_dump})\\\",
                weightTemplate: \\\"{weight_template}\\\"
            }})
        """.format(
            **kwargs
        )

    def graph_stype(self) -> str:
        from .graph_style import style

        return style
