import json
import os

import pytest

from retentioneering.exceptions.server import ServerErrorWithResponse
from tests.preprocessing_graph.fixtures.import_export import (
    config_file,
    graph_with_source,
    incorrect_data_with_cycle,
    not_full_graph_data,
    simple_graph,
    simple_graph_data,
)


class TestPreprocessingGraphJSONImportAndExport:
    def test_simple_graph_export(self, config_file, simple_graph, simple_graph_data):
        simple_graph.export_to_file(config_file.name)
        assert os.path.exists(config_file.name)

        with open(config_file.name, "r") as f:
            graph_json = json.load(f)
            assert simple_graph_data == graph_json

    def test_simple_graph_import(self, config_file, graph_with_source, simple_graph_data):
        with open(config_file.name, "w") as f:
            json.dump(simple_graph_data, f)

        graph = graph_with_source
        graph.import_from_file(config_file.name)
        assert graph.root.pk == simple_graph_data["nodes"][0]["pk"]

    def test_error_import_graph(self, config_file, graph_with_source, incorrect_data_with_cycle):
        # @TODO: make this test fail (currently it is not working). Vladimir Makhanov
        with open(config_file.name, "w") as f:
            json.dump(incorrect_data_with_cycle, f)

        graph = graph_with_source
        graph.import_from_file(config_file.name)

    def test_error_not_all_required_fields(self, config_file, graph_with_source, not_full_graph_data):
        with open(config_file.name, "w") as f:
            json.dump(not_full_graph_data, f)

        with pytest.raises(ServerErrorWithResponse):
            graph = graph_with_source
            graph.import_from_file(config_file.name)
