import uuid

from retentioneering.templates import PreprocessingGraphRenderer


class TestPreprocessingGraphTemplate:
    def test_render_html(self) -> None:
        import sys

        sys.path.insert(0, "..")
        server_id: str = str(uuid.uuid4())
        render = PreprocessingGraphRenderer()
        html = render.show(
            server_id=server_id,
            env="classic",
            width=900,
            height=600,
            graph_url="",
            graph_style_url="",
            tracking_hardware_id="",
            tracking_eventstream_index=-1,
            tracking_scope="",
        )
        assert "div" in html
        assert server_id in html
        assert "classic" in html
