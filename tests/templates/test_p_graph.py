import uuid

from retentioneering.templates import PreprocessingGraphRenderer


class TestPreprocessingGraphTemplate:
    def test_render_html(self) -> None:
        import sys

        sys.path.insert(0, "..")
        server_id: str = str(uuid.uuid4())
        render = PreprocessingGraphRenderer()
        html = render.show(server_id=server_id, env="classic", width=900, height=600)
        assert "div" in html
        assert server_id in html
        assert "classic" in html
