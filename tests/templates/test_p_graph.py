import uuid

from src.templates import PGraphRenderer


class TestPGraphTemplate:
    def test_render_html(self) -> None:
        import sys

        sys.path.insert(0, "..")
        server_id: str = str(uuid.uuid4())
        render = PGraphRenderer()
        html = render.show(server_id=server_id, env="classic")
        assert "div" in html
        assert server_id in html
        assert "classic" in html
