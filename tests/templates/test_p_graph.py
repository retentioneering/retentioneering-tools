from src.templates import PGraphRenderer


class TestPGraphTemplate:
    def test_render_html(self) -> None:
        render = PGraphRenderer()
        html = render.show()
        assert "div" in html
