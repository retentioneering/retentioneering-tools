"""Make one self-contained HTML file of the rendered deck, pen included.

Quarto's own `embed-resources` refuses the chalkboard plugin (the pen), so the
deck is rendered as usual (slides.html + _build_slides_files/) and this script
inlines what the page loads from disk: scripts, stylesheets (with the fonts
and images their url()s point to) and images. The chalkboard plugin builds its
cursor and sponge image paths at run time from its own location; those become
data URIs too.

    python bundle.py [slides.html] [export/workshop.html]
"""

from __future__ import annotations

import base64
import mimetypes
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
LOCAL = re.compile(r"^(?!https?:|data:|//|#)")


def data_uri(path: Path) -> str:
    kind = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return f"data:{kind};base64,{base64.b64encode(path.read_bytes()).decode()}"


def inline_css(css: str, base: Path) -> str:
    """A stylesheet made self-contained: its @imports inlined (recursively),
    its url(...)s, relative to the stylesheet, as data URIs."""

    def imported(m: re.Match) -> str:
        target = (base / m.group(2)).resolve()
        if not LOCAL.match(m.group(2)) or not target.is_file():
            return m.group(0)
        return inline_css(target.read_text(encoding="utf-8"), target.parent)

    css = re.sub(r"@import\s*(?:url\()?\s*(['\"])([^'\"]+)\1\s*\)?\s*;", imported, css)

    def repl(m: re.Match) -> str:
        ref = m.group(2).split("?")[0].split("#")[0]
        target = (base / ref).resolve()
        if not LOCAL.match(m.group(2)) or not target.is_file():
            return m.group(0)
        return f"url({data_uri(target)})"

    return re.sub(r"url\((['\"]?)([^'\")]+)\1\)", repl, css)


def chalkboard_images(js: str, base: Path) -> str:
    """`path + 'img/x.png'` (the plugin's own folder at run time) as data URIs."""

    def repl(m: re.Match) -> str:
        target = base / m.group(1)
        return f"'{data_uri(target)}{m.group(2)}'" if target.is_file() else m.group(0)

    # e.g. 'url(' + path + 'img/boardmarker-black.png), auto'
    return re.sub(r"path \+ '(img/[^')]+\.png)([^']*)'", repl, js)


def bundle(page: Path, out: Path) -> None:
    root = page.parent
    html = page.read_text(encoding="utf-8")

    def script(m: re.Match) -> str:
        attrs, src = m.group(1) + m.group(3), m.group(2)
        path = (root / src).resolve()
        if not LOCAL.match(src) or not path.is_file():
            return m.group(0)
        js = path.read_text(encoding="utf-8")
        if "RevealChalkboard" in js:
            js = chalkboard_images(js, path.parent)
        js = js.replace("</script", "<\\/script")
        return f"<script{attrs}>{js}</script>"

    html = re.sub(r'<script([^>]*?)\ssrc="([^"]+)"([^>]*)>\s*</script>', script, html)

    def stylesheet(m: re.Match) -> str:
        href = m.group(1)
        path = (root / href).resolve()
        if not LOCAL.match(href) or not path.is_file():
            return m.group(0)
        return f"<style>{inline_css(path.read_text(encoding='utf-8'), path.parent)}</style>"

    html = re.sub(
        r'<link[^>]*?rel="stylesheet"[^>]*?href="([^"]+)"[^>]*>', stylesheet, html
    )
    html = re.sub(
        r'<link[^>]*?href="([^"]+)"[^>]*?rel="stylesheet"[^>]*>', stylesheet, html
    )

    def image(m: re.Match) -> str:
        path = (root / m.group(2)).resolve()
        if not LOCAL.match(m.group(2)) or not path.is_file():
            return m.group(0)
        return f'{m.group(1)}"{data_uri(path)}"'

    html = re.sub(r'(<img[^>]*?\ssrc=)"([^"]+)"', image, html)

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    left = sorted(set(re.findall(r'(?:src|href)="(_build_slides_files/[^"]+)"', html)))
    print(
        f"{out} ({out.stat().st_size / 1e6:.1f} MB)"
        + (f"; still on disk: {left}" if left else "")
    )


if __name__ == "__main__":
    page = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "slides.html"
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else HERE / "export" / "workshop.html"
    bundle(page, out)
