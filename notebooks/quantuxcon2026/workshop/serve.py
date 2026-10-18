"""Serve the rendered slides locally for `./render.sh preview`.

A plain static file server for this folder, plus one endpoint, `/__mtime`,
with the modification time of slides.html. deck.html polls it when the page is
served from localhost and reloads the page after each render (the slide stays
the same: Reveal keeps it in the URL hash).

    python serve.py [port]
"""

from __future__ import annotations

import json
import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
SLIDES = HERE / "slides.html"


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?")[0] == "/__mtime":
            mtime = SLIDES.stat().st_mtime if SLIDES.exists() else 0
            body = json.dumps({"mtime": mtime}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path in ("/", "/index.html"):
            self.path = "/slides.html"
        super().do_GET()

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, format, *args):  # keep the terminal for render logs
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 4400
    server = ThreadingHTTPServer(
        ("127.0.0.1", port), partial(Handler, directory=str(HERE))
    )
    print(f"Slides at http://localhost:{port}/")
    server.serve_forever()
