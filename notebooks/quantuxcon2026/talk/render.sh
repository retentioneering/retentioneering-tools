#!/usr/bin/env bash
# Render slides.ipynb into revealjs slides (slides.html).
#
#   ./render.sh            render once
#   ./render.sh export     render once, then export the slides as still
#                          frames: export/slides.pdf and export/slides.html
#                          (export.py; needs uv, which fetches Playwright),
#                          and as one interactive file with the pen:
#                          export/where_ux_diverges.html (bundle.py)
#   ./render.sh preview    live preview at http://localhost:$PORT/ (4400 by
#                          default), re-renders whenever any of WATCHED is
#                          saved; the port stays the same between restarts, so
#                          an open browser tab just needs a reload
#
# Quarto writes the executed notebook back into the file it renders: every
# static widget carries the whole JS bundle (~1.5 MB each), and an edit saved
# while a render is running gets overwritten by that write-back. So Quarto
# never touches slides.ipynb: it renders a copy, _build_slides.ipynb, and in
# preview mode the copy is refreshed and re-rendered each time any of WATCHED
# changes (Quarto itself doesn't notice edits to the include, deck.html, the
# theme, custom.scss, or the helper module, talk.py).
set -euo pipefail
cd "$(dirname "$0")"

QUARTO="${QUARTO:-$(command -v quarto || echo /Applications/quarto/bin/quarto)}"
export QUARTO_PYTHON="${QUARTO_PYTHON:-../../../.venv/bin/python}"
BUILD=_build_slides.ipynb
WATCHED=(slides.ipynb deck.html custom.scss talk.py)

# Copy via a temp file + rename, so the preview never sees a half-written copy.
sync_copy() {
    cp slides.ipynb "$BUILD.tmp"
    mv "$BUILD.tmp" "$BUILD"
}

# Modification times of every watched file, as one string to compare.
mtime() {
    stat -f %m "${WATCHED[@]}" 2>/dev/null || stat -c %Y "${WATCHED[@]}"
}

# The event log is not tracked (13 MB): see README.md.
[[ -f data/lumeo_events.csv.gz ]] || { echo "render.sh: no data/lumeo_events.csv.gz, see README.md" >&2; exit 1; }

sync_copy

if [[ "${1:-}" == "preview" ]]; then
    shift
    # Quarto's own preview proved unreliable here (it sometimes misses the
    # refreshed copy, and renders on its own even with --no-render), so this
    # loop does the rendering and serve.py only serves slides.html; the page
    # reloads itself after each render (deck.html).
    # --no-execute-daemon: a fresh kernel per render, since a kept-alive one
    # would keep the old, already imported talk.py.
    "$QUARTO_PYTHON" serve.py "${PORT:-4400}" &
    PREVIEW=$!
    stop_preview() {
        kill $PREVIEW 2>/dev/null || true
        wait $PREVIEW 2>/dev/null || true
        rm -f "$BUILD" "$BUILD.tmp"
    }
    trap stop_preview EXIT
    last=""
    while kill -0 $PREVIEW 2>/dev/null; do
        sleep 1
        now=$(mtime | tr '\n' ' ')
        if [[ "$now" != "$last" ]]; then
            # Let a save finish (a notebook with outputs is tens of MB): copy
            # only once the files have stopped changing for two seconds.
            while sleep 2; settled=$(mtime | tr '\n' ' '); [[ "$settled" != "$now" ]]; do
                now=$settled
            done
            last=$now
            sync_copy
            echo "── $(date +%T) change detected, re-rendering…"
            "$QUARTO" render "$BUILD" --output slides.html --no-execute-daemon || true
            echo "── $(date +%T) done"
        fi
    done
elif [[ "${1:-}" == "export" ]]; then
    shift
    trap 'rm -f "$BUILD" "$BUILD.tmp"' EXIT
    "$QUARTO" render "$BUILD" --output slides.html "$@"
    uv run -q --project ../../.. --with playwright --with img2pdf python export.py
    # One self-contained HTML file, the pen included (bundle.py).
    "$QUARTO_PYTHON" bundle.py slides.html export/where_ux_diverges.html
    cp export/slides.pdf export/where_ux_diverges.pdf
else
    trap 'rm -f "$BUILD" "$BUILD.tmp"' EXIT
    "$QUARTO" render "$BUILD" --output slides.html "$@"
fi
