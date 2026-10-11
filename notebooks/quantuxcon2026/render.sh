#!/usr/bin/env bash
# Render slides.ipynb into revealjs slides (slides.html).
#
#   ./render.sh            render once
#   ./render.sh preview    live preview, re-renders whenever slides.ipynb is saved
#
# Quarto writes the executed notebook back into the file it renders: every
# static widget carries the whole JS bundle (~1.5 MB each), and an edit saved
# while a render is running gets overwritten by that write-back. So Quarto
# never touches slides.ipynb: it renders a copy, _build_slides.ipynb, and in
# preview mode the copy is refreshed each time slides.ipynb changes.
set -euo pipefail
cd "$(dirname "$0")"

QUARTO="${QUARTO:-$(command -v quarto || echo /Applications/quarto/bin/quarto)}"
export QUARTO_PYTHON="${QUARTO_PYTHON:-../../.venv/bin/python}"
BUILD=_build_slides.ipynb

# Copy via a temp file + rename, so the preview never sees a half-written copy.
sync_copy() {
    cp slides.ipynb "$BUILD.tmp"
    mv "$BUILD.tmp" "$BUILD"
}

mtime() {
    stat -f %m slides.ipynb 2>/dev/null || stat -c %Y slides.ipynb
}

sync_copy

if [[ "${1:-}" == "preview" ]]; then
    shift
    "$QUARTO" preview "$BUILD" --output slides.html "$@" &
    PREVIEW=$!
    # quarto is a wrapper script: stop its server child too, then clean up.
    stop_preview() {
        pkill -TERM -P $PREVIEW 2>/dev/null || true
        kill $PREVIEW 2>/dev/null || true
        wait $PREVIEW 2>/dev/null || true
        rm -f "$BUILD" "$BUILD.tmp"
    }
    trap stop_preview EXIT
    last=$(mtime)
    while kill -0 $PREVIEW 2>/dev/null; do
        sleep 1
        now=$(mtime)
        if [[ "$now" != "$last" ]]; then
            last=$now
            sync_copy
        fi
    done
else
    trap 'rm -f "$BUILD" "$BUILD.tmp"' EXIT
    "$QUARTO" render "$BUILD" --output slides.html "$@"
fi
