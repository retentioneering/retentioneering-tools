#!/usr/bin/env bash
# Render slides.ipynb into revealjs slides (slides.html).
#
#   ./render.sh            render once
#   ./render.sh preview    live preview, re-renders whenever slides.ipynb,
#                          deck.html, custom.scss or toy.py is saved
#
# Quarto writes the executed notebook back into the file it renders: every
# static widget carries the whole JS bundle (~1.5 MB each), and an edit saved
# while a render is running gets overwritten by that write-back. So Quarto
# never touches slides.ipynb: it renders a copy, _build_slides.ipynb, and in
# preview mode the copy is refreshed (which makes Quarto re-render) each time
# any of WATCHED changes. Quarto itself doesn't notice edits to the include
# (deck.html), the theme (custom.scss) or the helper module (toy.py).
set -euo pipefail
cd "$(dirname "$0")"

QUARTO="${QUARTO:-$(command -v quarto || echo /Applications/quarto/bin/quarto)}"
export QUARTO_PYTHON="${QUARTO_PYTHON:-../../.venv/bin/python}"
BUILD=_build_slides.ipynb
WATCHED=(slides.ipynb deck.html custom.scss toy.py)

# Copy via a temp file + rename, so the preview never sees a half-written copy.
sync_copy() {
    cp slides.ipynb "$BUILD.tmp"
    mv "$BUILD.tmp" "$BUILD"
}

# Modification times of every watched file, as one string to compare.
mtime() {
    stat -f %m "${WATCHED[@]}" 2>/dev/null || stat -c %Y "${WATCHED[@]}"
}

sync_copy

if [[ "${1:-}" == "preview" ]]; then
    shift
    # A fresh kernel per render: Quarto's kept-alive kernel would keep the old,
    # already imported toy.py.
    "$QUARTO" preview "$BUILD" --output slides.html --no-execute-daemon "$@" &
    PREVIEW=$!
    # quarto is a wrapper script: stop its server child too, then clean up.
    stop_preview() {
        pkill -TERM -P $PREVIEW 2>/dev/null || true
        kill $PREVIEW 2>/dev/null || true
        wait $PREVIEW 2>/dev/null || true
        rm -f "$BUILD" "$BUILD.tmp"
    }
    trap stop_preview EXIT
    last=$(mtime | tr '\n' ' ')
    while kill -0 $PREVIEW 2>/dev/null; do
        sleep 1
        now=$(mtime | tr '\n' ' ')
        if [[ "$now" != "$last" ]]; then
            last=$now
            sync_copy
        fi
    done
else
    trap 'rm -f "$BUILD" "$BUILD.tmp"' EXIT
    "$QUARTO" render "$BUILD" --output slides.html "$@"
fi
