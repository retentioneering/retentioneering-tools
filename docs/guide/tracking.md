# Tracking

retentioneering collects anonymous usage analytics to help improve the library. This page explains exactly what is tracked and how to opt out.

## What we track

We track **method calls and widget actions only**:

- Eventstream creation — dataset shape (number of rows and columns)
- Data processor calls — which processors are used (e.g. `filter_events`, `collapse_events`)
- Widget and headless method calls — which visualizations are used

Every event includes anonymous device metadata: OS, Python version, library version, and runtime environment (Jupyter, VS Code, Google Colab, or script).

## What we do not track

We **never** collect any sensitive data from your eventstream — no event names, no user identifiers, no path contents, no segment values, and no business metrics. Your data stays entirely on your machine.

## Opting out

Set the environment variable `RETENTIONEERING_NO_TRACK=1` before starting your notebook kernel:

```bash
# In your shell profile (.zshrc, .bashrc)
export RETENTIONEERING_NO_TRACK=1

# Or at the top of a notebook cell
import os
os.environ["RETENTIONEERING_NO_TRACK"] = "1"
```

### Google Colab

Add a secret named `RETENTIONEERING_NO_TRACK` with value `1` in Colab → Settings → Secrets, then enable notebook access for it. The secret persists across all Colab sessions automatically.
