"""Self-contained synthetic checkout comparison from the README.
Run: python examples/readme_quickstart.py
Open checkout.html in your browser after the script finishes.
"""

import pandas as pd
import retentioneering as rete

paths = {
    "A1": "checkout shipping purchase",
    "A2": "checkout shipping purchase",
    "A3": "checkout shipping",
    "B1": "checkout shipping address_error cart shipping purchase",
    "B2": "checkout shipping address_error cart shipping purchase",
    "B3": "checkout shipping address_error cart shipping",
}
events = pd.DataFrame(
    [
        {
            "user_id": user,
            "event": event,
            "variant": user[0],
            "timestamp": pd.Timestamp("2026-01-01") + pd.Timedelta(seconds=step),
        }
        for user, path in paths.items()
        for step, event in enumerate(path.split())
    ]
)

stream = rete.Eventstream(events, schema={"segment_cols": ["variant"]})
graph = stream.transition_graph(diff=("variant", "B", "A"), edge_weight="unique_paths")
graph.export_html("checkout.html")
graph  # Displays the interactive graph in a notebook

print("Open checkout.html in your browser.")
