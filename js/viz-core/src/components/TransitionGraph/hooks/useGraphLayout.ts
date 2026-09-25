import { useState, useEffect } from "react";
import { GraphLayoutResponse } from "../../../types";
import { WidgetHost } from "../../../WidgetHost";

/** The layout the backend shipped with the initial state (the `graph_layout`
 *  traitlet), or null when it didn't — an older backend, or a host that
 *  never sets it. */
function readShippedLayout(host: WidgetHost | null): GraphLayoutResponse | null {
  const raw = host?.get("graph_layout");
  if (typeof raw !== "string" || !raw) return null;
  try {
    const parsed = JSON.parse(raw) as GraphLayoutResponse;
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

export function useGraphLayout(host: WidgetHost | null) {
  // Prefer the layout that arrived with the widget's state: asking the
  // kernel for it on mount means waiting behind every cell queued ahead of
  // us, so under "Run all" the graph stayed on its spinner until the whole
  // notebook had finished.
  const [shipped] = useState<GraphLayoutResponse | null>(() => readShippedLayout(host));
  const [data, setData] = useState<GraphLayoutResponse | null>(shipped);
  // With a host present and nothing shipped, a compute ALWAYS fires on
  // mount, so loading must be true from the very first render. Initializing
  // it to false and flipping it inside the effect is too late: the
  // graph-build effect of the same commit still sees the stale false,
  // records fallback positions as permanent, and the arriving layout is then
  // silently discarded.
  const [isLoading, setIsLoading] = useState<boolean>(() => !!host && !shipped);

  useEffect(() => {
    if (!host) return;
    if (shipped) {
      if (shipped.error) {
        // Non-fatal: the graph falls back to its own deterministic layout.
        console.warn("graph_layout compute failed:", shipped.error);
      }
      return;
    }
    setIsLoading(true);
    host
      .compute<GraphLayoutResponse>("graph_layout", {})
      .then((response) => {
        if (response?.error) {
          console.warn("graph_layout compute failed:", response.error);
        }
        setData(response);
      })
      .catch(() => setData(null))
      .finally(() => setIsLoading(false));
  // Run only on mount (host reference is stable per widget instance).
  // Note: a later path_col change does not recompute the layout — positions
  // are persisted per widget anyway; Reset layout picks up the current one.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return { data, isLoading };
}
