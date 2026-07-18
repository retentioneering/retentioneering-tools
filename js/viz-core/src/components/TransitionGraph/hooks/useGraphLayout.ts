import { useState, useEffect } from "react";
import { GraphLayoutResponse } from "../../../types";
import { WidgetHost } from "../../../WidgetHost";

export function useGraphLayout(host: WidgetHost | null) {
  const [data, setData] = useState<GraphLayoutResponse | null>(null);
  // With a host present a compute ALWAYS fires on mount, so loading must be
  // true from the very first render. Initializing it to false and flipping
  // it inside the effect is too late: the graph-build effect of the same
  // commit still sees the stale false, records fallback positions as
  // permanent, and the arriving layout is then silently discarded.
  const [isLoading, setIsLoading] = useState<boolean>(() => !!host);

  useEffect(() => {
    if (!host) return;
    setIsLoading(true);
    host
      .compute<GraphLayoutResponse>("graph_layout", {})
      .then((response) => {
        if (response?.error) {
          // Non-fatal: the graph falls back to its own deterministic layout.
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
