import { useState, useEffect } from "react";
import { GraphLayoutResponse } from "../../../types";
import { WidgetHost } from "../../../WidgetHost";

export function useGraphLayout(host: WidgetHost | null) {
  const [data, setData] = useState<GraphLayoutResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!host) return;
    setIsLoading(true);
    host
      .compute<GraphLayoutResponse>("graph_layout", {
        sample_size: 1000,
        walk_length: 20,
        embedding_dim: 32,
        n_clusters: 5,
        random_state: 42,
        min_trajectory_length: 3,
        use_original_trajectories: true,
      })
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setIsLoading(false));
  // Run only on mount (host reference is stable per widget instance)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return { data, isLoading };
}
