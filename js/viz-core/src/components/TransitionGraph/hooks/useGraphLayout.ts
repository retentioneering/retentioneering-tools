import { useState, useEffect } from "react";
import { DataProvider, GraphLayoutResponse } from "../../../types";

export function useGraphLayout(dataProvider: DataProvider | null) {
  const [data, setData] = useState<GraphLayoutResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!dataProvider) return;
    setIsLoading(true);
    dataProvider
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
  // Run only on mount (dataProvider reference is stable per widget instance)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return { data, isLoading };
}
