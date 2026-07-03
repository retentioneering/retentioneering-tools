"""
AddClusters - data processor for clustering trajectories based on features.

Clusters trajectories and adds a new segment column with cluster labels.
Uses MetricBuilder for feature calculation.
"""

from typing import Any, Dict, List, Literal, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import HDBSCAN, KMeans
from sklearn.decomposition import NMF
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from retentioneering.data_processors.data_processor import DataProcessor
from retentioneering.eventstream.schema import EventstreamSchema
from retentioneering.exceptions import PreprocessingConfigError
from retentioneering.metrics.metric_builder import MetricBuilder

PROCESSOR_NAME = "add_clusters"

T_ClusteringMethod = Literal["kmeans", "hdbscan"]
T_Scaler = Literal["minmax", "std"] | None


class AddClusters(DataProcessor):
    """
    Data processor that clusters trajectories based on computed features.

    Adds a new segment column containing cluster labels for each trajectory.

    Attributes:
        segment_name: Name of the new segment column
        features: List of metric configurations (MetricBuilder format)
        method: Clustering method ("kmeans" or "hdbscan")
        scaler: Feature scaler method ("minmax", "std", or None)
        method_params: Parameters for the clustering algorithm
        eventstream: Eventstream instance (needed for MetricBuilder)
        path_id_col: Path ID column name (optional)
        event_col: Event column name (optional)
    """

    segment_name: str
    features: List[Dict[str, Any]]
    method: T_ClusteringMethod
    scaler: T_Scaler
    method_params: Dict[str, Any]
    nmf_k: int | None
    eventstream: Any
    path_id_col: str | None
    event_col: str | None

    def __init__(
        self,
        eventstream: Any,
        segment_name: str,
        features: List[Dict[str, Any]],
        method: T_ClusteringMethod = "kmeans",
        scaler: T_Scaler = "minmax",
        n_clusters: int | None = None,
        min_cluster_size: int | None = None,
        cluster_selection_epsilon: float | None = None,
        nmf_k: int | None = None,
        path_id_col: str | None = None,
        event_col: str | None = None,
    ) -> None:
        """
        Initialize AddClusters processor.

        Args:
            eventstream: Eventstream instance for metric calculation
            segment_name: Name of the new segment column with cluster labels
            features: List of metric configurations for MetricBuilder.
                     Each config is a dict with 'metric' and optional 'metric_args'.
                     Example: [
                         {"metric": "length"},
                         {"metric": "duration"},
                         {"metric": "event_count", "metric_args": {"events": "purchase"}}
                     ]
            method: Clustering method - "kmeans" or "hdbscan"
            scaler: Feature scaler - "minmax", "std", or None.
                    Default is "minmax".
            n_clusters: Number of clusters for k-means (required for kmeans)
            min_cluster_size: Minimum cluster size for HDBSCAN
            cluster_selection_epsilon: Cluster selection epsilon for HDBSCAN
            path_id_col: Path ID column (if None, taken from schema)
            event_col: Event column (if None, taken from schema)
        """
        self.eventstream = eventstream
        self.segment_name = segment_name
        self.features = features
        self.method = method
        self.scaler = scaler
        self.nmf_k = nmf_k
        self.path_id_col = path_id_col
        self.event_col = event_col

        # Validate method and collect method-specific parameters
        if method == "kmeans":
            if n_clusters is None:
                raise PreprocessingConfigError(PROCESSOR_NAME, "n_clusters is required for kmeans method")
            self.method_params = {"n_clusters": n_clusters}
        elif method == "hdbscan":
            self.method_params = {}
            if min_cluster_size is not None:
                self.method_params["min_cluster_size"] = min_cluster_size
            if cluster_selection_epsilon is not None:
                self.method_params["cluster_selection_epsilon"] = cluster_selection_epsilon
        else:
            raise PreprocessingConfigError(PROCESSOR_NAME, f"Unknown clustering method: {method}. Use 'kmeans' or 'hdbscan'.")

        super().__init__()

    def apply(
        self, df: pd.DataFrame, schema: EventstreamSchema
    ) -> Tuple[pd.DataFrame, EventstreamSchema]:
        """
        Apply clustering to trajectories and add cluster labels as a new segment.

        Args:
            df: Input DataFrame with eventstream data
            schema: EventstreamSchema with column definitions

        Returns:
            Tuple of (new_df, new_schema) with added cluster segment
        """
        # Validate segment name doesn't exist
        if self.segment_name in df.columns:
            if self.segment_name in schema.segment_cols:
                raise PreprocessingConfigError(PROCESSOR_NAME, f"Segment '{self.segment_name}' already exists.")
            else:
                raise PreprocessingConfigError(
                    PROCESSOR_NAME,
                    f"Name '{self.segment_name}' is already reserved in the eventstream."
                )

        path_id_col = self.path_id_col or schema.path_col
        event_col = self.event_col or schema.event_col  # noqa: F841 - reserved for future use

        # Build features using MetricBuilder
        metric_builder = MetricBuilder(self.eventstream)
        metrics_df = metric_builder.build_metrics(self.features, path_id_col)

        if metrics_df.empty:
            raise PreprocessingConfigError(PROCESSOR_NAME, "No features were computed. Check metric configurations.")

        # Handle NaN values - fill with 0 for clustering
        features = metrics_df.fillna(0).values

        if features.shape[1] == 0:
            raise PreprocessingConfigError(PROCESSOR_NAME, "No feature columns were generated from features.")

        # Apply scaling
        features_scaled = self._scale_features(features)

        # Apply NMF dimensionality reduction if requested
        if self.nmf_k is not None:
            nmf = NMF(n_components=self.nmf_k, random_state=42)
            features_scaled = nmf.fit_transform(features_scaled)

        # Perform clustering
        cluster_labels = self._cluster(features_scaled)

        # Create cluster labels Series indexed by path_id
        cluster_series = pd.Series(
            cluster_labels,
            index=metrics_df.index,
            name=self.segment_name
        )

        # Convert labels to string for categorical representation
        # HDBSCAN uses -1 for noise points, handle specially
        cluster_series = cluster_series.apply(
            lambda x: f"cluster_{x}" if x >= 0 else "noise"
        )

        # Map cluster labels to all events in the dataframe
        new_df = df.copy()
        new_df[self.segment_name] = new_df[path_id_col].map(cluster_series)
        new_df[self.segment_name] = new_df[self.segment_name].astype("category")

        # Update schema
        new_schema = schema.copy()
        new_schema.segment_cols.append(self.segment_name)

        return new_df, new_schema

    def _scale_features(self, features: np.ndarray) -> np.ndarray:
        """
        Scale features based on the configured method.

        Args:
            features: Raw feature matrix

        Returns:
            Scaled feature matrix
        """
        if self.scaler is None:
            return features
        elif self.scaler == "minmax":
            scaler = MinMaxScaler()
            return scaler.fit_transform(features)
        elif self.scaler == "std":
            scaler = StandardScaler()
            return scaler.fit_transform(features)
        else:
            raise PreprocessingConfigError(PROCESSOR_NAME, f"Unknown scaler method: {self.scaler}")

    def _cluster(self, features: np.ndarray) -> np.ndarray:
        """
        Perform clustering using the configured method.

        Args:
            features: Standardized feature matrix

        Returns:
            Array of cluster labels
        """
        if self.method == "kmeans":
            clusterer = KMeans(
                n_clusters=self.method_params["n_clusters"],
                random_state=42,
                n_init="auto"
            )
            return clusterer.fit_predict(features)
        elif self.method == "hdbscan":
            # Set defaults for HDBSCAN if not provided
            min_cluster_size = self.method_params.get("min_cluster_size", 5)
            cluster_selection_epsilon = self.method_params.get("cluster_selection_epsilon", 0.0)

            clusterer = HDBSCAN(
                min_cluster_size=min_cluster_size,
                cluster_selection_epsilon=cluster_selection_epsilon
            )
            return clusterer.fit_predict(features)
        else:
            raise PreprocessingConfigError(PROCESSOR_NAME, f"Unknown clustering method: {self.method}")
