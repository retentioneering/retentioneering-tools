"""
ClusterAnalysis - tool for one-shot clustering with segment overview.

Combines add_clusters (clustering) with segment_overview (aggregated metrics)
without saving intermediate eventstream. Supports:
- Parameter search: silhouette score over any combination of list-valued params
  (nmf_k, n_clusters, min_cluster_size, cluster_selection_epsilon).
- NMF: returns H matrix from NMF decomposition alongside results.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Literal

import numpy as np
import pandas as pd
from sklearn.cluster import HDBSCAN, KMeans
from sklearn.decomposition import NMF
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import MinMaxScaler, StandardScaler

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream


from retentioneering.metrics.metric_builder import MetricBuilder

SEGMENT_COL = "__cluster__"
SILHOUETTE_SAMPLE_SIZE = 2_000

T_ClusteringMethod = Literal["kmeans", "hdbscan"]
T_Scaler = Literal["minmax", "std"] | None


@dataclass
class ClusterAnalysis:
    eventstream: "Eventstream"

    def fit(
        self,
        features_config: List[Dict[str, Any]],
        method: T_ClusteringMethod = "kmeans",
        scaler: T_Scaler = "minmax",
        n_clusters: int | List[int] | None = None,
        min_cluster_size: int | List[int] | None = None,
        cluster_selection_epsilon: float | List[float] | None = None,
        nmf_k: int | List[int] | None = None,
        metrics_config: List[Dict[str, Any]] | None = None,
        path_id_col: str | None = None,
        event_col: str | None = None,
    ) -> Dict[str, Any]:
        path_id_col = path_id_col or self.eventstream.schema.path_col
        event_col = event_col or self.eventstream.schema.event_col

        # 1. Build feature matrix
        metric_builder = MetricBuilder(self.eventstream)
        metrics_df = metric_builder.build_metrics(features_config, path_id_col)
        features = metrics_df.fillna(0).values
        feature_names = metrics_df.columns.tolist()

        # 2. Scale
        features_scaled = self._scale_features(features, scaler)

        # 3. Search mode — any list-valued parameter triggers silhouette grid search
        is_search = (
            isinstance(nmf_k, (list, tuple))
            or isinstance(n_clusters, (list, tuple))
            or isinstance(min_cluster_size, (list, tuple))
            or isinstance(cluster_selection_epsilon, (list, tuple))
        )

        if is_search:
            search_data = self._search(
                features_scaled,
                feature_names,
                method,
                nmf_k,
                n_clusters,
                min_cluster_size,
                cluster_selection_epsilon,
            )
            result: Dict[str, Any] = {
                "silhouette": {
                    "params": search_data["params"],
                    "silhouette": search_data["silhouette"],
                }
            }

            best = search_data.get("best")
            if best is not None:
                result["overview_df"] = self._build_overview(
                    best["labels"],
                    metrics_df.index,
                    path_id_col,
                    event_col,
                    metrics_config or [],
                )
                if best.get("nmf_data") is not None:
                    nmf_data = best["nmf_data"]
                    nmf_data["W_cluster_means"] = self._compute_w_cluster_means(
                        best["W"], best["labels"]
                    )
                    result["nmf"] = nmf_data

            return result

        # 4. Normal mode — single NMF + cluster + overview
        nmf_data: Dict[str, Any] | None = None
        if nmf_k is not None:
            nmf_model = NMF(n_components=nmf_k, random_state=42)
            features_scaled = nmf_model.fit_transform(features_scaled)
            nmf_data = {
                "H_matrix": nmf_model.components_.tolist(),
                "features": feature_names,
            }

        cluster_labels = self._cluster(
            features_scaled,
            method,
            n_clusters,
            min_cluster_size,
            cluster_selection_epsilon,
        )

        if nmf_data is not None:
            nmf_data["W_cluster_means"] = self._compute_w_cluster_means(
                features_scaled, cluster_labels
            )

        overview_df = self._build_overview(
            cluster_labels,
            metrics_df.index,
            path_id_col,
            event_col,
            metrics_config or [],
        )

        return {
            "overview_df": overview_df,
            "nmf": nmf_data,
        }

    # ------------------------------------------------------------------

    def _scale_features(self, features: np.ndarray, scaler: T_Scaler) -> np.ndarray:
        if scaler is None:
            return features
        elif scaler == "minmax":
            return MinMaxScaler().fit_transform(features)
        elif scaler == "std":
            return StandardScaler().fit_transform(features)
        else:
            raise ValueError(f"Unknown scaler: {scaler}")

    def _cluster(
        self,
        features: np.ndarray,
        method: T_ClusteringMethod,
        n_clusters: int | None,
        min_cluster_size: int | None,
        cluster_selection_epsilon: float | None,
    ) -> np.ndarray:
        if method == "kmeans":
            return KMeans(
                n_clusters=n_clusters, random_state=42, n_init="auto"
            ).fit_predict(features)
        elif method == "hdbscan":
            return HDBSCAN(
                min_cluster_size=min_cluster_size or 5,
                cluster_selection_epsilon=cluster_selection_epsilon or 0.0,
                copy=True,
            ).fit_predict(features)
        else:
            raise ValueError(f"Unknown clustering method: {method}")

    @staticmethod
    def _safe_silhouette(features: np.ndarray, labels: np.ndarray) -> float | None:
        """Compute silhouette score, filtering noise (label=-1). Returns None if < 2 clusters."""
        mask = labels >= 0
        unique_labels = set(labels[mask])
        if len(unique_labels) < 2:
            return None
        sample_size = min(SILHOUETTE_SAMPLE_SIZE, mask.sum())
        return float(
            silhouette_score(
                features[mask], labels[mask], sample_size=sample_size, random_state=42
            )
        )

    def _search(
        self,
        features_scaled: np.ndarray,
        feature_names: List[str],
        method: T_ClusteringMethod,
        nmf_k: int | List[int] | None,
        n_clusters: int | List[int] | None,
        min_cluster_size: int | List[int] | None,
        cluster_selection_epsilon: float | List[float] | None,
    ) -> Dict[str, Any]:
        nmf_k_values = nmf_k if isinstance(nmf_k, (list, tuple)) else [nmf_k]
        is_nmf_search = isinstance(nmf_k, (list, tuple))

        params: List[Dict[str, Any]] = []
        scores: List[float | None] = []

        best_score: float = -2.0
        best_labels: np.ndarray | None = None
        best_nmf_data: Dict[str, Any] | None = None
        best_W: np.ndarray | None = None

        for nk in nmf_k_values:
            nmf_data: Dict[str, Any] | None = None
            if nk is not None:
                nmf_model = NMF(n_components=nk, random_state=42)
                X = nmf_model.fit_transform(features_scaled)
                nmf_data = {
                    "H_matrix": nmf_model.components_.tolist(),
                    "features": feature_names,
                }
            else:
                X = features_scaled

            if method == "kmeans":
                nc_values = (
                    n_clusters
                    if isinstance(n_clusters, (list, tuple))
                    else [n_clusters]
                )
                for nc in nc_values:
                    labels = KMeans(
                        n_clusters=nc, random_state=42, n_init="auto"
                    ).fit_predict(X)
                    score = self._safe_silhouette(X, labels)
                    p = {"n_clusters": nc}
                    if is_nmf_search:
                        p["nmf_k"] = nk
                    params.append(p)
                    scores.append(score)

                    if score is not None and score > best_score:
                        best_score = score
                        best_labels = labels
                        best_nmf_data = nmf_data
                        best_W = X if nk is not None else None

            elif method == "hdbscan":
                mcs_values = (
                    min_cluster_size
                    if isinstance(min_cluster_size, (list, tuple))
                    else [min_cluster_size or 5]
                )
                eps_values = (
                    cluster_selection_epsilon
                    if isinstance(cluster_selection_epsilon, (list, tuple))
                    else [cluster_selection_epsilon or 0.0]
                )
                for mcs in mcs_values:
                    for eps in eps_values:
                        labels = HDBSCAN(
                            min_cluster_size=mcs,
                            cluster_selection_epsilon=eps,
                        ).fit_predict(X)
                        score = self._safe_silhouette(X, labels)
                        p = {"min_cluster_size": mcs, "cluster_selection_epsilon": eps}
                        if is_nmf_search:
                            p["nmf_k"] = nk
                        params.append(p)
                        scores.append(score)

                        if score is not None and score > best_score:
                            best_score = score
                            best_labels = labels
                            best_nmf_data = nmf_data
                            best_W = X if nk is not None else None

        result: Dict[str, Any] = {"params": params, "silhouette": scores}
        if best_labels is not None:
            result["best"] = {
                "labels": best_labels,
                "nmf_data": best_nmf_data,
                "W": best_W,
            }
        return result

    @staticmethod
    def _compute_w_cluster_means(
        W: np.ndarray, labels: np.ndarray
    ) -> Dict[str, List[float]]:
        """Compute mean W coordinates per cluster. Returns {cluster_name: [mean_w1, mean_w2, ...]}."""
        result: Dict[str, List[float]] = {}
        for label in sorted(set(labels)):
            name = f"cluster_{label}" if label >= 0 else "noise"
            result[name] = W[labels == label].mean(axis=0).tolist()
        return result

    def _build_overview(
        self,
        cluster_labels: np.ndarray,
        path_index: pd.Index,
        path_id_col: str,
        event_col: str,
        metrics_config: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        from retentioneering.eventstream.eventstream import Eventstream

        cluster_series = pd.Series(cluster_labels, index=path_index)
        cluster_series = cluster_series.apply(
            lambda x: f"cluster_{x}" if x >= 0 else "noise"
        )

        df = self.eventstream.df.copy()
        df[SEGMENT_COL] = df[path_id_col].map(cluster_series).astype("category")

        schema_dict = dict(self.eventstream._schema) if self.eventstream._schema else {}
        segment_cols = list(schema_dict.get("segment_cols", []))
        segment_cols.append(SEGMENT_COL)
        schema_dict["segment_cols"] = segment_cols

        temp_stream = Eventstream(df, schema_dict, prepare=False)

        return temp_stream.segment_overview_data(
            segment_col=SEGMENT_COL,
            metrics_config=metrics_config,
            path_id_col=path_id_col,
            event_col=event_col,
        )
