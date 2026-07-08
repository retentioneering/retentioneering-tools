"""
ClusterAnalysis - tool for one-shot clustering with segment overview.

Combines add_clusters (clustering) with segment_overview (aggregated metrics)
without saving intermediate eventstream. Supports:
- Parameter search: silhouette score over any combination of list-valued params
  (nmf_components, n_clusters, min_cluster_size, cluster_selection_epsilon).
  Note: n_clusters is always required for the kmeans method — an nmf_components-only
  search still needs a concrete n_clusters value (int or list of ints).
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
T_Scaler = Literal["minmax", "standard"] | None


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
        nmf_components: int | List[int] | None = None,
        overview_metrics: List[Dict[str, Any]] | None = None,
        path_col: str | None = None,
        event_col: str | None = None,
    ) -> Dict[str, Any]:
        if method == "kmeans":
            if isinstance(n_clusters, (list, tuple)):
                n_clusters_missing = len(n_clusters) == 0 or any(
                    nc is None for nc in n_clusters
                )
            else:
                n_clusters_missing = n_clusters is None
            if n_clusters_missing:
                raise ValueError("n_clusters is required for kmeans method")

        path_col = path_col or self.eventstream.schema.path_col
        event_col = event_col or self.eventstream.schema.event_col

        # 1. Build feature matrix
        metric_builder = MetricBuilder(self.eventstream)
        metrics_df = metric_builder.build_metrics(features_config, path_col)
        features = metrics_df.fillna(0).values
        feature_names = metrics_df.columns.tolist()

        # 2. Scale
        features_scaled = self._scale_features(features, scaler)

        # 3. Search mode — any list-valued parameter triggers silhouette grid search
        is_search = (
            isinstance(nmf_components, (list, tuple))
            or isinstance(n_clusters, (list, tuple))
            or isinstance(min_cluster_size, (list, tuple))
            or isinstance(cluster_selection_epsilon, (list, tuple))
        )

        if is_search:
            search_data = self._search(
                features_scaled,
                feature_names,
                method,
                nmf_components,
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
                overview_df, cluster_labels = self._build_overview(
                    best["labels"],
                    metrics_df.index,
                    path_col,
                    event_col,
                    overview_metrics or [],
                )
                result["overview_df"] = overview_df
                result["cluster_labels"] = cluster_labels
                result["best_params"] = best.get("params")
                if best.get("nmf_data") is not None:
                    nmf_data = best["nmf_data"]
                    nmf_data["W_cluster_means"] = self._compute_w_cluster_means(
                        best["W"], best["labels"]
                    )
                    result["nmf"] = nmf_data

            return result

        # 4. Normal mode — single NMF + cluster + overview
        nmf_data: Dict[str, Any] | None = None
        if nmf_components is not None:
            nmf_model = NMF(n_components=nmf_components, random_state=42)
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

        overview_df, cluster_series = self._build_overview(
            cluster_labels,
            metrics_df.index,
            path_col,
            event_col,
            overview_metrics or [],
        )

        if method == "kmeans":
            best_params: Dict[str, Any] = {"n_clusters": n_clusters}
        else:
            best_params = {
                "min_cluster_size": min_cluster_size or 5,
                "cluster_selection_epsilon": cluster_selection_epsilon or 0.0,
            }
        if nmf_components is not None:
            best_params["nmf_components"] = nmf_components

        return {
            "overview_df": overview_df,
            "cluster_labels": cluster_series,
            "nmf": nmf_data,
            "best_params": best_params,
        }

    # ------------------------------------------------------------------

    def _scale_features(self, features: np.ndarray, scaler: T_Scaler) -> np.ndarray:
        if scaler is None:
            return features
        elif scaler == "minmax":
            return MinMaxScaler().fit_transform(features)
        elif scaler == "standard":
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
        nmf_components: int | List[int] | None,
        n_clusters: int | List[int] | None,
        min_cluster_size: int | List[int] | None,
        cluster_selection_epsilon: float | List[float] | None,
    ) -> Dict[str, Any]:
        nmf_component_values = (
            nmf_components
            if isinstance(nmf_components, (list, tuple))
            else [nmf_components]
        )
        is_nmf_search = isinstance(nmf_components, (list, tuple))

        params: List[Dict[str, Any]] = []
        scores: List[float | None] = []

        best_score: float = -2.0
        best_labels: np.ndarray | None = None
        best_nmf_data: Dict[str, Any] | None = None
        best_W: np.ndarray | None = None
        best_params: Dict[str, Any] | None = None

        for nk in nmf_component_values:
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
                        p["nmf_components"] = nk
                    params.append(p)
                    scores.append(score)

                    if score is not None and score > best_score:
                        best_score = score
                        best_labels = labels
                        best_nmf_data = nmf_data
                        best_W = X if nk is not None else None
                        best_params = dict(p)

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
                            p["nmf_components"] = nk
                        params.append(p)
                        scores.append(score)

                        if score is not None and score > best_score:
                            best_score = score
                            best_labels = labels
                            best_nmf_data = nmf_data
                            best_W = X if nk is not None else None
                            best_params = dict(p)

        result: Dict[str, Any] = {"params": params, "silhouette": scores}
        if best_labels is not None:
            result["best"] = {
                "labels": best_labels,
                "nmf_data": best_nmf_data,
                "W": best_W,
                "params": best_params,
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
        path_col: str,
        event_col: str,
        overview_metrics: List[Dict[str, Any]],
    ) -> tuple[pd.DataFrame, pd.Series]:
        """Returns (overview_df, cluster_series) — cluster_series maps path id to
        cluster label ("cluster_0", ..., "noise") and is cached by the widget so a
        later distribution request can rebuild the same temp stream without
        re-running clustering (see `get_metric_distribution`)."""
        cluster_series = pd.Series(cluster_labels, index=path_index)
        cluster_series = cluster_series.apply(
            lambda x: f"cluster_{x}" if x >= 0 else "noise"
        )

        overview_df = self._temp_cluster_stream(
            cluster_series, path_col
        ).segment_overview_data(
            segment_col=SEGMENT_COL,
            metrics=overview_metrics,
            path_col=path_col,
            event_col=event_col,
        )
        return overview_df, cluster_series

    def _temp_cluster_stream(
        self, cluster_labels: pd.Series, path_col: str
    ) -> "Eventstream":
        """Build a throwaway eventstream with `cluster_labels` injected as the
        `SEGMENT_COL` segment column — the same trick `_build_overview` uses to
        feed the heatmap, reused here to answer distribution requests."""
        from retentioneering.eventstream.eventstream import Eventstream

        df = self.eventstream.df.copy()
        df[SEGMENT_COL] = df[path_col].map(cluster_labels).astype("category")

        schema_dict = dict(self.eventstream._schema) if self.eventstream._schema else {}
        segment_cols = list(schema_dict.get("segment_cols", []))
        segment_cols.append(SEGMENT_COL)
        schema_dict["segment_cols"] = segment_cols

        return Eventstream(df, schema_dict, preprocess=False)

    def get_metric_distribution(
        self,
        cluster_labels: pd.Series,
        metric: Dict[str, Any],
        segment_value: str | List[str],
        complement: bool = False,
        path_col: str | None = None,
    ) -> Dict[str, Any]:
        """Distribution of `metric` across one or two clusters from the last Apply.

        `cluster_labels` is the `cluster_labels` Series `fit()` returns (path id →
        cluster name) — the widget caches it and passes it back in here rather than
        re-running clustering. Delegates to `SegmentOverview.get_metric_distribution`
        via a throwaway temp stream, exactly like the overview heatmap does.
        """
        path_col = path_col or self.eventstream.schema.path_col
        temp_stream = self._temp_cluster_stream(cluster_labels, path_col)
        return temp_stream.get_metric_distribution(
            segment_col=SEGMENT_COL,
            segment_value=segment_value,
            metric=metric,
            complement=complement,
            path_col=path_col,
        )
