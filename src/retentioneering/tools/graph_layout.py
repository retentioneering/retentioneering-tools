import random
import zlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from sklearn.cluster import AgglomerativeClustering

from retentioneering import engine
from retentioneering.eventstream.event_type import EventTypes
from retentioneering.exceptions import (
    EmptyEventstreamError,
    InvalidParameterError,
)
from .transition_matrix import TransitionMatrix

if TYPE_CHECKING:
    from retentioneering.eventstream.eventstream import Eventstream

# Horizontal offset of the pinned path_start / path_end anchors from the
# leftmost / rightmost interior node.
ANCHOR_MARGIN = 150.0


def _deterministic_hash(word: str) -> int:
    """Word2Vec seeds each word's initial vector via hashfxn(word). The
    default is Python's hash(), which is randomized per process
    (PYTHONHASHSEED) — the layout would differ between kernel restarts.
    CRC32 pins it."""
    return zlib.crc32(str(word).encode("utf-8"))


@dataclass
class GraphLayout:
    """Trajectory-embedding layout for the transition graph.

    The pipeline (ported from the original hopscotch implementation):

    1. Collect user trajectories (real paths by default, or random walks
       over the ``proba_out`` transition matrix).
    2. Embed events with word2vec over those trajectories — events that
       appear in similar contexts get nearby vectors.
    3. Recursively split the embedding space with agglomerative clustering
       and map the splits onto nested rectangles of the canvas, so related
       events land in the same region and the nesting reflects the cluster
       hierarchy.
    4. Pin the flow anchors: ``path_start`` on the far left, ``path_end`` on
       the far right, both at the vertical center of the content.

    Determinism: single-worker word2vec with a fixed seed and a
    process-independent ``hashfxn``, seeded sampling/jitter — the same
    stream produces the same layout, including across kernel restarts.
    """

    eventstream: "Eventstream"

    def fit(
        self,
        path_col: str | None = None,
        sample_size: int = 1000,
        walk_length: int = 20,
        embedding_dim: int = 32,
        n_clusters: int = 5,
        random_state: int = 42,
        min_trajectory_length: int = 3,
        use_original_trajectories: bool = True,
    ) -> dict[str, dict[str, float]]:
        path_col = path_col or self.eventstream.schema.path_col
        if path_col not in self.eventstream.schema.path_cols:
            raise InvalidParameterError(
                "path_col", path_col, self.eventstream.schema.path_cols
            )
        if self.eventstream.is_empty():
            raise EmptyEventstreamError(
                "Cannot compute graph layout for empty eventstream"
            )

        if use_original_trajectories:
            trajectories = self._get_original_trajectories(
                path_col, min_trajectory_length
            )
            if sample_size > 0 and len(trajectories) > sample_size:
                random.seed(random_state)
                trajectories = random.sample(trajectories, sample_size)
        else:
            tm_df = self._compute_transition_matrix(path_col, min_trajectory_length)
            trajectories = self._sample_trajectories(
                tm_df,
                num_walks=sample_size,
                walk_length=walk_length,
                seed=random_state,
            )

        if not trajectories:
            return {}

        embeddings, idx_to_event = self._embed_events(
            trajectories, vector_size=embedding_dim, seed=random_state
        )

        clusters = self._cluster_events(embeddings, n_clusters=n_clusters)

        event_types = EventTypes()
        anchors = {event_types.PATH_START.name, event_types.PATH_END.name}

        layout = self._generate_layout(
            embeddings,
            clusters,
            idx_to_event,
            random_state=random_state,
            skip_events=anchors,
        )

        # Pin the flow anchors: far left / far right, vertically centered on
        # the content (they are also excluded from the embedding placement
        # above — they occur in every path and carry no cluster signal).
        if layout:
            xs = [p["x"] for p in layout.values()]
            ys = [p["y"] for p in layout.values()]
            left = min(xs) - ANCHOR_MARGIN
            right = max(xs) + ANCHOR_MARGIN
            mid_y = (min(ys) + max(ys)) / 2
        else:
            left, right, mid_y = -500.0, 500.0, 0.0
        layout[event_types.PATH_START.name] = {"x": left, "y": mid_y}
        layout[event_types.PATH_END.name] = {"x": right, "y": mid_y}

        return layout

    # ── trajectories ───────────────────────────────────────────────────────────

    def _get_original_trajectories(
        self, path_col: str, min_length: int
    ) -> list[list[str]]:
        """Real user paths (including path_start/path_end, so the boundary
        nodes get embedded and positioned too)."""
        schema = self.eventstream.schema
        event_col_q = engine.quote_ident(schema.event_col)
        path_col_q = engine.quote_ident(path_col)
        index_col_q = engine.quote_ident(schema.index)
        subindex_col_q = engine.quote_ident(schema.subindex)

        df = self.eventstream.add_start_end_events(path_col=path_col).df
        # Ordered list aggregation: ordering inside a subquery is not
        # guaranteed to survive GROUP BY. The cast sidesteps categorical
        # columns coming back as category codes. The outer ORDER BY pins the
        # trajectory order — DuckDB's parallel GROUP BY returns rows in
        # varying order, and the word2vec training result depends on corpus
        # order, so without it the layout would differ between runs.
        query = f"""
        select list(cast({event_col_q} as varchar)
                    order by {index_col_q}, {subindex_col_q}) as events
        from df
        group by {path_col_q}
        order by {path_col_q}
        """
        paths = engine.run(query, df=df)["events"].tolist()

        return [
            [str(event) for event in path] for path in paths if len(path) >= min_length
        ]

    def _compute_transition_matrix(
        self, path_col: str, min_length: int
    ) -> pd.DataFrame:
        stream = self.eventstream
        if min_length > 1:
            try:
                stream = stream.filter_paths(
                    {"op": ">=", "metric": "length", "value": min_length},
                    path_col=path_col,
                )
            except EmptyEventstreamError:
                pass

        return TransitionMatrix(stream).fit(values="proba_out", path_col=path_col)

    def _sample_trajectories(
        self, tm_df: pd.DataFrame, num_walks: int, walk_length: int, seed: int
    ) -> list[list[str]]:
        random.seed(seed)
        np.random.seed(seed)

        events = tm_df.index.tolist()
        trajectories: list[list[str]] = []

        # event -> (next_events, probabilities); zero rows dropped up front
        transitions: dict[str, tuple[list[str], list[float]]] = {}
        for event in events:
            row = tm_df.loc[event]
            nonzero = row[row > 0]
            if not nonzero.empty:
                transitions[event] = (nonzero.index.tolist(), nonzero.values.tolist())
            else:
                transitions[event] = ([], [])

        # Ensure we start walks from all events to cover rare ones
        start_events = events * (num_walks // len(events) + 1)
        start_events = start_events[:num_walks]
        random.shuffle(start_events)

        for start_node in start_events:
            walk = [start_node]
            current_node = start_node

            for _ in range(walk_length - 1):
                if current_node not in transitions or not transitions[current_node][0]:
                    break

                next_nodes, probs = transitions[current_node]
                next_node = random.choices(next_nodes, weights=probs, k=1)[0]
                walk.append(next_node)
                current_node = next_node

            trajectories.append(walk)

        return trajectories

    # ── embedding ──────────────────────────────────────────────────────────────

    def _embed_events(
        self, trajectories: list[list[str]], vector_size: int, seed: int
    ) -> tuple[np.ndarray, dict[int, str]]:
        model = Word2Vec(
            sentences=trajectories,
            vector_size=vector_size,
            window=5,
            min_count=1,
            workers=1,  # single worker — required for determinism
            seed=seed,
            hashfxn=_deterministic_hash,
        )

        events = list(model.wv.index_to_key)
        embeddings = np.array([model.wv[event] for event in events])
        idx_to_event = {i: event for i, event in enumerate(events)}

        return embeddings, idx_to_event

    # ── clustering + nested-rectangles placement ───────────────────────────────

    def _cluster_events(self, embeddings: np.ndarray, n_clusters: int) -> np.ndarray:
        n_clusters = min(n_clusters, len(embeddings))
        if n_clusters < 2:
            return np.zeros(len(embeddings), dtype=int)
        clustering = AgglomerativeClustering(n_clusters=n_clusters)
        clustering.fit(embeddings)
        return clustering.labels_

    def _generate_layout(
        self,
        embeddings: np.ndarray,
        initial_clusters: np.ndarray,
        idx_to_event: dict[int, str],
        random_state: int,
        skip_events: set[str] | None = None,
    ) -> dict[str, dict[str, float]]:
        """Recursive nested-rectangles placement: split the current box among
        agglomerative sub-clusters of the embedding subset, recurse."""
        layout: dict[str, dict[str, float]] = {}
        rng = np.random.RandomState(random_state)

        def place_points(
            indices: list[int], x: float, y: float, w: float, h: float, depth: int = 0
        ) -> None:
            if len(indices) == 0:
                return

            if len(indices) == 1:
                index = indices[0]
                layout[idx_to_event[index]] = {
                    "x": x + w / 2 + rng.uniform(-w / 4, w / 4),
                    "y": y + h / 2 + rng.uniform(-h / 4, h / 4),
                }
                return

            # Deep recursion: just scatter the remainder in the box
            if depth > 3:
                for index in indices:
                    layout[idx_to_event[index]] = {
                        "x": x + rng.uniform(0, w),
                        "y": y + rng.uniform(0, h),
                    }
                return

            subset_embeddings = embeddings[indices]

            n_sub_clusters = min(len(indices), 4)  # split into up to 4 quadrants
            if n_sub_clusters < 2:
                for index in indices:
                    layout[idx_to_event[index]] = {
                        "x": x + rng.uniform(0, w),
                        "y": y + rng.uniform(0, h),
                    }
                return

            sub_clustering = AgglomerativeClustering(n_clusters=n_sub_clusters)
            sub_labels = sub_clustering.fit_predict(subset_embeddings)

            # Divide the current box into a grid of sub-boxes
            rows = int(np.ceil(np.sqrt(n_sub_clusters)))
            cols = int(np.ceil(n_sub_clusters / rows))

            cell_w = w / cols
            cell_h = h / rows

            for cluster_id in range(n_sub_clusters):
                cluster_indices = [
                    indices[i]
                    for i in range(len(indices))
                    if sub_labels[i] == cluster_id
                ]

                row = cluster_id // cols
                col = cluster_id % cols

                cell_x = x + col * cell_w
                cell_y = y + row * cell_h

                margin = 0.1
                place_points(
                    cluster_indices,
                    cell_x + cell_w * margin,
                    cell_y + cell_h * margin,
                    cell_w * (1 - 2 * margin),
                    cell_h * (1 - 2 * margin),
                    depth + 1,
                )

        skip = skip_events or set()
        all_indices = [i for i, e in idx_to_event.items() if e not in skip]

        # Centered on the origin (cytoscape preset coordinates). Partition by
        # the caller-requested initial_clusters first — one grid cell per
        # top-level cluster — then recurse independently inside each cell.
        # Without this split, n_clusters would be ignored: place_points would
        # instead recluster all_indices as a single box on its own terms.
        top_cluster_ids = sorted({int(initial_clusters[i]) for i in all_indices})
        if len(top_cluster_ids) <= 1:
            place_points(all_indices, -500, -500, 1000, 1000)
            return layout

        rows = int(np.ceil(np.sqrt(len(top_cluster_ids))))
        cols = int(np.ceil(len(top_cluster_ids) / rows))
        cell_w = 1000 / cols
        cell_h = 1000 / rows
        margin = 0.1

        for i, cluster_id in enumerate(top_cluster_ids):
            cluster_indices = [
                index for index in all_indices if initial_clusters[index] == cluster_id
            ]
            row = i // cols
            col = i % cols
            cell_x = -500 + col * cell_w
            cell_y = -500 + row * cell_h
            place_points(
                cluster_indices,
                cell_x + cell_w * margin,
                cell_y + cell_h * margin,
                cell_w * (1 - 2 * margin),
                cell_h * (1 - 2 * margin),
            )

        return layout
