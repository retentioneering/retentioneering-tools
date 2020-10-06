# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md

from .base_dataset import BaseDataset

from .get_edgelist import get_edgelist
from .get_adjacency import get_adjacency
from .step_matrix import step_matrix
from .get_clusters import get_clusters, filter_cluster, cluster_event_dist
from .plot_graph import plot_graph
from .project import project
from .extract_features import extract_features
from .compare import compare
from .funnel import funnel


BaseDataset.get_edgelist = get_edgelist
BaseDataset.get_adjacency = get_adjacency
BaseDataset.step_matrix = step_matrix
BaseDataset.get_clusters = get_clusters
BaseDataset.project = project
BaseDataset.filter_cluster = filter_cluster
BaseDataset.plot_graph = plot_graph
BaseDataset.extract_features = extract_features
BaseDataset.compare = compare
BaseDataset.cluster_event_dist = cluster_event_dist
BaseDataset.funnel = funnel
