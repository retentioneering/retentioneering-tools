from .base_dataset import BaseDataset

from .get_edgelist import get_edgelist, get_adjacency
from .step_matrix import step_matrix
from .get_clusters import get_clusters, filter_cluster, cluster_event_dist
from .plot_graph import plot_graph
from .extract_features import extract_features
from .compare import compare
from .funnel import funnel


BaseDataset.get_edgelist = get_edgelist
BaseDataset.get_adjacency = get_adjacency
BaseDataset.step_matrix = step_matrix
BaseDataset.get_clusters = get_clusters
BaseDataset.filter_cluster = filter_cluster
BaseDataset.plot_graph = plot_graph
BaseDataset.extract_features = extract_features
BaseDataset.compare = compare
BaseDataset.cluster_event_dist = cluster_event_dist
BaseDataset.funnel = funnel
