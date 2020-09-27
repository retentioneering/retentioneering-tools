from .base_dataset import BaseDataset

from .step_matrix import step_matrix
from .get_clusters import get_clusters, filter_cluster
from .plot_graph import plot_graph
from .extract_features import extract_features

BaseDataset.step_matrix = step_matrix
BaseDataset.get_clusters = get_clusters
BaseDataset.filter_cluster = filter_cluster
BaseDataset.plot_graph = plot_graph
BaseDataset.extract_features = extract_features

