from __future__ import annotations

from typing import Any, List, Literal, Tuple, cast

import matplotlib.pylab as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
import umap.umap_ as umap
from matplotlib import axes, rcParams
from numpy import ndarray
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.manifold import TSNE
from sklearn.mixture import GaussianMixture

from src.eventstream.types import EventstreamSchemaType, EventstreamType
from src.tooling.clusters.segments import Segments

FeatureType = Literal["tfidf", "count", "frequency", "binary", "time", "time_fraction", "markov"]
SklearnFeatureType = Literal["count", "frequency", "tfidf", "binary"]
NgramRange = Tuple[int, int]
Method = Literal["kmeans", "gmm"]
PlotType = Literal["cluster_bar"]
PlotProjectionMethod = Literal["tsne", "umap"]


class Clusters:
    """
    A class that holds the methods for cluster analysis.

    Parameters
    ----------
    eventstream : EventstreamType

    See Also
    --------
    :py:func:`src.eventstream.eventstream.Eventstream.clusters`
    """

    def __init__(self, eventstream: EventstreamType):
        self.__eventstream: EventstreamType = eventstream
        self.user_col = eventstream.schema.user_id
        self.event_col = eventstream.schema.event_name
        self.time_col = eventstream.schema.event_timestamp
        self.event_index_col = eventstream.schema.event_index

        self.__segments: Segments | None = None
        self.__features: pd.DataFrame | None = None
        self.__cluster_result: pd.Series | None = None
        self.__projection: pd.DataFrame | None = None
        self.__is_fitted: bool = False

        self._method: Method | None = None
        self._n_clusters: int | None = None
        self._user_clusters: pd.Series | None = None
        self._feature_type: FeatureType | None = None
        self._ngram_range: NgramRange | None = None
        self._vector: pd.DataFrame | None = None

    # public API

    def fit(
        self,
        method: Method | None = None,
        n_clusters: int | None = None,
        feature_type: FeatureType | None = None,
        ngram_range: NgramRange | None = None,
        vector: pd.DataFrame | None = None,
    ) -> None:
        """
        Fits clusters to the eventstream data.

        Parameters
        ----------
        method: {"kmeans", "gmm"}, default=None
            ``kmeans`` stands classic K-means algorithm. https://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html
            ``gmm`` stands for Gaussian mixture model. https://scikit-learn.org/stable/modules/generated/sklearn.mixture.GaussianMixture.html
        n_clusters: int, default=None
            The expected number of clusters to be passed to a clustering algorithm.
        feature_type : {"tfidf", "count", "frequency", "binary", "markov", "time", "time_fraction"}, default=None
            See :py:func:`extract_features`
        ngram_range : Tuple(int, int), default=None
            See :py:func:`extract_features`
        vector: pd.DataFrame, default=None
            ``pd.DataFrame`` representing custom vectorization of the user paths. The index corresponds to user_ids,
            the columns are vectorized values of the path.
        """

        self._method, self._n_clusters, self._feature_type, self._ngram_range, self._vector = self.__validate_input(
            method, n_clusters, feature_type, ngram_range, vector
        )

        self.__features, self.__cluster_result = self._prepare_clusters()
        self._user_clusters = self.__cluster_result.copy()

        self.__segments = Segments(
            eventstream=self.__eventstream,
            segments_df=self.__cluster_result.to_frame("segment").reset_index(),
        )

        self.__is_fitted = True

    def event_dist(
        self,
        cluster_id1: int,
        cluster_id2: int | None = None,
        top_n: int = 8,
        weight_col: str | None = None,
        targets: list[str] | None = None,
    ) -> go.Figure:
        """
        Plots a bar plot illustrating the distribution of ``top_n`` events in cluster ``cluster_id1``
        compared vs the entire dataset or vs cluster ``cluster_id2``.
        Parameters
        ----------
        cluster_id1: int
            ID of the cluster to compare.
        cluster_id2: int, (optional, default None)
            ID of the second cluster to compare with top events from first
            cluster. If None, then compares with entire dataset.
        top_n: int, (optional, default 8)
            Number of top events.
        weight_col: str (optional, default None)
            If None distribution will be compared based on events occurrences in
            datasets. If ``weight_col`` is specified, percentages of users
            (column name specified by parameter ``weight_col``) who have particular
            events will be plotted.
        targets: list of str (optional, default None)
            List of event names always to include for comparison regardless
            of the parameter top_n value. Target events will appear in the same
            order as specified.
        Returns
        -------
        Plots the distribution barchart.
        """
        if not self.__is_fitted:
            raise RuntimeError("Clusters are not defined. Consider to run 'fit()' or `set_clusters()` methods.")

        cluster1 = self.filter_cluster(cluster_id1).to_dataframe()

        if targets is None:
            targets = []

        if weight_col is not None:
            cluster1 = cluster1.drop_duplicates(subset=[self.event_col, weight_col])
            top_cluster = cluster1[self.event_col].value_counts() / cluster1[weight_col].nunique()

        else:
            top_cluster = cluster1[self.event_col].value_counts(normalize=True)

        # add zero events for missing targets
        for event in set(targets) - set(top_cluster.index):
            top_cluster.loc[event] = 0

        # create events order: top_n non-target events + targets:
        events_to_keep = top_cluster[lambda x: ~x.index.isin(targets)].iloc[:top_n].index.tolist()  # type: ignore
        target_separator_position = len(events_to_keep)
        events_to_keep += list(targets)
        top_cluster = top_cluster.loc[events_to_keep].reset_index()  # type: ignore

        if cluster_id2 is None:
            cluster2 = self.__eventstream.to_dataframe()
        else:
            cluster2 = self.filter_cluster(cluster_id2).to_dataframe()

        if weight_col is not None:
            cluster2 = cluster2.drop_duplicates(subset=[self.event_col, weight_col])
            # get events distribution from cluster 2:
            top_all = cluster2[self.event_col].value_counts() / cluster2[weight_col].nunique()
        else:
            # get events distribution from cluster 2:
            top_all = cluster2[self.event_col].value_counts(normalize=True)

        # make sure top_all has all events from cluster 1
        for event in set(top_cluster["index"]) - set(top_all.index):
            top_all.loc[event] = 0

        # keep only top_n events from cluster1
        top_all = top_all.loc[top_cluster["index"]].reset_index()  # type: ignore

        top_all.columns = [self.event_col, "freq"]  # type: ignore
        top_cluster.columns = [self.event_col, "freq"]  # type: ignore

        top_all["hue"] = "all" if cluster_id2 is None else f"cluster {cluster_id2}"
        top_cluster["hue"] = f"cluster {cluster_id1}"

        total_size = self.__eventstream.to_dataframe()[self.user_col].nunique()
        figure = self._plot_event_dist(
            top_all.append(top_cluster, ignore_index=True, sort=False),
            cl1=cluster_id1,
            sizes=[
                cluster1[self.user_col].nunique() / total_size,
                cluster2[self.user_col].nunique() / total_size,
            ],
            weight_col=weight_col,
            target_pos=target_separator_position,
            targets=targets,
            cl2=cluster_id2,
        )

        return figure

    def plot(self, targets: list[str] | list[list[str]] | None = None) -> go.Figure:
        """
        Plots a bar plot illustrating the clusters sizes and the conversion rates of
        the ``targets`` events within the clusters.

        Parameters
        ----------
        targets: list of str (optional, default None)
            Represents the list of the target events

        """
        if not self.__is_fitted:
            raise RuntimeError("Clusters are not defined. Consider to run 'fit()' or `set_clusters()` methods.")

        target_names, targets_bool = self._prepare_targets(targets)  # type: ignore
        return self._cluster_bar(
            clusters=self.__cluster_result.values,  # type: ignore
            target=cast(List[List[bool]], targets_bool),  # @TODO: fix types. Vladimir Makhanov
            target_names=target_names,
        )

    @property
    def user_clusters(self) -> pd.Series | None:
        """
        Returns ``user_id -> cluster_id`` mapping representing as ``pd.Series``. The index corresponds to
        user_ids, the values relate to the corresponding cluster_ids.

        Returns
        -------
        pd.Series
        """
        if not self.__is_fitted:
            raise RuntimeError("Clusters are not defined. Consider to run 'fit()' or `set_clusters()` methods.")

        return self.__cluster_result

    @property
    def cluster_mapping(self) -> dict:
        """
        Returns ``cluster_id -> list[user_ids]`` mapping.

        Returns
        -------
        dict
            The keys are cluster_ids, the values are the lists of the user_ids related to the corresponding cluster.
        """
        if not self.__is_fitted or self.__cluster_result is None:
            raise RuntimeError("Clusters are not defined. Consider to run 'fit()' or `set_clusters()` methods.")

        df = self.__cluster_result.to_frame("cluster_id").reset_index()
        user_col, cluster_col = df.columns
        cluster_map = df.groupby(cluster_col)[user_col].apply(list)
        return cluster_map.to_dict()

    @property
    def features(self) -> pd.DataFrame | None:
        """
        Returns the calculated features if the clusters are fitted. The index corresponds to user_ids,
        the columns are values of the vectorized user's trajectory.

        Returns
        -------
        pd.DataFrame
        """
        if self.__features is None:
            raise RuntimeError("The features are not calculated. Consider to run 'extract_features()` method.")

        return self.__features

    @property
    def params(self) -> dict:
        """
        Returns the parameters used for the last fitting.

        """
        return {
            "method": self._method,
            "n_clusters": self._n_clusters,
            "ngram_range": self._ngram_range,
            "feature_type": self._feature_type,
        }

    def set_clusters(self, user_clusters: pd.Series) -> None:
        """
        Sets custom user-cluster mapping.

        Parameters
        ----------
        user_clusters: pd.Series
            Series index corresponds to user_ids. Values are cluster_ids.
        """
        self._user_clusters = user_clusters
        self.__cluster_result = user_clusters.copy()
        self._n_clusters = user_clusters.nunique()
        self._feature_type = None
        self._method = None
        self._ngram_range = None
        self.__is_fitted = True
        return

    def filter_cluster(self, cluster_id: int | str) -> EventstreamType:
        """
        Truncates the eventstream and leaves the trajectories of the users who belong to the selected cluster.

        Parameters
        ----------
        cluster_id: int or str
            Cluster identifier to be selected.

            If :py:func:`create_clusters` was used for cluster generation, then
             0, 1, ... values are possible.
        Returns
        --------
        EventstreamType
            Eventstream with the users belonging to the selected cluster only.



        """
        from src.eventstream.eventstream import Eventstream

        if not self.__is_fitted:
            raise RuntimeError("Clusters are not defined. Consider to run 'fit()' or `set_clusters()` methods.")

        eventstream: Eventstream = self.__eventstream  # type: ignore

        cluster_users = self.__cluster_result[lambda s: s == cluster_id].index  # type: ignore
        df = eventstream.to_dataframe()[lambda df_: df_[self.user_col].isin(cluster_users)]  # type: ignore

        es = Eventstream(
            raw_data=df,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            schema=eventstream.schema.copy(),
        )
        return es

    def extract_features(self, feature_type: FeatureType, ngram_range: NgramRange | None = None) -> pd.DataFrame:
        """
        Calculates vectorized user paths.

        Parameters
        ----------
        feature_type : {"tfidf", "count", "frequency", "binary", "markov", "time", "time_fraction"}

            - ``tfidf`` see details in :sklearn_tfidf:`sklearn documentation<>`
            - ``count`` see details in :sklearn_countvec:`sklearn documentation<>`
            - | ``frequency`` the same as count but normalized to the total number of the events
              | in the user's trajectory.
            - ``binary`` 1 if a user had given n-gram at least once and 0 otherwise.
            - | ``markov`` available for bigrams only. For a given bigram ``(A, B)`` the vectorized values
              | are the user's transition probabilities from ``A`` to ``B``.
            - | ``time`` Associated with unigrams only. The total number of the seconds spent from the
              | beginning of a user's path until a given event.
            - | ``time_fraction`` the same as ``time`` but divided by the total length of the user's trajectory
              | (in seconds).
        ngram_range: Tuple(int, int)
            The lower and upper boundary of the range of n-values for different word n-grams or char n-grams to be
            extracted. For example, ngram_range=(1, 1) means only single events, (1, 2) means single events
            and bigrams. Ignored for ``markov``, ``time``, ``time_fraction`` feature types.

        Returns
        -------
        pd.DataFrame
            A DataFrame with the vectorized values. Index contains user_id, columns contain n-grams.
        """
        eventstream = self.__eventstream
        events = eventstream.to_dataframe()
        vec_data = None

        if feature_type in ["count", "frequency", "tfidf", "binary"] and ngram_range is not None:
            feature_type = cast(SklearnFeatureType, feature_type)
            events, vec_data = self._sklearn_vectorization(events, feature_type, ngram_range, self.user_col)

        elif feature_type == "markov":
            events, vec_data = self._markov_vectorization(events, self.user_col)

        if feature_type in ["time", "time_fraction"]:
            events.sort_values(by=[self.user_col, self.time_col], inplace=True)
            events.reset_index(inplace=True)
            events["time_diff"] = events.groupby(user_col)[time_col].diff().dt.total_seconds()  # type: ignore
            events["time_length"] = events["time_diff"].shift(-1)
            if feature_type == "time_fraction":
                vec_data = (
                    events.groupby([self.user_col])
                    .apply(lambda x: x.groupby(self.event_col)["time_length"].sum() / x["time_length"].sum())
                    .unstack(fill_value=0)
                )
            elif feature_type == "time":
                vec_data = (
                    events.groupby([self.user_col])
                    .apply(lambda x: x.groupby(self.event_col)["time_length"].sum())
                    .unstack(fill_value=0)
                )

        if vec_data is not None:
            vec_data.columns = [col + "_" + feature_type for col in vec_data.columns]

        return cast(pd.DataFrame, vec_data)

    def projection(
        self,
        method: PlotProjectionMethod = "tsne",
        targets: list[str] | None = None,
        plot_type: Literal["targets", "clusters"] = "clusters",
        **kwargs: Any,
    ) -> go.Figure:
        """
        Shows the clusters projection on a plane applying dimension reduction techniques.

        Parameters
        ----------
        method : {'umap', 'tsne'} (optional, default 'tsne')
            Type of manifold transformation.
        plot_type : {'targets', 'clusters'} (optional, default 'clusters')
            Type of color-coding used for projection visualization:
                - 'clusters': colors trajectories with different colors depending on cluster number.
                - 'targets': color trajectories based on reach to any event provided in 'targets' parameter.
                Must provide 'targets' parameter in this case.
        targets : list or tuple of str (optional, default  ())
            Vector of event_names as str. If user reach any of the specified events, the dot corresponding
            to this user will be highlighted as converted on the resulting projection plot
        plot_type : {'targets', 'clusters', None} (optional, default None)
            Type of color-coding used for projection visualization:

            - ``clusters`` colors trajectories with different colors depending on cluster number.
            - | ``targets`` colors trajectories based on reach to any event provided in 'targets' parameter.
              | Must provide ``targets`` parameter in this case.
            - If ``None``, then only calculates TSNE without visualization.
        **kwargs : optional
            Parameters for ``sklearn.manifold.TSNE()`` and ``umap.UMAP()``

        Returns
        -------
        sns.scatterplot
            Plot in the low-dimensional space for user trajectories indexed by user IDs.
        """

        if self.__features is None or self.__is_fitted is False:
            raise RuntimeError("Clusters and features must be defined. Consider to run 'fit()' method.")

        if targets is None:
            targets = []

        if plot_type == "clusters":
            if self.__cluster_result is not None:
                targets_mapping = self.__cluster_result.values
                legend_title = "cluster number:"
            else:
                raise RuntimeError("Clusters are not defined. Consider to run 'fit()' or `set_clusters()` methods.")

        elif plot_type == "targets":
            if (not targets) and (len(targets) < 1):
                raise ValueError(
                    "When plot_type='targets' is set, 'targets' must be defined as list of target event names"
                )
            else:
                targets = [list(pd.core.common.flatten(targets))]  # type: ignore
                legend_title = "conversion to (" + " | ".join(targets[0]).strip(" | ") + "):"  # type: ignore
                # @TODO: fix 'groupby + apply' inefficient combination. Vladimir Kukushkin
                targets_mapping = (
                    self.__eventstream.to_dataframe()
                    .groupby(self.user_col)[self.event_col]
                    .apply(lambda x: bool(set(*targets) & set(x)))
                    .to_frame()
                    .sort_index()[self.event_col]
                    .values
                )
        else:
            raise ValueError("Unexpected plot type: %s. Allowed values: clusters, targets" % plot_type)

        features = self.__features

        if method == "tsne":
            projection: pd.DataFrame = self._learn_tsne(features, **kwargs)
        elif method == "umap":
            projection = self._learn_umap(features, **kwargs)
        else:
            raise ValueError("Unknown method: %s. Allowed methods: tsne, umap" % method)

        self.__projection = projection

        figure, _ = self._plot_projection(
            projection=projection.values,
            targets=targets_mapping,  # type: ignore
            legend_title=legend_title,
        )

        return figure

    # inner functions
    def __validate_input(
        self,
        method: Method | None = None,
        n_clusters: int | None = None,
        feature_type: FeatureType | None = None,
        ngram_range: NgramRange | None = None,
        vector: pd.DataFrame | None = None,
    ) -> tuple[Method | None, int | None, FeatureType | None, NgramRange | None, pd.DataFrame | None]:

        _method = method or self._method
        _n_clusters = n_clusters or self._n_clusters
        _user_clusters = None

        if _method is None:
            raise ValueError("'method' must be defined for fitting.")

        if _n_clusters is None:
            raise ValueError("'n_clusters' must be defined for fitting.")

        if vector:
            if not isinstance(vector, pd.DataFrame):  # type: ignore
                raise ValueError("Vector is not a DataFrame!")
            if np.all(np.all(vector.dtypes == "float") and vector.isna().sum().sum() != 0):
                raise ValueError(
                    "Vector is wrong formatted! NaN should be replaced with 0 and all dtypes must be float!"
                )
            if feature_type:
                raise ValueError("Both 'vector' and 'feature_type' are defined. 'feature_type' will be ignored.")
            if ngram_range:
                raise ValueError("Both 'vector' and 'ngram_range' are defined. 'ngram_range' will be ignored.")

            _vector = vector
            _feature_type = None
            _ngram_range = None
        else:
            _feature_type = feature_type or self._feature_type
            _ngram_range = ngram_range or self._ngram_range
            _vector = vector or self._vector

            if _feature_type is None:
                raise ValueError("'feature_type' must be defined for fitting.")

            if _ngram_range is None:
                raise ValueError("'ngram_range' must be defined for fitting.")

        return _method, _n_clusters, _feature_type, _ngram_range, _vector

    def _prepare_clusters(self) -> tuple[pd.DataFrame, pd.Series]:
        features = pd.DataFrame()
        user_clusters = pd.Series(dtype=np.int64)

        if self._user_clusters is not None:
            user_clusters = self._user_clusters.copy()
        else:
            if self._vector is not None:
                features = self._vector.copy()
            else:
                if self._feature_type is not None and self._ngram_range is not None:
                    features = self.extract_features(self._feature_type, self._ngram_range)

            if self._n_clusters is not None:
                if self._method == "kmeans":
                    cluster_result = self._kmeans(features=features, n_clusters=self._n_clusters)
                elif self._method == "gmm":
                    cluster_result = self._gmm(features=features, n_clusters=self._n_clusters)
                else:
                    raise ValueError("Unknown method: %s" % self._method)

                user_clusters = pd.Series(cluster_result, index=features.index)

        return features, user_clusters

    @staticmethod
    def _plot_projection(projection: ndarray, targets: ndarray, legend_title: str) -> tuple:
        rcParams["figure.figsize"] = 8, 6

        scatter = sns.scatterplot(
            x=projection[:, 0],
            y=projection[:, 1],
            hue=targets,
            legend="full",
            palette=sns.color_palette("bright")[0 : np.unique(targets).shape[0]],  # noqa
        )

        # move legend outside the box
        scatter.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.0).set_title(legend_title)
        plt.setp(scatter.get_legend().get_title(), fontsize="12")

        return (
            scatter,
            projection,
        )

    @staticmethod
    def _learn_tsne(data: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        Calculates TSNE transformation for given matrix features.

        Parameters
        --------
        data: np.array
            Array of features.
        kwargs: optional
            Parameters for ``sklearn.manifold.TSNE()``

        Returns
        -------
        pd.DataFrame
            Calculated TSNE transform

        """

        tsne_params = [
            "angle",
            "early_exaggeration",
            "init",
            "learning_rate",
            "method",
            "metric",
            "min_grad_norm",
            "n_components",
            "n_iter",
            "n_iter_without_progress",
            "n_jobs",
            "perplexity",
            "verbose",
        ]

        kwargs = {k: v for k, v in kwargs.items() if k in tsne_params}
        res = TSNE(random_state=0, **kwargs).fit_transform(data.values)
        return pd.DataFrame(res, index=data.index.values)

    @staticmethod
    def _learn_umap(data: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        Calculates UMAP transformation for given matrix features.
        Parameters
        --------
        data: np.array
            Array of features.
        kwargs: optional
            Parameters for ``umap.UMAP()``

        Returns
        -------
        pd.DataFrame
            Calculated UMAP transform

        """
        reducer = umap.UMAP()
        _umap_filter = reducer.get_params()
        kwargs = {k: v for k, v in kwargs.items() if k in _umap_filter}
        embedding = umap.UMAP(random_state=0, **kwargs).fit_transform(data.values)
        return pd.DataFrame(embedding, index=data.index.values)

    @staticmethod
    def __get_vectorizer(
        feature_type: Literal["count", "frequency", "tfidf", "binary", "markov"],
        ngram_range: NgramRange,
        corpus: pd.DataFrame | pd.Series[Any],
    ) -> TfidfVectorizer | CountVectorizer:
        if feature_type == "tfidf":
            return TfidfVectorizer(ngram_range=ngram_range, token_pattern="[^~]+").fit(corpus)  # type: ignore
        elif feature_type in ["count", "frequency"]:
            return CountVectorizer(ngram_range=ngram_range, token_pattern="[^~]+").fit(corpus)  # type: ignore
        else:
            return CountVectorizer(ngram_range=ngram_range, token_pattern="[^~]+", binary=True).fit(  # type: ignore
                corpus
            )

    def _sklearn_vectorization(
        self,
        events: pd.DataFrame,
        feature_type: SklearnFeatureType,
        ngram_range: NgramRange,
        weight_col: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        corpus = events.groupby(weight_col)[self.event_col].apply(lambda x: "~~".join([el.lower() for el in x]))
        vectorizer = self.__get_vectorizer(feature_type=feature_type, ngram_range=ngram_range, corpus=corpus)
        vocabulary_items = sorted(vectorizer.vocabulary_.items(), key=lambda x: x[1])
        cols: list[str] = [dict_key[0] for dict_key in vocabulary_items]
        sorted_index_col = sorted(events[weight_col].unique())
        vec_data = pd.DataFrame(index=sorted_index_col, columns=cols, data=vectorizer.transform(corpus).todense())
        vec_data.index.rename(weight_col, inplace=True)
        if feature_type == "frequency":
            # @FIXME: legacy todo without explanation, idk why. Vladimir Makhanov
            sum = cast(Any, vec_data.sum(axis=1))
            vec_data = vec_data.div(sum, axis=0).fillna(0)
        return events, vec_data

    def _markov_vectorization(self, events: pd.DataFrame, weight_col: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        next_event_col = "next_" + self.event_col
        next_time_col = "next_" + self.time_col
        events = events.sort_values([weight_col, self.event_index_col])
        events[[next_event_col, next_time_col]] = events.groupby(weight_col)[[self.event_col, self.time_col]].shift(-1)
        vec_data = (
            events.groupby([weight_col, self.event_col, next_event_col])[self.event_index_col]
            .count()
            .reset_index()
            .rename(columns={self.event_index_col: "count"})
            .assign(bigram=lambda df_: df_[self.event_col] + "~" + df_[next_event_col])
            .assign(left_event_count=lambda df_: df_.groupby([weight_col, self.event_col])["count"].transform("sum"))
            .assign(bigram_weight=lambda df_: df_["count"] / df_["left_event_count"])
            .pivot(index=weight_col, columns="bigram", values="bigram_weight")
            .fillna(0)
        )
        vec_data.index.rename(weight_col, inplace=True)
        del events[next_event_col]
        del events[next_time_col]
        return events, vec_data

    # TODO: add save
    def _cluster_bar(self, clusters: ndarray, target: list[list[bool]], target_names: list[str]) -> go.Figure:

        cl = pd.DataFrame([clusters, *target], index=["clusters", *target_names]).T
        cl["cluster size"] = 1
        for t_n in target_names:
            cl[t_n] = cl[t_n].astype(int)

        bars = (
            cl.groupby("clusters").agg({"cluster size": "sum", **{t_n: "mean" for t_n in target_names}}).reset_index()
        )
        bars["cluster size"] /= bars["cluster size"].sum()

        bars = bars.melt("clusters", var_name="type", value_name="value")
        bars = bars[bars["type"] != " "].copy()

        fig_x_size = round((1 + bars["clusters"].nunique() ** 0.7 * bars["type"].nunique() ** 0.7))
        rcParams["figure.figsize"] = fig_x_size, 6

        bar = sns.barplot(x="clusters", y="value", hue="type", data=bars)

        self._make_legend_and_ticks(bar)

        # adjust the limits
        ymin, ymax = bar.get_ylim()
        if ymax > 1:
            bar.set_ylim(ymin, 1.05)

        return bar

    @staticmethod
    def _kmeans(features: pd.DataFrame, n_clusters: int = 8, random_state: int = 0) -> np.ndarray:
        km = KMeans(random_state=random_state, n_clusters=n_clusters)
        cl = km.fit_predict(features.values)
        return cl

    @staticmethod
    def _gmm(features: pd.DataFrame, n_clusters: int = 8, random_state: int = 0) -> np.ndarray:
        km = GaussianMixture(random_state=random_state, n_components=n_clusters)
        cl = km.fit_predict(features.values)
        return cl

    def _prepare_targets(self, targets: list[str] | list[list[str]] | None) -> tuple[list[str], list[ndarray]]:
        events = self.__eventstream.to_dataframe()

        if self.__cluster_result is None:
            raise RuntimeError("Can't find the clustering results. Consider to run 'fit' method first.")

        grouped_events = events.groupby(self.user_col)[self.event_col]

        if targets is not None:
            targets_bool = []
            target_names = []

            formated_targets: list[list[str]] = []
            # format targets to list of lists:
            for n, i in enumerate(targets):
                if type(i) != list:  # type: ignore
                    formated_targets.append([i])  # type: ignore
                else:
                    formated_targets.append(i)  # type: ignore

            for t in formated_targets:
                # get name
                target_names.append("CR: " + " ".join(t))
                # get bool vector
                targets_bool.append(
                    grouped_events.apply(lambda x: bool(set(t) & set(x))).to_frame().sort_index()[self.event_col].values
                )

        else:
            targets_bool = [np.array([False] * len(self.__cluster_result))]
            target_names = [" "]
        return target_names, targets_bool

    def _plot_event_dist(
        self,
        bars: pd.DataFrame,
        cl1: int,
        sizes: list[float],
        weight_col: str | None,
        target_pos: int,
        targets: list[str],
        cl2: int | None,
    ) -> go.Figure:
        event_col = self.__eventstream.schema.event_name

        fig_x_size = round(2 + (bars.shape[0] // 2) ** 0.8)
        rcParams["figure.figsize"] = fig_x_size, 6

        bar = sns.barplot(
            x=event_col,
            y="freq",
            hue="hue",
            hue_order=[f"cluster {cl1}", "all" if cl2 is None else f"cluster {cl2}"],
            data=bars,
        )

        self._make_legend_and_ticks(bar)

        if weight_col is None:
            bar.set(ylabel="% from total events")
        else:
            bar.set(ylabel=f"% of '{weight_col}' with given event")
        bar.set(xlabel=None)

        # add vertical lines for central step-matrix
        if targets:
            bar.vlines([target_pos - 0.52], *bar.get_ylim(), colors="Black", linewidth=0.7, linestyles="dashed")

        # adjust the limits
        ymin, ymax = bar.get_ylim()
        if ymax > 1:
            bar.set_ylim(ymin, 1.05)

        tit = f"top {bars.shape[0] // 2 - len(targets)} events in cluster {cl1} (size: {round(sizes[0] * 100, 2)}%) \n"
        tit += f"vs. all data (100%)" if cl2 is None else f"vs. cluster {cl2} (size: {round(sizes[1] * 100, 2)}%)"
        bar.set_title(tit)

        return bar

    @staticmethod
    def _make_legend_and_ticks(bar: axes.Axes) -> None:
        # move legend outside the box
        bar.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.0)
        y_value = ["{:,.2f}".format(x * 100) + "%" for x in bar.get_yticks()]
        bar.set_yticks(bar.get_yticks().tolist())
        bar.set_yticklabels(y_value)
        bar.set_xticklabels(bar.get_xticklabels(), rotation=90)
