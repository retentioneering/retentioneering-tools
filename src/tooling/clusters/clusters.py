from __future__ import annotations

from functools import partial
from typing import Any, List, Literal, Tuple, cast

import matplotlib.pylab as plt
import numpy as np
import pandas as pd
import seaborn as sns
import umap.umap_ as umap
from matplotlib import rcParams
from numpy import ndarray, unique
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.manifold import TSNE
from sklearn.mixture import GaussianMixture

from src.eventstream.types import EventstreamType
from src.tooling.clusters.segments import Segments

FeatureType = Literal["tfidf", "count", "frequency", "binary", "time", "time_fraction", "external", "markov"]
NgramRange = Tuple[int, int]
Method = Literal["kmeans", "gmm"]
PlotType = Literal["cluster_bar"]
PlotProjectionMethod = Literal["tsne", "umap"]


class Clusters:
    """
    Class gathers tools for cluster analysis.

    Attributes
    ----------
    eventstream: EventstreamType
    user_clusters: dict[str | int, list[int]] | None = None
        If ```dict``` Clusters can work with results of external clustering.
        If ```None``` with the method ```create_clusters``` :py:func:`src.tooling.clusters.clusters.create_clusters`
    """

    __eventstream: EventstreamType
    __clusters_list: ndarray
    __segments: Segments | None

    def __init__(self, eventstream: EventstreamType, user_clusters: dict[str | int, list[int]] | None = None):
        self.__eventstream: EventstreamType = eventstream
        self.__segments = None
        self._user_clusters = user_clusters
        if user_clusters:
            self._set_user_clusters(user_clusters)
        else:
            self.__clusters_list = ndarray(shape=(0, 0))
        self.__projection = None

    # public API
    def extract_features(self, feature_type: FeatureType = "tfidf", ngram_range: NgramRange | None = None):
        """
        Calculates vectorized user paths.

        Parameters
        ----------

        feature_type : {"tfidf", "count", "frequency", "binary", "markov"}, default="tfidf"

            - ``tfidf`` see details in :sklearn_tfidf:`sklearn documentation<>`
            - ``count`` see details in :sklearn_countvec:`sklearn documentation<>`
            - ``frequency`` an alias for ``count``.
            - ``binary`` uses the same CountVectorizer as ``count``, but with ``binary=True`` flag.
            - | ``markov`` available for bigrams only. The vectorized values are
              | associated with the transition probabilities in the corresponding Markov chain.
              | For Example: Assume a users has the following transitions: A->B 3 times, A->C 1 time, and A->A 4 times.
              | Then the vectorized values for these bigrams are 0.375, 0.125, 0.5.
        ngram_range : Tuple(int, int), default=(1, 1)
            The lower and upper boundary of the range of n-values for different word n-grams or char n-grams to be
            extracted. For example, ngram_range=(1, 1) means only single events, (1, 2) means single events
            and bigrams. Doesn't work for ``markov`` feature_type.

        Returns
        -------
        pd.DataFrame
            A DataFrame with the vectorized values. Index contains user_id, columns contain n-grams.
        """
        extract_features_partial = partial(self._extract_features, eventstream=self.__eventstream)
        return extract_features_partial(feature_type=feature_type, ngram_range=ngram_range)

    def create_clusters(
        self,
        feature_type: FeatureType = "tfidf",
        ngram_range: NgramRange = (1, 1),
        n_clusters: int = 8,
        method: Method = "kmeans",
        refit_cluster: bool = True,
        targets: list[str] | None = None,
        vector: pd.DataFrame | None = None,
    ):
        if self._user_clusters:
            targets_bool = [[True] * x for x in [len(y) for y in self._user_clusters.values()]]
            target_names: list[str] = list(map(str, list(self._user_clusters.keys())))
        else:
            target_names, targets_bool = self._prepare_clusters(
                feature_type=feature_type,
                method=method,
                n_clusters=n_clusters,
                ngram_range=ngram_range,
                refit_cluster=refit_cluster,
                targets=targets,
                vector=vector,
            )

        return self._cluster_bar(
            clusters=self.__clusters_list,  # type: ignore
            target=cast(List[List[bool]], targets_bool),  # @TODO: fix types. Vladimir Makhanov
            target_names=target_names,
        )

    def set_user_clusters(self, user_clusters: dict[str | int, list[int]]) -> None:
        self._set_user_clusters(user_clusters=user_clusters)

    @property
    def user_clusters(self) -> dict[str | int, list[int]] | None:
        return self._user_clusters

    @user_clusters.setter
    def user_clusters(self, user_clusters: dict[str | int, list[int]]):
        self._set_user_clusters(user_clusters=user_clusters)

    @property
    def calculated_clusters(self) -> dict:
        clusters = unique(self.__clusters_list)
        readable_data: dict[str | int, list[str | int]] = {x: list() for x in clusters}
        for row_num, cluster in enumerate(self.__clusters_list):
            readable_data[cluster].append(row_num)
        return readable_data

    def narrow_eventstream(self, cluster: int | str) -> EventstreamType:
        from src.eventstream.eventstream import Eventstream

        eventstream: Eventstream = self.__eventstream  # type: ignore
        cluster_events = []
        if self._user_clusters:
            cluster_events = self._user_clusters[cluster]
        else:
            pass
        df = self.__eventstream.to_dataframe()
        df = df[df[self.__eventstream.schema.event_id].isin(cluster_events)]
        es = Eventstream(
            raw_data=df,
            raw_data_schema=eventstream.schema.to_raw_data_schema(),
            schema=eventstream.schema.copy(),
        )
        return es

    def projection(
        self,
        method: PlotProjectionMethod = "tsne",
        targets: list[str] | None = None,
        ngram_range: NgramRange | None = None,
        feature_type: FeatureType = "tfidf",
        plot_type=None,
        **kwargs,
    ):
        """
        Does dimension reduction of user trajectories and draws projection plane.

        Parameters
        ----------
        method: {'umap', 'tsne'} (optional, default 'tsne')
            Type of manifold transformation.
        plot_type: {'targets', 'clusters', None} (optional, default None)
            Type of color-coding used for projection visualization:
                - 'clusters': colors trajectories with different colors depending on cluster number.
                IMPORTANT: must do .rete.get_clusters() before to obtain cluster mapping.
                - 'targets': color trajectories based on reach to any event provided in 'targets' parameter.
                Must provide 'targets' parameter in this case.
            If None, then only calculates TSNE without visualization.
        targets: list or tuple of str (optional, default  ())
            Vector of event_names as str. If user reach any of the specified events, the dot corresponding
            to this user will be highlighted as converted on the resulting projection plot
        feature_type: str, (optional, default 'tfidf')
            Type of vectorizer to use before dimension-reduction. Available vectorization methods:
            {'tfidf', 'count', 'binary', 'frequency', 'markov'}
        ngram_range: tuple, (optional, default (1,1))
            The lower and upper boundaries of the range of n-values for different
            word n-grams or char n-grams to be extracted before dimension-reduction.
            For example ngram_range=(1, 1) means only single events, (1, 2) means single events
            and bigrams. Doesn't work for ``markov`` feature_type.
        Returns
        --------
        Dataframe with data in the low-dimensional space for user trajectories indexed by user IDs.
        Return type
        --------
        pd.DataFrame
        """

        if targets is None:
            targets = []
        if ngram_range is None:
            ngram_range = (1, 1)

        event_col = self.__eventstream.schema.event_name
        index_col = self.__eventstream.schema.user_id

        if plot_type == "clusters":
            if self.__clusters_list.any():
                targets_mapping = self.__clusters_list
                legend_title = "cluster number:"
            else:
                raise AttributeError(
                    "Run .rete.get_clusters() before using plot_type='clusters' to obtain clusters mapping"
                )

        elif plot_type == "targets":
            if (not targets) and (len(targets) < 1):
                raise ValueError(
                    "When plot_type ='targets' must provide parameter targets as list of target event names"
                )
            else:
                targets = [list(pd.core.common.flatten(targets))]  # type: ignore
                legend_title = "conversion to (" + " | ".join(targets[0]).strip(" | ") + "):"  # type: ignore
                targets_mapping = (
                    self.__eventstream.to_dataframe()
                    .groupby(index_col)[event_col]
                    .apply(lambda x: bool(set(*targets) & set(x)))
                    .to_frame()
                    .sort_index()[event_col]
                    .values
                )
        else:
            raise ValueError("Unexpected plot type: %s. Allowed values: clusters, targets" % plot_type)

        features = self.extract_features(feature_type=feature_type, ngram_range=ngram_range)

        if method == "tsne":
            projection: pd.DataFrame = self._learn_tsne(features, **kwargs)
        elif method == "umap":
            projection = self._learn_umap(features, **kwargs)
        else:
            raise ValueError("Unknown method: %s. Allowed methods: tsne, umap" % method)

        self.__projection = projection

        # return only embeddings is no plot_type:
        if plot_type is None:
            return projection

        self._plot_projection(
            projection=projection.values,
            targets=targets_mapping,
            legend_title=legend_title,
        )

        return projection

    # inner functions

    def _plot_projection(self, projection, targets, legend_title):
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

    def _learn_tsne(self, data, **kwargs):
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
        Calculated TSNE transform
        Return type
        -------
        np.ndarray
        """

        TSNE_PARAMS = [
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

        kwargs = {k: v for k, v in kwargs.items() if k in TSNE_PARAMS}
        res = TSNE(random_state=0, **kwargs).fit_transform(data.values)
        return pd.DataFrame(res, index=data.index.values)

    def _learn_umap(self, data, **kwargs):
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
        Calculated UMAP transform
        Return type
        -------
        np.ndarray
        """
        reducer = umap.UMAP()
        _umap_filter = reducer.get_params()
        kwargs = {k: v for k, v in kwargs.items() if k in _umap_filter}
        embedding = umap.UMAP(random_state=0, **kwargs).fit_transform(data.values)
        return pd.DataFrame(embedding, index=data.index.values)

    def _set_user_clusters(self, user_clusters: dict[str | int, list[int]]) -> None:
        # @TODO: add some validation. Vladimir Makhanov
        self._user_clusters = user_clusters

        correct_data = {}
        for cluster, rows in user_clusters.items():
            _ = {row: cluster for row in rows}
            correct_data.update(_)

        correct_data = dict(sorted(correct_data.items()))
        self.__clusters_list = ndarray(list(correct_data.values()))

    def __get_vectorizer(
        self,
        feature_type: Literal["count", "frequency", "tfidf", "binary", "markov"],
        ngram_range: NgramRange,
        corpus,
    ) -> TfidfVectorizer | CountVectorizer:
        if feature_type == "tfidf":
            return TfidfVectorizer(ngram_range=ngram_range, token_pattern="[^~]+").fit(corpus)  # type: ignore
        elif feature_type in ["count", "frequency"]:
            return CountVectorizer(ngram_range=ngram_range, token_pattern="[^~]+").fit(corpus)  # type: ignore
        else:
            return CountVectorizer(ngram_range=ngram_range, token_pattern="[^~]+", binary=True).fit(  # type: ignore
                corpus
            )

    def _extract_features(
        self, eventstream: EventstreamType, feature_type: FeatureType = "tfidf", ngram_range: NgramRange | None = None
    ):
        if ngram_range is None:
            ngram_range = (1, 1)
        index_col = eventstream.schema.user_id
        event_col = eventstream.schema.event_name
        time_col = eventstream.schema.event_timestamp

        events = eventstream.to_dataframe()
        vec_data = None

        if (
            feature_type == "count"
            or feature_type == "frequency"
            or feature_type == "tfidf"
            or feature_type == "binary"
        ):
            events, vec_data = self._sklearn_vectorization(
                events, feature_type, ngram_range, index_col, eventstream.schema
            )

        elif feature_type == "markov":
            events, vec_data = self._markov_vectorization(events, index_col, eventstream.schema)

        if feature_type in ["time", "time_fraction"]:
            events.sort_values(by=[index_col, time_col], inplace=True)
            events.reset_index(inplace=True)
            events["time_diff"] = events.groupby(index_col)[time_col].diff().dt.total_seconds()  # type: ignore
            events["time_length"] = events["time_diff"].shift(-1)
            if feature_type == "time_fraction":
                vec_data = (
                    events.groupby([index_col])
                    .apply(lambda x: x.groupby(event_col)["time_length"].sum() / x["time_length"].sum())
                    .unstack(fill_value=0)
                )
            elif feature_type == "time":
                vec_data = (
                    events.groupby([index_col])
                    .apply(lambda x: x.groupby(event_col)["time_length"].sum())
                    .unstack(fill_value=0)
                )

        if vec_data is not None:
            vec_data.columns = [col + "_" + feature_type for col in vec_data.columns]

        return cast(pd.DataFrame, vec_data)

    def _sklearn_vectorization(
        self, events, feature_type, ngram_range, index_col, schema
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        event_col = schema.event_name
        corpus = events.groupby(index_col)[event_col].apply(lambda x: "~~".join([el.lower() for el in x]))
        vectorizer = self.__get_vectorizer(feature_type=feature_type, ngram_range=ngram_range, corpus=corpus)
        vocabulary_items = sorted(vectorizer.vocabulary_.items(), key=lambda x: x[1])
        cols: list[str] = [dict_key[0] for dict_key in vocabulary_items]
        sorted_index_col = sorted(events[index_col].unique())
        vec_data = pd.DataFrame(index=sorted_index_col, columns=cols, data=vectorizer.transform(corpus).todense())
        vec_data.index.rename(index_col, inplace=True)
        if feature_type == "frequency":
            # @FIXME: legacy todo without explanation, idk why. Vladimir Makhanov
            sum = cast(Any, vec_data.sum(axis=1))
            vec_data = vec_data.div(sum, axis=0).fillna(0)
        return events, vec_data

    @staticmethod
    def _markov_vectorization(events, index_col, schema) -> tuple[pd.DataFrame, pd.DataFrame]:
        event_col = schema.event_name
        event_index_col = schema.event_index
        time_col = schema.event_timestamp

        next_event_col = "next_" + event_col
        next_time_col = "next_" + time_col
        events = events.sort_values([index_col, event_index_col])
        events[[next_event_col, next_time_col]] = events.groupby(index_col)[[event_col, time_col]].shift(-1)
        vec_data = (
            events.groupby([index_col, event_col, next_event_col])[event_index_col]
            .count()
            .reset_index()
            .rename(columns={event_index_col: "count"})
            .assign(bigram=lambda df_: df_[event_col] + "~" + df_[next_event_col])
            .assign(left_event_count=lambda df_: df_.groupby([index_col, event_col])["count"].transform("sum"))
            .assign(bigram_weight=lambda df_: df_["count"] / df_["left_event_count"])
            .pivot(index=index_col, columns="bigram", values="bigram_weight")
            .fillna(0)
        )
        vec_data.index.rename(index_col, inplace=True)
        del events[next_event_col]
        del events[next_time_col]
        return events, vec_data

    # TODO: add save
    def _cluster_bar(self, clusters: ndarray, target: list[list[bool]], target_names: list[str]):
        """
        Plots bar charts with cluster sizes and average target conversion rate.
        Parameters
        ----------
        data : pd.DataFrame
            Feature matrix.
        clusters : "np.array"
            Array of cluster IDs.
        target: "np.array"
            Boolean vector, if ``True``, then user has `positive_target_event` in trajectory.
        target: list[np.ndarray]
            Boolean vector, if ``True``, then user has `positive_target_event` in trajectory.
        kwargs: optional
            Width and height of plot.
        Returns
        -------
        Saves plot to ``retention_config.experiments_folder``
        Return type
        -------
        PNG
        """
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

        # move legend outside the box
        bar.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.0)

        y_value = ["{:,.2f}".format(x * 100) + "%" for x in bar.get_yticks()]

        bar.set_yticks(bar.get_yticks().tolist())
        bar.set_yticklabels(y_value)
        bar.set(ylabel=None)

        # adjust the limits
        ymin, ymax = bar.get_ylim()
        if ymax > 1:
            bar.set_ylim(ymin, 1.05)

        return bar

    def _kmeans(self, features: pd.DataFrame, n_clusters: int = 8, random_state: int = 0) -> np.ndarray:

        km = KMeans(random_state=random_state, n_clusters=n_clusters)

        cl = km.fit_predict(features.values)

        return cl

    def _gmm(self, features: pd.DataFrame, n_clusters: int = 8, random_state: int = 0) -> np.ndarray:

        km = GaussianMixture(random_state=random_state, n_components=n_clusters)

        cl = km.fit_predict(features.values)

        return cl

    def _prepare_clusters(self, feature_type, method, n_clusters, ngram_range, refit_cluster, targets, vector):
        user_col = self.__eventstream.schema.user_id
        event_col = self.__eventstream.schema.event_name
        if feature_type == "external" and not isinstance(vector, pd.DataFrame):  # type: ignore
            raise ValueError("Vector is not a DataFrame!")
        if feature_type == "external" and vector is not None:
            # Check consistency and copy vector to features
            if np.all(np.all(vector.dtypes == "float") and vector.isna().sum().sum() == 0):
                features = vector.copy()
            else:
                raise ValueError(
                    "Vector is wrong formatted! NaN should be replaced with 0 and dtypes all must be float!"
                )
        else:
            features = self._extract_features(
                eventstream=self.__eventstream,
                feature_type=feature_type,
                ngram_range=ngram_range,
            )
        users_ids: pd.Series = features.index.to_series()
        if self.__segments is None or refit_cluster:
            if method == "kmeans":
                clusters_list = self._kmeans(features=features, n_clusters=n_clusters)
            elif method == "gmm":
                clusters_list = self._gmm(
                    features=features,
                    n_clusters=n_clusters,
                )
            else:
                raise ValueError("Unknown method: %s" % method)

            self.__clusters_list = clusters_list

            users_clusters = users_ids.to_frame().reset_index(drop=True)
            users_clusters["segment"] = pd.Series(clusters_list)

            self.__segments = Segments(
                eventstream=self.__eventstream,
                segments_df=users_clusters,
            )
        events = self.__eventstream.to_dataframe()
        grouped_events = events.groupby(user_col)[event_col]
        target_names, targets_bool = self._prepare_targets(event_col, grouped_events, targets)
        return target_names, targets_bool

    def _prepare_targets(self, event_col, grouped_events, targets):
        if targets is not None:
            targets_bool = []
            target_names = []

            formated_targets = []
            # format targets to list of lists:
            for n, i in enumerate(targets):
                if type(i) != list:  # type: ignore
                    formated_targets.append([i])
                else:
                    formated_targets.append(i)  # type: ignore

            for t in formated_targets:
                # get name
                target_names.append("CR: " + " ".join(t))
                # get bool vector
                targets_bool.append(
                    (grouped_events.apply(lambda x: bool(set(t) & set(x))).to_frame().sort_index()[event_col].values)
                )

        else:
            targets_bool = [np.array([False] * len(self.__clusters_list))]
            target_names = [" "]
        return target_names, targets_bool
