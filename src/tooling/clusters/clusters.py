from __future__ import annotations

from typing import Any, List, Literal, Tuple, Union, cast

import numpy
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import rcParams
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.mixture import GaussianMixture

from src.eventstream.types import EventstreamType

SEGMENTS_COLNAME = "segment"
SegmentVal = Union[int, str]
COUNT_COL_NAME = "count"
UserClass = Union[str, int]


class UserList:
    # readonly
    __users: pd.DataFrame
    __eventstream: EventstreamType

    def __init__(self, eventstream: EventstreamType) -> None:
        self.__eventstream = eventstream
        self.__users = self.__make_userlist()

    def get_eventstream(self):
        return self.__eventstream

    def to_dataframe(self):
        return self.__users.copy()

    def add_classes(self, colname: str, classes: pd.DataFrame) -> None:
        user_col = self.__eventstream.schema.user_id
        self.__users.reset_index(inplace=True, drop=True)
        merged = self.__users.merge(classes, on=user_col, how="left")
        merged.reset_index(inplace=True, drop=True)
        self.__users[colname] = merged[colname]

    def assign(self, colname: str, value: UserClass, users: pd.Series[Any] | list[Any]) -> None:
        user_col = self.__eventstream.schema.user_id
        matched = self.__users[user_col].isin(users)
        matched_users = self.__users[matched].copy()
        matched_users[colname] = value
        source_col = self.__users[colname]
        source_col.update(matched_users[colname])

    def get_count(self, colname: str) -> int:
        usercol = self.__eventstream.schema.user_id
        r = self.__users.groupby([colname])[usercol].count().reset_index()
        return r

    def mark_eventstream(self, colname: str, inplace: bool = False):
        eventstream = self.__eventstream if inplace else self.__eventstream.copy()

        usercol = eventstream.schema.user_id
        eventstream_df = eventstream.to_dataframe()

        users = self.__users[[usercol, colname]]
        merged = eventstream_df.merge(users, how="left", on=usercol)
        marked_col = merged[colname]
        eventstream.add_custom_col(name=colname, data=marked_col)
        return eventstream

    def get_eventstream_subset(self, colname: str, values: list[UserClass] = None) -> pd.Series[Any]:
        usercol = self.__eventstream.schema.user_id
        matched = self.__users[colname].isin(values=values)
        users_subset = self.__users[matched]
        eventstream_dataframe = self.__eventstream.to_dataframe(copy=True)
        _df = eventstream_dataframe[eventstream_dataframe[usercol].isin(users_subset[usercol])]
        return _df

    def __make_userlist(self) -> pd.Series[Any]:
        user_col = self.__eventstream.schema.user_id
        id_col = self.__eventstream.schema.event_id

        events = self.__eventstream.to_dataframe()
        users = events.groupby([user_col])[id_col].count().reset_index()

        users = users.sort_values(by=id_col, ascending=False)

        users.reset_index(inplace=True, drop=True)
        users.rename(columns={id_col: COUNT_COL_NAME}, inplace=True)

        return users


class Segments:
    # readonly
    __userlist: UserList
    __eventstream: EventstreamType

    def __init__(
        self,
        eventstream: EventstreamType,
        segments_df: pd.DataFrame = None,
    ):
        self.__userlist = UserList(eventstream=eventstream)
        self.__eventstream = eventstream

        if segments_df is not None:
            self.__userlist.add_classes(SEGMENTS_COLNAME, segments_df)
        else:
            # add empty segments
            userlist_df = self.__userlist.to_dataframe()
            userlist_df[SEGMENTS_COLNAME] = numpy.nan
            self.__userlist.add_classes(SEGMENTS_COLNAME, userlist_df)

    def show_segments(self):
        user_id = self.__eventstream.schema.user_id
        return self.__userlist.to_dataframe()[[user_id, SEGMENTS_COLNAME]]

    def add_segment(self, segment: SegmentVal, users: Union[pd.Series, List]):
        self.__userlist.assign(SEGMENTS_COLNAME, value=segment, users=users)

    def get_users(self, segment: SegmentVal):
        user_id = self.__eventstream.schema.user_id
        userlist_df = self.__userlist.to_dataframe()
        return userlist_df[userlist_df[SEGMENTS_COLNAME] == segment][user_id]

    def get_all_users(self):
        user_id = self.__eventstream.schema.user_id
        return self.__userlist.to_dataframe()[user_id]

    def get_all_segments(self):
        userlist_df = self.__userlist.to_dataframe()
        return userlist_df[SEGMENTS_COLNAME].unique()

    def get_segment_list(self):
        userlist_df = self.__userlist.get_count(SEGMENTS_COLNAME)
        return userlist_df


FeatureType = Literal["tfidf", "count", "frequency", "binary", "time", "time_fraction", "external"]
NgramRange = Tuple[int, int]
Method = Literal["kmeans", "gmm"]
PlotType = Literal["cluster_bar"]


class Clusters:
    __eventstream: EventstreamType
    __clusters_list: list[int]
    segments: Segments | None

    def __init__(self, eventstream: EventstreamType):
        self.__eventstream = eventstream
        self.segments = None

    def __get_vectorizer(
        self,
        feature_type: Literal["count", "frequency", "tfidf", "binary"],
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

    def extract_features(
        self, eventstream: EventstreamType, feature_type: FeatureType = "tfidf", ngram_range: NgramRange = (1, 1)
    ):
        index_col = eventstream.schema.user_id
        event_col = eventstream.schema.event_id
        time_col = eventstream.schema.event_timestamp

        events = eventstream.to_dataframe()

        corpus = events.groupby(index_col)[event_col].apply(lambda x: "~~".join([el.lower() for el in x]))

        vec_data = None

        if (
            feature_type == "count"
            or feature_type == "frequency"
            or feature_type == "tfidf"
            or feature_type == "binary"
        ):
            vectorizer = self.__get_vectorizer(feature_type=feature_type, ngram_range=ngram_range, corpus=corpus)

            vocabulary_items = sorted(vectorizer.vocabulary_.items(), key=lambda x: x[1])
            cols: List[str] = [dict_key[0] for dict_key in vocabulary_items]
            sorted_index_col = sorted(events[index_col].unique())

            vec_data = pd.DataFrame(index=sorted_index_col, columns=cols, data=vectorizer.transform(corpus).todense())
            vec_data.index.rename(index_col, inplace=True)

            if feature_type == "frequency":
                # TODO: fix me
                sum = cast(Any, vec_data.sum(axis=1))
                vec_data = vec_data.div(sum, axis=0).fillna(0)

        if feature_type in ["time", "time_fraction"]:
            events.sort_values(by=[index_col, time_col], inplace=True)
            events.reset_index(inplace=True)
            events["time_diff"] = events.groupby(index_col)[time_col].diff().dt.total_seconds()
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

    # TODO: add save
    def cluster_bar(self, clusters: List[int], target: List[List[bool]], target_names: List[str], plot_name=None):
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
        target: list of np.arrays
            Boolean vector, if ``True``, then user has `positive_target_event` in trajectory.
        plot_name : str, optional
            Name of plot to save. Default: ``'clusters_bar_{timestamp}.svg'``
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

    def kmeans(self, features: pd.DataFrame, n_clusters: int = 8, random_state: int = 0):

        km = KMeans(random_state=random_state, n_clusters=n_clusters)

        cl = km.fit_predict(features.values)

        return cl

    def gmm(self, features: pd.DataFrame, n_clusters: int = 8, random_state: int = 0):

        km = GaussianMixture(random_state=random_state, n_components=n_clusters)

        cl = km.fit_predict(features.values)

        return cl

    def create_clusters(
        self,
        feature_type: FeatureType = "tfidf",
        ngram_range: NgramRange = (1, 1),
        n_clusters: int = 8,
        method: Method = "kmeans",
        plot_type: PlotType = None,
        refit_cluster: bool = True,
        targets: List[str] = None,
        vector: pd.DataFrame = None,
    ):
        user_col = self.__eventstream.schema.user_id
        event_col = self.__eventstream.schema.event_id

        if feature_type == "external" and not isinstance(vector, pd.DataFrame):
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
            features = self.extract_features(
                eventstream=self.__eventstream,
                feature_type=feature_type,
                ngram_range=ngram_range,
            )

        users_ids: pd.Series = features.index.to_series()

        if self.segments is None or refit_cluster:
            if method == "kmeans":
                clusters_list = self.kmeans(features=features, n_clusters=n_clusters)
            else:
                clusters_list = self.gmm(
                    features=features,
                    n_clusters=n_clusters,
                )

            self.__clusters_list = clusters_list

            users_clusters = users_ids.to_frame().reset_index(drop=True)
            users_clusters["segment"] = pd.Series(clusters_list)

            self.segments = Segments(
                eventstream=self.__eventstream,
                segments_df=users_clusters,
            )

        events = self.__eventstream.to_dataframe()
        grouped_events = events.groupby(user_col)[event_col]

        targets_bool = []
        target_names = [" "]

        if targets is not None:
            targets_bool = []
            target_names = []

            formated_targets = []
            # format targets to list of lists:
            for n, i in enumerate(targets):
                if type(i) != list:
                    formated_targets.append([i])
                else:
                    formated_targets.append(i)

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

        if plot_type == "cluster_bar":
            self.cluster_bar(
                clusters=self.__clusters_list,
                # TODO: fix types
                target=cast(List[List[bool]], targets_bool),
                target_names=target_names,
            )
        return targets_bool
