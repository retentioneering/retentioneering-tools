Clusters
========

The following user guide is also available as `Google Colab notebook <https://colab.research.google.com/drive/1czRNCWcena5KlyPIJR7RRuXNQltl9mKQ?usp=share_link>`_.

Loading data
------------

Throughout this guide we use our demonstration :doc:`simple_shop </datasets/simple_shop>` dataset. It has already been converted to :doc:`Eventstream<eventstream>` and assigned to ``stream`` variable. If you want to use your own dataset, upload it following :ref:`this instruction<eventstream_creation>`.

.. code-block:: python

    import numpy as np
    import pandas as pd
    from retentioneering import datasets

    stream = datasets.load_simple_shop()\
        .split_sessions(session_cutoff=(30, 'm'))

Above we use an additional call of the :ref:`split_sessions<split_sessions>` data processor helper. We will need this session split in one of the examples in this user guide.

General usage
-------------

The primary way of using the ``Clusters`` tool is sort of traditional. You can create a separate instance of ``Clusters`` explicitly, and call ``fit()`` method then. As soon as the clusters are fitted, you can apply multiple tools for cluster analysis. A basic tool that shows cluster sizes and some other statistics is :ref:`plot()<clusters_plot>`.

.. code-block:: python

    from retentioneering.tooling.clusters import Clusters

    clusters = Clusters(eventstream=stream)
    clusters.fit(method='kmeans', n_clusters=4, feature_type='tfidf', ngram_range=(1, 1))
    clusters.plot()

.. figure:: /_static/user_guides/clusters/basic_plot.png

Fitting clusters
----------------

Fitting clusters is an obligatory step for cluster analysis. If a ``Clusters`` object is not fitted (i.e. the clusters are not defined), you can not use any cluster analysis tool. To do this, you can either use pre-defined clustering algorithms such as k-means, or pass custom user-cluster mapping.

Pre-defined clustering methods
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`Clusters.fit()<retentioneering.tooling.clusters.clusters.Clusters.fit>` is a method for fitting clusters. Its implementation is mainly based on sklearn clustering methods. Here is an example of such a fitting.

.. code-block:: python

    clusters = Clusters(eventstream=stream)
    clusters.fit(method='kmeans', n_clusters=4, feature_type='tfidf', ngram_range=(1, 1))

So far the ``method`` argument supports two options: :sklearn_kmeans:`kmeans<>` and :sklearn_gmm:`gmm<>`. ``n_clusters`` means the number of clusters since both K-means and GMM algorithms need it to be set.

.. _clusters_feature_type_ngram_range:

The following couple of arguments ``feature_type`` and ``ngram_range`` stands for the type of vectorization. By vectorization we mean the way user trajectories are converted to vectors in some feature space. In general, the vectorization procedure comprises two steps:

- Split user paths into short subsequences of particular length called ``n-grams``.
- Calculate some statistics taking into account how often each n-gram is represented in a user's trajectory.

``ngram_range`` argument controls the range of n-gram length to be used in the vectorization. For example, ``ngram_range=(1, 3)`` means that we are going to use n-grams of length 1 (single events, that is, *unigrams*), 2 (*bigrams*), and 3 (*trigrams*).

``feature type`` argument stands for the type of vectorization.  Besides standard ``tfidf``, ``count``, ``frequency``, and ``binary`` features, ``markov`` and time-related (``time`` and ``time_fraction``) features are available. See :py:meth:`Clusters.extract_features()<retentioneering.tooling.clusters.clusters.Clusters.extract_features>` for the details.

If this vectorization is not enough, you can use your custom features passing it as a pandas DataFrame to the ``vector`` argument.

Custom clustering
~~~~~~~~~~~~~~~~~

You can ignore the pre-defined clustering methods and set custom clusters. To do this, pass user-cluster mapping pandas Series to the :py:meth:`Clusters.set_clusters()<retentioneering.tooling.clusters.clusters.Clusters.set_clusters>` method. Once the method is called, the ``Clusters`` object is considered as fitted, so you can use the cluster analysis methods afterwards.

The following example demonstrates random splitting into 4 clusters. ``user_clusters`` variable holds the mapping information on how the users correspond to the clusters. We pass this variable next as an argument for ``set_clusters`` method.

.. code-block:: python

    import numpy as np

    user_ids = stream.to_dataframe()['user_id'].unique()
    np.random.seed(42)
    cluster_ids = np.random.choice([0, 1, 2, 3], size=len(user_ids))
    user_clusters = pd.Series(cluster_ids, index=user_ids)
    user_clusters

.. parsed-literal::

    219483890    2
    964964743    3
    629881394    0
    629881395    2
    495985018    2
                ..
    125426031    3
    26773318     3
    965024600    0
    831491833    1
    962761227    2
    Length: 3751, dtype: int64

.. code-block:: python

    clusters_random = Clusters(stream)
    clusters_random.set_clusters(user_clusters)
    clusters_random.plot()

.. figure:: /_static/user_guides/clusters/basic_plot_random_clustering.png

From this diagram, we see that the cluster sizes are close to each other which is exactly what we expect from random splitting.

Cluster analysis methods
------------------------

Visualization
~~~~~~~~~~~~~

.. _clusters_plot:

Basic cluster statistics
^^^^^^^^^^^^^^^^^^^^^^^^

The :py:meth:`Clusters.plot()<retentioneering.tooling.clusters.clusters.Clusters.plot>` method is used for visualizing basic cluster statistics. By default it shows the cluster sizes as the percentage of the eventstream users belonging to a specific cluster. If the ``targets`` parameter is defined, the conversion rate for each cluster and each target event is displayed as well. Conversion rate is the proportion of users belonging to a specific cluster who had a target event at least ones.

.. code-block:: python

    clusters.plot(targets=['cart', 'payment_done'])

.. figure:: /_static/user_guides/clusters/plot_target.png

The diagram above shows that cluster 0 contains ~40% of the eventstream users, 60% of them have at least one ``cart`` event in their trajectories, and only ~7% of them successfully paid at least once.

Projections
^^^^^^^^^^^

Since the feature spaces are of high dimensions, fitted clusters are hard to visualize. For this purpose 2D-projections are used. Due to the nature of projection, it provides a simplified picture, but at least it makes the visualization possible.

Our :py:meth:`Clusters.projection()<retentioneering.tooling.clusters.clusters.Clusters.projection>` implementation supports two popular and powerful dimensionality reduction techniques: :sklearn_kmeans:`TSNE<>` and :umap:`UMAP<>`.

.. code-block:: python

    clusters.projection(method='tsne')

.. figure:: /_static/user_guides/clusters/projection_tsne.png

In this image, each dot represents a single user. Users with similar behavior are located close to each other.

``plot_type='targets'`` along with ``targets`` argument color the projected dots with respect to conversion rates associated with the events defined in ``targets``. If at least one target event appeared in a user’s trajectory, the user is colored as converted.

.. code-block:: python

    clusters.projection(method='tsne', plot_type='targets', targets=['cart'])

.. figure:: /_static/user_guides/clusters/projection_targets.png

Exploring individual clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Essentially, any cluster splitting provides nothing but a mapping rule which assigns each user to some group. The way we understand why one cluster differs from another is always tricky. However, either we consider the entire eventstream or its subset (a user cluster), the exploration techniques may be the same. It means having a cluster defined, we can leave the users from this cluster and explore their paths. This is what :py:meth:`Clusters.filter_cluster()<retentioneering.tooling.clusters.clusters.Clusters.filter_cluster>` method is designed for. It returns the narrowed eventstream so we can apply any :doc:`Retentioneering path analysis tool</user_guide>` afterwards. In the following example we apply the :py:meth:`transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>` method.

.. code-block:: python

    clusters\
        .filter_cluster(cluster_id=0)\
        .add_start_end_events()\
        .transition_graph(
            targets={
                'positive': 'payment_done',
                'negative': 'path_end'
            }
        )

.. raw:: html

    <iframe
        width="680"
        height="630"
        src="../_static/user_guides/clusters/cluster_transition_graph.html"
        frameborder="0"
        allowfullscreen
    ></iframe>
    <br><br>

Here we additionally used :ref:`add_start_end_events<add_start_end_events>` data processor helper. It adds ``path_end`` event that is used as a negative target event in the transition graph.

Clusters comparison
~~~~~~~~~~~~~~~~~~~

It is natural to describe cluster characteristics in terms of event frequencies generated by the cluster users. :py:meth:`Clusters.event_dist()<retentioneering.tooling.clusters.clusters.Clusters.event_dist>` allows to do this. It takes the ``cluster_id1`` argument as a cluster to be described and plots ``top_n`` the most frequent events related to this cluster. In comparison, it shows the frequencies of the same events but within the ``cluster_id2`` cluster if the latter is defined. Otherwise, the frequencies over the entire eventstream are shown.

The next example demonstrates that within cluster 0 the ``catalog`` event takes ~37% of all events generated by the users from this cluster, whereas in the original eventstream the ``catalog`` event holds ~30% of all events only.

.. code-block:: python

    clusters.event_dist(cluster_id1=0)

.. figure:: /_static/user_guides/clusters/event_dist.png

The Clusters tool shares :ref:`the idea of using weighting column<transition_graph_weights>`. The most common values for this argument are ``user_id`` and ``session_id`` (assuming that the session split was created and ``session_id`` column exists). If you want to display such cluster statistics as the shares of the unique users or unique sessions, you can use the ``weight_col`` argument. Namely, for each event the proportion of the unique user paths/sessions where a particular event appear is calculated.

Also, in the example below we demonstrate the ``top_n`` argument that controls the number of the events
to be compared.

.. code-block:: python

    clusters.event_dist(cluster_id1=0, top_n=5, weight_col='user_id')

.. figure:: /_static/user_guides/clusters/plot_weight_col_user_id.png

Now, we see that 100% of the users in cluster 0 had at least one ``catalog`` event, whereas only 97% of the users in the entire eventstream had the same event.

Similarly, by defining ``weight_col='session_id'`` we get the following diagram:

.. code-block:: python

    clusters.event_dist(cluster_id1=0, top_n=5, weight_col='session_id')

.. figure:: /_static/user_guides/clusters/plot_weight_col_session_id.png


As we see from this diagram, if we look at the sessions generated by the users from cluster 0, only ~95% of these sessions contain at least one ``catalog`` event. In comparison, the sessions from the entire eventstream include ``catalog`` event only in ~83% of cases.

You can not only compare clusters with the whole eventstream, but with other clusters too. Simply define ``cluster_id2`` argument for that.

.. code-block:: python

    clusters.event_dist(cluster_id1=0, cluster_id2=1, top_n=5)

.. figure:: /_static/user_guides/clusters/plot_cluster1_cluster2.png

We see that the ``all`` bars from the previous diagram have been replaced with the ``cluster 1`` bars.

.. note::

    Some retentioneering tools support groups comparison. For cluster comparison you can also try to use :ref:`differential step matrix<step_matrix_differential>` or :ref:`segmental funnel<funnel_segments>`.

.. _clusters_clustering_results:

Getting clustering results
--------------------------

If you want to get the clustering results, there are two methods to do this.

:py:meth:`Clusters.user_clusters()<retentioneering.tooling.clusters.clusters.Clusters.user_clusters>` returns a pandas.Series containing user_ids as index and cluster_ids as values.

.. code-block:: python

    clusters.user_clusters

.. parsed-literal::

    219483890    2
    964964743    3
    629881394    0
    629881395    2
    495985018    2
                ..
    125426031    3
    26773318     3
    965024600    0
    831491833    1
    962761227    2
    Length: 3751, dtype: int64

:py:meth:`Clusters.cluster_mapping()<retentioneering.tooling.clusters.clusters.Clusters.cluster_mapping>`
returns a dictionary containing ``cluster_id`` → ``list of user_ids`` mapping.

.. code-block:: python

    cluster_mapping = clusters.cluster_mapping
    list(cluster_mapping.keys())

.. parsed-literal::

    [0, 1, 2, 3]

Now, we are explicitly confirmed that there are 4 clusters in the result. To get 10 user ids belonging to, say, cluster #0 we can use the following code:

.. code-block:: python

    cluster_mapping[0][:10]

.. parsed-literal::

    [2724645,
     4608042,
     5918715,
     6985523,
     7584012,
     7901023,
     8646372,
     8715027,
     8788425,
     10847418]

Extracting features
-------------------

In some scenarios, one might want to get the vectorized features that are used under the hood of the ``Clusters`` mechanisms. :py:meth:`Clusters.extract_features()<retentioneering.tooling.clusters.clusters.Clusters.extract_features>` is the method that is called inside :py:meth:`Clusters.fit()<retentioneering.tooling.clusters.clusters.Clusters.fit>`. It uses a couple of parameters ``feature_type`` and ``ngram_range`` that we :ref:`have already discussed<clusters_feature_type_ngram_range>`.

Note that if you use the features that are based on n-grams, they are named according to the following pattern ``event_1 ... event_n_FEATURE_TYPE``. For example, for a bigram ``cart`` → ``delivery_choice`` and ``feature_type='tfidf'``, the corresponding feature name will be ``cart delivery_choice_tfidf``.

As for the time-based features such as ``time``, ``time_fraction``, they are associated with a single event, so their names will be ``cart_time`` or ``delivery_choice_time_fraction``.

.. code-block:: python

    clusters.extract_features(ngram_range=(1, 2), feature_type='tfidf')

.. raw:: html

    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>cart_tfidf</th>
          <th>cart cart_tfidf</th>
          <th>...</th>
          <th>product2 catalog_tfidf</th>
          <th>product2 main_tfidf</th>
        </tr>
        <tr>
          <th>user_id</th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>122915</th>
          <td>0.050615</td>
          <td>0.0</td>
          <td>...</td>
          <td>0.145072</td>
          <td>0.0</td>
        </tr>
        <tr>
          <th>463458</th>
          <td>0.000000</td>
          <td>0.0</td>
          <td>...</td>
          <td>0.190706</td>
          <td>0.0</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>999916163</th>
          <td>0.517996</td>
          <td>0.0</td>
          <td>...</td>
          <td>0.000000</td>
          <td>0.0</td>
        </tr>
        <tr>
          <th>999941967</th>
          <td>0.000000</td>
          <td>0.0</td>
          <td>...</td>
          <td>0.000000</td>
          <td>0.0</td>
        </tr>
      </tbody>
    </table>
    <br>

If the features have been already calculated, you can get them by calling :py:meth:`Clusters.features<retentioneering.tooling.clusters.clusters.Clusters.features>` property.

.. code-block:: python

    clusters.features

Eventstream.clusters property
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There is another way to treat the Clusters tool. This way is aligned with the usage of the most retentioneering tools. Instead of creating an explicit Clusters class instance, you can use :py:meth:`Eventstream.clusters<retentioneering.eventstream.eventstream.Eventstream.clusters>` property. This property holds a Clusters instance that is embedded right into an eventstream.

.. code-block:: python

    clusters = stream.clusters
    clusters.fit(method='kmeans', n_clusters=4, feature_type='tfidf', ngram_range=(1, 1))
    clusters.plot()

You can use ``stream.clusters`` directly, not assigning it to a separate variable like this:

.. code-block:: python

    stream.clusters\
        .fit(method='kmeans', n_clusters=4, feature_type='tfidf', ngram_range=(1, 1))
    stream.clusters.plot()

.. note::

    Once ``Eventstream.clusters`` instance is created, it is kept inside the Eventstream object forever until the eventstream is alive. You can re-fit it as many times as you want, but you can not remove it.
