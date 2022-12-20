.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red

Clusters
========

``Clusters`` class holds the methods for cluster analysis. This section is designed as follows:

- How to fit clusters to an eventstream.
- How to analyze the fitted clusters.

All the examples below assume that you have a prepared eventstream ready for clustering analysis. Also, we assume that the initial eventstream is assigned as ``stream`` variable and the Clusters instance as ``clusters`` variable. simple_shop dataset is used as a data source.

The constructor of ``Clusters`` accepts a single parameter -- ``Eventstream``'s instance.

.. code:: ipython3

    import sys
    sys.path.insert(0, '~/rete/retentioneering-tools-new-arch/')
    from src import datasets
    from src.tooling.clusters import Clusters

    stream = datasets.load_simple_shop()
    clusters = Clusters(stream)


Fitting clusters
----------------

The major method which build the clusters is ``fit()``. Basically, the fitting procedure consists of two steps:

- Converting user paths to vectors (vectorization),

- Applying a clustering algorithm.

For vectorization we use a standard approach came from NLP area . We split the sequence into *n-grams* -- subsequences of length *n*. ``ngram_range`` parameter controls the range of *n*. I.e. ``(ngram_range)=(1, 2)`` means that subsequences of length 1 and 2 will be used in the vectorization.

The next vectorization question is how to treat the frequencies of a n-gram occurrences. There are many alternatives for this purpose included to ``feature_type`` parameter:

- ``count``: the number of occurrences of a given n-gram.

- ``binary``: 1 if a user had a given event at least once and 0 otherwise.

- ``frequency``: the same as ``count`` but normalized to the total number of the events in the user's trajectory.

- ``tfidf``: term frequency–inverse document frequency. The event's frequency in the user's trajectory but weighted to the overall frequency of the same event in the whole eventstream.

- ``markov``: available for bigrams only (``ngram_range=(2, 2)``). For a given bigram ``(A, B)`` the vectorized values are the user's transition probabilities from ``A`` to ``B``. An example. Assume a user has the following transitions: ``A->B`` 3 times, ``A->C`` 1 time, and ``A->A`` 4 times. Then the total number of the events the user has is 8, and the ``markov``-vectorized values for these bigrams are 3/8, 1/8, 1/2.

- ``time``: associated with unigrams only (``ngram_range=(1, 1)``). The total number of the seconds spent from the beginning of a user's path until a given event.

- ``time_fraction``: the same as ``time`` but divided by the total length of the user's trajectory (in seconds).


As for the clustering algorithms, ``method`` parameter stands for this choice. So far two algorithms are supported: ``kmeans`` and ``gmm``. Under the hood sklearn implementation of these algorithms is used.
    `https://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans <https://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans>`_

    `https://scikit-learn.org/stable/modules/generated/sklearn.mixture.GaussianMixture.html <https://scikit-learn.org/stable/modules/generated/sklearn.mixture.GaussianMixture.html>`_

Finally, here's how ``fit()`` method is commonly called:

.. code:: ipython3

    clusters.fit(method='kmeans', n_clusters=4, feature_type='tfidf', ngram_range=(1, 1))

In case you want to use your own user clustering apply ``set_clusters`` method. In the example below we create dummy 4 random clusters.

.. code:: ipython3

    user_ids = stream.to_dataframe()['user_id'].unique()
    cluster_ids = np.random.choice([0, 1, 2, 3], size=len(user_ids))
    user_clusters = pd.Series(cluster_ids, index=user_ids)

    clusters.set_clusters(user_clusters)

Once the clusters are fitted you can analyze them using the following methods:

.. table:: Cluster analysis methods overview
    :widths: 20 80
    :class: tight-table

    +----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | Method               | Description                                                                                                                                                      |
    +======================+==================================================================================================================================================================+
    | ``event_dist``       | Plots a bar plot illustrating the distribution of ``top_n`` events in cluster ``cluster_id1`` compared vs the entire dataset or vs cluster ``cluster_id2``.      |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``plot``             | Plots a bar plot illustrating the clusters sizes and the conversion rates of the ``targets`` events within the clusters.                                         |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``user_clusters``    | Returns ``user_id -> cluster_id`` mapping representing as ``pd.Series``. The index corresponds to user_ids, the values relate to the corresponding cluster_ids.  |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``cluster_mapping``  | Returns ``cluster_id -> list[user_ids]`` mapping.                                                                                                                |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``features``         | Returns the calculated features if the clusters are fitted. The index corresponds to user_ids, the columns are values of the vectorized user's trajectory.       |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``set_clusters``     | Sets custom user-cluster mapping.                                                                                                                                |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``filter_cluster``   | Truncates the eventstream and leaves the trajectories of the users who belong to the selected cluster.                                                           |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``extract_features`` | Calculates vectorized user paths.                                                                                                                                |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``projection``       | Shows the clusters projection on a plane applying dimension reduction techniques.                                                                                |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+

There are multiple nice visualizations which come handy in analyzing fitted clusters.

Visualizing methods
~~~~~~~~~~~~~~~~~~~

plot()
^^^^^^
This basic method allows to visualize a couple of important indicators: cluster size and
the conversion rate within a cluster. The latter is associated with ``targets`` parameters.

.. code:: ipython3

    cluster_mapping = clusters.cluster_mapping

event_dist()
^^^^^^^^^^^^

projection()
^^^^^^^^^^^^



Getting clustering results
--------------------------

cluster_mapping()
~~~~~~~~~~~~~~~~~


.. code:: ipython3

    cluster_mapping = clusters.cluster_mapping

user_clusters()
~~~~~~~~~~~~~~~




Methods for custom clustering
-----------------------------


Other methods
-------------

extract_features()
~~~~~~~~~~~~~~~~~~

features()
~~~~~~~~~~

filter_cluster()
~~~~~~~~~~~~~~~~

fit()
~~~~~

``fit()`` is a main ``Clusters`` method for fitting clusters to a given eventstream. Roughly, it represents eventstream user paths as vectors in some feature space and applies a clustering algorithm to this data. Once the clusters are fitted to the eventstream, the methods related to cluster analysis becomes available.


set_clusters()
~~~~~~~~~~~~~~

extract_features()
~~~~~~~~~~~~~~~~~~

Most machine learning algorithms including clustering algorithms deal with the data structured as a table. So the first step you need to do in clickstream cluster analysis is to convert user paths to numerical features. This procedure is called *vectorization* and this is what ``extract_features()`` method is designed for.

There are 5 vectorization types:


fit()
~~~~~

-

plot()
~~~~~~

event_dist()
~~~~~~~~~~~~
