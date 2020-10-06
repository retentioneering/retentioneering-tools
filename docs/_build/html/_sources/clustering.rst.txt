Users behavior clustering
~~~~~~~~~~~~~~~~~~~~~~~~~

Basic example
=============

This notebook can be found :download:`here <_static/examples/clusters_tutorial.ipynb>`
or open directly in
`google colab <https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/docs/source/_static/examples/clusters_tutorial.ipynb>`__.


We will use a sample user activity dataset to illustrate how behavior clustering works. Let’s first import retentioneering, import sample dataset and update config to set used column names:

.. code:: ipython3

    import retentioneering

    # load sample user behavior data as a pandas dataframe:
    data = retentioneering.datasets.load_simple_shop()

    # update config to pass columns names:
    retentioneering.config.update({
        'user_col': 'user_id',
        'event_col':'event',
        'event_time_col':'timestamp',
    })

Trajectories vectorization
==========================

Each user trajectory is represented as a sequence of events. Before we apply any ML algorithms to users dataset we need a way to convert each user trajectory from a sequence of events to a numerical vector. This field of ML learning extensively was developed in applications for text processing. Text analysis in some sense is similar to analysis of discrete user trajectories of behavioural logs. In text processing each text document (in our case - user trajectory) consists of discrete words (in our case - event names) and we need to convert text to numerical values. Let’s work through some examples.

Function rete.extract_features() returns a dataframe of vectorized user trajectories:

.. code:: ipython3

    vec = data.rete.extract_features(feature_type='count',
                                     ngram_range=(1, 1))

In the obtained dataframe each row corresponds to a vector representing each user from the original dataset. Each column (or vector dimension) corresponds to unique events in the dataset and the values are how many times a particular event was present in this user’s trajectory. These are supported types of vectorization (parameter feature_type):

    * ‘count’ : number of occurrences of given event
    * ‘binary’ : 1 if user had given event at least once and 0 otherwise
    * ‘frequency’ : same as count but normalized to total number of events in user trajectory
    * ‘tfidf’ : term frequency–inverse document frequency, frequency of event in user trajectory but weighted to overall frequency of event in the dataset.


Second important parameter for extract_features is ngram_range, which sets the lower and upper limit for event sequences to be extracted. For example an ngram_range of (1, 1) means only individual events, (1, 2) means unigrams and bigrams of events, and (2, 2) means only bigrams of events.

Clusterization
==============

After we know general idea about user trajectories vectorization we can now use get_clusters method to split users on groups based on how similar is their behavior:

.. code:: ipython3

    data.rete.get_clusters(method='kmeans',
                           n_clusters=8,
                           feature_type='tfidf',
                           ngram_range=(1,2));

Under the hood each user trajectory (sequence of event names) got transformed to a numeric vector. In the example above we used ‘ftidf’ vectorization (default vectorizer), where vocabulary is sequences of events from 1 to 2 (parameter ngram_range), meaning that we count individual events up to sequences of 2 (bi-grams).

Parameter n_clusters corresponds to the number of desired clusters. Parameter method - type of clusterization algorithm to use (currently support ‘kmeans’ and ‘gmm’).

Result of the method above is assigned to a new rete attribute: cluster_mapping, which is a dictionary containing user_id’s for each cluster:


.. code:: ipython3

    data.rete.cluster_mapping

.. parsed-literal::

    {0: [7584012,
      7901023,
      10847418,
      12133064,
      15882438,
      20104222,
    ...,
    1: [463458,
      1475907,
      10007545,
      10768877,
      10769994,

Now, if we need to obtain all user_id’s from a specific cluster, it can be done very easily using cluster_mapping dictionary. For example:
.. code:: ipython3

    clus_2 = data.rete.cluster_mapping[2]

here, clus_2 will contain all user_id’s of users from cluster 2.

Visualizing results
===================

Very often it is useful to have a high-level overview of the results of clusterization immediately after clusterization was done. Clusters statistics can be shown with the clusterization by including plot_type parameter:

.. code:: ipython3

    data.rete.get_clusters(method='kmeans',
                           feature_type='tfidf',
                           n_clusters=8,
                           ngram_range=(1,2),
                           plot_type='cluster_bar');

.. image:: _static/clustering/clustering_0.svg

By default it shows the relative size of each cluster. We can add conversion to any specified event to the clusters statistics using parameter targets, where we can specify target events. High-level overview bar plot will now include conversion rate (% of users within the cluster who have specified event at least once) for specified target:

.. code:: ipython3

    data.rete.get_clusters(method='kmeans',
                           feature_type='tfidf',
                           n_clusters=8,
                           ngram_range=(1,2),
                           plot_type='cluster_bar',
                           targets=['payment_done']);

.. image:: _static/clustering/clustering_1.svg

Parameter targets can contain any number of events. For each added event, corresponding conversion rate will be included to cluster overview bar plot. This is very useful when you need to get a quick intuition about the resulting clusters:

.. code:: ipython3

    data.rete.get_clusters(method='kmeans',
                           feature_type='tfidf',
                           n_clusters=8,
                           ngram_range=(1,2),
                           plot_type='cluster_bar',
                           targets=['payment_done','cart']);

.. image:: _static/clustering/clustering_2.svg

In example above we can see that clusters 4 and 5 have relatively high conversion rates to purchase compared to other clusters (CR: ‘payment_done’). Interestingly, cluster 0 has very high conversion to visit ‘cart’ (same as clusters 4 and 5) but don’t have any conversions to ‘payment_done’. This must be a cluster of users who reach the cart but get lost somewhere between cart and payment_done. This way we can immediately start building our intuition about resulting clusters.

Exploring individual clusters
=============================

After clusterization is done we can explore individual clusters using a full arsenal of retentioneering tools. Function filter_cluster can be used to isolate individual dataset for a given cluster number or list of clusters:

.. code:: ipython3

    clus_0 = data.rete.filter_cluster(0)

Now, clus_0 is a regular pandas dataframe containing only users from cluster 0. Since it is regular pandas dataframe we can directly apply rete tools such as plot_graph or step_matrix to explore it:

.. code:: ipython3

    clus_0.rete.plot_graph(thresh=0.1,
                           weight_col='user_id',
                           targets = {'lost':'red',
                                      'payment_done':'green'})

.. raw:: html


            <iframe
                width="700"
                height="600"
                src="_static/clustering/index_0.html"
                frameborder="0"
                allowfullscreen
            ></iframe>

|

We can see that this cluster #0 consists of users who explore catalog, products 1 and 2, then reach the ‘cart’, but lost after the cart. To see how users in cluster 0 get to the cart we can plot step_matrix centered around cart:

.. code:: ipython3

    clus_0.rete.step_matrix(max_steps=12,
                            centered={'event': 'cart',
                                      'left_gap': 4,
                                      'occurrence': 1});

.. image:: _static/clustering/clustering_3.svg

Other clusters can be explored in a similar way. Note, that dataframe containing multiple clusters can be extracted by passing a list of cluster numbers to filter_cluster() function. For example, if we would like to obtain dataset only containing users from clusters 4 and 5 for subsequent analysis, we can simply do:

.. code:: ipython3

    clus_4_5 = data.rete.filter_cluster([4,5])

Compare clusters
================

Function rete.cluster_event_dist() helps to quickly understand at a high level behavior pattern within a given cluster by comparing the distribution of top_n events within selected cluster vs all dataset or with another cluster. Let’s see an example. Suppose we would like to explore cluster 2, which has a low conversion rate to ‘payment_done’ event.

.. code:: ipython3

    data.rete.cluster_event_dist(2)

.. image:: _static/clustering/cluster_event_dist_0.svg

We can immediately see the distribution of events (by default top_n = 8) within selected cluster 2 compared with the distribution from the whole dataset. Percents on Y axis correspond to how frequently a given event is present in the given cluster. On the histogram above we can see that users from cluster 2 are much more often interacting with product 2 compared with the entire dataset.

We can also compare two clusters between each other. For this we need to pass two positional arguments corresponding to cluster numbers.


.. code:: ipython3

    data.rete.cluster_event_dist(2, 7)

.. image:: _static/clustering/cluster_event_dist_1.svg

Here we can see a comparison of top 8 frequent events in cluster 2 vs cluster 7. We can see that cluster 7 is similar to cluster 2. Both clusters have low conversion rate, but users from cluster 7 more frequently interact with product 1 whereas users from cluster 2 interact with product 2.

Note, that in the above example Y-axis values were showing the percentage that a given event represented from the selected cluster. Very often we are actually more interested to compare percentages of users who have particular events between two groups. This type of normalization can be used by passing the name of the index column we would like to normalize by. In our case it’s user_id’s: weight_col=’user_id’ (default None):


.. code:: ipython3

    data.rete.cluster_event_dist(2, 7,
                                 weight_col='user_id')

.. image:: _static/clustering/cluster_event_dist_2.svg

Now in the histogram above we can see that actually 100% of users from cluster 2 have interacted with product 2 and 100% of users from cluster 7 have interacted with product 1. It gives. All users from both clusters have interacted with catalog and were lost (no conversion). Interestingly, non-converted users who interacted with product 2 (from cluster 2) are more likely to visit cart (35% of users) before they are lost, than lost users who interacted with product 1 (20% of users from cluster 7). This effect was difficult to notice when we compared cluster 2 and 7 without weight_col=’user_id’ normalization.

If there are some events of particular importance which you always want to include in comparison (regardless of selected top_n parameter) you can pass those events as a list as targets parameter. Those events will always appear in comparison histogram on the right after the dashed line (in the same order as specified):


.. code:: ipython3

    data.rete.cluster_event_dist(2,
                                 weight_col='user_id',
                                 targets=['cart','payment_done'])

.. image:: _static/clustering/cluster_event_dist_3.svg

Also you can compare users flow from different segments using
`differential step matrix <https://retentioneering.github.io/retentioneering-tools/_build/html/step_matrix.html#differential-step-matrix>`__

Visualize cluster using project()
=================================

Sometimes it is useful to have a high-level overview of your users trajectories to have a clear visual control over the final separation of your clusters. This can be performed by dimension reduction techniques, when multidimensional vectorization is applied to user trajectories, transforming them into 2D vectors. After such transformation we can visualize every behavior of every user on a single plane where each user will be represented with a single dot. 
This dimension-reduction transformation is performed in a way that approximately conserves the original distances between user points of high-dimensional space, where the cluster search algorithm was applied. Therefore, users with similar behavior will get transformed into very close dots on a plane, while the users with differences in their behavioral patterns would appear as very separated points on the plane. Due to limitations of 2D dimensions sometimes transformation may introduce distortions, so that originally well separated clusters may appear overlapping. This might be the case when you try to process data overloaded with different types of events, building original high dimensional space as a very sparse structure. Keep in mind that UMAP and TSNE are not deterministic algorithms, so different runs of procedure may lead to different results of transformation, keeping the overall pair distance structure in an approximate manner. 
Retentioneering library provides tools for two popular transformation methods: `TSNE <https://scikit-learn.org/stable/modules/generated/sklearn.manifold.TSNE.html>`__ and `UMAP <https://umap-learn.readthedocs.io/en/latest/index.html>`__. Let’s explore some examples:


You can also visualize clusterization results using rete.project() function (read below how it works). After you run clustering as in this notebook above, you can pass plot_type ='clusters':

.. code:: ipython3

    data.rete.project(plot_type ='clusters',
                      method='tsne',
                      perplexity=128);

.. image:: _static/clustering/project_4.svg

The distances on this 2D map are a good indicator of behavioral proximity between users. Please take a note of clusters' locations and let's try to plot this map by visualizing whether users of these clusters were converted into the target events. We can define our targets with targets argument, for example in case we want to explore conversions into the 'cart' event: targets = ['cart'] and providing the option plot_type ='targets'. Now we can see, that cluster 4 contains most of the 'cart' visitors, whereas cluster 1 represents users with very distinct and clear low cart conversion behavior:

.. code:: ipython3

    data.rete.project(plot_type ='targets',
                      targets = ['cart'],
                      method='tsne');

.. image:: _static/clustering/project_0.svg

As keyword arguments to project() function you can pass any parameters supported by `scikit-learn tsne <https://scikit-learn.org/stable/modules/generated/sklearn.manifold.TSNE.html>`__ implementation. For example:

.. code:: ipython3

    data.rete.project(plot_type ='targets',
                      targets = ['cart'],
                      method='tsne',
                      perplexity = 128);

.. image:: _static/clustering/project_1.svg

Analagously, if you use method='umap', you can pass any additional `umap parameters <https://umap-learn.readthedocs.io/en/latest/api.html>`__ to project() function.

Parameter targets (list of event names) used to highlight users who reach any target event vs those who have not. For example, we can highlight users on the projection map who reach the product page (product1 or product2):

.. code:: ipython3

    data.rete.project(plot_type ='targets',
                      targets = ['product1', 'product2'],
                      method='tsne',
                      perplexity = 128);

.. image:: _static/clustering/project_2.svg

.. code:: ipython3

    data.rete.project(plot_type ='targets',
                      targets = ['payment_done'],
                      method='tsne',
                      perplexity = 128);

.. image:: _static/clustering/project_3.svg

Unlike other 2D plots the coordinates of these TSNE and UMAP projections does not have actual units - the ticks and the mesh is provided only to assist exploration with clear metric for distance beetween users and to locate particular user on the plot. The best interpretation of the plots is that we can see the geographical map of user locations corresponding to their behavioral similarities within each other.

