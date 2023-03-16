.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red


Clusters
========
The following user guide is also available as `Google Colab notebook <https://colab.research.google.com/drive/1czRNCWcena5KlyPIJR7RRuXNQltl9mKQ?usp=share_link>`_.

Loading data
------------

Throughout this guide we use our demonstration :doc:`simple_shop </datasets/simple_shop>` dataset. It has already been converted to :doc:`Eventstream<eventstream>` and assigned to ``stream`` variable.

.. code-block:: python

    import numpy as np
    import pandas as pd
    from retentioneering import datasets

    stream = datasets.load_simple_shop()\
        .split_sessions(session_cutoff=(30, 'm'))

Above we use an additional call of :doc:`SplitSessions</api/data_processors/split_sessions>` data processor.
We’ll need this session info in one of the examples in this user guide.

General usage
-------------

The cases in this section demonstrate the ways you can treat ``Clusters``
tool. In each case we'll do the same thing:

- create ``Clusters`` instance,
- fit clusters with ``method='kmeans'``, ``n_clusters=4``, ``feature_type='tfidf'``, ``ngram_range=(1, 1)`` parameters,
- use a tool for cluster analysis (e.g. :py:meth:`plot()<retentioneering.tooling.clusters.clusters.Clusters.plot>` for simplicity).

Separate instance of Clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One way of using the ``Clusters`` tool is sort of traditional. You can
create a separate instance of ``Clusters`` explicitly, and call ``fit``
and ``plot`` methods then.

.. code-block:: python

    from retentioneering.tooling.clusters import Clusters

    clusters = Clusters(eventstream=stream)
    clusters.fit(method='kmeans', n_clusters=4, feature_type='tfidf', ngram_range=(1, 1))
    clusters.plot()

.. figure:: /_static/user_guides/clusters/basic_plot.png

Eventstream.clusters property
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:doc:`Eventstream.clusters</api/tooling/clusters>` property creates an instance
of ``Clusters`` class, stores it inside ``Eventstream`` object, and returns a
link to this instance. So you can either save this link as a separate variable
and treat it how we showed in the previous example:

.. code-block:: python

    clusters = stream.clusters
    clusters.fit(method='kmeans', n_clusters=4, feature_type='tfidf', ngram_range=(1, 1))
    clusters.plot()

or you can use ``stream.clusters`` link as is:

.. code-block:: python

    stream.clusters\
        .fit(method='kmeans', n_clusters=4, feature_type='tfidf', ngram_range=(1, 1))
    stream.clusters.plot()

We pay your attention that once created ``Eventstream.clusters`` is kept inside Eventstream
object forever until eventstream is alive. You can re-fit it as many times as you want, but
you can not remove it.

Fitting clusters
----------------

Fitting clusters is a core and obligatory step for cluster analysis. If the ``Clusters``
object is not fitted, you can not use any cluster analysis tool.

Retentioneering clustering
~~~~~~~~~~~~~~~~~~~~~~~~~~

A primary way to set clusters is to use :py:meth:`Clusters.fit()<retentioneering.tooling.clusters.clusters.Clusters.fit>` method.
It's implementation is mainly based on sklearn clustering methods. Here's an example of such a fitting.

.. code-block:: python

    clusters = Clusters(eventstream=stream)
    clusters.fit(method='kmeans', n_clusters=4, feature_type='tfidf', ngram_range=(1, 1))

So far ``method`` supports two options: :sklearn_kmeans:`kmeans<>` and :sklearn_gmm:`gmm<>`.
``n_clusters`` obviously means the number of clusters since both K-means and GMM
algorithms need it to be pre-defined.

The following couple of arguments ``feature type`` and ``ngram_range`` stands for the type
of vectorization. By vectorization we mean the way user trajectories are converted to vectors
in some feature space. In general, vectorization procedure comprises two steps:

- Split user paths into short subsequencies called ``n-grams``.
- Calculate some statistics taking into account how often each n_gram is represented in a user's trajectory.

``ngram_range`` argument controls the range of n-gram length to be used in the vectorization.
For example, ``ngram_range=(1, 3)`` means that we're going to use n-grams of length 1
(single events, that is, *unigrams*), 2 (*bigrams*), and 3 (*trigrams*).

``feature type`` argument stands for the type of vectorization.  Besides standard
``tfidf``, ``count``, ``frequency`` and ``binary`` features, ``markov`` and time-related
(``time`` and ``time_fraction``) features are available.
See :py:meth:`Clusters.extract_features()<retentioneering.tooling.clusters.clusters.Clusters.extract_features>`
for the details.

If this vectorization is not enough, you can use your own features passing it as a ``pandas.DataFrame``
to ``vector`` argument.

Custom clustering
~~~~~~~~~~~~~~~~~

We believe that advanced data scientists could tune a great clustering model
by their own, so all they need from Clusters module is just to upload
clustering results and then use Clusters analytical tools. In this case you can
use the results of your own clustering by passing ``pandas.Series`` representing
the mapping between the users and the clusters to
:py:meth:`Clusters.set_clusters()<retentioneering.tooling.clusters.clusters.Clusters.set_clusters>`
method. Once the method is called, the ``Clusters`` object is
considered as fitted, so you can call an analytical method afterwards.

The next example demonstrates random splitting into 4 clusters. ``user_clusters``
variable holds the mapping information on how the users correspond to the clusters.
We pass this variable next as an argument for ``set_clusters`` method.

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

From this plot we see that the cluster sizes are close to each other
which is exactly what we expect from random splitting.

Cluster analysis
----------------

Visualization
~~~~~~~~~~~~~

.. _clusters_plot:

Basic cluster statistics
^^^^^^^^^^^^^^^^^^^^^^^^

:py:meth:`Clusters.plot()<retentioneering.tooling.clusters.clusters.Clusters.plot>`
method is used for visualising basic cluster statistics.
By default it shows the cluster sizes as the percentage of the
eventstream users belonging to a specific cluster. If ``targets``
parameter is defined, the conversion rate for each cluster and
each target event is displayed as well. By conversion rate we mean
the proportion of the users belonging to a specific cluster
who had at least one target event.

.. code-block:: python

    clusters.plot(targets=['cart'])

.. figure:: /_static/user_guides/clusters/plot_target.png

The diagram above shows that cluster 0 contains ~40% of the
eventstream users, 60% of them have at least one ``cart``
event in their trajectories, and only ~7% of them paid at least
once.

Projections
^^^^^^^^^^^

Since the feature spaces are of high dimensions, fitted clusters are
hard to visualize. For this purpose 2D-projections are used. Due to
the nature of projection, it provides a simplified or event distorted
picture, but at least it makes clusters visualization possible.

Our
:py:meth:`Clusters.projection()<retentioneering.tooling.clusters.clusters.Clusters.projection>`
implementation supports two techniques, :sklearn_kmeans:`TSNE<>` and
:umap:`UMAP<>`, perhaps the most popular among contemporary dimensionality
reduction algorithms.

.. code-block:: python

    clusters.projection(method='tsne')

.. figure:: /_static/user_guides/clusters/projection_tsne.png

Each dot represents a single user. Users with similar behaviour are
located close to each other.

``plot_type='targets'`` along with ``targets`` argument color the
projected dots with respect to conversion rates associated with
the events defined in ``targets``. If at least one target event
appeared in a user’s trajectory, the user will be colored as converted.

.. code-block:: python

    clusters.projection(method='tsne', plot_type='targets', targets=['cart'])

.. figure:: /_static/user_guides/clusters/projection_targets.png

Exploring individual clusters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Essentially, any cluster splitting provides nothing but a mapping
rule which assigns each user to some group. The way we understand
why one clusters differs from another is always tricky, but either
we consider the entire eventstream or its subset (a user cluster)
the exploration techniques may be the same. It means having a cluster
defined we can narrow the entire eventstream and leave only the
paths belonging to the users from a particular cluster. This is what
:py:meth:`Clusters.filter_cluster()<retentioneering.tooling.clusters.clusters.Clusters.filter_cluster>`
method was designed for. It returns the narrowed eventstream so we can
apply any :doc:`Retentioneering analytical tool</user_guide>` afterwards.
In the following example we apply
:py:meth:`transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>`
method.

.. code-block:: python

    clusters\
        .filter_cluster(cluster_id=0)\
        .transition_graph(
            targets={
                'lost': 'bad',
                'payment_done': 'nice'
            }
        )

.. raw:: html

    <iframe
        width="600"
        height="600"
        src="../_static/user_guides/clusters/cluster_transition_graph.html"
        frameborder="0"
        allowfullscreen
    ></iframe>

Cluster comparison
~~~~~~~~~~~~~~~~~~

It's natural to describe cluster characteristics in terms of event
frequencies generated by the users from the cluster.
:py:meth:`Clusters.event_dist()<retentioneering.tooling.clusters.clusters.Clusters.event_dist>`
allows to do this. It takes ``cluster_id1`` cluster to be described
and plots ``top_n`` the most frequent events related to this cluster.
In comparison, it shows the frequencies of the same events but within
``cluster_id2`` if the latter is defined. Otherwise, the frequencies
over the entire eventstream are shown.

The next example demonstrates that within cluster 0 event ``catalog`` takes
~37% of all events generated by the users from this cluster, whereas
in the original eventstream ``catalog`` event holds ~30% of all events only.

.. code-block:: python

    clusters.event_dist(cluster_id1=0)

.. figure:: /_static/user_guides/clusters/event_dist.png

Such definition of event frequency often is not convenient since it's hard
to interpret. One may consider to use ``weight_col`` argument instead which
normalize event frequencies with respect to the defined column. The most
common argument values are ``user_id'`` and ``session_id`` (assuming that
the session split was created and ``session_id`` column exists).
Thus, ``weight_col='user_id'`` displays the fractions of the users who had
at least one particular event. ``weight_col='session_id'`` displays the
fractions of the sessions which contain at least one particular event.

Also we use ``top_n`` argument which controls the number of the events
to be compared.

.. code-block:: python

    clusters.event_dist(cluster_id1=0, top_n=5, weight_col='user_id')

.. figure:: /_static/user_guides/clusters/plot_weight_col_user_id.png

Now, we see that 100% of the users in cluster 0 had at least one ``catalog``
event, whereas only 97% of the users in the entire eventstream had the
same event.

Similarly, defining ``weight_col='user_id'`` we get the following diagram:

.. code-block:: python

    clusters.event_dist(cluster_id1=0, top_n=5, weight_col='session_id')

.. figure:: /_static/user_guides/clusters/plot_weight_col_session_id.png


As we see from this diagram, if we look at the sessions generated
by the users from cluster 0, only ~95% of these sessions contain
at least one ``catalog`` event. In comparison, the sessions from
the entire eventstream contain ``catalog`` event only in ~83% of cases.

You can not only comparing clusters with the whole eventstream, but
with other clusters too. Simply define ``cluster_id2`` argument for
that.

.. code-block:: python

    clusters.event_dist(cluster_id1=0, cluster_id2=1, top_n=5)

.. figure:: /_static/user_guides/clusters/plot_cluster1_cluster2.png

We see that ``all`` value in the diagram legend has been replaced
with ``cluster 1`` value.

.. note ::

    Some retentioneering tools support groups comparison. For cluster
    comparison you can also try to use differential :doc:`step matrix </api/tooling/step_matrix>`
    (i.e. with ``groups`` argument defined) or :doc:`funnel </api/tooling/funnel>`
    with ``segments`` argument.

.. _clusers_clustering_results:

Getting clustering results
~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to explicitly get the results of the clustering (i.e.
mapping rule ``user_id -> cluster_id``), there are two methods
to do this.

:py:meth:`Clusters.user_clusters()<retentioneering.tooling.clusters.clusters.Clusters.user_clusters>`
returns a ``pandas.Series`` containing user_ids as index and cluster_ids
as values.

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
returns a dictionary containing ``cluster_id -> list[user_ids]`` mapping.

.. code-block:: python

    cluster_mapping = clusters.cluster_mapping
    list(cluster_mapping.keys())

.. parsed-literal::

    [0, 1, 2, 3]

.. code-block:: python

    list(cluster_mapping.values())[0][:10]

.. parsed-literal::

    [629881394,
     729416583,
     24427596,
     730545582,
     836120732,
     428990197,
     753512589,
     968444450,
     190361938,
     754402650]


Extracting features
~~~~~~~~~~~~~~~~~~~

In some scenarios one might want to get the vectorized features
which ``Clusters`` can calculate.
:py:meth:`Clusters.extract_features()<retentioneering.tooling.clusters.clusters.Clusters.extract_features>`
is the method which is called inside
:py:meth:`Clusters.fit()<retentioneering.tooling.clusters.clusters.Clusters.fit>`.
It uses a couple of parameters ``feature_type`` and ``ngram_range``.
See :py:meth:`Clusters.fit()<retentioneering.tooling.clusters.clusters.Clusters.fit>` for the details.

Note that feature names which are based on ngrams are designed according
to the following pattern ``event_1 ... event_n_FEATURE_TYPE``. For example,
for a bigram `cart -> delivery_choice` and `feature_type='tfidf'` the
corresponding feature name will be `cart delivery_choice_tfidf`.

As for time-based features (`time`, `time_fraction`), they are associated
with a single event, so their names would be `cart_time` or
`delivery_choice_time_fraction`

.. code-block:: python

    clusters.extract_features(ngram_range=(1, 1), feature_type='tfidf')

.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>cart_tfidf</th>
          <th>cart cart_tfidf</th>
          <th>...</th>
          <th>session_start catalog_tfidf</th>
          <th>session_start main_tfidf</th>
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
          <td>0.049744</td>
          <td>0.0</td>
          <td>...</td>
          <td>0.000000</td>
          <td>0.09694</td>
        </tr>
        <tr>
          <th>463458</th>
          <td>0.000000</td>
          <td>0.0</td>
          <td>...</td>
          <td>0.102726</td>
          <td>0.00000</td>
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
          <td>0.431186</td>
          <td>0.0</td>
          <td>...</td>
          <td>0.172471</td>
          <td>0.00000</td>
        </tr>
        <tr>
          <th>999941967</th>
          <td>0.000000</td>
          <td>0.0</td>
          <td>...</td>
          <td>0.400147</td>
          <td>0.00000</td>
        </tr>
      </tbody>
    </table>
    <p>3751 rows × 68 columns</p>
    </div>


If the clusters are already fitted, instead of calculate
:py:meth:`Clusters.extract_features()<retentioneering.tooling.clusters.clusters.Clusters.extract_features>`
explicitly, you can use ``clusters.features`` property which
returns ``pandas.DataFrame`` representing the calculated features.

.. code-block:: python

    clusters.features
