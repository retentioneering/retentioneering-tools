Funnels
~~~~~~~

Basic example
=============

This notebook can be found :download:`here <_static/examples/funnel_tutorial.ipynb>`
or open directly in `google colab <https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/docs/source/_static/examples/funnel_tutorial.ipynb>`__.

Conversion funnel is the basic first step in almost all product analytics workflow.
To learn how to plot basic funnels in Retentioneering framework let's work through a
basic example. To start we need to import retentioneering, load sample dataset and
update config:

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

Function rete.funnel() plot simple conversion funnel:

.. code:: ipython3

    data.rete.funnel(targets = ['catalog', 'cart', 'payment_done'])


.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_0.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

`targets` is required parameter for rete.funnel() function and is a list of
event names you are interested to plot on the funnel. For each specified target
event we calculate absolute number of user_id's, who reach this event at least
once. Also percentage numbes (relative to total number and relative to previous
stage) are shown.

Order of stages on the funnel corresponds to the order in which events are
passed in targets parameter.



Targets grouping
================

Sometimes during funnel analysis several events can have similar importance
and it doesn't matter which particular event was reached. In this case, multiple
events we would like to group as one stage can be passed as sub-list in targets
parameter. Let's plot a funnel where we group 'product1' and 'product2':

.. code:: ipython3

    data.rete.funnel(targets = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'])

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_1.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

You can now see new 'product1 | product2' stage on the funnel with 2010 unique users
who reached any product page (product1 or product2).

Users grouping
==============

Sometimes it is useful to compare funnels side-bi-side of several user groups.
For example, to have a quck comparison of funnels of users from different channels, or
from test and control groups in A/B test, or to compare multiple behavioral segments and etc.

This can be done by passing list of collections of user id's via
groups parameter. To illustrate this functionality let's plot funnels for two groups:
users who converted to 'payment_done' and users who did not. First, we need to obtain
two collections of user_ids and then pass it to groups parameters for rete.funnel function:

.. code:: ipython3

    g1 = set(data[data['event']=='payment_done']['user_id'])
    g2 = set(data['user_id']) - g1

    data.rete.funnel(targets = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
                     groups = (g1, g2),
                     group_names = ('converted', 'not_converted'))


.. raw:: html

            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_1b.html"
                frameborder="0"
                allowfullscreen
            ></iframe>

We can immediately see at the high level how two groups compare between each
other at particular stages. As expected not converted users are majority, and we
can see that most of non_converted users lost after visiting cart. Interestly,
for converted users we can see that some users add product to cart directly
from the catalog, without visiting product page (for converted users more unique users
visited cart page than product page).

Let's consider another example when we compare funnels between multiple users groups
segmented according to their behavior (read more about behavioral clustering
`here <https://retentioneering.github.io/retentioneering-tools/_build/html/clustering.html>`__).

First, let's cluster users with respect to their behavior:

.. code:: ipython3

    data.rete.get_clusters(method='kmeans',
                           n_clusters=8,
                           feature_type='tfidf',
                           ngram_range=(1,1));

With the clustering procedure above we grouped users together in a groups with
similar behavior. The dictionary containing lists of user ids for each cluster was
assigned to rete.cluster_mapping attribute. Now, let's plot funnels which compares
several obtained clusters:

.. code:: ipython3

    clus1_ids = data.rete.cluster_mapping[1]
    clus2_ids = data.rete.cluster_mapping[2]
    clus3_ids = data.rete.cluster_mapping[3]
    clus6_ids = data.rete.cluster_mapping[6]

    data.rete.funnel(targets = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
                     groups = (clus1_ids, clus2_ids, clus3_ids, clus6_ids),
                     group_names = ('cluster 1', 'cluster 2', 'cluster 3', 'cluster 6'))

.. raw:: html

            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_2.html"
                frameborder="0"
                allowfullscreen
            ></iframe>

After such funnel plot we can immediately have the intuition about obtained clusters.
Cluster 1 - very low motivated traffic which doesn't go deeper than catalog level,
cluster 2 - users who reach product level, but have lower conversion to cart, cluster 3 -
those are highly motivated users with most of the convertions, cluster 6 - users who reach
cart level but mostly churned somewhere between cart and payment_done events

To understand deeper what are the common behavioral patterns for each graph we can
`plot graphs <https://retentioneering.github.io/retentioneering-tools/_build/html/plot_graph.html>`__ or
`step matrix <https://retentioneering.github.io/retentioneering-tools/_build/html/step_matrix.html>`__.