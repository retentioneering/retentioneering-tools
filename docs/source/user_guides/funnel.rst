Funnel
======

This notebook can be open directly in
`Google Colab <https://colab.research.google.com/drive/1VjFXazgIdMKLyHaqMoKTWhnq5_29lRIs?usp=share_link>`_

Basic example
-------------

Conversion funnel is the basic first step in almost all product
analytics workflow. To learn how to plot basic funnels in
Retentioneering framework let’s work through a basic example.

In order to start, we need to:

- import required libraries
- load sample dataset
- create ``eventstream`` object @TODO: Link to explanation of eventstream. dpanina.

.. code-block:: python

    # import retentioneering
    from src.eventstream import Eventstream
    # @TODO: check imports in final version. dpanina

    # load sample user behavior data as a pandas dataframe:
    raw_data = pd.read_csv('retentioneering-tools-new-arch/src/datasets/data/simple-onlineshop.csv')  # @TODO: либо использовать датафрейм, либо прописать загрузку эвентстрима. j.ostanina

    # create source eventstream
    source = Eventstream(raw_data)


Creating an instance of the Funnel class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At the moment when an instance of a class is created, it is still
``naive``. To pass it the parameters specified in brackets, you need to
use the ``.fit()`` method.

.. code-block:: python

    from src.tooling.funnel import Funnel

    funnel = Funnel(
        eventstream=source,
        stages = ['catalog', 'cart', 'payment_done']
    )
    funnel.fit()
    funnel.plot()

.. raw:: html

            <iframe
                width="700"
                height="400"
                src="../_static/funnel/funnel_0_basic.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

Customization
-------------

Stages
~~~~~~

Stages is required parameter for funnel() method, and it is a list of
event names you are interested to observe in the funnel. For each
specified stage we calculate and show:

- absolute unique number of user_id’s who reach this stage at least once.
- percentage from the first stage (“% of initial”)
- percentage from the previous stage (“% of previous”)

The order of stages on the funnel plot corresponds to the order in which
events are passed in ``stages`` parameter.

Stage grouping
~~~~~~~~~~~~~~

Sometimes during funnel analysis several events can have similar
importance, and it doesn’t matter which particular event was reached. In
this case, we would like to group multiple events as one stage, and they
can be passed as sub-list in ``stage`` parameter.

Let’s plot a funnel where we group ``product1`` and ``product2``:

.. code-block:: python

    funnel = Funnel(
        eventstream=source,
        stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done']
    )
    funnel.fit()
    funnel.plot()

.. raw:: html

            <iframe
                width="700"
                height="400"
                src="../_static/funnel/funnel_1_stages.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

You can now see new ``product1 | product2`` stage on the funnel with
2010 unique users who reached any product page
(``product1 or product2``). NOTE: If one user has both events in his
path he will be counted as one unique user.

Stage names
~~~~~~~~~~~

If you need to group long list of events, you have two ways:

#. return to preprocessing and use grouping data processor (See @TODO: Link to
   preprocessing. dpanina)
#. give a new name to your group just to see the plot, without changing your ``eventstream``

Let’s turn to the second method. We can use ``stage_names`` parameter.
This list should be the same length as ``stages``.

.. code-block:: python

    funnel = Funnel(
        eventstream=source,
        stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
        stage_names = ['catalog', 'product', 'cart', 'payment_done']
    )
    funnel.fit()
    funnel.plot()

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="../_static/funnel/funnel_2_stage_names.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

Funnel type and sequence parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Parameter ``funnel_type`` has two possible options:

#. ``open`` - it’s default value and we use it when only the user presence on the
   stage is significant. And we don’t care about the order of the stages in
   user’s path and also about if user was only on first or on all previous
   stages.
#. ``closed`` - in return can be of two types:

    - If it is important to see only users who were on the first stage and analyse the
      funnel stages only after passing it. In the other words, user path
      before the first stage of the funnel dropped and then funnel is built
      according to the rules of the ``open`` funnel. Parameter
      ``sequence=False`` should be used in that case.
    - If it is important to look at the users who move to each next stage
      only if earlier they were on all previous ones. Parameter ``sequence=True``
      should be used in that case.

In order to feel the difference - see very simple example (@TODO: Link
to API reference funnel. dpanina)

Let’s build ``closed`` funnel with ``sequence=False``.

With comparison to ``open`` funnel we can see that some users come to
``cart`` not from ``catalog`` or ``product`` stages. And real conversion
from these stages is lower than we saw in ``open`` funnel.

.. code-block:: python

    funnel = Funnel(
        eventstream=source,
        stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
        stage_names = ['catalog', 'product', 'cart', 'payment_done'],
        funnel_type='closed'
    )
    funnel.fit()
    funnel.plot()

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="../_static/funnel/funnel_3_closed.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

And let’s take a look at the most strict funnel ``funnel_type=closed``
and ``sequence=True`` Here the conversion to the ``cart`` even lower
than in ``funnel_type=closed`` and ``sequence=False``. That’s mean that
some users who visit ``catalog`` go strait to the cart and it can be
basicly another type of users (for example who was on this web-site
before and left some products in the cart earlier or there is another
way to reach ``cart`` stage)

.. code-block:: python

    funnel = Funnel(
        eventstream=source,
        stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
        stage_names = ['catalog', 'product', 'cart', 'payment_done'],
        funnel_type='closed',
        sequence=True
    )
    funnel.fit()
    funnel.plot()

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="../_static/funnel/funnel_4_sequence.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

User segments
~~~~~~~~~~~~~

Sometimes it is useful to compare funnels stage-by-stage of several user
segments. For example, to have a quick comparison of funnels of users:

- from different channels
- from test and control groups in A/B test
- to compare multiple behavioral segments and etc.

This can be done by passing list of collections of user id’s via groups
parameter. To illustrate this functionality let’s plot funnels for two
groups: users who converted to ``payment_done`` and users who did not.
First, we need to obtain two collections of ``user_ids`` and then pass
it to groups parameters for ``eventstream.funnel()`` method:

.. code-block:: python

    source_df = source.to_dataframe()
    segment1 = set(source_df[source_df['event'] == 'payment_done']['user_id'])
    segment2 = set(source_df['user_id']) - segment1

    funnel = Funnel(
        eventstream=source,
        stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
        stage_names = ['catalog', 'product', 'cart', 'payment_done'],
        segments = (segment1, segment2),
        segment_names = ('converted', 'not_converted')
    )
    funnel.fit()
    funnel.plot()

.. raw:: html

            <iframe
                width="700"
                height="400"
                src="../_static/funnel/funnel_5_segments_open.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

We can immediately see at the high level how two groups compare between
each other at particular stages. As expected ``not_converted`` users are
majority, and we can see that most of ``not_converted`` users lost after
visiting cart. Interestly, for converted users we can see that some
users add product to cart directly from the catalog, without visiting
product page (for converted users more unique users visited cart page
than product page).

Now let’s have a look at the ``closed`` funnel:

.. code-block:: python

    funnel = Funnel(
        eventstream=source,
        stages=['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
        stage_names=['catalog', 'product', 'cart', 'payment_done'],
        funnel_type='closed',
        segments=(segment1, segment2),
        segment_names=('converted', 'not_converted')
    )
    funnel.fit()
    funnel.plot()

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="../_static/funnel/funnel_6_segments_closed.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

It is interesting to notice that our hypothesis about the fact that
users add product to cart directly from the catalog is incorrect, and
those users appear in the ``cart`` from the others stages, not from
``catalog``.

Clustering
^^^^^^^^^^

Let’s consider another example when we compare funnels between multiple
users groups segmented according to their behavior - clustering.

.. code-block:: python

    from src.tooling.clusters import Clusters

    clusters = Clusters(eventstream=source)
    clusters.fit(method='kmeans',
                 n_clusters=8,
                 feature_type='tfidf',
                 ngram_range=(1,1));

With the clustering procedure above we grouped users together in a
groups with similar behavior. The dictionary containing lists of user
ids for each cluster was assigned to ``rete.cluster_mapping`` attribute.
Now, let’s plot funnels which compares several obtained clusters:

.. code-block:: python

    clus1_ids = clusters.cluster_mapping[1]
    clus2_ids = clusters.cluster_mapping[2]
    clus3_ids = clusters.cluster_mapping[3]
    clus6_ids = clusters.cluster_mapping[6]

    funnel = Funnel(
        eventstream=source,
        stages=['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
        segments=(clus1_ids, clus2_ids, clus3_ids, clus6_ids),
        segment_names=('cluster 1', 'cluster 2', 'cluster 3', 'cluster 6'))
    funnel.fit()
    funnel.plot()

.. raw:: html

            <iframe
                width="700"
                height="400"
                src="../_static/funnel/funnel_7_clusters.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

To understand deeper what are the common behavioral patterns for each
graph we can plot graphs or step matrix. (@TODO: Link to graphs and step
matrix. dpanina)

ShortCut for Funnel (as an eventstream method)
----------------------------------------------

By default, the ``.plot()`` method is called.

.. code-block:: python

    ff = source.funnel(stages = ['catalog', 'cart', 'payment_done']);

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="../_static/funnel/funnel_8_eventstream.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

.. code-block:: python

    ff.values

.. raw:: html

    <div>
    <style scoped>
        .dataframe tbody tr th:only-of-type {
            vertical-align: middle;
        }

        .dataframe tbody tr th {
            vertical-align: top;
        }

        .dataframe thead th {
            text-align: right;
        }
    </style>
    <table border="1" class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th></th>
          <th>unique_users</th>
          <th>%_of_initial</th>
          <th>%_of_total</th>
        </tr>
        <tr>
          <th>segment_name</th>
          <th>stages</th>
          <th></th>
          <th></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th rowspan="3" valign="top">all users</th>
          <th>catalog</th>
          <td>3611</td>
          <td>100.00</td>
          <td>100.00</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>1924</td>
          <td>53.28</td>
          <td>53.28</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>653</td>
          <td>33.94</td>
          <td>18.08</td>
        </tr>
      </tbody>
    </table>
    </div>

.. code-block:: python

    source.funnel(stages = ['catalog', 'cart', 'payment_done'], show_plot=False).values

.. raw:: html

    <div>
    <style scoped>
        .dataframe tbody tr th:only-of-type {
            vertical-align: middle;
        }

        .dataframe tbody tr th {
            vertical-align: top;
        }

        .dataframe thead th {
            text-align: right;
        }
    </style>
    <table border="1" class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th></th>
          <th>unique_users</th>
          <th>%_of_initial</th>
          <th>%_of_total</th>
        </tr>
        <tr>
          <th>segment_name</th>
          <th>stages</th>
          <th></th>
          <th></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th rowspan="3" valign="top">all users</th>
          <th>catalog</th>
          <td>3611</td>
          <td>100.00</td>
          <td>100.00</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>1924</td>
          <td>53.28</td>
          <td>53.28</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>653</td>
          <td>33.94</td>
          <td>18.08</td>
        </tr>
      </tbody>
    </table>
    </div>
