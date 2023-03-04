.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red


Funnel
======

The following user guide is also available as
`Google Colab notebook <https://colab.research.google.com/drive/1VjFXazgIdMKLyHaqMoKTWhnq5_29lRIs?usp=share_link>`_

Loading data
------------

Here we use ``simple_shop`` dataset, which has already converted to ``Eventstream``.
If you want to know more about ``Eventstream`` and how to use it, please study
:doc:`this guide<eventstream>`

.. code-block:: python

    from retentioneering import datasets

    stream = datasets.load_simple_shop()

Basic example
-------------

Building a conversion funnel is the basic first step in almost all
product analytics workflows. To learn how to plot basic funnels using
the Retentioneering library, let us work through a basic example.

Funnel tool is mainly available as
:py:meth:`Eventstream.funnel()<retentioneering.eventstream.eventstream.Eventstream.funnel>` method.
The implementation is based on `Plotly funnel charts <https://plotly.com/python/funnel-charts/>`_.
Here's how it visualizes ``simple_shop`` eventstream:

.. code-block:: python

    stream.funnel(stages = ['catalog', 'cart', 'payment_done']);

.. raw:: html

            <iframe
                width="700"
                height="400"
                src="../_static/user_guides/funnel/funnel_0_basic.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

Customization
-------------

Stages
~~~~~~

``Stages`` is required parameter. It should contain a list of event names
you would like to observe in the funnel.

For each specified stage event, the following statistics will be calculated:

- absolute unique number of user_id’s who reach this stage at least once.
- conversion from the first stage (`% of initial`)
- conversion from the previous stage (`% of previous`)

The order of stages on the funnel plot corresponds to the order in which
events are passed in ``stages`` parameter.

Stage grouping
~~~~~~~~~~~~~~

In many practical cases, we would like to be able to group multiple
events as one stage - for example, if it doesn’t matter which particular
event was reached. We can access this function by passing lists of
events (along with single events) in the ``stage`` parameter.

Let us plot a funnel with `product1` and `product2` grouped that way:

.. code-block:: python

    stream.funnel(stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done']);

.. raw:: html

            <iframe
                width="700"
                height="400"
                src="../_static/user_guides/funnel/funnel_1_stages.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

As you can see, the new ``product1 | product2`` stage is created for
the funnel. It corresponds to having 2010 unique users who reached
some product page(``product1 or product2``).

NOTE: If a user path has both of the events, the user still counts as one.

Stage names
~~~~~~~~~~~

Grouping big sets of events with the previous method could be less
practical, as the displayed name of the event group will be hard to
interpret. You could avoid this problem by doing one of the following:

#. use grouping data processor for grouping relevant events.
   See :py:meth:`GroupEvents<retentioneering.data_processors_lib.group_events.GroupEvents>`)
#. use the ``stage_names`` funnel parameter

In the following example, let us use the second method. We define
``stage_names`` as a list of funnel stage names (the length of which
has to be equal to the number of stages):

.. code-block:: python

    stream.funnel(
                   stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
                   stage_names = ['catalog', 'product', 'cart', 'payment_done']
                   );

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="../_static/user_guides/funnel/funnel_2_stage_names.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

Funnel type and sequence parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Parameter ``funnel_type`` could take one of the two values:

#. ``open`` (default) - used if the metric of interest is user presence
   on a given stage. The funnel will disregard the user presence on previous
   stages. This means that, for each stage, all stage visits will be
   counted - regardless of whether the previous stages were passed.
#. | ``closed`` - only users who have the first specified stage will be counted and only part of their path after this stage will be considered.
   | The ``sequence`` parameter further specifies the behaviour:

    - | If ``sequence`` is set to ``False``, all users who visited the first stage
      | who visited all previous stages no matter when will be counted
    - | If ``sequence`` is set to ``True``, only users who visited all previous
      | stages before will be counted

The example below illustrates the behaviour differences:

.. figure:: /_static/user_guides/funnel/type_sequence.png


Now let’s return to our ``simple_shop`` dataset and build ``closed`` funnel with ``sequence=False``.

In comparison to ``open`` funnel we can see that some users come to
``cart`` without passing ``catalog`` or ``product`` beforehand.
The real forward conversion for these stages is lower than
we see in the ``open`` funnel.

.. code-block:: python

    stream.funnel(
                  stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
                  tage_names = ['catalog', 'product', 'cart', 'payment_done'],
                  funnel_type='closed'
                  );


.. raw:: html


            <iframe
                width="700"
                height="400"
                src="../_static/user_guides/funnel/funnel_3_closed.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

Now we take a look at a funnel with ``funnel_type=closed``
and ``sequence=True``. The conversion to the ``cart`` stage is even lower
than it is for ``funnel_type=closed`` and ``sequence=False``.
It means that some users visiting ``catalog`` go strait to the cart stage,
which we could interpret as being a specific class of users (for instance,
those who were on the web-site before, and left some products in the cart
earlier or there is another way to reach ``cart`` stage)

.. code-block:: python

    stream.funnel(
                   stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
                   stage_names = ['catalog', 'product', 'cart', 'payment_done'],
                   funnel_type='closed',
                   sequence=True
                   );

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="../_static/user_guides/funnel/funnel_4_sequence.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

User segments
~~~~~~~~~~~~~

It can be useful to make separate funnels for different user groups,
and compare them stage-by-stage.

Groups of users could be represented by:

- users from different channels
- users from test and control groups in A/B test
- users from different behavioral segments

To achieve the desired effect, we can pass lists of user ids
to the ``groups`` parameter. Let us plot funnels for two user
groups:

- users who had reached the ``payment_done`` stage
- users who had not:

.. code-block:: python

    stream_df = stream.to_dataframe()
    segment1 = set(stream_df[stream_df['event'] == 'payment_done']['user_id'])
    segment2 = set(stream_df['user_id']) - segment1

    stream.funnel(
        stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
        stage_names = ['catalog', 'product', 'cart', 'payment_done'],
        segments = (segment1, segment2),
        segment_names = ('converted', 'not_converted')
    );

.. raw:: html

            <iframe
                width="700"
                height="400"
                src="../_static/user_guides/funnel/funnel_5_segments_open.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

We see how the two groups compare to each other at particular stages.
As expected, the ``not_converted`` users are the majority, and we can
see that most of them are "lost" after visiting ``cart``. Interestingly,
we can see that some users add product to cart directly from the catalog,
without visiting a product page(which is represented by the fact that
more users have visited ``cart`` than ``product``).

Now, let us have a look at the ``closed`` funnel:

.. code-block:: python

    stream.funnel(
        stages=['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
        stage_names=['catalog', 'product', 'cart', 'payment_done'],
        funnel_type='closed',
        segments=(segment1, segment2),
        segment_names=('converted', 'not_converted')
    );

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="../_static/user_guides/funnel/funnel_6_segments_closed.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

Now we see - our assumption that some users add product to cart
directly from the catalog is incorrect. In fact, those users appear
in ``cart`` passing from the others stages, not from ``catalog``.

Clustering
^^^^^^^^^^

Consider another example - we compare funnels for multiple users groups,
segmented according to clusterization results:

.. code-block:: python

    from retentioneering.tooling.clusters import Clusters

    clusters = Clusters(eventstream=stream)
    clusters.fit(method='kmeans',
                 n_clusters=8,
                 feature_type='tfidf',
                 ngram_range=(1,1));


With this clustering procedure, we grouped users based
on their behavioural patterns. The dictionary containing cluster
user lists is assigned to the
:py:meth:`Clusters.cluster_mapping<retentioneering.tooling.clusters.clusters.Clusters.cluster_mapping>` attribute.

Let us plot the cluster funnels to compare cluster conversions:

.. code-block:: python

    clus1_ids = clusters.cluster_mapping[1]
    clus2_ids = clusters.cluster_mapping[2]
    clus3_ids = clusters.cluster_mapping[3]
    clus6_ids = clusters.cluster_mapping[6]

    stream.funnel(
        eventstream=stream,
        stages=['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
        segments=(clus1_ids, clus2_ids, clus3_ids, clus6_ids),
        segment_names=('cluster 1', 'cluster 2', 'cluster 3', 'cluster 6'));


.. raw:: html

            <iframe
                width="700"
                height="400"
                src="../_static/user_guides/funnel/funnel_7_clusters.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

We could further expand our user behaviour analysis by plotting
:doc:`transition graphs<transition_graph>` or :doc:`step matrices<step_matrix>`.

Using a separate instance
-------------------------

By design, :py:meth:`Eventstream.funnel()<retentioneering.eventstream.eventstream.Eventstream.funnel>`
is a shortcut method which uses an instance of
:py:meth:`Funnel<retentioneering.tooling.funnel.funnel.Funnel>` under the hood.
Eventstream method creates an instance of Funnel object and stores the eventstream internally.

Sometimes it's reasonable to work with a separate instance of Funnel class. In this case you also have to
call :py:meth:`Funnel.fit()<retentioneering.tooling.funnel.funnel.Funnel.fit>` and
:py:meth:`Funnel.plot()<retentioneering.tooling.funnel.funnel.Funnel.plot()>` methods explicitly.

Here's an example how you can do it:

.. code-block:: python

    from retentioneering.tooling.funnel import Funnel

    funnel = Funnel(
                    eventstream=stream,
                    stages = ['catalog', 'cart', 'payment_done']
                     )
    funnel.fit()
    funnel.plot();

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="../_static/user_guides/funnel/funnel_8_eventstream.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

Common tooling properties
-------------------------

values
~~~~~~

``Funnel.plot()`` is displayed by default, but
:py:meth:`Funnel.values<retentioneering.tooling.funnel.funnel.Funnel.values>` property is also available.

In order to avoid unnessesary recalculations while you need different representations
of one eventstream with the same parameters - that would be helpful to save that fitted
instance in separate variable.

.. code-block:: python

    ff = stream.funnel(
                       stages = ['catalog', 'cart', 'payment_done'],
                       show_plot=False
                       );

    ff.values

.. raw:: html

    <div><table class="dataframe">
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

params
~~~~~~
