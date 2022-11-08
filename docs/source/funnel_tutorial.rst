Funnel
=================

This notebook can be found :download:`here <_static/examples/funnel_tutorial.ipynb>`
or open directly in `google colab <https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/docs/source/_static/examples/funnel_tutorial.ipynb>`__.

Install retentioneering if running from google.colab or for the first
time:

.. code:: ipython3

    # !pip install retentioneering

Basic example
-------------
Conversion funnel is the basic first step in almost all product
analytics workflow. To learn how to plot basic funnels in
Retentioneering framework let’s work through a basic example.

In order to start, we need to: - ``import retentioneering``, - load
sample dataset - create ``eventstream`` object @TODO: Link to explanation of eventstream. dpanina

.. code:: ipython3

    import retentioneering
    from src.eventstream import Eventstream, EventstreamSchema, RawDataSchema
    # @TODO: check imports in final version. dpanina
    # load sample user behavior data as a pandas dataframe:
    raw_data = pd.read_csv('simple-onlineshop.csv')

    # create data schema
    raw_data_schema = RawDataSchema(
        event_name="event", event_timestamp="timestamp", user_id="user_id")

    # create source eventstream
    source = Eventstream(
        raw_data=raw_data,
        raw_data_schema=raw_data_schema,
        schema=EventstreamSchema()
    )

There are two ways to plot funnel using retentioneering:

1) To create instance of a class Funnel

.. code:: ipython3

    from src.tooling.funnel import Funnel

    funnel = Funnel(
        eventstream=source,
        stages = ['catalog', 'cart', 'payment_done']
    )
    funnel.draw_plot().show()


.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_0.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

2) To call an eventstream method eventstream.funnel() Further in this
   tutorial we will use exactly this method

.. code:: ipython3

    source.funnel(
        stages = ['catalog', 'cart', 'payment_done']
    )

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_1.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>



Stages
------

Stages is required parameter for funnel() method, and it is a list of
event names you are interested to observe in the funnel. For each
specified stage we calculate and show: - absolute unique number of
user_id’s who reach this stage at least once. - percentage from the
first stage (“% of initial”) - percentage from the previous stage (“% of
previous”)

The order of stages on the funnel plot corresponds to the order in which
events are passed in ``stages`` parameter.

Stage grouping
--------------

Sometimes during funnel analysis several events can have similar
importance, and it doesn’t matter which particular event was reached. In
this case, we would like to group multiple events as one stage, and they
can be passed as sub-list in ``stage`` parameter.

Let’s plot a funnel where we group ``product1`` and ``product2``:

.. code:: ipython3

    source.funnel(stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'])

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_2.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

You can now see new ``product1 | product2`` stage on the funnel with
2010 unique users who reached any product page
(``product1 or product2``). NOTE: If one user has both events in his
path he will be counted as one unique user.

Stage names
-----------

If you need to group long list of events, you have two ways:
1) return to preprocessing and use grouping data processor (See @TODO: Link to preprocessing. dpanina)
2) give a new name to your group just to see the plot,
without changing your ``eventstream``

Let’s turn to the second method. We can use ``stage_names`` parameter.
This list should be the same length as ``stages``.

.. code:: ipython3

    source.funnel(stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
                  stage_names = ['catalog', 'product', 'cart', 'payment_done']
                  )


.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_3.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

Funnel type and sequence parameters
-----------------------------------

Parameter ``funnel_type`` has two possible options:
1) \ ``open``\  - it’s default value and we use it when only the user presence on the
stage is significant. And we don’t care about the order of the stages in
user’s path and also about if user was only on first or on all previous
stages.
2) \ ``closed``\  - in return can be of two types: - If it is
important to see only users who were on the first stage and analyse the
funnel stages only after passing it. In the other words, user path
before the first stage of the funnel dropped and then funnel is built
according to the rules of the ``open`` funnel. Parameter
``sequence=False`` should be used in that case. - If it is important to
look at the users who move to each next stage only if earlier they were
on all previous ones. Parameter ``sequence=True`` should be used in that
case.

In order to feel the difference - see very simple example (@TODO: Link to API reference funnel. dpanina)

Let’s build ``closed`` funnel with ``sequence=False``.

With comparison to ``open`` funnel we can see that some users come to
``cart`` not from ``catalog`` or ``product`` stages. And real conversion
from these stages is lower than we saw in ``open`` funnel.

.. code:: ipython3

    source.funnel(stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
                  stage_names = ['catalog', 'product', 'cart', 'payment_done'],
                  funnel_type='closed'
                  )

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_4.html"
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

.. code:: ipython3

    source.funnel(stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
                  stage_names = ['catalog', 'product', 'cart', 'payment_done'],
                  funnel_type='closed',
                  sequence=True
                  )

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_5.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>


User segments
-------------

Sometimes it is useful to compare funnels stage-by-stage of several user
segments. For example, to have a quick comparison of funnels of users: -
from different channels - from test and control groups in A/B test - to
compare multiple behavioral segments and etc.

This can be done by passing list of collections of user id’s via groups
parameter. To illustrate this functionality let’s plot funnels for two
groups: users who converted to ``payment_done`` and users who did not.
First, we need to obtain two collections of ``user_ids`` and then pass
it to groups parameters for ``eventstream.funnel()`` method:

.. code:: ipython3

    source_df = source.to_dataframe()
    segment1 = set(source_df[source_df['event_name'] == 'payment_done']['user_id'])
    segment2 = set(source_df['user_id']) - segment1

    source.funnel(stages = ['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
                     stage_names = ['catalog', 'product', 'cart', 'payment_done'],
                     segments = (segment1, segment2),
                     segment_names = ('converted', 'not_converted'))

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_6.html"
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

Now let's have a look at the ``closed`` funnel:

.. code:: ipython3

    source.funnel(stages=['catalog', ['product1', 'product2'], 'cart', 'payment_done'],
                     stage_names=['catalog', 'product', 'cart', 'payment_done'],
                     funnel_type='closed',
                     segments=(segment1, segment2),
                     segment_names=('converted', 'not_converted'))

.. raw:: html


            <iframe
                width="700"
                height="400"
                src="_static/funnel/funnel_7.html"
                frameborder="0"
                align="left"
                allowfullscreen
            ></iframe>

It is interesting to notice that our hypothesis about the fact that
users add product to cart directly from the catalog is incorrect, and
those users appear in the ``cart`` from the others stages, not from
``catalog``.

Clustering
------------

@TODO: Clustering. dpanina

To understand deeper what are the common behavioral patterns for each
graph we can plot graphs or step matrix. (@TODO: Link to graphs and step matrix. dpanina)
