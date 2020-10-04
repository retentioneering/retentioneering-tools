Plot_graph function
~~~~~~~~~~~~~~~~~~~


No normalization
================


This notebook can be found :download:`here <_static/examples/graph_tutorial.ipynb>`.

To understand better how different normalization types work and how to use plot_graph function
let's use sample dataset:

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

We will start from simplest case: norm_type=None, weight_col=None.
In this case we are looking simply at absolute values - total number of transitions.
Let's visualize our dataset using plot_graph function:

.. code:: ipython3

    data.rete.plot_graph(norm_type=None,
                         weight_col=None,
                         thresh=250)

.. raw:: html


            <iframe
                width="700"
                height="600"
                src="_static/plot_graph/index_0.html"
                frameborder="0"
                allowfullscreen
            ></iframe>

|

Here, numbers for each edge correspond to total number of given transition in the dataset
(for example, total number of 'cart'->'delivery_choice' transitions in example above is 1686).
Parameter 'thresh' sets a limit, below which edge stop showing on a graph to avoid cluttering.

Some of the events might be particular importance to visualize. To highlight those events with
red or green colors (and all inbound edges) use parameter targets to provide a dictionary with
required colors ('green' or 'red'):

.. code:: ipython3

    data.rete.plot_graph(norm_type=None,
                         weight_col=None,
                         thresh=250,
                         targets = {'payment_done':'green',
                                    'lost':'red'})

.. raw:: html


            <iframe
                width="700"
                height="600"
                src="_static/plot_graph/index_1.html"
                frameborder="0"
                allowfullscreen
            ></iframe>

|



norm_type = 'full'
==================

This type of normalization provides normalization by entire dataset. To understand
intuitively how to interpret results let's consider an example:

.. code:: ipython3

    data.rete.plot_graph(norm_type='full',
                         weight_col=None,
                         thresh=0.01,
                         targets = {'payment_done':'green',
                                    'lost':'red'})

.. raw:: html


            <iframe
                width="700"
                height="600"
                src="_static/plot_graph/index_2.html"
                frameborder="0"
                allowfullscreen
            ></iframe>

|

In this case percents on graph edges indicates the percentage of given transition from
all transitions. For example, transition 'catalog'->'catalog' represents 15% of all transtions
in the dataset.

Very often we are interested not in the fraction given transition represents from all
transitions, but in what percentage of users have given transition from all users.
This can be obtained using weight_col='user_id' parameter:

.. code:: ipython3

    data.rete.plot_graph(norm_type='full',
                         weight_col='user_id',
                         thresh=0.06,
                         targets = {'payment_done':'green',
                                    'lost':'red'})

.. raw:: html


            <iframe
                width="700"
                height="600"
                src="_static/plot_graph/index_3.html"
                frameborder="0"
                allowfullscreen
            ></iframe>
|

In this case, % on graph enges corresponds to % of users from the dataset who have
given transition. For example, 36% of all users made a transition from 'cart' to
'delivery_choice'.

norm_type = 'node'
==================

Sometimes we would like to know, from all users, who reach 'cart' what percent transitioned to
'delivery_choice', or from all users who reach 'payment_card' what percent completed the purchase
(transitioned to 'payment_done').

These type of questions can be addressed with norm_type = 'node'. Let's consider another example:

.. code:: ipython3

    data.rete.plot_graph(norm_type='node',
                         weight_col='user_id',
                         thresh=0.2,
                         targets = {'payment_done':'green',
                                    'lost':'red'})

.. raw:: html


            <iframe
                width="700"
                height="600"
                src="_static/plot_graph/index_4.html"
                frameborder="0"
                allowfullscreen
            ></iframe>
|

Here, percent on edge A --> B correspond to percent of users who transtioned to state B
out all users who got to state A. For example, we can tell that 70% of users who got to 'cart'
transitioned to 'delivery_choice'. Or 91% of users who select 'payment_card' transitioned to
'payment_done' and only 54% of users who selected 'payment_cash' transitioned to 'payment_done'.


Normalization cheat-sheet
=========================

Summary table of all normalization types:

.. image:: _static/plot_graph/norm_types.svg