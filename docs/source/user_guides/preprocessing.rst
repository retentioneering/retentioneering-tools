.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red

Preprocessing
=============

By preprocessing we mean any eventstream data transformation preceding applying
analytical tools. This process is crucial for analytical research. Some analytical
methods are sensitive to data they accept as input, so the data must be prepared in
a special way. Another case when the preprocessing arises is when you want to explore
some parts of an eventstream (instead of the entire eventstream) so you have to
truncate the data in an efficient way. Finally, often you want to clean and
wrangle the data in order to remove technical, misleading events or user paths entirely.

The Preprocessing module allows you to treat all the data preparations efficiently.
The core element of this module is a :doc:`data processor <../api/preprocessing_api>`.
This document doesn't cover the details on how each data processor works, so if you
haven't read :doc:`data processors user guide <../user_guides/dataprocessors>`
so far, we recommend that you explore it.

Applying a single data processor in most cases is not enough for solving your
practical preprocessing problem. In fact, preprocessing pipelines often involve
multiple data processors, and requre complex splitting and merging logic.
That's why it's natural to use graph structures to describe all these calculations.
We introduce you to the *preprocessing graph* - an object which organises all
the preprocessing calculations.

The idea of preprocessing graph is simple. Each node of the graph is a single
data processor. The nodes are linked according to the sequential order of the
calculations. The graph root is associated with the original state of an eventstream.
On the other hand, any graph node may be considered as a specific state of the
original eventstream. This state is comprehensively described by the sequence
of data processors we apply following the graph path from the root to this
specific node.

Note that a preprocessing graph just frames the calculation logic. To
get an eventstream state corresponding to a specific graph node, we need to
run the calculation explicitly. See :ref:`Running the calculation
<preprocessing_running_the_calculation>` section for the details.

.. _preprocessing_case_study:

Case study
----------

Problem statement
~~~~~~~~~~~~~~~~~

The best way to explain the variety of the preprocessing features that retentioneering
offers is to consider a case study. We will construct a preprocessing graph using
our demonstration :doc:`simple_shop dataset <../datasets/simple_shop>`, which we load as an
``Eventstream`` object. You can learn more about ``Eventstream`` in our :doc:`eventstream guide<eventstream>`.

.. code-block:: python

    import pandas as pd
    from retentioneering import datasets

    stream = datasets.load_simple_shop()

Suppose we want to transform the initial eventstream in the following way:

1. Add ``path_start`` and ``path_end`` events.
2. Group ``product1`` and ``product2`` events into single ``product`` event;
3. Among the users who are not identified as new, we want to remove those
   trajectories which are considered as truncated;
4. Split the paths into the sessions.

The idea behind this case is that we want to demonstrate not only "linear"
preprocessing actions (i.e. the calculations which are executed step-by-step,
with no branching logic required), but to show more complex branching and
merging preprocessing logic as well.

A draft of a solution
~~~~~~~~~~~~~~~~~~~~~

:doc:`Data processors <../user_guides/dataprocessors>` are bricks for our
preprocessing. So we have to represent our solution as a combination of the
data processors.

As for requirements 1, 2, and 4 of the case study, they are straightforward.
Each of them relates to a single data processor application. In contrast,
requirement 3 is a bit tricky. First, we need to apply the
:py:meth:`NewUsersEvents <retentioneering.data_processors_lib.new_users.NewUsersEvents>`
data processor, marking the trajectories with the ``new_user`` and ``existing_user``
markers. Next, we apply the
:py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>`
data processor twice: once to get the new users from the previous step,
and then to get the existing users. Note that the preprocessing
flow splits at this point. Next, for the branch related to the existing users
we need to sequentially apply the
:py:meth:`TruncatedEvents <retentioneering.data_processors_lib.truncated_events.TruncatedEvents>`
data processor for marking the paths as truncated or not, and then another
:py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>`
data processor to leave intact trajectories only. Finally, we need to merge
the data from the two separated branches and apply the
:py:meth:`SplitSessions <retentioneering.data_processors_lib.split_sessions.SplitSessions>`
data processor in the end. An outline of the described solution is represented
on the image below.

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_outline.png
    :height: 600

    An outline of the possible case study solution.

Pay attention to the splitting and merging logic. After the 3rd node the eventstream
is split into the two disjoint eventstreams (one contains only new users,
another contains only existing users). Once we finish processing the existing
users' trajectories we need to merge these two eventstreams. There's a
special merging node developed for this purpose. We'll talk about it
later in this user guide.

Next, we specify the information about the graph nodes and the underlying data processors.
The table below contains the list of the nodes, the data
processors they are associated with, and the particular parameters they need
to be applied to them. We find this a bit more informative and we'll
build the preprocessing graph according to this plan.

.. _preprocessing_solution_plan:

.. table:: The schema of the preprocessing graph for the case study
    :widths: 10 20 40 20
    :class: tight-table

    +-------+---------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | Node  | Data processor                                                                                    | Parameters                                                                                                                                             | Parents      |
    +=======+===================================================================================================+========================================================================================================================================================+==============+
    | node1 | :py:meth:`StartEndEvents <retentioneering.data_processors_lib.start_end_events.StartEndEvents>`   | –                                                                                                                                                      | Source       |
    +-------+---------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node2 | :py:meth:`GroupEvents <retentioneering.data_processors_lib.group_events.GroupEvents>`             | ``event_name='product'``, ``func=group_products``                                                                                                      | node1        |
    +-------+---------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node3 | :py:meth:`NewUsersEvents <retentioneering.data_processors_lib.new_users.NewUsersEvents>`          | copy&paste the data from `here <https://drive.google.com/file/d/1ixztbZj1GWg_xNpTZOKGOYBtoJlJmOtO/view?usp=sharing>`_  to ``new_users_list`` parameter | node2        |
    +-------+---------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node4 | :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>`          | ``func=get_new_users``                                                                                                                                 | node3        |
    +-------+---------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node5 | :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>`          | ``func=get_existing_users``                                                                                                                            | node3        |
    +-------+---------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node6 | :py:meth:`TruncatedEvents <retentioneering.data_processors_lib.truncated_events.TruncatedEvents>` | ``left_truncated_cutoff=(1, 'h')``, ``right_truncated_cutoff=(1, 'h')``                                                                                | node5        |
    +-------+---------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node7 | :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>`          | ``func=remove_truncated_paths``                                                                                                                        | node6        |
    +-------+---------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node8 | :py:meth:`MergeNode <retentioneering.graph.nodes.MergeNode>`                                      | –                                                                                                                                                      | node4, node7 |
    +-------+---------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node9 | :py:meth:`SplitSessions <retentioneering.data_processors_lib.split_sessions.SplitSessions>`       | ``session_cutoff=(30, 'm')``                                                                                                                           | node8        |
    +-------+---------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+

The functions which are passed to ``func`` parameter in the FilterEvents and GroupEvents
data processors will be defined below.

Code-generated preprocessing graph
----------------------------------

We're starting from creating an empty graph.

.. _preprocessing_graph_creation:

.. code-block:: python

    from retentioneering.graph.p_graph import PGraph

    graph = PGraph(stream)

As you see, :py:meth:`PGraph<retentioneering.graph.p_graph.PGraph>` constructor
requires an instance of Eventstream. The graph's root is associated with the initial
state of the eventstream which will be changed according to the graph logic.

Creating a single node
~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`EventsNode <retentioneering.graph.nodes.EventsNode>` is a basic class for
preprocessing graph node representation. As we mentioned earlier, each node
is associated with a particular :doc:`data processor <../api/preprocessing_api>`
(merging node is an exception). As an example, let's create a
:py:meth:`GroupEvents <retentioneering.data_processors_lib.group_events.GroupEvents>`
node (``node2``).

.. code-block:: python

    from retentioneering.graph.p_graph import EventsNode
    from retentioneering.data_processors_lib import GroupEvents, GroupEventsParams

    def group_products(df, schema):
        return df[schema.event_name].isin(['product1', 'product2'])

    group_events_params = {
        "event_name": "product",
        "func": group_products
    }

    data_processor_params = GroupEventsParams(**group_events_params)
    data_processor = GroupEvents(params=data_processor_params)
    node2 = EventsNode(data_processor)

What's happening in this example? The data processor's parameters are set with
the help of ``*Params`` class where the asterisk stands for a data processor
name. Namely, there's
:py:meth:`GroupEventsParams <retentioneering.data_processors_lib.group_events.GroupEventsParams>`
parameter class for
:py:meth:`GroupEvents <retentioneering.data_processors_lib.group_events.GroupEvents>`.
The arguments of a ``*Params`` class constructor are exactly the same as
the corresponding parameter names. For
:py:meth:`GroupEventsParams <retentioneering.data_processors_lib.group_events.GroupEventsParams>`
they are ``event_name`` and ``func`` which we keep here as
``group_events_params`` dictionary items. ``group_products``
function returns the mask for grouping events ``product1`` and ``product2``.

Next, we pass ``data_processor_params`` object to the only parameter ``params`` of the
:py:meth:`GroupEvents() <retentioneering.data_processors_lib.group_events.GroupEvents>`
constructor and assign its result to the ``data_processor`` variable.

Finally, we pass the data processor instance to the ``EventsNode`` class constructor
and get our node.

Since all three classes' constructors involved in the node creation process
have a single parameter, it's convenient to create a node with a single line
of code as follows:

.. code-block:: python

    node2 = EventsNode(GroupEvents(params=GroupEventsParams(**group_events_params)))

If you were surprised why we didn't start with ``node1``, here's a clue.
The reason is that the
:py:meth:`StartEndEvents <retentioneering.data_processors_lib.start_end_events.StartEndEvents>`
data processor doesn't have any arguments. However, even in this case we
have to create an instance of ``StartEndEventsParams`` and pass it to the
data processor constructor. Look how you can do it:

.. code-block:: python

    from retentioneering.data_processors_lib import StartEndEvents, StartEndEventsParams

    node1 = EventsNode(StartEndEvents(params=StartEndEventsParams()))

Linking nodes
~~~~~~~~~~~~~

In order to link a node to its parents, call
:py:meth:`PGraph.add_node() <retentioneering.graph.p_graph.PGraph.add_node>`.
The method accepts a node object and its parents list. A regular node must
have a single parent, whereas a merging node must have at least two parents.
We'll demonstrate how merging nodes work in the next subsection. As of now,
here's how to connect a pair of nodes of our graph:

.. code-block:: python

    graph.add_node(node=node1, parents=[graph.root])
    graph.add_node(node=node2, parents=[node1])

Note that ``node1`` is linked to a special ``graph.root`` node which is a
mandatory attribute of any graph. ``node2`` is connected to a regular node ``node1``.

So we've described how to create the graph nodes and how to link the nodes.
Using these two basic operations we can construct the whole graph.

Building the whole graph
~~~~~~~~~~~~~~~~~~~~~~~~

Let's create the other graph nodes and link them step-by-step according
to the :ref:`plan <preprocessing_solution_plan>`.

To create ``node3`` we need to
`download <https://drive.google.com/file/d/1ixztbZj1GWg_xNpTZOKGOYBtoJlJmOtO/view?usp=sharing>`_
the list of the new users beforehand. This list contains user_ids of
the users who are considered as new (i.e. they have not visited the
system any time before the dataset start). We assign the downloaded
list to ``new_users`` variable and then pass it to
:py:meth:`NewUsersParams <retentioneering.data_processors_lib.new_users.NewUsersParams>`.

.. code-block:: python

    from retentioneering.data_processors_lib import NewUsersEvents, NewUsersParams

    google_drive_file_id = '1ixztbZj1GWg_xNpTZOKGOYBtoJlJmOtO'
    link = f'https://drive.google.com/uc?id={google_drive_file_id}&export=download'
    new_users = pd.read_csv(link, header=None)[0].tolist()
    node3 = EventsNode(NewUsersEvents(params=NewUsersParams(new_users_list=new_users)))
    graph.add_node(node=node3, parents=[node2])

Creation of the next ``node4`` and ``node5`` is similar. We need to create a
couple of nodes with
:py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>`
data processors and pass them filtering functions ``get_new_users()`` and
``get_existing_users()``. These two functions recognize synthetic events
``new_user`` and ``existing_user`` added by NewUsersEvent data processor
at the previous step and leave the paths of new users and existing
users only correspondingly.

.. code-block:: python

    from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams

    def get_new_users(df, schema):
        new_users = df[df[schema.event_name] == 'new_user']\
            [schema.user_id]\
            .unique()
        return df[schema.user_id].isin(new_users)

    def get_existing_users(df, schema):
        existing_users = df[df[schema.event_name] == 'existing_user']\
            [schema.user_id]\
            .unique()
        return df[schema.user_id].isin(existing_users)


    node4 = EventsNode(FilterEvents(params=FilterEventsParams(func=get_new_users)))
    node5 = EventsNode(FilterEvents(params=FilterEventsParams(func=get_existing_users)))
    graph.add_node(node=node4, parents=[node3])
    graph.add_node(node=node5, parents=[node3])

There's nothing new in the creation of the ``node6``. We just pass a couple of
``left_truncated_cutoff`` and ``right_truncated_cutoff`` parameters to
:py:meth:`TruncatedEventsParams <retentioneering.data_processors_lib.truncated_events.TruncatedEventsParams>`
and set up a :py:meth:`TruncatedEvents <retentioneering.data_processors_lib.truncated_events.TruncatedEvents>`
node.

.. code-block:: python

    from retentioneering.data_processors_lib import TruncatedEvents, TruncatedEventsParams

    params = {
        "left_truncated_cutoff": (1, 'h'),
        "right_truncated_cutoff": (1, 'h'),
    }
    node6 = EventsNode(TruncatedEvents(params=TruncatedEventsParams(**params)))
    graph.add_node(node=node6, parents=[node5])

For ``node7`` we apply similar filtering technique as we used for filtering
new/existing users above. The remove_truncated_paths() function implements this filter.


.. code-block:: python

    def remove_truncated_paths(df, schema):
        truncated_users = df[df[schema.event_name].isin(['truncated_left', 'truncated_right'])]\
            [schema.user_id]\
            .unique()
        return ~df[schema.user_id].isin(truncated_users)

    node7 = EventsNode(FilterEvents(params=FilterEventsParams(func=remove_truncated_paths)))
    graph.add_node(node=node7, parents=[node6])

Next, ``node8``. As we discussed earlier,
:py:meth:`MergeNode <retentioneering.graph.nodes.MergeNode>` has two
special features. Unlike ``EventsNode``, ``MergeNode`` is not associated
with any data processor since it has a separate role -- concatenate
the outputs of its parents. Another distinction from ``EventsNode``
is that the number of parents might be arbitrary (greater than 1).
The following two lines of the code demonstrate both these features:

.. code-block:: python

    from retentioneering.graph.p_graph import MergeNode

    node8 = MergeNode()
    graph.add_node(node=node8, parents=[node4, node7])


Finally, for ``node9`` we wrap
:py:meth:`SplitSessions <retentioneering.data_processors_lib.split_sessions.SplitSessions>`
data processor to a node passing a single parameter ``session_cutoff``
and link it to the merging node:

.. code-block:: python

    from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams

    node9 = EventsNode(SplitSessions(params=SplitSessionsParams(session_cutoff=(30, 'm'))))
    graph.add_node(node=node9, parents=[node8])

.. _preprocessing_running_the_calculation:

Running the calculation
~~~~~~~~~~~~~~~~~~~~~~~

So we have built the graph, now it's time to run the entire calculation which
the graph frames. In order to run the calculation from the graph root to a specific node,
call :py:meth:`PGraph.combine() <retentioneering.graph.p_graph.PGraph.combine>`
method with a single parameter ``node`` which accepts the corresponding node object.
The result is represented as the
:py:meth:`Eventstream <retentioneering.eventstream.eventstream.Eventstream>` class.

.. code-block:: python


    processed_stream = graph.combine(node=node9)
    processed_stream.to_dataframe().head()

.. raw:: html

    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>session_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>2</th>
          <td>bc2e5bf8-c199-40a6-9155-d57a1c060377</td>
          <td>path_start</td>
          <td>2</td>
          <td>path_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890.0</td>
          <td>219483890.0_1</td>
        </tr>
        <tr>
          <th>6</th>
          <td>7aaabd5f-a063-46fc-91b3-2e89c24fa53d</td>
          <td>existing_user</td>
          <td>6</td>
          <td>existing_user</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890.0</td>
          <td>219483890.0_1</td>
        </tr>
        <tr>
          <th>8</th>
          <td>cfe74a57-ee9e-4043-8293-12fac5adf3ff</td>
          <td>session_start</td>
          <td>8</td>
          <td>session_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890.0</td>
          <td>219483890.0_1</td>
        </tr>
        <tr>
          <th>13</th>
          <td>2c660a5e-8386-4334-877c-8980979cdb30</td>
          <td>group_alias</td>
          <td>13</td>
          <td>product</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890.0</td>
          <td>219483890.0_1</td>
        </tr>
        <tr>
          <th>16</th>
          <td>0d2f7c47-fdce-498c-8b3f-5f6228ff8884</td>
          <td>session_end</td>
          <td>16</td>
          <td>session_end</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890.0</td>
          <td>219483890.0_1</td>
        </tr>
      </tbody>
    </table>
    <br>

.. note::

    You can combine the calculations at any node. In practice, it's useful
    for debugging the calculations.

Summary
~~~~~~~

Here we just provide the same code combined in a single chunk so you could
simply copy and paste it and see the results.

.. code-block:: python

    import pandas as pd
    from retentioneering import datasets
    from retentioneering.data_processors_lib import StartEndEvents, StartEndEventsParams
    from retentioneering.data_processors_lib import GroupEvents, GroupEventsParams
    from retentioneering.data_processors_lib import NewUsersEvents, NewUsersParams
    from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams
    from retentioneering.data_processors_lib import TruncatedEvents, TruncatedEventsParams
    from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams
    from retentioneering.graph.p_graph import PGraph, EventsNode, MergeNode

    stream = datasets.load_simple_shop()

    # node1
    node1 = EventsNode(StartEndEvents(params=StartEndEventsParams()))

    # node2
    def group_products(df, schema):
        return df[schema.event_name].isin(['product1', 'product2'])

    group_events_params={
        "event_name": "product",
        "func": group_products
    }
    node2 = EventsNode(GroupEvents(params=GroupEventsParams(**group_events_params)))

    # node3
    google_drive_file_id = '1ixztbZj1GWg_xNpTZOKGOYBtoJlJmOtO'
    link = f'https://drive.google.com/uc?id={google_drive_file_id}&export=download'
    new_users = pd.read_csv(link, header=None)[0].tolist()
    node3 = EventsNode(NewUsersEvents(params=NewUsersParams(new_users_list=new_users)))

    # node4, node5
    def get_new_users(df, schema):
        new_users = df[df[schema.event_name] == 'new_user']\
            [schema.user_id]\
            .unique()
        return df[schema.user_id].isin(new_users)

    def get_existing_users(df, schema):
        existing_users = df[df[schema.event_name] == 'existing_user']\
            [schema.user_id]\
            .unique()
        return df[schema.user_id].isin(existing_users)

    node4 = EventsNode(FilterEvents(params=FilterEventsParams(func=get_new_users)))
    node5 = EventsNode(FilterEvents(params=FilterEventsParams(func=get_existing_users)))

    # node6
    params = {
        "left_truncated_cutoff": (1, 'h'),
        "right_truncated_cutoff": (1, 'h'),
    }
    node6 = EventsNode(TruncatedEvents(params=TruncatedEventsParams(**params)))

    # node7, node8, node9
    def remove_truncated_paths(df, schema):
        truncated_users = df[df[schema.event_name].isin(['truncated_left', 'truncated_right'])]\
            [schema.user_id]\
            .unique()
        return ~df[schema.user_id].isin(truncated_users)

    node7 = EventsNode(FilterEvents(params=FilterEventsParams(func=remove_truncated_paths)))
    node8 = MergeNode()
    node9 = EventsNode(SplitSessions(params=SplitSessionsParams(session_cutoff=(30, 'm'))))

    # linking the nodes to get the graph
    graph = PGraph(stream)
    graph.add_node(node=node1, parents=[graph.root])
    graph.add_node(node=node2, parents=[node1])
    graph.add_node(node=node3, parents=[node2])
    graph.add_node(node=node4, parents=[node3])
    graph.add_node(node=node5, parents=[node3])
    graph.add_node(node=node6, parents=[node5])
    graph.add_node(node=node7, parents=[node6])
    graph.add_node(node=node8, parents=[node4, node7])
    graph.add_node(node=node9, parents=[node8])

    # getting the calculation results
    processed_stream = graph.combine(node=node9)
    processed_stream.to_dataframe().head()

.. _preprocessing_chain_usage_complex_example:

Method chaining preprocessing graph
-----------------------------------

In the previous section we have constructed complex example.

But let us consider one more way of preprocessing graph usage.
It is based on :ref:`method chaining<helpers_and_chain_usage>` approach and could be easily applied
if there is no need to merge different branches of preprocessing graph.
In the end we will illustrate the result with :doc:`TransitionGraph<transition_graph>` visualization.

We are going to use the same simple-onlineshop dataset converted to the eventstream object.
If we try to use ``TransitionGraph`` without applying data processors, we can get
results that are difficult to analyze:

.. code-block:: python

    stream.transition_graph()

.. raw:: html


    <iframe
        width="700"
        height="600"
        src="../_static/user_guides/preprocessing/transition_graph.html"
        frameborder="0"
        align="left"
        allowfullscreen
    ></iframe>


By using the transition graph interactive options, we could focus
on specific event transitions. However, even the
general user workflow can be difficult to see - because of many
ungrouped events, loops, and states.

We can address this problem by using a combination of data processors we
have seen previously. One example of a processing graph would look like
this:

-  apply **DeleteUsersByPathLength** to remove users that could have
   appeared by accident;

-  apply **StartEndEvents** to mark the start and finish user states;

-  apply **SplitSessions** to mark user sessions;

-  apply **GroupEvents** multiple times to group similar events into
   groups;

-  apply **CollapseLoops** with different parameters for different loop
   representations on the transition graph plot.

.. figure:: /_static/user_guides/preprocessing/preprocessing_pgraph_chain.png



As the result, we should get three similar eventstreams that differ only
in their way of encoding loops. That is the main inherent advantage of
using the graph structure for transformations. We only need to execute
all common data processors once, and then we can quickly alternate
between different "heads" of the transformation.

Let us compose this graph:

.. code-block:: python

    def group_browsing(df, schema):
        return df[schema.event_name].isin(['catalog', 'main'])

    def group_products(df, schema):
        return df[schema.event_name].isin(['product1', 'product2'])

    def group_delivery(df, schema):
        return df[schema.event_name].isin(['delivery_choice', 'delivery_courier', 'delivery_pickup'])

    def group_payment(df, schema):
        return df[schema.event_name].isin(['payment_choice', 'payment_done', 'payment_card', 'payment_cash'])


    stream_7_nodes = stream.delete_users(events_num=6)\
                            .add_start_end()\
                            .split_sessions(session_cutoff=(30, 'm'))\
                            .group(event_name='browsing', func=group_browsing)\
                            .group(event_name='delivery', func=group_delivery)\
                            .group(event_name='payment', func=group_payment)

Looking at the simplest version, where loops are replaced with the
event they consist of:

.. code-block:: python

    stream_out = stream_7_nodes.collapse_loops(suffix=None)
    stream_out.transition_graph()



.. raw:: html

    <iframe
        width="700"
        height="600"
        src="../_static/user_guides/preprocessing/transition_graph_collapse_loops_none.html"
        frameborder="0"
        align="left"
        allowfullscreen
    ></iframe>


This transition graph is much more comprehensible. After applying the
data processors, we can see that:

- All sessions start with a "browsing" event. And more than 30% of transitions
  from "browsing" lead to the end of the session.

- There many returning sessions - 2459 transitions lead to further sessions.

- After transitioning from "cart" to "delivery", about 30% of transitions
  do not proceed to "payment".

We can also see the general user flow quite clearly now, which is a huge
improvement compared to the original plot.

To learn more about loops and where they occur, let us plot two other
versions of the eventstream:

.. code-block:: python

    stream_out = stream_7_nodes.collapse_loops(suffix='loop')
    stream_out.transition_graph()



.. raw:: html

    <iframe
        width="700"
        height="600"
        src="../_static/user_guides/preprocessing/transition_graph_collapse_loops_loop.html"
        frameborder="0"
        align="left"
        allowfullscreen
    ></iframe>


In this plot (which is a bit more convoluted than the previous one), we
see that loops mostly occur when users are browsing, and are less
frequent at the ``delivery`` or ``payment stages``. However, there are a
lot more transitions to ``payment_loop`` or ``delivery_loop`` than there
are to ``payment`` or ``delivery``!

This could suggest that there is a problem with the delivery/payment
process, or that we could improve the process by reducing the number of
transitions (i.e. "clicks") it takes to make an order a delivery or to
pay.

Now we can attempt to look at the typical loop length using the third
created eventstream:

.. code-block:: python

    stream_out = stream_7_nodes.collapse_loops(suffix='count')
    stream_out.transition_graph()



.. raw:: html

     <iframe
        width="700"
        height="600"
        src="../_static/user_guides/preprocessing/transition_graph_collapse_loops_count.html"
        frameborder="0"
        align="left"
        allowfullscreen
    ></iframe>


This plot is much more complex than the previous two; to properly
analyze it, we would need to filter out some loop events based on their
frequency. Still, we can see that the longest loops occur at the
browsing stage - and cart, payment, or delivery loops are limited by 2-3
steps, meaning that the problem we found might not be as critical as it
first appeared.
