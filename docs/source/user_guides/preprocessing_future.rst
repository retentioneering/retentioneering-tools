.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red

Preprocessing
=============

:red:`TODO: Mention what the preprocessing is and where it comes from`.

:red:`TODO: Refer to eventstream concept`.

:red:`TODO: Recommend to read data processors UG before reading this`.

:red:`Warning: the document contains many hyperlinks related to this UG. Pass them to colab carefully`

.. _preprocessing_case_study:

Case study
----------

Problem statement
~~~~~~~~~~~~~~~~~

The best way to explain the variety of the preprocessing options that retentioneering offers is to consider a case study. We will construct a preprocessing graph using our demonstration :doc:`simple_shop dataset <../datasets/simple_shop>` dataset assuming that its instance is assigned to ``stream`` variable.

Suppose we want to transform the initial eventstream in the following way:

1. Add ``path_start`` and ``path_end`` events.
2. Group ``product1`` and ``product2`` events into single ``product`` event;
3. Among the users who are not identified as new, we want to remove those trajectories which are considered as truncated;
4. Split the paths into the sessions;

The idea behind this case is that we want to demonstrate not only "linear" preprocessing actions (i.e. the actions which are applied ste-by-step), but to show branching and merging preprocessing logic as well.

A draft of a solution
~~~~~~~~~~~~~~~~~~~~~

As for requirements 1, 2, and 4 of the case study, they straightforward. Each of them relates to a single data processor application. But requirement 3 is a bit tricky. First, we need to apply :py:meth:`NewUsersEvents <retentioneering.data_processors_lib.new_users.NewUsersEvents>` data processor, marking the trajectories with ``new_user`` and ``existing_users`` markers. Next, we apply :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>` data processor twice: once for getting the new users from the previous step, and then for getting the already existing users. Please note that the preprocessing flow splits at this point. Next, for the branch related to the existing users we need to sequentially apply :py:meth:`TruncatedEvents <retentioneering.data_processors_lib.truncated_events.TruncatedEvents>` data processor for marking the paths as truncated or not, and then another :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>` data processors to leave intact trajectories only. An outline of the described solution is represented below.

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_outline.png
    :height: 600

Please pay attention to splitting and merging logic. After the 3rd node the eventstream is split into the two disjoint eventstreams (one contains only new users, another one contains existing users only). Once we finish processing the existing users' trajectories we need to merge these two eventstreams. There's a special merging node develped for this purpose. We'll talk about it later in this user guide.

There are two options to implement this preprocessing graph: using preprocessing GUI or using code along with special classes. In this user guide we'll describe both of them.

Preprocessing GUI
-----------------

Preprocessing GUI allows you to build a preprocessing graph using literally few lines of code. The following code displays a visual graph constructor:


.. _preprocessing_graph_creation:

.. code-block:: python

    from retentioneering.graph.p_graph import PGraph

    graph = PGraph(stream)
    graph.display()

As you see, :py:meth:`PGraph<retentioneering.graph.p_graph.PGraph>` constructor requires an instance of Eventstream. :py:meth:`PGraph.display()<retentioneering.graph.p_graph.PGraph.display>` method reveals a canvas with a single ``Source`` node which is considered as a root node for the future graph. The root is associated with the initial state of the eventstream.

To add a new node connected to the root (or any other node) click at the dot menu placed on the right part of the node. As we discussed earlier, each node of a preprocessing graph is essentially a wrap of some  data processor. So once you've created a node, you need to assign a particular data processor to it. Next, you can set or modify the node's parameters. Click on the node, and the node menu appears on the right.

As for the merging nodes, all they do is just concatenate the outputs of the previous nodes (similar to ``UNION ALL`` SQL operator). So they don't have any parameters and don't connected to any data processor.

Now, let's go back to :ref:`our case study <preprocessing_case_study>` and try to build a graph according to the provided plan. In the table below we show what nodes should be created, what data processors they are associated with, what data processors' parameters should be set, and how the nodes should be connected.

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

The last thing we need to discuss is the implementations of some auxiliary functions. Such data processors as FilterEvents and GroupEvents require python code of the functions which implement the filtering grouping logic correspondingly. Here are the listings of all these functions.

:red:`TODO: Adjust the text if needed after GroupEvents and FilterEvents are available in the preprocessing graph`

.. _preprocessing_group_products:

``group_products`` function renames ``product1`` and ``product2`` to a new ``product`` event.

.. code-block:: python

    # node2. Groups product1 and product2 events into a single event.
    def group_products(df, schema):
        return df[schema.event_name].isin(['product1', 'product2'])

.. _preprocessing_get_new_users:

``get_new_users`` function leaves the paths of the users who have ``new_user`` synthetic event.

.. code-block:: python

    # node4. Leave the paths of the new users only.
    def get_new_users(df, schema):
        new_users = df[df[schema.event_name] == 'new_user']\
            [schema.user_id]\
            .unique()
        return df[schema.user_id].isin(new_users)

.. _preprocessing_get_existing_users:

``get_existing_users`` works almost identically to ``get_new_users``, except the fact that it leaves only the users (their paths to be exact) who have ``existing_user`` synthetic event.

.. code-block:: python

    # node5. Leave the paths of the existing users only.
    def get_existing_users(df, schema):
        existing_users = df[df[schema.event_name] == 'existing_user']\
            [schema.user_id]\
            .unique()
        return df[schema.user_id].isin(existing_users)


.. _preprocessing_remove_truncated_paths:

``remove_truncated_paths`` function removes the paths of the users who have either ``truncated_left`` or ``truncated_right`` event.

.. code-block:: python

    # node7. Remove the paths which are considered as truncated.
    def remove_truncated_paths(df, schema):
        truncated_users = df[df[schema.event_name].isin(['truncated_left', 'truncated_right'])]\
            [schema.user_id]\
            .unique()
        return ~df[schema.user_id].isin(truncated_users)

As a result, you'll get a graph which looks like this:

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph.png
    :width: 800

:red:`TODO: Replace the image with a nicer one`

.. _preprocessing_result :

Get calculation result
~~~~~~~~~~~~~~~~~~~~~~

So we have built the graph, now it's time to run the entire calculation which the graph frames. Unfortunately, here we have to step back from the GUI and return to the code.

In order to run the calculation from the graph root to a specific node, call :py:meth:`PGraph.combine() <retentioneering.graph.p_graph.PGraph.combine>` method with a single paramter ``node`` which accepts the corresponding node object. As soon the calculation is finished, the result is stored at :py:meth:`PGraph.combine_result <retentioneering.graph.p_graph.PGraph.combine_result>` property. The result is represented as :py:meth:`Eventstream <retentioneering.eventstream.eventstream.Eventstream>` class. Be careful: the property stores only the result of the last calculation.

.. code-block:: python

    graph.combine(node=node9)
    processed_stream = graph.combine_result
    processed_stream.head()

:red:`TODO: insert the output of processed_stream.head()`

.. note::

    You can combine the calculations at any node. In practise, it's useful for debugging the calculations.

Code-generated preprocessing graph
----------------------------------

For more complicated scenarios of using the preprocessing module it might be convenient to generate a preprocessing graph using code purely, with no GUI at all. This usually relates to complex research covering multiple hypothesis and involving much data wrangling.

Behind each action we described in `Preprocessing GUI`_ there's a special class, method, or argument. So we'll reproduce the same steps, but implementing them with code.

Here's an outline for creation a preprocessing graph with code:

- Create an instance of the preprocessing graph :py:meth:`PGraph<retentioneering.graph.p_graph.PGraph>` class;
- Create instances of :py:meth:`EventsNode <retentioneering.graph.nodes.EventsNode>` class (or :py:meth:`MergeNode <retentioneering.graph.nodes.MergeNode>` for the merging nodes), assign a data processor to it and set the parameters using the corresponding ``*Params`` class.
- Link the nodes with edges by calling :py:meth:`PGraph.add_node() <retentioneering.graph.p_graph.PGraph.add_node>` assigning appropriate nodes as parents.
- In order to run the all the calculations from the root to a particular node call :py:meth:`PGraph.combine() <retentioneering.graph.p_graph.PGraph.combine>` method passing an identifier of a target node.

Now, let's show in details how to build a preprocessing graph implementing the same solution for :ref:`the same case study <preprocessing_case_study>`. If you're experienced enough in Python you might find useful to read :ref:`one-chunk solution <preprocessing_code_single_chunk>` skipping all the explanations.

Creating an empty graph
~~~~~~~~~~~~~~~~~~~~~~~

It was already shown how to create an empty preprocessing graph associated with an eventstream: :ref:`See <preprocessing_graph_creation>`.

.. code-block:: python

    from retentioneering.graph.p_graph import PGraph

    graph = PGraph(stream)

Any time while building a graph you can visualize it by calling :py:meth:`PGraph.display() <retentioneering.graph.p_graph.display>` method.

Creating a single node
~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`EventsNode <retentioneering.graph.nodes.EventsNode>` is a basic class for a preprocessing graph node representation. As we mentioned earlier, each node is associated with a particular :doc:`data processor <../api/preprocessing_api>` (merging node is an exception here). Let's create for example a node for :py:meth:`GroupEvents <retentioneering.data_processors_lib.group_events.GroupEvents>` (``node2``).

.. code-block:: python

    from retentioneering.graph.p_graph import EventsNode
    from retentioneering.data_processors_lib import GroupEvents, GroupEventsParams

    group_events_params = {
        "event_name": "product",
        "func": group_products
    }

    data_processor_params = GroupEventsParams(**group_events_params)
    data_processor = GroupEvents(params=data_processor_params)
    node2 = EventsNode(data_processor)

What's happening in this example. A data processor's parameters are set with a help of ``*Params`` class where the asterisk stands for a data processor name. Namely, there's :py:meth:`GroupEventsParams <retentioneering.data_processors_lib.group_events.GroupEventsParams>`. The arguments of a ``*Params`` class constructor are exactly the same as the corresponding parameter names. For :py:meth:`GroupEventsParams <retentioneering.data_processors_lib.group_events.GroupEventsParams>` they are ``event_name`` and ``func`` which we keep here as ``group_events_params`` dictionary. ``group_products`` function is the same as we've defined in :ref:`Preprocessing GUI <preprocessing_group_products>` section.

Next, we pass the parameters ``data_processor_params`` object to the only parameter ``params``  :py:meth:`GroupEvents() <retentioneering.data_processors_lib.group_events.GroupEvents>` constructor and assign it to ``data_processor`` variable.

Finally, we pass the data processor instance to ``EventsNode`` class constructor and get our node.

Since all three classes' constructors involved in the node creation process have a single parameter, it's convenient to create a node with a single line of code as follows:

.. code-block:: python

    node2 = EventsNode(GroupEvents(params=GroupEventsParams(**group_events_params)))

Linking nodes
~~~~~~~~~~~~~

In order to link a node to its parents call :py:meth:`PGraph.add_node() <retentioneering.graph.p_graph.display>`. The method accepts a node object and its parents list. A regular node must have a single parent, whereas a merging node must have at least two parents. We'll demonstrate how it works in the next sub-section.

Building the whole graph
~~~~~~~~~~~~~~~~~~~~~~~~

Let's create the other nodes step-by-step.

If you were surprised why didn't we start with ``node1``, here's a clue. The reason is that :py:meth:`StartEndEvents <retentioneering.data_processors_lib.start_end_events.StartEndEvents>` data processor doesn't have any arguments. However, even in this case we have to create an instance of :py:meth:`StartEndEventsParams <retentioneering.data_processors_lib.start_end_events.StartEndEventsParams>` and pass it to the data processor constructor. Look how you can do it:

.. code-block:: python

    from retentioneering.data_processors_lib import StartEndEvents, StartEndEventsParams

    node1 = EventsNode(StartEndEvents(params=StartEndEventsParams()))
    graph.add_node(node=node1, parents=[graph.root])

Since the node is the first, we link it to the special ``graph.root`` parent. Please note that event the parent is single, we still have to wrap it to a list before passing to the ``parents`` paramter.

To create ``node3`` we need to download the list of the new users beforehand. We assign the list to ``new_users`` variable and then pass it to :py:meth:`NewUsersParams <retentioneering.data_processors_lib.new_users.NewUsersParams>`

.. code-block:: python

    from retentioneering.data_processors_lib import NewUsersEvents, NewUsersParams

    google_drive_file_id = '1ixztbZj1GWg_xNpTZOKGOYBtoJlJmOtO'
    link = f'https://drive.google.com/uc?id={google_drive_file_id}&export=download'
    new_users = pd.read_csv(link, header=None)[0].tolist()
    node3 = EventsNode(NewUsersEvents(params=NewUsersParams(new_users_list=new_users)))
    graph.add_node(node=node2, parents=[node1])

Creation of the next ``node4`` and ``node5`` is very similar. We need to create a couple of nodes with :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>` data processors and pass them the same filtering functions :ref:`get_new_users() <preprocessing_get_new_users>` and :ref:`get_existing_users() <preprocessing_get_existing_users>` that we used in `Preprocessing GUI`_ section.

.. code-block:: python

    from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams

    node4 = EventsNode(FilterEvents(params=FilterEventsParams(func=get_new_users)))
    node5 = EventsNode(FilterEvents(params=FilterEventsParams(func=get_existing_users)))
    graph.add_node(node=node4, parents=[node3])
    graph.add_node(node=node5, parents=[node3])

There's nothing new in creation of the ``node6``. We just pass a couple of ``left_truncated_cutoff`` and ``right_truncated_cutoff`` parameters to :py:meth:`TruncatedEventsParams <retentioneering.data_processors_lib.truncated_events.TruncatedEventsParams>` and set up a :py:meth:`TruncatedEvents <retentioneering.data_processors_lib.truncated_events.TruncatedEvents>` node. However, we should mention that since ``node3`` is the splitting point in the graph, nodes 4 and 5 both have the same ``node3`` parent.

.. code-block:: python

    from retentioneering.data_processors_lib import TruncatedEvents, TruncatedEventsParams

    params = {
        "left_truncated_cutoff": (1, 'h'),
        "right_truncated_cutoff": (1, 'h'),
    }
    node6 = EventsNode(TruncatedEvents(params=TruncatedEventsParams(**params)))
    graph.add_node(node=node6, parents=[node5])

For ``node7`` we apply similar filtering technique as we used for filtering new/existing users above. Function :ref:`remove_truncated_paths() <preprocessing_remove_truncated_paths>` is the same as we used in `Preprocessing GUI`_ section.

.. code-block:: python

    node7 = EventsNode(FilterEvents(params=FilterEventsParams(func=remove_truncated_paths)))
    graph.add_node(node=node7, parents=[node6])

Next, ``node8``. As we discussed earlier, :py:meth:`MergeNode <retentioneering.graph.nodes.MergeNode>` has two special features. Unlike ``EventsNode``, ``MergeNode`` is not associated with any data processor since it has a separate role -- concatenate the outputs ot its parents. Another distinction from ``EventsNode`` is that the number of parents might be arbitrary (greater than 1). The following two lines of the code demonstrate both these features:

.. code-block:: python

    from retentioneering.graph.p_graph import MergeNode

    node8 = MergeNode()
    graph.add_node(node=node8, parents=[node4, node7])


Finally, for ``node9`` we wrap :py:meth:`SplitSessions <retentioneering.data_processors_lib.split_sessioins.SplitSessions>` passing a single parameter ``session_cutoff`` and link it to the merging node:

.. code-block:: python

    from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams

    node9 = EventsNode(SplitSessions(params=SplitSessionsParams(session_cutoff=(30, 'm'))))
    graph.add_node(node=node9, parents=[node8])


Running calculation
~~~~~~~~~~~~~~~~~~~

The way of getting the calculation result is identical to the way we've described in :ref:`Preprocessing GUI <preprocessing_result>` section.

.. _preprocessing_code_single_chunk:

Summary
~~~~~~~

Here we just provide the same code combined in a single chunk so you could simply copy and paste it and see the results.

.. code-block:: python

    from retentioneering.data_processors_lib import StartEndEvents, StartEndEventsParams
    from retentioneering.data_processors_lib import GroupEvents, GroupEventsParams
    from retentioneering.data_processors_lib import NewUsersEvents, NewUsersParams
    from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams
    from retentioneering.data_processors_lib import TruncatedEvents, TruncatedEventsParams
    from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams
    from retentioneering.graph.p_graph import PGraph, EventsNode, MergeNode

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
    node6 = EventsNode(FilterEvents(params=FilterEventsParams(func=remove_truncated_paths)))

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
    graph.combine(node=node9)
    processed_stream = graph.combine_result
    processed_stream.head()
