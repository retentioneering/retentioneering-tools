Preprocessing
=============

.. _preprocessing_case_study:

Case study
----------

Let's do a case study and construct a preprocessing graph using our demonstration :doc:`simple_shop dataset <../datasets/simple_shop>` dataset. Suppose we want to transform the initial eventstream in the following way:

- Group ``product1`` and ``product2`` events into single ``product`` event;
- Among the users who are not identified as new, we want to remove those trajectories which are considered as truncated.
- Split the paths into the sessions;
- Add ``path_start`` and ``path_end`` events.


Preprocessing GUI
-----------------


Code-generated preprocessing graph
----------------------------------

For more complicated scenarios of using the preprocessing module it might be useful to generate a preprocessing graph from code. This usually relates to complex research covering multiple hypothesis and involving much data wrangling.

Here's an outline for creation a preprocessing graph with code:

- Create an instance of the preprocessing graph :py:meth:`PGraph<retentioneering.graph.p_graph.PGraph>` class;
- Create instances of :py:meth:`EventsNode <retentioneering.graph.nodes.EventsNode>` class (in some cases you need to use :py:meth:`MergeNode <retentioneering.graph.nodes.MergeNode>`; we'll discuss it later);
- Link the nodes with edges by calling :py:meth:`PGraph.add_node() <retentioneering.graph.p_graph.PGraph.add_node>` assigning appropriate nodes as parents.
- In order to run the all the calculations from the root to a particular node call :py:meth:`PGraph.combine() <retentioneering.graph.p_graph.PGraph.combine>` method passing an identifier of a target node.

For simplicity, we'll use :ref:`the same case study <preprocessing_case_study>` mentioned above.

Creating an empty graph
~~~~~~~~~~~~~~~~~~~~~~~

The only parameter of the :py:meth:`PGraph<retentioneering.graph.p_graph.PGraph>` constructor is an eventstream the graph will be associated with. Here we use ``simple_shop`` dataset again.

.. code-block:: python

    from retentioneering import datasets
    from retentioneering.graph.p_graph import PGraph

    stream = datasets.load_simple_shop()
    pgraph = PGraph(stream)

Creating a single node
~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`EventsNode <retentioneering.graph.nodes.EventsNode>` is a basic class for a preprocessing graph node representation. This is a wrapping class for :doc:`data processors <../api/preprocessing_api>` that is used in the context of preprocessing graph. That's why any node is associated with a specific data processor and its specific parameters. Let's create for example a node for :doc:`GroupEvents <../api/data_processors/group>`.

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
    node1 = EventsNode(data_processor)

What's happening in this example. Each data processor has its own set of parameters. This set is represented as a corresponding parameter class. In this example class ``GroupEventsParams`` is connected to ``GroupEvents`` data processor. So before we create a data processor instance, we need to create an instance of the corresponding parameters class (``data_processor_params`` variable). Next, we pass the parameters instance to the only parameter ``params`` of a data processor class constructor and get ``data_processor`` variable. Finally, we pass the data processor instance to ``EventsNode`` class constructor and get our node.

Since all three classes' constructors involved in the node creation process have a single parameter, it's convenient to create a node with a single line of code as follows:

.. code-block:: python

    node1 = EventsNode(GroupEvents(params=GroupEventsParams(**group_events_params)))

We've assigned the node instance to ``node1`` variable meaning that the other nodes will be assigned to ``node<N>`` variables.

Let's create the other nodes step-by-step.

To get the list of the new users that we already seen in preprocessing GUI section we get the list of unique users (``users`` variable), and then randomly sample a half of them. As soon as we get ``new_users`` list we pass it to ``NewUsersParams``.

.. code-block:: python

    import numpy as np
    from retentioneering.data_processors_lib import NewUsersEvents, NewUsersParams

    users = stream.to_dataframe()['user_id'].unique()
    np.random.seed(42)
    new_users = np.random.choice(users, size=len(users)//2).tolist()
    node2 = EventsNode(NewUsersEvents(params=NewUsersParams(new_users_list=new_users)))

Creation of the next node (``TruncatedEvents``) is not difficult. We just want to highlight here that sometimes it's convenient to apply python unpacking technique. First, we collect all the data processor's parameters to a dictionary, and then pass them to ``TruncatedEventsParams`` as the unpacked dictionary.

.. code-block:: python

    from retentioneering.data_processors_lib import TruncatedEvents, TruncatedEventsParams

    params = {
        "left_truncated_cutoff": (1, 'h'),
        "right_truncated_cutoff": (1, 'h'),
    }
    node3 = EventsNode(TruncatedEvents(params=TruncatedEventsParams(**params)))

At this step we demonstrate you how :doc:`FilterEvents <../api/data_processors/filter>` nodes work. However, this is very similar to ``GroupEvents`` which we have already discussed.

.. code-block:: python

    from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams

    def get_existing_user_paths(df, schema):
        existing_users = df[df[schema.event_name] == 'existing_user']\
            [schema.user_id]\
            .unique()
        return df[schema.user_id].isin(existing_users)

    def get_new_user_paths(df, schema):
        return df[schema.user_id].isin(new_users)

    def remove_truncated_paths(df, schema):
        truncated_users = df[df[schema.event_name].isin(['truncated_left', 'truncated_right'])]\
            [schema.user_id]\
            .unique()
        return ~df[schema.user_id].isin(truncated_users)

    node4 = EventsNode(FilterEvents(params=FilterEventsParams(func=get_existing_user_paths)))
    node5 = EventsNode(FilterEvents(params=FilterEventsParams(func=remove_truncated_paths)))
    node6 = EventsNode(FilterEvents(params=FilterEventsParams(func=get_new_user_paths)))

The next step is a bit tricky. Here we introduce you :py:meth:`MergeNode <retentioneering.graph.nodes.MergeNode>` -- a special class for merging nodes. Unlike ``EventsNode``, ``MergeNode`` is not associated with any data processor since it has a separate role -- concatenate the outputs ot its parents. Another distinction from ``EventsNode`` is that the number of parents might be arbitrary (greater than 1).

.. code-block:: python

    from retentioneering.graph.p_graph import MergeNode

    node7 = MergeNode()

Finally, we create the last two nodes for :doc:`SplitSessions <../api/data_processors/split_sessions>` and :doc:`StartEndEvents <../api/data_processors/add_start_end>` data processors.

.. code-block:: python

    from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams
    from retentioneering.data_processors_lib import StartEndEvents, StartEndEventsParams

    node8 = EventsNode(SplitSessions(params=SplitSessionsParams(session_cutoff=(30, 'm'))))
    node9 = EventsNode(StartEndEvents(params=StartEndEventsParams()))

Please note that :doc:`StartEndEvents <../api/data_processors/add_start_end>` data processor have no parameters, but even in this case we have to pass an empty ``StartEndEventsParams`` instance to ``params`` argument.

.. code-block:: python

    from retentioneering.graph.p_graph import PGraph

    graph = PGraph(stream)
    graph.add_node(node=node1, parents=[graph.root])
    graph.add_node(node=node2, parents=[node1])
    graph.add_node(node=node3, parents=[node2])
    graph.add_node(node=node4, parents=[node3])
    graph.add_node(node=node5, parents=[node4])
    graph.add_node(node=node6, parents=[node2])
    graph.add_node(node=node7, parents=[node5, node6])
    graph.add_node(node=node8, parents=[node7])
    graph.add_node(node=node9, parents=[node8])



Linking nodes
~~~~~~~~~~~~~



Running calculation
~~~~~~~~~~~~~~~~~~~


.. code-block:: python

    from retentioneering.graph.p_graph import PGraph, EventsNode





.. code-block:: python

    from retentioneering.graph.p_graph import PGraph, EventsNode
    from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams
    from retentioneering.data_processors_lib import StartEndEvents, StartEndParams

    # creating single nodes
    node1 = EventsNode(StartEndEvents(params=StartEndEventsParams()))
    node2 = EventsNode(SplitSessions(params=SplitSessionsParams(session_cutoff=(1, 'h'))))

    # creating a preprocessing graph and linking the nodes
    pgraph = PGraph(source_stream=stream)
    pgraph.add_node(node=node1, parents=[pgraph.root])
    pgraph.add_node(node=node2, parents=[node1])
