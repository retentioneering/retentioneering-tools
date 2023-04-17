Preprocessing graph
===================

The following user guide is also available as `Google Colab notebook <https://colab.research.google.com/drive/1ogWPkxvDSklGcSZLvx30mrol0Ms3ENO9?usp=share_link>`_.

The concept of preprocessing graph
----------------------------------

By preprocessing we mean any eventstream data transformation preceding applying analytical tools. This process is crucial for analytical research. Some analytical methods are sensitive to data they accept as input, so the data must be prepared in a special way. Another case when the preprocessing arises is when you want to explore some parts of an eventstream (instead of the entire eventstream) so you have to truncate the data in an efficient way. Finally, often you want to clean and wrangle the data in order to remove technical, misleading events or user paths entirely.

The Preprocessing module allows you to treat all the data preparations efficiently. The core element of this module is a :doc:`data processor <../api/preprocessing_api>`. This document does not cover the details on how each data processor works, so if you have not read :doc:`data processors user guide <../user_guides/dataprocessors>` so far, we recommend you to explore it.

Applying a single data processor in most cases is not enough for solving your practical preprocessing problem. In fact, preprocessing pipelines often involve multiple data processors, and requre complex splitting and merging logic. That is why it is natural to use graph structures to describe all these calculations. We introduce you to the *preprocessing graph* - an object which organises preprocessing calculations.

The idea of preprocessing graph is simple. Each node of the graph is a single data processor. The nodes are linked according to the sequential order of the calculations. The graph root is associated with the original state of an eventstream. On the other hand, any graph node may be considered as a specific state of the original eventstream. This state is comprehensively described by the sequence of data processors we apply following the graph path from the root to this specific node.

Note that a preprocessing graph just frames the calculation logic. To get an eventstream state corresponding to a specific graph node, we need to run the calculation explicitly. See :ref:`Running the calculation <preprocessing_running_the_calculation>` section for the details.

.. _preprocessing_case_study:

Case study
----------

Problem statement
~~~~~~~~~~~~~~~~~

The best way to explain the variety of the preprocessing features that retentioneering offers is to consider a case study. We will construct a preprocessing graph using our demonstration :doc:`simple_shop dataset <../datasets/simple_shop>`, which we load as an ``Eventstream`` object. You can learn more about ``Eventstream`` in our :doc:`eventstream guide<eventstream>`.

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

The idea behind this case is that we want to demonstrate not only "linear" preprocessing actions (i.e. the calculations which are executed step-by-step, with no branching logic required), but to show more complex branching and merging preprocessing logic as well.

A draft of a solution
~~~~~~~~~~~~~~~~~~~~~

:doc:`Data processors <../user_guides/dataprocessors>` are bricks for our preprocessing. So we have to represent our solution as a combination of the data processors.

As for requirements 1, 2, and 4 of the case study, they are straightforward. Each of them relates to a single data processor application. In contrast, requirement 3 is a bit tricky. First, we need to apply the :py:meth:`LabelNewUsers <retentioneering.data_processors_lib.label_new_users.LabelNewUsers>` data processor, marking the trajectories with the ``new_user`` and ``existing_user`` markers. Next, we apply the :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>` data processor twice: once to get the new users from the previous step, and then to get the existing users. Note that the preprocessing flow splits at this point. Next, for the branch related to the existing users we need to sequentially apply the :py:meth:`LabelCroppedPaths <retentioneering.data_processors_lib.label_cropped_paths.LabelCroppedPaths>` data processor for marking the paths as truncated or not, and then another :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>` data processor to leave intact trajectories only. Finally, we need to merge the data from the two separated branches and apply the :py:meth:`SplitSessions <retentioneering.data_processors_lib.split_sessions.SplitSessions>` data processor in the end. An outline of the described solution is represented on the image below.

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_outline.png
    :height: 600

    An outline of the possible case study solution.

Pay attention to the splitting and merging logic. After the 3rd node the eventstream is split into the two disjoint eventstreams (one contains only new users, another contains only existing users). Once we finish processing the existing users' trajectories we need to merge these two eventstreams. There's a special merging node developed for this purpose. We'll talk about it later in this user guide.

Next, we specify the information about the graph nodes and the underlying data processors. The table below contains the list of the nodes, the data processors they are associated with, and the particular parameters they need to be applied to them. We find this a bit more informative and we wil build the preprocessing graph according to this plan.

.. _preprocessing_solution_plan:

.. table:: The schema of the preprocessing graph for the case study
    :widths: 10 20 40 20
    :class: tight-table

    +-------+-------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | Node  | Data processor                                                                                              | Parameters                                                                                                                                                    | Parents      |
    +=======+=============================================================================================================+===============================================================================================================================================================+==============+
    | node1 | :py:meth:`AddStartEndEvents <retentioneering.data_processors_lib.add_start_end_events.AddStartEndEvents>`   | –                                                                                                                                                             | Source       |
    +-------+-------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node2 | :py:meth:`GroupEvents <retentioneering.data_processors_lib.group_events.GroupEvents>`                       | ``event_name='product'``, ``func=group_products``                                                                                                             | node1        |
    +-------+-------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node3 | :py:meth:`LabelNewUsers <retentioneering.data_processors_lib.label_new_users.LabelNewUsers>`                | pass `this csv-file <https://docs.google.com/spreadsheets/d/1iggpIT5CZcLILLZ94wCZPQv90tERwi1IB5Y1969C8zc/edit?usp=sharing>`_  to ``new_users_list`` parameter | node2        |
    +-------+-------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node4 | :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>`                    | ``func=get_new_users``                                                                                                                                        | node3        |
    +-------+-------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node5 | :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>`                    | ``func=get_existing_users``                                                                                                                                   | node3        |
    +-------+-------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node6 | :py:meth:`LabelCroppedPaths <retentioneering.data_processors_lib.label_cropped_paths.LabelCroppedPaths>`    | ``left_truncated_cutoff=(1, 'h')``, ``right_truncated_cutoff=(1, 'h')``                                                                                       | node5        |
    +-------+-------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node7 | :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>`                    | ``func=remove_truncated_paths``                                                                                                                               | node6        |
    +-------+-------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node8 | :py:meth:`MergeNode <retentioneering.graph.nodes.MergeNode>`                                                | –                                                                                                                                                             | node4, node7 |
    +-------+-------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+
    | node9 | :py:meth:`SplitSessions <retentioneering.data_processors_lib.split_sessions.SplitSessions>`                 | ``session_cutoff=(30, 'm')``                                                                                                                                  | node8        |
    +-------+-------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+

The functions which are passed to ``func`` parameter in the FilterEvents and GroupEvents data processors will be defined below.

There are two ways to build a preprocessing graph: with the preprocessing GUI tool or with code. We start from the GUI and then move to the code-generated graphs in the next section.

.. _preprocessing_gui:

Preprocessing GUI
-----------------

Preprocessing GUI tool allows to create preprocessing graphs using graphical interface. To display this tool, call the :py:meth:`Eventstream.preprocessing_graph() <retentioneering.eventstream.eventstream.Eventstream.preprocessing_graph>` method:

..
    TODO: check the API link. Vladimir Kukushkin.

.. code-block:: python

    pgraph = stream.preprocessing_graph()

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_gui_empty.png
    :height: 600

    An empty preprocessing graph.

As we see, an empty graph contains a single source node that is associated with the sourcing eventstream. Let us create the first node in the graph according to the plan. Click on the triple dots inside the node, select "Add data processor node" option and choose the ``AddStartEndEvents`` data processor as it is shown below:

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_gui_add_node.png
    :height: 350

    Choosing a data processor that wraps the node.

``AddStartEndEvents`` node appears. It is connected to the sourcing node. If we click on the node, on the right we will see the node menu. Since :py:meth:`StartEndEvents <retentioneering.data_processors_lib.add_start_end_events.StartEndEvents>` data processor has no parameters, the only option available in the menu is a subtitle. Let us label the node with ``node1`` according to the plan.

There is another important option which is worth to be mentioned. In the bottom you can see "Save Graph". If you click it, the current state of the preprocessing graph is saved into the sourcing eventstream. So if you run ``stream.preprocessing_graph()`` again, the graph state will be restored.

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_gui_node_menu_1.png
    :height: 600

    The node menu for the AddStartEndEvents data processor.

Let us create the second node: ``GroupEvents``. Click at ``node1``'s triple dots (we note that "Delete node" option is available now as well), choose "Add data processor node" and choose ``GroupEvents``. If you click on this node, the node menu appears, where you can enter the following parameter values:

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_gui_node_menu_2.png
    :height: 600

    The node menu for the GroupEvents data processor.

As we see, the menu contains all the parameters of the :py:meth:`GroupEvents <retentioneering.data_processors_lib.group_events.GroupEvents>` data processor so you can set them right here. We set the node subtitle to ``node2``, ``event_name`` to ``product``, and leave ``event_type`` as is. As for the ``func`` parameter, we just copy & paste the following code:

.. code-block:: python

    def group_products(df, schema):
        return df[schema.event_name].isin(['product1', 'product2'])

Next, we create :py:meth:`LabelNewUsers <retentioneering.data_processors_lib.label_new_users.LabelNewUsers>` as ``node3``. Then we download `the file <https://docs.google.com/spreadsheets/d/1iggpIT5CZcLILLZ94wCZPQv90tERwi1IB5Y1969C8zc/edit?usp=sharing>`_ containing new users ids and upload it to the ``new_users_list`` argument. Manual input is also supported, but since the number of the new users is high, it is more reasonable to upload them from the file.

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_gui_label_new_users_events.png
    :height: 600

    How to upload the list of new users.

Now, we are going to implement splitting logic for ``node4`` and ``node5``. You can create two :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>` children from ``node3`` . Similar to ``node2``, we use ``get_new_users`` function for ``node4`` and ``get_existing_users`` for ``node5``. The functions are defined below:

.. code-block:: python

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

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_gui_nodes_4_5.png
    :height: 600

    Splitting the logic after ``node3``.

At the next step we create :py:meth:`LabelCroppedPaths <retentioneering.data_processors_lib.label_cropped_paths.LabelCroppedPaths>` as ``node6`` with ``left_cropped_cutoff=(1, 'h')`` parameter. Then we connect another :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>` node (``node7``) with the ``remove_truncated_paths`` function defined below:

.. code-block:: python

    def remove_truncated_paths(df, schema):
        truncated_users = df[df[schema.event_name].isin(['cropped_left', 'cropped_right'])]\
            [schema.user_id]\
            .unique()
        return ~df[schema.user_id].isin(truncated_users)

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_gui_nodes_6_7.png
    :height: 600

    ``node6`` and ``node7`` creation.

Now, we need to merge two branches into one node. Special :py:meth:`MergeNode <retentioneering.graph.nodes.MergeNode>` is designed for this purpose. To merge multiple branches select the ending points of these branches (at least 2) using Ctrl+click or Cmd+click, "Merge Nodes" button appears in the top of the canvas. After clicking this button, the merging node appears.

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_gui_merge_node_creation.png
    :height: 600

    Merge node creation.

Finally, we create the last node :py:meth:`SplitSessions <retentioneering.data_processors_lib.split_sessions.SplitSessions>` inheriting it from the merging node and setting up ``session_cutoff=(30, 'm')`` parameter.

.. figure:: /_static/user_guides/preprocessing/preprocessing_graph_gui_node_9.png
    :height: 600

    SplitSessions node.

The graph is ready. It is time to show how to launch the calculation related to this graph. Click on the node you associate with a calculation endpoint. In our case this is ``node9``. As it is shown in the previous screenshot, in the top right corner you will see "Save & Combine" (or just "Combine" if the graph has already been saved). As soon as the result is combined, you can extract the resulting eventstream by accessing :py:meth:`PGraph.combine_result<retentioneering.graph.p_graph.PGraph>`. This attribute keeps the last combining result.

.. code-block:: python

    pgraph.combine_result.to_dataframe()

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

Code-generated preprocessing graph
----------------------------------

In this section we will explain how to build the same preprocessing graph as a solution for the :ref:`case study <preprocessing_case_study>` but using code only. The supplementary functions that are used for ``GroupEvents`` and ``FilterEvents`` (namely, ``group_products``, ``get_new_users``, ``get_existing_users``, and ``remove_truncated_paths``) are the same as we used in the :ref:`Preprocessing GUI <preprocessing_gui>` section.

We are starting from creating an empty graph.

.. _preprocessing_graph_creation:

.. code-block:: python

    from retentioneering.graph.p_graph import PGraph

    pgraph = PGraph(stream)

As you see, :py:meth:`PGraph<retentioneering.graph.p_graph.PGraph>` constructor requires an instance of Eventstream. The graph's root is associated with the initial state of the eventstream which will be changed according to the graph logic.

Creating a single node
~~~~~~~~~~~~~~~~~~~~~~

:py:meth:`EventsNode <retentioneering.graph.nodes.EventsNode>` is a basic class for preprocessing graph node representation. As we mentioned earlier, each node is associated with a particular :doc:`data processor <../api/preprocessing_api>` (merging node is an exception). As an example, let us create a :py:meth:`GroupEvents <retentioneering.data_processors_lib.group_events.GroupEvents>` node (``node2``).

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

What is happening in this example? The data processor's parameters are set with the help of ``*Params`` class where the asterisk stands for a data processor name. Namely, there is :py:meth:`GroupEventsParams <retentioneering.data_processors_lib.group_events.GroupEventsParams>` parameter class for :py:meth:`GroupEvents <retentioneering.data_processors_lib.group_events.GroupEvents>`. The arguments of a ``*Params`` class constructor are exactly the same as the corresponding parameter names. For :py:meth:`GroupEventsParams <retentioneering.data_processors_lib.group_events.GroupEventsParams>` they are ``event_name`` and ``func`` which we keep here as ``group_events_params`` dictionary items. ``group_products`` function returns the mask for grouping events ``product1`` and ``product2``.

Next, we pass ``data_processor_params`` object to the only parameter ``params`` of the :py:meth:`GroupEvents() <retentioneering.data_processors_lib.group_events.GroupEvents>` constructor and assign its result to the ``data_processor`` variable.

Finally, we pass the data processor instance to the ``EventsNode`` class constructor and get our node.

Since all three classes' constructors involved in the node creation process have a single parameter, it's convenient to create a node with a single line of code as follows:

.. code-block:: python

    node2 = EventsNode(GroupEvents(params=GroupEventsParams(**group_events_params)))

If you were surprised why we did not start with ``node1`` according to the plan, here is a clue. The reason is that the :py:meth:`AddStartEndEvents <retentioneering.data_processors_lib.add_start_end_events.AddStartEndEvents>` data processor does not have any arguments. However, even in this case we have to create an instance of ``StartEndEventsParams`` and pass it to the data processor constructor. Look how you can do it:

.. code-block:: python

    from retentioneering.data_processors_lib import AddStartEndEvents, AddStartEndEventsParams

    node1 = EventsNode(AddStartEndEvents(params=AddStartEndEventsParams()))

Linking nodes
~~~~~~~~~~~~~

In order to link a node to its parents, call :py:meth:`PGraph.add_node() <retentioneering.graph.p_graph.PGraph.add_node>`. The method accepts a node object and its parents list. A regular node must have a single parent, whereas a merging node must have at least two parents. We will demonstrate how merging nodes work in the next subsection. As of now, here is how to connect a pair of nodes of our graph:

.. code-block:: python

    pgraph.add_node(node=node1, parents=[pgraph.root])
    pgraph.add_node(node=node2, parents=[node1])

Note that ``node1`` is linked to a special ``graph.root`` node which is a mandatory attribute of any graph. ``node2`` is connected to a regular node ``node1``.

So we have described how to create the graph nodes and how to link the nodes. Using these two basic operations we can construct the whole graph.

Building the whole graph
~~~~~~~~~~~~~~~~~~~~~~~~

Let us create the other graph nodes and link them step-by-step according to the :ref:`plan <preprocessing_solution_plan>`.

To create ``node3`` we need either to `download <https://docs.google.com/spreadsheets/d/1iggpIT5CZcLILLZ94wCZPQv90tERwi1IB5Y1969C8zc/edit?usp=sharing>`_ the list of the new users beforehand. This list contains user_ids of the users who are considered as new (i.e. they have not visited the system any time before the dataset start). We assign the downloaded list to ``new_users`` variable and then pass it to :py:meth:`LabelNewUsersParams <retentioneering.data_processors_lib.label_new_users.LabelNewUsersParams>`.

.. code-block:: python

    from retentioneering.data_processors_lib import LabelNewUsers, LabelNewUsersParams

    google_spreadsheet_id = '1iggpIT5CZcLILLZ94wCZPQv90tERwi1IB5Y1969C8zc'
    link = f'https://docs.google.com/spreadsheets/u/1/d/{google_spreadsheet_id}/export?format=csv&id={google_spreadsheet_id}'
    new_users = pd.read_csv(link, header=None)[0].tolist()
    node3 = EventsNode(LabelNewUsers(params=LabelNewUsersParams(new_users_list=new_users)))
    pgraph.add_node(node=node3, parents=[node2])

Creation of the next ``node4`` and ``node5`` is similar. We need to create a couple of nodes with :py:meth:`FilterEvents <retentioneering.data_processors_lib.filter_events.FilterEvents>` data processors and pass them filtering functions ``get_new_users()`` and ``get_existing_users()``. These two functions recognize synthetic events ``new_user`` and ``existing_user`` added by LabelNewUsers data processor at the previous step and leave the paths of new users and existing users only correspondingly.

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
    pgraph.add_node(node=node4, parents=[node3])
    pgraph.add_node(node=node5, parents=[node3])

There is nothing new in the creation of the ``node6``. We just pass a couple of ``left_cropped_cutoff`` and ``right_truncated_cutoff`` parameters to :py:meth:`LabelCroppedPathsParams <retentioneering.data_processors_lib.label_cropped_paths.LabelCroppedPathsParams>` and set up a :py:meth:`LabelCroppedPaths <retentioneering.data_processors_lib.label_cropped_paths.LabelCroppedPaths>` node.

.. code-block:: python

    from retentioneering.data_processors_lib import LabelCroppedPaths, LabelCroppedPathsParams

    params = {
        "left_truncated_cutoff": (1, 'h'),
        "right_truncated_cutoff": (1, 'h'),
    }
    node6 = EventsNode(LabelCroppedPaths(params=LabelCroppedPathsParams(**params)))
    pgraph.add_node(node=node6, parents=[node5])

For ``node7`` we apply similar filtering technique as we used for filtering new/existing users above. The remove_truncated_paths() function implements this filter.


.. code-block:: python

    def remove_truncated_paths(df, schema):
        truncated_users = df[df[schema.event_name].isin(['cropped_left', 'cropped_right'])]\
            [schema.user_id]\
            .unique()
        return ~df[schema.user_id].isin(truncated_users)

    node7 = EventsNode(FilterEvents(params=FilterEventsParams(func=remove_truncated_paths)))
    pgraph.add_node(node=node7, parents=[node6])

Next, ``node8``. As we discussed earlier, :py:meth:`MergeNode <retentioneering.graph.nodes.MergeNode>` has two special features. Unlike ``EventsNode``, ``MergeNode`` is not associated with any data processor since it has a separate role -- concatenate the outputs of its parents. Another distinction from ``EventsNode`` is that the number of parents might be arbitrary (greater than 1). The following two lines of the code demonstrate both these features:

.. code-block:: python

    from retentioneering.graph.p_graph import MergeNode

    node8 = MergeNode()
    pgraph.add_node(node=node8, parents=[node4, node7])


Finally, for ``node9`` we wrap :py:meth:`SplitSessions <retentioneering.data_processors_lib.split_sessions.SplitSessions>` data processor to a node passing a single parameter ``session_cutoff`` and link it to the merging node:

.. code-block:: python

    from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams

    node9 = EventsNode(SplitSessions(params=SplitSessionsParams(session_cutoff=(30, 'm'))))
    pgraph.add_node(node=node9, parents=[node8])

.. _preprocessing_running_the_calculation:

Running the calculation
~~~~~~~~~~~~~~~~~~~~~~~

So we have built the graph, now it's time to run the entire calculation which the graph frames. In order to run the calculation from the graph root to a specific node, call :py:meth:`PGraph.combine() <retentioneering.graph.p_graph.PGraph.combine>` method with a single parameter ``node`` which accepts the corresponding node object. The result is represented as the :py:meth:`Eventstream <retentioneering.eventstream.eventstream.Eventstream>` class.

.. code-block:: python

    processed_stream = pgraph.combine(node=node9)
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

    You can combine the calculations at any node. In practice, it is useful for debugging the calculations.

Summary
~~~~~~~

Here we just provide the same code combined in a single chunk so you could simply copy and paste it and see the results.

.. code-block:: python

    import pandas as pd
    from retentioneering import datasets
    from retentioneering.data_processors_lib import AddStartEndEvents, AddStartEndEventsParams
    from retentioneering.data_processors_lib import GroupEvents, GroupEventsParams
    from retentioneering.data_processors_lib import LabelNewUsers, LabelNewUsersParams
    from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams
    from retentioneering.data_processors_lib import LabelCroppedPaths, LabelCroppedPathsParams
    from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams
    from retentioneering.graph.p_graph import PGraph, EventsNode, MergeNode

    stream = datasets.load_simple_shop()

    # node1
    node1 = EventsNode(AddStartEndEvents(params=AddStartEndEventsParams()))

    # node2
    def group_products(df, schema):
        return df[schema.event_name].isin(['product1', 'product2'])

    group_events_params={
        "event_name": "product",
        "func": group_products
    }
    node2 = EventsNode(GroupEvents(params=GroupEventsParams(**group_events_params)))

    # node3
    google_spreadsheet_id = '1iggpIT5CZcLILLZ94wCZPQv90tERwi1IB5Y1969C8zc'
    link = f'https://docs.google.com/spreadsheets/u/1/d/{google_spreadsheet_id}/export?format=csv&id={google_spreadsheet_id}'
    new_users = pd.read_csv(link, header=None)[0].tolist()
    node3 = EventsNode(LabelNewUsers(params=LabelNewUsersParams(new_users_list=new_users)))

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
    node6 = EventsNode(LabelCroppedPaths(params=LabelCroppedPathsParams(**params)))

    # node7, node8, node9
    def remove_truncated_paths(df, schema):
        truncated_users = df[df[schema.event_name].isin(['cropped_left', 'cropped_right'])]\
            [schema.user_id]\
            .unique()
        return ~df[schema.user_id].isin(truncated_users)

    node7 = EventsNode(FilterEvents(params=FilterEventsParams(func=remove_truncated_paths)))
    node8 = MergeNode()
    node9 = EventsNode(SplitSessions(params=SplitSessionsParams(session_cutoff=(30, 'm'))))

    # linking the nodes to get the graph
    pgraph = PGraph(stream)
    pgraph.add_node(node=node1, parents=[pgraph.root])
    pgraph.add_node(node=node2, parents=[node1])
    pgraph.add_node(node=node3, parents=[node2])
    pgraph.add_node(node=node4, parents=[node3])
    pgraph.add_node(node=node5, parents=[node3])
    pgraph.add_node(node=node6, parents=[node5])
    pgraph.add_node(node=node7, parents=[node6])
    pgraph.add_node(node=node8, parents=[node4, node7])
    pgraph.add_node(node=node9, parents=[node8])

    # getting the calculation results
    processed_stream = pgraph.combine(node=node9)
    processed_stream.to_dataframe().head()

.. _preprocessing_chain_usage_complex_example:

Method chaining for preprocessing graph
---------------------------------------

In the previous sections we have constructed complex example. Let us consider one more way of preprocessing graph usage. It is based on :ref:`method chaining<helpers_and_chain_usage>` approach and could be easily applied if there is no need branching and merging logic. In the end we will illustrate the result with :doc:`TransitionGraph<transition_graph>` visualization.

We are going to use the same :doc:`simple_shop dataset <../datasets/simple_shop>` dataset. If we try to use TransitionGraph without applying data processors, we can get results that are difficult to analyze:

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


By using the transition graph interactive options, we could focus on specific event transitions. However, even the general user workflow can be difficult to see - because of many ungrouped events, loops, and states.

We can address this problem by using a combination of data processors we have seen previously. One example of a processing graph would look like this:

-  apply :py:meth:`DropPaths <retentioneering.data_processors_lib.drop_paths.DropPaths>` to remove users that could have appeared by accident;
-  apply :py:meth:`AddStartEndEvents <retentioneering.data_processors_lib.add_start_end_events.AddStartEndEvents>` to mark the start and finish user states;
-  apply :py:meth:`SplitSessions <retentioneering.data_processors_lib.split_sessions.SplitSessions>` to mark user sessions;
-  apply :py:meth:`GroupEvents <retentioneering.data_processors_lib.group_events.GroupEvents>` multiple times to group similar events into groups;
-  apply :py:meth:`CollapseLoops <retentioneering.data_processors_lib.collapse_loops.CollapseLoops>` with different parameters for different loop representations on the transition graph plot.

.. figure:: /_static/user_guides/preprocessing/preprocessing_pgraph_chain.png

As the result, we should get three similar eventstreams that differ only in their way of encoding loops. That is the main inherent advantage of using the graph structure for transformations. We only need to execute all common data processors once, and then we can quickly alternate between different "heads" of the transformation.

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

    stream_7_nodes = stream\
        .drop_paths(events_num=6)\
        .add_start_end_events()\
        .split_sessions(session_cutoff=(30, 'm'))\
        .group_events(event_name='browsing', func=group_browsing)\
        .group_events(event_name='delivery', func=group_delivery)\
        .group_events(event_name='payment', func=group_payment)

Looking at the simplest version, where loops are replaced with the event they consist of:

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

..
    TODO: It's better to rebuild the transition graph with edges_norm_type='node'.

This transition graph is much more comprehensible. After applying the data processors, we can see that:

- All sessions start with a ``browsing`` event. And more than 30% of transitions from ``browsing`` lead to the end of the session.
- There many returning sessions - 2459 transitions lead to further sessions.
- After transitioning from "cart" to "delivery", about 30% of transitions do not proceed to "payment".

We can also see the general user flow quite clearly now, which is a sufficient improvement compared to the original plot.

To learn more about loops and where they occur, let us plot two other versions of the eventstream:

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


In this plot (which is a bit more convoluted than the previous one), we see that loops mostly occur when users are browsing, and are less frequent at the ``delivery`` or ``payment stages``. However, there are a lot more transitions to ``payment_loop`` or ``delivery_loop`` than there are to ``payment`` or ``delivery``. This could suggest that there is a problem with the delivery/payment process, or that we could improve the process by reducing the number of transitions (i.e. "clicks") it takes to make an order a delivery or to pay.

Now we can attempt to look at the typical loop length using the third created eventstream:

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

This plot is a bit more complex than the previous two; to properly analyze it, we would need to filter out some loop events based on their frequency. Still, we can see that the longest loops occur at the browsing stage - and cart, payment, or delivery loops are limited by 2-3 steps, meaning that the problem we found might not be as critical as it first appeared.
