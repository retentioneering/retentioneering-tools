Transition Graph
================

The following user guide is also available as `Google Colab notebook <https://colab.research.google.com/drive/14HJDyqV5D6gUYeqBvNfYCxcXe8xoJJLF?usp=share_link>`_.

Loading data
------------

Throughout this guide we use our demonstration :doc:`simple_shop </datasets/simple_shop>` dataset. It has already been converted to :doc:`Eventstream<eventstream>` and assigned to ``stream`` variable.

.. code-block:: python

    from retentioneering import datasets

    stream = datasets.load_simple_shop()

Basic example
-------------

The transition graph is a weighted directed graph which illustrates how often the users from an eventstream move from one event to another. The nodes stand for the unique events. A pair of nodes (say, ``A`` and ``B``) is connected with a directed edge if the transition ``A -> B`` appeared at least once in the eventstream. The edge weights stand for the transition frequency which might be calculated in multiple ways (see :ref:`Edge weights section <transition_graph_edge_weights>`).

The primary way to build a transition graph is to call :py:meth:`Eventstream.transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>` method:

.. code-block:: python

    stream.transition_graph()

.. raw:: html

    <div style="overflow:auto;">
    <iframe
        width="650"
        height="650"
        src="../_static/user_guides/transition_graph/basic_example.html"
        frameborder="0"
        allowfullscreen
    ></iframe>
    </div>

According to the transition graph definition, we see here the events represented as nodes. By default, the edge weights are calculated as the total number of the corresponding transitions occurred in the eventstream. All the edges are labeled with these numbers in the graph. For example, among the others we can see that there were 1709 ``catalog -> cart`` transitions, 1042 ``main -> main`` self-transitions, and there were no ``product1 -> payment_done`` transitions. The thickness of the edges is proportional to their weight values, while the size of the nodes is proportional to their frequencies (the total number of the event occurrences in the eventstream).

The graph is interactive. You can move the nodes, zoom in/out the chart, and finally reveal or hide a :ref:`control panel <transition_graph_control_panel>` by clicking on the left edge of the chart. You can check the interactive features out even in the transition graphs illustrating this document.

Transition graph parameters
---------------------------

.. _transition_graph_weights:

Weights
~~~~~~~

.. _transition_graph_edge_weights:

Edge weights
^^^^^^^^^^^^

The edge weight values are controlled by ``norm_type`` and ``weights`` parameters. However, ``weights`` parameter is a bit complex for usage, so in this section we'll refer to a fake ``weight_col`` parameter as an alias for ``weights={'edges': weight_col}`` pattern.
:red:`TODO: align it with the final version of the argument name in the code`

As we mentioned earlier, the most straightforward way to assign the edge weights is to calculate the number of the transition in the entire eventstream. In this case we need to use ``norm_type=None``, i.e. no normalization is needed. By weight normalization we mean dividing the transition counts (calculated for ``norm_type=None`` case) by some denominator, so we get rational weights instead of integer. ``norm_type='full'`` defines the denominator as the total number of the transitions. ``norm_type='node'`` works as follows. Consider a hypothetical ``A -> B`` transition. To normalize the weight of this edge we need to divide the number of ``A -> B`` transitions by the total number of the transitions coming out of ``A`` node.

In many cases it is reasonable to count the number of unique users or sessions instead of the number of transitions. This behavior is controlled by ``weight_col`` parameter. By default, ``weight_col=None`` is associated with the number of the transitions as it was described in the previous paragraph. You can also pass the names of the columns related to users or sessions (typically they are ``user_id`` and ``session_id``; see :ref:`here <eventstream_field_names>` and :py:meth:`here <retentioneering.data_processors_lib.split_sessions.SplitSessions>`) in the eventstream.

In order to check whether you understand these definitions correctly, let us consider a simplified example and look into the matter of the edge weights calculation. Suppose we have the following eventstream:

.. _transition_graph_calculation_example:

.. raw:: html

    user1: <font color='red'>A</font>, <font color='red'>B</font>, <font color='SlateBlue'>A</font>, <font color='SlateBlue'>C</font>, <font color='green'>A</font>, <font color='green'>B</font><br>
    user2: <font color='magenta'>A</font>, <font color='magenta'>B</font>, <font color='orange'>C</font>, <font color='orange'>C</font>, <font color='orange'>C</font><br>
    user3: <font color='DarkTurquoise'>C</font>, <font color='DarkTurquoise'>D</font>, <font color='DarkTurquoise'>C</font>, <font color='DarkTurquoise'>D</font>, <font color='DarkTurquoise'>C</font>, <font color='DarkTurquoise'>D</font><br><br>

This eventstream consists of 3 unique users and 4 unique events. The event colors denote sessions. We ignore the timestamps since the edge weights calculation does not take them into account.

The table |edge_weights_col_none| describes how the edge weights are calculated in case of ``weight_col=None``.

.. |edge_weights_col_none| replace:: Table 1

.. figure:: /_static/user_guides/transition_graph/weight_col_none.png

    Table 1. The calculation of the edge weights for weight_col=None and different normalization types.

So we have 8 edges in total. At first, we calculate for each edge the total number of such transitions occurred in the eventstream. As a result, we get the values in ``norm_type=None`` column. Next, we estimate the total number of the transitions in the eventstream: 14. To get the weights in ``norm_type='full'`` column, we divide the weights in ``norm_type=None`` column by 14. Finally, we estimate that we have 4, 2, 6, 1 transitions starting from event ``A``, ``B``, ``C``, and ``D`` correspondingly. Those are the denominators for ``norm_type='node'`` column. To calculate the weights for this option, we divide the values in ``norm_type=None`` by these denominators.

The calculation of edge weights for ``weight_col='user_id'`` is described in |edge_weights_col_user_id|.

.. |edge_weights_col_user_id| replace:: Table 2

.. figure:: /_static/user_guides/transition_graph/weight_col_user_id.png

    Table 2. The calculation of the edge weights for weight_col='user_id' and different normalization types.

Now, for ``norm_type=None`` option we calculate the number of unique users who had a specific transition. For ``norm_type='full'`` the denominator is 3 as the total number of users in the eventstream. As for ``norm_type='node'`` option, we have 2, 2, 3, 1 unique users who experienced events ``A``, ``B``, ``C``, ``D``. These values comprise the denominators. Again, to get the weights in ``norm_type='column'``, we divide the values from ``norm_type=None`` column by these corresponding denominators.

Finally, in |edge_weights_col_session_id| we demonstrate the calculations for ``weight_col='session_id'`` .

.. |edge_weights_col_session_id| replace:: Table 3

.. figure:: /_static/user_guides/transition_graph/weight_col_session_id.png

    Table 3. The calculation of the edge weights for weight_col='session_id' and different normalization types.

In comparison with the case for ``user_id`` weight column, there are some important differences. Transitions ``B → A``, ``C → A``, ``B → C`` are excluded since they are terminated by the session endings (their weights are zeros). As for the other transitions, we calculate the number of unique sessions they belong to. This is how we get ``norm_type=None`` column. The total number of the sessions in the eventstream is 6. This is the denominator for ``norm_type='full'`` column. The denominators for ``norm_type='node'`` column are calculated as the number of the unique sessions with ``A``, ``B``, ``C``, and ``D`` events. They are 4, 0, 2, and 1 correspondingly. Note that for ``B → A`` and ``B → C`` edges we have indeterminate form 0/0, since we have excluded all the transitions starting from ``B``. We define the corresponding weights as 0. Also, the denominator for ``C → *`` edges is 2, not 3 since we have excluded one ``C → A`` transition.

Node weights
^^^^^^^^^^^^

Besides edge weights, a transition graph also have node weights that control the diameters of the nodes. Obviously, node weights do not support ``norm_type='node'`` since it involves edges by design. However, ``norm_type=None`` and ``norm_type='full'`` options are available and they leverage the same calculation logic as we used for the edge weights calculation. We explain this logic using the same :ref:`example eventstream <transition_graph_calculation_example>`.

So for ``norm_type=None`` option the node weights are simply the counters of the events over the entire eventstream (in case of ``weight_col=None``) or the number of unique users or sessions (in case of ``weight_col='user_id'`` or ``weight_col='session_id'``) that had a specific event. For ``norm_type='full'`` we divide the non-normalized weights by either the overall number of events (17), or the number of unique users (3), or the number of unique sessions (6). See the calculations for each of the described cases in |node_weights_col_none|, |node_weights_col_user_id|, and |node_weights_col_session_id| below:

.. |node_weights_col_none| replace:: Table 4

.. figure:: /_static/user_guides/transition_graph/node_weight_col_none.png
    :width: 450

    Table 4. The calculation of the node weights for weight_col=None and different normalization types.


.. |node_weights_col_user_id| replace:: Table 5

.. figure:: /_static/user_guides/transition_graph/node_weight_col_user_id.png
    :width: 450

    Table 5. The calculation of the node weights for weight_col='user_id' and different normalization types.


.. |node_weights_col_session_id| replace:: Table 6

.. figure:: /_static/user_guides/transition_graph/node_weight_col_session_id.png
    :width: 450

    Table 6. The calculation of the node weights for weight_col='session_id' and different normalization types.

.. _transition_graph_setting_the_weights:

Setting the weight options
^^^^^^^^^^^^^^^^^^^^^^^^^^

Finally we demonstrate how to set weighting options for a graph. As it has been discussed, ``norm_type`` argument accepts ``None``, ``full`` or ``node`` values. As for weighting col, it is set separately for nodes and for values via ``weights`` dictionary. Its ``nodes`` key stands for nodes weighting column, and ``edges`` key stands for edges weighting column.

.. code-block:: python

    stream.transition_graph(
        norm_type='node',
        weights={'nodes': 'events', 'edges': 'user_id'}
    )

:red:`TODO: provide actual code and graph, and rewrite the paragraph`.

.. _transition_graph_thresholds:

Thresholds
~~~~~~~~~~

The weights that we have discussed above are associated with importance of the edges and the nodes. In practice, a transition graph often contains enormous number of the nodes and the edges. The threshold mechanism sets the minimal weight for nodes and edges to be displayed in the canvas.

Note that the thresholds may use their own weighting columns both for nodes and for edges independently of those weighting columns defined in ``weights`` target. So the weights displayed on a graph might be different from the weights that thresholds use in making their decision for hiding the nodes/edges. Moreover, multiple weighting columns might be used. In this case, the decision whether an item (a node or an edge) should be hidden is made applying logical AND: an item is hidden if it does not meet all the threshold conditions.

:red:`TODO: provide a code example with the correct threshold parameter names`.

Targets
~~~~~~~

As we have already mentioned, nodes are often of a different importance. Sometimes we need not just to hide unimportant nodes, but to highlight important nodes instead. Transition graph identifies three types of the nodes: positive, negative, and sourcing. Three colors correspond to these node types: green, ren and orange correspondingly. You can color the nodes with these colors by defining their types in ``targets`` parameter:

.. code-block:: python

    stream\
        .add_start_end()\
        .transition_graph(
            targets={'payment_done': 'nice', 'path_end': 'bad', 'path_start': 'source'}
        )

:red:`TODO: Add html, replace the code with the actual.`

In the example above we additionally use :py:meth:`add_start_end()<retentioneering.eventstream.eventstream.Eventstream.add_start_end>` data processor helper to add ``path_start`` and ``path_end`` events.

.. _transition_graph_control_panel:

Control panel
-------------

The control panel is a visual interface allows you to interactively control the transition grap behavior. It also allows to even control the underlying eventstream in some scenarios (grouping events, including/excluding events).

is hidden on the left side of the transition graph interface. By default, it is hidden. To reveal it move your mouse to the left edge of the graph canvas and click it.

.. figure:: /_static/user_guides/transition_graph/control_panel_01_reveal_the_control_panel.png
    :width: 800

    How to reveal hidden control panel.

The control panel consists of 5 blocks: Weights, Nodes, Thresholds, Export, and Settings. By default, all these blocks are expanded. You can collapse them by clicking minus sign located at the top right corner of each block.

.. |collapse_blocks| image:: /_static/user_guides/transition_graph/control_panel_02_collapse_blocks.png
    :height: 600

.. |collapsed_blocks| image:: /_static/user_guides/transition_graph/control_panel_03_collapsed_blocks.png
    :height: 600

The control panel consists of 5 blocks: Weights, Nodes, Thresholds, Export, and Settings. By default, all these blocks are expanded. You can collapse them by clicking minus sign located at the top right corner of each block.

.. table:: Blocks collapse & expansion.

    +----------------------------------------------+-------------------------------------------+
    | |collapse_blocks|                            | |collapsed_blocks|                        |
    +==============================================+===========================================+
    | Click the minus sign to collapse the blocks. | Click the plus sign to expand the blocks. |
    +----------------------------------------------+-------------------------------------------+

Weights
~~~~~~~

Weights block contains selectors for choosing weighting columns separately for nodes and edges. Unfortunately, so far you can not choose normalization type in this interface. The only way to set the normalization type is using ``norm_type`` argument in :py:meth:`Eventstream.transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>` method as it has been shown :ref:`here <transition_graph_setting_the_weights>`.

:red:`Check actual name for norm_type`

Nodes
~~~~~

Nodes block enumerates all the unique events represented in the transition graph and allows to perform such operations as grouping, deleting, and renaming events.

Node item actions
^^^^^^^^^^^^^^^^^

Each node list item contains the following 5 elements:

.. figure:: /_static/user_guides/transition_graph/control_panel_06_nodes_item.png
    :width: 300

    The elements of the node list.

1. Focus icon. If you click it, the graph changes its position in the canvas so the selected node is placed in the center.
2. Event name.
3. The number of the event occurrences in the eventstream.
4. This switcher removes the event from the eventstream. Recalculation is required.
5. This switcher hides the node and all the edges connected to the node from the canvas. In contrast with the removing switcher, the node is literally hidden, so no recalculation is required.

You can also rename a node. Just double click its name in the list and enter a new one.

.. note::

    By recalculation we mean that some additional calculations are required in order to display the graph state according to the selected options. Click |warning| icon to recalculate the graph values. Sometimes it is reasonable to do multiple modifications in the control panel, and then call the recalculation at once.

.. note::

    All the grouping and deleting actions do not affect the initial eventstream due to eventstream immutability property. See for the details. :red:`Set a precise link to a section of the eventstream concept document`.


Grouping events
^^^^^^^^^^^^^^^

Control panel interface supports easy and intuitive event grouping. Suppose you want to group ``product1`` and ``product2``. There are two ways to do this:

1. Drag & drop method. Drag one node (say, ``product2``) and drop it to ``product1`` node. ``product1_group`` event appears containing the couple of events ``product1`` and ``product2``. Grouping node has a folder icon that triggers aggregation action. Once you click it, the grouped nodes are merged in the transition graph.

2. Add group method. Click "+ Add group" button, ``untitled_group`` appears. Drag & drop all the nodes to be grouped to this group and click folder icon to apply the display the grouping in the graph visualization.

.. |grouping_1| image:: /_static/user_guides/transition_graph/control_panel_07_nodes_grouping.png

.. |grouping_2| image:: /_static/user_guides/transition_graph/control_panel_08_nodes_grouping_2.png

.. table:: A grouping node. The folder icon triggers merging action.

    +------------------------------------------+----------------------------------------+
    | |grouping_1|                             | |grouping_2|                           |
    +==========================================+========================================+
    | Grouping nodes using drag & drop method. | Grouping nodes using add group method. |
    +------------------------------------------+----------------------------------------+

To rename a grouping node, double click its name and enter a new one. To ungroup the grouped nodes drag & drop the nodes out of the grouping node (or drop it right on the grouping node). As soon as the last event is out, the grouping node disappears.

Also, note that grouping actions require graph recalculation.

Thresholds
~~~~~~~~~~

Thresholds block contains two sliders: one is associated with the nodes, another one with the edges. You can set up a threshold value either by moving a slider or by entering a value explicitly. Also, you can set up a weighting column for each slider independently of the weighting column defined in Weights block (we have already mentioned it :ref:`here <transition_graph_thresholds>`. Using multiple weighting columns per one slider is also supported. As soon as you select a weighting column in the dropdown menu, the threshold slider connects to it, but the threshold values set for the previous weighting column are still kept.

:red:`TODO: insert actual code & html`.

Export
~~~~~~

Graph export supports two formats: HTML and JSON. HTML format is useful when you want to embed the resulting graph into different environments and reports. Such a file supports all the interactive actions as if you treated the graph in Jupyter environment. For example, the graphs that are embedded in this document are embedded right in this way. JSON format might useful when you need to get the nodes coordinates.

Settings
~~~~~~~~

The following displaying options are available:

- Show weights. Hide/display the edge weights.
- Show node names. Hide/display the node names.
- Show all edges for targets. By default, threshold filters hide the edges disregarding the node types. In case you have defined target nodes, you usually want to carefully analyze them, and all the edges connected to these nodes are important. This displaying option allows to ignore the threshold filters and always display any edge connected to target nodes.
- Show nodes without links. Setting a threshold filter might remove all the edges connected to a node. Such isolated nodes might be considered as useless. This displaying option hides them in the canvas.
- Show edge info on hover. By default, a tooltip with an edge info pops up when you mouse over the edge. It might be disturbing for large graphs, so this option suppresses the tooltips.

Graph properties
~~~~~~~~~~~~~~~~

A summary with all the important chosen graph settings is available by clicking ⓘ icon in the bottom right corner.

:red:`TODO: insert an actual screenshot`

Transition matrix
-----------------

Transition matrix is a sub-part of transition graph. It contains edge weights only so that the weight of, say, ``A → B`` transition is located at ``A`` row and ``B`` column of the transition matrix. The calculation logic is exactly the same as we have described :ref:`here <transition_graph_edge_weights>`, and the arguments are similar to :ref:`weights-related arguments <transition_graph_setting_the_weights>` of transition graph (use ``weight_col`` instead of ``weights``).

.. code-block:: python

    stream.transition_matrix(norm_type='node', weight_col='user_id')

:red:`TODO: replace with the actual parameters.`

Using a separate instance
-------------------------

Common tooling properties
-------------------------
