Transition Graph
================

|colab| |jupyter|


.. |jupyter| raw:: html

    <a href="../_static/user_guides_notebooks/transition_graph.ipynb">
    <img src="https://img.shields.io/static/v1?label=Download&message=Jupyter+Notebook&color=%23F37626&logo=jupyter&logoColor=%23F37626"
        alt="Download - Jupyter Notebook">
    </a>

.. |colab| raw:: html

    <a href="https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/docs/source/_static/user_guides_notebooks/transition_graph.ipynb" target="_blank">
      <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Google Colab"/>
    </a>

Loading data
------------

Throughout this guide we use our demonstration :doc:`simple_shop </datasets/simple_shop>` dataset. It has already been converted to :doc:`Eventstream<eventstream>` and assigned to ``stream`` variable. If you want to use your own dataset, upload it following :ref:`this instruction<eventstream_creation>`.

.. code-block:: python

    from retentioneering import datasets

    stream = datasets.load_simple_shop()

A basic example
---------------

The transition graph is a weighted directed graph that illustrates how often the users from an eventstream move from one event to another. The nodes stand for the unique events. A pair of nodes (say, ``A`` and ``B``) is connected with a directed edge if the transition ``A → B`` appeared at least once in the eventstream. Transition means that event ``B`` appeared in a user path right after event ``B``. For example, in path ``A, C, B`` there is no transition ``A → B`` since event ``C`` stands between ``A`` and ``B``.

Each node and edge is associated with its weight. Roughly speaking, the weights are the numbers that reflect a node or an edge frequency. They might be calculated in multiple ways (see :ref:`Weights section <transition_graph_weights>`).

The primary way to build a transition graph is to call :py:meth:`Eventstream.transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>` method:

.. code-block:: python

    stream.transition_graph()

.. raw:: html

    <div style="overflow:auto;">
    <iframe
        width="670"
        height="630"
        src="../_static/user_guides/transition_graph/basic_example.html"
        frameborder="0"
        allowfullscreen
    ></iframe>
    </div>

According to the transition graph definition, we see here the events represented as nodes connected with the edges. By default, the nodes and edges weights are the number of unique users who experienced the corresponding event or transition. All the edges are labeled with these numbers in the graph. For example, among the others, we can see that there are 1324 unique users who had ``catalog → cart`` transitions, 603 users with ``main → main`` self-transitions, and there were none with ``product1 → payment_done`` transitions. The thickness of the edges and the size of the nodes are proportional to their weights.

The graph is interactive. You can move the nodes, zoom in/out the chart, and finally reveal or hide a :ref:`control panel <transition_graph_control_panel>` by clicking on the left edge of the chart. You can check the interactive features out even in the transition graphs embedded in this document.

.. _transition_graph_python_params:

Transition graph parameters
---------------------------

.. _transition_graph_weights:

Weights
~~~~~~~

.. _transition_graph_edge_weights:

Edge weights calculation
^^^^^^^^^^^^^^^^^^^^^^^^

The edge weight values are controlled by ``edges_norm_type`` and ``edges_weight_col`` parameters of :py:meth:`Eventstream.transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>` method.

Let us start from the explanation of the configuration ``edges_norm_type=None`` and ``edges_weight_col='event_id'`` which means that no normalization is needed and ``event_id`` column is used as a weighting column (we will explain the concept of weighting columns below). This combination defines edge weight as the number of the transitions associated with the edge in the entire eventstream.

By weight normalization we mean dividing the transition counts (calculated for ``edges_norm_type=None`` case) by some denominator, so we get rational weights instead of integer. Except ``None``, two normalization types are possible: ``full`` and ``node``. Full normalization defines the denominator as the overall number of the transitions in the eventstream. Node normalization works as follows. Consider a hypothetical ``A → B`` transition. To normalize the weight of this edge we need to divide the number of ``A → B`` transitions by the total number of the transitions coming out of ``A`` node. In other words, node-normalized weight is essentially the probability of a user to transit to event ``B`` standing on event ``A``.

Now, let us move to weighting column definition. In many cases it is reasonable to count the number of unique users or sessions instead of the number of transitions. This behavior is controlled by ``edges_norm_type`` parameter. By default, ``edges_weight_col='event_id'`` that is associated with the number of the transitions. You can also pass the names of the columns related to users or sessions in the eventstream. Typically they are ``user_id`` and ``session_id``, but to be sure, check your :ref:`eventstream data schema <eventstream_field_names>` and ``session_col`` parameter in the :py:meth:`SplitSessions data processor<retentioneering.data_processors_lib.split_sessions.SplitSessions>` if you used it.

Having ``edges_weight_col`` defined allows you to calculate the weighs as the unique values represented in ``edges_weight_col`` column. This also relates to ``full`` and ``node`` normalization types. For example, ``edges_norm_type='full'`` and ``edges_weight_col='user_id'`` configuration means that we divide the number of the unique users who had a specific transition by the number of the unique users in the entire eventstream.

.. _transition_graph_calculation_example:

A simplified example
^^^^^^^^^^^^^^^^^^^^

In order to check whether you understand these definitions correctly, let us consider a simplified example and look into the matter of the edge weights calculation. Suppose we have the following eventstream:

.. raw:: html

    user1: <font color='red'>A</font>, <font color='red'>B</font>, <font color='SlateBlue'>A</font>, <font color='SlateBlue'>C</font>, <font color='green'>A</font>, <font color='green'>B</font><br>
    user2: <font color='magenta'>A</font>, <font color='magenta'>B</font>, <font color='orange'>C</font>, <font color='orange'>C</font>, <font color='orange'>C</font><br>
    user3: <font color='DarkTurquoise'>C</font>, <font color='DarkTurquoise'>D</font>, <font color='DarkTurquoise'>C</font>, <font color='DarkTurquoise'>D</font>, <font color='DarkTurquoise'>C</font>, <font color='DarkTurquoise'>D</font><br><br>

This eventstream consists of 3 unique users and 4 unique events. The event colors denote sessions (there are 6 sessions). We ignore the timestamps since the edge weights calculation does not take them into account. Note that throughout this example we will suppress ``edge_`` prefix for the ``edges_norm_type`` and ``edges_weight_col``.

|edge_weights_col_none| describes how the edge weights are calculated in case of ``weight_col='event_id'``.

.. |edge_weights_col_none| replace:: Table 1

.. figure:: /_static/user_guides/transition_graph/weight_col_none.png

    Table 1. The calculation of the edge weights for weight_col='event_id' and different normalization types.

So we have 8 unique edges in total. At first, we calculate for each edge the total number of such transitions occurred in the eventstream. As a result, we get the values in ``norm_type=None`` column. Next, we estimate the total number of the transitions in the eventstream: 14. To get the weights in ``norm_type='full'`` column, we divide the weights in ``norm_type=None`` column by 14. Finally, we estimate that we have 4, 2, 6, 1 transitions starting from event ``A``, ``B``, ``C``, and ``D`` correspondingly. Those are the denominators for ``norm_type='node'`` column. To calculate the weights for this option, we divide the values in ``norm_type=None`` by these denominators.

The calculation of the edge weights for ``weight_col='user_id'`` is described in |edge_weights_col_user_id|.

.. |edge_weights_col_user_id| replace:: Table 2

.. figure:: /_static/user_guides/transition_graph/weight_col_user_id.png

    Table 2. The calculation of the edge weights for weight_col='user_id' and different normalization types.

Now, for ``norm_type=None`` option we calculate the number of unique users who had a specific transition. For ``norm_type='full'`` the denominator is 3 as the total number of users in the eventstream. As for ``norm_type='node'`` option, we have 2, 2, 3, 1 unique users who experienced ``A → *``, ``B → *``, ``C → *``, ``D → *`` transitions. These values comprise the denominators. Again, to get the weights in ``norm_type='column'``, we divide the values from ``norm_type=None`` column by these corresponding denominators.

Finally, in |edge_weights_col_session_id| we demonstrate the calculations for ``weight_col='session_id'`` .

.. |edge_weights_col_session_id| replace:: Table 3

.. figure:: /_static/user_guides/transition_graph/weight_col_session_id.png

    Table 3. The calculation of the edge weights for weight_col='session_id' and different normalization types.

In comparison with the case for ``user_id`` weight column, there are some important differences. Transitions ``B → A``, ``C → A``, ``B → C`` are excluded since they are terminated by the session endings (their weights are zeros). As for the other transitions, we calculate the number of unique sessions they belong to. This is how we get ``norm_type=None`` column. The total number of the sessions in the eventstream is 6. This is the denominator for ``norm_type='full'`` column. The denominators for ``norm_type='node'`` column are calculated as the number of the unique sessions with ``A → *``, ``B → *``, ``C → *``, and ``D → *`` transitions. They are 4, 0, 2, and 1 correspondingly. Note that for ``B → A`` and ``B → C`` edges we have indeterminate form 0/0, since we have excluded all the transitions starting from ``B``. We define the corresponding weights as 0. Also, the denominator for ``C → *`` edges is 2, not 3 since we have excluded one ``C → A`` transition.

Node weights
^^^^^^^^^^^^

Besides edge weights, a transition graph also have node weights that control the diameters of the nodes. Unfortunately, so far only one option is supported: ``norm_type=None`` along with weighting columns. By default, ``weight_col='user_id'``.

If you want to know how the node weights for ``norm_type='full'`` are calculated, expand the following text snippet:

.. toggle::

    Obviously, node weights do not support ``norm_type='node'`` since it involves edges by design. However, ``node_norm_type=None`` and ``norm_type='full'`` options might be calculated. They leverage the same calculation logic as we used for the edge weights calculation.

    We explain this logic using the same :ref:`example eventstream <transition_graph_calculation_example>`.

    So for ``norm_type=None`` option the node weights are simply the counters of the events over the entire eventstream (in case of ``weight_col='event_id'``) or the number of unique users or sessions (in case of ``weight_col='user_id'`` or ``weight_col='session_id'``) that had a specific event. For ``norm_type='full'`` we divide the non-normalized weights by either the overall number of events (17), or the number of unique users (3), or the number of unique sessions (6). See the calculations for each of the described cases in |node_weights_col_none|, |node_weights_col_user_id|, and |node_weights_col_session_id| below:

    .. |node_weights_col_none| replace:: Table 4

    .. figure:: /_static/user_guides/transition_graph/node_weight_col_none.png
        :width: 450

        Table 4. The calculation of the node weights for weight_col='event_id' and different normalization types.


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

Finally, we demonstrate how to set the weighting options for a graph. As it has been discussed, ``edges_norm_type`` argument accepts ``None``, ``full`` or ``node`` values. A weighting column is set by ``edges_weight_col`` argument. Below is a table that summarizes the definitions of edge weights when these two arguments are used jointly.

.. table:: The definitions of edge weights for different combinations of normalization type and weighting columns. ``A → B`` is considered as an edge example.
    :widths: 21 20 25 35
    :class: tight-table

    +----------------------------------------+-------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | edges_norm_type → \ edges_weight_col ↓ | None                                                                          | full                                                                                                                        | node                                                                                                                                                                              |
    +========================================+===============================================================================+=============================================================================================================================+===================================================================================================================================================================================+
    | event_id                               | The total number of the ``A → B`` **transitions**.                            | The total number of the ``A → B`` transitions divided by the number of all the **transitions**.                             | The total number of the ``A → B`` transitions divided by the **total number of** ``A → *`` **transitions**.                                                                       |
    +----------------------------------------+-------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | None or user_id                        | The total number of the **unique users** who had the ``A → B`` transition.    | The total number of the **unique users** who had the ``A → B`` transition divided by the number of all the **users**.       | The total number of the **unique users** who had the ``A → B`` transition divided by the number of the **unique users who had any** ``A → *`` **transition**.                     |
    +----------------------------------------+-------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | session_id                             | The total number of the **unique sessions** who had the ``A → B`` transition. | The total number of the **unique sessions** who had the ``A → B`` transition divided by the number of all the **sessions**. | The total number of the **unique sessions** where the ``A → B`` transition occurred divided by the number of the **unique sessions where any** ``A → *`` **transition occurred**. |
    +----------------------------------------+-------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Here is an example of the using these arguments:

.. code-block:: python

    stream.transition_graph(
        edges_norm_type='node',
        edges_weight_col='user_id'
    )

.. raw:: html

    <div style="overflow:auto;">
    <iframe
        width="670"
        height="630"
        src="../_static/user_guides/transition_graph/weights.html"
        frameborder="0"
        allowfullscreen
    ></iframe>
    </div>

From this graph we see, for example, that being at ``product1`` event, 62.3% of the users transit to ``catalog`` event, 43.3% - to ``cart`` event, and 11.4% - to ``main`` event. As you can notice, when you use some normalization, the values are not necessarily sum up to 1. This happens because a user can be at ``product1`` state multiple times, so they can jump to multiple of these three events.

.. _transition_graph_thresholds:

Thresholds
~~~~~~~~~~

The weights that we have discussed above are associated with importance of the edges and the nodes. In practice, a transition graph often contains enormous number of the nodes and the edges. The threshold mechanism sets the minimal weights for nodes and edges to be displayed in the canvas.

Note that the thresholds may use their own weighting columns both for nodes and for edges independently of those weighting columns defined in ``edges_weight_col`` arguments. So the weights displayed on a graph might be different from the weights that the thresholds use in making their decision for hiding the nodes/edges. Moreover, multiple weighting columns might be used. In this case, the decision whether an item (a node or an edge) should be hidden is made applying logical OR: an item is hidden if it does not meet any threshold condition.

Also note that, by default, if all the edges connected to a node are hidden, the node becomes hidden as well. You can turn this option off :ref:`here <transition_graph_settings>`.

The thresholds are set with a couple of ``nodes_threshold``, ``edges_threshold`` parameters. Each parameter is a dictionary. The keys are weighting column names, the values are the threshold values.

.. code-block:: python

    stream.transition_graph(
        edges_norm_type='node',
        edges_weight_col='user_id',
        edges_threshold={'user_id': 0.12},
        nodes_threshold={'event_id': 500}
    )

.. raw:: html

    <div style="overflow:auto;">
    <iframe
        width="670"
        height="630"
        src="../_static/user_guides/transition_graph/thresholds.html"
        frameborder="0"
        allowfullscreen
    ></iframe>
    </div>

This example is an extension of the previous one. We use the same normalization configuration as before. Since we have added an edges threshold of ``0.12`` for ``user_id`` weighting column, the edge ``product1`` → ``main`` that we observed in the previous example is hidden now (its weight is 11.4%). As for the nodes threshold, note that event ``payment_cash`` is hidden now (as we can see from the Nodes block in the Control panel, its weight is 197).

.. _transition_graph_targets:

Targets
~~~~~~~

As we have already mentioned, the graph nodes are often of different importance. Sometimes we need not just to hide unimportant nodes, but to highlight important nodes instead. Transition graph identifies three types of the nodes: positive, negative, and sourcing. Three colors relate to these node types: green, ren and orange correspondingly. You can color the nodes with these colors by defining their types in the ``targets`` parameter:

.. code-block:: python

    stream\
        .add_start_end_events()\
        .transition_graph(
            targets={
                'positive': ['payment_done', 'cart'],
                'negative': 'path_end',
                'source': 'path_start'
            }
        )

.. raw:: html

    <div style="overflow:auto;">
    <iframe
        width="670"
        height="630"
        src="../_static/user_guides/transition_graph/targets.html"
        frameborder="0"
        allowfullscreen
    ></iframe>
    </div>

In the example above we additionally use :py:meth:`Eventstream.add_start_end_events() <retentioneering.eventstream.helpers.add_start_end_events_helper.AddStartEndEventsHelperMixin.add_start_end_events>` data processor helper to add ``path_start`` and ``path_end`` events.

.. _transition_graph_settings:

Graph settings
~~~~~~~~~~~~~~

You can set up the following boolean flags:

- ``show_weights``. Hide/display the edge weight labels. Default value is True.
- ``show_percents``. Display edge weights as percents. Available only if an edge normalization type is chosen. Default value is False.
- ``show_nodes_names``. Hide/display the node names. Default value is True.
- ``show_all_edges_for_targets``. By default, the threshold filters hide the edges disregarding the node types. In case you have defined target nodes, you usually want to carefully analyze them. Hence, all the edges connected to these nodes are important. This displaying option allows to ignore the threshold filters and always display any edge connected to a target node. Default value is True.
- ``show_nodes_without_links``. Setting a threshold filter might remove all the edges connected to a node. Such isolated nodes might be considered as useless. This displaying option hides them in the canvas as well. Default value is True.
- ``show_edge_info_on_hover``. By default, a tooltip with an edge info pops up when you mouse over the edge. It might be disturbing for large graphs, so this option suppresses the tooltips. Default value is False.

These flags could be specified as separate arguments as follows:

.. code-block:: python

    stream.transition_graph(
        edges_norm_type='node',
        show_weights=True,
        show_percents=True,
        show_nodes_names=True,
        show_all_edges_for_targets=False,
        show_nodes_without_links=False,
        show_edge_info_on_hover=True
    )

.. raw:: html

    <div style="overflow:auto;">
    <iframe
        width="670"
        height="630"
        src="../_static/user_guides/transition_graph/settings.html"
        frameborder="0"
        allowfullscreen
    ></iframe>
    </div>

.. _transition_graph_control_panel:

Control panel
-------------

The control panel is a visual interface allowing you to interactively control transition graph behavior. It also allows even to control the underlying eventstream in some scenarios (grouping events, renaming events, including/excluding events).

The control panel consists of 5 blocks: Weights, Nodes, Thresholds, Export, and Settings. By default, all these blocks are expanded. You can collapse them by clicking minus sign located at the top right corner of each block.

.. |collapse_blocks| image:: /_static/user_guides/transition_graph/control_panel_02_collapse_blocks.png
    :height: 600

.. |collapsed_blocks| image:: /_static/user_guides/transition_graph/control_panel_03_collapsed_blocks.png
    :height: 600

.. table:: Blocks collapse & expansion.

    +----------------------------------------------+-------------------------------------------+
    | |collapse_blocks|                            | |collapsed_blocks|                        |
    +==============================================+===========================================+
    | Click the minus sign to collapse the blocks. | Click the plus sign to expand the blocks. |
    +----------------------------------------------+-------------------------------------------+

.. warning::

    All the settings that are tweaked in the Control panel are available only in scope of the current transition graph displayed in the current Jupyter cell. As soon as you run :py:meth:`Eventstream.transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>` again, all the settings will be reset to the defaults unless you call the method with particular parameters.

.. _transition_graph_weights_GUI:

Weights block
~~~~~~~~~~~~~

The Weights block contains selectors that choose weighting columns separately for nodes and edges. Unfortunately, so far you can not choose normalization type in this interface. The only way to set the normalization type is using ``edge_norm_type`` argument in :py:meth:`Eventstream.transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>` method as it has been shown :ref:`here <transition_graph_setting_the_weights>`. ``event_id`` weighting column refers to ``edge_norm_type=None``.

For the nodes only ``event_id`` and ``user_id`` weighting columns are available. The same columns are available for the edges, but additionally the columns that are passed as the ``edges_weight_col`` and ``custom_weight_cols`` arguments of the :py:meth:`Eventstream.transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>` are also available.

.. figure:: /_static/user_guides/transition_graph/control_panel_04_weights.png
    :width: 250

    Weighting columns dropdown menu in the Weights block.

.. _transition_graph_nodes_block:

Nodes block
~~~~~~~~~~~

The Nodes block enumerates all the unique events represented in the transition graph and allows to perform such operations as grouping, renaming, and switching events.

.. figure:: /_static/user_guides/transition_graph/control_panel_05_nodes.png
    :width: 250

    The Nodes block.

.. note::

    Nodes switcher requires graph recalculation.


Node item actions
^^^^^^^^^^^^^^^^^

Each node list item contains the following 4 elements:

.. figure:: /_static/user_guides/transition_graph/control_panel_06_nodes_item.png
    :width: 250

    The elements of the node list.

1. Focus icon. If you click it, the graph changes its position in the canvas so the selected node is placed in the center.
2. Event name. Double click it if you want to rename the node.
3. The number of the event occurrences in the eventstream.
4. This switcher hides the node and all the edges connected to the node from the canvas.

Grouping events
^^^^^^^^^^^^^^^

The Control panel interface supports easy and intuitive event grouping. Suppose you want to group ``product1`` and ``product2`` events into one. There are two ways to do this:

1. Drag & drop method. Drag one node (say, ``product2``) and drop it to ``product1`` node. ``product1_group`` event appears which contains events ``product1`` and ``product2``.

2. Add group method. Click "+ Add group" button, ``untitled_group`` appears. Drag & drop all the nodes to be grouped to this group.

Grouping node has a folder icon that triggers aggregation action. Once you click it, the grouped nodes are merged and the changes are displayed in the transition graph. Recalculation is required to update the node and edge weights.

.. note::

    By recalculation we mean that some additional calculations are required in the backend in order to display the graph state according to the selected options. To recalculate the values, click yellow |warning| icon and request the recalculation. Sometimes it is reasonable to do multiple modifications in the control panel, and then call the recalculation at once.

.. |grouping_1| image:: /_static/user_guides/transition_graph/control_panel_07_nodes_grouping.png

.. |grouping_2| image:: /_static/user_guides/transition_graph/control_panel_08_nodes_grouping_2.png

.. table:: A grouping node. The folder icon triggers merging action.

    +------------------------------------------+------------------------------------------+
    | |grouping_1|                             | |grouping_2|                             |
    +==========================================+==========================================+
    | Grouping nodes using drag & drop method. | Grouping nodes using + Add group method. |
    +------------------------------------------+------------------------------------------+

To rename a grouping node, double click its name and enter a new one. To ungroup the grouped nodes drag & drop the nodes out of the grouping node (or drop it right on the grouping node). As soon as the last event is out, the grouping node disappears.

.. note::

    All the grouping and renaming actions do not affect the initial eventstream due to eventstream immutability property.
    However, it is possible to export the modified eventstream using the :ref:`TransitionGraph.recalculation_result <transition_graph_recalculation_result>` attribute.


.. todo::

    Set a precise link to a section of the eventstream concept document. Vladimir Kukushkin

.. _transition_graph_threshold_GUI:

Thresholds block
~~~~~~~~~~~~~~~~

The Thresholds block contains two sliders: one is associated with the nodes, another one - with the edges. You can set up a threshold value either by moving a slider or by entering a value explicitly. Also, you can set up a weighting column for each slider independently of the weighting column defined in the Weights block (we have already mentioned this feature :ref:`here <transition_graph_thresholds>`). A single slider is shared between multiple weighting columns. As soon as you select a weighting column in the dropdown menu, the threshold slider attaches to it. If you change another weighting column, the slider saves the previously entered threshold value and associate it with the previous weighting column.

.. figure:: /_static/user_guides/transition_graph/control_panel_09_thresholds.png
    :width: 250

    The Thresholds block.

Normalization type block
~~~~~~~~~~~~~~~~~~~~~~~~

Along with the Weights block, the Normalization type block carries the information on the nodes and edges weights. However, so far this block does not allow to change the normalization type.

.. figure:: /_static/user_guides/transition_graph/control_panel_10_normalization_type.png
    :width: 250

    The Normalization type block.

Export block
~~~~~~~~~~~~

Transition graph export supports four formats: HTML, JSON, SVG, and PNG.

.. figure:: /_static/user_guides/transition_graph/control_panel_11_export.png
    :width: 250

    The Export block.

PNG is a general-purpose format. It is used when you need to save a graph as an image.

HTML format is useful when you want to embed the resulting graph into different environments, reports, etc. Such a file supports all the interactive actions as if you treated the graph in Jupyter environment. For example, the graphs that are embedded in this user guide were exported right in this way, so that they are still interactive.

.. _transition_graph_export_json:

JSON format might be useful when you need to get the nodes coordinates.
However, it is important to note that when using this option, only the node coordinates are saved. Any other adjustments or modifications you made in the graph GUI, such as :ref:`thresholds<transition_graph_threshold_GUI>`, :ref:`weights<transition_graph_weights_GUI>`, :ref:`settings<transition_graph_settings_block_GUI>` will be lost and not included in the JSON file. To get the modified state of the eventstream, use :ref:`recalculation_result property<transition_graph_recalculation_result>`.

SVG is a commonly used format for vector graphics.

.. _transition_graph_settings_block_GUI:

Settings block
~~~~~~~~~~~~~~

The control panel also contains a block with checkbox interface for the :ref:`already mentioned settings<transition_graph_settings>`.

.. figure:: /_static/user_guides/transition_graph/control_panel_12_settings.png
    :width: 250

    The Settings block.

.. _transition_graph_layout_dump:

Import and export graph layout
------------------------------

To restore :ref:`previously saved node positions<transition_graph_export_json>`, you need to pass the file path of the JSON file to the ``layout_dump`` parameter.

.. code-block:: python

    path_to_file = '/path/to/node_params.json'
    stream.transition_graph(layout_dump=path_to_file)

.. raw:: html

    <div style="overflow:auto;">
    <iframe
        width="670"
        height="630"
        src="../_static/user_guides/transition_graph/layout_dump.html"
        frameborder="0"
        allowfullscreen
    ></iframe>
    </div>

In general, layout dump mechanism is applied to the same eventstream that was a source of the JSON file. Technically, you can apply it to another eventstream. In this case, the saved node positions are assigned only for those nodes which are enlisted in the JSON file. Other nodes are ignored and placed at some default positions. This trick is convenient when you need to visualize transition graphs for multiple clusters of the same eventstream. You can use the same JSON file for all these graphs.

.. note::

    In order to reproduce your graph state as closely as possible you can pass all the adjustments you have made in the transition graph GUI as :ref:`parameters<transition_graph_python_params>` and their corresponding values to the new transition graph.


Graph properties
----------------

A summary with all the important chosen graph settings is available by clicking ⓘ icon in the bottom right corner.

.. figure:: /_static/user_guides/transition_graph/graph_properties.png
    :width: 350

    Graph properties.

.. _transition_graph_recalculation_result:

Saving the modified eventstream
--------------------------------

When you perform GUI actions that affect eventstream (like grouping events), the original eventstream is not changed.
To work with the updated data, you can export the modified eventstream using the :py:meth:`TransitionGraph.recalculation_result<retentioneering.tooling.transition_graph.transition_graph.TransitionGraph.recalculation_result>`  property.

Suppose you have built a transition graph and obtained the following grouped events ``delivery_choice_group``, ``payment_choice_group``, ``product1_group``.

.. code-block:: python

    tg = stream.transition_graph()

.. figure:: /_static/user_guides/transition_graph/recalculation_result.png

As we see from below, ``recalculation_result``  property contains these grouped events:

.. code-block:: python

    tg.recalculation_result.to_dataframe()


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
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>c209a540-5bdf-45ef-84da-917b0751fb3f</td>
          <td>raw</td>
          <td>0</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>ab361d87-ed46-45d1-950b-9e58c57a017c</td>
          <td>raw</td>
          <td>1</td>
          <td>product1_group</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>fa45b90d-5c03-4d09-b977-5c8a3d2fc32f</td>
          <td>raw</td>
          <td>2</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>1b423d0c-a611-48d1-a20f-a264dd51b4b2</td>
          <td>raw</td>
          <td>3</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>4</th>
          <td>c0fcfaf6-2899-4f07-93e8-776a5147f8d4</td>
          <td>raw</td>
          <td>4</td>
          <td>catalog</td>
          <td>2019-11-01 21:38:19.283663</td>
          <td>964964743</td>
        </tr>
        <tr>
          <th>5</th>
          <td>bde282ab-5153-492e-83c1-8903ccf78a6b</td>
          <td>raw</td>
          <td>5</td>
          <td>cart</td>
          <td>2019-11-01 21:38:36.761221</td>
          <td>964964743</td>
        </tr>
      <tr>
          <th>6</th>
          <td>82c5197e-9a5e-4c16-b6be-92c3aa0c2c08</td>
          <td>raw</td>
          <td>6</td>
          <td>delivery_choice_group</td>
          <td>2019-11-01 21:38:37.564693</td>
          <td>964964743</td>
        </tr>
      </tbody>
    </table>
    <br>


.. _transition_graph_transition_matrix:

Transition matrix
-----------------

:py:meth:`Transition matrix<retentioneering.eventstream.eventstream.Eventstream.transition_matrix>` is a sub-part of transition graph. It contains edge weights only so that the weight of, say, ``A → B`` transition is located at ``A`` row and ``B`` column of the transition matrix. The calculation logic is exactly the same as we have described :ref:`here for transition graphs <transition_graph_edge_weights>`, and the arguments are similar to :ref:`weights-related arguments <transition_graph_setting_the_weights>` of transition graph. Use ``norm_type`` instead of ``edges_norm_type`` and ``weight_col`` instead of ``edges_weight_col``.

.. code-block:: python

    stream.transition_matrix(norm_type='node', weight_col='user_id')

.. raw:: html

    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>cart</th>
          <th>catalog</th>
          <th>...</th>
          <th>payment_done</th>
          <th>payment_cash</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>cart</th>
          <td>0.000594</td>
          <td>0.283848</td>
          <td>...</td>
          <td>0.000000</td>
          <td>0.0</td>
        </tr>
        <tr>
          <th>catalog</th>
          <td>0.418458</td>
          <td>0.633375</td>
          <td>...</td>
          <td>0.000000</td>
          <td>0.0</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>...</td>
          <td>0.000000</td>
          <td>0.0</td>
        </tr>
        <tr>
          <th>payment_cash</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>...</td>
          <td>0.708333</td>
          <td>0.0</td>
        </tr>
      </tbody>
    </table>

For example, from this matrix we can see that the weight of the edge ``cart → catalog`` is ~0.28 with respect to given weights configuration: ``norm_type='node'`` and ``weight_col='user_id'``.

Using a separate instance
-------------------------

By design, :py:meth:`Eventstream.transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>` is a shortcut method that uses :py:meth:`TransitionGraph<retentioneering.tooling.transition_graph.transition_graph.TransitionGraph>` class under the hood. This method creates an instance of TransitionGraph class and embeds it into the eventstream object. Eventually, ``Eventstream.transition_graph()`` returns exactly this instance.

Sometimes it is reasonable to work with a separate instance of TransitionGraph class. An alternative way to get the same visualization that ``Eventstream.transition_graph()`` produces is to call :py:meth:`TransitionGraph.plot()<retentioneering.tooling.transition_graph.transition_graph.TransitionGraph.plot>` method explicitly.

Here is an example how you can manage it:

.. code-block:: python

    from retentioneering.tooling.transition_graph import TransitionGraph

    tg = TransitionGraph(stream)

    tg.plot(
        edges_norm_type='node',
        edges_weight_col='user_id',
        edges_threshold={'user_id': 0.12},
        nodes_threshold={'event_id': 500},
        targets={'positive': ['payment_done', 'cart']}
    )

.. raw:: html

    <div style="overflow:auto;">
    <iframe
        width="670"
        height="630"
        src="../_static/user_guides/transition_graph/separated_instance.html"
        frameborder="0"
        allowfullscreen
    ></iframe>
    </div>
