Preprocessing
=============

Preprocessing GUI
-----------------


Code-generated preprocessing graph
----------------------------------

For more complex scenarios of using the preprocessing module it might be useful to generate a preprocessing graph from code. This usually relates to complex research covering multiple hypothesis and involving much data wrangling.

Here's an outline for creation a preprocessing graph with code:

- Create an instance of the preprocessing graph :py:meth:`PGraph<retentioneering.graph.p_graph.PGraph>` class;
- Create instances of :py:meth:`EventsNode <retentioneering.graph.nodes.EventsNode>` class (in some cases you need to use :py:meth:`MergeNode <retentioneering.graph.nodes.MergeNode>`; we'll discuss it later);
- Link the nodes with edges by calling :py:meth:`PGraph.add_node() <retentioneering.graph.p_graph.PGraph.add_node>` assigning appropriate nodes as parents.
- In order to run the all the calculations from the root to a particular node call :py:meth:`PGraph.combine() <retentioneering.graph.p_graph.PGraph.combine>` method passing an identifier of a target node.

Let's study all these steps on a detailed level.

Creating an empty graph
~~~~~~~~~~~~~~~~~~~~~~~

The only parameter of the :py:meth:`PGraph<retentioneering.graph.p_graph.PGraph>` constructor is an eventstream the graph will be associated with. As usual in our user guides, we'll use simple_shop dataset.

.. code-block:: python

    from retentioneering.graph.p_graph import PGraph

    pgraph = PGraph(stream)


Creating a single node
~~~~~~~~~~~~~~~~~~~~~~

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
