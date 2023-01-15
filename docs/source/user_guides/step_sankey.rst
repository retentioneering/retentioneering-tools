.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red

StepSankey
==========

The following user guide is also available as `Google Colab notebook <https://colab.research.google.com/drive/1o6npbrtscHqg1AUAkIIemA3h4a1XslSV?usp=share_link>`_.

Loading data
------------

We use ``simple_shop`` dataset for demonstration purposes. If you want to use your own dataset, upload it following :doc:`this instruction</user_guides/eventstream>`.

:red:`TODO: change the anchored link when rst doc is ready`

.. code-block:: python

    import pandas as pd
    from retentioneering import datasets

    stream = datasets.load_simple_shop()

Basic example
-------------

The ``StepSankey`` diagram represents eventstream as a step-wise directed graph. The nodes are associated with events appeared at a specified step in a user's trajectory. The nodes are sorted from left to right according to the ordinal number of step (1, 2, etc). The edges visualize how often transition from event ``A`` happened at ``i``-th step to event ``B`` happened at ``i+1``-th step occurred. The nodes and edges size reflects the number of unique users involved in them.

``StepSankey`` diagram in some sense is an extension of :doc:`StepMatrix</user_gudes/step_matrix>` diagram. It shows the distribution of the events with respect to an ordinal step (the nodes height is proportional to the event frequency at a specified step). But ``StepSankey`` also reflects the connection between adjacent steps which ``StepMatrix`` has a lack of.

The diagram is interactive, so you can hover the nodes and edges and look at the detailed info, move the nodes, and even merge them.

The implementation is based on `Plotly Sankey diagram <https://plotly.com/python/sankey-diagram/>`_.

Here's how ``StepSankey`` visualizes ``simple_shop`` eventstream:

.. code-block:: python

    stream.step_sankey(max_steps=5)

.. raw:: html

    <iframe
        width="700"
        height="500"
        src="../_static/step_sankey/basic_step_sankey.html"
        frameborder="0"
        allowfullscreen
    ></iframe>

Here we can see user flow. The nodes are grouped into columns in step-wise manner, so the first column corresponds to the events occurred at users' first step, the second column corresponds to the second step and so on. As it was said above, the height of a rectangular representing a node is proportional to the frequency this particular event occurred at this particular step. From this diagram we can see (if we hover the mouse cursor on the node) that at first step event ``catalog`` appeared 2.69K times (71.61% of the users) whereas event ``main`` appeared 1.07 times (28.39% of the users). That's why the red rectangular (for ``catalog`` event) is ~2.5 times higher than the green rectangular (for ``main`` event). The percentage of the users is calculated with respect to all the users participating in the parent eventstream.

An edge's width is proportional to the frequency the corresponding transition occurred in the eventstream. Hovering mouse on the edges, you can reveal not only this information, but also the info on how long the transition took the users in average. For example, we can see that the transition ``catalog (1st step) -> catalog (2nd step)`` appeared in 869 paths, and it took 29 seconds in average.

.. |hover_node1| image:: /_static/step_sankey/hover_node1.png
.. |hover_node2| image:: /_static/step_sankey/hover_node2.png
.. |hover_edge| image:: /_static/step_sankey/hover_edge.png

.. table:: The screenshots of the data chunks on mouse hovering

    +---------------+---------------+--------------+
    | |hover_node1| | |hover_node2| | |hover_edge| |
    +---------------+---------------+--------------+

``path_end`` event
------------------

As you may know, ``path_end`` is a special synthetic event which explicitly indicates a trajectory end. It is yielded as a result of :py:meth:`StartEndEvents<src.data_processors_lib.start_end_events.StartEndEvents>` data processor. Like for :doc:`StepMatrix</user_gudes/step_matrix>`, ``path_end`` event is special for StepSankey. If a user's path is shorter than ``max_steps`` param, ``path_end`` pads the path so that its length becomes exactly ``max_steps``. Having this behavior implemented, we can guarantee that the sum of the user fractions over each column (i.e. each step) is exactly 1.

.. code-block:: python

    stream\
        .add_start_end()\
        .step_sankey(max_steps=5)

Concealing rare events
----------------------
Another similarity with :doc:`StepMatrix</user_gudes/step_matrix>` is that StepSankey has ``thresh`` parameter which allows to hide rare event in order not
