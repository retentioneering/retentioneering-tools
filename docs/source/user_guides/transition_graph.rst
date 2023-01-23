.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red

Transition Graph
================

The following user guide is also available as `Google Colab notebook <https://colab.research.google.com/drive/14HJDyqV5D6gUYeqBvNfYCxcXe8xoJJLF?usp=share_link>`_

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

The transition graph is a weighted directed graph which illustrates how often the users from an eventstream move from one event to another. The nodes stand for the unique events. A pair of nodes (say, ``A`` and ``B``) is connected with a directed edge if the transition ``A -> B`` appeared at least once in the eventstream. The edge weights stand for the transition frequency which might be calculated in multiple ways (see `Edge weights`_).

The primary way to build a transition graph is to call :py:meth:`Eventstream.transition_graph()<retentioneering.eventstream.eventstream.Eventstream.transition_graph>`:

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

The graph is interactive. You can move the nodes, zoom in/out the chart, and finally reveal or hide a `control panel`_ by clicking on the left edge of the chart. You can check it out even in the transition graphs illustrating this document.

Edge weights
------------

The edge weight values are controlled by ``norm_type`` and ``weights`` parameters. However, ``weights`` parameter is a bit complex for usage, so in this section we'll refer to a fake ``weight_col`` parameter as an alias for ``weights={'edges': weight_col}`` pattern.

As we mentioned earlier, the most straightforward way to assign the edge weights is to calculate the number of the transition in the entire eventstream. In this case we need to use ``norm_type=None`` (i.e. no normalization is needed; we'll discuss what we mean by normalization a bit further). But often this is not enough, and we want to know how many unique users or unique sessions were involved in a specific transition. This is when ``weight_col`` parameter comes handy. This parameter sets a column which is used for getting the unique identifiers related to a given transition. For example, if ``weight_col='user_id'`` then for each transition the number of unique users who had this transition is calculated. If ``session_id`` column is set (see :py:meth:`SplitSessions<retentioneering.data_processors_lib.split_sessions.SplitSessions>` data processor) then ``weight_col='session_id'`` will calculate the number of unique sessions for each transition.

It's natural for many types of analysis use rations instead of natural numbers. In case of edge weights, we might want to know, for example, how many times a particular transition occurred in comparison with the total number of the transitions in the entire eventstream. Thus, we need to divide an edge weight represented as a transitions count by some denominator. That's what we mean by weight normalization. A couple of normalization options is possible.

``norm_type='node'`` defines the denominator as the number of

We'll explain how the weights are calculated using a tiny eventstream:

.. raw:: html

    user1: <font color='red'>A</font>, <font color='red'>B</font>, <font color='SlateBlue'>A</font>, <font color='SlateBlue'>C</font>, <font color='green'>A</font>, <font color='green'>B</font><br>
    user2: <font color='magenta'>A</font>, <font color='magenta'>B</font>, <font color='orange'>C</font>, <font color='orange'>C</font>, <font color='orange'>C</font><br>
    user3: <font color='DarkTurquoise'>C</font>, <font color='DarkTurquoise'>D</font>, <font color='DarkTurquoise'>C</font>, <font color='DarkTurquoise'>D</font>, <font color='DarkTurquoise'>C</font>, <font color='DarkTurquoise'>D</font><br><br>

This eventstream consists of 3 users and 4 events. The event colors denote sessions. We ignore the timestamps since the edge weights calculation doesn't take them into account.




The table |edge_weights_norm_type_none| describes

.. |edge_weights_norm_type_none| replace:: The edge weights calculation for ``norm_type = None``

.. raw:: html

    <table class="dataframe">
      <thead style="thead tr th{border: 1px;}">
        <tr>
          <th>norm_type</th>
          <th halign="center">None</th>
          <th colspan="2" align="left">node</th>
          <th colspan="2" align="left">full</th>
        </tr>
        <tr>
          <th>components</th>
          <th>weight</th>
          <th>denom</th>
          <th>weight</th>
          <th>denom</th>
          <th>weight</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>A -&gt; B</th>
          <td>3</td>
          <td>4</td>
          <td>0.750</td>
          <td>14</td>
          <td>0.214</td>
        </tr>
        <tr>
          <th>A -&gt; C</th>
          <td>1</td>
          <td>4</td>
          <td>0.250</td>
          <td>14</td>
          <td>0.071</td>
        </tr>
        <tr>
          <th>B -&gt; A</th>
          <td>1</td>
          <td>2</td>
          <td>0.500</td>
          <td>14</td>
          <td>0.071</td>
        </tr>
        <tr>
          <th>B -&gt; C</th>
          <td>1</td>
          <td>2</td>
          <td>0.500</td>
          <td>14</td>
          <td>0.071</td>
        </tr>
        <tr>
          <th>C -&gt; A</th>
          <td>1</td>
          <td>6</td>
          <td>0.167</td>
          <td>14</td>
          <td>0.071</td>
        </tr>
        <tr>
          <th>C -&gt; C</th>
          <td>2</td>
          <td>6</td>
          <td>0.333</td>
          <td>14</td>
          <td>0.143</td>
        </tr>
        <tr>
          <th>C -&gt; D</th>
          <td>3</td>
          <td>6</td>
          <td>0.500</td>
          <td>14</td>
          <td>0.214</td>
        </tr>
        <tr>
          <th>D -&gt; C</th>
          <td>2</td>
          <td>2</td>
          <td>1.000</td>
          <td>14</td>
          <td>0.143</td>
        </tr>
      </tbody>
    </table>

The default way to present them is to simply calculate the total number of occurrences of the corresponding transitions in the eventstream. This behavior relates to ``norm_type=None`` parameter value.



Control panel
-------------

.. |control_panel1| image:: /_static/user_guides/transition_graph/control_panel_1.png
.. |control_panel2| image:: /_static/user_guides/transition_graph/control_panel_2.png

.. table:: A screenshot of the control panel.

    +------------------+------------------+
    | |control_panel1| | |control_panel2| |
    +------------------+------------------+
    | upper menu part  | lower menu part  |
    +------------------+------------------+

Using a separate instance
-------------------------

Common tooling properties
-------------------------
