Plot_graph function
~~~~~~~~~~~~~~~~~~~

Normalization intro
===================

There are multiple options for normalization:

.. image:: _static/plot_graph/norm_types.svg

No normalization
================

In this case we are looking at absolute values

norm_type = 'full'
==================

This is full normalization

norm_type = 'node'
==================

Normalization based on node.

Here, sum of all weights for out-bound edges from
any node will always sums to 100%