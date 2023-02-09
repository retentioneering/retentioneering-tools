.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red

What is Retentioneering?
========================

Retentioneering is a `Python library <https://github.com/retentioneering/retentioneering-tools>`_ aiming to assist product analysts and marketing analysts. It makes easier to process and analyze clickstreams, user paths (trajectories), and event logs. With a help of Retentioneering tools you can explore user behavior, segment users, make hypothesis what leads users to a desirable action or makes them leave the product. The library methods can reveal much more insights than funnel analysis, as it will automatically build the behavioral segments and patterns, highlighting what events and patterns impact your conversion rates, retention and revenue.

:red:`TODO: replace the github link as soon as we make 3.0 public`

.. figure:: /_static/getting_started/what_is_rete/intro_0.png

    A simplified scenario of user behavior exploration with Retentioneering.

Being a natural part of `Jupyter <https://jupyter.org/>`_ environment, Retentioneering extends the abilities of `pandas <https://pandas.pydata.org>`_, `NetworkX <https://networkx.org/>`_, `scikit-learn <https://scikit-learn.org>`_ libraries for efficient processing of sequential events data. However, you don't need to be a Python expert: the most Retentioneering tools are interactive and tailored for analytical research, so you can wrangle the data, explore customer journey maps, make visualizations with literally few lines of code.

In general, Retentioneering consists of two major parts: :ref:`Preprocessing module <quick_start_preprocessing>` and :ref:`Analytical tools <quick_start_rete_tools>`.

The preprocessing module provides a wide range of hands-on methods specifically designed for processing clickstream data. For example, this module contains separated methods for grouping or filtering events, splitting a clickstream into the sessions, and many more which you can either call from code or simply use preprocessing GUI. Using these methods allows you not only dramatically reduce the amount of code, but also minimize the number of potential errors. Finally, if you deal with a branchy analytical research which often happens in practice, the preprocessing methods will help you to make the calculations structured and reproducible organizing them as a calculation graph. This is especially useful for collaborative work within a team.

Retentioneering analytical tools provide a powerful set of techniques to perform in-depth analysis of customer journey maps, bringing behavior-driven segmentation of users to product analysis. The tools are focused on informative and interactive visualizations which helps to deeply understand complex structure of a clickstream. Namely, you can explore behavioral patterns using such tools as :ref:`transition graph <quick_start_transition_graph>`, :ref:`step matrix <quick_start_step_matrix>`, multiple :ref:`clustering and vectorization engines <quick_start_cluster_analysis>`, :ref:`good old funnels <quick_start_funnels>`, and others.

The library is installed with a standard ``pip install retentioneering`` command.

Raw data might be downloaded from such sources as Google Analytics BigQuery stream or any other similar streams. All that you need is to convert the data to the list of triples user_id, event, timestamp and pass it to Retentioneering tools. Sample datasets are also included in the package for a quick start.

We recommend to start your journey with :doc:`Quick start document <../getting_started/quick_start>`.
