What is Retentioneering?
========================

Retentioneering is a `Python library <https://github.com/retentioneering/retentioneering-tools>`_ that makes analyzing clickstreams, user paths (trajectories), and event logs much easier, and yields much broader and deeper insights than funnel analysis. You can use Retentioneering to explore user behavior, segment users, and form hypotheses about what drives users to desirable actions or to churning away from a product. Retentioneering uses clickstream data to build behavioral segments, highlighting the events and patterns in user behavior that impact your conversion rates, retention, and revenue. The Retentioneering library is created for data analysts, marketing analysts, product owners, managers, and anyone else whose job is to improve a product’s quality.


.. figure:: /_static/getting_started/what_is_rete/intro_0.png

    A simplified scenario of user behavior exploration with Retentioneering.

As a natural part of the `Jupyter <https://jupyter.org/>`_ environment, Retentioneering extends the abilities of `pandas <https://pandas.pydata.org>`_, `NetworkX <https://networkx.org/>`_, `scikit-learn <https://scikit-learn.org>`_ libraries to process sequential events data more efficiently. Retentioneering tools are interactive and tailored for analytical research, so you do not have to be a Python expert to use it. With just a few lines of code, you can wrangle data, explore customer journey maps, and make visualizations.

Retentioneering consists of two major parts: :ref:`the preprocessing module <quick_start_preprocessing>`, and :ref:`the path analysis tools <quick_start_rete_tools>`.

The *preprocessing module* provides a wide range of hands-on methods specifically designed for processing clickstream data, which can be called either using code, or via the preprocessing GUI. With separate methods for grouping or filtering events, splitting a clickstream into sessions, and much more, the Retentioneering preprocessing module enables you to dramatically reduce the amount of code, and therefore potential errors. Plus, if you’re dealing with a branchy analysis, which often happens, the preprocessing methods will help you make the calculations structured and reproducible, and organize them as a calculation graph. This is especially helpful for working with a team.

The *path analysis tools* bring behavior-driven segmentation of users to product analysis by providing a powerful set of techniques for performing in-depth analysis of customer journey maps. The tools feature informative and interactive visualizations that make it possible to quickly understand in very high resolution the complex structure of a clickstream. Tools like :ref:`the transition graph <quick_start_transition_graph>`, :ref:`the step matrix <quick_start_step_matrix>`, multiple :ref:`clustering and vectorization engines <quick_start_cluster_analysis>`, :ref:`funnels <quick_start_funnels>`, and others enable you to easily and intuitively explore user behavior patterns in-depth.

The library is installed with a standard ``pip install retentioneering`` command.

Raw data can be downloaded from Google Analytics BigQuery stream, or any other such streams. Just convert that data to the list of triples - user_id, event, and timestamp - and pass it to Retentioneering tools. The package also includes some datasets for a quick start.

We recommend starting your Retentioneering journey with the :doc:`Quick Start document <../getting_started/quick_start>`.
