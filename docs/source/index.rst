.. raw:: html

    <style>
        .orange {color:#ffc524; font-weight:bold;}
    </style>

.. role:: orange

Retentioneering is a powerful Python library for in-depth clickstream and product analysis.

.. grid:: 2
    :margin: 0 3 0 0
    :class-container: small_icon

    .. grid-item-card:: QUICK START
        :img-top: _static/tool_icons/quick_start.svg
        :link: getting_started/quick_start
        :link-type: doc
        :text-align: center

    .. grid-item-card:: INSTALLATION
        :img-top: _static/tool_icons/install.svg
        :link: getting_started/installation
        :link-type: doc
        :text-align: center

.. grid:: 4
    :margin: 0 2 0 0

    .. grid-item-card:: Descriptive methods
        :img-top: _static/tool_icons/describe.png
        :link: eventstream_descriptive_methods
        :link-type: ref

    .. grid-item-card:: Transition graph
        :img-top: _static/tool_icons/transition_graph.png
        :link: /user_guides/transition_graph
        :link-type: doc

    .. grid-item-card:: Step Sankey
        :img-top: _static/tool_icons/step_sankey.png
        :link: /user_guides/step_sankey
        :link-type: doc

    .. grid-item-card:: Step matrix
        :img-top: _static/tool_icons/step_matrix.png
        :link: /user_guides/step_matrix
        :link-type: doc

.. grid:: 4
    :margin: 0 2 0 0

    .. grid-item-card:: Clusters
        :img-top: _static/tool_icons/clusters.png
        :link: /user_guides/clusters
        :link-type: doc

    .. grid-item-card:: Cohorts
        :img-top: _static/tool_icons/cohorts.png
        :link: /user_guides/cohorts
        :link-type: doc

    .. grid-item-card:: Funnel
        :img-top: _static/tool_icons/funnel.png
        :link: /user_guides/funnel
        :link-type: doc

    .. grid-item-card:: Preprocessing Graph :orange:`(beta)`
        :img-top: _static/tool_icons/preprocessing_graph.png
        :link: /user_guides/preprocessing
        :link-type: doc



What is Retentioneering?
========================


You can use Retentioneering to explore user behavior, segment users, and form hypotheses about what drives users to desirable actions or to churning away from a product.

Retentioneering uses clickstream data to build behavioral segments, highlighting the events and patterns in user behavior that impact your conversion rates, retention, and revenue. The Retentioneering library is created for data analysts, marketing analysts, product owners, managers, and anyone else whose job is to improve a product’s quality.

As a natural part of the `Jupyter <https://jupyter.org/>`_ environment, Retentioneering extends the abilities of `pandas <https://pandas.pydata.org>`_, `NetworkX <https://networkx.org/>`_, `scikit-learn <https://scikit-learn.org>`_ libraries to process sequential events data more efficiently. Retentioneering tools are interactive and tailored for analytical research, so you do not have to be a Python expert to use it. With just a few lines of code, you can wrangle data, explore customer journey maps, and make visualizations.

**Date**: |today| **Version**: |version|

.. toctree::
    :maxdepth: 1
    :hidden:

    Getting started <getting_started.rst>
    User guide <user_guide.rst>
    API reference <api_reference.rst>
    Tutorials <tutorials.rst>
    Datasets <datasets.rst>
    Release notes <release_notes.rst>
