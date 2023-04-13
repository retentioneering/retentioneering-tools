Quick start with Retentioneering
================================

Retentioneering is a Python library for in-depth analysis of what is commonly called user clickstream. We find the traditional term clickstream to be too constrictive, as user actions may not just be clicks; instead, we use the term *event* to mean any user action, and *eventstream* to refer to a set of actions performed by the user. A set of events belonging to a particular user is called *user path* or *user trajectory*, and sometimes *customer journey map* (CJM) is used as a synonym for eventstream.

Each event is tied to the user who experienced it, and a timestamp. Hence, at a basic level, eventstream comprises a set of triples like these:

.. parsed-literal::

    ('user_1', 'login', '2019-01-01 00:00:00'),
    ('user_1', 'main_page_visit', '2019-01-01 00:00:00'),
    ('user_1', 'cart_button_click', '2019-01-01 00:00:00'),
    ...

Any eventstream research consists of three fundamental steps:

- Loading data
- Preparing the data
- Applying Retentioneering tools

This document is a brief overview of how to follow these steps. For more detail, see the :doc:`User Guides <../user_guide>`.

Loading data
------------

This is the introduction to our core class :doc:`Eventstream <../user_guides/eventstream>`, which stores eventstream events and enables you to work with them efficiently.

We have provided a small :doc:`simple_shop <../datasets/simple_shop>` dataset for you to use for demo purposes here, and throughout the documentation.

.. code-block:: python

    from retentioneering import datasets

    # load sample user behavior data:
    stream = datasets.load_simple_shop()

In the shell of eventstream object there is a regular pandas.DataFrame which can be revealed by calling :py:meth:`to_dataframe()<retentioneering.eventstream.eventstream.Eventstream.to_dataframe>` method:

.. code-block:: python

    stream.to_dataframe().head()

.. raw:: html

    <div>
    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>user_id</th>
          <th>event</th>
          <th>timestamp</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>219483890</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
        </tr>
        <tr>
          <th>1</th>
          <td>219483890</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
        </tr>
        <tr>
          <th>2</th>
          <td>219483890</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
        </tr>
        <tr>
          <th>3</th>
          <td>219483890</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
        </tr>
        <tr>
          <th>4</th>
          <td>964964743</td>
          <td>catalog</td>
          <td>2019-11-01 21:38:19.283663</td>
        </tr>
      </tbody>
    </table>
    </div>

In this fragment of the dataset, user ``219483890`` has 4 events with timestamps on the website on ``2019-11-01``.

If you are OK with the simple_shop dataset, you can proceed to the next section. Alternatively, you can create an eventstream by uploading your own dataset. It must be represented as a csv-table with at least three columns (``user_id``, ``event``, and ``timestamp``). Upload your table as a pandas.DataFrame and create the eventstream as follows:

.. code-block:: python

    import pandas as pd
    from retentioneering.eventstream import Eventstream

    # load your own csv
    data = pd.read_csv("your_own_data_file.csv")
    stream = Eventstream(data)

If the input table columns have different names, either rename them in the DataFrame, or explicitly set data schema (see :ref:`Eventstream user guide <eventstream_custom_fields>` for the instructions). Likewise, if the table has additional custom columns, setting the data schema is also required.

Getting a CSV file with data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you use Google Analytics, raw data in the form of {user, event, timestamp} triples can be streamed via Google Analytics 360 or free Google Analytics App+Web into BigQuery. From the BigQuery console, you can run an SQL query and export data into a csv file. Alternatively, you can use the Python BigQuery connector to get directly into the DataFrame. For large datasets, we suggest sampling the users in an SQL query, filtering by the user_id (just add this condition to SQL WHERE statement to get 10% of your users:

.. parsed-literal::

    and ABS(MOD(FARM_FINGERPRINT(fullVisitorId), 10)) = 0)

.. _quick_start_preprocessing:

Preparing the data
------------------

Raw data often needs to be prepared before analytical techniques are applied. Retentioneering provides a wide range of preprocessing tools that are comprised of elementary parts called “data processors.” With the help of data processors, a product analyst can easily add, delete, or group events, flexibly truncate an eventstream, split the trajectories into sessions, and much more. See the :doc:`Data processors user guide <../user_guides/dataprocessors>` for a comprehensive description of this Swiss army knife for data processors.

Below is a brief example of how the data processors work.

Suppose you wanted to analyze only the first session of each user, rather than their whole trajectory. Here is how you can do that with just a few lines of code:

.. code-block:: python

    # eventstream preprocessing example
    stream \
        .split_sessions(timeout=(30, 'm')) \
        .filter(func=lambda df_, schema: df_['session_id'].str.endswith('_1')) \
        .to_dataframe() \
        .head()

.. raw:: html

    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>user_id</th>
          <th>event</th>
          <th>timestamp</th>
          <th>session_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>219483890</td>
          <td>session_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>1</th>
          <td>219483890</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>3</th>
          <td>219483890</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>5</th>
          <td>219483890</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>7</th>
          <td>219483890</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890_1</td>
        </tr>
      </tbody>
    </table>
    <br>

At the beginning, we take a ``stream`` variable that contains the eventstream instance created in the previous section. The :ref:`split_sessions<split_sessions>` method creates a new column called ``session_id``, in which values ending with the suffix ``_<int>`` indicate the ordinal number of each user’s session. In the end, we need to leave only those records where ``session_id`` ends with ``_1`` (meaning the first session). This is exactly what the filter method does. We also apply the ``to_dataframe()`` method, which you are already familiar with.

In real life, analytical eventstream research is likely to be branchy. You might want to wrangle an initial eventstream’s data in many ways, check multiple hypotheses, and look at different parts of the eventstream. All of this is easily and efficiently managed using the preprocessing graph. It enables you to keep all the records and code related to the research in a calculation graph. This tool is especially recommended for those who need to share parts of the analytical code with team members. See the :doc:`Preprocessing user guide <../user_guides/preprocessing>` for more details.

.. _quick_start_rete_tools:

Applying path analysis tools
----------------------------

Retentioneering offers many powerful tools for exploring the behavior of your users, including transition graphs, step matrices, step Sankey diagrams, funnels, cluster, and cohort analysis. A brief demo of each is presented below. For more details, see :ref:`the user guides <UG_path_analysis_tools>`.

.. _quick_start_transition_graph:

Transition graph
~~~~~~~~~~~~~~~~

Transition graph is an interactive tool that shows how many users jump from one event to another. It represents user paths as a Markov random walk model. The graph is interactive: you can drag the graph nodes, zoom in and out of the graph layout, or use a control panel on the left edge of the graph. The transition graph also allows you to highlight the most valuable nodes, and hide noisy nodes and edges.

.. code-block:: python

    stream.transition_graph()

.. raw:: html

    <iframe
        width="680"
        height="630"
        src="../_static/getting_started/quick_start/transition_graph.html"
        frameborder="0"
        allowfullscreen
    ></iframe>

See :doc:`Transition graph user guide<../user_guides/transition_graph>` for a deeper understanding of this tool.

.. _quick_start_step_matrix:

Step matrix
~~~~~~~~~~~

The step matrix provides a stepwise look at CJM. It shows the event distribution with respect to a step ordinal number.

.. code-block:: python

    stream.step_matrix(
        max_steps=16,
        thresh=0.2,
        centered={
            'event': 'cart',
            'left_gap': 5,
            'occurrence': 1
        },
        targets=['payment_done']
    )

.. figure:: /_static/getting_started/quick_start/step_matrix.png
    :width: 900

The step matrix above is centered by ``cart`` event. For example, it shows (see column ``-1``) that the events in the user trajectories one step before ``cart`` event are distributed as follows: 60% of the users have ``catalog`` event right before ``cart``, 24% of the users have ``product2`` event, and 16% of the users are distributed among 5 events which are folded to an artificial ``THRESHOLDED_5`` event.

See :doc:`Step matrix user guide<../user_guides/step_matrix>` user guide for a deeper understanding of this tool.

Step Sankey diagram
~~~~~~~~~~~~~~~~~~~

The step Sankey diagram is similar to the step matrix. It also shows the event distribution with respect to step number. However, it has some more advanced features:

- it explicitly shows the user flow from one step to another; and
- it is interactive.

.. code-block:: python

    stream.step_sankey(max_steps=6, thresh=0.05)

.. raw:: html

    <div style="overflow:auto;">
    <iframe
        width="1200"
        height="500"
        src="../_static/getting_started/quick_start/step_sankey.html"
        frameborder="0"
        allowfullscreen
    ></iframe>
    </div>

See :doc:`step Sankey user guide<../user_guides/step_sankey>` for a deeper understanding of this tool.

.. _quick_start_cluster_analysis:

Cluster analysis
~~~~~~~~~~~~~~~~

.. code-block:: python

    from retentioneering.tooling.clusters import Clusters

    clusters = Clusters(stream)
    clusters.fit(method='kmeans', n_clusters=8, feature_type='tfidf', ngram_range=(1, 2))
    clusters.plot(targets=['payment_done', 'cart'])

.. figure:: /_static/getting_started/quick_start/clusters.png
    :width: 900

Users with similar behavior are grouped in the same cluster. Clusters with low conversion rates can indicate a systematic problem in the product: a specific behavior pattern that does not lead to product goals. The obtained user segments can be explored in more depth to understand the problematic behavior patterns. In the example above for instance, cluster 4 has a low conversion rate to ``payment_done``, but a high conversion rate to ``cart`` visit.

See :doc:`Clusters user guide<../user_guides/clusters>` for a deeper understanding of this tool.

.. _quick_start_funnels:

Funnel analysis
~~~~~~~~~~~~~~~

Building a conversion funnel is a basic part of much analytical research. Funnel is a diagram that shows how many users sequentially walk through specific events (funnel stages) in their paths. For each stage event, the following values are calculated:

- absolute unique number of users who reached this stage at least once;
- conversion rate from the first stage (% of initial); and
- conversion rate from the previous stage (% of previous).

.. code-block:: python

    stream.funnel(stages=['catalog', 'cart', 'payment_done'])

.. raw:: html

    <iframe
        width="700"
        height="400"
        src="../_static/getting_started/quick_start/funnel.html"
        frameborder="0"
        allowfullscreen
    ></iframe>

See :doc:`Funnel user guide<../user_guides/funnel>` for a deeper understanding of this tool.

Cohort analysis
~~~~~~~~~~~~~~~

Cohorts is a powerful tool that shows trends of user behavior over time. It helps to isolate the impact of different marketing activities, or changes in a product for different groups of users.

Here is an outline of the *cohort matrix* calculation:

- Users are split into groups (``CohortGroups``) depending on the time of their first appearance in the eventstream; and
- The retention rate of the active users is calculated in each period (``CohortPeriod``) of the observation.

.. code-block:: python

    stream.cohorts(
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=False,
    )

.. figure:: /_static/getting_started/quick_start/cohorts.png
    :width: 500
    :height: 500

See :doc:`Cohorts user guide<../user_guides/cohorts>` for a deeper understanding of this tool.
