.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red

Quick start with Retentioneering
================================

Retentioneering is a python library for in-depth clickstream analysis. We find *clickstream* term a bit narrow since user actions might be not necessarily clicks, so we use instead term *event* referring any user action and *eventstream* referring a set of actions. Each event is associated with a user who experienced it and timestamp when the event occurred. Hence, at a basic level eventstream is a set of triples like these:

.. parsed-literal::

    ('user_1', 'login', '2019-01-01 00:00:00'),
    ('user_1', 'main_page_visit', '2019-01-01 00:00:00'),
    ('user_1', 'cart_button_click', '2019-01-01 00:00:00'),
    ...

A set of events belonging to a particular user is called *user path* or *user trajectory*. Sometimes we will use *CJM (customer journey map)* term as a synonym to eventstream.

Any eventstream research consists of three fundamental steps:

- Loading data
- Preparing the data
- Applying retentioneering tools

In this document we briefly describe how to follow these steps. For more details see the :doc:`User Guides <../user_guide>`.

Loading data
------------

Here we introduce you our core class :doc:`Eventstream <../user_guides/eventstream>` which not only stores eventstream events, but also allows to treat them in an efficient way.

For demonstration purposes we offer you to use our small :doc:`simple_shop <../datasets/simple_shop>` dataset.

.. code-block:: python

    from retentioneering import datasets

    # load sample user behavior data:
    stream = datasets.load_simple_shop()

In the shell of eventstream object there is a regular pandas DataFrame which can be revealed by calling :py:meth:`to_dataframe()<retentioneering.eventstream.eventstream.Eventstream.to_dataframe>` method:

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

As you can see in this fragment of the dataset, user with id ``219483890`` has 4 events on the website with specific timestamps on ``2019-11-01``. If you're ok with the ``simple_shop`` dataset, you can follow the next section.

Alternatively, you can create an eventstream by uploading your own dataset. In this case, your data must be represented as a csv-table with at least three columns ``user_id``,  ``event``, ``timestamp``. Upload  it as a pandas DataFrame and create eventstream as follows:

.. code-block:: python

    import pandas as pd
    from retentioneering.eventstream import Eventstream

    # load your own csv
    data = pd.read_csv("your_own_data_file.csv")
    stream = Eventstream(data)

If the input table columns have different names, you need either to rename them in the pandas DataFrame or explicitly set data schema (see :ref:`Eventstream user guide <eventstream_custom_fields>` for the details). Setting the data schema is also required if the input table has additional custom columns.

How to get a csv-file with data? For example, if you use Google Analytics, raw data in the form of ``{user, event, timestamp}`` triples can be streamed via Google Analytics 360 or free Google Analytics App+Web into BigQuery. From the BigQuery console you can run SQL query and export data into csv file, alternatively you can use the Python BigQuery connector to get directly into the dataframe. For large datasets, we suggest to sample the users in an SQL query, filtering by the user_id (just add this condition to SQL ``WHERE`` statement to get 10% of your users:

.. parsed-literal::

    and ABS(MOD(FARM_FINGERPRINT(fullVisitorId), 10)) = 0)

.. _quick_start_preprocessing:

Preparing the data
------------------

Raw data often needs to be prepared before applying analytical techniques. Retentioneering provides a wide range of preprocessing tools which should become a Swiss knife for a product analytics. We call them *data processors*. With a help of data processors, a product analyst can easily add, delete or group events, truncate an eventstream in a flexible manner, split the trajectories into sessions, and many more. See :doc:`Data processors user guide <../user_guides/dataprocessors>` for the comprehensive description.

We provide below a short example so you could catch an idea how the data processors work. Suppose you want to analyze only first session of each user instead of the whole eventstream. Here's how you can easily achieve this with few lines of code:

.. code-block:: python

    # eventstream preprocessing example
    stream \
        .split_sessions(session_cutoff=(30, "m")) \
        .filter(func=lambda df_, schema: df_["session_id"].str.endswith("_1")) \
        .to_dataframe() \
        [["user_id", "event", "timestamp", "session_id"]] \
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

In the beginning we take ``stream`` variable which contains the eventstream instance created in the previous section. ``split_sessions`` method creates a new column ``session_id`` with the values ending with suffix ``_<int>`` indicating the ordinary number of each user's session. Finally, we need to leave only those records where ``session_id`` ends with ``_1``. This is exactly what the next method ``filter`` does. Also, we apply ``to_dataframe()`` method which you already know and select the standard triple columns plus ``session_id``.

Unlike this toy example, in practice analytical research on an eventstream might be branchy. You may want to wrangle an initial eventstream data in many ways, check multiple hypothesis, look at different parts of the eventstream. All these scenarios might be efficiently managed using the *preprocessing graph*. It allows you to keep all the records and code related to the research in a calculation graph. We especially recommend to try this tool for those who work in analytical teams and need to share some parts of the analytical code among team members. See :doc:`Preprocessing user guide <../user_guides/preprocessing>` for more details.

.. _quick_start_rete_tools:

Applying retentioneering tools
------------------------------

Retentioneering has many powerful tools for exploring users' behavior, including transition graphs, step matrices, step Sankey diagrams, funnels, cluster and cohort analysis. Below we show just a short demo for each of them. For more details, please study :ref:`user guides <UG core tools>`.

.. _quick_start_transition_graph:

Transition graph
~~~~~~~~~~~~~~~~

Transition graph is an interactive tool which illustrates how many users jump from one event to another. In fact, it represents user paths as a Markov random walk model. The graph is interactive, and you can drag the graph nodes, zoom-in/zoom-out the graph layout, or use a menu panel on the left edge of the graph. Also, you can highlight the most valuable nodes and hide noisy nodes and edges.

.. code-block:: python

    stream.transition_graph(
        thresholds={
            'nodes': {'events': 0.06},
            'edges': {'events': 0.06}
        },
        norm_type=None,
        targets={
            "lost": "bad",
            "payment_done": "nice",
            "main": "source"
        }
    );

.. raw:: html

    <iframe
        width="700"
        height="600"
        src="../_static/getting_started/quick_start/transition_graph.html"
        frameborder="0"
        allowfullscreen
    ></iframe>

:red:`TODO: replace this html with another one once transition graph is fixed`

See :doc:`TransitionGraph user guide<../user_guides/transition_graph>` to understand this tool deeper.

.. _quick_start_step_matrix:

Step matrix
~~~~~~~~~~~

Step matrix provides a stepwise look at CJM. It shows the event distribution with respect to a step ordinal number.

.. code-block:: python

    stream.step_matrix(
        max_steps=16,
        thresh=0.2,
        centered={
            "event": "cart",
            "left_gap": 5,
            "occurrence": 1
        },
        targets=['payment_done']
    );

.. figure:: /_static/getting_started/quick_start/step_matrix.png
    :width: 900

The step matrix above is centered by ``cart`` event. For example, it shows (see ``-1`` column) that the events in the user trajectories one step before ``cart`` event are distributed as follows: 60% of the users have ``catalog`` event right before ``cart``, 24% of the users have ``product2`` event, and 16% of the users are distributed among 5 events which are folded to an artificial ``THRESHOLDED_5`` event.

See :doc:`StepMatrix user guide<../user_guides/step_matrix>` to understand this tool deeper.

Step Sankey diagram
~~~~~~~~~~~~~~~~~~~

Step Sankey diagram is similar to step matrix. It also shows the event distribution with respect to step number. However, it has some advances:

- it explicitly shows the user flow from one step to another,
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

See :doc:`StepSankey user guide<../user_guides/step_sankey>` to understand this tool deeper.

:red:`Replace an image with a correct one as soon as https://github.com/retentioneering/retentioneering-tools-new-arch/pull/166 is ready`.

.. _quick_start_cluster_analysis:

Cluster analysis
~~~~~~~~~~~~~~~~

.. code-block:: python

    from retentioneering.tooling.clusters import Clusters

    clusters = Clusters(stream)
    clusters.fit(method="kmeans", n_clusters=8, feature_type="tfidf", ngram_range=(1, 2))
    clusters.plot(targets=["payment_done", "cart"])

.. figure:: /_static/getting_started/quick_start/clusters.png
    :width: 900

Users with similar behavior are grouped in the same cluster. Clusters with low conversion rate can indicate a systematic problem in the product: specific behavior pattern which does not lead to product goals. Obtained user segments can be explored deeper to understand problematic behavior pattern. In the example above for instance, cluster 4 has low conversion rate to ``payment_done`` but high conversion rate to ``cart`` visit.

See :doc:`Clusters user guide<../user_guides/clusters>` to understand this tool deeper.

.. _quick_start_funnels:

Funnel analysis
~~~~~~~~~~~~~~~

For much analytical research building a conversion funnel is a basic part. Funnel is a diagram which shows how many users sequentially walk through specific events (funnel stages) in their paths. For each stage event the following values are calculated:

- absolute unique number of the users who reached this stage at least once;
- conversion rate from the first stage (`% of initial`);
- conversion rate from the previous stage (`% of previous`).

.. code-block:: python

    stream.funnel(stages = ['catalog', 'cart', 'payment_done']);


.. raw:: html

    <iframe
        width="700"
        height="400"
        src="../_static/getting_started/quick_start/funnel.html"
        frameborder="0"
        allowfullscreen
    ></iframe>

See :doc:`Funnel user guide<../user_guides/funnel>` to understand this tool deeper.

Cohort analysis
~~~~~~~~~~~~~~~

Cohorts is a powerful tool that shows the differences and the trends in user behavior over time. It helps to isolate the impact of different marketing activities or changes in a product for different groups of users.

Here's an outline of the ``Cohort Matrix`` calculation:

- Users are split into groups (``CohortGroups``) depending on the time of their first appearance in the eventstream;
- The retention rate of the active users is calculated in each period (``CohortPeriod``) of the observation.

.. code-block:: python

    stream.cohorts(
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=False,
        cut_bottom=0,
        cut_right=0,
        cut_diagonal=0
    );

.. figure:: /_static/getting_started/quick_start/cohorts.png
    :width: 500
    :height: 500

See :doc:`Cohorts user guide<../user_guides/cohorts>` to understand this tool deeper.
