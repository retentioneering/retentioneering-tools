.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red

Quick start with Retentioneering
================================

Retentioneering makes product analytics very easy once you have the raw data.

Every user action and every visited page or screen from your website or app, all these interactions we call events. To understand deeply how different types of user behavior in your product affects your business metrics, you need to analyze the sequences of events for each user which are called user paths or trajectories.

1. Load data
------------

Retentioneering stores data in its core class ``Eventstream`` which allow to treat a clickstream in an efficient way. To create an eventstream you can pick from any of two options:

**Option 1. To start with our dummy online shop dataset sample.**


.. code-block:: python

    from retentioneering import datasets

    # load sample user behavior data:
    stream = datasets.load_simple_shop()

In the shell of eventstream object there is a regular pandas dataframe:

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

|

As you can see in this fragment of example dataset, user with id ``219483890`` has 4 events on the website with specific timestamps on ``2019-11-01``. This is all you need to try out what Retentioneering is about. You are ready to go with this dataset and proceed to step 2.

**Option 2. Alternatively, you can start with your own dataset.**

If you have your raw data of user behavior for example in csv format simply upload it as pandas dataframe:

.. code-block:: python

    import pandas as pd
    from retentioneering.eventstream import Eventstream

    # load your own csv
    data = pd.read_csv("yourowndatafile.csv")
    stream = Eventstream(data)

We assume that the data contains at least three columns: ``user_id``, ``event``, ``timestamp``. If your columns have another names, you need either to rename them in the pandas dataframe or explicitly set data schema. :red:`Give a link.`.

How to get a csv file with data? Raw data in the form of {user,event,timestamp} can be streamed via Google Analytics 360 or free Google Analytics App+Web into BigQuery. From the BigQuery console you can run SQL query and export data into csv file, alternatively you can use the Python BigQuery connector to get directly into the dataframe. If you have big datasets, we suggest you take fraction of users in SQL query, filtering by the user id (just add this condition to SQL WHERE statement to get 10% of your users : ``and ABS(MOD(FARM_FINGERPRINT(fullVisitorId), 10)) = 0)``.


2. Prepare the data
-------------------
Raw data is often needed to be prepared before applying analytical techniques. Retentioneering provides a wide range of preprocessing tools which should become a Swiss knife for a product analytics. We call them *data processors*. With a help of data processors a product analyst easily add, delete or group events, truncate a clickstream in a flexible manner, split the trajectories into sessions, and many more.

Suppose you want to analyze only user first sessions instead of the whole clickstream. Here's how you can easily achieve this using the developed data processors:

.. code-block:: python

    # eventstream preprocessing
    stream \
        .split_sessions(session_cutoff=(30, "m")) \
        .filter_events(func=lambda df_: df_["session_id"].endswith("_1")) \
        .to_dataframe()

``split_sessions`` method creates a new column ``session_id`` with the values ending with suffix ``_<int>`` indicating the ordinary number of each user's session. Thus, we need to leave only those records where `session_id` ends with ``_1``. This is exactly what the next method ``filter_events`` does. Finally, we convert the output eventstream to a pandas dataframe.

Also, for more complex preprocessing scenarios Retentioneering offers a great graphical tool which allows you to represent sheets of preprocessing code as a neat calculation graph. See :red:`set the link` for the details.


3. Explore the data
-------------------

Retentioneering has many powerful tools for exploring users behavior, including transition graphs, step matrices, step Sankey diagrams, and cluster analysis. Below we show just a short demo. For more details please visit the documentation page :red:`set the link`.

Transition graph
~~~~~~~~~~~~~~~~

:red:`Set a user guide link`

:red:`Update the actual parameters in the future`

:red:`Insert an interactive html as soon as html export is ready`

.. code-block:: python

    stream.transition_graph(
        thresholds={
            'nodes': {'number_of_events': 0.06},
            'edges': {'number_of_events' : 0.06}
        },
        norm_type=None,
        targets={
            "lost": "bad",
            "payment_done": "nice",
            "main": "source"
        }
    )

.. raw:: html

    <iframe
        width="600"
        height="600"
        src="../_static/quick_start/transition_graph.html"
        frameborder="0"
        allowfullscreen
    ></iframe>


:red:`The graph on the image above is not interactive so far`
The ``Transition graph`` represents CJM as Markov random walk model and shows how often the users jumps from one event to another. The graph is interactive and you can move the graph nodes by clicking them, zoom-in/zoom-out the graph layout, etc. Also, you can highlight the most valuable nodes and hide noisy nodes and edges.

Step matrix
~~~~~~~~~~~
:red:`Set a user guide link`

Step matrix provides a step-wise look at CJM. It shows the event distribution with respect to a step ordinal number.

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
    )

:red:`Figure out what's going on with path_end PLAT-342`

.. figure:: /_static/quick_start/step_matrix.png
    :width: 900

    Fig. XXX. Step matrix

Step Sankey diagram
~~~~~~~~~~~~~~~~~~~
:red:`Set a user guide link`

Step Sankey diagram is similar to step matrix, but it has some advances:
- it explicitly shows the user flow,
- it is interactive.

.. code-block:: python

    stream.step_sankey(max_steps=10, thresh=0.05)

.. raw:: html

    <iframe
        width="900"
        height="500"
        src="../_static/quick_start/step_sankey.html"
        frameborder="0"
        allowfullscreen
    ></iframe>

Cluster analysis
~~~~~~~~~~~~~~~~
:red:`Set a user guide link`

.. code-block:: python

    from src.tooling.clusters import Clusters

    clusters = Clusters(stream)
    clusters.fit(method="kmeans", n_clusters=8, feature_type="tfidf", ngram_range=(1, 2))
    clusters.plot(targets=["payment_done", "cart"])

.. figure:: /_static/quick_start/clusters.png
    :width: 900

Users with similar behavior grouped in the same cluster. Clusters with low conversion rate can represent systematic problem in the product: specific behavior pattern which does not lead to product goals. Obtained user segments can be explored deeper to understand problematic behavior pattern. In the example above for instance, cluster 4 has low conversion rate to purchase but high conversion rate to cart visit.
