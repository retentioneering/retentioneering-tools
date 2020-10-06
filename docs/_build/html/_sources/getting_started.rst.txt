Quick start with Retentioneering
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Retentioneering makes product analytics very easy once you have the raw data.

Every user action and every visited page or screen from your website or app,
all these interactions we call events. To understand deeply how different types
of user behavior in your product affects your business metrics, you need to
analyze the sequences of events for each user.

1. Load data
============

To start you can pick from any of two options:

**Option 1. To start with our dummy online shop dataset sample.**


.. code:: ipython3

    import retentioneering

    # load sample user behavior data as a pandas dataframe:
    data = retentioneering.datasets.load_simple_shop()

Here ``data`` is a regular Pandas Dataframe with clickstream example:

.. code:: ipython3

    data.head()

.. raw:: html

    <div>
    <style scoped>
        .dataframe tbody tr th:only-of-type {
            vertical-align: middle;
        }

        .dataframe tbody tr th {
            vertical-align: top;
        }

        .dataframe thead th {
            text-align: right;
        }
    </style>
    <table border="1" class="dataframe">
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

As you can see in this fragment of example dataset, user with id 219483890 has 4 events
on the website with specific timestamps on 2019-11-01. This is all you need to try out
what Retentioneering is about. You are ready to go with this dataset and proceed to step 2.

**Option 2. Alternatively, you can start with your dataset.**

If you have your raw data of user behavior for example in csv format simply import
it as pandas dataframe:

.. code:: ipython3

    import retentioneering
    import pandas as pd

    # load your own csv
    data = pd.read_csv('yourowndatafile.csv')


How to get a csv file with data? Raw data in the form of {user,event,timestamp} can
be streamed via Google Analytics 360 or free Google Analytics App+Web into BigQuery.
From the BigQuery console you can run SQL query and export data into csv file,
alternatively you can use the Python BigQuery connector to get directly into the dataframe.
If you have big datasets, we suggest you take fraction of users in SQL query,
filtering by the user id (just add this condition to SQL WHERE statement to get 10%
of your users : “and ABS(MOD(FARM_FINGERPRINT(fullVisitorId), 10)) = 0)”.


2. Explore the data
===================

Next step is to simply specify columns names, so that Rete will know how your
own data matches the conventional dataset of user_ids, event names, timestamps.
This is defined by this global config dictionary which will be used by Rete functions:

.. code:: ipython3

    # update config to pass columns names:
    retentioneering.config.update({
        'user_col': 'user_id',
        'event_col':'event',
        'event_time_col':'timestamp',
    })


Now we are ready to explore the user behavior in our data. For example,
you can plot graph (read more about plot_graph function
`here <https://retentioneering.github.io/retentioneering-tools/_build/html/plot_graph.html>`__):

.. code:: ipython3

    data.rete.plot_graph(norm_type='full',
                         weight_col='user_id',
                         thresh=0.06,
                         targets = {'payment_done':'green',
                                    'lost':'red'})

.. raw:: html


            <iframe
                width="700"
                height="600"
                src="_static/plot_graph/index_3.html"
                frameborder="0"
                allowfullscreen
            ></iframe>
|

Note, that graph is interactive and you can move graph nodes by
clicking on it and interactively zoom-in / zoom-out the graph layout.

You can also plot step_matrix (read more about step_matrix function
`here <https://retentioneering.github.io/retentioneering-tools/_build/html/step_matrix.html>`__):

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh = 0.2,
                          centered={'event':'cart',
                                    'left_gap':5,
                                    'occurrence':1},
                          targets=['payment_done']);

.. image:: _static/step_matrix/step_matrix_8.svg

or you can explore what types of behavior clusters are present in your dataset
(read more about exploring behavior clusters
`here <https://retentioneering.github.io/retentioneering-tools/_build/html/clustering.html>`__):

.. code:: ipython3

    data.rete.get_clusters(method='kmeans',
                           n_clusters=8,
                           ngram_range=(1,2),
                           plot_type='cluster_bar',
                           targets=['payment_done','cart']);

.. image:: _static/clustering/clustering_2.svg

Users with similar behavior grouped in the same cluster. Clusters with low conversion
rate can represent systematic problem in the product: specific behavior pattern which
does not lead to product goals. Obtained user segments can be explored deeper to
understand problematic behavior pattern. In the example above for instance,
cluster 4 has low conversion rate to purchase but high conversion rate to cart visit.