Getting started
~~~~~~~~~~~~~~~

Basic rete run
==============

Retentioneering works as Pandas Dataframe accessor, meaning that if you work with
your users logs data using Pandas Dataframe you can apply retentioneering right after
import! All you need is import retentioneering, import sample dataframe (or use you
own!):

.. code:: ipython3

    import retentioneering

    # load sample data
    from retentioneering import datasets
    data = datasets.load_simple_shop()

Here ``data`` is a regular Pandas Dataframe:

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
Last step is to simply specify columns names. Retentioneering module needs to know what columns
in your dataset correspond to event names, timestamps, and user_ids. In our case it's `event`,
`timestamp` and `user_id` respectively:

.. code:: ipython3

    # update config to specify column names
    retentioneering.config.update({
        'event_col':'event',
        'event_time_col':'timestamp',
        'index_col': 'user_id'
    })


Congradulations! Now complete arsenal of retentioneering tools is ready for use. For example,
you can plot graph (read more about plot_graph here):

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

You can also plot step_matrix (read more about step_matrix here):

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh = 0.2,
                          centered={'event':'cart',
                                    'left_gap':5,
                                    'occurrence':1},
                          targets=['payment_done']);

.. image:: _static/step_matrix/step_matrix_8.svg

or you can explore what type of behavior cluster are present in your dataset
(read more about exploring behavior clusters here):

.. code:: ipython3

    data.rete.get_clusters(method='kmeans',
                           n_clusters=8,
                           ngram_range=(1,2),
                           plot_type='cluster_bar',
                           targets=['payment_done','cart']);

.. image:: _static/clustering/clustering_2.svg


