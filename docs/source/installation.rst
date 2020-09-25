Getting started
~~~~~~~~~~~~~~~

Python and Jupyter
==================

Firstly, you need to install python and Jupyter.
We support only Python 3.
For quick start better to install `Anaconda <https://www.anaconda.com/>`__.

Retentioneering install
=======================

- You can install our package using pip

    .. code:: bash

        pip3 install retentioneering

- Or directly from source

    .. code:: bash

        git clone https://github.com/retentioneering/retentioneering-tools
        cd retentioneering-tools
        pip3 install .

Getting started
===============

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
          <th>client_id</th>
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
`timestamp` and `client_id` respectively:

.. code:: ipython3

    # update config to specify column names
    retentioneering.config.update({
        'event_col':'event',
        'event_time_col':'timestamp',
        'index_col': 'client_id'
    })


Congradulations! Now complete arsenal of retentioneering tools is ready for use. For example,
you can plot `step_matrix` (read more about step_matrix here):

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh = 0.2,
                          centered={'event':'cart',
                                    'left_gap':5,
                                    'occurrence':1},
                          targets=['payment_done']);