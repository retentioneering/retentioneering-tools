Sequences
=========

|colab| |jupyter|


.. |jupyter| raw:: html

    <a href="../_static/user_guides_notebooks/sequences.ipynb">
    <img src="https://img.shields.io/static/v1?label=Download&message=Jupyter+Notebook&color=%23F37626&logo=jupyter&logoColor=%23F37626"
        alt="Download - Jupyter Notebook">
    </a>

.. |colab| raw:: html

    <a href="https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/docs/source/_static/user_guides_notebooks/sequences.ipynb" target="_blank">
      <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Google Colab"/>
    </a>


Basic example
-------------

Sequences is a tool that displays the frequency of n-grams. n-gram is a term referring to event sequence of length n. For example, a path ``A`` → ``B`` → ``C`` → ``D`` contains two 3-grams: ``A`` → ``B`` → ``C`` and ``B`` → ``C`` → ``D``.

Hereafter we use :doc:`simple_shop </datasets/simple_shop>` dataset, which has already been converted to :doc:`Eventstream<eventstream>` and assigned to ``stream`` variable. If you want to use your own dataset, upload it following :ref:`this instruction<eventstream_creation>`.

.. code-block:: python

    from retentioneering import datasets

    stream = datasets.load_simple_shop()

To run sequences tool, use the :py:meth:`Eventstream.sequences()<retentioneering.eventstream.eventstream.Eventstream.sequences>` method.

.. code-block:: python

    stream.sequences()

.. figure:: /_static/user_guides/sequences/basic_example.png

Let us explore the output. The output is a pandas DataFrame colored with a heatmap. Particular sequences form the DataFrame index. By default, single events are considered as sequences (1-grams). To adjust this behavior use ``ngram_range`` argument.

The columns mostly display metrics that reflect frequency of a particular n-gram. The possible metrics are:

- ``paths``. The number of unique paths that contain a particular event sequence.
- ``paths_share``: The ratio of paths containing a sequence to the total number of paths.
- ``count``: The number of occurrences of a particular sequence (might occur multiple times within a path).
- ``count_share``: The ratio of a particular count to the sum of counts over all sequences.

``sequence_type`` column allows to differentiate important types of sequences: *loops* and *cycles*. A sequence of length >= 2 is a ``loop`` if it consists of a single unique event. A sequence of length >= 3 is a ``cycle`` if its starting and ending event are the same events.

Finally, ``path_id_sample`` column contains samples of random path ids that contain given sequence. They are useful when you need to explore deeper why a particular sequence could occur.

.. note::

    ``paths`` and ``paths_share`` metric names are replaced with the corresponding ``weight_col`` values in the output. Namely, for the default ``weight_col='user_id'`` value, ``user_id`` and ``user_id_share`` are used as the column titles. Also, ``path_id_sample`` is replaced with ``user_id_sample``.

Tuning the arguments
--------------------

Now let us consider another example to demonstrate how the arguments can be tuned. We also use here the :py:meth:`SplitSessions<retentioneering.data_processors_lib.split_sessions.SplitSessions>` data processor in order to split the eventstream into sessions and get additional ``session_id`` column.

.. code-block:: python

    stream\
        .split_sessions(timeout=(30, 'm'))\
        .sequences(
            ngram_range=(2, 3),
            weight_col='session_id',
            metrics=['count', 'count_share', 'paths_share'],
            threshold=['count', 1200],
            sorting=['count_share', False],
            heatmap_cols=['session_id_share'],
            sample_size=3
        )

.. figure:: /_static/user_guides/sequences/tuning_the_arguments.png

To set the range of n-gram length (i.e. n) use ``ngram_range`` argument. This is a very important parameter because it limits the number of all possible n-grams to be discovered. If the upper length is set too high, the number of n-grams might be immense, so it takes much time to compute them all. In practice, it is rarely reasonable to compute all the n-grams of length >= 6-7. So be careful with it.

``weight_col`` sets the eventstream column that contain path identifiers. Similar to :ref:`transition graph<transition_graph_edge_weights>` and :ref:`step matrix<step_matrix_weight_col>`, you can calculate the sequence statistics within a whole path (by ``user_id``) or within its subpaths (for example, by ``session_id``). In this example we switch it to ``weight_col='session_id'``.

``metrics`` parameter defines the metrics to be included in the output columns. The metric names were defined in the previous section.

Since the number of all sequences is often large we usually need to include in the output the most valuable sequences. With the ``threshold`` parameter you can define a column to be used as a filter and the corresponding threshold value. The values above given threshold are included in the output. In the example we define ``threshold=['count', 1200]`` meaning that the filtering column is ``count`` and the threshold value is 1200.

Sorting of the output table is controlled by the ``sorting`` parameter. The heatmap is defined by ``heatmap_cols`` parameter. Note that instead of ``heatmap_cols=['session_id_share']`` we could use ``heatmap_cols=['paths_share']`` which would be an alias in case of ``weight_col='session_id'``.

Finally, the ``sample_size`` parameter defines the length of the list with sampled path_ids.

Comparing groups
----------------

One of the most powerful application of the Sequences tool is comparing sequences frequencies between two groups of users. We will use a random split of the users just for demonstration purposes.

.. code-block:: python

    np.random.seed(111)
    users = set(stream.to_dataframe()['user_id'])
    group1 = set(np.random.choice(list(users), size=len(users)//2))
    group2 = users - group1

.. code-block:: python

    stream.sequences(
        groups=[group1, group2],
        group_names=['A', 'B'],
        metrics=['paths_share', 'count_share'],
        threshold=[('user_id_share', 'delta_abs'), 0],
        sorting=[('count_share', 'delta'), False]
    )

.. figure:: /_static/user_guides/sequences/groups.png

To activate group mode for Sequences, you simply need to set ``groups`` parameter that defines two sets of users to be compared. Optionally, you can define the names of these groups with ``group_names`` parameter so the output columns will be labeled with the corresponding titles.

Metrics columns are designed as follows. Each metric is represented with four columns:

- metric value for the first group (A),
- metric value for the second group (B),
- ``delta_abs``: the metric difference between the second and the first group (B - A),
- ``delta_res``: the relative value of the delta compared to the value for the first group (B - A) / A.

Unlike regular output, Sequences output for groups contains `pandas.MultiIndex <https://pandas.pydata.org/pandas-docs/stable/user_guide/advanced.html>`_ in the columns. So while using ``threshold``, ``sorting``, and ``heatmap_cols`` you need to refer a column as an element of 2-level multiindex.

Common tooling properties
-------------------------

values
~~~~~~

If you want to get the underlying pandas DataFrame you can use property :py:meth:`Sequences.values<retentioneering.tooling.sequences.sequences.Sequences.values>`. An additional flag ``show_plot=False`` supresses the output.

.. code-block:: python

    seq_df = stream.sequences(show_plot=False).values
    seq_df

.. raw:: html

    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>user_id</th>
          <th>user_id_share</th>
          <th>count</th>
          <th>count_share</th>
          <th>sequence_type</th>
          <th>user_id_sample</th>
        </tr>
        <tr>
          <th>Sequence</th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>path_end</th>
          <td>3751</td>
          <td>1.00</td>
          <td>3751</td>
          <td>0.09</td>
          <td>other</td>
          <td>[696492792]</td>
        </tr>
        <tr>
          <th>path_start</th>
          <td>3751</td>
          <td>1.00</td>
          <td>3751</td>
          <td>0.09</td>
          <td>other</td>
          <td>[807066609]</td>
        </tr>
        <tr>
          <th>catalog</th>
          <td>3611</td>
          <td>0.96</td>
          <td>14518</td>
          <td>0.36</td>
          <td>other</td>
          <td>[969637876]</td>
        </tr>
        <tr>
          <th>main</th>
          <td>2385</td>
          <td>0.64</td>
          <td>5635</td>
          <td>0.14</td>
          <td>other</td>
          <td>[274091445]</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>1924</td>
          <td>0.51</td>
          <td>2842</td>
          <td>0.07</td>
          <td>other</td>
          <td>[712986878]</td>
        </tr>
        <tr>
          <th>product2</th>
          <td>1430</td>
          <td>0.38</td>
          <td>2172</td>
          <td>0.05</td>
          <td>other</td>
          <td>[196471324]</td>
        </tr>
        <tr>
          <th>delivery_choice</th>
          <td>1356</td>
          <td>0.36</td>
          <td>1686</td>
          <td>0.04</td>
          <td>other</td>
          <td>[162041520]</td>
        </tr>
        <tr>
          <th>product1</th>
          <td>1122</td>
          <td>0.30</td>
          <td>1515</td>
          <td>0.04</td>
          <td>other</td>
          <td>[368983170]</td>
        </tr>
        <tr>
          <th>payment_choice</th>
          <td>958</td>
          <td>0.26</td>
          <td>1107</td>
          <td>0.03</td>
          <td>other</td>
          <td>[418845606]</td>
        </tr>
        <tr>
          <th>delivery_courier</th>
          <td>748</td>
          <td>0.20</td>
          <td>834</td>
          <td>0.02</td>
          <td>other</td>
          <td>[397948421]</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>653</td>
          <td>0.17</td>
          <td>706</td>
          <td>0.02</td>
          <td>other</td>
          <td>[827859068]</td>
        </tr>
        <tr>
          <th>payment_card</th>
          <td>521</td>
          <td>0.14</td>
          <td>565</td>
          <td>0.01</td>
          <td>other</td>
          <td>[204780950]</td>
        </tr>
        <tr>
          <th>delivery_pickup</th>
          <td>469</td>
          <td>0.13</td>
          <td>506</td>
          <td>0.01</td>
          <td>other</td>
          <td>[470581033]</td>
        </tr>
        <tr>
          <th>payment_cash</th>
          <td>190</td>
          <td>0.05</td>
          <td>197</td>
          <td>0.00</td>
          <td>other</td>
          <td>[766327250]</td>
        </tr>
      </tbody>
    </table>
