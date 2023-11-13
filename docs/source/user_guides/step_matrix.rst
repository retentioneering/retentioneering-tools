Step matrix
===========

|colab| |jupyter|


.. |jupyter| raw:: html

    <a href="../_static/user_guides_notebooks/step_matrix.ipynb">
    <img src="https://img.shields.io/static/v1?label=Download&message=Jupyter+Notebook&color=%23F37626&logo=jupyter&logoColor=%23F37626"
        alt="Download - Jupyter Notebook">
    </a>

.. |colab| raw:: html

    <a href="https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/docs/source/_static/user_guides_notebooks/step_matrix.ipynb" target="_blank">
      <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Google Colab"/>
    </a>

Step matrix definition
----------------------

Step matrix is a powerful tool in the Retentioneering arsenal. It allows and getting a quick high-level understanding of user behavior. The step matrix features powerful customization options to tailor the output depending on the goal of the analysis.

To better understand how the step matrix works, let us first consider an intuitive example. Assume we have an eventstream as follows:

.. figure:: /_static/user_guides/step_matrix/step_matrix_demo.png


We can visualize this dataset as a step-wise heatmap, indicating the distribution of the events appeared at a specific step:

.. code-block:: python

    import pandas as pd
    from retentioneering.eventstream import Eventstream

.. code-block:: python

    simple_example = pd.DataFrame(
        [
            ['user1', 'main', 0],
            ['user2', 'main', 0],
            ['user3', 'main', 0],
            ['user4', 'catalog', 0],
            ['user1', 'catalog', 1],
            ['user3', 'catalog', 1],
            ['user4', 'product', 1],
            ['user1', 'product', 2],
            ['user3', 'catalog', 2],
            ['user4', 'main', 2],
            ['user1', 'cart', 3],
            ['user3', 'product', 3],
            ['user4', 'catalog', 3],
            ['user1', 'catalog', 5],
            ['user3', 'cart', 5],
            ['user3', 'order', 6]
        ],
        columns=['user_id', 'event', 'timestamp']
    )


    Eventstream(simple_example)\
        .step_matrix(max_steps=7, threshold=0);

.. figure:: /_static/user_guides/step_matrix/output_6_1.png


The matrix rows correspond to the unique events, and the columns correspond to the steps in the user
trajectories. That is, ``(i, j)`` matrix element shows the share of the users with event ``i`` appeared at step ``j``.

Hereafter we use :doc:`simple_shop </datasets/simple_shop>` dataset, which has already been converted to :doc:`Eventstream<eventstream>` and assigned to ``stream`` variable. If you want to use your own dataset, upload it following :ref:`this instruction<eventstream_creation>`.


.. code-block:: python

    from retentioneering import datasets

    stream = datasets.load_simple_shop()

Basic example
-------------

The primary way to visualize a step matrix is to call :py:meth:`Eventstream.step_matrix()<retentioneering.eventstream.eventstream.Eventstream.step_matrix>` method. Here is how it is applied to ``simple_shop`` eventstream:

.. code-block:: python

    stream.step_matrix(max_steps=12, threshold=0)

.. figure:: /_static/user_guides/step_matrix/output_12_2.png

As we can see, the sum of the values in the matrix columns is 1 (i.e. at each step). Looking at the first column we can say that the users start their sessions from events ``catalog`` (72%) and ``main`` (28%). Also, we notice that ``payment_done`` event, which might be considered as an event of interest, appears in the trajectories no earlier than at the 7th step (row ``payment_done`` has zeros until step 7).

.. _transition_matrix_terminating_event:

Terminating event
-----------------

As you may have noticed, the step matrix above has ``ENDED`` event which is located in the last row, whereas this event is not represented in the ``simple_shop`` eventstream. ``ENDED`` is a special synthetic event that explicitly indicates a trajectory’s end. If a user’s path is shorter than ``max_steps`` parameter, then ``ENDED`` event is padded to the path so that it becomes exactly of length ``max_steps``. With this behavior, the sum of the user fractions over each column (i.e each step) is exactly 1. Essentially, ``ENDED`` row represents the cumulative share of leaving users. The event exists in scope of step matrix only, so that it does not affect the sourcing eventstream at all.

.. _transition_matrix_collapsing_events:

Collapsing rare events
----------------------

In a typical scenario, it can be useful to hide rare events in a step matrix, not removing them from the step matrix calculation. If we remove them, the matrix values will be distorted. This behaviour is controlled by the ``threshold`` argument. An event is considered as rare if its maximum frequency over all the steps is less than ``threshold``. All such rare events are not removed from the matrix, but instead collapsed to ``thresholded_N`` artificial event, where ``N`` stands for the number of the collapsed events. The ``thresholded_N`` event appears in step matrix only, and is not added to the sourcing eventstream.

Let us look how the events are collapsed if we set ``threshold=0.05``, and compare the result with the previous step matrix (which had ``threshold=0`` parameter).

.. code-block:: python

    stream.step_matrix(max_steps=16, threshold=0.05)

.. figure:: /_static/user_guides/step_matrix/output_16_1.png


Now, we see that all 6 rare events are hidden and grouped together in the ``THRESHOLDED_6`` row. We also notice that ``THRESHOLDED_6`` event contains ``delivery_courier``, ``delivery_pickup``, ``payment_cash``, ``payment_card``, ``payment_done``, and ``payment_choice`` events. Let us check why, say, the ``payment_choice`` event has been collapsed. In the previous step matrix we see that at step 5 this event contains 3% of the users, 4% at step 6, and 3% at step 7, etc. Since the maximum value (4%) is less than
``threshold=0.05``, the event is collapsed.

Note that the number ``_6`` in ``THRESHOLDED_6`` event name contains no information on specific steps. For example, from the matrix with ``threshold=0`` we see that at step 4 only one event among these 6 is represented (``delivery_courier``), so it is the only event that is collapsed at this step. On the other hand, at step 5 ``delivery_pickup`` and ``payment_choice`` appear, so they are collapsed to the ``THRESHOLDED_6`` event. Finally, at step 7, all these 6 events are collapsed.

You can use the ``target`` parameter if you want to prevent some events from the collapsing.

Target events analysis
----------------------

It is common that some events are more important than the others, so we want to pay attention to them.
This includes such events as adding an item to the cart, order confirmation, payment, etc. Such events often have much lower occurrence rate compared to other events (like visiting main page or catalog). As a result, they are collapsed to the ``THRESHOLDED_N`` event. Even if they are not, it would be worth highlighting them in the diagram at putting them in separate place. This can be done with the ``targets`` parameter:

.. code-block:: python

    stream.step_matrix(
        max_steps=16,
        threshold=0.05,
        targets=['payment_done']
    )

.. figure:: /_static/user_guides/step_matrix/output_20_2.png

Specified target events are always shown at the bottom of step matrix regardless of the selected threshold. As we have chosen the ``payment_done`` event as a target, the row with ``payment_done`` has been moved at the bottom of the matrix and now has its own color palette.

Multiple targets are also supported:

.. code-block:: python

    stream.step_matrix(
        max_steps=16,
        threshold=0.05,
        targets=['product1', 'cart', 'payment_done']
    )

.. figure:: /_static/user_guides/step_matrix/output_22_2.png

Now we have selected three target events: ``product1``, ``cart``, ``payment_done``, so we can see them at the bottom of the diagram. Each of them has its own palette and color scaling.

If we want to compare some target events and plot them using the same color scaling, we can combine them in a sub-list inside the targets list:

.. code-block:: python

    stream.step_matrix(
        max_steps=16,
        threshold=0.05,
        targets=['product1', ['cart', 'payment_done']]
    )

.. figure:: /_static/user_guides/step_matrix/output_25_2.png

With the colors defined in this way, we can compare how many users reached ``cart`` vs ``payment_done`` at particular step in their trajectories.

Targets can be presented as accumulated values with ``accumulated`` parameter. Meaning that, we can display the cumulative shares of the users having this event at each step. The corresponding row names start with ``ACC_`` prefix. There are two options for displaying these rows:

1. ``accumulated='only'`` display rows with accumulated values only;
2. ``accumulated='both'`` display rows with both accumulated and not accumulated values.

The step matrix below demonstrates ``accumulated='only'`` option:

.. code-block:: python

    stream.step_matrix(
        max_steps=16,
        threshold=0.05,
        targets=['product1', ['cart', 'payment_done']],
        accumulated='only'
    )

.. figure:: /_static/user_guides/step_matrix/output_28_1.png

In comparison with the previous step matrix, at the bottom we see three rows ``ACC_product1``, ``ACC_cart``, ``ACC_payment_done`` instead of ``product1``, ``cart``, and ``payment_done``. Now, let us show how ``accumulated='both'`` option works.

.. code-block:: python

    stream.step_matrix(
        max_steps=16,
        threshold=0.05,
        targets=['product1', ['cart', 'payment_done']],
        accumulated='both'
    )

.. figure:: /_static/user_guides/step_matrix/output_29_2.png

Above, we see two target blocks: one is with accumulated values, another one with the original values.

Centered step matrix
--------------------

Sometimes we are interested in the flow of users through a specific event to answer such questions as how do users reach a specific event and what do they do afterwards? This information can be visualized with the ``centered`` parameter:

.. code-block:: python

    stream.step_matrix(
        max_steps=16,
        threshold=0.2,
        centered={
            'event': 'cart',
            'left_gap': 5,
            'occurrence': 1
        }
    )

.. figure:: /_static/user_guides/step_matrix/output_32_2.png

The ``centered`` parameter is a dictionary that requires three keys:

-  ``event``: name of the event we focus on. Reaching this event is associated with step 0. Negative step numbers correspond to the events occurred before the selected event. Positive step numbers correspond to the events occurred after the selected event;

-  ``left_gap``: integer number that indicates how many steps before the centered event we want to show in the step matrix;

-  ``occurrence``: the occurrence number of the target event to trigger the ``centered`` parameter. For example, in the coding example above, all the trajectories will be aligned to have the first ``cart`` occurrence as step 0.

Importantly, when the ``centered`` parameter is used, only the users who have ``centered['event']`` occurred at list ``centered['occurrence']`` times are considered. The share of such users with respect to all the users from the eventstream is represented in the diagram's title. In the example above, 51.3% of the users reached the event ``cart`` at least once.

Another property of step matrix is that at step 0 column we always have zeros at any row except the row that relates to the centering event: at that row there is always 1.

.. figure:: /_static/user_guides/step_matrix/SM_occurence=1.png

To better understand the meaning of the ``occurrence`` parameter, let us calculate another step matrix. This time with ``occurrence=2``:

.. code-block:: python

    stream.step_matrix(
        max_steps=16,
        threshold=0.2,
        centered={
            'event': 'cart',
            'left_gap': 5,
            'occurrence': 2
        }
    )

.. figure:: /_static/user_guides/step_matrix/output_36_2.png

Here we can see that the proportion of the users whose steps are considered in our matrix has noticeably decreased. Now it is 15.2%, because we are evaluating the second occurrence of the ``cart`` event, which
means we are considering the users who had this event at least twice.

A combination of ``targets`` and ``centered`` parameters is also possible:

.. code-block:: python

    stream.step_matrix(
        max_steps=16,
        threshold=0.2,
        centered={
            'event': 'cart',
            'left_gap': 5,
            'occurrence': 1
        },
        targets=['payment_done']
    )

.. figure:: /_static/user_guides/step_matrix/output_39_2.png

From the step matrix above, we see that the maximum in the target row appear at step 5 (with the value of 0.22). We can interpret this as follows: if a user reaches the ``cart`` event and makes a purchase afterwards, it is likely that it took them 5 steps.

Events sorting
--------------

By default, rows of the step matrix are sorted in the following order:

1. Original events by the order of their first appearance in the eventstream;
2. ``ENDED`` event;
3. ``THRESHOLDED`` events;
4. target events.

Sometimes, it is needed to obtain a step matrix with events ranked in a specific order - for example, when you compare two step matrices. This can be done with the ``sorting`` parameter that accepts a list of event names in the required order to show up in the step matrix. Here is an example:

.. code-block:: python

    stream.step_matrix(max_steps=16, threshold=0.07)

.. figure:: /_static/user_guides/step_matrix/output_43_2.png

We pass the following list ofr the events to the ``sorting`` parameter:

.. code-block:: python

    custom_order = [
        'path_start',
        'main',
        'catalog',
        'product1',
        'product2',
        'cart',
        'THRESHOLDED_7',
        'ENDED'
    ]

    stream.step_matrix(
        max_steps=16,
        threshold=0.07,
        sorting=custom_order
    )

.. figure:: /_static/user_guides/step_matrix/output_47_2.png

.. note::

    It is convenient to modify the order of the event list with the help of :py:meth:`StepMatrix.values<retentioneering.tooling.step_matrix.step_matrix.StepMatrix.values>` property. See :ref:`here <step_matrix_values_property>` for the details.

.. note::

    The custom ordering affects non-target events only. Target events are always located at the bottom, and they are sorted in same order as they are specified in the ``targets`` parameter.

.. _step_matrix_differential:

Differential step matrix
------------------------

Definition and general usage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes we would like to compare behaviors of multiple groups of users - for example, the users who had a target event versus those who had not, or test and control groups in an A/B test.

Suppose we have two abstract groups of the users: ``g1`` and ``g2``. Let ``g1`` consists of the users who had the ``payment_done`` event, and ``g2`` - who had not. Suppose also that ``M1`` and ``M2`` are the step matrices that calculated for the groups ``g1`` and ``g2`` correspondingly. So we want to compare behaviours of the users from ``g1`` and ``g2`` groups. In this case, it is reasonable to calculate a new step matrix as difference between ``M1`` and ``M2``.

``groups`` parameter is responsible for differential step matrix plotting. It requires a collection of two user lists related to two user groups. Each list should be represented as a collection of user ids.

In the example below we demonstrate how the ``groups`` parameter works. We also choose ``cart`` as a central event, because usually it is closely followed by a purchase or user disappearance.

.. code-block:: python

    stream_df = stream.to_dataframe()

    g1 = set(stream_df[stream_df['event'] == 'payment_done']['user_id'])
    g2 = set(stream_df['user_id']) - g1

    stream.step_matrix(
        max_steps=16,
        threshold=0.05,
        centered={
            'event': 'cart',
            'left_gap': 5,
            'occurrence': 1
        },
        groups=(g1, g2)
    )

.. figure:: /_static/user_guides/step_matrix/output_51_2.png

According to the step matrix definition, the values that are close to 0 mean that the corresponding values in the original matrices ``M1`` and ``M2`` are roughly equal. Large positive/negative value indicates that the corresponding value in ``M1`` matrix is much greater/less than the corresponding number in ``M2`` matrix. As a result, the step matrix heatmap highlights the cells where the difference is big.

For example, from the step matrix above we see that the values to the left from the central event ``cart`` are close to zero. It means that the behavior of users in the two groups is roughly the same. However, to the right of the ``cart`` event large positive and negative values appear. The positive values relate to such events as ``payment_done``, ``payment_choice``, or ``payment_choice``. The users from ``g2`` must have no ``payment_done`` event at all due to the group definition. As for the last two mentioned events, they relate to the payment process, so it is not a surprise that the users from the ``g2`` experiences these events group less often than the users from ``g1`` group.

Note that the values in each column of a differential step matrix are always sum up to 0, since the columns in both ``M1`` and ``M2`` matrices always sum up to 1. That is its fundamental property.

Cluster analysis
~~~~~~~~~~~~~~~~

Consider another example of differential step matrix usage. Now we will compare behaviors within two user clusters which are obtained by applying the :doc:`Clusters </user_guides/clusters>` tool. As before, we focus the analysis on ``payment_done`` and event ``cart`` events.

.. code-block:: python

    from retentioneering.tooling.clusters import Clusters

    clusters = Clusters(eventstream=stream)
    features = clusters.extract_features(feature_type='count', ngram_range=(1, 1))
    clusters.fit(method='kmeans', n_clusters=8, X=features, random_state=42)
    clusters.plot(targets=['payment_done', 'cart']);

.. figure:: /_static/user_guides/step_matrix/output_57_0.png

So we have defined 8 clusters. The diagram above shows :ref:`the distribution of the conversion rate to the target events <clusters_plot>` (``payment_done`` and ``cart``) among the clusters. Suppose we are interested in how clusters #1 and #3 differ.

All we need is to get ``user_id`` collections from the :ref:`cluster_mapping <clusters_clustering_results>` attribute and pass it to the ``groups`` parameter of step matrix:

.. code-block:: python

    g1 = clusters.cluster_mapping[1]
    g2 = clusters.cluster_mapping[4]

    stream.step_matrix(
        max_steps=16,
        threshold=0.05,
        centered={
            'event': 'cart',
            'left_gap': 5,
            'occurrence': 1
        },
        groups=(g1,g2)
    )

.. figure:: /_static/user_guides/step_matrix/output_59_1.png

The differential step matrix shows the difference between clusters #1 and #4. Users from cluster #1, after adding a product to the cart tend to return to the catalog and continue shopping more often or, on the opposite, finish their trajectory. On the other hand, users from cluster #4 tend to fall into payment flow and eventually make purchase. They can also return to the catalog, but in later steps.

Weighting step matrix values
----------------------------

So far, we have been defining step matrix values as the shares of users appearing in an eventstream at a certain step. However, sometimes it is reasonable to calculate similar fractions not over users, but over some other entities as well - typically, over user sessions.

To demonstrate how to do this, we need to split the eventstream into the sessions at first with the help of :py:meth:`SplitSessions data processor <retentioneering.data_processors_lib.split_sessions.SplitSessions>`. Let session timeout be 30 minutes.

.. code-block:: python

    stream_with_sessions = stream.split_sessions((30, 'm'))

Step matrix shares the same mechanism of weighting that is used in :ref:`transition graph <transition_graph_weights>`. ``weight_col`` parameter accepts a name of the weighting column in the eventstream. In our case, we pass ``session_id`` value.

.. code-block:: python

    stream_with_sessions.step_matrix(max_steps=16, weight_col='session_id', threshold=0)

.. figure:: /_static/user_guides/step_matrix/output_69_2.png

For example, ``cart`` value at step 3 is 0.05 which means that at step 3 only 5% of the sessions had ``cart`` event.

Let us compare the result with the user-weighted matrix:

.. code-block:: python

    stream_with_sessions.step_matrix(max_steps=16, weight_col='user_id', threshold=0))

.. figure:: /_static/user_guides/step_matrix/output_72_2.png


Now, we can see the difference between these two types of weighting. The number of unique sessions is greater than the number of unique users, so the proportion of the ``cart`` event at the third step when
normalizing by users is higher than for sessions (0.09 vs 0.05).

Using a separate instance
-------------------------

By design, :py:meth:`Eventstream.step_matrix()<retentioneering.eventstream.eventstream.Eventstream.step_matrix>` is a shortcut method that uses :py:meth:`StepMatrix<retentioneering.step_matrix.step_matrix.StepMatrix>` class under the hood. This method creates an instance of StepMatrix class and embeds it into the eventstream object. Eventually, ``Eventstream.step_matrix()`` returns exactly this instance.

Sometimes it is reasonable to work with a separate instance of StepMatrix class. An alternative way to get the same visualization that ``Eventstream.step_matrix()`` produces is to call :py:meth:`StepMatrix.fit()<retentioneering.step_matrix.step_matrix.StepMatrix.fit>` and :py:meth:`StepMatrix.plot()<retentioneering.step_matrix.step_matrix.StepMatrix.plot>` methods explicitly.

Here is an example how you can manage it:

.. code-block:: python

    from retentioneering.tooling.step_matrix import StepMatrix

    step_matrix = StepMatrix(stream)
    step_matrix.fit(max_steps=12, targets=['payment_done'], threshold=0)
    step_matrix.plot()

.. figure:: /_static/user_guides/step_matrix/output_75_0.png


Common tooling properties
-------------------------

.. _step_matrix_values_property:

values
~~~~~~

:py:meth:`StepMatrix.values<retentioneering.tooling.step_matrix.step_matrix.StepMatrix.values>` property returns the values underlying recent ``StepMatrix.plot()`` call. The property is common for many retentioneering tools. It allows you to avoid unnecessary calculations if the tool object has already been fitted.

Two pandas.DataFrame objects are returned: one for the step matrix, another one for the additional targets block.

.. code-block:: python

    stream.step_matrix(
        max_steps=6,
        targets=['product1', ['cart', 'payment_done']],
        threshold=0,
        show_plot=False
    ).values[0]

.. raw:: html

    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>1</th>
          <th>2</th>
          <th>3</th>
          <th>4</th>
          <th>5</th>
          <th>6</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>catalog</th>
          <td>0.716076</td>
          <td>0.445215</td>
          <td>0.384164</td>
          <td>0.310051</td>
          <td>0.251400</td>
          <td>0.211677</td>
        </tr>
        <tr>
          <th>main</th>
          <td>0.283924</td>
          <td>0.162357</td>
          <td>0.121834</td>
          <td>0.094108</td>
          <td>0.085311</td>
          <td>0.079712</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>0.000000</td>
          <td>0.089843</td>
          <td>0.109571</td>
          <td>0.080778</td>
          <td>0.064783</td>
          <td>0.047454</td>
        </tr>
        <tr>
          <th>delivery_choice</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.054119</td>
          <td>0.061584</td>
          <td>0.049054</td>
          <td>0.034391</td>
        </tr>
        <tr>
          <th>payment_choice</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.033591</td>
          <td>0.043455</td>
        </tr>
        <tr>
          <th>product1</th>
          <td>0.000000</td>
          <td>0.070115</td>
          <td>0.045055</td>
          <td>0.042655</td>
          <td>0.031991</td>
          <td>0.025860</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.003999</td>
        </tr>
        <tr>
          <th>payment_card</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.017595</td>
        </tr>
        <tr>
          <th>delivery_pickup</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.014396</td>
          <td>0.016796</td>
          <td>0.015463</td>
        </tr>
        <tr>
          <th>delivery_courier</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.025327</td>
          <td>0.032791</td>
          <td>0.024793</td>
        </tr>
        <tr>
          <th>payment_cash</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.004799</td>
        </tr>
        <tr>
          <th>product2</th>
          <td>0.000000</td>
          <td>0.114370</td>
          <td>0.065849</td>
          <td>0.057851</td>
          <td>0.045854</td>
          <td>0.035724</td>
        </tr>
        <tr>
          <th>ENDED</th>
          <td>0.000000</td>
          <td>0.118102</td>
          <td>0.219408</td>
          <td>0.313250</td>
          <td>0.388430</td>
          <td>0.455079</td>
        </tr>
      </tbody>
    </table>


.. code-block:: python

    stream.step_matrix(
        max_steps=6,
        targets=['product1', ['cart', 'payment_done']],
        threshold=0,
        show_plot=False
    ).values[1]



.. raw:: html

    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>1</th>
          <th>2</th>
          <th>3</th>
          <th>4</th>
          <th>5</th>
          <th>6</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>product1</th>
          <td>0.0</td>
          <td>0.070115</td>
          <td>0.045055</td>
          <td>0.042655</td>
          <td>0.031991</td>
          <td>0.025860</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>0.0</td>
          <td>0.089843</td>
          <td>0.109571</td>
          <td>0.080778</td>
          <td>0.064783</td>
          <td>0.047454</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>0.0</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.003999</td>
        </tr>
      </tbody>
    </table>

params
~~~~~~

:py:meth:`StepMatrix.params<retentioneering.tooling.step_matrix.step_matrix.StepMatrix.params>` property returns the StepMatrix parameters that was used in the last ``StepMatrix.fit()`` call.

.. code-block:: python

    stream.step_matrix(
        max_steps=6,
        targets=['product1', ['cart', 'payment_done']],
        threshold=0,
        show_plot=False
    ).params


.. parsed-literal::

    {'max_steps': 6,
     'weight_col': 'user_id',
     'precision': 2,
     'targets': ['product1', ['cart', 'payment_done']],
     'accumulated': None,
     'sorting': None,
     'threshold': 0,
     'centered': None,
     'groups': None}
