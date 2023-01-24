.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red


DataProcessor user guide
========================

The following user guide is also available as
`Google Colab <https://colab.research.google.com/drive/1uXTt14stXKjWR_paEzqPl5_rZLFyclrm?usp=share_link>`_


Creating an eventstream
-----------------------

Here we use ``simple_shop`` dataset, which has already converted to
``Eventstream``. If you want to know more about ``Eventstream`` and how
to use it, please study our :doc:`eventstream guide<eventstream>`.


.. code-block:: python

    import pandas as pd
    from retentioneering import datasets

    stream = datasets.load_simple_shop()

What is DataProcessor?
----------------------

Each ``Data Processor`` is the determined algorithm on how eventstream
data will be modified.

The data processors are designed to be nodes of a
``Preprocessing graph`` which allows apply data processors sequentially
according to some logic.

To get overview of the eventstream concept see :doc:`this guide<../getting_started/eventstream_concept>`.

To understand deeper the concept of eventstream and what the
preprocessing graph is, please study our guides:

- :doc:`eventstream guide<eventstream>`
- :doc:`preprocessing guide<preprocessing>`


.. figure:: /_static/user_guides/data_processor/dp_0_PGraph.png


General usage
-------------

.. code-block:: python

    from retentioneering.graph.p_graph import PGraph, EventsNode

    from retentioneering.data_processors_lib import CollapseLoops, CollapseLoopsParams
    from retentioneering.data_processors_lib import DeleteUsersByPathLength, DeleteUsersByPathLengthParams
    from retentioneering.data_processors_lib import FilterEvents, FilterEventsParams
    from retentioneering.data_processors_lib import GroupEvents, GroupEventsParams
    from retentioneering.data_processors_lib import NewUsersEvents, NewUsersParams
    from retentioneering.data_processors_lib import LostUsersEvents, LostUsersParams
    from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams
    from retentioneering.data_processors_lib import StartEndEvents, StartEndEventsParams
    from retentioneering.data_processors_lib import TruncatePath, TruncatePathParams
    from retentioneering.data_processors_lib import TruncatedEvents, TruncatedEventsParams
    from retentioneering.data_processors_lib import PositiveTarget, PositiveTargetParams
    from sretentioneeringrc.data_processors_lib import NegativeTarget, NegativeTargetParams

In order to use each ``DataProcessor`` you need to import it Class and
Class with its parameters.

To demonstrate the data processors in action we should:

-  create preprocessing graph instance (``PGraph``)
-  create dataprocessor instance with defined parameters
-  create node
-  add node to ``PGraph``
-  combine ``PGraph``

Let’s see how to create simple graph with one node:

.. code-block:: python

    graph = PGraph(source_stream=stream)
    dp_start_end = StartEndEvents(StartEndEventsParams())
    node_0 = EventsNode(dp_start_end)
    graph.add_node(node=node_0, parents=[graph.root])
    res = graph.combine(node_0).to_dataframe()
    res[res['user_id'] == 219483890]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>6f05bcbf-ee66-4167-922f-7846886d61ee</td>
          <td>path_start</td>
          <td>0</td>
          <td>path_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>42fc2caa-9f80-43d8-8b35-63decc431852</td>
          <td>raw</td>
          <td>1</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>a044fe59-43e4-4c96-b5e1-f22fae8e77ab</td>
          <td>raw</td>
          <td>2</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>867b1212-eb30-4633-a740-55b9625764ff</td>
          <td>raw</td>
          <td>3</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>4</th>
          <td>7648d661-2bd6-488b-9f7b-e261b9e48feb</td>
          <td>raw</td>
          <td>4</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2715</th>
          <td>0500c159-b81a-4521-89b8-3aa63cc3642d</td>
          <td>raw</td>
          <td>2715</td>
          <td>main</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2716</th>
          <td>49551407-cce1-4b26-8997-3cc0027fb81f</td>
          <td>raw</td>
          <td>2716</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:01.331109</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2717</th>
          <td>8dfa650f-7e8c-4a47-9873-f11f8a4a3683</td>
          <td>raw</td>
          <td>2717</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5721</th>
          <td>e92d6097-251c-407c-9846-edc5cba9906c</td>
          <td>raw</td>
          <td>5721</td>
          <td>main</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5722</th>
          <td>1f4ed249-93ea-4f9f-8699-03925c6c41b7</td>
          <td>raw</td>
          <td>5722</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:15.228575</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5723</th>
          <td>9e39fb3c-a451-49b2-b2a8-139494be49f9</td>
          <td>raw</td>
          <td>5723</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42.309028</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5724</th>
          <td>eb4b64ed-5dcc-426b-ad12-fd8a392884f2</td>
          <td>raw</td>
          <td>5724</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:52.255859</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5725</th>
          <td>62a15022-82d0-4ee9-9102-7fe06c8ada9a</td>
          <td>raw</td>
          <td>5725</td>
          <td>product1</td>
          <td>2020-01-06 22:11:01.709800</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5726</th>
          <td>cc70ddd1-93f3-456b-9a95-9c88782f758a</td>
          <td>raw</td>
          <td>5726</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:02.899490</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5727</th>
          <td>e0f5ec75-4697-4e25-96ef-9c4326fb27d7</td>
          <td>raw</td>
          <td>5727</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10210</th>
          <td>25c7f1e1-c950-4457-b5a8-1fe985b0e0fe</td>
          <td>raw</td>
          <td>10210</td>
          <td>main</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10211</th>
          <td>e64fdfc6-5550-4e51-b2db-bc765cff212f</td>
          <td>raw</td>
          <td>10211</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51.717127</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10212</th>
          <td>886cc4d7-6a98-4763-ad0d-d82807a2c043</td>
          <td>raw</td>
          <td>10212</td>
          <td>lost</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10213</th>
          <td>7c80180c-7868-406f-817a-7cdd53895cb2</td>
          <td>path_end</td>
          <td>10213</td>
          <td>path_end</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    </div>


Now let us add one more node ``SplitSessions``:

.. code-block:: python

    dp_split_sessions = SplitSessions(SplitSessionsParams(session_cutoff=(10, 'm')))
    node_1 = EventsNode(dp_split_sessions)

    graph.add_node(node=node_1, parents=[node_0])

    res = graph.combine(node_1).to_dataframe()
    res[res['user_id'] == 219483890]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>session_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>62c66255-d8f3-46c8-8a54-c4dee44eab48</td>
          <td>path_start</td>
          <td>0</td>
          <td>path_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>2</th>
          <td>a707a406-6424-4778-b95a-5b8f63af9330</td>
          <td>session_start</td>
          <td>2</td>
          <td>session_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>3</th>
          <td>0581bfae-ec01-4266-acd1-f3d3d896b9b3</td>
          <td>raw</td>
          <td>3</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>5</th>
          <td>67bf6297-71cd-40a1-8770-42358a0d0356</td>
          <td>raw</td>
          <td>5</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>7</th>
          <td>e7654c14-4e7f-43d6-a161-6b958372b406</td>
          <td>raw</td>
          <td>7</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>9</th>
          <td>423e52a7-10ff-4996-9a9c-29216afe266e</td>
          <td>raw</td>
          <td>9</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>11</th>
          <td>2c17f7a3-9778-4f31-a7e9-e3c9fe0610db</td>
          <td>session_end</td>
          <td>11</td>
          <td>session_end</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>6256</th>
          <td>e2f12cb6-ac51-4d91-81b5-f8ec30c375d1</td>
          <td>session_start</td>
          <td>6256</td>
          <td>session_start</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>6257</th>
          <td>01872942-a753-447f-a5e5-7922df1ee449</td>
          <td>raw</td>
          <td>6257</td>
          <td>main</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>6259</th>
          <td>36705600-4a42-4645-9ed8-f0225700af27</td>
          <td>raw</td>
          <td>6259</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:01.331109</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>6261</th>
          <td>59251302-3d8c-427b-9feb-142a66662149</td>
          <td>raw</td>
          <td>6261</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>6263</th>
          <td>95bfcbb6-75fc-464d-9987-94fa68ea5add</td>
          <td>session_end</td>
          <td>6263</td>
          <td>session_end</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>13326</th>
          <td>a653a842-c9e7-4d4c-8cb3-68a815620485</td>
          <td>session_start</td>
          <td>13326</td>
          <td>session_start</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13327</th>
          <td>fc18f1fc-d532-4a71-905f-610dd9bbeaf7</td>
          <td>raw</td>
          <td>13327</td>
          <td>main</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13329</th>
          <td>71f9889a-4675-411b-993f-251c89c846e6</td>
          <td>raw</td>
          <td>13329</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:15.228575</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13331</th>
          <td>3e057730-20c3-4fa6-96e1-5b075d31cc3f</td>
          <td>raw</td>
          <td>13331</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42.309028</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13333</th>
          <td>83292e74-d91d-49ef-afe7-5e7f2b3a1596</td>
          <td>raw</td>
          <td>13333</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:52.255859</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13335</th>
          <td>57cf3ed9-4ef9-4806-9a3c-734135d8bacd</td>
          <td>raw</td>
          <td>13335</td>
          <td>product1</td>
          <td>2020-01-06 22:11:01.709800</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13337</th>
          <td>dee004d3-cb81-4fea-aafc-b6992e57d11d</td>
          <td>raw</td>
          <td>13337</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:02.899490</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13339</th>
          <td>8733f02b-fd9a-4a59-bfb0-d3cf39b683ab</td>
          <td>raw</td>
          <td>13339</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13341</th>
          <td>5b3d5a01-480d-45ca-a6cd-5c6b285a5fab</td>
          <td>session_end</td>
          <td>13341</td>
          <td>session_end</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>23990</th>
          <td>27a5c829-ea23-45ad-ac4e-872861764a3d</td>
          <td>session_start</td>
          <td>23990</td>
          <td>session_start</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23991</th>
          <td>fa8de613-84b7-421f-8777-36680b538731</td>
          <td>raw</td>
          <td>23991</td>
          <td>main</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23993</th>
          <td>5015201a-1751-4d4f-9a2b-d865b5b7f1f9</td>
          <td>raw</td>
          <td>23993</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23995</th>
          <td>7a757a54-62a8-4a37-98e2-d7f3ec6f5cd2</td>
          <td>raw</td>
          <td>23995</td>
          <td>lost</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23997</th>
          <td>12542a00-c0d3-4809-a76e-452cbcb3a26c</td>
          <td>session_end</td>
          <td>23997</td>
          <td>session_end</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23998</th>
          <td>0e9447b3-4bf9-45ac-8413-9085c8e432e1</td>
          <td>path_end</td>
          <td>23998</td>
          <td>path_end</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
      </tbody>
    </table>
    </div>



Helpers and chain usage
-----------------------

However, one might use a more convenient way for a single data processor
usage. ``Helpers`` are ``Eventstream`` shortcut methods that implement
the same logic as code above. Each data processor has its helper method.
The table above shows the mapping between data processors and their
helpers:

+-------------------------+----------+-----------------------------------------------------+-----------------+
| Data                    | Type     | What it does                                        | Helper          |
| processor               |          |                                                     |                 |
+=========================+==========+=====================================================+=================+
| StartEndEvents          | Adding   | Adds two synthetic events in each user’s path:      | add_start_end   |
|                         |          | ``path_start`` and ``path_end``                     |                 |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| SplitSessions           | Adding   | Cuts user path into sessions and adds synthetic     | split_sessions  |
|                         |          | events ``session_start``, ``session_end``.          |                 |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| NewUsersEvents          | Adding   | Adds synthetic event ``new_user`` in the beginning  | add_new_users   |
|                         |          | of a user’s path if the user is considered as new.  |                 |
|                         |          | Otherwise adds ``existing_user``.                   |                 |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| LostUsersEvents         | Adding   | Adds synthetic event ``lost_user`` in the end of    | lost_users      |
|                         |          | user’s path if the user never comes back to the     |                 |
|                         |          | product. Otherwise adds ``absent_user`` event.      |                 |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| PositiveTarget          | Adding   | Adds synthetic event ``positive_target`` for all    | positive_target |
|                         |          | events which are considered as positive.            |                 |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| NegativeTarget          | Adding   | Adds synthetic event ``negative_target`` for all    | negative_target |
|                         |          | events which are considered as positive.            |                 |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| TruncatedEvents         | Adding   | Adds synthetic events ``truncated_left`` and/or     | truncated_events|
|                         |          | ``truncated_right`` for those user paths which are  |                 |
|                         |          | considered as truncated by the edges of the whole   |                 |
|                         |          | dataset.                                            |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| FilterEvents            | Removing | Remove events from an eventstream                   | filter          |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| DeleteUsersByPathLength | Removing | Deletes a too short user paths (in terms of number  | delete_users    |
|                         |          | of events or time duration).                        |                 |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| TruncatePath            | Removing | Leaves a part of an eventstream between a couple    | truncate_path   |
|                         |          | of selected events.                                 |                 |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| GroupEvents             | Grouping | Group given events into a single synthetic event.   | group           |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+
| CollapseLoops           | Grouping | Replaces sequences of repetitive events with new    | collapse_loops  |
|                         |          | synthetic events. E.g. ``A, A, A -> A``.            |                 |
|                         |          |                                                     |                 |
+-------------------------+----------+-----------------------------------------------------+-----------------+


Method chaining is supported for ``helpers`` as it is present in other
python libraries, for example in Pandas.

Let’s see how we can get the same result as in *General Usage* block of
current guide but using helper methods:

.. code-block:: python

    res = stream.add_start_end().split_sessions(session_cutoff=(10, 'm')).to_dataframe()
    res[res['user_id'] == 219483890]




.. raw:: html



    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>session_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>40480dee-6b91-4b0b-a8d8-4ff07dc59f45</td>
          <td>path_start</td>
          <td>0</td>
          <td>path_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>2</th>
          <td>0f541c90-6974-4690-9ee3-208ad62ce4a0</td>
          <td>session_start</td>
          <td>2</td>
          <td>session_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>3</th>
          <td>4e6da77c-4df7-4f4d-95ce-18edd7082cd0</td>
          <td>raw</td>
          <td>3</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>5</th>
          <td>cf916e34-8914-4eec-9917-382dda59e750</td>
          <td>raw</td>
          <td>5</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>7</th>
          <td>34c8a713-11f0-4c94-a4a4-2a047be2888f</td>
          <td>raw</td>
          <td>7</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>9</th>
          <td>c08c74a5-33a4-42ba-80b5-e60202c066d3</td>
          <td>raw</td>
          <td>9</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>11</th>
          <td>04fdad24-b311-498b-b165-d50a532d0c16</td>
          <td>session_end</td>
          <td>11</td>
          <td>session_end</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>6256</th>
          <td>f76605bd-669b-4891-a210-9da8b668d210</td>
          <td>session_start</td>
          <td>6256</td>
          <td>session_start</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>6257</th>
          <td>cde2704b-c8e5-4989-8d46-5a3c38a5601f</td>
          <td>raw</td>
          <td>6257</td>
          <td>main</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>6259</th>
          <td>6336b4a9-2421-4ff7-962a-fae5de73e723</td>
          <td>raw</td>
          <td>6259</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:01.331109</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>6261</th>
          <td>57c9dc38-0b11-4cf5-b1ae-1e8225f8b1fb</td>
          <td>raw</td>
          <td>6261</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>6263</th>
          <td>4aab51c6-ca85-47f8-bafe-0775f64af768</td>
          <td>session_end</td>
          <td>6263</td>
          <td>session_end</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>13326</th>
          <td>8b91fc06-38f9-4cbd-ab67-47ba24cc5281</td>
          <td>session_start</td>
          <td>13326</td>
          <td>session_start</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13327</th>
          <td>361351d1-4c1d-4d93-81f7-797f476f2c4f</td>
          <td>raw</td>
          <td>13327</td>
          <td>main</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13329</th>
          <td>f02812e0-7664-4e2a-a2b2-2d214f7d3599</td>
          <td>raw</td>
          <td>13329</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:15.228575</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13331</th>
          <td>ee7fb802-2d16-429f-a369-26cd3ca396f8</td>
          <td>raw</td>
          <td>13331</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42.309028</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13333</th>
          <td>d21af527-61cb-444a-a761-3b472e7c11ff</td>
          <td>raw</td>
          <td>13333</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:52.255859</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13335</th>
          <td>cd3a3ad9-91f0-4711-a07d-3c77b663c955</td>
          <td>raw</td>
          <td>13335</td>
          <td>product1</td>
          <td>2020-01-06 22:11:01.709800</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13337</th>
          <td>79c31aef-8bb9-4c08-a297-16b4ac4cc4ca</td>
          <td>raw</td>
          <td>13337</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:02.899490</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13339</th>
          <td>6a4949c8-9d7f-4ebd-b213-e7f5b8db94f8</td>
          <td>raw</td>
          <td>13339</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>13341</th>
          <td>2993bb6c-0598-47f5-b70d-c51d8edd21d9</td>
          <td>session_end</td>
          <td>13341</td>
          <td>session_end</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>23990</th>
          <td>3657db16-c558-4e7e-b3a7-260025e45adf</td>
          <td>session_start</td>
          <td>23990</td>
          <td>session_start</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23991</th>
          <td>4657e0d6-a9d4-4c21-b815-95beac017db5</td>
          <td>raw</td>
          <td>23991</td>
          <td>main</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23993</th>
          <td>bf60639e-5363-4c1a-98f4-a44a76c80d42</td>
          <td>raw</td>
          <td>23993</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23995</th>
          <td>2b174c8e-5acb-49a0-8c57-d16a4636e33a</td>
          <td>raw</td>
          <td>23995</td>
          <td>lost</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23997</th>
          <td>41f884d2-8fd7-4b68-a48b-ef9cbd1d80b1</td>
          <td>session_end</td>
          <td>23997</td>
          <td>session_end</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23998</th>
          <td>cdc65cad-6bce-4867-a5e4-f9a7911c8852</td>
          <td>path_end</td>
          <td>23998</td>
          <td>path_end</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
      </tbody>
    </table>
    </div>






To demonstrate implementation of ``DataProcessors`` we will use exactly
``helpers``.

Data Processors library
-----------------------

There are three kinds of data processors.

- Adding: processors that add events to eventstream,
- Removing: processors that remove events from eventstream,
- Editing: processors that modify existing events (including grouping operations).

Adding processors
~~~~~~~~~~~~~~~~~

The processors of that type add some artificial (we call them
*synthetic*) events which often comes handy for wrangling an eventstream.

StartEndEvents
^^^^^^^^^^^^^^

For each user ``StartEndEvents`` data processor generates an event
called ``path_start`` right before the first user event, and an event
``path_end`` right after the last user event.

.. figure:: /_static/user_guides/data_processor/dp_1_start_end.png


Applying ``StartEndEvents`` to mark user trajectory start and finish:

.. code-block:: python

    res = stream.add_start_end().to_dataframe()
    res[res['user_id'] == 219483890]


.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>be34dd99-e5ff-4856-b6d1-55bb18a4a0da</td>
          <td>path_start</td>
          <td>0</td>
          <td>path_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>42fc2caa-9f80-43d8-8b35-63decc431852</td>
          <td>raw</td>
          <td>1</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>a044fe59-43e4-4c96-b5e1-f22fae8e77ab</td>
          <td>raw</td>
          <td>2</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>867b1212-eb30-4633-a740-55b9625764ff</td>
          <td>raw</td>
          <td>3</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>4</th>
          <td>7648d661-2bd6-488b-9f7b-e261b9e48feb</td>
          <td>raw</td>
          <td>4</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2715</th>
          <td>0500c159-b81a-4521-89b8-3aa63cc3642d</td>
          <td>raw</td>
          <td>2715</td>
          <td>main</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2716</th>
          <td>49551407-cce1-4b26-8997-3cc0027fb81f</td>
          <td>raw</td>
          <td>2716</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:01.331109</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2717</th>
          <td>8dfa650f-7e8c-4a47-9873-f11f8a4a3683</td>
          <td>raw</td>
          <td>2717</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5721</th>
          <td>e92d6097-251c-407c-9846-edc5cba9906c</td>
          <td>raw</td>
          <td>5721</td>
          <td>main</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5722</th>
          <td>1f4ed249-93ea-4f9f-8699-03925c6c41b7</td>
          <td>raw</td>
          <td>5722</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:15.228575</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5723</th>
          <td>9e39fb3c-a451-49b2-b2a8-139494be49f9</td>
          <td>raw</td>
          <td>5723</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42.309028</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5724</th>
          <td>eb4b64ed-5dcc-426b-ad12-fd8a392884f2</td>
          <td>raw</td>
          <td>5724</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:52.255859</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5725</th>
          <td>62a15022-82d0-4ee9-9102-7fe06c8ada9a</td>
          <td>raw</td>
          <td>5725</td>
          <td>product1</td>
          <td>2020-01-06 22:11:01.709800</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5726</th>
          <td>cc70ddd1-93f3-456b-9a95-9c88782f758a</td>
          <td>raw</td>
          <td>5726</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:02.899490</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5727</th>
          <td>e0f5ec75-4697-4e25-96ef-9c4326fb27d7</td>
          <td>raw</td>
          <td>5727</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10210</th>
          <td>25c7f1e1-c950-4457-b5a8-1fe985b0e0fe</td>
          <td>raw</td>
          <td>10210</td>
          <td>main</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10211</th>
          <td>e64fdfc6-5550-4e51-b2db-bc765cff212f</td>
          <td>raw</td>
          <td>10211</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51.717127</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10212</th>
          <td>886cc4d7-6a98-4763-ad0d-d82807a2c043</td>
          <td>raw</td>
          <td>10212</td>
          <td>lost</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10213</th>
          <td>9cc88a8d-47fa-44c0-9151-0e4cc91ab181</td>
          <td>path_end</td>
          <td>10213</td>
          <td>path_end</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    </div>


As we see from the dataframe above, the generated events ``path_start``
and ``path_end`` have the same timestamps as the corresponding first and
last events.

We recommend applying this data processor each time you analyze any
eventstream since it sets the borders of an eventstream explicitly. It
can be useful for plotting and analyzing user lifetime across all users,
or conveniently displaying user trajectory borders in
``TransitionGraph``, ``StepMatrix``, and ``StepSankey`` tools.

SplitSessions
^^^^^^^^^^^^^

Cuts user paths into sessions based on the defined ``session_cutoff``
timeout parameter. For each session it creates a couple of synthetic
events ``session_start`` and ``session_end`` in a manner similar to
``StartEndEvents``. Session identifiers are formed according to the
template ``<user_id>_<user_session_number>`` and can be found in
``session_id`` column. The ``user_session_number`` is associated with a
session ordinal number within a user path and always starts with 1.

.. figure:: /_static/user_guides/data_processor/dp_2_split_sessions.png

Applying ``SplitSessions`` to split user paths into sessions with
session cutoff = 10 minutes:

.. code-block:: python

    res = stream.split_sessions(session_cutoff=(10, 'm')).to_dataframe()
    res[res['user_id'] == 219483890]


.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>session_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>3ff525d1-29f7-48ec-a00c-277977d64827</td>
          <td>session_start</td>
          <td>0</td>
          <td>session_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>1</th>
          <td>b04a2dd2-30e3-49cc-aacb-1fbd53027336</td>
          <td>raw</td>
          <td>1</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>3</th>
          <td>dc23aa94-a91f-41e9-8302-34eeead8f829</td>
          <td>raw</td>
          <td>3</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>5</th>
          <td>4b04fc68-ba50-4b95-bb6c-61a86bd263b0</td>
          <td>raw</td>
          <td>5</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>7</th>
          <td>cd57f0d2-19d5-4d5e-883d-c288415324ef</td>
          <td>raw</td>
          <td>7</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>9</th>
          <td>2ef7dcc1-26a6-475a-ba8f-2a5765e0b26a</td>
          <td>session_end</td>
          <td>9</td>
          <td>session_end</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>5316</th>
          <td>c2719f8f-0540-439b-85da-e15ffbacde58</td>
          <td>session_start</td>
          <td>5316</td>
          <td>session_start</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>5317</th>
          <td>7e98a67d-9200-4101-a9d5-bd8f53e9346b</td>
          <td>raw</td>
          <td>5317</td>
          <td>main</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>5319</th>
          <td>d5f5e418-15f6-4bf3-9db2-498263878877</td>
          <td>raw</td>
          <td>5319</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:01.331109</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>5321</th>
          <td>7e1617fc-95ed-418e-93ea-0d3ce43bf70d</td>
          <td>raw</td>
          <td>5321</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>5323</th>
          <td>21aca491-9ab2-43b4-96a1-25bd23794394</td>
          <td>session_end</td>
          <td>5323</td>
          <td>session_end</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>11556</th>
          <td>9942f655-c626-4214-b948-fd7ffe3587cb</td>
          <td>session_start</td>
          <td>11556</td>
          <td>session_start</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>11557</th>
          <td>cc677f4f-80f1-4a8b-9918-f0a641d65d69</td>
          <td>raw</td>
          <td>11557</td>
          <td>main</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>11559</th>
          <td>c7458ab1-1543-48f9-803e-4b43da56665b</td>
          <td>raw</td>
          <td>11559</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:15.228575</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>11561</th>
          <td>70b6c7b6-f033-4d26-90de-c03f5384e807</td>
          <td>raw</td>
          <td>11561</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42.309028</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>11563</th>
          <td>db77cf80-d1a7-469b-ae9f-6f05eccc3195</td>
          <td>raw</td>
          <td>11563</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:52.255859</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>11565</th>
          <td>533e063c-e7cf-451f-b1f2-7588a5fd0c09</td>
          <td>raw</td>
          <td>11565</td>
          <td>product1</td>
          <td>2020-01-06 22:11:01.709800</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>11567</th>
          <td>b6888b4d-9410-4249-bfdf-73315649c4e4</td>
          <td>raw</td>
          <td>11567</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:02.899490</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>11569</th>
          <td>4a438a8f-9053-408d-bd0d-0dcc79e3516e</td>
          <td>raw</td>
          <td>11569</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>11571</th>
          <td>40461433-c426-4517-86f2-ce2571fb5d24</td>
          <td>session_end</td>
          <td>11571</td>
          <td>session_end</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
          <td>219483890_3</td>
        </tr>
        <tr>
          <th>21042</th>
          <td>14f65d78-0308-4de6-a5de-ec26dc18d88e</td>
          <td>session_start</td>
          <td>21042</td>
          <td>session_start</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>21043</th>
          <td>b84dec40-6e32-4ced-bdd6-89f5cbaa8b19</td>
          <td>raw</td>
          <td>21043</td>
          <td>main</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>21045</th>
          <td>9a7eab21-0ab2-4cd6-84d7-6df0137b01e5</td>
          <td>raw</td>
          <td>21045</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>21047</th>
          <td>7693e531-64c2-4868-b577-c3b1305caf2d</td>
          <td>raw</td>
          <td>21047</td>
          <td>lost</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>21049</th>
          <td>ac3ac736-ec0b-4c4a-8c85-0a278d44bcf2</td>
          <td>session_end</td>
          <td>21049</td>
          <td>session_end</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
      </tbody>
    </table>
    </div>


The result for one user is displayed above. We see that the user
trajectory is partitioned into three sessions. The time distance between
consecutive events within each session is less than 10 minutes.

Splitting user paths into sessions is an essential step in clickstream
analysis. Sometimes, it is not clear which session cutoff is the best
(consider observations 9 and 5316 in the table above). In such cases, it
can be a good practice to generate multiple session splits, and compare
them in some fashion. Also, this is where
:py:meth:`Eventstream.timedelta_hist()<retentioneering.tooling.timedelta_hist.timedelta_hist.TimedeltaHist>`
method can help.




NewUsersEvents
^^^^^^^^^^^^^^

Given a list of users considered as new, the method labels such users in
the eventstream by adding a synthetic ``new_user`` event to the
beginning of the user’s trajectory. For all other users, adds an
``existing_user`` synthetic event. When passed ``'all'`` instead of the
list, all users will be labeled as new.

.. figure:: /_static/user_guides/data_processor/dp_3_new_users.png


.. code-block:: python

    new_users = [219483890, 964964743, 965024600]
    res = stream.add_new_users(new_users_list=new_users).to_dataframe()
    res[res['user_id'] == 219483890].head()




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>824de84d-a60e-43b2-93c4-b158b15a4fde</td>
          <td>new_user</td>
          <td>0</td>
          <td>new_user</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>42fc2caa-9f80-43d8-8b35-63decc431852</td>
          <td>raw</td>
          <td>1</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>a044fe59-43e4-4c96-b5e1-f22fae8e77ab</td>
          <td>raw</td>
          <td>2</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>867b1212-eb30-4633-a740-55b9625764ff</td>
          <td>raw</td>
          <td>3</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>4</th>
          <td>7648d661-2bd6-488b-9f7b-e261b9e48feb</td>
          <td>raw</td>
          <td>4</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    </div>



We can see that user ``219483890`` is marked as a new user.

But user ``501098384`` is marked as an existing user:

.. code-block:: python

    res[res['user_id'] == 501098384].head()




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>17387</th>
          <td>d8b5a816-958f-4b2c-96f5-f72a4b506744</td>
          <td>existing_user</td>
          <td>17387</td>
          <td>existing_user</td>
          <td>2020-04-02 05:36:04.896839</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>17388</th>
          <td>f4f5fec1-c87b-460c-9f43-98e1fc48f62b</td>
          <td>raw</td>
          <td>17388</td>
          <td>main</td>
          <td>2020-04-02 05:36:04.896839</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>17389</th>
          <td>3c934986-4d1d-45f5-b2db-829c8304c983</td>
          <td>raw</td>
          <td>17389</td>
          <td>catalog</td>
          <td>2020-04-02 05:36:05.371141</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>17390</th>
          <td>5f7307de-fbfc-49f1-9568-24c32c9b080f</td>
          <td>raw</td>
          <td>17390</td>
          <td>main</td>
          <td>2020-04-02 05:36:40.814504</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>17391</th>
          <td>ed053977-12c8-4ca4-94b5-2f6699c58b49</td>
          <td>raw</td>
          <td>17391</td>
          <td>catalog</td>
          <td>2020-04-02 05:36:41.190946</td>
          <td>501098384</td>
        </tr>
      </tbody>
    </table>
    </div>


Styding users’ first steps in the product is crucial for product
analysis, so we have to have an explicit indicator for that. This is
exactly what ``NewUsersEvent`` does.

This processor can be useful when you have data that chronologically
precedes the clickstream you are working with. For instance, your
clickstream might be covering 1-month user data, while also having the
user login data for the whole year. In that case, if you can compose a
list of all new users, ``NewUsersEvents``
will split users into two categories - new users, and users who have
appeared this year before.

LostUsersEvents
^^^^^^^^^^^^^^^

Given a list of users considered as ``lost``, the method labels such
users in the eventstream by adding a synthetic ``lost_user`` event to
the end of the user’s trajectory. For all other users, adds an
``absent_user`` event. When passed a ``lost_cutoff`` timedelta value,
the method labels users based on the following strategy: if the
timedelta between the user last event and the eventstream last event
exceeds ``lost_cutoff``, label as ``lost_user``; otherwise, label as
``absent_user``.

:red:`TODO: Make an image illustrating lost_cutoff parameter. dpanina`

.. figure:: /_static/user_guides/data_processor/dp_4_lost_users.png


.. code-block:: python

    lost_users_list = [219483890, 964964743, 965024600]
    res = stream.lost_users(lost_users_list=lost_users_list).to_dataframe()
    res[res['user_id'] == 219483890].tail()




.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>5175</th>
          <td>e0f5ec75-4697-4e25-96ef-9c4326fb27d7</td>
          <td>raw</td>
          <td>5175</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9329</th>
          <td>25c7f1e1-c950-4457-b5a8-1fe985b0e0fe</td>
          <td>raw</td>
          <td>9329</td>
          <td>main</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9330</th>
          <td>e64fdfc6-5550-4e51-b2db-bc765cff212f</td>
          <td>raw</td>
          <td>9330</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51.717127</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9331</th>
          <td>886cc4d7-6a98-4763-ad0d-d82807a2c043</td>
          <td>raw</td>
          <td>9331</td>
          <td>lost</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9332</th>
          <td>30b9d281-265f-4279-8167-a2ea1962cfb6</td>
          <td>lost_user</td>
          <td>9332</td>
          <td>lost_user</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    </div>


In opposite to user ``219483890``, user ``501098384`` is labeled as
``absent_user``.

.. code-block:: python

    res[res['user_id'] == 501098384].tail()




.. raw:: html



    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>39127</th>
          <td>e6cd3c44-5206-4f11-8639-aa559e3e522b</td>
          <td>raw</td>
          <td>39127</td>
          <td>catalog</td>
          <td>2020-04-29 12:48:01.809577</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>39128</th>
          <td>5f0bc836-605a-4813-9b91-8928e33e2a06</td>
          <td>raw</td>
          <td>39128</td>
          <td>main</td>
          <td>2020-04-29 12:48:01.938488</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>39129</th>
          <td>db3514c6-548f-4343-a362-aafa3dea0c9a</td>
          <td>raw</td>
          <td>39129</td>
          <td>catalog</td>
          <td>2020-04-29 12:48:06.595390</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>39130</th>
          <td>25bb08b8-6871-4d5e-a53c-16a9bb6f45d7</td>
          <td>raw</td>
          <td>39130</td>
          <td>lost</td>
          <td>2020-04-29 12:48:07.595390</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>39131</th>
          <td>76073cce-bbb0-4a9b-91b3-b0e7f59ed4c9</td>
          <td>absent_user</td>
          <td>39131</td>
          <td>absent_user</td>
          <td>2020-04-29 12:48:07.595390</td>
          <td>501098384</td>
        </tr>
      </tbody>
    </table>
    </div>



The function of this dataprocessor is somewhat similar to
``NewUsersEvents``, except for the fact that it adds labels to the end
of user trajectory.

We can also run ``LostUsersEvents`` with ``lost_cutoff`` passed, to
arbitrarily label some users as lost. Assume we consider a user as
absent if there was no event after 30 days.

.. code-block:: python

    res = stream.lost_users(lost_cutoff=(30, 'D')).to_dataframe()

Before we inspect the results of applying the data processor, let’s
notice that the eventstream ends at ``2020-04-29 12:48:07``.

.. code-block:: python

    res['timestamp'].max()




.. parsed-literal::

    Timestamp('2020-04-29 12:48:07.595390')



So user ``495985018`` is labeled as lost since her last event occurred
on ``2019-11-02``. It’s more than 30 days before the end of the
eventstream.

.. code-block:: python

    res[res['user_id'] == 495985018]




.. raw:: html



    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>47</th>
          <td>543c47e3-3f91-42b4-ae49-9ff32f242b4b</td>
          <td>raw</td>
          <td>47</td>
          <td>catalog</td>
          <td>2019-11-02 01:14:08.664850</td>
          <td>495985018</td>
        </tr>
        <tr>
          <th>48</th>
          <td>7c4c9735-889e-4ab2-9800-eb764d521431</td>
          <td>raw</td>
          <td>48</td>
          <td>cart</td>
          <td>2019-11-02 01:14:37.435643</td>
          <td>495985018</td>
        </tr>
        <tr>
          <th>49</th>
          <td>30fa3cae-20f7-4529-968d-1b82953b58c4</td>
          <td>raw</td>
          <td>49</td>
          <td>lost</td>
          <td>2019-11-02 01:14:38.435643</td>
          <td>495985018</td>
        </tr>
        <tr>
          <th>50</th>
          <td>6d830e1b-78b8-4330-8f2b-8b301d210364</td>
          <td>lost_user</td>
          <td>50</td>
          <td>lost_user</td>
          <td>2019-11-02 01:14:38.435643</td>
          <td>495985018</td>
        </tr>
      </tbody>
    </table>
    </div>



On the other hand, user ``819489198`` is labeled as ``absent`` because
her last event occurred on ``2020-04-15``, and this is less than 30 days
before ``2020-04-29``.

.. code-block:: python

    res[res['user_id'] == 819489198]




.. raw:: html



    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>26529</th>
          <td>b858a02b-d9c7-4819-98e5-50f3d4733367</td>
          <td>raw</td>
          <td>26529</td>
          <td>main</td>
          <td>2020-04-15 21:02:36.903678</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26530</th>
          <td>d32227e3-4db6-4492-a468-83a1c5215047</td>
          <td>raw</td>
          <td>26530</td>
          <td>catalog</td>
          <td>2020-04-15 21:02:37.658557</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26531</th>
          <td>fd0c8683-9c50-406d-badb-c476ee91dc88</td>
          <td>raw</td>
          <td>26531</td>
          <td>catalog</td>
          <td>2020-04-15 21:02:48.699804</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26532</th>
          <td>94d0a42c-736b-4441-84bf-929dae2d278d</td>
          <td>raw</td>
          <td>26532</td>
          <td>product2</td>
          <td>2020-04-15 21:02:51.173118</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26534</th>
          <td>139627e1-df18-46ee-88c1-af6aa2df27f8</td>
          <td>raw</td>
          <td>26534</td>
          <td>catalog</td>
          <td>2020-04-15 21:03:05.813046</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26536</th>
          <td>158eb213-7b26-4cb7-897d-53b5f0cc5943</td>
          <td>raw</td>
          <td>26536</td>
          <td>cart</td>
          <td>2020-04-15 21:03:35.216033</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26541</th>
          <td>46804fe4-f49b-4b46-8e34-e73523e1cc91</td>
          <td>raw</td>
          <td>26541</td>
          <td>delivery_choice</td>
          <td>2020-04-15 21:03:40.745520</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26542</th>
          <td>73298dd0-e4d3-48bf-bf0c-7936534dda29</td>
          <td>raw</td>
          <td>26542</td>
          <td>delivery_pickup</td>
          <td>2020-04-15 21:03:46.448349</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26543</th>
          <td>7ca8bb43-7f76-42a6-8f5a-1c8a56443e57</td>
          <td>raw</td>
          <td>26543</td>
          <td>payment_choice</td>
          <td>2020-04-15 21:03:46.575300</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26544</th>
          <td>76c46c99-74c3-40b3-bc98-83f6780553cf</td>
          <td>raw</td>
          <td>26544</td>
          <td>payment_card</td>
          <td>2020-04-15 21:03:46.862126</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26545</th>
          <td>81e986b0-834c-4d79-b9d3-aa62322ca789</td>
          <td>raw</td>
          <td>26545</td>
          <td>payment_done</td>
          <td>2020-04-15 21:03:47.074946</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26546</th>
          <td>9929ae30-020c-4dff-b251-8847006b50da</td>
          <td>absent_user</td>
          <td>26546</td>
          <td>absent_user</td>
          <td>2020-04-15 21:03:47.074946</td>
          <td>819489198</td>
        </tr>
      </tbody>
    </table>
    </div>



PositiveTarget
^^^^^^^^^^^^^^

For this dataprocessor two parameters are used:

-  ``positive_target_events`` - list of ``events`` associated with some
   kind of conversional goal of the user behavior in the product.
-  ``func`` - this function must accept parent ``Eventstream`` as an
   argument and return ``pandas.DataFrame`` containing only the lines
   corresponding to the events which are considered as positive.

Due to default behavior, for each user trajectory event from the
specified list and with minimum timestamp is taken and cloned with
``positive_target_<EVENTNAME>`` as ``event`` and ``positive_target``
type.


.. figure:: /_static/user_guides/data_processor/dp_5_positive.png

.. code-block:: python

    positive_events = ['cart', 'payment_done']
    res = stream.positive_target(positive_target_events=positive_events).to_dataframe()

Consider user ``219483890`` who has ``cart`` event appeared in her
trajectory with ``event_index = 2``. Right after it a synthetic event
``positive_target_cart`` was added.

.. code-block:: python

    res[res['user_id'] == 219483890]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>42fc2caa-9f80-43d8-8b35-63decc431852</td>
          <td>raw</td>
          <td>0</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>a044fe59-43e4-4c96-b5e1-f22fae8e77ab</td>
          <td>raw</td>
          <td>1</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>867b1212-eb30-4633-a740-55b9625764ff</td>
          <td>raw</td>
          <td>2</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>b4ef435c-df36-4886-9d26-da03bc82454c</td>
          <td>positive_target</td>
          <td>3</td>
          <td>positive_target_cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>4</th>
          <td>7648d661-2bd6-488b-9f7b-e261b9e48feb</td>
          <td>raw</td>
          <td>4</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2393</th>
          <td>0500c159-b81a-4521-89b8-3aa63cc3642d</td>
          <td>raw</td>
          <td>2393</td>
          <td>main</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2394</th>
          <td>49551407-cce1-4b26-8997-3cc0027fb81f</td>
          <td>raw</td>
          <td>2394</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:01.331109</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2395</th>
          <td>8dfa650f-7e8c-4a47-9873-f11f8a4a3683</td>
          <td>raw</td>
          <td>2395</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5114</th>
          <td>e92d6097-251c-407c-9846-edc5cba9906c</td>
          <td>raw</td>
          <td>5114</td>
          <td>main</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5115</th>
          <td>1f4ed249-93ea-4f9f-8699-03925c6c41b7</td>
          <td>raw</td>
          <td>5115</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:15.228575</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5116</th>
          <td>9e39fb3c-a451-49b2-b2a8-139494be49f9</td>
          <td>raw</td>
          <td>5116</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42.309028</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5117</th>
          <td>eb4b64ed-5dcc-426b-ad12-fd8a392884f2</td>
          <td>raw</td>
          <td>5117</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:52.255859</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5118</th>
          <td>62a15022-82d0-4ee9-9102-7fe06c8ada9a</td>
          <td>raw</td>
          <td>5118</td>
          <td>product1</td>
          <td>2020-01-06 22:11:01.709800</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5119</th>
          <td>cc70ddd1-93f3-456b-9a95-9c88782f758a</td>
          <td>raw</td>
          <td>5119</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:02.899490</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5120</th>
          <td>e0f5ec75-4697-4e25-96ef-9c4326fb27d7</td>
          <td>raw</td>
          <td>5120</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9186</th>
          <td>25c7f1e1-c950-4457-b5a8-1fe985b0e0fe</td>
          <td>raw</td>
          <td>9186</td>
          <td>main</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9187</th>
          <td>e64fdfc6-5550-4e51-b2db-bc765cff212f</td>
          <td>raw</td>
          <td>9187</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51.717127</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9188</th>
          <td>886cc4d7-6a98-4763-ad0d-d82807a2c043</td>
          <td>raw</td>
          <td>9188</td>
          <td>lost</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    </div>



In opposite to this user, user ``24427596`` has no positive events, so
her path remains unchanged:

.. code-block:: python

    res[res['user_id'] == 24427596]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>68</th>
          <td>c968717d-631c-4668-89a4-75ea88d3ad55</td>
          <td>raw</td>
          <td>68</td>
          <td>main</td>
          <td>2019-11-02 07:28:07.285541</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>69</th>
          <td>7f50c45c-db2b-4d69-8ea0-95a22d757fae</td>
          <td>raw</td>
          <td>69</td>
          <td>catalog</td>
          <td>2019-11-02 07:28:14.319850</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>70</th>
          <td>d4a0527a-2084-4297-ad5e-2dddeda70d90</td>
          <td>raw</td>
          <td>70</td>
          <td>catalog</td>
          <td>2019-11-02 07:29:08.301333</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>71</th>
          <td>30bab9c7-50eb-429a-b6f0-0e2d91eb3d26</td>
          <td>raw</td>
          <td>71</td>
          <td>catalog</td>
          <td>2019-11-02 07:29:41.848396</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>72</th>
          <td>cb8f747a-f46a-4c8a-bd35-690d540425a9</td>
          <td>raw</td>
          <td>72</td>
          <td>lost</td>
          <td>2019-11-02 07:29:42.848396</td>
          <td>24427596</td>
        </tr>
      </tbody>
    </table>
    </div>



This data processor can make it easier to label events that we would
like to consider positive. This might come useful in the futher analysis
with such tools as ``TransitionGraph``, ``StepMatrix``, and
``SankeyStep`` so the positive events will be shown expliicitly.

Another way to set positive events is to change the default ``func``.
And define a custom one.

For example we need to mark each ``positive_target_event``, not only the
first one in the trajectory.

.. code-block:: python

    def custom_func(eventstream, positive_target_events) -> pd.DataFrame:

        event_col = eventstream.schema.event_name
        df = eventstream.to_dataframe()

        return df[df[event_col].isin(positive_target_events)]

    res = stream.positive_target(positive_target_events=positive_events, func=custom_func).to_dataframe()


.. code-block:: python

    res[res['user_id'] == 219483890]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>335dfca9-df6f-4f98-8e9a-f47dacf59bdf</td>
          <td>raw</td>
          <td>0</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>b3d1c279-f242-4309-9923-6b1d7cea3ddc</td>
          <td>raw</td>
          <td>1</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>6a369664-5eb5-4f98-bb2a-d909bc26ff11</td>
          <td>raw</td>
          <td>2</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>9c69ef2e-5e95-4f06-ad5d-21272a881cf1</td>
          <td>positive_target</td>
          <td>3</td>
          <td>positive_target_cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>4</th>
          <td>12a0a65e-5df5-49af-b5a3-9b24de9938b0</td>
          <td>raw</td>
          <td>4</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2489</th>
          <td>cf3daba5-864f-4be0-b165-101074f216b5</td>
          <td>raw</td>
          <td>2489</td>
          <td>main</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2490</th>
          <td>30abc553-f0eb-4b56-9fc6-75eb567be640</td>
          <td>raw</td>
          <td>2490</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:01.331109</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2491</th>
          <td>22505aa4-8c1a-41e9-983b-fd3c3e1b3496</td>
          <td>raw</td>
          <td>2491</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5335</th>
          <td>fca0c09b-0da3-466c-9dda-f3fdd03e6db7</td>
          <td>raw</td>
          <td>5335</td>
          <td>main</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5336</th>
          <td>8e1b8837-3868-4322-a9c6-a2ea885eb12b</td>
          <td>raw</td>
          <td>5336</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:15.228575</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5337</th>
          <td>ca6a8459-1900-4a0d-b138-973165de8e2f</td>
          <td>raw</td>
          <td>5337</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42.309028</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5338</th>
          <td>d6b448b8-2e2e-4094-a300-5da4b4d101f5</td>
          <td>positive_target</td>
          <td>5338</td>
          <td>positive_target_cart</td>
          <td>2020-01-06 22:10:42.309028</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5339</th>
          <td>d611690f-9881-46cb-899e-e9842dd57f0b</td>
          <td>raw</td>
          <td>5339</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:52.255859</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5340</th>
          <td>96c74186-44b4-4997-9582-e804b798b0fc</td>
          <td>raw</td>
          <td>5340</td>
          <td>product1</td>
          <td>2020-01-06 22:11:01.709800</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5341</th>
          <td>41c9c2de-ff5e-4662-88fb-c18d11efaf92</td>
          <td>raw</td>
          <td>5341</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:02.899490</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5342</th>
          <td>ccfd64de-e3b2-47fd-91ca-19c929c1a1c6</td>
          <td>raw</td>
          <td>5342</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9619</th>
          <td>9cc70118-181b-4c7e-870f-61850e2acb25</td>
          <td>raw</td>
          <td>9619</td>
          <td>main</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9620</th>
          <td>ea8d8d76-2f01-418e-bf2b-1ee433d2146d</td>
          <td>raw</td>
          <td>9620</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51.717127</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9621</th>
          <td>c78fea75-f925-4b90-acfd-e1153320bd06</td>
          <td>raw</td>
          <td>9621</td>
          <td>lost</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    </div>



NegativeTarget
^^^^^^^^^^^^^^

The idea of ``NegativeTarget`` data processor is exactly the same as for
``PositiveTarget`` but for applied to negative labels instead of
positive.

-  ``negative_target_events`` - list of ``events`` associated with some
   kind of negative result of the user behavior in the product.
-  ``func`` - this function must accept parent ``Eventstream`` as an
   argument and return ``pandas.DataFrame`` containing only the lines
   corresponding to the events which are considered as negative.


.. figure:: /_static/user_guides/data_processor/dp_6_negative.png

.. code-block:: python

    negative_events = ['lost']

    res = stream.negative_target(negative_target_events=negative_events).to_dataframe()

Functions similarly to the ``PositiveTarget`` dataprocessor - in this
case, it will add negative event next to the ``lost`` event:

.. code-block:: python

    res[res['user_id'] == 24427596]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>62</th>
          <td>c968717d-631c-4668-89a4-75ea88d3ad55</td>
          <td>raw</td>
          <td>62</td>
          <td>main</td>
          <td>2019-11-02 07:28:07.285541</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>63</th>
          <td>7f50c45c-db2b-4d69-8ea0-95a22d757fae</td>
          <td>raw</td>
          <td>63</td>
          <td>catalog</td>
          <td>2019-11-02 07:28:14.319850</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>64</th>
          <td>d4a0527a-2084-4297-ad5e-2dddeda70d90</td>
          <td>raw</td>
          <td>64</td>
          <td>catalog</td>
          <td>2019-11-02 07:29:08.301333</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>65</th>
          <td>30bab9c7-50eb-429a-b6f0-0e2d91eb3d26</td>
          <td>raw</td>
          <td>65</td>
          <td>catalog</td>
          <td>2019-11-02 07:29:41.848396</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>66</th>
          <td>cb8f747a-f46a-4c8a-bd35-690d540425a9</td>
          <td>raw</td>
          <td>66</td>
          <td>lost</td>
          <td>2019-11-02 07:29:42.848396</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>67</th>
          <td>6ec3a6cf-8e5b-4c61-b5bd-83275cf7c75b</td>
          <td>negative_target</td>
          <td>67</td>
          <td>negative_target_lost</td>
          <td>2019-11-02 07:29:42.848396</td>
          <td>24427596</td>
        </tr>
      </tbody>
    </table>
    </div>



TruncatedEvents
^^^^^^^^^^^^^^^

``TruncatedEvents`` addresses a common practical problem when some
trajectories appear to be truncated due to the dataset’s natural
boundaries.

.. figure:: /_static/user_guides/data_processor/dp_7_truncate_timeline.png


The diagram above illustrates this problem. Consider two user paths –
blue and orange – apart of the other paths of an eventstream. In
reality, the blue path started before the beginning of the eventstream.
But we can’t see that since we observe no events to the left from the
beginning of the eventstream. That’s why the path real start is dashed.
So instead of the real beginning of the user path we observe the fake
beginning and in fact the trajectory is truncated.

The similar situation happens to orange user path. Instead of the latent
real end we can observe fake end only.

One of possible ways to reveal potentially truncated paths is to detect
such trajectories that are “too short” for a typical trajectory, and
whose shortness can be attributed to being truncated.

``TruncatedEvents`` data processor uses ``left_truncated_cutoff`` and
``right_truncated_cutoff`` timedeltas and labels user trajectories as
``truncated_left`` or ``truncated_right`` basing on the following
policy:

-  if the last event of a user trajectory is distanced from the first
   event of the whole eventstream by less than
   ``left_truncated_cutoff``, consider the user trajectory truncated
   from the left, and create ``truncated_left`` synthetic event at the
   trajectory start;

-  if the first event of a user trajectory is distanced from the last
   event of the whole eventstream by less than
   ``right_truncated_cutoff``, consider the user trajectory truncated
   from the right, and create ``truncated_right`` synthetic event at the
   trajectory end.

.. figure:: /_static/user_guides/data_processor/dp_8_truncate.png



Sometimes, it can be a good practice to use different cutoff values, and
compare them in some fashion. Also, this is where
:py:meth:`Eventstream.timedelta_hist()<retentioneering.tooling.timedelta_hist.timedelta_hist.TimedeltaHist>` method
with specified parameter ``event_pair=('path_start', 'cart')`` can help.



.. code-block:: python

    params = {
        'left_truncated_cutoff': (4, 'D'),
        'right_truncated_cutoff': (3, 'D')
    }

    res = stream.truncated_events(**params).to_dataframe()

Notice the eventstream’s start and end:

.. code-block:: python

    print('Eventstream start: {}'.format(res.timestamp.min()))
    print('Eventstream end: {}'.format(res.timestamp.max()))


.. parsed-literal::

    Eventstream start: 2019-11-01 17:59:13.273932
    Eventstream end: 2020-04-29 12:48:07.595390


This user’s trajectory ends at ``2019-11-02 01:14:38`` which is too
close to the eventstream start according to ``left_truncated_cutoff``
value, so the ``TruncatedEvents`` dataprocessor labels it as truncated
from the left:

.. code-block:: python

    res[res['user_id'] == 495985018]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>47</th>
          <td>994fe4d8-bed5-41ad-9404-ad1cab3da21f</td>
          <td>truncated_left</td>
          <td>47</td>
          <td>truncated_left</td>
          <td>2019-11-02 01:14:08.664850</td>
          <td>495985018</td>
        </tr>
        <tr>
          <th>48</th>
          <td>543c47e3-3f91-42b4-ae49-9ff32f242b4b</td>
          <td>raw</td>
          <td>48</td>
          <td>catalog</td>
          <td>2019-11-02 01:14:08.664850</td>
          <td>495985018</td>
        </tr>
        <tr>
          <th>49</th>
          <td>7c4c9735-889e-4ab2-9800-eb764d521431</td>
          <td>raw</td>
          <td>49</td>
          <td>cart</td>
          <td>2019-11-02 01:14:37.435643</td>
          <td>495985018</td>
        </tr>
        <tr>
          <th>50</th>
          <td>30fa3cae-20f7-4529-968d-1b82953b58c4</td>
          <td>raw</td>
          <td>50</td>
          <td>lost</td>
          <td>2019-11-02 01:14:38.435643</td>
          <td>495985018</td>
        </tr>
      </tbody>
    </table>
    </div>



This user’s trajectory starts at ``2020-04-29 12:24:21`` which is too
close to the eventstream end according to ``left_truncated_cutoff``, so
the ``TruncatedEvents`` data processor labels it as truncated from the
right:

.. code-block:: python

    res[res['user_id'] == 831491833]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>35627</th>
          <td>85af3b86-9af9-4d0a-9160-62e138b80883</td>
          <td>raw</td>
          <td>35627</td>
          <td>catalog</td>
          <td>2020-04-29 12:24:21.538805</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35628</th>
          <td>2fc33ede-f74f-404b-9ea9-51a3b7ab073d</td>
          <td>raw</td>
          <td>35628</td>
          <td>catalog</td>
          <td>2020-04-29 12:24:33.841264</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35629</th>
          <td>07b40626-a938-4e40-b922-d00b83393695</td>
          <td>raw</td>
          <td>35629</td>
          <td>product2</td>
          <td>2020-04-29 12:24:39.415424</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35630</th>
          <td>8045b8cd-3645-4778-907d-dc30b1501f1c</td>
          <td>raw</td>
          <td>35630</td>
          <td>cart</td>
          <td>2020-04-29 12:24:59.928499</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35631</th>
          <td>21153030-b55d-48e1-8846-521e89417a18</td>
          <td>raw</td>
          <td>35631</td>
          <td>catalog</td>
          <td>2020-04-29 12:25:06.262205</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35632</th>
          <td>b11a0ca1-0e6f-4d21-9257-2efdfccb8b21</td>
          <td>raw</td>
          <td>35632</td>
          <td>lost</td>
          <td>2020-04-29 12:25:07.262205</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35633</th>
          <td>c22c6aa7-6619-402a-9fd4-dcc8b9bc3ebd</td>
          <td>truncated_right</td>
          <td>35633</td>
          <td>truncated_right</td>
          <td>2020-04-29 12:25:07.262205</td>
          <td>831491833</td>
        </tr>
      </tbody>
    </table>
    </div>



Synthetic events order
^^^^^^^^^^^^^^^^^^^^^^

As you may have noticed, each synthetic event has its “parent” which
defines the synthetic event timestamp. Obviously, in case you apply
multiple data processors timestamp collisions might occur, so it’s not
clear how the events should be ordered. For colliding events we define
the sorting order according to their types:

-  profile
-  path_start
-  new_user
-  existing_user
-  truncated_left
-  session_start
-  session_start_truncated
-  group_alias
-  raw
-  raw_sleep
-  None
-  synthetic
-  synthetic_sleep
-  positive_target
-  negative_target
-  session_end_truncated
-  session_end
-  session_sleep
-  truncated_right
-  absent_user
-  lost_user
-  path_end

So the earlier an event type appears in this list, the less index it
will have in ``event_index`` eventstream column.

Removing processors
~~~~~~~~~~~~~~~~~~~

FilterEvents
^^^^^^^^^^^^

``FilterEvents`` keeps events based on the masking function ``func``.
The function must return a boolean mask for the input dataframe - that
is, any series of boolean ``True or False`` variables that can be used
as a filter for the dataframe underlying the eventstream.

.. figure:: /_static/user_guides/data_processor/dp_9_filter.png


Let us say we are interested only in specific events - for example, only
in events of users that appear in some pre-defined list of users.
``FilterEvents`` allows us to access only these events:

.. code-block:: python

    def save_specific_users(df, schema):
        users_to_save = [219483890, 964964743, 965024600]
        return df[schema.user_id].isin(users_to_save)

    res = stream.filter(func=save_specific_users).to_dataframe()

As you see below, the resulting eventstream includes these 3 users only:

.. code-block:: python

    res['user_id'].unique().astype(int)




.. parsed-literal::

    array([219483890, 964964743, 965024600])



Note that the masking function accepts not only ``pandas.DataFrame``
which associated with the eventstream, but ``schema`` parameter as well.
Having this parameter, you can access any eventstream column which is
defined in its :py:meth:`EventstreamSchema<retentioneering.eventstream.schema.EventstreamSchema>`

This makes such masking functions reusable regardless of eventstream
column titles.

Here’s another example. Using ``FilterEvents`` data processor, we can
also remove specific events from the eventstream. Let us remove all
``catalog`` and ``main`` events, assuming they are non-informative for
us:

.. code-block:: python

    stream.to_dataframe()\
        ['event']\
        .value_counts()\
        [lambda s: s.index.isin(['catalog', 'main'])]




.. parsed-literal::

    catalog    14518
    main        5635
    Name: event, dtype: int64



.. code-block:: python

    def exclude_events(df, schema):
        events_to_exclude = ['catalog', 'main']
        return ~df[schema.event_name].isin(events_to_exclude)

    res = stream.filter(func=exclude_events).to_dataframe()

We can see that ``res`` dataframe doesn’t have “useless” events anymore.

.. code-block:: python

    res['event']\
        .value_counts()\
        [lambda s: s.index.isin(['catalog', 'main'])]




.. parsed-literal::

    Series([], Name: event, dtype: int64)



DeleteUsersByPathLength
^^^^^^^^^^^^^^^^^^^^^^^

According to the title, ``DeleteUsersByPathLength`` removes the paths
which are considered as “too short”. In practice, short paths usually
(but not necessarily) mean that they are too rubbish to be thoroughly
analyzed. However, the genesis of short paths is still of concern to
product analytics.

Path length can be measured in two units:

- the number of events comprising a path,
- the time distance between the beginning and the end of a path.

The former is associated with ``events_num`` parameter, the latter –
with ``cutoff`` parameter. Thus, ``DeleteUsersByPathLength`` removes all
the paths of length less than ``events_num`` or ``cutoff``.

Diagram for specified ``events_num``:

.. figure:: /_static/user_guides/data_processor/dp_10_delete_events.png


Diagram for specified ``cutoff``:

.. figure:: /_static/user_guides/data_processor/dp_10_delete_cutoff.png


Let us showcase both variants of the ``DeleteUsersByPathLength``
dataprocessor:

A minimum number of events specified:

.. code-block:: python

    res = stream.delete_users(events_num=25).to_dataframe()

Any remaining user has at least 25 events. For example, user
``629881394`` has 48 events.

.. code-block:: python

    len(res[res['user_id'] == 629881394])



.. parsed-literal::

    48



A minimum path length (user lifetime) specified:

.. code-block:: python

    res = stream.delete_users(cutoff=(1, 'M')).to_dataframe()

Any remaining user has been “alive” for at least a full month. For
example, user ``964964743`` started her trajectory on ``2019-11-01`` and
ended on ``2019-12-09``.

.. code-block:: python

    res[res['user_id'] == 964964743].iloc[[0, -1]]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>4</th>
          <td>501c77a0-9a7f-461a-bb52-b1375d50e2aa</td>
          <td>raw</td>
          <td>4</td>
          <td>catalog</td>
          <td>2019-11-01 21:38:19.283663</td>
          <td>964964743</td>
        </tr>
        <tr>
          <th>3785</th>
          <td>62c08131-854d-4562-9628-1537569e4bb5</td>
          <td>raw</td>
          <td>3785</td>
          <td>lost</td>
          <td>2019-12-09 01:43:58.766850</td>
          <td>964964743</td>
        </tr>
      </tbody>
    </table>
    </div>


TruncatePath
^^^^^^^^^^^^

For each user trajectory ``TruncatePath`` drops all events either before
or after a certain event. These parameters specify the data processor’s
behavior:

-  ``drop_before``: event name before which part of the user’s path is
   dropped. Specified event remains in the eventstream.

-  ``drop_after``: event name after which part of the user’s path is
   dropped. Specified event remains in the eventstream.

-  ``occurrence_before``: if set to ``first`` (by default), all events
   before the first occurrence of the ``drop_before`` event are dropped.
   If set to ``last``, all events before the last occurrence of the
   ``drop_before`` event are dropped.

-  ``occurrence_after``: the same behavior as in the
   ``occurrence_before``, but for right (after the event) path
   truncation.

-  ``shift_before``: sets the number of steps by which the truncate
   point is shifted from the selected event. If the value is negative,
   then the offset occurs to the left along the timeline; if positive,
   then the offset occurs to the right.

-  ``shift_after``: the same behavior as in the shift_before, but for
   right (after the event) path truncation.

If the specified event is not present in a user path, the path remains
unchanged.

.. figure:: /_static/user_guides/data_processor/dp_11_truncate_path.png


Suppose we want to see what happens to the user after she jumps to
``cart`` event. Suppose also that we need to explore a bit what events
brought her to ``cart`` event. So we use ``drop_before='cart'`` along
with ``shift_before=-2`` parameters.

.. code-block:: python

    res = stream.truncate_path(drop_before='cart', shift_before=-2).to_dataframe()

Now some users have their trajectories truncated, because they have at
least one ``cart`` in their path:

.. code-block:: python

    res[res['user_id'] == 219483890]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>42fc2caa-9f80-43d8-8b35-63decc431852</td>
          <td>raw</td>
          <td>0</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>a044fe59-43e4-4c96-b5e1-f22fae8e77ab</td>
          <td>raw</td>
          <td>1</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>867b1212-eb30-4633-a740-55b9625764ff</td>
          <td>raw</td>
          <td>2</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>7648d661-2bd6-488b-9f7b-e261b9e48feb</td>
          <td>raw</td>
          <td>3</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2706</th>
          <td>0500c159-b81a-4521-89b8-3aa63cc3642d</td>
          <td>raw</td>
          <td>2706</td>
          <td>main</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2707</th>
          <td>49551407-cce1-4b26-8997-3cc0027fb81f</td>
          <td>raw</td>
          <td>2707</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:01.331109</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2708</th>
          <td>8dfa650f-7e8c-4a47-9873-f11f8a4a3683</td>
          <td>raw</td>
          <td>2708</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5825</th>
          <td>e92d6097-251c-407c-9846-edc5cba9906c</td>
          <td>raw</td>
          <td>5825</td>
          <td>main</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5826</th>
          <td>1f4ed249-93ea-4f9f-8699-03925c6c41b7</td>
          <td>raw</td>
          <td>5826</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:15.228575</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5827</th>
          <td>9e39fb3c-a451-49b2-b2a8-139494be49f9</td>
          <td>raw</td>
          <td>5827</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42.309028</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5828</th>
          <td>eb4b64ed-5dcc-426b-ad12-fd8a392884f2</td>
          <td>raw</td>
          <td>5828</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:52.255859</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5829</th>
          <td>62a15022-82d0-4ee9-9102-7fe06c8ada9a</td>
          <td>raw</td>
          <td>5829</td>
          <td>product1</td>
          <td>2020-01-06 22:11:01.709800</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5830</th>
          <td>cc70ddd1-93f3-456b-9a95-9c88782f758a</td>
          <td>raw</td>
          <td>5830</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:02.899490</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5831</th>
          <td>e0f5ec75-4697-4e25-96ef-9c4326fb27d7</td>
          <td>raw</td>
          <td>5831</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:28.271366</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10316</th>
          <td>25c7f1e1-c950-4457-b5a8-1fe985b0e0fe</td>
          <td>raw</td>
          <td>10316</td>
          <td>main</td>
          <td>2020-02-14 21:04:49.450696</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10317</th>
          <td>e64fdfc6-5550-4e51-b2db-bc765cff212f</td>
          <td>raw</td>
          <td>10317</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51.717127</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>10318</th>
          <td>886cc4d7-6a98-4763-ad0d-d82807a2c043</td>
          <td>raw</td>
          <td>10318</td>
          <td>lost</td>
          <td>2020-02-14 21:04:52.717127</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    </div>



As we can see, this path now starts with the two events preceding
``cart`` (``event_index = 0, 1``), and ``cart`` right after them
``event_index = 2``. Another ``cart`` event occured here
(``event_index = 5827``) but since the default
``occurrence_before='first'`` was triggered this second ``cart`` was
ignored by the data processor.

Some users, however, do not have any ``cart`` events - and their
trajectories have not been changed:

.. code-block:: python

    res[res['user_id'] == 24427596]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>89</th>
          <td>c968717d-631c-4668-89a4-75ea88d3ad55</td>
          <td>raw</td>
          <td>89</td>
          <td>main</td>
          <td>2019-11-02 07:28:07.285541</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>90</th>
          <td>7f50c45c-db2b-4d69-8ea0-95a22d757fae</td>
          <td>raw</td>
          <td>90</td>
          <td>catalog</td>
          <td>2019-11-02 07:28:14.319850</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>91</th>
          <td>d4a0527a-2084-4297-ad5e-2dddeda70d90</td>
          <td>raw</td>
          <td>91</td>
          <td>catalog</td>
          <td>2019-11-02 07:29:08.301333</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>92</th>
          <td>30bab9c7-50eb-429a-b6f0-0e2d91eb3d26</td>
          <td>raw</td>
          <td>92</td>
          <td>catalog</td>
          <td>2019-11-02 07:29:41.848396</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>93</th>
          <td>cb8f747a-f46a-4c8a-bd35-690d540425a9</td>
          <td>raw</td>
          <td>93</td>
          <td>lost</td>
          <td>2019-11-02 07:29:42.848396</td>
          <td>24427596</td>
        </tr>
      </tbody>
    </table>
    </div>



We can also perform truncation from the right, or specify for truncation
point to be not the first, but the last occurence of ``cart``. To
demonstrate both, let us set ``drop_after = "cart"`` and
``occurrence_after = "last"``:

.. code-block:: python

    res = stream.truncate_path(drop_after='cart', occurrence_after="last").to_dataframe()

Now, any trajectory which includes ``cart`` is truncated to end with the
last ``cart``:

.. code-block:: python

    res[res['user_id'] == 219483890]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>42fc2caa-9f80-43d8-8b35-63decc431852</td>
          <td>raw</td>
          <td>0</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>a044fe59-43e4-4c96-b5e1-f22fae8e77ab</td>
          <td>raw</td>
          <td>1</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>867b1212-eb30-4633-a740-55b9625764ff</td>
          <td>raw</td>
          <td>2</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>7648d661-2bd6-488b-9f7b-e261b9e48feb</td>
          <td>raw</td>
          <td>3</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2634</th>
          <td>0500c159-b81a-4521-89b8-3aa63cc3642d</td>
          <td>raw</td>
          <td>2634</td>
          <td>main</td>
          <td>2019-12-06 16:22:57.484842</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2635</th>
          <td>49551407-cce1-4b26-8997-3cc0027fb81f</td>
          <td>raw</td>
          <td>2635</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:01.331109</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2636</th>
          <td>8dfa650f-7e8c-4a47-9873-f11f8a4a3683</td>
          <td>raw</td>
          <td>2636</td>
          <td>catalog</td>
          <td>2019-12-06 16:23:48.116617</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5638</th>
          <td>e92d6097-251c-407c-9846-edc5cba9906c</td>
          <td>raw</td>
          <td>5638</td>
          <td>main</td>
          <td>2020-01-06 22:10:13.635011</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5639</th>
          <td>1f4ed249-93ea-4f9f-8699-03925c6c41b7</td>
          <td>raw</td>
          <td>5639</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:15.228575</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5640</th>
          <td>9e39fb3c-a451-49b2-b2a8-139494be49f9</td>
          <td>raw</td>
          <td>5640</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42.309028</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    </div>



Editing processors
~~~~~~~~~~~~~~~~~~

GroupEvents
^^^^^^^^^^^

Given a masking function passed as ``func``, ``GroupEvents`` replaces
all the events located by ``func`` with newly created synthetic events
of ``event_name`` name and ``event_type`` type (``group_alias`` by
default). The timestamps of these synthetic events are the same as their
parent’s ones. ``func`` can be any function that returns a series of
boolean (``True/False``) variables that can be used as a filter for the
dataframe underlying the eventstream.


:red:`TODO: Replace ‘filter’ with ‘func’ on the diagram. dpanina`

.. figure:: /_static/user_guides/data_processor/dp_12_group.png



With ``GroupEvents``, we can group events based on event name. Suppose
we need to assign a common name ``product`` to events ``product1`` and
``product2``. Here’s how we can manage it.

.. code-block:: python

    def group_events(df, schema):
        events_to_group = ['product1', 'product2']
        return df[schema.event_name].isin(events_to_group)

    params = {
        'event_name': 'product',
        'func': group_events
    }

    res = stream.group(**params).to_dataframe()

As we can see, user ``456870964`` now has a couple of ``product`` events
(``event_index = 160, 164``) with ``event_type = ‘group_alias’``).

.. code-block:: python

    res[res['user_id'] == 456870964]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>157</th>
          <td>b1102d92-8152-4aa7-859d-a20e3090c252</td>
          <td>raw</td>
          <td>157</td>
          <td>catalog</td>
          <td>2019-11-03 11:46:55.411714</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>158</th>
          <td>9bbf2cdd-e153-4abb-a652-a99118a42db2</td>
          <td>raw</td>
          <td>158</td>
          <td>catalog</td>
          <td>2019-11-03 11:47:46.131302</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>159</th>
          <td>c1eceba5-81e1-4810-bf27-a57bc1f0a83f</td>
          <td>raw</td>
          <td>159</td>
          <td>catalog</td>
          <td>2019-11-03 11:47:58.401143</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>160</th>
          <td>e8479863-bea8-4162-b37a-e23ef6d9bc7c</td>
          <td>group_alias</td>
          <td>160</td>
          <td>product</td>
          <td>2019-11-03 11:48:43.243587</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>162</th>
          <td>e08b3c04-f2ca-41ce-a913-d36ecc3c604d</td>
          <td>raw</td>
          <td>162</td>
          <td>cart</td>
          <td>2019-11-03 11:49:17.050519</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>163</th>
          <td>36efca4e-af57-426d-acd4-7cce2b92ace9</td>
          <td>raw</td>
          <td>163</td>
          <td>catalog</td>
          <td>2019-11-03 11:49:17.516398</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>164</th>
          <td>82ea6d58-631d-4832-affb-baacd1862391</td>
          <td>group_alias</td>
          <td>164</td>
          <td>product</td>
          <td>2019-11-03 11:49:28.927721</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>166</th>
          <td>769e8525-25bd-4494-aa15-718dde81ec3c</td>
          <td>raw</td>
          <td>166</td>
          <td>catalog</td>
          <td>2019-11-03 11:49:30.788195</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>167</th>
          <td>ceef23f1-20e6-4e20-87f3-98bcca0398f3</td>
          <td>raw</td>
          <td>167</td>
          <td>lost</td>
          <td>2019-11-03 11:49:31.788195</td>
          <td>456870964</td>
        </tr>
      </tbody>
    </table>
    </div>



We can also make sure that previously these events were named as
``product1`` and ``product2`` and had ``raw`` event type.

.. code-block:: python

    stream.to_dataframe().query('user_id == 456870964')




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>140</th>
          <td>b1102d92-8152-4aa7-859d-a20e3090c252</td>
          <td>raw</td>
          <td>140</td>
          <td>catalog</td>
          <td>2019-11-03 11:46:55.411714</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>141</th>
          <td>9bbf2cdd-e153-4abb-a652-a99118a42db2</td>
          <td>raw</td>
          <td>141</td>
          <td>catalog</td>
          <td>2019-11-03 11:47:46.131302</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>142</th>
          <td>c1eceba5-81e1-4810-bf27-a57bc1f0a83f</td>
          <td>raw</td>
          <td>142</td>
          <td>catalog</td>
          <td>2019-11-03 11:47:58.401143</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>143</th>
          <td>ee964e27-7ae0-4d19-9653-f141a1842321</td>
          <td>raw</td>
          <td>143</td>
          <td>product1</td>
          <td>2019-11-03 11:48:43.243587</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>144</th>
          <td>e08b3c04-f2ca-41ce-a913-d36ecc3c604d</td>
          <td>raw</td>
          <td>144</td>
          <td>cart</td>
          <td>2019-11-03 11:49:17.050519</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>145</th>
          <td>36efca4e-af57-426d-acd4-7cce2b92ace9</td>
          <td>raw</td>
          <td>145</td>
          <td>catalog</td>
          <td>2019-11-03 11:49:17.516398</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>146</th>
          <td>980c2915-b039-4e9c-830f-83bbce4428c0</td>
          <td>raw</td>
          <td>146</td>
          <td>product2</td>
          <td>2019-11-03 11:49:28.927721</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>147</th>
          <td>769e8525-25bd-4494-aa15-718dde81ec3c</td>
          <td>raw</td>
          <td>147</td>
          <td>catalog</td>
          <td>2019-11-03 11:49:30.788195</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>148</th>
          <td>ceef23f1-20e6-4e20-87f3-98bcca0398f3</td>
          <td>raw</td>
          <td>148</td>
          <td>lost</td>
          <td>2019-11-03 11:49:31.788195</td>
          <td>456870964</td>
        </tr>
      </tbody>
    </table>
    </div>



You can also notice that newly created ``product`` events have
``event_id`` different from their parent’s event_ids.

CollapseLoops
^^^^^^^^^^^^^

``CollapseLoops`` replaces all uninterrupted series of repetitive user
events (loops) with one new ``loop``-like event. The exact name of the
new event is defined by the suffix parameter:

-  given ``suffix=None``, names new event event_name, i.e. passes along
   the name of the repeating event;

-  given ``suffix="loop"``, names new event ``event_name_loop``;

-  given ``suffix="count"``, names new event
   ``event_name_loop_{number of event repetitions}``.

The timestamp that the new event has is determined by
``timestamp_aggregation_type`` value:

-  given ``timestamp_aggregation_type="max"`` (the default option),
   passes the timestamp of the last event from the loop;

-  given ``timestamp_aggregation_type="min"``, passes the timestamp of
   the first event from the loop;

-  given ``timestamp_aggregation_type="mean"``, passes the average loop
   timestamp.

.. figure:: /_static/user_guides/data_processor/dp_13_collapse_loops.png


.. code-block:: python

    res = stream.collapse_loops().to_dataframe()

Consider for example user ``2112338``. In the original eventstream she
had 3 consecutive ``catalog`` events.

.. code-block:: python

    stream.to_dataframe().query('user_id == 2112338')




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>3550</th>
          <td>1ffc8237-6105-4173-bdef-1206e5d21076</td>
          <td>raw</td>
          <td>3550</td>
          <td>main</td>
          <td>2019-12-24 12:58:04.891249</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>3551</th>
          <td>dc7ab63f-542f-4ae9-ba0a-582c5f2fd6ed</td>
          <td>raw</td>
          <td>3551</td>
          <td>catalog</td>
          <td>2019-12-24 12:58:08.096923</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>3552</th>
          <td>36fc09c0-e281-42a5-be66-59e2586b82b5</td>
          <td>raw</td>
          <td>3552</td>
          <td>catalog</td>
          <td>2019-12-24 12:58:16.429552</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>3553</th>
          <td>463acbf2-c030-4eb0-81fc-ee0303ff699d</td>
          <td>raw</td>
          <td>3553</td>
          <td>catalog</td>
          <td>2019-12-24 12:58:44.965104</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>3554</th>
          <td>10844313-e362-4f3c-8881-172f73ce50dc</td>
          <td>raw</td>
          <td>3554</td>
          <td>main</td>
          <td>2019-12-24 12:58:52.984853</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>3555</th>
          <td>bdc752f4-7cbe-4017-97e7-0548bc23198f</td>
          <td>raw</td>
          <td>3555</td>
          <td>lost</td>
          <td>2019-12-24 12:58:53.984853</td>
          <td>2112338</td>
        </tr>
      </tbody>
    </table>
    </div>



In the result dataframe these events have been collapsed to a single
``catalog_loop`` event. The timestamp of this synthetic event is the
same as the timestamp of the last looping event:
``2019-12-24 12:58:44``.

.. code-block:: python

    res[res['user_id'] == 2112338]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>5061</th>
          <td>1ffc8237-6105-4173-bdef-1206e5d21076</td>
          <td>raw</td>
          <td>5061</td>
          <td>main</td>
          <td>2019-12-24 12:58:04.891249</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>5066</th>
          <td>6136b972-871f-48db-9917-e9074411552b</td>
          <td>group_alias</td>
          <td>5066</td>
          <td>catalog_loop</td>
          <td>2019-12-24 12:58:44.965104</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>5069</th>
          <td>10844313-e362-4f3c-8881-172f73ce50dc</td>
          <td>raw</td>
          <td>5069</td>
          <td>main</td>
          <td>2019-12-24 12:58:52.984853</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>5070</th>
          <td>bdc752f4-7cbe-4017-97e7-0548bc23198f</td>
          <td>raw</td>
          <td>5070</td>
          <td>lost</td>
          <td>2019-12-24 12:58:53.984853</td>
          <td>2112338</td>
        </tr>
      </tbody>
    </table>
    </div>



To see the length of the loops we removed, we can set suffix to
``count``. Also, let’s see how ``timestamp_aggregation_type`` works if
we set it to ``mean``.

.. code-block:: python

    params = {
        'suffix': 'count',
        'timestamp_aggregation_type': 'mean'
    }

    res = stream.collapse_loops(**params).to_dataframe()
    res[res['user_id'] == 2112338]




.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>5071</th>
          <td>1ffc8237-6105-4173-bdef-1206e5d21076</td>
          <td>raw</td>
          <td>5071</td>
          <td>main</td>
          <td>2019-12-24 12:58:04.891249000</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>5076</th>
          <td>9d63ac98-b403-43b6-addd-25c2df3f779b</td>
          <td>group_alias</td>
          <td>5076</td>
          <td>catalog_loop_3</td>
          <td>2019-12-24 12:58:23.163859712</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>5079</th>
          <td>10844313-e362-4f3c-8881-172f73ce50dc</td>
          <td>raw</td>
          <td>5079</td>
          <td>main</td>
          <td>2019-12-24 12:58:52.984853000</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>5080</th>
          <td>bdc752f4-7cbe-4017-97e7-0548bc23198f</td>
          <td>raw</td>
          <td>5080</td>
          <td>lost</td>
          <td>2019-12-24 12:58:53.984853000</td>
          <td>2112338</td>
        </tr>
      </tbody>
    </table>
    </div>



Now synthetic ``catalog_loop_3`` event has ``12:58:23`` time which is
the average of ``12:58:08``, ``12:58:16``, ``12:58:44``.

The ``CollapseLoops`` dataprocessor can be useful for compressing the
data - by packing the loop information into one event, - or removal of
looping events, in case they are not desirable (which can be a common
case in clickstream visualization).

Custom data processors
----------------------

We have made a review of all data processors that currently exist in our
library.

But if you need specific data transformations that you have to do
regularly, then you can make a custom dataprocessor. For details see our
custom dataprocessors UG.


:red:`TODO: Create UG and add link. dpanina`

Advanced usage example
----------------------

Let us give an example of a processing graph that an analyst could use
to prepare the data for analysis or visualization. We are using the same
simple-onlineshop dataset we have seen before.

If we try to visualize the data without using dataprocessors, we can get
results that are difficult to analyze:

.. code-block:: python

    from retentioneering.transition_graph import TransitionGraph

    tgraph = TransitionGraph(eventstream=stream, graph_settings={})
    tgraph.plot_graph(thresholds={'nodes': None, 'edges': None}, targets=None)



.. raw:: html


    <iframe
        width="900"
        height="800"
        src="../_static/user_guides/data_processor/transition_graph.html"
        frameborder="0"
        align="left"
        allowfullscreen
    ></iframe>



Of course, by using the (quite impressive) transition graph interactive
options, we could focus on specific event transitions. However, even the
general user workflow can be difficult to see - because of big amount of
ungrouped events, loops, and states.

We can address this problem by using a combination of dataprocessors we
have seen previously. One example of a processing graph would look like
this:

-  apply **DeleteUsersByPathLength** to remove users that could have
   appeared by accident;

-  apply **StartEndEvents** to mark the start and finish user states;

-  apply **SplitSessions** to mark user sessions;

-  apply **GroupEvents** multiple times to group similar events into
   groups;

-  apply **CollapseLoops** with different parameters, for different loop
   representation on the transition graph plot

.. figure:: /_static/user_guides/data_processor/dp_14_pgraph_advanced.png



As the result, we should get three similar eventstreams that differ only
in their way of encoding loops. That is the main inherent advantage of
using the graph structure for transformations - we only need to execute
all common dataprocessors once, and then we can quickly alternate
between different “heads” of the transformation.

Let us compose this graph:

.. code-block:: python

    def group_browsing(df, schema):
        return df[schema.event_name].isin(['catalog', 'main'])

    def group_products(df, schema):
        return df[schema.event_name].isin(['product1', 'product2'])

    def group_delivery(df, schema):
        return df[schema.event_name].isin(['delivery_choice', 'delivery_courier', 'delivery_pickup'])

    def group_payment(df, schema):
        return df[schema.event_name].isin(['payment_choice', 'payment_done', 'payment_card', 'payment_cash'])


    stream_7_nodes = stream.delete_users(events_num=6)\
                            .add_start_end()\
                            .split_sessions(session_cutoff=(30, 'm'))\
                            .group(event_name='browsing', func=group_browsing)\
                            .group(event_name='delivery', func=group_delivery)\
                            .group(event_name='payment', func=group_payment)

Looking at the simpliest version, where loops are replaced with the
event they consist of:

.. code-block:: python

    stream_out = stream_7_nodes.collapse_loops(suffix=None)
    tgraph = TransitionGraph(eventstream=stream_out, graph_settings={})
    tgraph.plot_graph(thresholds={'nodes': None, 'edges': None}, targets=None)



.. raw:: html

    <iframe
        width="900"
        height="800"
        src="../_static/user_guides/data_processor/transition_graph_collapse_loops_none.html"
        frameborder="0"
        align="left"
        allowfullscreen
    ></iframe>


This transition graph is much easier to look at. After applying the
dataprocessors, we can now see that:

-  “lost” is a synthetic event marking an end of user trajectory, and
   should be removed in the next graph version(to be fair, we could see
   this in the first plot as well; however, details like this are very
   easy to miss, and simplifying the resulting plot alleviates the
   problem)

-  users start their sessions either by browsing(most users) or by going
   to cart(small but noticeable share of users who probably spent over
   30 minutes on product specifications)

-  after finishing a session, about 47.5% of users leave the website for
   good

-  after transitioning from “cart” to “delivery”, about 30% of users do
   not proceed to “payment”

We can also see the general user flow quite clearly now, which is a huge
improvement compared to the original plot.

To learn more about loops and where they occur, let us plot two other
versions of the eventstream:

.. code-block:: python

    stream_out = stream_7_nodes.collapse_loops(suffix='loop')
    tgraph = TransitionGraph(eventstream=stream_out, graph_settings={})
    tgraph.plot_graph(thresholds={'nodes': None, 'edges': None}, targets=None)



.. raw:: html

    <iframe
        width="900"
        height="800"
        src="../_static/user_guides/data_processor/transition_graph_collapse_loops_loop.html"
        frameborder="0"
        align="left"
        allowfullscreen
    ></iframe>


In this plot (which is a bit more convoluted than the previous one), we
see that loops mostly occur when users are browsing, and are less
frequent at the ``delivery`` or ``payment stages``. However, there are a
lot more transitions to ``payment_loop`` or ``delivery_loop`` than there
are to ``payment`` or ``delivery``!

This could suggest that there is a problem with the delivery/payment
process, or that we could improve the process by reducing the number of
transitions (i.e. “clicks”) it takes to make an order a delivery or to
pay.

Now we can attempt to look at the typical loop length using the third
created eventstream:

.. code-block:: python

    stream_out = stream_7_nodes.collapse_loops(suffix='count')
    tgraph = TransitionGraph(eventstream=stream_out, graph_settings={})
    tgraph.plot_graph(thresholds={'nodes': None, 'edges': None}, targets=None)



.. raw:: html

     <iframe
        width="900"
        height="800"
        src="../_static/user_guides/data_processor/transition_graph_collapse_loops_count.html"
        frameborder="0"
        align="left"
        allowfullscreen
    ></iframe>


This plot is much more complex than the previous two; to properly
analyze it, we would need to filter out some loop events based on their
frequency. Still, we can see that the longest loops occur at the
browsing stage - and cart, payment, or delivery loops are limited by 2-3
steps, meaning that the problem we found might not be as critical as it
first appeared.
