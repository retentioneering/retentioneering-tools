.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red


DataProcessor user guide
=========================

The following user guide is also available as
`Google Colab <https://colab.research.google.com/drive/1uXTt14stXKjWR_paEzqPl5_rZLFyclrm?usp=share_link>`_.


Creating an eventstream
-----------------------

Here we use ``simple_shop`` dataset, which we load as an ``Eventstream`` object. You can learn more about
``Eventstream`` in our :doc:`eventstream guide<eventstream>`.
To get an overview of the eventstream concept, see :doc:`this guide<../getting_started/eventstream_concept>`.

.. code-block:: python

    import pandas as pd
    from retentioneering import datasets

    stream = datasets.load_simple_shop()

What is a DataProcessor?
------------------------

Each ``Data Processor`` represents an algorithm that modifies eventstream data.

Data processors are designed to be nodes of a
``Preprocessing graph``, which allows us to apply data processors sequentially, in a custom order.

More about preprocessing graph see in :doc:`preprocessing guide<preprocessing>`.


.. figure:: /_static/user_guides/data_processor/dp_0_PGraph.png


.. _helpers_and_chain_usage:

Helpers and chain usage
-----------------------

A ``Helper`` is an ``Eventstream`` method that applies a single data processor to the data. It is a useful shortcut for
when one wants to avoid creating a preprocessing graph. Each data processor has a corresponding helper method -
the table below showcases the mapping between them:

.. table:: Data processors overview
    :align: center
    :widths: 15 60
    :class: tight-table

    +--------------------------------------------+-----------------------------------------------------+
    | | Data processor                           | What it does                                        |
    | | Helper                                   |                                                     |
    +============================================+=====================================================+
    | | StartEndEvents                           | Adds two synthetic events in each user’s path:      |
    | | :ref:`add_start_end<add_start_end>`      | ``path_start`` and ``path_end``.                    |
    |                                            |                                                     |
    +--------------------------------------------+-----------------------------------------------------+
    | | SplitSessions                            | Cuts user path into sessions and adds synthetic     |
    | | :ref:`split_sessions<split_sessions>`    | events ``session_start``, ``session_end``.          |
    |                                            |                                                     |
    +--------------------------------------------+-----------------------------------------------------+
    | | NewUsersEvents                           | Adds synthetic event ``new_user`` in the beginning  |
    | | :ref:`add_new_users<add_new_users>`      | of a user’s path if the user is considered as new.  |
    |                                            | Otherwise adds ``existing_user``.                   |
    |                                            |                                                     |
    +--------------------------------------------+-----------------------------------------------------+
    | | LostUsersEvents                          | Adds synthetic event ``lost_user`` in the end of    |
    | | :ref:`lost_users<lost_users>`            | user’s path if the user never comes back to the     |
    |                                            | product. Otherwise adds ``absent_user`` event.      |
    |                                            |                                                     |
    +--------------------------------------------+-----------------------------------------------------+
    | | PositiveTarget                           | Adds synthetic event ``positive_target`` for all    |
    | | :ref:`positive_target<positive_target>`  | events which are considered as positive.            |
    |                                            |                                                     |
    +--------------------------------------------+-----------------------------------------------------+
    | | NegativeTarget                           | Adds synthetic event ``negative_target`` for all    |
    | | :ref:`negative_target<negative_target>`  | events which are considered as positive.            |
    |                                            |                                                     |
    +--------------------------------------------+-----------------------------------------------------+
    | | TruncatedEvents                          | Adds synthetic events ``truncated_left`` and/or     |
    | | :ref:`truncated_events<truncated_events>`| ``truncated_right`` for those user paths which are  |
    |                                            | considered as truncated by the edges of the whole   |
    |                                            | dataset.                                            |
    +--------------------------------------------+-----------------------------------------------------+
    | | FilterEvents                             | Removes events from an eventstream.                 |
    | | :ref:`filter<filter>`                    |                                                     |
    +--------------------------------------------+-----------------------------------------------------+
    | | DeleteUsersByPathLength                  | Removes a too short user paths (in terms of number  |
    | | :ref:`delete_users<delete_users>`        | of events or time duration).                        |
    |                                            |                                                     |
    +--------------------------------------------+-----------------------------------------------------+
    | | TruncatePath                             | Leaves a part of an eventstream between a couple    |
    | | :ref:`truncate_path<truncate_path>`      | of selected events.                                 |
    |                                            |                                                     |
    +--------------------------------------------+-----------------------------------------------------+
    | | GroupEvents                              | Groups given events into a single synthetic event.  |
    | | :ref:`group<group>`                      |                                                     |
    +--------------------------------------------+-----------------------------------------------------+
    | | CollapseLoops                            | Groups sequences of repetitive events with new      |
    | | :ref:`collapse_loops<collapse_loops>`    | synthetic events. E.g. ``A, A, A -> A``.            |
    +--------------------------------------------+-----------------------------------------------------+

Method chaining is supported for ``helpers`` as it is present in other
python libraries, for example in Pandas.

Using helper methods, we can replicate the *General Usage* coding blocks output:

.. code-block:: python

    res = stream.add_start_end().split_sessions(session_cutoff=(10, 'm')).to_dataframe()
    res[res['user_id'] == 219483890]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>path_start</td>
          <td>0</td>
          <td>path_start</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>2</th>
          <td>session_start</td>
          <td>2</td>
          <td>session_start</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>3</th>
          <td>raw</td>
          <td>3</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>11</th>
          <td>session_end</td>
          <td>11</td>
          <td>session_end</td>
          <td>2019-11-01 17:59:32</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>6256</th>
          <td>session_start</td>
          <td>6256</td>
          <td>session_start</td>
          <td>2019-12-06 16:22:57</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>23997</th>
          <td>session_end</td>
          <td>23997</td>
          <td>session_end</td>
          <td>2020-02-14 21:04:52</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
        <tr>
          <th>23998</th>
          <td>path_end</td>
          <td>23998</td>
          <td>path_end</td>
          <td>2020-02-14 21:04:52</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
      </tbody>
    </table>
    <br>

We will also use ``helpers`` in all further examples below.
See complex example in our preprocessing user guide :ref:`general usage<preprocessing_solution_plan>`
and :ref:`method chaining<chain_usage_complex_example>`.


.. _dataprocessors_library:

Data Processors library
-----------------------

Data processors can be partitioned into three groups:

- Adding: processors that add events to an eventstream;
- Removing: processors that remove events from an eventstream;
- Editing: processors that modify existing events in an eventstream (including grouping operations).

Adding processors
~~~~~~~~~~~~~~~~~

The processors of that type add some artificial (*synthetic*) events to an eventstream.
Let us go through each of them.

.. _add_start_end:

StartEndEvents
^^^^^^^^^^^^^^

For each user, :py:meth:`StartEndEvents<retentioneering.data_processors_lib.start_end_events.StartEndEvents>`
generates an event called ``path_start`` right before the first user event, and an event
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
          <td>path_start</td>
          <td>0</td>
          <td>path_start</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>raw</td>
          <td>1</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>10213</th>
          <td>path_end</td>
          <td>10213</td>
          <td>path_end</td>
          <td>2020-02-14 21:04:52</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    <br>

As the DataFrame above shows, the generated events ``path_start``
and ``path_end`` have identical timestamps as the corresponding first and
last events.

We recommend applying this data processor each time you analyze an
eventstream - since it explicitly sets the borders of an eventstream. It
can help plot and explore user lifetime across all users,
or conveniently display user trajectory borders in
``TransitionGraph``, ``StepMatrix``, and ``StepSankey`` tools.

.. _split_sessions:

SplitSessions
^^^^^^^^^^^^^

:py:meth:`SplitSessions<retentioneering.data_processors_lib.split_sessions.SplitSessions>`
data processor cuts user paths into sessions based on the defined ``session_cutoff``
timeout parameter. For each session, it creates a couple of synthetic
events ``session_start`` and ``session_end``, like
``StartEndEvents``. Session identifiers are formed according to the
template ``<user_id>_<user_session_number>`` and can be found in
``session_id`` column. The ``user_session_number`` is associated with a
session ordinal number within a user path and always starts with 1.

.. figure:: /_static/user_guides/data_processor/dp_2_split_sessions.png

Applying ``SplitSessions`` to split user paths into sessions with
session cutoff=10 minutes:

.. code-block:: python

    res = stream.split_sessions(session_cutoff=(10, 'm')).to_dataframe()
    res[res['user_id'] == 219483890]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>session_start</td>
          <td>0</td>
          <td>session_start</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>1</th>
          <td>raw</td>
          <td>1</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>9</th>
          <td>session_end</td>
          <td>9</td>
          <td>session_end</td>
          <td>2019-11-01 17:59:32</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>5316</th>
          <td>session_start</td>
          <td>5316</td>
          <td>session_start</td>
          <td>2019-12-06 16:22:57</td>
          <td>219483890</td>
          <td>219483890_2</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>21049</th>
          <td>session_end</td>
          <td>21049</td>
          <td>session_end</td>
          <td>2020-02-14 21:04:52</td>
          <td>219483890</td>
          <td>219483890_4</td>
        </tr>
      </tbody>
    </table>
    <br>

The result for one user is displayed above. We see that the user
trajectory is partitioned into three sessions. The time distance between
consecutive events within each session is less than 10 minutes.

Splitting user paths into sessions is an essential step in clickstream
analysis. Sometimes, it needs to be clarified which session cutoff to
choose. In such cases, generating multiple session splits and comparing them
in some fashion can be a good practice.

It can be helpful to explore the distribution between all consecutive events
in each user path. For this purpose you can use one of eventstream descriptive methods
:py:meth:`TimedeltaHist<retentioneering.tooling.timedelta_hist.TimedeltaHist>`
See more about :ref:`eventstream descriptive methods<eventstream_descriptive_methods>`.


.. _add_new_users:

NewUsersEvents
^^^^^^^^^^^^^^

Given a list of users (considered "new"), the
:py:meth:`NewUsersEvents<retentioneering.data_processors_lib.new_users.NewUsersEvents>`
data processor labels those users in an eventstream by adding a synthetic ``new_user``
event to each user trajectory start. For all other users, adds an
``existing_user`` synthetic event. All users will be labeled as new when
passed 'all' instead of a list.

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
          <td>new_user</td>
          <td>0</td>
          <td>new_user</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>raw</td>
          <td>1</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>raw</td>
          <td>2</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>raw</td>
          <td>3</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>4</th>
          <td>raw</td>
          <td>4</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    <br>

We can see that user ``219483890`` is marked as a new user.

But user ``501098384`` is marked as an existing user:

.. code-block:: python

    res[res['user_id'] == 501098384].head()


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>existing_user</td>
          <td>17387</td>
          <td>existing_user</td>
          <td>2020-04-02 05:36:04</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>17388</th>
          <td>raw</td>
          <td>17388</td>
          <td>main</td>
          <td>2020-04-02 05:36:04</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>17389</th>
          <td>raw</td>
          <td>17389</td>
          <td>catalog</td>
          <td>2020-04-02 05:36:05</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>17390</th>
          <td>raw</td>
          <td>17390</td>
          <td>main</td>
          <td>2020-04-02 05:36:40</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>17391</th>
          <td>raw</td>
          <td>17391</td>
          <td>catalog</td>
          <td>2020-04-02 05:36:41</td>
          <td>501098384</td>
        </tr>
      </tbody>
    </table>
    <br>

This data processor can be helpful when you have data that chronologically
precedes the clickstream you are working with. For instance, your clickstream
might cover 1-month of user data, and also you have the user login data
for the whole year. In that case, you can use ``NewUsersEvents``
to split users into two categories:

- new users,
- users who have appeared this year before.

.. _lost_users:

LostUsersEvents
^^^^^^^^^^^^^^^

Given a list of users (considered "lost"), the
:py:meth:`LostUsersEvents<retentioneering.data_processors_lib.lost_users.LostUsersEvents>`
data processor labels those users by adding a synthetic ``lost_user`` event to each
user trajectory end. For all other users, adds an
``absent_user`` synthetic event. When passed a ``lost_cutoff`` timedelta value,
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
          <td>raw</td>
          <td>5175</td>
          <td>catalog</td>
          <td>2020-01-06 22:11:28</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9329</th>
          <td>raw</td>
          <td>9329</td>
          <td>main</td>
          <td>2020-02-14 21:04:49</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9330</th>
          <td>raw</td>
          <td>9330</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>9332</th>
          <td>lost_user</td>
          <td>9332</td>
          <td>lost_user</td>
          <td>2020-02-14 21:04:52</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    <br>

As opposed to user ``219483890``, the user ``501098384`` is labeled as an
``absent_user``.

.. code-block:: python

    res[res['user_id'] == 501098384].tail()


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>39127</td>
          <td>catalog</td>
          <td>2020-04-29 12:48:01</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>39128</th>
          <td>raw</td>
          <td>39128</td>
          <td>main</td>
          <td>2020-04-29 12:48:01</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>39129</th>
          <td>raw</td>
          <td>39129</td>
          <td>catalog</td>
          <td>2020-04-29 12:48:06</td>
          <td>501098384</td>
        </tr>
        <tr>
          <th>39130</th>
          <td>absent_user</td>
          <td>39130</td>
          <td>absent_user</td>
          <td>2020-04-29 12:48:06</td>
          <td>501098384</td>
        </tr>
      </tbody>
    </table>
    <br>

The function of this data processor is similar to
``NewUsersEvents``, except that it adds labels to the end
of user trajectory.

We can also run ``LostUsersEvents`` with ``lost_cutoff`` passed, to
arbitrarily label some users as lost. Assume we consider a user
absent if there have been no events for 30 days:

.. code-block:: python

    res = stream.lost_users(lost_cutoff=(30, 'D')).to_dataframe()


Before we inspect the results of applying the data processor,
notice that the eventstream ends at ``2020-04-29 12:48:07``.

.. code-block:: python

    res['timestamp'].max()


.. parsed-literal::

    Timestamp('2020-04-29 12:48:07.595390')


User ``495985018`` is labeled as lost since her last event occurred
on ``2019-11-02``. It’s more than 30 days before the end of the
eventstream.

.. code-block:: python

    res[res['user_id'] == 495985018]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>47</td>
          <td>catalog</td>
          <td>2019-11-02 01:14:08</td>
          <td>495985018</td>
        </tr>
        <tr>
          <th>48</th>
          <td>raw</td>
          <td>48</td>
          <td>cart</td>
          <td>2019-11-02 01:14:37</td>
          <td>495985018</td>
        </tr>
        <tr>
          <th>49</th>
          <td>lost_user</td>
          <td>49</td>
          <td>lost_user</td>
          <td>2019-11-02 01:14:37</td>
          <td>495985018</td>
        </tr>
      </tbody>
    </table>
    <br>

On the other hand, user ``819489198`` is labeled ``absent`` because
her last event occurred on ``2020-04-15``, less than 30 days
before ``2020-04-29``.

.. code-block:: python

    res[res['user_id'] == 819489198]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>26529</td>
          <td>main</td>
          <td>2020-04-15 21:02:36</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>26544</th>
          <td>raw</td>
          <td>26544</td>
          <td>payment_card</td>
          <td>2020-04-15 21:03:46</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26545</th>
          <td>raw</td>
          <td>26545</td>
          <td>payment_done</td>
          <td>2020-04-15 21:03:47</td>
          <td>819489198</td>
        </tr>
        <tr>
          <th>26546</th>
          <td>absent_user</td>
          <td>26546</td>
          <td>absent_user</td>
          <td>2020-04-15 21:03:47</td>
          <td>819489198</td>
        </tr>
      </tbody>
    </table>
    <br>

.. _positive_target:

PositiveTarget
^^^^^^^^^^^^^^

:py:meth:`PositiveTarget<retentioneering.data_processors_lib.positive_target.PositiveTarget>`
data processor supports two parameters:

-  ``positive_target_events`` - list of "positive" events
   (for instance, associated with some conversion goal of the user behavior)
-  ``func`` - this function accepts parent ``Eventstream`` as an
   argument and returns ``pandas.DataFrame`` contains only the lines
   of the events we would like to label as positive.

By default, for each user trajectory, an event from the
specified list (and minimum timestamp) is taken and cloned with
``positive_target_<EVENTNAME>`` as the ``event`` and ``positive_target``
type.


.. figure:: /_static/user_guides/data_processor/dp_5_positive.png

.. code-block:: python

    positive_events = ['cart', 'payment_done']
    res = stream.positive_target(
        positive_target_events=positive_events
        ).to_dataframe()

Consider user ``219483890``, whose ``cart`` event appeared in her
trajectory with ``event_index=2``. A synthetic event
``positive_target_cart`` is added right after it.

.. code-block:: python

    res[res['user_id'] == 219483890]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>0</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>raw</td>
          <td>1</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>raw</td>
          <td>2</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>positive_target</td>
          <td>3</td>
          <td>positive_target_cart</td>
          <td>2019-11-01 17:59:29</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>5116</th>
          <td>raw</td>
          <td>5116</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5117</th>
          <td>raw</td>
          <td>5117</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:52</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>9187</th>
          <td>raw</td>
          <td>9187</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    <br>

In opposite to this user, user ``24427596`` has no positive events, so
her path remains unchanged:

.. code-block:: python

    res[res['user_id'] == 24427596]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>68</td>
          <td>main</td>
          <td>2019-11-02 07:28:07</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>69</th>
          <td>raw</td>
          <td>69</td>
          <td>catalog</td>
          <td>2019-11-02 07:28:14</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>71</th>
          <td>raw</td>
          <td>71</td>
          <td>catalog</td>
          <td>2019-11-02 07:29:42</td>
          <td>24427596</td>
        </tr>
      </tbody>
    </table>
    <br>

This data processor can make it easier to label events that we would
like to consider as positive. It might be helpful for further analysis
with tools like ``TransitionGraph``, ``StepMatrix``, and
``SankeyStep`` - as it will help to highlight the positive events.

Another way to set positive events is to pass a custom function in ``func``.
For example, assume we need to mark each ``positive_target_event`` in a trajectory, not just the
first one:

.. code-block:: python

    def custom_func(eventstream, positive_target_events) -> pd.DataFrame:

        event_col = eventstream.schema.event_name
        df = eventstream.to_dataframe()

        return df[df[event_col].isin(positive_target_events)]

    res = stream.positive_target(
              positive_target_events=positive_events,
              func=custom_func
              ).to_dataframe()


.. code-block:: python

    res[res['user_id'] == 219483890]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>0</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>raw</td>
          <td>1</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>raw</td>
          <td>2</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>positive_target</td>
          <td>3</td>
          <td>positive_target_cart</td>
          <td>2019-11-01 17:59:29</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>5116</th>
          <td>raw</td>
          <td>5116</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5117</th>
          <td>positive_target</td>
          <td>5117</td>
          <td>positive_target_cart</td>
          <td>2020-01-06 22:10:42</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5118</th>
          <td>raw</td>
          <td>5118</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:52</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>9188</th>
          <td>raw</td>
          <td>9188</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    <br>

.. _negative_target:

NegativeTarget
^^^^^^^^^^^^^^

The idea of
:py:meth:`NegativeTarget<retentioneering.data_processors_lib.negative_target.NegativeTarget>`
data processor is the same as ``PositiveTarget``, but
applied to negative labels instead of positive ones.

-  ``negative_target_events`` - list of "positive" ``events``
    (for instance, associated with some negative result of the user behavior)
-  ``func`` - this function accepts parent ``Eventstream`` as an
   argument and returns ``pandas.DataFrame``, which contains only the lines
   of the events we would like to label as negative.


.. figure:: /_static/user_guides/data_processor/dp_6_negative.png

.. code-block:: python

    negative_events = ['delivery_courier']

    res = stream.negative_target(
              negative_target_events=negative_events
              ).to_dataframe()

Works similarly to the ``PositiveTarget`` data processor - in this
case, it will add negative event next to the ``delivery_courier`` event:

.. code-block:: python

    res[res['user_id'] == 629881394]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>7</th>
          <td>raw</td>
          <td>7</td>
          <td>main</td>
          <td>2019-11-01 22:28:54</td>
          <td>629881394</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>39</th>
          <td>raw</td>
          <td>39</td>
          <td>delivery_courier</td>
          <td>2019-11-01 22:36:02</td>
          <td>629881394</td>
        </tr>
        <tr>
          <th>41</th>
          <td>negative_target</td>
          <td>41</td>
          <td>negative_target_delivery_courier</td>
          <td>2019-11-01 22:36:02</td>
          <td>629881394</td>
        </tr>
        <tr>
          <th>44</th>
          <td>raw</td>
          <td>44</td>
          <td>payment_choice</td>
          <td>2019-11-01 22:36:02</td>
          <td>629881394</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>..</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>13724</th>
          <td>raw</td>
          <td>13724</td>
          <td>catalog</td>
          <td>2020-03-30 03:19:59</td>
          <td>629881394</td>
        </tr>
      </tbody>
    </table>
    <br>

.. _truncated_events:

TruncatedEvents
^^^^^^^^^^^^^^^

:py:meth:`TruncatedEvents<retentioneering.data_processors_lib.truncated_events.TruncatedEvents>`
addresses a common practical problem, when some trajectories are
truncated due to the dataset’s natural boundaries.

.. figure:: /_static/user_guides/data_processor/dp_7_truncate_timeline.png

The diagram above illustrates this problem. Consider two user paths –
blue and orange. In
reality, the blue path started before the beginning of the eventstream.
But we cannot observe that - since we haven’t access to the events to the
left from the beginning of the eventstream.
So, instead of the actual start of the user path, we observe a "false"
beginning, and the observed trajectory is truncated.

A similar situation occurs with the orange user path. Instead of the
actual trajectory end, we only observe the "false" trajectory end.

One possible way to mark truncated paths is to detect
trajectories that are "too short" for a typical trajectory, and
whose shortness can be attributed to being truncated.

``TruncatedEvents`` data processor uses passed ``left_truncated_cutoff`` and
``right_truncated_cutoff`` timedeltas and labels user trajectories as
``truncated_left`` or ``truncated_right`` based on the following
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



Sometimes, it can be a good practice to use different cutoff values and
compare them in some fashion to select the best.

It can be helpful to use
:py:meth:`TimedeltaHist<retentioneering.tooling.timedelta_hist.TimedeltaHist>` method
with specified ``event_pair=('eventstream_start', 'path_end')`` for choosing ``left_truncated_cutoff``
value and ``event_pair=('path_start', 'eventstream_end')`` for choosing ``right_truncated_cutoff``.

See more about :ref:`eventstream descriptive methods<eventstream_descriptive_methods>`.


.. code-block:: python

    params = {
        'left_truncated_cutoff': (4, 'D'),
        'right_truncated_cutoff': (3, 'D')
    }

    res = stream.truncated_events(**params).to_dataframe()

Displaying the eventstream start and end timestamps:

.. code-block:: python

    print('Eventstream start: {}'.format(res.timestamp.min()))
    print('Eventstream end: {}'.format(res.timestamp.max()))


.. parsed-literal::

    Eventstream start: 2019-11-01 17:59:13.273932
    Eventstream end: 2020-04-29 12:48:07.595390


The trajectory of the following user ends at ``2019-11-02 01:14:38`` - which is too
close to the eventstream start(for the given ``left_truncated_cutoff``
value), so the ``TruncatedEvents`` data processor labels it as truncated
from the left:

.. code-block:: python

    res[res['user_id'] == 495985018]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>truncated_left</td>
          <td>47</td>
          <td>truncated_left</td>
          <td>2019-11-02 01:14:08</td>
          <td>495985018</td>
        </tr>
        <tr>
          <th>48</th>
          <td>raw</td>
          <td>48</td>
          <td>catalog</td>
          <td>2019-11-02 01:14:08</td>
          <td>495985018</td>
        </tr>
        <tr>
          <th>49</th>
          <td>raw</td>
          <td>49</td>
          <td>cart</td>
          <td>2019-11-02 01:14:37</td>
          <td>495985018</td>
        </tr>
      </tbody>
    </table>
    <br>

The trajectory of the following user starts at ``2020-04-29 12:24:21`` - which is too
close to the eventstream end(for the given ``right_truncated_cutoff``
value), so
the ``TruncatedEvents`` data processor labels it as truncated from the
right:

.. code-block:: python

    res[res['user_id'] == 831491833]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>35627</td>
          <td>catalog</td>
          <td>2020-04-29 12:24:21</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35628</th>
          <td>raw</td>
          <td>35628</td>
          <td>catalog</td>
          <td>2020-04-29 12:24:33</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35629</th>
          <td>raw</td>
          <td>35629</td>
          <td>product2</td>
          <td>2020-04-29 12:24:39</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35630</th>
          <td>raw</td>
          <td>35630</td>
          <td>cart</td>
          <td>2020-04-29 12:24:59</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35631</th>
          <td>raw</td>
          <td>35631</td>
          <td>catalog</td>
          <td>2020-04-29 12:25:06</td>
          <td>831491833</td>
        </tr>
        <tr>
          <th>35632</th>
          <td>truncated_right</td>
          <td>35632</td>
          <td>truncated_right</td>
          <td>2020-04-29 12:25:06</td>
          <td>831491833</td>
        </tr>
      </tbody>
    </table>
    <br>


Removing processors
~~~~~~~~~~~~~~~~~~~

.. _filter:

FilterEvents
^^^^^^^^^^^^

:py:meth:`FilterEvents<retentioneering.data_processors_lib.filter_events.FilterEvents>`
keeps events based on the masking function ``func``.
The function should return a boolean mask for the input dataframe(a series
of boolean True or False variables that filter the DataFrame underlying
the eventstream).

.. figure:: /_static/user_guides/data_processor/dp_9_filter.png


Let us say we are interested only in specific events - for example, only
in events of users that appear in some pre-defined list of users.
``FilterEvents`` allows us to access only those events:

.. code-block:: python

    def save_specific_users(df, schema):
        users_to_save = [219483890, 964964743, 965024600]
        return df[schema.user_id].isin(users_to_save)

    res = stream.filter(func=save_specific_users).to_dataframe()

The resulting eventstream includes these three users only:

.. code-block:: python

    res['user_id'].unique().astype(int)


.. parsed-literal::

    array([219483890, 964964743, 965024600])


Note that the masking function accepts not just ``pandas.DataFrame``
associated with the eventstream, but ``schema`` parameter as well.
Having this parameter, you can access any eventstream column,
defined in its
:py:meth:`EventstreamSchema<retentioneering.eventstream.schema.EventstreamSchema>`.

This makes such masking functions reusable regardless of eventstream
column titles.

Using ``FilterEvents`` data processor, we can
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

We can see that ``res`` DataFrame does not have "useless" events anymore.

.. code-block:: python

    res['event']\
        .value_counts()\
        [lambda s: s.index.isin(['catalog', 'main'])]


.. parsed-literal::

    Series([], Name: event, dtype: int64)

.. _delete_users:

DeleteUsersByPathLength
^^^^^^^^^^^^^^^^^^^^^^^

:py:meth:`DeleteUsersByPathLength<retentioneering.data_processors_lib.delete_users_by_path_length.DeleteUsersByPathLength>`
removes the paths which we consider "too short". We might
be interested in excluding such paths - in case they are too short to
be informative for our task.

Path length can be specified in the following ways:

- setting the number of events comprising a path,
- setting the time distance between the beginning and the end of the path.

The former is associated with ``events_num`` parameter, the latter –
with ``cutoff`` parameter. Thus, ``DeleteUsersByPathLength`` removes all
the paths of length less than ``events_num`` or ``cutoff``.

Diagram for specified ``events_num``:

.. figure:: /_static/user_guides/data_processor/dp_10_delete_events.png


Diagram for specified ``cutoff``:

.. figure:: /_static/user_guides/data_processor/dp_10_delete_cutoff.png


Let us showcase both variants of the ``DeleteUsersByPathLength``
data processor:

A minimum number of events specified:

.. code-block:: python

    res = stream.delete_users(events_num=25).to_dataframe()

Any remaining user has at least 25 events. For example, user
``629881394`` has 48 events.

.. code-block:: python

    len(res[res['user_id'] == 629881394])


.. parsed-literal::

    48


A minimum path length (user lifetime) is specified:

.. code-block:: python

    res = stream.delete_users(cutoff=(1, 'M')).to_dataframe()


Any remaining user has been "alive" for at least a month. For
example, user ``964964743`` started her trajectory on ``2019-11-01`` and
ended on ``2019-12-09``.

.. code-block:: python

    res[res['user_id'] == 964964743].iloc[[0, -1]]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>4</td>
          <td>catalog</td>
          <td>2019-11-01 21:38:19</td>
          <td>964964743</td>
        </tr>
        <tr>
          <th>3457</th>
          <td>raw</td>
          <td>3457</td>
          <td>delivery_pickup</td>
          <td>2019-12-09 01:43:57</td>
          <td>964964743</td>
        </tr>
      </tbody>
    </table>
    <br>

.. _truncate_path:

TruncatePath
^^^^^^^^^^^^

For each user trajectory, :py:meth:`TruncatePath<retentioneering.data_processors_lib.truncate_path.TruncatePath>`
drops all events before or after a particular event.
The following parameters specify the behavior:

-  ``drop_before``: event name before which part of the user’s path is
   dropped. The specified event remains in the eventstream.

-  ``drop_after``: event name after which part of the user’s path is
   dropped. The specified event remains in the eventstream.

-  ``occurrence_before``: if set to ``first`` (by default), all events
   before the first occurrence of the ``drop_before`` event are dropped.
   If set to ``last``, all events before the last occurrence of the
   ``drop_before`` event are dropped.

-  ``occurrence_after``: the same behavior as in the
   ``occurrence_before``, but for right (after the event) path
   truncation.

-  ``shift_before``: sets the number of steps by which the truncate
   point is shifted from the selected event. If the value is negative,
   the offset occurs to the left along the timeline; if positive,
   then the offset occurs to the right.

-  ``shift_after``: the same behavior as in the shift_before, but for
   right (after the event) path truncation.

The path remains unchanged if the specified event is not present in a user path.

.. figure:: /_static/user_guides/data_processor/dp_11_truncate_path.png


Suppose we want to see what happens to the user after she jumps to a
``cart`` event and also to find out which events preceded the ``cart`` event.
To do this, we can use ``TruncatePath`` with specified
``drop_before='cart'`` and ``shift_before=-2``:

.. code-block:: python

    res = stream.truncate_path(
              drop_before='cart',
              shift_before=-2
              ).to_dataframe()

Now some users have their trajectories truncated, because they had at
least one ``cart`` in their path:

.. code-block:: python

    res[res['user_id'] == 219483890]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>0</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>raw</td>
          <td>1</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>raw</td>
          <td>2</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>raw</td>
          <td>3</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>10317</th>
          <td>raw</td>
          <td>10317</td>
          <td>catalog</td>
          <td>2020-02-14 21:04:51</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    <br>

As we can see, this path now starts with the two events preceding the
``cart`` (``event_index=0,1``) and the ``cart`` event right after them
(``event_index=2``). Another ``cart`` event occurred here
(``event_index=5827``), but since the default
``occurrence_before='first'`` was triggered, the data processor
ignored this second cart.

Some users do not have any ``cart`` events - and their
trajectories have not been changed:

.. code-block:: python

    res[res['user_id'] == 24427596]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>89</td>
          <td>main</td>
          <td>2019-11-02 07:28:07</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>90</th>
          <td>raw</td>
          <td>90</td>
          <td>catalog</td>
          <td>2019-11-02 07:28:14</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>91</th>
          <td>raw</td>
          <td>91</td>
          <td>catalog</td>
          <td>2019-11-02 07:29:08</td>
          <td>24427596</td>
        </tr>
        <tr>
          <th>92</th>
          <td>raw</td>
          <td>92</td>
          <td>catalog</td>
          <td>2019-11-02 07:29:41</td>
          <td>24427596</td>
        </tr>
      </tbody>
    </table>
    <br>

We can also perform truncation from the right, or specify for the truncation
point to be not the first but the last occurrence of the ``cart``. To
demonstrate both, let us set ``drop_after="cart"`` and
``occurrence_after="last"``:

.. code-block:: python

    res = stream.truncate_path(
              drop_after='cart',
              occurrence_after="last"
              ).to_dataframe()

Now, any trajectory which includes a ``cart`` is truncated to the end with the
last ``cart``:

.. code-block:: python

    res[res['user_id'] == 219483890]


.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>0</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>raw</td>
          <td>1</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>raw</td>
          <td>2</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>5639</th>
          <td>raw</td>
          <td>5639</td>
          <td>catalog</td>
          <td>2020-01-06 22:10:15</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>5640</th>
          <td>raw</td>
          <td>5640</td>
          <td>cart</td>
          <td>2020-01-06 22:10:42</td>
          <td>219483890</td>
        </tr>
      </tbody>
    </table>
    <br>

Editing processors
~~~~~~~~~~~~~~~~~~

.. _group:

GroupEvents
^^^^^^^^^^^

Given a masking function passed as a ``func``,
:py:meth:`GroupEvents<retentioneering.data_processors_lib.group_events.GroupEvents>` replaces
all the events marked by ``func`` with newly created synthetic events
of ``event_name`` name and ``event_type`` type (``group_alias`` by
default). The timestamps of these synthetic events are the same as their
parents'. ``func`` can be any function that returns a series of
boolean (``True/False``) variables that can be used as a filter for the
DataFrame underlying the eventstream.


.. figure:: /_static/user_guides/data_processor/dp_12_group.png



With ``GroupEvents``, we can group events based on the event name. Suppose
we need to assign a common name ``product`` to events ``product1`` and
``product2``:

.. code-block:: python

    def group_events(df, schema):
        events_to_group = ['product1', 'product2']
        return df[schema.event_name].isin(events_to_group)

    params = {
        'event_name': 'product',
        'func': group_events
    }

    res = stream.group(**params).to_dataframe()

As we can see, user ``456870964`` now has two ``product`` events
(``event_index=160, 164``) with ``event_type=‘group_alias’``).

.. code-block:: python

    res[res['user_id'] == 456870964]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>157</td>
          <td>catalog</td>
          <td>2019-11-03 11:46:55</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>158</th>
          <td>raw</td>
          <td>158</td>
          <td>catalog</td>
          <td>2019-11-03 11:47:46</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>159</th>
          <td>raw</td>
          <td>159</td>
          <td>catalog</td>
          <td>2019-11-03 11:47:58</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>160</th>
          <td>group_alias</td>
          <td>160</td>
          <td>product</td>
          <td>2019-11-03 11:48:43</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>162</th>
          <td>raw</td>
          <td>162</td>
          <td>cart</td>
          <td>2019-11-03 11:49:17</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>163</th>
          <td>raw</td>
          <td>163</td>
          <td>catalog</td>
          <td>2019-11-03 11:49:17</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>164</th>
          <td>group_alias</td>
          <td>164</td>
          <td>product</td>
          <td>2019-11-03 11:49:28</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>166</th>
          <td>raw</td>
          <td>166</td>
          <td>catalog</td>
          <td>2019-11-03 11:49:30</td>
          <td>456870964</td>
        </tr>
      </tbody>
    </table>
    <br>

Previously, both events were named
``product1`` and ``product2`` and had ``raw`` event types:

.. code-block:: python

    stream.to_dataframe().query('user_id == 456870964')


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>140</td>
          <td>catalog</td>
          <td>2019-11-03 11:46:55</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>141</th>
          <td>raw</td>
          <td>141</td>
          <td>catalog</td>
          <td>2019-11-03 11:47:46</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>142</th>
          <td>raw</td>
          <td>142</td>
          <td>catalog</td>
          <td>2019-11-03 11:47:58</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>143</th>
          <td>raw</td>
          <td>143</td>
          <td>product1</td>
          <td>2019-11-03 11:48:43</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>144</th>
          <td>raw</td>
          <td>144</td>
          <td>cart</td>
          <td>2019-11-03 11:49:17</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>145</th>
          <td>raw</td>
          <td>145</td>
          <td>catalog</td>
          <td>2019-11-03 11:49:17</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>146</th>
          <td>raw</td>
          <td>146</td>
          <td>product2</td>
          <td>2019-11-03 11:49:28</td>
          <td>456870964</td>
        </tr>
        <tr>
          <th>147</th>
          <td>raw</td>
          <td>147</td>
          <td>catalog</td>
          <td>2019-11-03 11:49:30</td>
          <td>456870964</td>
        </tr>
      </tbody>
    </table>
    <br>

You can also notice that the newly created ``product`` events have
``event_id`` that differs from their parents' event_ids.

.. _collapse_loops:

CollapseLoops
^^^^^^^^^^^^^

:py:meth:`CollapseLoops<retentioneering.data_processors_lib.collapse_loops.CollapseLoops>`
replaces all uninterrupted series of repetitive user
events (loops) with one new ``loop`` - like event.
The ``suffix`` parameter defines the name of the new event:

-  given ``suffix=None``, names new event with the old event_name, i.e. passes along
   the name of the repeating event;
-  given ``suffix="loop"``, names new event ``event_name_loop``;
-  given ``suffix="count"``, names new event
   ``event_name_loop_{number of event repetitions}``.

The ``timestamp_aggregation_type`` value determines the new event timestamp:

-  given ``timestamp_aggregation_type="max"`` (the default option), passes the
   timestamp of the last event from the loop;
-  given ``timestamp_aggregation_type="min"``, passes the timestamp of
   the first event from the loop;
-  given ``timestamp_aggregation_type="mean"``, passes the average loop
   timestamp.

.. figure:: /_static/user_guides/data_processor/dp_13_collapse_loops.png


.. code-block:: python

    res = stream.collapse_loops().to_dataframe()

Consider for example user ``2112338``. In the original eventstream she
had three consecutive ``catalog`` events.

.. code-block:: python

    stream.to_dataframe().query('user_id == 2112338')


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
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
          <td>raw</td>
          <td>3550</td>
          <td>main</td>
          <td>2019-12-24 12:58:04</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>3551</th>
          <td>raw</td>
          <td>3551</td>
          <td>catalog</td>
          <td>2019-12-24 12:58:08</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>3552</th>
          <td>raw</td>
          <td>3552</td>
          <td>catalog</td>
          <td>2019-12-24 12:58:16</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>3553</th>
          <td>raw</td>
          <td>3553</td>
          <td>catalog</td>
          <td>2019-12-24 12:58:44</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>3554</th>
          <td>raw</td>
          <td>3554</td>
          <td>main</td>
          <td>2019-12-24 12:58:52</td>
          <td>2112338</td>
        </tr>
      </tbody>
    </table>
    <br>

In the resulting DataFrame, the repeating "catalog" events have been collapsed to a single
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
          <td>raw</td>
          <td>5061</td>
          <td>main</td>
          <td>2019-12-24 12:58:04</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>5066</th>
          <td>group_alias</td>
          <td>5066</td>
          <td>catalog_loop</td>
          <td>2019-12-24 12:58:44</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>5069</th>
          <td>raw</td>
          <td>5069</td>
          <td>main</td>
          <td>2019-12-24 12:58:52</td>
          <td>2112338</td>
        </tr>
      </tbody>
    </table>
    <br>

We can set the suffix to see the length of the loops we removed.
Also, let us see how ``timestamp_aggregation_type`` works if
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
          <td>raw</td>
          <td>5071</td>
          <td>main</td>
          <td>2019-12-24 12:58:04</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>5076</th>
          <td>group_alias</td>
          <td>5076</td>
          <td>catalog_loop_3</td>
          <td>2019-12-24 12:58:23</td>
          <td>2112338</td>
        </tr>
        <tr>
          <th>5079</th>
          <td>raw</td>
          <td>5079</td>
          <td>main</td>
          <td>2019-12-24 12:58:52</td>
          <td>2112338</td>
        </tr>
      </tbody>
    </table>
    <br>

Now, the synthetic ``catalog_loop_3`` event has ``12:58:23`` time -
the average of ``12:58:08``, ``12:58:16`` and ``12:58:44``.

The ``CollapseLoops`` data processor can be useful for compressing the
data:

- by packing loop information into single events,
- removing looping events, in case they are not desirable
  (which can be a common case in clickstream visualization).

.. _synthetic_events_order:

Synthetic events order
----------------------

Let us summarize the information about event type and event order in the eventstream.
As we have already discussed in the eventstream guide: :ref:`event_type column<event_type_explanation>` and
:ref:`reindex method<reindex_explanation>`.

All events came from a sourcing DataFrame are of ``raw`` event type.
When we apply adding or editing data processors new synthetic events are created.
General idea is that each synthetic event has a "parent" or "parents" that
defines its timestamp.

When you apply multiple data processors, timestamp collisions might occur, so it is
unclear how the events should be ordered. For colliding events,
the following sorting order is applied, based on event types (earlier event types
are added earlier), also you can see which data processor
for which event_type is responsible:

.. table:: Mapping of event_types and data processors.
    :widths: 10 40 40
    :class: tight-table

    +-------+-------------------------+-------------------------------------------+
    | Order | event_type              | helper                                    |
    +=======+=========================+===========================================+
    |  1    | profile                 |                                           |
    +-------+-------------------------+-------------------------------------------+
    |  2    | path_start              | :ref:`add_start_end<add_start_end>`       |
    +-------+-------------------------+-------------------------------------------+
    |  3    | new_user                | :ref:`add_new_users<add_new_users>`       |
    +-------+-------------------------+-------------------------------------------+
    |  4    | existing_user           | :ref:`add_new_users<add_new_users>`       |
    +-------+-------------------------+-------------------------------------------+
    |  5    | truncated_left          | :ref:`truncated_events<truncated_events>` |
    +-------+-------------------------+-------------------------------------------+
    |  6    | session_start           | :ref:`split_sessions<split_sessions>`     |
    +-------+-------------------------+-------------------------------------------+
    |  7    | session_start_truncated | :ref:`split_sessions<split_sessions>`     |
    +-------+-------------------------+-------------------------------------------+
    |  8    | group_alias             | :ref:`group<group>`                       |
    +-------+-------------------------+-------------------------------------------+
    |  9    | raw                     |                                           |
    +-------+-------------------------+-------------------------------------------+
    |  10   | raw_sleep               |                                           |
    +-------+-------------------------+-------------------------------------------+
    |  11   | None                    |                                           |
    +-------+-------------------------+-------------------------------------------+
    |  12   | synthetic               |                                           |
    +-------+-------------------------+-------------------------------------------+
    |  13   | synthetic_sleep         |                                           |
    +-------+-------------------------+-------------------------------------------+
    |  14   | positive_target         | :ref:`positive_target<positive_target>`   |
    +-------+-------------------------+-------------------------------------------+
    |  15   | negative_target         | :ref:`negative_target<negative_target>`   |
    +-------+-------------------------+-------------------------------------------+
    |  16   | session_end_truncated   | :ref:`split_sessions<split_sessions>`     |
    +-------+-------------------------+-------------------------------------------+
    |  17   | session_end             | :ref:`split_sessions<split_sessions>`     |
    +-------+-------------------------+-------------------------------------------+
    |  18   | session_sleep           |                                           |
    +-------+-------------------------+-------------------------------------------+
    |  19   | truncated_right         | :ref:`truncated_events<truncated_events>` |
    +-------+-------------------------+-------------------------------------------+
    |  20   | absent_user             | :ref:`lost_users<lost_users>`             |
    +-------+-------------------------+-------------------------------------------+
    |  21   | lost_user               | :ref:`lost_users<lost_users>`             |
    +-------+-------------------------+-------------------------------------------+
    |  22   | path_end                | :ref:`add_start_end<add_start_end>`       |
    +-------+-------------------------+-------------------------------------------+


Custom data processors
----------------------

We have covered all data processors that currently exist in our
library.

You can create a custom data processor to implement the data transformations you
often use. For details, please refer to our custom data processors User Guide.

:red:`TODO: Create UG and add link. dpanina`
