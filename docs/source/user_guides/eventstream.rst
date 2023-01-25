.. raw:: html

    <style>
        .red {color:#24ff83; font-weight:bold;}
    </style>

.. role:: red


Eventstream
===========

The following user guide is also available as `Google Colab notebook <https://colab.research.google.com/drive/1-VuWTmgx57YDmQtdt6CMnV3z2fcjwj32?usp=sharing>`_.

What is eventstream
-------------------

:doc:`Eventstream</getting_started/eventstream_concept>` is a core class in the retentioneering library. From a user point of view this class is used for three reasons:

- Data container. The initial clickstream is stored in an ``Eventstream`` object.

- Preprocessing. Eventstream allows you to efficiently work with clickstream data preparation process. See a :doc:`user guide on preprocessing</user_guides/dataprocessors>`.

- Applying analytical tools. Eventstream provides simple interfaces to retentioneering tools, so you can seamlessly apply them. See a :doc:`user guide on retentioneering tooling methods</user_guide>`.

The structure of an eventstream is designed as follows. Let :math:`U` be a set of unique users, :math:`E` be a set of unique events. Eventstream is a set of sequential events :math:`\{(u_i, e_j, t_k)\}` which means that user :math:`u_i` experienced event :math:`e_j` at time :math:`t_k`, where :math:`i = 1, 2, \ldots |U|`, :math:`j = 1, 2, \ldots, |E|`, :math:`k = 1, 2, \ldots`.


Eventstream creation
--------------------

In some sense, ``Eventstream`` is a container for a clickstream represented by a ``pandas.DataFrame``, so an Eventstream instance is created by passing a dataframe to Eventstream constructor. The constructor expects the dataframe to have at least 3 columns: ``user_id``, ``event``, ``timestamps``. Let's create a dummy dataframe for that:

.. code-block:: python

    import pandas as pd

    df1 = pd.DataFrame([
        ['user_1', 'A', '2023-01-01 00:00:00'],
        ['user_1', 'B', '2023-01-01 00:00:01'],
        ['user_2', 'B', '2023-01-01 00:00:02'],
        ['user_2', 'A', '2023-01-01 00:00:03'],
        ['user_2', 'A', '2023-01-01 00:00:04'],
    ], columns=['user_id', 'event', 'timestamp'])

Having such a dataframe, you can create an eventstream simply as follows:

.. code-block:: python

    from retentioneering.eventstream import Eventstream
    stream1 = Eventstream(df1)

Before we go further we need to introduce you a displaying method :py:meth:`to_dataframe()<retentioneering.eventstream.eventstream.Eventstream.to_dataframe>` which shows the data underlying an eventstream.  converts an eventstream to the corresponding data frame.

.. code-block:: python

    stream1.to_dataframe()

.. raw:: html

    <table class="dataframe">
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
          <td>14a6f776-ff43-43aa-859e-db67402f7c93</td>
          <td>raw</td>
          <td>0</td>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
        </tr>
        <tr>
          <th>1</th>
          <td>c0ba82a9-b7fd-4096-b89d-209c04fc9688</td>
          <td>raw</td>
          <td>1</td>
          <td>B</td>
          <td>2023-01-01 00:00:01</td>
          <td>user_1</td>
        </tr>
        <tr>
          <th>2</th>
          <td>72ead540-e997-4168-8ce5-c4cc181a72cb</td>
          <td>raw</td>
          <td>2</td>
          <td>B</td>
          <td>2023-01-01 00:00:02</td>
          <td>user_2</td>
        </tr>
        <tr>
          <th>3</th>
          <td>e7ddad2b-04c1-4360-ac23-f51494bfa3f0</td>
          <td>raw</td>
          <td>3</td>
          <td>A</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
        </tr>
        <tr>
          <th>4</th>
          <td>5ac8b0dc-ac94-4c68-b0b3-73933a86b65f</td>
          <td>raw</td>
          <td>4</td>
          <td>A</td>
          <td>2023-01-01 00:00:04</td>
          <td>user_2</td>
        </tr>
      </tbody>
    </table>
    <br>

We'll discuss the columns of the resulting dataframe later in `Displaying eventstream`_ section.

Coming back to eventstream creation; in case a parent dataframe has different names, you can either rename them in the dataframe or set a mapping rule which would tell the Eventstream constructor
where events, user_ids and timestamps are located. This can be done with :py:meth:`RawDataSchema<retentioneering.eventstream.schema.RawDataSchema>` class. Here's how it works. Let's create a dataframe containing the same data but with different column names:

.. code-block:: python

    from retentioneering.eventstream import RawDataSchema

    df2 = pd.DataFrame([
        ['user_1', 'A', '2023-01-01 00:00:00'],
        ['user_1', 'B', '2023-01-01 00:00:01'],
        ['user_2', 'B', '2023-01-01 00:00:02'],
        ['user_2', 'A', '2023-01-01 00:00:03'],
        ['user_2', 'A', '2023-01-01 00:00:04'],
    ], columns=['client_id', 'action', 'datetime'])

    raw_data_schema_df2 = RawDataSchema(
        user_id='client_id',
        event_name='action',
        event_timestamp='datetime'
    )

    stream2 = Eventstream(df2, raw_data_schema=raw_data_schema_df2)

As you see, ``RawDataSchema`` constructor maps fields ``user_id``, ``event_name``, and ``event_timestamp`` with the corresponding field names from your sourcing dataframe.

Another common case is when your dataframe has some important columns which you want to be included in the eventstream. ``RawDataSchema`` supports this scenario too with a help of ``custom_cols`` argument. This argument accepts a list of dictionaries, one dict per one custom field. A single dict must contain two fields: ``raw_data_col`` and ``custom_col``. The former stands for a field name from the sourcing dataframe, the latter stands for the corresponding field name to be set at the resulting eventstream.

Suppose we use a dataframe ``df3`` similar to the previous ``df2`` but extended with ``session`` column which we want to be used in the eventstream as ``session_id`` column. Here's an example how we can do this.

.. code-block:: python

    from retentioneering.eventstream import RawDataSchema

    df3 = pd.DataFrame([
        ['user_1', 'A', '2023-01-01 00:00:00', 'session_1'],
        ['user_1', 'B', '2023-01-01 00:00:01', 'session_1'],
        ['user_2', 'B', '2023-01-01 00:00:02', 'session_2'],
        ['user_2', 'A', '2023-01-01 00:00:03', 'session_3'],
        ['user_2', 'A', '2023-01-01 00:00:04', 'session_3'],
    ], columns=['client_id', 'action', 'datetime', 'session'])

    raw_data_schema_df3 = RawDataSchema(
        user_id='client_id',
        event_name='action',
        event_timestamp='datetime',
        custom_cols=[{'raw_data_col': 'session', 'custom_col': 'session_id'}]
    )

    stream3 = Eventstream(df3, raw_data_schema=raw_data_schema_df3)

If the core triple columns of ``df3`` dataframe were titled with the default names ``user_id``, ``event``, ``timestamp`` (instead of ``client_id``, ``action``, ``datetime``) then you could just ignore their mapping in setting ``RawDataSchema`` and pass ``custom_cols`` argument only.

:red:`TODO: mention EventstreamSchema`

:red:`TODO: provide an example when raw_data_schema accepts a dict instead of RawDataSchema`

:red:`TODO: mention user sampling`

Displaying eventstream
----------------------

Now let's look closely which columns are represented in an eventstream and discuss the work of :py:meth:`to_dataframe()<retentioneering.eventstream.eventstream.Eventstream.to_dataframe>` method using the example of ``stream3`` eventstream.

.. code-block:: python

    stream3.to_dataframe()

.. raw:: html

    <table class="dataframe">
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
          <td>af1efd95-e280-4988-bbb1-30569be06665</td>
          <td>raw</td>
          <td>0</td>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>session_1</td>
        </tr>
        <tr>
          <th>1</th>
          <td>06662e65-7bb4-407d-88f0-93a0d7b6dcd2</td>
          <td>raw</td>
          <td>1</td>
          <td>B</td>
          <td>2023-01-01 00:00:01</td>
          <td>user_1</td>
          <td>session_1</td>
        </tr>
        <tr>
          <th>2</th>
          <td>131b0799-46e8-4370-ac51-e1a9113ebaaa</td>
          <td>raw</td>
          <td>2</td>
          <td>B</td>
          <td>2023-01-01 00:00:02</td>
          <td>user_2</td>
          <td>session_2</td>
        </tr>
        <tr>
          <th>3</th>
          <td>a85fc194-757d-4573-be53-e7fc53553fcf</td>
          <td>raw</td>
          <td>3</td>
          <td>A</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>session_3</td>
        </tr>
        <tr>
          <th>4</th>
          <td>01d1a919-a5e5-4359-99f7-cbd29d421394</td>
          <td>raw</td>
          <td>4</td>
          <td>A</td>
          <td>2023-01-01 00:00:04</td>
          <td>user_2</td>
          <td>session_3</td>
        </tr>
      </tbody>
    </table>
    <br>

Among the standard triple ``user_id``, ``event``, ``timestamp`` and custom column ``session_id`` we see the columns ``event_id``, ``event_type``, ``event_index``. They a sort of technical but sometimes they might be useful in preprocessing so here's their description.

- ``event_id``. A string identifier of an evenstream row.

- ``event_type``. All the events came from a sourcing dataframe are of ``raw`` event type. "Raw" means that these event are used as a source for an eventstream, like raw data. However, preprocessing methods can add some so called synthetic events which have different event types. See the details in :doc:`Preprocessing user guide</user_guides/dataprocessors>`.

- ``event_index``. An integer which is associated with the event order. By default, an eventstream is sorted by timestamp. As for the synthetic events which are often placed at the beginning or in the end of a user's path, special sorting is applied. See :doc:`Preprocessing user guide</user_guides/dataprocessors>` for the details. :red:`TODO: set a precise link to synthetic events sorting subsection`. Please note that the event index might contain gaps. It's ok due to its design see :doc:`Eventstream concept</getting_started/eventstream_concept>` for the details. :red:`TODO: set a precise link to a subsection`.

There are some additional options which one might find useful.

-  ``show_deleted``. Since all the events once uploaded to an eventstream are immutable (:red:`Set an appropriate link to eventstream concept section`). By default, ``show_deleted`` flag is ``False``, so the events which are considered as deleted due to preprocessing steps are not showed in the resulting dataframe. If ``show_deleted=True``, all the events from the original state of the eventstream and all the in-between preprocessing states are appeared.
-  ``copy``. When this flag is ``True`` (by default it's ``False``) then an explicit copy of the dataframe is created.

Descriptive methods
-------------------

As soon as we've created an eventstream we usually want to explore it. ``Eventstream`` provides a set of methods for such a first touch exploration. To illustrate the work of these methods we need a larger dataset, so we'll use our standard demonstration :py:meth:`simple_shop<retentioneering.datasets.load.load_simple_shop>` dataset. For demonstration purposes we add `session_id` column by applying :py:meth:`SplitSessions<retentioneering.data_processors_lib.split_sessions.SplitSessions>` data processor.

:red:`TODO: fix the link to simple_shop`

.. code-block:: python

    from retentioneering import datasets

    stream = datasets\
        .load_simple_shop()\
        .split_sessions(session_cutoff=(30, 'm'))
    stream.head()

.. raw:: html

    <div style="overflow:auto;">
    <table class="dataframe">
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
          <td>8c48e664-9d1e-4e90-9d3b-7a2807620862</td>
          <td>session_start</td>
          <td>0</td>
          <td>session_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>1</th>
          <td>0fdffb6e-16ed-4dff-bb7f-ef57a6b5db61</td>
          <td>raw</td>
          <td>1</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>3</th>
          <td>24bdad84-de40-41d6-8786-90468ecd7b98</td>
          <td>raw</td>
          <td>3</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>5</th>
          <td>696c942d-24da-4fe8-9840-5a69e8744f6e</td>
          <td>raw</td>
          <td>5</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
        <tr>
          <th>7</th>
          <td>974dbef0-f7f4-4ea5-8f3f-b70a90f0bfc3</td>
          <td>raw</td>
          <td>7</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
          <td>219483890_1</td>
        </tr>
      </tbody>
    </table>
    </div>

General statistics
~~~~~~~~~~~~~~~~~~

Similarly to pandas, we use :py:meth:`describe()<retentioneering.eventstream.eventstream.Eventstream.describe>` for a general description of an eventstream.

.. code-block:: python

    stream.describe()

:red:`TODO: align with adjusted behaviour` `PLAT-542 <https://retentioneering.atlassian.net/browse/PLAT-542>`_

The output consists of three data blocks: basic statistics, time distribution and events distribution. ``session_col`` argument points to an eventstream column which contains session ids (``session_id`` is the default value). If defined, session statistics is also included. Otherwise all the values related to sessions are not displayed.

By `eventstream start` and `eventstream end` in the "Basic statistics" block we mean timestamps of the first event and the last events in the eventstream correspondingly. `eventstream length` is a time distance between event stream start and end. "User path/session time length" shows some time-based statistics over user paths and sessions. Blocks "User path/session time length" and "Number of events per user path/session" provides similar information on the length of users paths and sessions (correspondingly), but the former is calculated in days and the latter in the number of events. Often, such time-related information requires deeper analysis, so simple statistics are not enough, and we want to see the entire distribution. For these purposes the following group of methods has been designed.

Time-based histograms
~~~~~~~~~~~~~~~~~~~~~

User lifetime
^^^^^^^^^^^^^

Proceeding the previous point, one of the most important time-related values is the user lifetime. Since an eventstream has its natural time borders, by lifetime we mean the length of the observed user path as the time distance between the first and the last event represented in the trajectory. The histogram for this value is plotted by :py:meth:`user_lifetime_hist()<retentioneering.eventstream.eventstream.Eventstream.user_lifetime_hist>` method.

.. code-block:: python

    stream.user_lifetime_hist()

.. figure:: /_static/user_guides/eventstream/01_user_lifetime_hist_simple.png
    :width: 400

The method has multiple parameters. Let's start with those which are responsible for data formatting.

- ``bins`` is a common for setting the number of the histogram bins;

- ``timedelta_unit`` defines a `datetime unit <https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-units>`_ which is used for the lifetime measuring;

- ``log_scale`` sets logarithmic scale for the bins;

- ``lower_cutoff_quantile``, ``upper_cutoff_quantile`` indicates the lower and apper quantiles (as floats between 0 and 1), the values between the quantiles only are considered for the histogram.

:red:`Demonstrate the work of the other parameters`

.. note::

    The method is especially useful for working together with :py:meth:`DeleteUsersByPathLength<retentioneering.data_processors_lib.delete_users_by_path_length.DeleteUsersByPathLength>` and :py:meth:`TruncatedEvents<retentioneering.data_processors_lib.truncated_events.TruncatedEvents>`. See :doc:`the user guide on preprocessing</user_guides/dataprocessors>` for the details.

Timedelta between two events
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. container:: toggle

    .. container:: header

        :red:`Method timedelta_hist() has a serious bug. Please don't use it and ignore the hidden documentation below`

    So we've defined user lifetime as the timedelta between the beginning and the end of a user's path. This can be generalized. :py:meth:`timedelta_hist()<retentioneering.eventstream.eventstream.Eventstream.timedelta_hist>` shows a histogram for the distribution of the timedeltas between a couple of specified events.

    The method supports the same formatting arguments (``bins``, ``timedelta_unit``, ``log_scale``, ``lower_cutoff_quantile``, ``upper_cutoff_quantile``) as :py:meth:`user_lifetime_hist()<retentioneering.eventstream.eventstream.Eventstream.user_lifetime_hist>`.

    If no arguments passed (except formatting arguments), timedeltas between all adjacent events are calculated within each user path. For example, this tiny evenstream

    .. figure:: /_static/user_guides/eventstream/02_timedelta_trivial_example.png
        :width: 400

    generates 4 timedeltas :math:`\Delta_1, \Delta_2, \Delta_3, \Delta_4` as shown in the diagram. The timedeltas between events B and D, D and C, C and E are not taken into account because two events from each pair are from different users.

    .. code-block:: python

        stream.timedelta_hist(log_scale=True, timedelta_unit='m')

    .. figure:: /_static/user_guides/eventstream/03_timedelta_log_scale.png
        :width: 400

    This distribution of the adjacent events is sort of common. It looks like a bi-modal (which is not true: remember, we use log-scale here), but these two bells help us to estimate estimate a timeout for splitting sessions. From this charts we can see that it is reasonable to set it to somewhat between 10 and 100 minutes.

    Another use case for :py:meth:`timedelta_hist()<retentioneering.eventstream.eventstream.Eventstream.timedelta_hist>` is visualizing the distribution of the timedeltas between two specific events. Assume we want to know how much time it takes for a user to go from product1 to cart. Then we set `event_pair=('product1', 'cart')` and pass it to ``timdelta_hist``:

    .. code-block:: python

        stream.timedelta_hist(event_pair=('product1', 'cart'), timedelta_unit='m')

    .. figure:: /_static/user_guides/eventstream/04_timedelta_pair_of_events.png
        :width: 400

    We see that such occurrences are not very numerous. This is because the method still considers only adjacent pairs of events (in this case ``product1`` and ``cart`` are assumed to go one right after another in a user's path). That's why the histogram is heavily skewed to 0. ``only_adjacent_event_pairs`` parameter allows to consider any cases when a user goes from ``product1`` to ``cart`` non-directly but passing through some other events:

    .. code-block:: python

        stream.timedelta_hist(event_pair=('product1', 'cart'), timedelta_unit='m')

    .. figure:: /_static/user_guides/eventstream/05_timedelta_only_adjacent_event_pairs.png
        :width: 400

    Now, the histogram is still skewed to 0, but this time not so heavily.

    As you may notice from the previous chart, quite many timedeltas have relatively high values. Yes, we can interpret this in a way like the users are picky, so it takes them long to go from ``product1`` to ``cart`` or probably ``product1`` seems not so popular so the users don't want to purchase it. Anyway, sometimes we are interested to look only at those events which appeared within a user session. So if we've already split the paths into sessions we can use ``weight_col='session_id'``:

    .. code-block:: python

        stream\
            .timedelta_hist(
                event_pair=('product1', 'cart'),
                timedelta_unit='m',
                only_adjacent_event_pairs=False,
                weight_col='session_id'
            )

    .. figure:: /_static/user_guides/eventstream/06_timedelta_sessions.png
        :width: 400

    It's clear now that within a session the users walk from ``product1`` to ``cart`` event in less than 3 minutes.

    For frequently occurring events we might be interested in aggregation some values over sessions or users. For example, transition ``main -> catalog`` is quite frequent. Some users do these transitions quickly, some of them not. It might be reasonable to aggregate the timedeltas over each user path firstly (therefore, we get one value per one user at this step), and then visualize the distribution of these aggregated values. This can be done by passing an additional argument ``aggregation='mean'`` or ``aggregation='median'``.

    .. code-block:: python

        stream\
            .timedelta_hist(
                event_pair=('product1', 'cart'),
                timedelta_unit='m',
                only_adjacent_event_pairs=False,
                weight_col='user_id',
                aggregation='mean'
            )

    :red:`insert an image with the output histogram`


Events intensity
^^^^^^^^^^^^^^^^

Another nice way to review an eventstream from time point of view is to look how evenly the events are distributed over time. :py:meth:`event_timestamp_hist()<retentioneering.eventstream.eventstream.Eventstream.event_timestamp_hist>`.

.. code-block:: python

    stream.event_timestamp_hist()

.. figure:: /_static/user_guides/eventstream/08_event_timestamp_hist.png
    :width: 400

We can notice the heavy skew in the data towards the period between April and May of 2020. Let us check whether it is specific to the ``cart``, ``product1``, and ``product2`` events. There's an argument ``event_list`` for this.

.. code-block:: python

    stream.event_timestamp_hist(event_list=['cart', 'product1', 'product2'])

.. figure:: /_static/user_guides/eventstream/09_event_timestamp_hist_event_list.png
    :width: 400

Nothing changed. The skew is probably related to user path sampling or the general popularity of the simple shop over time.

We could also get rid of the period between April and May, if we think it is too different from the general time frame:

.. code-block:: python

    stream.event_timestamp_hist(upper_cutoff_quantile=0.43)

.. figure:: /_static/user_guides/eventstream/10_event_timestamp_hist_quantile.png
    :width: 400
