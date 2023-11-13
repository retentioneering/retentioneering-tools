Eventstream
===========

|colab| |jupyter|


.. |jupyter| raw:: html

    <a href="../_static/user_guides_notebooks/eventstream.ipynb">
    <img src="https://img.shields.io/static/v1?label=Download&message=Jupyter+Notebook&color=%23F37626&logo=jupyter&logoColor=%23F37626"
        alt="Download - Jupyter Notebook">
    </a>

.. |colab| raw:: html

    <a href="https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/docs/source/_static/user_guides_notebooks/eventstream.ipynb" target="_blank">
      <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Google Colab"/>
    </a>

What is Eventstream?
--------------------

Eventstream is the core class in the retentioneering library. This data structure is designed
around three following purposes:

.. todo::

    Set a link to eventstream concept as soon as it is ready. Vladimir Kukushkin

- **Data container**. Eventstream class implements a convenient approach to storing clickstream data.

- **Preprocessing**. Eventstream allows to efficiently implement a data
  preparation process.
  See :doc:`Preprocessing user guide <../user_guides/preprocessing>` for more details.

- **Applying analytical tools**. Eventstream integrates with retentioneering tools and
  allows you to seamlessly apply them. See :ref:`user guides on the path analysis tools<UG_path_analysis_tools>`.


.. _eventstream_creation:

Eventstream creation
--------------------

Default field names
~~~~~~~~~~~~~~~~~~~

An ``Eventstream`` is a container for clickstream data, that is initialized from a ``pandas.DataFrame``.
The class constructor expects the DataFrame to have at least 3 columns:
``user_id``, ``event``, ``timestamp``.

Let us create a dummy DataFrame to illustrate Eventstream init process:

.. code-block:: python

    import pandas as pd

    df1 = pd.DataFrame(
        [
            ['user_1', 'A', '2023-01-01 00:00:00'],
            ['user_1', 'B', '2023-01-01 00:00:01'],
            ['user_2', 'B', '2023-01-01 00:00:02'],
            ['user_2', 'A', '2023-01-01 00:00:03'],
            ['user_2', 'A', '2023-01-01 00:00:04'],
        ],
        columns=['user_id', 'event', 'timestamp']
    )

Having such a dataframe, you can create an eventstream simply as follows:

.. code-block:: python

    from retentioneering.eventstream import Eventstream
    stream1 = Eventstream(df1)

To do the inverse transformation (i.e. obtain a DataFrame from an eventstream object),
:py:meth:`to_dataframe()<retentioneering.eventstream.eventstream.Eventstream.to_dataframe>` method can be used.
However, the method is not just a converter. Using it, we can display the internal ``Eventstream`` structure:

.. _eventstream_stream1:

.. code-block:: python

    stream1.to_dataframe()

.. raw:: html

    <div>
    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>0</td>
          <td>52c63aed-b1ff-4fea-806d-8484a8978443</td>
        </tr>
        <tr>
          <th>1</th>
          <td>B</td>
          <td>2023-01-01 00:00:01</td>
          <td>user_1</td>
          <td>raw</td>
          <td>1</td>
          <td>e9537f88-f776-4047-ae16-66f8b64c4076</td>
        </tr>
        <tr>
          <th>2</th>
          <td>B</td>
          <td>2023-01-01 00:00:02</td>
          <td>user_2</td>
          <td>raw</td>
          <td>2</td>
          <td>bb6f1cd3-a630-4d48-94c2-8a3cf5a3d22f</td>
        </tr>
        <tr>
          <th>3</th>
          <td>A</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>raw</td>
          <td>3</td>
          <td>3680e70b-166d-475d-8ac1-166ea213f5f4</td>
        </tr>
        <tr>
          <th>4</th>
          <td>A</td>
          <td>2023-01-01 00:00:04</td>
          <td>user_2</td>
          <td>raw</td>
          <td>4</td>
          <td>c22d97a7-63a1-4d57-b738-16863107dfb7</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br>

We will describe the columns of the resulting DataFrame later in `Displaying eventstream`_ section.

.. _eventstream_raw_data_schema:

Changing default field names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For custom DataFrame column names you can either rename them
using pandas, or set a mapping rule that would tell the Eventstream constructor
the mapping to the correct column names.
This can be done with Eventstream attribute ``raw_data_schema`` with uses
:py:meth:`RawDataSchema<retentioneering.eventstream.schema.RawDataSchema>`
class under the hood.

Let us illustrate its usage with the following example with the same dataframe
containing the same data but with different column names
(``client_id``, ``action`` and ``datetime``):

.. code-block:: python

    df2 = pd.DataFrame(
        [
            ['user_1', 'A', '2023-01-01 00:00:00'],
            ['user_1', 'B', '2023-01-01 00:00:01'],
            ['user_2', 'B', '2023-01-01 00:00:02'],
            ['user_2', 'A', '2023-01-01 00:00:03'],
            ['user_2', 'A', '2023-01-01 00:00:04']
        ],
         columns=['client_id', 'action', 'datetime']
    )

    raw_data_schema = {
        'user_id': 'client_id',
        'event_name': 'action',
        'event_timestamp': 'datetime'
    }

    stream2 = Eventstream(df2, raw_data_schema=raw_data_schema)
    stream2.to_dataframe().head(3)

.. raw:: html

    <div>
    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>0</td>
          <td>0fd741dd-8140-4182-8046-cf23906208c6</td>
        </tr>
        <tr>
          <th>1</th>
          <td>B</td>
          <td>2023-01-01 00:00:01</td>
          <td>user_1</td>
          <td>raw</td>
          <td>1</td>
          <td>3a4db2d7-a4b8-4845-ab21-0950f6a2bfc0</td>
        </tr>
        <tr>
          <th>2</th>
          <td>B</td>
          <td>2023-01-01 00:00:02</td>
          <td>user_2</td>
          <td>raw</td>
          <td>2</td>
          <td>fd4552d9-db28-47cc-b7a1-4408a895cff9</td>
        </tr>
      </tbody>
    </table>
    <br>

As we see, ``raw_data_schema`` argument maps fields ``client_id``, ``action``,
and ``datetime`` so that they are imported to the eventstream correctly.

.. _eventstream_custom_cols:

Custom columns
~~~~~~~~~~~~~~

Another common case is when your DataFrame has some additional columns
that you want to add to the eventstream. By default, these columns are included automatically. In case you want to explicitly control what columns should be included, you can add them in the ``custom_cols`` argument.

Suppose the initial DataFrame now also contains two columns: ``session`` and ``device``, and you want to leave the former column only. Then you can use ``custom_cols`` parameter that works as a whitelist.

.. code-block:: python

    df3 = pd.DataFrame(
        [
            ['user_1', 'A', '2023-01-01 00:00:00', 'session_1', 'mobile'],
            ['user_1', 'B', '2023-01-01 00:00:01', 'session_1', 'mobile'],
            ['user_2', 'B', '2023-01-01 00:00:02', 'session_2', 'desktop'],
            ['user_2', 'A', '2023-01-01 00:00:03', 'session_3', 'desktop'],
            ['user_2', 'A', '2023-01-01 00:00:04', 'session_3', 'desktop']
        ],
        columns=['client_id', 'action', 'datetime', 'session', 'device']
    )

    raw_data_schema = {
        'user_id': 'client_id',
        'event_name': 'action',
        'event_timestamp': 'datetime',
    }

    stream3 = Eventstream(df3, raw_data_schema=raw_data_schema, custom_cols=['session'])
    stream3.to_dataframe().head(3)

.. raw:: html

    <div>
    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>event_type</th>
          <th>session</th>
          <th>event_index</th>
          <th>event_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>session_1</td>
          <td>0</td>
          <td>c15ff01a-6822-464f-a4cc-dc6118e44e6d</td>
        </tr>
        <tr>
          <th>1</th>
          <td>B</td>
          <td>2023-01-01 00:00:01</td>
          <td>user_1</td>
          <td>raw</td>
          <td>session_1</td>
          <td>1</td>
          <td>359c5e0e-d533-4101-8f9c-86247cb78590</td>
        </tr>
        <tr>
          <th>2</th>
          <td>B</td>
          <td>2023-01-01 00:00:02</td>
          <td>user_2</td>
          <td>raw</td>
          <td>session_2</td>
          <td>2</td>
          <td>ce03482c-0b47-42eb-9947-8ee3cc39ecd9</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br>

As we see from above, the ``session`` column was included in the eventstream while ``device`` was not.

The same results could be achieved if you pass ``custom_cols`` as a key in the ``RawDataSchema``. In this case, the value must be a list of dictionaries, one dict per one custom field. A single dict must contain two fields: ``raw_data_col`` and ``custom_col``. The former stands for a field name from the sourcing dataframe, the latter stands for the corresponding field name to be set in the resulting eventstream.

.. code-block:: python

    raw_data_schema = {
        'user_id': 'client_id',
        'event_name': 'action',
        'event_timestamp': 'datetime',
        'custom_cols': [
            {
                'raw_data_col': 'session',
                'custom_col': 'session_id'
            }
        ]
    }

    stream3 = Eventstream(df3, raw_data_schema=raw_data_schema)
    stream3.to_dataframe().head(3)

Here we see that the original ``session`` column is stored in ``session_id`` column,
according to the defined ``raw_data_schema``

If the core triple columns of the DataFrame are titled with the default names
``user_id``, ``event``, ``timestamp`` (instead of ``client_id``, ``action``, ``datetime``)
then you can ignore their mapping in the ``raw_data_schema`` and pass ``custom_cols`` argument only.

.. _eventstream_field_names:

Eventstream field names
~~~~~~~~~~~~~~~~~~~~~~~

You can also set the names of the eventstream columns (the default names are ``user_id``, ``event``, ``timestamp``) by defining ``schema`` argument. It is a dictionary with :py:meth:`the following possible keys<retentioneering.eventstream.schema.EventstreamSchema>`, the values are the desired column names. In the example below we set a schema for the same :ref:`stream1<eventstream_stream1>` and define the names of the basic columns as ``client_id``, ``action``, and ``datetime``:

.. code-block:: python

    new_eventstream_schema = {
        'user_id': 'client_id',
        'event_name': 'action',
        'event_timestamp': 'datetime'
    }

    stream1_new_schema = Eventstream(df1, schema=new_eventstream_schema)
    stream1_new_schema.to_dataframe().head(3)


.. raw:: html

    <div>
    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>action</th>
          <th>datetime</th>
          <th>client_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>0</td>
          <td>884a38f1-dc62-4567-a10b-5c20a690a173</td>
        </tr>
        <tr>
          <th>1</th>
          <td>B</td>
          <td>2023-01-01 00:00:01</td>
          <td>user_1</td>
          <td>raw</td>
          <td>1</td>
          <td>8ed1d3fb-8026-413a-a426-9f8858cd9d73</td>
        </tr>
        <tr>
          <th>2</th>
          <td>B</td>
          <td>2023-01-01 00:00:02</td>
          <td>user_2</td>
          <td>raw</td>
          <td>2</td>
          <td>965de90a-6a68-42a5-8d8a-8e23534bfd72</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br>

The full list of an eventstream fields and the corresponding values is available in :py:meth:`Eventstream.schema<retentioneering.eventstream.schema.EventstreamSchema>` attribute:

.. code-block:: python

    stream1_new_schema.schema

.. parsed-literal::

    EventstreamSchema(
        event_id='event_id',
        event_type='event_type',
        event_index='event_index',
        event_name='action',
        event_timestamp='datetime',
        user_id='client_id',
        custom_cols=[]
    )

Path start and end
~~~~~~~~~~~~~~~~~~

For many practical reasons it is useful to keep synthetic events indicating path start and path end. Eventstream constructor adds ``path_start`` and ``path_end`` events explicitly. See also :ref:`add_start_end_events<add_start_end_events>` data processor.

User sampling
~~~~~~~~~~~~~

Contemporary data analysis usually involve working with large datasets.
Using retentioneering to work with such datasets might cause the following
undesirable effects:

- High computational costs.

- The messy big picture (especially in case of applying such tools as
  :doc:`Transition Graph</user_guides/transition_graph>`, :doc:`StepMatrix</user_guides/step_matrix>`
  and :doc:`StepSankey</user_guides/step_sankey>`). Insufficient user paths or large number of almost
  identical paths (especially short paths) often add no value to the analysis.
  It might be reasonable to get rid of them.

- Due to Eventstream design, all the data uploaded to an Eventstream instance is kept immutable.
  Even if you remove some eventstream rows while preprocessing, the data stays untouched:
  it just becomes hidden and is marked as removed.
  Thus, the only chance to tailor the dataset to a reasonable size is to sample the user
  paths at entry point - while applying Eventstream constructor.

.. todo::

    Set a link to eventstream concept as soon as it is ready. Vladimir Kukushkin

The size of the original dataset can be reduced by path sampling. In theory, this procedure could affect
the eventstream analysis, especially in case you have rare but important events and behavioral patterns.
Nevertheless, the sampling is less likely to distort the big picture, so we recommend to use it
when it is needed.

We also highlight that user path sampling means that we remove some random paths entirely. We guarantee
that the sampled paths contain all the events from the original dataset, and they are not truncated.

There are a couple sampling parameters in the Eventstream constructor: ``user_sample_size``
and ``user_sample_seed``. There are two ways of setting the sample size:

- A float number. For example, ``user_sample_size=0.1`` means that we want to leave 10%
  ot the paths and remove 90% of them.
- An integer sample size is also possible. In this case a specified number of events will be left.

``user_sample_seed`` is a standard way to make random sampling reproducible
(see `this Stack Overflow explanation <https://stackoverflow.com/questions/21494489/what-does-numpy-random-seed0-do>`_).
You can set it to any integer number.

Below is a sampling example for :doc:`simple_shop </datasets/simple_shop>` dataset.

.. code-block:: python

    from retentioneering import datasets

    simple_shop_df = datasets.load_simple_shop(as_dataframe=True)
    sampled_stream = Eventstream(
        simple_shop_df,
        user_sample_size=0.1,
        user_sample_seed=42
    )

    print('Original number of the events:', len(simple_shop_df))
    print('Sampled number of the events:', len(sampled_stream.to_dataframe()))

    unique_users_original = simple_shop_df['user_id'].nunique()
    unique_users_sampled = sampled_stream.to_dataframe()['user_id'].nunique()

    print('Original unique users number: ', unique_users_original)
    print('Sampled unique users number: ', unique_users_sampled)


.. parsed-literal::
    Original number of the events: 32283
    Sampled number of the events: 3298
    Original unique users number:  3751
    Sampled unique users number:  375

We see that the number of the users has been reduced from 3751 to 375 (10% exactly). The number
of the events has been reduced from 32283 to 3298 (10.2%), but we didn't expect to see exact 10% here.

.. _to_dataframe explanation:

Displaying eventstream
----------------------

Now let us look at columns represented in an eventstream and discuss
:py:meth:`to_dataframe()<retentioneering.eventstream.eventstream.Eventstream.to_dataframe>`
method using the example of ``stream3`` eventstream.

.. code-block:: python

    stream3.to_dataframe()

.. raw:: html

    <div style="overflow:auto;">
    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>session_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>session_1</td>
          <td>raw</td>
          <td>0</td>
          <td>7427d9f5-8666-4821-b0a9-f74a962f6d72</td>
        </tr>
        <tr>
          <th>1</th>
          <td>B</td>
          <td>2023-01-01 00:00:01</td>
          <td>user_1</td>
          <td>session_1</td>
          <td>raw</td>
          <td>1</td>
          <td>6c9fef69-a176-45d1-bb13-628796e68602</td>
        </tr>
        <tr>
          <th>2</th>
          <td>B</td>
          <td>2023-01-01 00:00:02</td>
          <td>user_2</td>
          <td>session_2</td>
          <td>raw</td>
          <td>2</td>
          <td>7aee8104-b1cc-4df4-8a8d-f569395ffad9</td>
        </tr>
        <tr>
          <th>3</th>
          <td>A</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>session_3</td>
          <td>raw</td>
          <td>3</td>
          <td>3b3610b2-8016-4259-bf68-6daf34518e34</td>
        </tr>
        <tr>
          <th>4</th>
          <td>A</td>
          <td>2023-01-01 00:00:04</td>
          <td>user_2</td>
          <td>session_3</td>
          <td>raw</td>
          <td>4</td>
          <td>945e6514-2f41-457c-ba70-2ac35150b41e</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br>

Besides the standard triple ``user_id``, ``event``, ``timestamp`` and custom column ``session_id``
we see the columns ``event_id``, ``event_type``, ``event_index``.
These are some technical columns, containing the following:

.. _event_type_explanation:

- ``event_type`` - all the events that come from the sourcing DataFrame are of ``raw`` event type.
  However, preprocessing methods can add some synthetic events that have various event types.
  See the details in :ref:`data processors user guide<dataprocessors_adding_processors>`.

- ``event_index`` - an integer which is associated with the event order. By default, an eventstream
  is sorted by timestamp, and optionally by ``event`` column. As for the synthetic events which are often placed at the beginning or in the
  end of a user's path, special sorting is applied. See explanation of :ref:`index and reindex logic<index_explanation>`
  for the details and also :ref:`data processors user guide<synthetic_events_order>`.
  Please note that the event index might has duplicated values. It is ok due to its design.

- ``event_id`` - a string identifier of an eventstream row.

.. todo::

    Set a link to eventstream concept as soon as it is ready. Vladimir Kukushkin
    see :ref:`Eventstream concept<join algorithm>` for the details.

There are additional arguments that may be useful.

-  ``show_deleted``. Eventstream is immutable data container. It means that all the events
   once uploaded to an eventstream are kept. Even if we remove some events, they are just
   marked as removed. By default, ``show_deleted=False`` so these events are hidden in the
   output DataFrame. If ``show_deleted=True``, all the events from the original state
   of the eventstream and all the in-between preprocessing states are displayed.

.. todo::

    Set a link to eventstream concept as soon as it is ready. Vladimir Kukushkin
    see :ref:`Eventstream concept<join algorithm>` for the details.

-  ``copy`` - when this flag is ``True`` (by default it is ``False``) then an explicit copy
   of the DataFrame is created. See details in
   `pandas documentation <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html#:~:text=If%20None%2C%20infer.-,copybool,-or%20None%2C%20default>`_.

.. _index_explanation:

Eventstream index and reindex
-----------------------------

In the previous section, we have already mentioned the sorting algorithm when we described special
``event_type`` and ``event_index`` eventstream columns.
Now, let us take a closer look at the sorting logic and illustrate it with several examples.

By default, raw events are sorted by timestamp column. So the events with the same timestamps are kept in the same
order as they are represented in the sourcing DataFrame.
In case you need some custom ordering for the events with the same timestamps use the ``events_order`` parameter.
For example, due to some technical reasons events can arrive at the server with the same timestamp and
randomly change the order and create unnecessary variability.

After initial sorting, the relative order of raw events is fixed, and re-sorting is not possible.

In the dummy dataframe below, there are two users, each with a pair of events ("A" and "B") that have equal timestamps.
If we create an eventstream with default parameters, the order will be preserved, exactly the same as in the input dataframe.

.. code-block:: python

    df4 = pd.DataFrame(
        [
            ['user_1', 'A', '2023-01-01 00:00:00'],
            ['user_1', 'B', '2023-01-01 00:00:00'],
            ['user_2', 'B', '2023-01-01 00:00:03'],
            ['user_2', 'A', '2023-01-01 00:00:03'],
            ['user_2', 'A', '2023-01-01 00:00:04']
        ],
        columns=['user_id', 'event', 'timestamp']
    )

    stream4 = Eventstream(df4)
    stream4.to_dataframe()


.. raw:: html

    <div>
    <div style="overflow:auto;">
    <table class="dataframe">
       <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>event_type</th>
          <th>event_index</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>0</td>
        </tr>
        <tr>
          <th>1</th>
          <td>B</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>1</td>
        </tr>
        <tr>
          <th>2</th>
          <td>B</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>raw</td>
          <td>2</td>
        </tr>
        <tr>
          <th>3</th>
          <td>A</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>raw</td>
          <td>3</td>
        </tr>
        <tr>
          <th>4</th>
          <td>A</td>
          <td>2023-01-01 00:00:04</td>
          <td>user_2</td>
          <td>raw</td>
          <td>4</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br>

Now we create a new Eventstream from our dummy DataFrame, but with the specified ``events_order=["B", "A"]`` parameter.
As we can see, the first two events have swapped places.

.. code-block:: python

    Eventstream(df4, events_order=["B", "A"]).to_dataframe()

.. raw:: html

    <div>
    <div style="overflow:auto;">
    <table class="dataframe">
       <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>event_type</th>
          <th>event_index</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>B</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>0</td>
        </tr>
        <tr>
          <th>1</th>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>1</td>
        </tr>
        <tr>
          <th>2</th>
          <td>B</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>raw</td>
          <td>2</td>
        </tr>
        <tr>
          <th>3</th>
          <td>A</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>raw</td>
          <td>3</td>
        </tr>
        <tr>
          <th>4</th>
          <td>A</td>
          <td>2023-01-01 00:00:04</td>
          <td>user_2</td>
          <td>raw</td>
          <td>4</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br>


As for the synthetic events which are often placed at the beginning or in the end of a user's path, special sorting is applied.
There is a set of pre-defined event types, that are arranged in the following default order:

.. code-block:: python

    IndexOrder = [
        "profile",
        "path_start",
        "new_user",
        "existing_user",
        "cropped_left",
        "session_start",
        "session_start_cropped",
        "group_alias",
        "raw",
        "raw_sleep",
        None,
        "synthetic",
        "synthetic_sleep",
        "positive_target",
        "negative_target",
        "session_end_cropped",
        "session_end",
        "session_sleep",
        "cropped_right",
        "absent_user",
        "lost_user",
        "path_end"
    ]

Most of these types are created by build-in :ref:`data processors<dataprocessors_library>`.
Note that some of the types are not used right now and were created for future development.

To see full explanation about which data processor creates which ``event_type`` you can explore
the :ref:`data processors user guide<dataprocessors_adding_processors>`.

If needed, you can pass a custom sorting list to the Eventstream constructor as
the ``index_order`` argument.

In case you already have an eventstream instance, you can assign a custom sorting list
to ``Eventstream.index_order`` attribute. Afterwards, you should use
:py:meth:`index_events()<retentioneering.eventstream.eventstream.Eventstream.index_events>` method to
apply this new sorting. For demonstration purposes we use here a
:py:meth:`AddPositiveEvents<retentioneering.data_processors_lib.add_positive_events.AddPositiveEvents>`
data processor, which adds new event with prefix ``positive_target_``.

.. code-block:: python

    add_events_stream = stream4.add_positive_events(targets=['B'])
    add_events_stream.to_dataframe()

.. raw:: html

    <div>
    <div style="overflow:auto;">
    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>event_type</th>
          <th>event_index</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>0</td>
        </tr>
        <tr>
          <th>1</th>
          <td>B</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>1</td>
        </tr>
        <tr>
          <th>2</th>
          <td>positive_target_B</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>positive_target</td>
          <td>1</td>
        </tr>
        <tr>
          <th>3</th>
          <td>B</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>raw</td>
          <td>2</td>
        </tr>
        <tr>
          <th>4</th>
          <td>positive_target_B</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>positive_target</td>
          <td>2</td>
        </tr>
        <tr>
          <th>5</th>
          <td>A</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>raw</td>
          <td>3</td>
        </tr>
        <tr>
          <th>6</th>
          <td>A</td>
          <td>2023-01-01 00:00:04</td>
          <td>user_2</td>
          <td>raw</td>
          <td>4</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br>

We see that ``positive_target_B`` events with type ``positive_target``
follow their ``raw`` parent event ``B``. Assume we would like to change their order.

.. code-block:: python

    custom_sorting = [
        'profile',
        'path_start',
        'new_user',
        'existing_user',
        'cropped_left',
        'session_start',
        'session_start_cropped',
        'group_alias',
        'positive_target',
        'raw',
        'raw_sleep',
        None,
        'synthetic',
        'synthetic_sleep',
        'negative_target',
        'session_end_cropped',
        'session_end',
        'session_sleep',
        'cropped_right',
        'absent_user',
        'lost_user',
        'path_end'
    ]

    add_events_stream.index_order = custom_sorting
    add_events_stream.index_events()
    add_events_stream.to_dataframe()

.. raw:: html

    <div>
    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>event_type</th>
          <th>event_index</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>A</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>0</td>
        </tr>
        <tr>
          <th>1</th>
          <td>positive_target_B</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>positive_target</td>
          <td>1</td>
        </tr>
        <tr>
          <th>2</th>
          <td>B</td>
          <td>2023-01-01 00:00:00</td>
          <td>user_1</td>
          <td>raw</td>
          <td>1</td>
        </tr>
        <tr>
          <th>3</th>
          <td>positive_target_B</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>positive_target</td>
          <td>2</td>
        </tr>
        <tr>
          <th>4</th>
          <td>B</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>raw</td>
          <td>2</td>
        </tr>
        <tr>
          <th>5</th>
          <td>A</td>
          <td>2023-01-01 00:00:03</td>
          <td>user_2</td>
          <td>raw</td>
          <td>3</td>
        </tr>
        <tr>
          <th>6</th>
          <td>A</td>
          <td>2023-01-01 00:00:04</td>
          <td>user_2</td>
          <td>raw</td>
          <td>4</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br>

As we can see, the order of the events changed, and now ``raw`` events ``B``
follow ``positive_target_B`` events.


.. _eventstream_descriptive_methods:

Descriptive methods
-------------------

Eventstream provides a set of methods for a first touch data
exploration. To showcase how these methods work, we
need a larger dataset, so we will use our :doc:`simple_shop</datasets/simple_shop>`
dataset.
For demonstration purposes, we add ``session_id`` column by applying
:py:meth:`SplitSessions<retentioneering.data_processors_lib.split_sessions.SplitSessions>` data processor.


.. code-block:: python

    from retentioneering import datasets

    stream_with_sessions = datasets\
        .load_simple_shop()\
        .split_sessions(timeout=(30, 'm'))

    stream_with_sessions.to_dataframe().head()

.. raw:: html

    <div>
    <div style="overflow:auto;">
    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event</th>
          <th>timestamp</th>
          <th>user_id</th>
          <th>session_id</th>
          <th>event_type</th>
          <th>event_index</th>
          <th>event_id</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>session_start</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
          <td>session_start</td>
          <td>0</td>
          <td>92aa043e-02ac-4a4d-9f37-4bfc9dd101dc</td>
        </tr>
        <tr>
          <th>1</th>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
          <td>219483890_1</td>
          <td>raw</td>
          <td>1</td>
          <td>c1368d21-85fe-4ed0-864b-87b79eca8076</td>
        </tr>
        <tr>
          <th>3</th>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
          <td>219483890_1</td>
          <td>raw</td>
          <td>3</td>
          <td>4f437751-b117-4ef2-ba23-e91fe3a022fc</td>
        </tr>
        <tr>
          <th>5</th>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
          <td>219483890_1</td>
          <td>raw</td>
          <td>5</td>
          <td>740eeca9-db1f-4279-a0aa-ceb07de60638</td>
        </tr>
        <tr>
          <th>7</th>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
          <td>219483890_1</td>
          <td>raw</td>
          <td>7</td>
          <td>4f0d2ba7-396f-492e-bbf0-887ef14211e6</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br>

General statistics
~~~~~~~~~~~~~~~~~~

.. _eventstream_describe:

Describe
^^^^^^^^

In a similar fashion to pandas, we use :py:meth:`describe()<retentioneering.eventstream.eventstream.Eventstream.describe>`
for getting a general description of an eventstream.

.. code-block:: python

    stream_with_sessions.describe()

.. raw:: html


    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th></th>
          <th>value</th>
        </tr>
        <tr>
          <th>category</th>
          <th>metric</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th rowspan="6" valign="top">overall</th>
          <th>unique_users</th>
          <td>3751</td>
        </tr>
        <tr>
          <th>unique_events</th>
          <td>14</td>
        </tr>
        <tr>
          <th>unique_sessions</th>
          <td>6454</td>
        </tr>
        <tr>
          <th>eventstream_start</th>
          <td>2019-11-01 17:59:13</td>
        </tr>
        <tr>
          <th>eventstream_end</th>
          <td>2020-04-29 12:48:07</td>
        </tr>
        <tr>
          <th>eventstream_length</th>
          <td>179 days 18:48:53</td>
        </tr>
        <tr>
          <th rowspan="5" valign="top">path_length_time</th>
          <th>mean</th>
          <td>9 days 11:15:18</td>
        </tr>
        <tr>
          <th>std</th>
          <td>23 days 02:52:25</td>
        </tr>
        <tr>
          <th>median</th>
          <td>0 days 00:01:21</td>
        </tr>
        <tr>
          <th>min</th>
          <td>0 days 00:00:00</td>
        </tr>
        <tr>
          <th>max</th>
          <td>149 days 04:51:05</td>
        </tr>
        <tr>
          <th rowspan="5" valign="top">path_length_steps</th>
          <th>mean</th>
          <td>12.05</td>
        </tr>
        <tr>
          <th>std</th>
          <td>11.43</td>
        </tr>
        <tr>
          <th>median</th>
          <td>9.0</td>
        </tr>
        <tr>
          <th>min</th>
          <td>3</td>
        </tr>
        <tr>
          <th>max</th>
          <td>122</td>
        </tr>
        <tr>
          <th rowspan="5" valign="top">session_length_time</th>
          <th>mean</th>
          <td>0 days 00:00:52</td>
        </tr>
        <tr>
          <th>std</th>
          <td>0 days 00:01:08</td>
        </tr>
        <tr>
          <th>median</th>
          <td>0 days 00:00:30</td>
        </tr>
        <tr>
          <th>min</th>
          <td>0 days 00:00:00</td>
        </tr>
        <tr>
          <th>max</th>
          <td>0 days 00:23:44</td>
        </tr>
        <tr>
          <th rowspan="5" valign="top">session_length_steps</th>
          <th>mean</th>
          <td>7.0</td>
        </tr>
        <tr>
          <th>std</th>
          <td>4.18</td>
        </tr>
        <tr>
          <th>median</th>
          <td>6.0</td>
        </tr>
        <tr>
          <th>min</th>
          <td>3</td>
        </tr>
        <tr>
          <th>max</th>
          <td>55</td>
        </tr>
      </tbody>
    </table>
    <br>

The output consists of three main categories:

- **overall statistics**
- full user-path statistics
    - time distribution
    - steps (events) distribution
- sessions statistics
    - time distribution
    - steps (events) distribution

.. _explain_describe_params:

``session_col`` parameter is optional and points to the eventstream column that contains session ids
(``session_id`` is the default value). If such a column is defined, session statistics are also included.
Otherwise, the values related to sessions are not displayed.

There is one more parameter - ``raw_events_only`` (default False) that could be useful if some synthetic
events have already been added by :ref:`adding data processors <dataprocessors_adding_processors>`.
Note that those events affect all "\*_steps" categories.

Now let us go through the main categories and take a closer look at some of the metrics:

**overall**

By ``eventstream start`` and ``eventstream end`` in the "Overall" block we denote timestamps of the
first event and the last event in the eventstream correspondingly. ``eventstream_length``
is the time distance between event stream start and end.

**path/session length time** and **path/session length steps**

These two blocks show some time-based statistics over user paths and sessions.
Categories "path/session_length_time" and "path/session length steps" provide similar information
on the length of users paths and sessions correspondingly. The former is calculated in
days and the latter in the number of events.

It is important to mention that all the values in "\*_steps" categories are rounded to the 2nd decimal digit,
and in "\*_time" categories - to seconds. This is also true for the next method.

.. _eventstream_describe_events:

Describe events
^^^^^^^^^^^^^^^

The :py:meth:`describe_events()<retentioneering.eventstream.eventstream.Eventstream.describe_events>`
method provides event-wise statistics about an eventstream. Its output consists of three main blocks:

#. **basic statistics**
#. full user-path statistics,
    - time to first occurrence (FO) of each event,
    - steps to first occurrence (FO) of each event,
#. sessions statistics (if this column exists),
    - time to first occurrence (FO) of each event,
    - steps to first occurrence (FO) of each event.

You can find detailed explanations of each metric in
:py:meth:`api documentation<retentioneering.eventstream.eventstream.Eventstream.describe_events>`.

The default parameters are ``session_col='session_id'``, ``raw_events_only=False``.
With them, we will get statistics for each event present in our data. These two arguments
work exactly the same way as in the :ref:`describe()<explain_describe_params>` method.

.. code-block:: python

    stream = datasets.load_simple_shop()
    stream.describe_events()

.. raw:: html

    <div style="overflow:auto;">
    <table class="dataframe">
      <thead>
        <tr>
          <th></th>
          <th colspan="4" halign="left">basic_statistics</th>
          <th colspan="5" halign="left">time_to_FO_user_wise</th>
          <th colspan="5" halign="left">steps_to_FO_user_wise</th>
        </tr>
        <tr>
          <th></th>
          <th>number_of_occurrences</th>
          <th>unique_users</th>
          <th>number_of_occurrences_shared</th>
          <th>unique_users_shared</th>
          <th>mean</th>
          <th>std</th>
          <th>median</th>
          <th>min</th>
          <th>max</th>
          <th>mean</th>
          <th>std</th>
          <th>median</th>
          <th>min</th>
          <th>max</th>
        </tr>
        <tr>
          <th>event</th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
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
          <th>cart</th>
          <td>2842</td>
          <td>1924</td>
          <td>0.09</td>
          <td>0.51</td>
          <td>3 days 08:59:14</td>
          <td>11 days 19:28:46</td>
          <td>0 days 00:00:56</td>
          <td>0 days 00:00:01</td>
          <td>118 days 16:11:36</td>
          <td>4.51</td>
          <td>4.09</td>
          <td>3.0</td>
          <td>1</td>
          <td>41</td>
        </tr>
        <tr>
          <th>catalog</th>
          <td>14518</td>
          <td>3611</td>
          <td>0.45</td>
          <td>0.96</td>
          <td>0 days 05:44:21</td>
          <td>3 days 03:22:32</td>
          <td>0 days 00:00:00</td>
          <td>0 days 00:00:00</td>
          <td>100 days 08:19:51</td>
          <td>0.30</td>
          <td>0.57</td>
          <td>0.0</td>
          <td>0</td>
          <td>7</td>
        </tr>
        <tr>
          <th>delivery_choice</th>
          <td>1686</td>
          <td>1356</td>
          <td>0.05</td>
          <td>0.36</td>
          <td>5 days 09:18:08</td>
          <td>15 days 03:19:15</td>
          <td>0 days 00:01:12</td>
          <td>0 days 00:00:03</td>
          <td>118 days 16:11:37</td>
          <td>6.78</td>
          <td>5.56</td>
          <td>5.0</td>
          <td>2</td>
          <td>49</td>
        </tr>
        <tr>
          <th>delivery_courier</th>
          <td>834</td>
          <td>748</td>
          <td>0.03</td>
          <td>0.20</td>
          <td>6 days 18:14:55</td>
          <td>16 days 17:51:39</td>
          <td>0 days 00:01:28</td>
          <td>0 days 00:00:06</td>
          <td>118 days 16:11:38</td>
          <td>8.96</td>
          <td>6.84</td>
          <td>7.0</td>
          <td>3</td>
          <td>45</td>
        </tr>
        <tr>
          <th>delivery_pickup</th>
          <td>506</td>
          <td>469</td>
          <td>0.02</td>
          <td>0.13</td>
          <td>7 days 21:12:17</td>
          <td>18 days 22:51:54</td>
          <td>0 days 00:01:34</td>
          <td>0 days 00:00:06</td>
          <td>114 days 01:24:06</td>
          <td>9.51</td>
          <td>8.06</td>
          <td>7.0</td>
          <td>3</td>
          <td>71</td>
        </tr>
        <tr>
          <th>main</th>
          <td>5635</td>
          <td>2385</td>
          <td>0.17</td>
          <td>0.64</td>
          <td>3 days 20:15:36</td>
          <td>9 days 02:58:23</td>
          <td>0 days 00:00:07</td>
          <td>0 days 00:00:00</td>
          <td>97 days 21:24:23</td>
          <td>2.00</td>
          <td>2.94</td>
          <td>1.0</td>
          <td>0</td>
          <td>20</td>
        </tr>
        <tr>
          <th>payment_card</th>
          <td>565</td>
          <td>521</td>
          <td>0.02</td>
          <td>0.14</td>
          <td>6 days 21:42:26</td>
          <td>17 days 18:52:33</td>
          <td>0 days 00:01:40</td>
          <td>0 days 00:00:08</td>
          <td>138 days 04:51:25</td>
          <td>11.14</td>
          <td>7.34</td>
          <td>9.0</td>
          <td>5</td>
          <td>65</td>
        </tr>
        <tr>
          <th>payment_cash</th>
          <td>197</td>
          <td>190</td>
          <td>0.01</td>
          <td>0.05</td>
          <td>13 days 23:17:25</td>
          <td>24 days 00:00:02</td>
          <td>0 days 00:02:18</td>
          <td>0 days 00:00:10</td>
          <td>118 days 16:11:39</td>
          <td>14.15</td>
          <td>11.10</td>
          <td>9.5</td>
          <td>5</td>
          <td>73</td>
        </tr>
        <tr>
          <th>payment_choice</th>
          <td>1107</td>
          <td>958</td>
          <td>0.03</td>
          <td>0.26</td>
          <td>6 days 12:49:38</td>
          <td>17 days 02:54:51</td>
          <td>0 days 00:01:24</td>
          <td>0 days 00:00:06</td>
          <td>118 days 16:11:39</td>
          <td>9.42</td>
          <td>6.37</td>
          <td>7.0</td>
          <td>4</td>
          <td>52</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>706</td>
          <td>653</td>
          <td>0.02</td>
          <td>0.17</td>
          <td>7 days 01:37:54</td>
          <td>17 days 09:10:00</td>
          <td>0 days 00:01:34</td>
          <td>0 days 00:00:08</td>
          <td>115 days 09:18:59</td>
          <td>12.21</td>
          <td>8.29</td>
          <td>10.0</td>
          <td>5</td>
          <td>84</td>
        </tr>
        <tr>
          <th>product1</th>
          <td>1515</td>
          <td>1122</td>
          <td>0.05</td>
          <td>0.30</td>
          <td>5 days 23:49:43</td>
          <td>16 days 04:36:13</td>
          <td>0 days 00:00:50</td>
          <td>0 days 00:00:00</td>
          <td>118 days 19:38:40</td>
          <td>5.46</td>
          <td>6.04</td>
          <td>3.0</td>
          <td>1</td>
          <td>61</td>
        </tr>
        <tr>
          <th>product2</th>
          <td>2172</td>
          <td>1430</td>
          <td>0.07</td>
          <td>0.38</td>
          <td>4 days 06:13:24</td>
          <td>13 days 03:26:17</td>
          <td>0 days 00:00:34</td>
          <td>0 days 00:00:00</td>
          <td>126 days 23:36:45</td>
          <td>4.32</td>
          <td>4.51</td>
          <td>3.0</td>
          <td>1</td>
          <td>36</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br>

If the number of unique events in an eventstream is high,
we can leave events only from the list defined in ``event_list`` parameter.
In the example below we leave the ``cart`` and ``payment_done`` events only as the events of high importance.
We also transpose the output DataFrame for a nicer view.

.. code-block:: python

    stream.describe_events()
    stream.describe_events(event_list=['payment_done', 'cart']).T

.. raw:: html

      <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>event</th>
          <th>cart</th>
          <th>payment_done</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th rowspan="4" valign="top">basic_statistics</th>
          <th>number_of_occurrences</th>
          <td>2842</td>
          <td>706</td>
        </tr>
        <tr>
          <th>unique_users</th>
          <td>1924</td>
          <td>653</td>
        </tr>
        <tr>
          <th>number_of_occurrences_shared</th>
          <td>0.09</td>
          <td>0.02</td>
        </tr>
        <tr>
          <th>unique_users_shared</th>
          <td>0.51</td>
          <td>0.17</td>
        </tr>
        <tr>
          <th rowspan="5" valign="top">time_to_FO_user_wise</th>
          <th>mean</th>
          <td>3 days 08:59:14</td>
          <td>7 days 01:37:54</td>
        </tr>
        <tr>
          <th>std</th>
          <td>11 days 19:28:46</td>
          <td>17 days 09:10:00</td>
        </tr>
        <tr>
          <th>median</th>
          <td>0 days 00:00:56</td>
          <td>0 days 00:01:34</td>
        </tr>
        <tr>
          <th>min</th>
          <td>0 days 00:00:01</td>
          <td>0 days 00:00:08</td>
        </tr>
        <tr>
          <th>max</th>
          <td>118 days 16:11:36</td>
          <td>115 days 09:18:59</td>
        </tr>
        <tr>
          <th rowspan="5" valign="top">steps_to_FO_user_wise</th>
          <th>mean</th>
          <td>4.51</td>
          <td>12.21</td>
        </tr>
        <tr>
          <th>std</th>
          <td>4.09</td>
          <td>8.29</td>
        </tr>
        <tr>
          <th>median</th>
          <td>3.0</td>
          <td>10.0</td>
        </tr>
        <tr>
          <th>min</th>
          <td>1</td>
          <td>5</td>
        </tr>
        <tr>
          <th>max</th>
          <td>41</td>
          <td>84</td>
        </tr>
      </tbody>
    </table>
    <br>

Often, such simple descriptive statistics are not enough to deeply understand the time-related values,
so we want to see their distribution. For these purposes the following group of methods has been implemented.


Time-based histograms
~~~~~~~~~~~~~~~~~~~~~

.. _eventstream_user_lifetime:

User lifetime
^^^^^^^^^^^^^

One of the most important time-related statistics is user lifetime. By lifetime we
mean the time distance between the first and the last event represented
in a user's trajectory. The histogram for this variable is plotted by
:py:meth:`user_lifetime_hist()<retentioneering.eventstream.eventstream.Eventstream.user_lifetime_hist>` method.

.. code-block:: python

    stream.user_lifetime_hist()

.. figure:: /_static/user_guides/eventstream/01_user_lifetime_hist_simple.png
    :width: 500


The method has multiple parameters:

.. _common_hist_params:

- ``timedelta_unit`` defines a
  `datetime unit <https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-units>`_
  that is used for the lifetime measuring;

- ``log_scale`` sets logarithmic scale for the bins;

- ``lower_cutoff_quantile``, ``upper_cutoff_quantile`` indicate the lower and upper quantiles
  (as floats between 0 and 1), the values between the quantiles only are considered for the histogram;

- ``bins`` defines the number of histogram bins. Also can be the name of a reference rule or
  number of bins. See details in
  `numpy documentation <https://numpy.org/doc/stable/reference/generated/numpy.histogram_bin_edges.html>`_;

- ``width`` and ``height`` set figure width and height in inches.

.. note::

    The method is especially useful for selecting parameters to
    :py:meth:`DropPaths<retentioneering.data_processors_lib.drop_paths.DropPaths>`.
    See :doc:`the user guide on preprocessing</user_guides/dataprocessors>` for details.

.. _eventstream_timedelta_hist:

Timedelta between two events
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Previously, we have defined user lifetime as the timedelta between the beginning and the end of a user's path.
This can be generalized.
:py:meth:`timedelta_hist()<retentioneering.eventstream.eventstream.Eventstream.timedelta_hist>`
method shows a histogram for the distribution of timedeltas between a couple of specified events.

The method supports similar formatting arguments (``timedelta_unit``, ``log_scale``,
``lower_cutoff_quantile``, ``upper_cutoff_quantile``, ``bins``, ``width``, ``height``) as we have already mentioned
in :ref:`user_lifetime_hist<common_hist_params>` method.

If no arguments are passed (except formatting arguments), timedeltas between all adjacent events are
calculated within each user path. For example, this tiny eventstream

.. figure:: /_static/user_guides/eventstream/02_timedelta_trivial_example.png
    :width: 500

generates 4 timedeltas :math:`\Delta_1, \Delta_2, \Delta_3, \Delta_4` as shown in the diagram.
The timedeltas between events B and D, D and C, C and E are not taken into account because two events
from each pair belong to different users.

Here is how the histogram looks for the ``simple_shop`` dataset with ``log_scale=True`` and ``timedelta_unit='m'``:

.. code-block:: python

    stream.timedelta_hist(log_scale=True, timedelta_unit='m')

.. figure:: /_static/user_guides/eventstream/03_timedelta_log_scale.png
    :width: 500

This distribution of the adjacent events fairly common. It looks like a bimodal (which is not true:
remember we use log-scale here), but these two bells help us to estimate a timeout for splitting sessions.
From this charts we can see that it is reasonable to set it to some value between 10 and 100 minutes.

Be careful if there are some synthetic events in the data. Usually those events are assigned with the same
timestamp as their "parent" raw events. Thus, the distribution of the timedeltas between
events will be heavily skewed to 0. Parameter ``raw_events_only=True`` can help in such a situation.
Let us add to our dataset some common synthetic events using :ref:`AddStartEndEvents<add_start_end_events>` and
:ref:`SplitSessions<split_sessions>` data processors.

.. code-block:: python

    stream_with_synthetic = datasets\
        .load_simple_shop()\
        .add_start_end_events()\
        .split_sessions(timeout=(30, 'm'))

    stream_with_synthetic.timedelta_hist(log_scale=True, timedelta_unit='m')
    stream_with_synthetic.timedelta_hist(
        raw_events_only=True,
        log_scale=True,
        timedelta_unit='m'
    )

.. figure:: /_static/user_guides/eventstream/04_timedelta_raw_events_only_false.png
    :width: 500

.. figure:: /_static/user_guides/eventstream/05_timedelta_raw_events_only_true.png
    :width: 500

You can see that on the second plot there is no high histogram bar located at :math:`\approx 10^{-3}`,
so that the second histogram looks more natural.

Another use case for :py:meth:`timedelta_hist()<retentioneering.eventstream.eventstream.Eventstream.timedelta_hist>`
is visualizing the distribution of timedeltas between two specific events. Assume we want to
know how much time it takes for a user to go from ``product1`` to ``cart``.
Then we set ``event_pair=('product1', 'cart')`` and pass it to ``timedelta_hist``:

.. code-block:: python

    stream.timedelta_hist(event_pair=('product1', 'cart'), timedelta_unit='m')

.. figure:: /_static/user_guides/eventstream/06_timedelta_pair_of_events.png
    :width: 500

From the Y scale, we see that such occurrences are not very numerous. This is because the method still works with only
adjacent pairs of events (in this case ``product1`` and ``cart`` are assumed to go one right after
another in a user's path). That is why the histogram is skewed to 0.
``adjacent_events_only`` parameter allows us to work with any cases when a user goes from
``product1`` to ``cart`` non-directly but passing through some other events:

.. code-block:: python

    stream.timedelta_hist(
        event_pair=('product1', 'cart'),
        timedelta_unit='m',
        adjacent_events_only=False
    )

.. figure:: /_static/user_guides/eventstream/07_timedelta_adjacent_events_only.png
    :width: 500

We see that the number of observations has increased, especially around 0. In other words,
for the vast majority of the users transition ``product1 → cart`` takes less than 1 day.
On the other hands, we observe a "long tail" of the users whose journey from ``product1``
to ``cart`` takes multiple days. We can interpret this as there are two behavioral clusters:
the users who are open for purchases, and the users who are picky. However, we also notice
that adding a product to a cart does not necessarily mean that a user intends to make a
purchase. Sometimes users adds an item to a cart just to check its final price, delivery
options, etc.

Here we should make a stop and explain how timedeltas between event pairs are calculated.
Below you can see the picture of one user path and timedeltas that will be displayed in a ``timedelta_hist``
with the parameters ``event_pair=('A', 'B')`` and ``adjacent_events_only=False``.

Let us consider each time delta calculation:

- :math:`\Delta_1` is calculated between 'A' and 'B' events. 'D' and 'F' are ignored because
  of ``adjacent_events_only=False``.
- The next 'A' event is colored grey and is skipped because there is one more 'A' event closer
  to the 'B' event. In such cases, we pick the 'A' event, that is closer to the next 'B' and calculate
  :math:`\Delta_2`.

.. figure:: /_static/user_guides/eventstream/08_event_pair_explanation.png
    :width: 500

    Single user path

Now let us get back to our example. Due to the fact we have a lot of users with short trajectories and
a few users with very long paths our histogram is unreadable.

To make entire plot more comprehensible - the ``log_scale`` parameter can be used.
We have already used that parameter for the ``x axis``, but it is also available fot the ``y axis``.
For example: ``log_scale=(False, True)``.

Another way to resolve that problem, is to look separately on different parts of our plot.
For that purpose we can use parameters ``lower_cutoff_quantile`` and ``upper_cutoff_quantile``.
These parameters specify boundaries for the histogram and will be applied last.

In the example below, firstly, we keep users with ``event_pair=('product1', 'cart')``
and ``adjacent_events_only=False``, and after it we truncate 90% of users with the shortest
trajectories and keep 10% of the longest.

.. code-block:: python

    stream.timedelta_hist(
            event_pair=('product1', 'cart'),
            timedelta_unit='m',
            adjacent_events_only=False,
            lower_cutoff_quantile=0.9
        )

.. figure:: /_static/user_guides/eventstream/timedelta_lower_cutoff_quantile.png

Here it is the same algorithm, but 10% of users with the shortest trajectories will be kept.

.. code-block:: python

    stream.timedelta_hist(
            event_pair=('product1', 'cart'),
            timedelta_unit='m',
            adjacent_events_only=False,
            upper_cutoff_quantile=0.1
        )

.. figure:: /_static/user_guides/eventstream/timedelta_upper_cutoff_quantile.png

If we set both parameters, boundaries will be calculated simultaneously and truncated afterward.

Let us turn to another case. Sometimes we are interested in looking only at events
within a user session. If we have already split the paths into sessions, we can use ``weight_col='session_id'``:

.. code-block:: python

    stream_with_synthetic\
        .timedelta_hist(
            event_pair=('product1', 'cart'),
            timedelta_unit='m',
            adjacent_events_only=False,
            weight_col='session_id'
        )

.. figure:: /_static/user_guides/eventstream/09_timedelta_sessions.png
    :width: 500

It is clear now that within a session the users walk from ``product1`` to ``cart`` event in less than 3 minutes.

For frequently occurring events we might be interested in aggregating the timedeltas over sessions or users.
For example, transition ``main -> catalog`` is quite frequent. Some users do these transitions quickly,
some of them do not. It might be reasonable to aggregate the timedeltas over each user path first
(we would get one value per one user at this step), and then visualize the distribution of
these aggregated values. This can be done by passing an additional argument
``time_agg='mean'`` or ``time_agg='median'``.

.. code-block:: python

    stream\
        .timedelta_hist(
            event_pair=('main', 'catalog'),
            timedelta_unit='m',
            adjacent_events_only=False,
            weight_col='user_id',
            time_agg='mean'
        )

.. figure:: /_static/user_guides/eventstream/10_timedelta_time_agg_mean.png
    :width: 500


Eventstream global events
^^^^^^^^^^^^^^^^^^^^^^^^^

``event_pair`` argument can accept a couple of auxiliary events: ``eventstream_start`` and ``eventstream_end``.
They indicate the first and the last events in an evenstream.

It is especially useful for choosing ``left_cutoff`` and  ``right_cutoff`` parameters for
:py:meth:`LabelCroppedPaths<retentioneering.data_processors_lib.label_cropped_paths.LabelCroppedPaths>` data processor.
Before you choose it, you can explore how a path's beginning/end margin from the right/left edge of an eventstream.
In the histogram below, :math:`\Delta_1` illustrates such a margin for ``event_pair=('eventstream_start', 'B')``.
Note that here only one timedelta is calculated - from the 'eventstream_start' to the first occurrence of specified
event.

.. figure:: /_static/user_guides/eventstream/11_timedelta_event_pair_with_global.png
    :width: 500


:math:`\Delta_1` in the following example illustrates a margin for ``event_pair=('B', 'eventstream_end')``.
And again, only one timedelta per userpath is calculated - from the 'B' event (its last occurrence) to the
'eventstream_end'.

.. figure:: /_static/user_guides/eventstream/11_timedelta_event_pair_with_global_end.png
    :width: 500



.. code-block:: python

    stream_with_synthetic\
        .timedelta_hist(
            event_pair=('eventstream_start', 'path_end'),
            timedelta_unit='h',
            adjacent_events_only=False
        )


.. figure:: /_static/user_guides/eventstream/12_timedelta_eventstream_start_path_end.png
    :width: 500

For more details on how this histogram helps to define ``left_cutoff`` and  ``right_cutoff`` parameters see
:ref:`LabelCroppedPaths section<label_cropped_paths>` in the data processors user guide.

.. _eventstream_events_timestamp:

Event intensity
^^^^^^^^^^^^^^^

There is another helpful diagram that can be used for eventstream overview.
Sometimes we want to know how the events are distributed over time. The histogram for this distribution is plotted by
:py:meth:`event_timestamp_hist()<retentioneering.eventstream.eventstream.Eventstream.event_timestamp_hist>`
method.

.. code-block:: python

    stream.event_timestamp_hist()

.. figure:: /_static/user_guides/eventstream/13_event_timestamp_hist.png
    :width: 500

We can notice the heavy skew in the data towards the period between April and May of 2020.
One of the possible interpretations of this fact is that the product worked in beta version until April 2020,
and afterwards a stable were released so that new users started to arrive much more intense.
``event_timestamp_hist`` has ``event_list`` argument, so we can check this hypothesis
by choosing ``path_start`` in the event list .

.. code-block:: python

    stream\
        .add_start_end_events()\
        .event_timestamp_hist(event_list=['path_start'])

.. figure:: /_static/user_guides/eventstream/14_event_timestamp_hist_event_list.png
    :width: 500

From this histogram we see that our hypothesis is true. New users started to arrive much more intense in April 2020.

Similar to :py:meth:`timedelta_hist()<retentioneering.eventstream.eventstream.Eventstream.timedelta_hist>`,
``event_timestamp_hist`` also has parameters ``raw_events_only``, ``upper_cutoff_quantile``,
``lower_cutoff_quantile``, ``bins``, ``width`` and ``height`` that work with the same logic.
