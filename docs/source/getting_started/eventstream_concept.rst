:orphan:

Eventstream class overview
==========================

Why Eventstream
---------------

Let's start from a review of a process how data analyst (we also use researcher as a synonym) usually work with data. On |fig_research_workflow| you can see a simplified workflow of a typical analytical research. What data analysts actually do: they download collected clickstream data from a data storage, prepare the data, analyze it by applying an analytical tool, and deliver actionable results to stakeholders.

.. |fig_research_workflow| replace:: Fig. 1
.. figure:: /_static/getting_started/eventstream_concept/research_workflow.png
    :width: 800

    Fig. 1. A simplified workflow of a typical analytical research

What's important for us that two of these steps are repeated throughout the research. These steps are:

- | **Data reprocessing**. I.e. preparing the data to be analyzed (cleaning the data up, reformatting, etc).

- **Applying an analytical tool to prepared data**. By tool we mean a wide range of analytical techniques (fitting a statistical model, making a visualization, etc).

The loop appears mainly due to two reasons:

- The research question is poorly defined, so the data analyst have to formalize it in multiple possible ways. It's not clear until it's done whether a particular formalization provides a reasonable result, so the data analyst has to walk through all the possible paths.

- The data is never perfect. In practice, it's often inconsistent, full of surprising problems, outliers, technical issues, etc. But even if you've cleaned up your data enough you still have to tailor it towards a specific analytical tool. The more tools you're going to apply, the more data formats you need to convert your data to.

Assume for simplicity, that the goal of a research is to explore users behavior in scope of their first steps in the product. Also, we assume that some cleaning up operations preceded, and the data is consistent now. Obviously, “first steps” are needed to be defined in a clear way, so the data analyst might consider multiple possible cases, e.g.:

- User’s first day,

- User’s first session,

- A sub-path from user’s registration to a target event (like completing an onboarding tour).

Next, by “to explore users behavior” one could mean applying different techniques, such as analyzing Markov chain model, performing cluster analysis, analyzing a step-wise Sankey diagram. As a result, we obtain 9 different analytical cases (see |fig_research_example|).

.. |fig_research_example| replace:: Fig. 2
.. figure:: /_static/getting_started/eventstream_concept/research_example.png
    :width: 800

    Fig. 2. An example of an analytical research with branching logic.

Supporting all the corresponding code in a single Jupyter notebook is often neither convenient nor resource-efficient. Indeed, you have to keep all the dataframes in the notebook’s memory, you have to control the order of executing notebook’s cells, and you need to organize the code so it could be easy-to-read. Things become even worse when a data analyst needs to share this notebook for maintenance with another colleague: diving into the sheets of the code might be tough for a newcomer.

All the problems described above inclined us to create a solution which could treat a clickstream in an efficient way.

Introducing Eventstream class
-----------------------------

``Eventstream`` is a core class of the library. The role ``Eventstream`` plays is 3-folded.

- Data container. ``Eventstream`` stores the original clickstream data. All the modified states of the original data a researcher gets throughout the analysis are also ``Eventstream`` instances.

- Preprocessing. ``Eventstream`` provides a wide range of methods which wrangle clickstream data in many useful ways.

- Applying analytical tools. Having a particular prepared state of an eventstream, you can apply an analytical tool by calling an appropriate method.

We assume that the original clickstream which generates an ``Eventstream`` class instance is represented by a ``pandas.DataFrame`` and consists of three columns: user_id, event, timestamp. In this case you can create an eventstream especially easy:

.. code:: python

    import pandas as pd
    import retentioneering as rete

    df = pd.read_csv("clickstream_data.csv")
    stream = rete.Eventstream(df)

In case your dataframe has columns named differently you can either rename them or use

:red:`Simplify the library path to RawDataSchema`

.. code-block:: python

    import pandas as pd
    import retentioneering as rete
    from retentioneering.eventstream.schema import RawDataSchema

    raw_data_schema = RawDataSchema(
        event_name='action',
        event_timestamp='datatime',
        user_id='uid'
    )

    df = pd.read_csv("clickstream_data.csv")
    stream = rete.Eventstream(df, raw_data_schema=raw_data_schema)

But for demonstrating purposes we'll use an embedded clickstream *simple_shop*

.. code-block:: python

    import retentioneering as rete

    stream = rete.datasets.load.load_simple_shop

As soon as you create an eventstream, you can check the underlying dataframe by calling ``rete.Eventstream.to_dataframe()`` method:

.. code-block:: python

    stream.to_dataframe()\
        .head()

.. |fig_eventstream_columns| replace:: Fig. 3
.. figure:: /_static/getting_started/eventstream_concept/eventstream_columns.png
    :width: 800

    Fig. 3. An example of a preprocessing graph.

As we see on |fig_eventstream_columns|, the underlying dataframe contains 3 original columns ``event``, ``timestamp``, ``user_id``, and 3 additional columns:

- event_id. This is a unique identifier of the event.

- event_type. All the original events are of ``raw`` type. Special synthetic events have different type. :red:`Give a reference to synthetic events review`

- event_index. This column is used for sorting an eventstream. Normally, all the raw events are sorted by timestamp column. However, there are some corner cases for additional synthetic events. :red:`Give a reference to synthetic events review`.


Preprocessing
-------------

As it was mentioned above, we define *preprocessing* as any data preparations preceding applying a core analytical tool. The sequence of preprocessing calculations naturally constitutes a directed acyclic graph (DAG). The nodes represent some specific calculations while the edges define the order of the calculations to be run. Here's an example of such a graph on |fig_preprocessing_graph|.

.. |fig_preprocessing_graph| replace:: Fig. 4
.. figure:: /_static/getting_started/eventstream_concept/preprocessing_graph.png
    :width: 800

    Fig. 4. An example of a preprocessing graph.


We start the description of preprocessing graph from its elementary part -- *atomic operation*.

Atomic operations
~~~~~~~~~~~~~~~~~

On the basic level, there are 3 possible atomic operations one could apply to an eventstream: insert, delete, edit.

Insert operations are associated with adding so called *synthetic events* meaning that these events are not
represented in the original clickstream. These events aim to bring some additional information about a current
state of a user at her particular path step. For example, when we split an eventstream into
sessions we add ``session_start`` and ``session_end`` synthetic events indicating the explicit
beginning and the end of each session.

Delete operations are used when you need to remove some useless/rubbish/technical events from the eventstream,
or remove some paths entirely or partially.

Edit operations are useful when you need to rename or group some events. In many products user events
have their natural taxonomy, so you might want to group them in order to provide different levels of granularity.

.. _join algorithm:

All these operations might be implemented with ``LEFT OUTER JOIN`` operator. Why ``LEFT OUTER JOIN``?
i) It guarantees that the keys from the left table are kept safe and
ii) adds some new keys from the right table which are not represented in the left table.

These properties allow us to manage all the preprocessing calculations keeping the original events intact.
And this fundamental property, in turn, makes switching between eventstream states possible.
The exact way how we do this is described in the next section.

:red:`TODO: Make nicer images`

.. |atomic_insert| image:: /_static/getting_started/eventstream_concept/atomic_insert.png
.. |atomic_delete| image:: /_static/getting_started/eventstream_concept/atomic_delete.png
.. |atomic_edit| image:: /_static/getting_started/eventstream_concept/atomic_edit.png

+---------+-------------------+
| Insert  +  |atomic_insert|  +
+---------+-------------------+
| Delete  +  |atomic_delete|  +
+---------+-------------------+
| Edit    +  |atomic_edit|    +
+---------+-------------------+


Data processors
~~~~~~~~~~~~~~~

``DataProcessor`` is an abstract class for building nodes of a preprocessing graph, and any its child class is called a *data processor*. Unlike atomic operations which are abstract and doesn't specify particular logic, data processors define how exactly eventstream should be modified. Each data processor has a supplementary class (a child of abstract ``ParamsModel`` class) which contains its parameters as attribute references.

For example, ``SplitSessions`` data processor adds explicit synthetic events to an eventstream indicating session boundaries. A pair of ``session_end`` and ``session_start`` events is added as soon as the distance between two sequential events in a user's trajectory is greater than a specified threshold -- ``timeout``. This parameter is embedded into ``SplitSessionsParams`` as the attribute reference.

Similar to atomic operations, data processors could be categorized into three parts according to whether they add, remove or group events. Here we provide a brief overview. The comprehensive documentation on data processors is located :red:`TODO: here`.

.. table:: Data processors overview
    :widths: 15 10 60 15
    :class: tight-table

    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | Data processor           | Type      | What it does                                                                                                                                                   | Helper           |
    +==========================+===========+================================================================================================================================================================+==================+
    | StartEndEvents           | Adding    | Adds two synthetic events in each user's path: ``path_start`` and ``path_end``                                                                                 | start_end_event  |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | SplitSessions            | Adding    | Cuts user path into sessions and adds synthetic events ``session_start``, ``session_end``.                                                                     | split_sessions   |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | NewUsersEvents           | Adding    | Adds synthetic event ``new_user`` in the beginning of a user's path if the user is considered as new. Otherwise adds ``existing_user``.                        | add_new_users    |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | LostUsersEvents          | Adding    | Adds synthetic event ``lost_user`` in the end of user's path if the user never comes back to the product. Otherwise adds ``absent_user`` event.                | lost_users       |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | PositiveTarget           | Adding    | Adds synthetic event ``positive_target`` for all events which are considered as positive.                                                                      | positive_target  |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | NegativeTarget           | Adding    | Adds synthetic event ``negative_target`` for all events which are considered as negative.                                                                      | negative_target  |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | TruncatedEvents          | Adding    | Adds synthetic events ``truncated_left`` and/or ``truncated_right`` for those user paths which are considered as truncated by the edges of the whole dataset.  | truncated_events |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | FilterEvents             | Removing  | Remove events from an eventstream                                                                                                                              | filter           |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | DeleteUsersByPathLength  | Removing  | Deletes a too short user paths (in terms of number of events or time duration).                                                                                | delete_users     |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | TruncatePath             | Removing  | Leaves a part of an eventstream between a couple of selected events.                                                                                           | truncate_path    |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | GroupEvents              | Grouping  | Group given events into a single synthetic event.                                                                                                              | group            |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
    | CollapseLoops            | Grouping  | Replaces sequences of repetitive events with new synthetic events. E.g. ``A, A, A -> A``.                                                                      | collapse_loops   |
    +--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+

Custom data processors
~~~~~~~~~~~~~~~~~~~~~~
:red:`Move to DataProcessors user guide`

In case the data processors implemented in the library don't cover your needs, you can develop your own data processor. The interface is as follows:

- Data processor class must be inherited from ``DataProcessor`` class, while its parameters class must be a child of ``ParamsModel`` class.

- Parameters class must simply describe the parameters as class attributes.

- Constructor of the data processor class must accept ``params`` parameter of parameters class.

- ``apply`` method must be implemented in the data processor class. The method must accept eventstream parameter and return another eventstream representing the changed state of the input eventstream. ``ref`` column indicates the reference of the original event. It is used in ``LEFT OUTER JOIN`` (see Atomic operations section :red:`TODO: set the link`). The behavior of the method implementation depends on the type of the data processor: adding, removing or grouping.

Editing data processor
^^^^^^^^^^^^^^^^^^^^^^

Let's have an example here. Consider simpleshop_dataset. Suppose you want to round the timestamp column up so seconds/minutes/hours. If you used pure pandas you would implement it like this.

.. code-block:: python

    def round_timestamp(df, freq: Literal["H", "M", "S"]) -> pd.DataFrame:
        df["timestamp"] = df["timestamp"].dt.floor(freq)
        return df

Now, you need to wrap this logic into the data processor class, and here things go more complicate.

.. code-block:: python

    from typing import Literal
    from retentioneering.eventstream.eventstream import Eventstream
    from retentioneering.data_processor.data_processor import DataProcessor
    from retentioneering.params_model import ParamsModel

    class RoundTimestampParams(ParamsModel):
        freq: Literal["H", "M", "S"] = "S"

    class RoundTimestamp(DataProcessor):
        params: RoundTimestampParams

        def __init__(self, params: RoundTimestampParams) -> None:
            super().__init__(params=params)

        def apply(self, eventstream: Eventstream) -> Eventstream:
            time_col = eventstream.schema.event_timestamp
            freq = self.params.freq
            df = eventstream.to_dataframe()\
                .assign(**{time_col: lambda df_: df_[time_col].dt.floor(freq)})\
                .assign(ref=lambda df_: df_[eventstream.schema.event_id])\

            eventstream = Eventstream(
                schema=stream.schema.copy(),
                raw_data_schema=stream.schema.copy(),
                raw_data=df,
                relations=[{"raw_col": "ref", "eventstream": eventstream}],
            )

            return eventstream

Finally, we need to build a graph with a single node encompassing ``RoundTimestamp`` data processor.

.. code-block:: python

    from retentioneering.graph.p_graph import PGraph, EventsNode

    node = EventsNode(RoundTimestamp(params=RoundTimestampParams()))
    graph = PGraph(stream)
    graph.add_node(node, parents=[graph.root])

    graph.combine(node=node).to_dataframe()

Adding data processor
^^^^^^^^^^^^^^^^^^^^^
:red:`TODO:`

Removing data processor
^^^^^^^^^^^^^^^^^^^^^^^
:red:`TODO:`




Preprocessing graph
~~~~~~~~~~~~~~~~~~~

Nodes and edges
^^^^^^^^^^^^^^^

The nodes of preprocessing graph belong to ``EventNode`` class and could be of two types. In general, a node is a shell for its underlying data processor. This regular node accepts a single eventstream as input and defines how it should be modified. The entire structure of this node is illustrated on |fig_event_node_structure|.

.. |fig_event_node_structure| replace:: Fig. 5
.. figure:: /_static/getting_started/eventstream_concept/event_node_structure.png
    :width: 200

    Fig. 5. The nested structure of EventNode class.

Unlike these regular nodes, merging nodes accept multiple eventstreams as input, concatenate them, and drop possible duplicates.

Linking graph nodes according to preprocessing logic, we obtain a ``preprocessing graph``. Preprocessing graphs are instances of ``PGraph`` class. To add a node to the graph use  ``add_node`` method. The links are set via ``parents`` parameter of the method. Here’s an tiny example how to create a simple preprocessing graph consisting of two nodes ``StartEndEvents`` and ``SplitSessions``.

.. code-block:: python

    from retentioneering.graph.p_graph import PGraph, EventsNode
    from retentioneering.data_processors_lib import SplitSessions, SplitSessionsParams
    from retentioneering.data_processors_lib import StartEndEvents, StartEndParams

    # creating single nodes
    node1 = EventsNode(StartEndEvents(params=StartEndEventsParams()))
    node2 = EventsNode(SplitSessions(params=SplitSessionsParams(timeout=(1, 'h'))))

    # creating a preprocessing graph and linking the nodes
    pgraph = PGraph(source_stream=stream)
    pgraph.add_node(node=node1, parents=[pgraph.root])
    pgraph.add_node(node=node2, parents=[node1])


Preprocessing graph as a calculation schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now, it's important to note that when we construct a preprocessing graph we don’t run calculations. Preprocessing graph just profiles a calculation schema defining what exactly and when exactly should be calculated. Particularly, when the calculation logic splits, it doesn't mean that the split branches are run in parallel simultaneously.

In order to run a calculation directly, you should call ``combine`` (:red:`TODO: See combine method`) method. Here you need to choose a node which you consider as an endpoint meaning that the calculation should run from the root (the initial eventstream state) to the selected node. ``combine`` returns you the modified eventstream state according to the given preprocessing calculation path.

.. code-block:: python

    # run the calculation from the root node to SplitSessions node
    processed_stream = pgraph.combine(node=node2)


We also highlight that having an eventstream combined at some graph's point doesn’t affect the original data -- it stays immutable. In fact, the records you see removed are just marked as removed and invisible for you at this state. The renamed or grouped events are shown as renamed, but their predecessors are kept physically untouched. You can check it setting the visibility with ``show_deleted`` flag of ``Eventstream.to_dataframe()`` method.


Chaining preprocessing methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In many real-world scenarios preprocessing graph has simple linear structure (e.g. no splitting, no merging). For such cases instead of constructing a preprocessing graph it would be useful to chain so-called *helpers* methods. Helpers are special ``Eventstream`` methods associated with corresponding data processors. They simply take ``Eventstream`` instance as input and return a modified eventstream. Here's how the implementation of the  graph from the example above could be improved:

.. code-block:: python

    processed_stream = stream \
        .add_start_end() \
        .split_sessions(timeout=(1, 'h'))


GUI
^^^

There's another elegant way to construct a preprocessing graph. This could be done using GUI.

:red:`TODO: Describe it`


Retentioneering tools
---------------------

Retentioneering tools are designed as stand-alone classes, but the instances of these classes might be embedded into ``Eventstream`` class instance. This allows either to create a separate tooling instance and treat it as usual or to use it in chaining manner.

Suppose we need to split paths of an eventstream into 4 clusters and compare the event distribution in cluster 0 vs cluster 1. Below are two ways how this could be achieved.

Treating clustering tool as a separate instance:

.. code-block:: python

    from retentioneering.tooling.clusters import Clusters

    clusters = Clusters(stream)
    clusters.fit(method='kmeans', feature_type='tfidf', ngram_range=(1, 1), n_clusters=4)
    clusters.event_dist(cluster_id1=0, cluster_id2=1)

Treating clustering tool as chaining methods:

.. code-block:: python

    stream\
        .clusters\
        .fit(method='kmeans', feature_type='tfidf', ngram_range=(1, 1), n_clusters=4)\
        .event_dist(cluster_id1=0, cluster_id2=1)

The table below contains a brief overview of Retentioneering tools. The comprehensive description on all the tools work can be found in these sections: :doc:`User Guide </user_guide>` and :doc:`API Reference </api/tooling_api>`.

.. table:: Retentioneering tools overview
    :widths: 20 80
    :class: tight-table

    +--------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
    | Tooling class                                          | Description                                                                                                 |
    +========================================================+=============================================================================================================+
    | :doc:`TransitionGraph</user_guides/transition_graph>`  | Plots an interactive transition graph according to Markov process underlying the eventstream.               |
    +--------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
    | :doc:`StepMatrix</user_guides/step_matrix>`            | Plots a step-wise matrix showing the distribution of the events over a given step coloured with a heatmap.  |
    +--------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
    | :doc:`StepSankey</user_guides/step_sankey>`            | Visualizes user paths in step-wise manner using sankey diagram.                                             |
    +--------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
    | :doc:`Clusters</user_guides/clusters>`                 | Provides a set of instruments for cluster analysis of the user paths.                                       |
    +--------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
    | :doc:`Funnel</user_guides/funnel>`                     | Plots conversion funnel consisted of given events.                                                          |
    +--------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
    | :doc:`Cohorts</user_guides/cohorts>`                   | Provides a set of the instruments for cohort analysis.                                                      |
    +--------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
    | :doc:`StatTests</user_guides/stattests>`               | Tests statistical hypothesis                                                                                |
    +--------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+
