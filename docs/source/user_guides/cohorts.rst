Cohorts
=======

The following user guide is also available as
`Google Colab notebook <https://colab.research.google.com/drive/11Eqicd5fNLdtr_IqdtyYp4oAFmybNg46?usp=share_link>`_

Cohorts intro
-------------

Cohorts it's a powerfull tool that shows differences and trends in users behavior over time.

It helps to isolate the impact of different marketing activities or changes in a product for different groups of users.

Basic algorithm of ``Cohort Matrix`` calculation:

- Users divided into ``Cohorts`` or ``CohortGroups`` depending on the time of their first appearance in the eventstream
- Then the retention rate of active users calculated in each further period (``CohortPeriod``) of observation.

To better understand how it works let’s first consider an intuitive
example. Here it is small dataset of event logs:

.. code-block:: python

    source_df = pd.DataFrame(
        [
            [1, "event", "2021-01-28 00:01:00"],
            [2, "event", "2021-01-30 00:01:00"],
            [1, "event", "2021-02-03 00:01:00"],
            [3, "event", "2021-02-04 00:01:00"],
            [4, "event", "2021-02-05 00:01:00"],
            [4, "event", "2021-03-06 00:01:00"],
            [1, "event", "2021-03-07 00:01:00"],
            [2, "event", "2021-03-07 00:01:00"],
            [3, "event", "2021-03-29 00:01:00"],
            [5, "event", "2021-03-30 00:01:00"],
            [4, "event", "2021-04-06 00:01:00"]
         ],
         columns=["user_id", "event", "timestamp"]
    )
    source_df

.. raw:: html


    <div><table class="dataframe">
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
          <td>1</td>
          <td>event</td>
          <td>2021-01-28 00:01:00</td>
        </tr>
        <tr>
          <th>1</th>
          <td>2</td>
          <td>event</td>
          <td>2021-01-30 00:01:00</td>
        </tr>
        <tr>
          <th>2</th>
          <td>1</td>
          <td>event</td>
          <td>2021-02-03 00:01:00</td>
        </tr>
        <tr>
          <th>3</th>
          <td>3</td>
          <td>event</td>
          <td>2021-02-04 00:01:00</td>
        </tr>
        <tr>
          <th>4</th>
          <td>4</td>
          <td>event</td>
          <td>2021-02-05 00:01:00</td>
        </tr>
        <tr>
          <th>5</th>
          <td>4</td>
          <td>event</td>
          <td>2021-03-06 00:01:00</td>
        </tr>
        <tr>
          <th>6</th>
          <td>1</td>
          <td>event</td>
          <td>2021-03-07 00:01:00</td>
        </tr>
        <tr>
          <th>7</th>
          <td>2</td>
          <td>event</td>
          <td>2021-03-07 00:01:00</td>
        </tr>
        <tr>
          <th>8</th>
          <td>3</td>
          <td>event</td>
          <td>2021-03-29 00:01:00</td>
        </tr>
        <tr>
          <th>9</th>
          <td>5</td>
          <td>event</td>
          <td>2021-03-30 00:01:00</td>
        </tr>
        <tr>
          <th>10</th>
          <td>4</td>
          <td>event</td>
          <td>2021-04-06 00:01:00</td>
        </tr>
      </tbody>
    </table>
    </div>


We can visualize this dataset as a heatmap indicating what fraction of
users from each cohort remained in the clickstream at each time period:

.. code-block:: python

    from retentioneering.eventstream import Eventstream
    from retentioneering.tooling.cohorts import Cohorts

    source = Eventstream(source_df)
    cohorts = Cohorts(
        eventstream=source,
        cohort_start_unit="M",
        cohort_period=(1,"M"),
        average=False
    )

    cohorts.fit()
    cohorts.heatmap(figsize=(6,5));

.. figure:: /_static/user_guides/cohorts/cohorts_1_simple_coh_matrix.png

-  ``CohortGroup`` - start datetime of each cohort.
-  ``CohortPeriod`` - the number of defined periods from each
   ``CohortGroup``.
-  ``Values`` - percentage of active users during a given period.

Each ``CohortGroup`` includes users whose acquisition date is within the period from
start date of current cohort to the start date of the following cohort
(i.e. the first time a user visits your website).
So each user has unique ``CohortGroup``.

Let’s take a look at the calculation in details:

For current ``Cohort Matrix``:

-  ``CohortGroup`` is a month
-  ``CohortPeriod`` is 1 month

There are 3 ``CohortGroups`` in total. Each ``CohortGroup`` represents
users acquired in a particular month (e.g. the January cohort
(``2021-01``) includes all users who had their first session in
January).

Thus, the value in the column referring to the ``CohortPeriod = 0``
will contain maximum users for each row (Fig.1), and in final heatmap it
will be always - 100% (Fig.2), users have just joined the eventstream,
and no one has left it yet.

.. figure:: /_static/user_guides/cohorts/cohorts_2_coh_matrix_calc_1.png

.. figure:: /_static/user_guides/cohorts/cohorts_3_coh_matrix_calc_2.png

Now let’s look at the ``CohortPeriod = 1`` . In our case, it’s 1 month
from the start of the observation period. During the next month of
monitoring users, we can see the activity of ``50%`` of users from the
first cohort, ``100%`` of users from the second cohort. The data on
which the table was built does not cover period 1 of the last cohort
(``2020-04``), so there is no data for this cell, it remains empty, like
all subsequent periods for this cohort.

And finally ``CohortPeriod = 2``. Users 1 and 2 are present in the data
for March, so ``100%`` of the users of the first cohort reached the
second period. For second cohort (``2021-02``) second period is April,
so only user 4 is presenting, it means, that only ``50%`` of users from
this cohort reached the second period.

Below we will explore how to use and customize ``Cohort`` tool using
``Retentioneering`` library.

Basic example
-------------

Loading data
~~~~~~~~~~~~

Here we use ``simple_shop`` dataset, which has already converted to ``Eventstream``.
If you want to know more about ``Eventstream`` and how to use it, please study
:doc:`this guide<eventstream>`

.. code-block:: python

    from retentioneering import datasets

    # load eventstream
    source = datasets.load_simple_shop()

Creating an instance of the Cohorts class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At the moment when an instance of a class is created, it is still
“naive”. In order to start calculation using passed parameters, you need
to use the :py:meth:`Cohorts.fit()<retentioneering.tooling.cohorts.cohorts.Cohorts.fit>` method.

.. code-block:: python

    from retentioneering.tooling.cohorts import Cohorts

    cohorts = Cohorts(
        eventstream=source,
        cohort_start_unit="M",
        cohort_period=(1,"M")
    )

    cohorts.fit()


Methods and attributes
~~~~~~~~~~~~~~~~~~~~~~

To visualize data as a heatmap, we can call
:py:meth:`Cohorts.heatmap()<retentioneering.tooling.cohorts.cohorts.Cohorts.heatmap>` method.

.. code-block:: python

    cohorts.heatmap(figsize=(6,5));

.. figure:: /_static/user_guides/cohorts/cohorts_4_basic.png

To get values of the heatmap, we can use
:py:meth:`Cohorts.values<retentioneering.tooling.cohorts.cohorts.Cohorts.values>` property, and then the
output will be a dataframe.

.. code-block:: python

    cohorts.values

.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th>CohortPeriod</th>
          <th>0</th>
          <th>1</th>
          <th>2</th>
          <th>3</th>
          <th>4</th>
        </tr>
        <tr>
          <th>CohortGroup</th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>2019-11</th>
          <td>1.0</td>
          <td>0.393822</td>
          <td>0.328185</td>
          <td>0.250965</td>
          <td>0.247104</td>
        </tr>
        <tr>
          <th>2019-12</th>
          <td>1.0</td>
          <td>0.333333</td>
          <td>0.257028</td>
          <td>0.232932</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-01</th>
          <td>1.0</td>
          <td>0.386179</td>
          <td>0.284553</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-02</th>
          <td>1.0</td>
          <td>0.319066</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-03</th>
          <td>1.0</td>
          <td>0.140000</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-04</th>
          <td>1.0</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>Average</th>
          <td>1.0</td>
          <td>0.314480</td>
          <td>0.289922</td>
          <td>0.241948</td>
          <td>0.247104</td>
        </tr>
      </tbody>
    </table>
    </div>


There are some NANs in the table. These gaps can mean one of two things:

1. During the specified period, users from the cohort did not perform
   any actions (and were active again in the next period).
2. Users from the latest-start cohorts have not yet reached the last
   periods of the observation. These NaNs are usually concentrated in
   the lower right corner of the table.

We can also build lineplots based on our data. Where by default each
line - is one ``CohortGroup``, ``show_plot='cohorts'``.

.. code-block:: python

    cohorts.lineplot(figsize=(5,5), show_plot='cohorts');

.. figure:: /_static/user_guides/cohorts/cohorts_5_lineplot_default.png

In addition, we can plot the average values for cohorts

.. code-block:: python

    cohorts.lineplot(figsize=(7,5), show_plot='average');

.. figure:: /_static/user_guides/cohorts/cohorts_6_lineplot_average.png

Specifying the ``show_plot='all'`` we will get a plot that shows
lineplot for each cohort and also for their average values

.. code-block:: python

    cohorts.lineplot(figsize=(7,5), show_plot='all');

.. figure:: /_static/user_guides/cohorts/cohorts_7_lineplot_all.png

Customization
-------------

Now let’s talk about setting cohort parameters in more detail.

Cohort_start_unit and Cohort_period
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the examples we looked at earlier, the parameters
``cohort_start_unit='M'`` and ``cohort_period=(1,'M')`` .

.. code-block:: python

    cohorts = Cohorts(
        eventstream=source,
        cohort_start_unit='M',
        cohort_period=(1, 'M')
    )
    cohorts.fit()
    cohorts.heatmap(figsize=(6,5));

.. figure:: /_static/user_guides/cohorts/cohorts_8_MM.png

Parameter ``cohort_start_unit`` is the way of rounding the moment from
which the cohort count begins. Minimum timestamp rounding down to the
selected datetime unit.


.. figure:: /_static/user_guides/cohorts/cohorts_9_num_expl.png

Parameter ``cohort_period`` is the window of time that you want to
examine. It is used in calculating:

1. Start datetime for each ``CohortGroup``. That means that we take the
   rounded with ``cohort_start_unit`` timestamp of the first click of
   the first user in the clickstream and count the ``cohort_period``
   from it. All users who performed actions during this period fall into
   the first cohort (zero period).
2. ``CohortPeriods`` for each cohort from it’s start moment. After
   actions described in paragraph 1, we again count the period of the
   cohort. New users who appeared in the clickstream during this period
   become the second cohort (zero period). And users from the first
   cohort who committed actions during this period are counted as the
   first period of the first cohort.

Let’s try to change those parameters.

.. code-block:: python

    cohorts = Cohorts(
        eventstream=source,
        cohort_start_unit='W',
        cohort_period=(3, 'W')
    )
    cohorts.fit()
    cohorts.heatmap(figsize=(8,7));

.. figure:: /_static/user_guides/cohorts/cohorts_10_weeks.png

Now the cohort period lasts 3 weeks, our heatmap has become more
detailed. The number of cohorts also increased from 5 to 8

Note! Parameters ``cohort_start_unit`` and ``cohort_period`` should be
consistent. Due to “Y” and “M” are non-fixed types it can be used only
with each other or if ``cohort_period_unit`` is more detailed than
``cohort_start_unit``.

For more details see
`numpy documentation <https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-and-timedelta-arithmetic>`_

Average
~~~~~~~

-  If ``True`` - calculating average for each cohort period. Default
   value.
-  If ``False`` - averages are not calculated

.. code-block:: python

    cohorts = Cohorts(
        eventstream=source,
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=False
    )
    cohorts.fit()
    cohorts.heatmap(figsize=(5,5));

.. figure:: /_static/user_guides/cohorts/cohorts_11_average.png

Cut matrix
~~~~~~~~~~

There are three ways to сut the matrix to get rid of boundary values,
for example, when there is not enough data available at the moment to
adequately analyze the behavior of the cohort.

-  ``cut_bottom`` - Drop from cohort_matrix ‘n’ rows from the bottom of
   the cohort matrix.
-  ``cut_right`` - Drop from cohort_matrix ‘n’ columns from the right
   side.
-  ``cut_diagonal`` - Drop from cohort_matrix diagonal with ‘n’ last
   period-group cells.

Average values are always recalculated.

.. code-block:: python

    cohorts = Cohorts(
        eventstream=source,
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=True,
        cut_bottom=1
    )
    cohorts.fit()
    cohorts.heatmap(figsize=(6,5));

.. figure:: /_static/user_guides/cohorts/cohorts_12_cut_bottom.png

After applying ``cut_bottom=1`` ``CohortGroup`` starts from ``2020-04``
were deleted from our matrix.

.. code-block:: python

    cohorts = Cohorts(
        eventstream=source,
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=True,
        cut_bottom=1,
        cut_right=1
    )

    cohorts.fit()
    cohorts.heatmap(figsize=(5,5));

.. figure:: /_static/user_guides/cohorts/cohorts_13_cut_right.png

Parameter ``cut_right`` allows to remove the last period column, which
reflected information only for the first cohort.

.. code-block:: python

    cohorts = Cohorts(
        eventstream=source,
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=True,
        cut_diagonal=1
    )
    cohorts.fit()
    cohorts.heatmap(figsize=(5,5));

.. figure:: /_static/user_guides/cohorts/cohorts_14_cut_diagonal.png

Parameter ``cut diagonal`` - deletes values below the diagonal that runs
to the left and down from the last period of the first cohort. Thus, we
get rid of all boundary values.

ShortCut for Cohorts (as an eventstream method)
===============================================

We can also use :doc:`Eventstream.cohorts</api/tooling/cohorts>` method which
creates an instance of ``Cohorts`` class and applies
:py:meth:`Cohorts.fit()<retentioneering.tooling.cohorts.cohorts.Cohorts.fit>` method as well.

In order to avoid unnessesary recalculations while you need different representations
of one matrix with the same parameters - that would be helpful to save that fitted
instance in separate variable.

Heatmap is displayed by default, but :py:meth:`Cohorts.values<retentioneering.tooling.cohorts.cohorts.Cohorts.values>`
and `:py:meth:`Cohorts.lineplot()<retentioneering.tooling.cohorts.cohorts.Cohorts.lineplot>` are also
available, now it can be done in one line:


.. code-block:: python

    source.cohorts(
        cohort_start_unit='M',
        cohort_period=(1,'M'),
        average=False,
        cut_bottom=0,
        cut_right=0,
        cut_diagonal=0
    );

.. figure:: /_static/user_guides/cohorts/cohorts_15_eventstream.png

.. code-block:: python

    source.cohorts(
        cohort_start_unit='M',
        cohort_period=(1,'M'),
        average=False,
        cut_bottom=0,
        cut_right=0,
        cut_diagonal=0,
        show_plot=False
    ).values

.. raw:: html


    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th>CohortPeriod</th>
          <th>0</th>
          <th>1</th>
          <th>2</th>
          <th>3</th>
          <th>4</th>
        </tr>
        <tr>
          <th>CohortGroup</th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>2019-11</th>
          <td>1.0</td>
          <td>0.393822</td>
          <td>0.328185</td>
          <td>0.250965</td>
          <td>0.247104</td>
        </tr>
        <tr>
          <th>2019-12</th>
          <td>1.0</td>
          <td>0.333333</td>
          <td>0.257028</td>
          <td>0.232932</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-01</th>
          <td>1.0</td>
          <td>0.386179</td>
          <td>0.284553</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-02</th>
          <td>1.0</td>
          <td>0.319066</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-03</th>
          <td>1.0</td>
          <td>0.140000</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-04</th>
          <td>1.0</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
      </tbody>
    </table>
    </div>


.. code-block:: python

    source.cohorts(
        cohort_start_unit='M',
        cohort_period=(1,'M'),
        average=False,
        cut_bottom=0,
        cut_right=0,
        cut_diagonal=0,
        show_plot=False
    ).lineplot();

.. figure:: /_static/user_guides/cohorts/cohorts_16_eventstream_lineplot.png


.. code-block:: python

    ch = source.cohorts(
        cohort_start_unit='M',
        cohort_period=(1,'M'),
        average=False,
        cut_bottom=0,
        cut_right=0,
        cut_diagonal=0,
        show_plot=False
    )
    ch.values

.. raw:: html


     <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th>CohortPeriod</th>
          <th>0</th>
          <th>1</th>
          <th>2</th>
          <th>3</th>
          <th>4</th>
        </tr>
        <tr>
          <th>CohortGroup</th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>2019-11</th>
          <td>1.0</td>
          <td>0.393822</td>
          <td>0.328185</td>
          <td>0.250965</td>
          <td>0.247104</td>
        </tr>
        <tr>
          <th>2019-12</th>
          <td>1.0</td>
          <td>0.333333</td>
          <td>0.257028</td>
          <td>0.232932</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-01</th>
          <td>1.0</td>
          <td>0.386179</td>
          <td>0.284553</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-02</th>
          <td>1.0</td>
          <td>0.319066</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-03</th>
          <td>1.0</td>
          <td>0.140000</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
        <tr>
          <th>2020-04</th>
          <td>1.0</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
          <td>NaN</td>
        </tr>
      </tbody>
    </table>
    </div>
