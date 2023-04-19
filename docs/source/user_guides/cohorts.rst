Cohorts
=======

The following user guide is also available as
`Google Colab notebook <https://colab.research.google.com/drive/11Eqicd5fNLdtr_IqdtyYp4oAFmybNg46?usp=share_link>`_.

Cohorts intro
-------------

Cohorts it is a common approach in product analysis that reveals differences and trends in users behavior evolving over time. It helps to indicate the impact of different marketing activities or changes in a product for different time-based groups of users. In particular, cohorts are used to estimate the retention rate of a product users.

The core element of the tool is a *cohort matrix*. Here is an outline of how it is calculated:

- The users are divided into ``CohortGroups`` depending on the time of their first appearance in the eventstream. The groups form the rows of the cohort matrix.
- The timeline is split into ``CohortPeriods``. They form the columns of the cohort matrix.
- A value of the cohort matrix is a retention rate of given ``CohortGroups`` at given ``CohortPeriod``. Retention rate is the proportion of the users who are still active at given period in comparison with the same users who were active at the first cohort period. We associate user activity with any user event appeared in the eventstream.

To better understand how the cohort tool works, let us consider the following eventstream.

.. code-block:: python

    simple_df = pd.DataFrame(
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
    simple_df

The primary way to build a transition graph is to call :py:meth:`Eventstream.cohorts()<retentioneering.eventstream.eventstream.Eventstream.cohorts>` method. If we split the users into monthly cohorts and check whether they are active on monthly basis, we obtain the following cohort matrix:

.. code-block:: python

    from retentioneering.eventstream import Eventstream

    simple_stream = Eventstream(simple_df)
    simple_stream.cohorts(
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=False
    )

.. figure:: /_static/user_guides/cohorts/cohorts_1_simple_coh_matrix.png

-  ``CohortGroup`` - starting datetime of each cohort;
-  ``CohortPeriod`` - the number of defined periods from each ``CohortGroup``.
-  ``Values`` - the percentage of active users during a given period.

Each ``CohortGroup`` includes users whose acquisition date is within the period from start date of current cohort to the start date of the following cohort (i.e. the first time a user visits your website). So each user belongs to a unique ``CohortGroup``.

Let us take a look at the calculation in details: For the cohort matrix above:

-  ``CohortGroup`` is a month,
-  ``CohortPeriod`` is 1 month.

There are 3 ``CohortGroups`` in total. Each ``CohortGroup`` represents users acquired in a particular month (e.g. the January cohort
(``2021-01``) includes all users who had their first session in January).

Thus, the values in the column ``CohortPeriod=0`` contain maximum users over each row (Fig. 1), and in the final heatmap it
are always - 100%, users have just joined the eventstream, and no one has left it yet.

.. figure:: /_static/user_guides/cohorts/cohorts_2_coh_matrix_calc_1.png

Now let us look at the ``CohortPeriod=1`` . In our case, it is 1 month from the start of the observation period. During the next month, we can see the activity of ``50%`` of users from the first cohort, and ``100%`` of users from the second cohort. The dataset does not cover period 1 of the last cohort (``2020-04``), so there is no data for this cell, and it remains empty, like all subsequent periods for this cohort.

Finally, ``CohortPeriod=2``. Users 1 and 2 are present in the data for March, so 100% of the users of the first cohort reached the second period. For the second cohort (``2021-02``) the second period is April, so only user 4 is presented, which means, that only 50% of the users from this cohort reached the second period.

Below we explore how to use and customize the Cohorts tool using retentioneering library. Hereafter we use :doc:`simple_shop </datasets/simple_shop>` dataset, which has already been converted to :doc:`Eventstream<eventstream>` and assigned to ``stream`` variable. If you want to use your own dataset, upload it following :ref:`this instruction<eventstream_creation>`.

.. code-block:: python

    from retentioneering import datasets

    stream = datasets.load_simple_shop()

Cohort start unit and cohort period
-----------------------------------

In the examples we looked at earlier, we used the parameters ``cohort_start_unit='M'`` and ``cohort_period=(1,'M')``.

.. code-block:: python

    stream.cohorts(
        cohort_start_unit='M',
        cohort_period=(1, 'M')
    )

.. figure:: /_static/user_guides/cohorts/cohorts_8_MM.png

The ``cohort_start_unit`` parameter is the way of rounding the moment from which the cohort count begins. Minimum timestamp rounding down to the selected datetime unit.

.. figure:: /_static/user_guides/cohorts/cohorts_9_num_expl.png

The ``cohort_period`` parameter defines time window that you want to examine. It is used for the following:

1. Start datetime for each ``CohortGroup``. It means that we take the rounded with ``cohort_start_unit`` timestamp of the first click of
   the first user in the eventstream and count the ``cohort_period`` from it. All users who performed actions during this period fall into
   the first cohort (zero period).

2. ``CohortPeriods`` for each cohort from its start moment. After the actions described in paragraph 1, we again count the period of the
   cohort. New users who appeared in the eventstream during this period become the second cohort (zero period). The users from the first
   cohort who committed actions during this period are counted as the first period of the first cohort.

Let us see what happens when we change the parameters.

.. code-block:: python

    stream.cohorts(
        cohort_start_unit='W',
        cohort_period=(3, 'W')
    )

.. figure:: /_static/user_guides/cohorts/cohorts_10_weeks.png

Now, the cohort period lasts 3 weeks, and our heatmap has become more detailed. The number of cohorts also increased from 5 to 8.

.. note::

    Parameters ``cohort_start_unit`` and ``cohort_period`` should be consistent. Due to “Y” and “M” are non-fixed types it can be used only with each other or if ``cohort_period_unit`` is more detailed than ``cohort_start_unit``.

For more details see
`numpy documentation <https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-and-timedelta-arithmetic>`_.

Average values
--------------

-  If ``True`` - calculating average for each cohort period. Default value.
-  If ``False`` - averages are not calculated.

.. code-block:: python

    stream.cohorts(
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=False
    )

.. figure:: /_static/user_guides/cohorts/cohorts_11_average.png

Cut matrix
----------

There are three ways to cut the matrix to get rid of boundary values, which can be useful when there is not enough data available at the moment to adequately analyze the behavior of the cohort.

-  ``cut_bottom`` - Drop from cohort_matrix ‘n’ rows from the bottom of
   the cohort matrix.
-  ``cut_right`` - Drop from cohort_matrix ‘n’ columns from the right
   side.
-  ``cut_diagonal`` - Drop from cohort_matrix diagonal with ‘n’ last
   period-group cells.

Average values are always recalculated.

.. code-block:: python

    stream.cohorts(
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=True,
        cut_bottom=1
    )

.. figure:: /_static/user_guides/cohorts/cohorts_12_cut_bottom.png

After applying ``cut_bottom=1``, ``CohortGroup`` starts from ``2020-04`` were deleted from our matrix.

.. code-block:: python

    stream.cohorts(
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=True,
        cut_bottom=1,
        cut_right=1
    )

.. figure:: /_static/user_guides/cohorts/cohorts_13_cut_right.png

Parameter ``cut_right`` allows us to remove the last period column, which reflects information only for the first cohort.

.. code-block:: python

    stream.cohorts(
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=True,
        cut_diagonal=1
    )

.. figure:: /_static/user_guides/cohorts/cohorts_14_cut_diagonal.png

Parameter ``cut diagonal`` deletes values below the diagonal that runs to the left and down from the last period of the first cohort. Thus, we get rid of all boundary values.

Using a separate instance
-------------------------

By design, :py:meth:`Eventstream.cohorts()<retentioneering.eventstream.eventstream.Eventstream.cohorts>` is a shortcut method that uses :py:meth:`Cohorts<retentioneering.tooling.cohorts.cohorts.Cohorts>` class under the hood. This method creates an instance of Cohorts class and embeds it into the eventstream object. Eventually, ``Eventstream.cohorts()`` returns exactly this instance.

Sometimes it is reasonable to work with a separate instance of Cohorts class. An alternative way to get the same visualization that ``Eventstream.cohorts()`` produces is to call :py:meth:`Cohorts.fit()<retentioneering.tooling.cohorts.cohorts.Cohorts.fit>` and :py:meth:`Cohorts.heatmap()<retentioneering.tooling.cohorts.cohorts.Cohorts.heatmap()>` methods explicitly. The former method calculates all the values needed for the visualization, the latter displays these values as a heatmap-colored matrix.

.. code-block:: python

    from retentioneering.tooling.cohorts import Cohorts

    cohorts = Cohorts(
        eventstream=stream,
        cohort_start_unit='M',
        cohort_period=(1, 'M'),
        average=False
    )

    cohorts.fit()
    cohorts.heatmap()

.. figure:: /_static/user_guides/cohorts/cohorts_15_eventstream.png

Lineplot
--------

We can also build lineplots based on our data. By default, each line is one ``CohortGroup``, ``show_plot='cohorts'``.

.. code-block:: python

    cohorts.lineplot(width=5, height=5), show_plot='cohorts')

.. figure:: /_static/user_guides/cohorts/cohorts_5_lineplot_default.png

In addition, we can plot the average values for cohorts:

.. code-block:: python

    cohorts.lineplot(width=7, height=5, show_plot='average')

.. figure:: /_static/user_guides/cohorts/cohorts_6_lineplot_average.png

Specifying the ``show_plot='all'`` we get a plot that shows lineplot for each cohort and their average values:

.. code-block:: python

    cohorts.lineplot(width=7, height=5, show_plot='all');

.. figure:: /_static/user_guides/cohorts/cohorts_7_lineplot_all.png


Common tooling properties
-------------------------

values
~~~~~~

:py:meth:`Cohorts.values<retentioneering.tooling.cohorts.cohorts.Cohorts.values>` property returns the values underlying recent ``Cohorts.heatmap()`` call. The property is common for many retentioneering tools. It allows you to avoid unnecessary calculations if the tool object has already been fitted.

.. code-block:: python

    cohorts = stream.cohorts(
        cohort_start_unit='M',
        cohort_period=(1,'M'),
        average=False,
        show_plot=False
    ).values
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
      </tbody>
    </table>
    </div>

There are some NANs in the table. These gaps can mean one of two things:

1. During the specified period, users from the cohort did not perform
   any actions (and were active again in the next period).
2. Users from the latest-start cohorts have not yet reached the last
   periods of the observation. These NaNs are usually concentrated in
   the lower right corner of the table.


params
~~~~~~

:py:meth:`Cohorts.params<retentioneering.tooling.cohorts.cohorts.Cohorts.params>` property returns the Cohorts parameters that was used in the last ``Cohorts.fit()`` call.

.. code-block:: python

    cohorts.params

.. parsed-literal::

        {"cohort_start_unit": 'M',
        "cohort_period": (1,'M'),
        "average": False,
        "cut_bottom": 0,
        "cut_right": 0,
        "cut_diagonal": 0}
