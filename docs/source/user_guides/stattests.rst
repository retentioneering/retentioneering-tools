Stattests
=========

The following user guide is also available as `Google Colab notebook <https://colab.research.google.com/drive/1u0s-aMMnYrufmSTvLFtA1JS7nYBwfqwx?usp=share_link>`_.

The Stattests class comprise simple utilities for two-group statistical hypothesis testing.

Loading data
------------

Throughout this guide we use our demonstration :doc:`simple_shop </datasets/simple_shop>` dataset. It has already been converted to :doc:`Eventstream<eventstream>` and assigned to ``stream`` variable. If you want to use your own dataset, upload it following :ref:`this instruction<eventstream_creation>`.

.. code-block:: python

    import numpy as np
    from retentioneering import datasets

    stream = datasets.load_simple_shop()

General usage
-------------

The primary way to use the Stattests is to call :py:meth:`Eventstream.stattests()<retentioneering.eventstream.eventstream.Eventstream.stattests>` method. Beforehand, we need to set the following arguments: ``groups`` and ``func``. The former defines two user groups to be compared, the latter -- a metric of interest to be compared in these two groups.

For our first example, we will split users 50/50 based on the index:

.. code-block:: python

    data = stream.to_dataframe()
    users = data['user_id'].unique()
    index_separator = int(users.shape[0]/2)
    user_groups = users[:index_separator], users[index_separator:]

    print(user_groups[0])
    print(user_groups[1])

.. parsed-literal::

    array([219483890, 964964743, 629881394, ..., 901422808, 523047643,
           724268790])
    array([315196393, 443659932, 865093748, ..., 965024600, 831491833,
           962761227])

Optionally, we cat define the names of the groups to be display in the method output with the ``group_names`` argument.

Let us say we are interested in the proportion of ``cart`` events in a user's path. So the ``func`` parameter will look like this:

.. code-block:: python

    def cart_share(df):
        return len(df[df['event'] == 'cart']) / len(df)

The interface of the ``func`` function expects its single argument to be a pandas.DataFrame that contains a single user path. The function output must be a either a scalar number or a string. For example, if we pick a ``some_user`` id and apply ``cart_share`` function to the corresponding trajectory, we get the metric value of a single user.

.. code-block:: python

    some_user = user_groups[0][378]
    cart_share(data[data['user_id'] == some_user])


.. parsed-literal::

    0.14285714285714285

Let us run the test that is defined by ``test`` argument. There is no need to specify a test hypothesis type - when applicable, the method computes the statistics for both one-sided hypothesis tests. ``Stattests`` outputs the statistic that could be significant, indicating which of the groups metric value could be *greater*:

.. code-block:: python

    stream.stattests(
        groups=user_groups,
        func=cart_share,
        group_names=['random_group_1', 'random_group_2'],
        test='ttest'
    )

.. parsed-literal::

    random_group_1 (mean ± SD): 0.067 ± 0.077, n = 1875
    random_group_2 (mean ± SD): 0.068 ± 0.081, n = 1876
    'random_group_1' is greater than 'random_group_2' with p-value: 0.34855
    power of the test: 6.40%

The method outputs the test p-value, along with group statistics and an estimate of test power (which is a heuristic designed for t-test). As expected, we see that the p-value is too high to register a statistical difference.

Test power
~~~~~~~~~~

Changing the ``alpha`` parameter will influence estimated power of the test. For example, if we lower if to 0.01 (from the default 0.05), we would expect the power to also drop:

.. code-block:: python

    stream.stattests(
        groups=user_groups,
        func=cart_share,
        group_names=['random_group_1', 'random_group_2'],
        test='ttest',
        alpha=0.01
    )

.. parsed-literal::

    random_group_1 (mean ± SD): 0.067 ± 0.077, n = 1875
    random_group_2 (mean ± SD): 0.068 ± 0.081, n = 1876
    'random_group_1' is greater than 'random_group_2' with p-value: 0.34855
    power of the test: 1.38%


Categorical variables
~~~~~~~~~~~~~~~~~~~~~

We might be interested in testing for difference in a categorical variable - for instance, in an indicator variable that indicates whether a user entered ``cart`` state zero, one, two or more than two times. In such cases, a contingency table independence test could be suitable.

Let us check if the distribution of the mentioned variable differs between the users who checked:

- ``product1`` exclusively
- ``product2`` exclusively:

.. code-block:: python

    user_group_1 = set(data[data['event'] == 'product1']['user_id'])
    user_group_2 = set(data[data['event'] == 'product2']['user_id'])

    user_group_1 -= user_group_1 & user_group_2
    user_group_2 -= user_group_1 & user_group_2

.. code-block:: python

    def cart_count(df):
        cart_count = len(df[df['event'] == 'cart'])
        if cart_count <= 2:
            return str(cart_count)
        return '>2'

    some_user = user_groups[0][378]
    cart_count(data[data['user_id'] == some_user])

.. parsed-literal::

    '2'

.. code-block:: python

    some_user = user_groups[0][379]
    cart_count(data[data['user_id'] == some_user])

.. parsed-literal::

    '0'

To test the statistical difference between the distribution of ``0``, ``1``, ``2``, and ``>2`` categories we apply ``chi2_contingency`` test.

.. code-block:: python

    stream.stattests(
        groups=(user_group_1, user_group_2),
        func=cart_count,
        group_names=('product_1_group', 'product_2_group'),
        test='chi2_contingency'
    )

.. parsed-literal::

    product_1_group (size): n = 580
    product_2_group (size): n = 1430
    Group difference test with p-value: 0.00000

In this case, the output contains only the ``group_names``, group sizes and the resulting test statistics. We can see that the variable of interest indeed differs between the exclusive users of two products.
