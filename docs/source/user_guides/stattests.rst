Stattests tooling user guide
============================

The following user guide is also available as
`Google Colab <https://colab.research.google.com/drive/1u0s-aMMnYrufmSTvLFtA1JS7nYBwfqwx?usp=share_link>`_.

``Stattests`` is a simple utility for two-group user hypothesis testing.

Here we use ``simple_shop`` dataset, which has already converted to ``Eventstream``.
If you want to know more about ``Eventstream`` and how to use it, please study
:doc:`this guide<eventstream>`.


Loading data
------------

.. code-block:: python

    import numpy as np

    from retentioneering import datasets

.. code-block:: python

    stream = datasets.load_simple_shop()
    stream.to_dataframe().head()

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
          <td>297683c8-a576-4138-92e5-f47923bc566e</td>
          <td>raw</td>
          <td>0</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:13.273932</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>1</th>
          <td>fca8899d-5da3-4f99-80fd-27e5751aef73</td>
          <td>raw</td>
          <td>1</td>
          <td>product1</td>
          <td>2019-11-01 17:59:28.459271</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>2</th>
          <td>1e416f4e-a7c4-4e51-a9b6-bbe53d1cc4fb</td>
          <td>raw</td>
          <td>2</td>
          <td>cart</td>
          <td>2019-11-01 17:59:29.502214</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>3</th>
          <td>ef8ee4e2-c9de-4642-83b8-0c48b9a99341</td>
          <td>raw</td>
          <td>3</td>
          <td>catalog</td>
          <td>2019-11-01 17:59:32.557029</td>
          <td>219483890</td>
        </tr>
        <tr>
          <th>4</th>
          <td>b9c8591e-932c-4c61-a261-b05d8ad1753d</td>
          <td>raw</td>
          <td>4</td>
          <td>catalog</td>
          <td>2019-11-01 21:38:19.283663</td>
          <td>964964743</td>
        </tr>
      </tbody>
    </table>

General Stattests usage
-----------------------

To use the stattests method, we specify ``groups`` parameter in the
method. This parameter will contain two lists of user ids, each defining
a group of users selected for comparison. For our first example, we will
split users 50/50 based on the index:

.. code-block:: python

    data = stream.to_dataframe()
    users = data['user_id'].unique()
    user_groups = users[:int(users.shape[0]/2)], users[int(users.shape[0]/2):]

.. code-block:: python

    user_groups[0]

.. parsed-literal::

    array([219483890, 964964743, 629881394, ..., 901422808, 523047643,
           724268790])

.. code-block:: python

    user_groups[1]

.. parsed-literal::

    array([315196393, 443659932, 865093748, ..., 965024600, 831491833,
           962761227])

.. code-block:: python

    group_names = ('random_group_1', 'random_group_2')

We also need to define a ``func`` parameter - this needs to be the
function of interest, i.e. that the difference of which we are trying to
detect between the user groups.

Let us say we are interested in number of ``cart`` events to all other user events ratio:

.. code-block:: python

    def cart_share(df):
        return df[df['event'] == 'cart'].shape[0] / df.shape[0]

.. code-block:: python

    some_user = user_groups[0][378]
    cart_share(data[data['user_id'] == some_user])


.. parsed-literal::

    0.14285714285714285

Let us run the test. There is no need to specify a test hypothesis type
- where applicable, the method computes the statistics for both
one-sided hypothesis tests. ``Stattests`` outputs the statistic that could
be significant, indicating which of the groups could be *greater*:

.. code-block:: python

    stream.stattests(
        groups=user_groups,
        func=cart_share,
        group_names=group_names,
        test='ttest'
    )


.. parsed-literal::

    random_group_1 (mean ± SD): 0.067 ± 0.077, n = 1875
    random_group_2 (mean ± SD): 0.068 ± 0.081, n = 1876
    'random_group_1' is greater than 'random_group_2' with P-value: 0.34855
    power of the test: 6.40%

The method outputs the test P-value, along with group statistics and an
estimate of test power (which is a heuristic designed for t-test). As
expected, we see that the P-value is too high to register a statistical
difference.

Test power
~~~~~~~~~~

Changing the ``alpha`` parameter will influence estimated power of the
test. For example, if we lower if to 0.01(from the default 0.05), we
would expect the power to also drop:

.. code-block:: python

    stream.stattests(groups=user_groups, func=cart_share, group_names=group_names, test='ttest', alpha=0.01)

.. parsed-literal::

    random_group_1 (mean ± SD): 0.067 ± 0.077, n = 1875
    random_group_2 (mean ± SD): 0.068 ± 0.081, n = 1876
    'random_group_1' is greater than 'random_group_2' with P-value: 0.34855
    power of the test: 1.38%


Categorical variables
~~~~~~~~~~~~~~~~~~~~~

We might be interested in testing for difference in a categorical
variable - for instance, in an indicator variable that indicates whether
a user entered ``cart`` state zero, one, two or more than two times. In
such cases, a contingency table independence test could be suitable.

Let us check if the distribution of the mentioned variable differs
between users who checked:

- ``product1`` exclusively
- ``product2`` exclusively:

.. code-block:: python

    user_group_1 = data[data['event'] == 'product1']['user_id'].unique()
    user_group_2 = data[data['event'] == 'product2']['user_id'].unique()

    user_group_1 = user_group_1[~np.isin(user_group_1, user_group_2)]
    user_group_2 = user_group_2[~np.isin(user_group_2, user_group_1)]

.. code-block:: python

    def cart_count(df):
        cart_count = df[df['event'] == 'cart'].shape[0]
        if cart_count < 3:
            return str(cart_count)
        return '>=3'

    some_user = user_groups[0][378]
    cart_count(data[data['user_id'] == some_user])

.. parsed-literal::

    '2'

.. code-block:: python

    some_user = user_groups[0][379]
    cart_count(data[data['user_id'] == some_user])

.. parsed-literal::

    '0'

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
    Group difference test with P-value: 0.00000

In this case, the output contains only the ``group_names``, group sizes and
the resulting test statistics. We can see that the variable of interest
indeed differs between the exclusive users of two products.

To learn more about tests calculation see
