Stattests tooling user guide
----------------------------

`Google Colab <https://colab.research.google.com/drive/1u0s-aMMnYrufmSTvLFtA1JS7nYBwfqwx?usp=share_link>`_

This notebook is a user guide on eventstream *stattests* - a simple
utility for two-group user hypothesis testing.

We will be using the simple-onlineshop dataset as a sample clickstream:


Loading data
------------

.. code:: ipython3

    import numpy as np

    from retentioneering import datasets

.. code:: ipython3

    stream = datasets.load_simple_shop()
    stream.to_dataframe().head()




.. raw:: html


      <div id="df-535416c3-59fa-4e27-a6bd-a58b96a91408">
        <div class="colab-df-container">
          <div>
    <style scoped>
        .dataframe tbody tr th:only-of-type {
            vertical-align: middle;
        }

        .dataframe tbody tr th {
            vertical-align: top;
        }

        .dataframe thead th {
            text-align: right;
        }
    </style>
    <table border="1" class="dataframe">
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
    </div>
          <button class="colab-df-convert" onclick="convertToInteractive('df-535416c3-59fa-4e27-a6bd-a58b96a91408')"
                  title="Convert this dataframe to an interactive table."
                  style="display:none;">

      <svg xmlns="http://www.w3.org/2000/svg" height="24px"viewBox="0 0 24 24"
           width="24px">
        <path d="M0 0h24v24H0V0z" fill="none"/>
        <path d="M18.56 5.44l.94 2.06.94-2.06 2.06-.94-2.06-.94-.94-2.06-.94 2.06-2.06.94zm-11 1L8.5 8.5l.94-2.06 2.06-.94-2.06-.94L8.5 2.5l-.94 2.06-2.06.94zm10 10l.94 2.06.94-2.06 2.06-.94-2.06-.94-.94-2.06-.94 2.06-2.06.94z"/><path d="M17.41 7.96l-1.37-1.37c-.4-.4-.92-.59-1.43-.59-.52 0-1.04.2-1.43.59L10.3 9.45l-7.72 7.72c-.78.78-.78 2.05 0 2.83L4 21.41c.39.39.9.59 1.41.59.51 0 1.02-.2 1.41-.59l7.78-7.78 2.81-2.81c.8-.78.8-2.07 0-2.86zM5.41 20L4 18.59l7.72-7.72 1.47 1.35L5.41 20z"/>
      </svg>
          </button>

      <style>
        .colab-df-container {
          display:flex;
          flex-wrap:wrap;
          gap: 12px;
        }

        .colab-df-convert {
          background-color: #E8F0FE;
          border: none;
          border-radius: 50%;
          cursor: pointer;
          display: none;
          fill: #1967D2;
          height: 32px;
          padding: 0 0 0 0;
          width: 32px;
        }

        .colab-df-convert:hover {
          background-color: #E2EBFA;
          box-shadow: 0px 1px 2px rgba(60, 64, 67, 0.3), 0px 1px 3px 1px rgba(60, 64, 67, 0.15);
          fill: #174EA6;
        }

        [theme=dark] .colab-df-convert {
          background-color: #3B4455;
          fill: #D2E3FC;
        }

        [theme=dark] .colab-df-convert:hover {
          background-color: #434B5C;
          box-shadow: 0px 1px 3px 1px rgba(0, 0, 0, 0.15);
          filter: drop-shadow(0px 1px 2px rgba(0, 0, 0, 0.3));
          fill: #FFFFFF;
        }
      </style>

          <script>
            const buttonEl =
              document.querySelector('#df-535416c3-59fa-4e27-a6bd-a58b96a91408 button.colab-df-convert');
            buttonEl.style.display =
              google.colab.kernel.accessAllowed ? 'block' : 'none';

            async function convertToInteractive(key) {
              const element = document.querySelector('#df-535416c3-59fa-4e27-a6bd-a58b96a91408');
              const dataTable =
                await google.colab.kernel.invokeFunction('convertToInteractive',
                                                         [key], {});
              if (!dataTable) return;

              const docLinkHtml = 'Like what you see? Visit the ' +
                '<a target="_blank" href=https://colab.research.google.com/notebooks/data_table.ipynb>data table notebook</a>'
                + ' to learn more about interactive tables.';
              element.innerHTML = '';
              dataTable['output_type'] = 'display_data';
              await google.colab.output.renderOutput(dataTable, element);
              const docLink = document.createElement('div');
              docLink.innerHTML = docLinkHtml;
              element.appendChild(docLink);
            }
          </script>
        </div>
      </div>




General stattests usage
-----------------------

To use the stattests method, we specify ``groups`` parameter in the
method. This parameter will contain two lists of user ids, each defining
a group of users selected for comparison. For our first example, we will
split users 50/50 based on index:

.. code:: ipython3

    data = stream.to_dataframe()
    users = data['user_id'].unique()
    user_groups = users[:int(users.shape[0]/2)], users[int(users.shape[0]/2):]

.. code:: ipython3

    user_groups[0]




.. parsed-literal::

    array([219483890, 964964743, 629881394, ..., 901422808, 523047643,
           724268790])



.. code:: ipython3

    user_groups[1]




.. parsed-literal::

    array([315196393, 443659932, 865093748, ..., 965024600, 831491833,
           962761227])



.. code:: ipython3

    group_names = ('random_group_1', 'random_group_2')

We also need to define a user path function - this needs to be the
function of interest, i.e. that the difference of which we are trying to
detect between the user groups. Let us say we are interested in the rate
of “cart” events relative to all other events of a user:

.. code:: ipython3

    def cart_share(df):
        return df[df['event'] == 'cart'].shape[0] / df.shape[0]

.. code:: ipython3

    some_user = user_groups[0][378]
    cart_share(data[data['user_id'] == some_user])




.. parsed-literal::

    0.14285714285714285



Let us run the test. There is no need to specify a test hypothesis type
- where applicable, the method computes the statistics for both
one-sided hypothesis tests. stattests outputs the statistic that could
be significant, indicating which of the groups could be “greater”:

.. code:: ipython3

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




.. parsed-literal::

    <retentioneering.tooling.stattests.stattests.StatTests at 0x7f391cbcbc40>



The method outputs the test P-value, along with group statistics and an
estimate of test power(which is a heuristic designed for t-test). As
expected, we see that the P-value is too high to register a statistical
difference.

Changing the “alpha” parameter will influence estimated power of the
test. For example, if we lower if to 0.01(from the default 0.05), we
would expect the power to also drop:

.. code:: ipython3

    stream.stattests(groups=user_groups, func=cart_share, group_names=group_names, test='ttest', alpha=0.01)


.. parsed-literal::

    random_group_1 (mean ± SD): 0.067 ± 0.077, n = 1875
    random_group_2 (mean ± SD): 0.068 ± 0.081, n = 1876
    'random_group_1' is greater than 'random_group_2' with P-value: 0.34855
    power of the test: 1.38%




.. parsed-literal::

    <retentioneering.tooling.stattests.stattests.StatTests at 0x7f391caa5b50>



We might be interested in testing for difference in a categorical
variable - for instance, in an indicator variable that indicates whether
a user entered “cart” state zero, one, two or more than two times. In
such cases, a contingency table independence test could be suitable.

Let us check if the distribution of the mentioned variable differs
between users who checked product 1 exclusively and useers who checked
product 2 exclusively:

.. code:: ipython3

    user_group_1 = data[data['event']=='product1']['user_id'].unique()
    user_group_2 = data[data['event']=='product2']['user_id'].unique()

    user_group_1 = user_group_1[~np.isin(user_group_1, user_group_2)]
    user_group_2 = user_group_2[~np.isin(user_group_2, user_group_1)]

.. code:: ipython3

    def cart_count(df):
        cart_count = df[df['event']=='cart'].shape[0]
        if cart_count < 3:
            return str(cart_count)
        return '>=3'

    some_user = user_groups[0][378]
    cart_count(data[data['user_id']==some_user])




.. parsed-literal::

    '2'



.. code:: ipython3

    some_user = user_groups[0][379]
    cart_count(data[data['user_id']==some_user])




.. parsed-literal::

    '0'



.. code:: ipython3

    stream.stattests(
        groups=(user_group_1, user_group_2),
        func=cart_count,
        group_names=('product_1_group', 'product_2_group'),
        test='chi2_contingency'
    )

In this case, the output contains only the group names, group sizes and
the resulting test statistics. We can see that the variable of interest
indeed differs between the exclusive users of two products.
