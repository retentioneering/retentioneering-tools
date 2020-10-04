Users flow and step matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~

Step matrix intro
=================

Step matrix is a very powerful tool in retentioneering arsenal. It allows to get quickly
high-level understing of user behaviour. Step matrix has powerful customization options
to tailor the output depending the goal of the analysis.

To better understand how `step_matrix` works let's first consider intuitive example. Let's say we
are analyzing web-store logs and have dataset with event logs from four user sessions with the following
events in following order:

.. image:: _static/step_matrix/step_matrix_demo.svg

We can visualize this dataset as a heatmap indicating what fraction of users were at wich step in
their trajectories:

.. image:: _static/step_matrix/step_matrix_demo_plot.svg

This is the simplest step matrix. It has individual unique events as a rows, columns corresponds
to the positional number of event in user log and the value in the matrix shows what percentage
users have given event at a given step. Note, that total value in each column is always 1 (all
users must be at specific state at each step or have ENDED their trajectory).

Below we will explore how to plot and customize step matrix.

Basic example
=============

This notebook can be found :download:`here <_static/examples/step_matrix_tutorial.ipynb>`
or open directly in `google colab <https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/docs/source/_static/examples/step_matrix_tutorial.ipynb>`__.

To run examples below we need to import retentioneering, import sample dataset and update config
to set names for the columns:

.. code:: ipython3

    import retentioneering

    # load sample user behavior data as a pandas dataframe:
    data = retentioneering.datasets.load_simple_shop()

    # update config to pass columns names:
    retentioneering.config.update({
        'user_col': 'user_id',
        'event_col':'event',
        'event_time_col':'timestamp',
    })



To understand intuitively what is step_matrix let us begin with plotting step_matrix
for extremely simple dataset containg events for only one user:

.. code:: ipython3

    single_user = data[data['user_id']==613604495].reset_index(drop=True)
    single_user

.. raw:: html

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
          <th>user_id</th>
          <th>event</th>
          <th>timestamp</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>613604495</td>
          <td>main</td>
          <td>2019-11-02 23:25:03.672939</td>
        </tr>
        <tr>
          <th>1</th>
          <td>613604495</td>
          <td>catalog</td>
          <td>2019-11-02 23:25:07.390498</td>
        </tr>
        <tr>
          <th>2</th>
          <td>613604495</td>
          <td>catalog</td>
          <td>2019-11-02 23:25:48.043605</td>
        </tr>
        <tr>
          <th>3</th>
          <td>613604495</td>
          <td>product2</td>
          <td>2019-11-02 23:26:08.845033</td>
        </tr>
        <tr>
          <th>4</th>
          <td>613604495</td>
          <td>cart</td>
          <td>2019-11-02 23:26:37.007346</td>
        </tr>
        <tr>
          <th>5</th>
          <td>613604495</td>
          <td>catalog</td>
          <td>2019-11-02 23:26:38.406224</td>
        </tr>
        <tr>
          <th>6</th>
          <td>613604495</td>
          <td>cart</td>
          <td>2019-11-02 23:27:09.279245</td>
        </tr>
        <tr>
          <th>7</th>
          <td>613604495</td>
          <td>catalog</td>
          <td>2019-11-02 23:27:11.432713</td>
        </tr>
        <tr>
          <th>8</th>
          <td>613604495</td>
          <td>product2</td>
          <td>2019-11-02 23:27:43.193619</td>
        </tr>
        <tr>
          <th>9</th>
          <td>613604495</td>
          <td>cart</td>
          <td>2019-11-02 23:27:48.110186</td>
        </tr>
        <tr>
          <th>10</th>
          <td>613604495</td>
          <td>delivery_choice</td>
          <td>2019-11-02 23:27:48.292051</td>
        </tr>
        <tr>
          <th>11</th>
          <td>613604495</td>
          <td>delivery_pickup</td>
          <td>2019-11-02 23:27:59.789239</td>
        </tr>
      </tbody>
    </table>
    </div>


|

Let's plot a simple intuitive step_matrix for our single user dataset:

.. code:: ipython3

    single_user.rete.step_matrix(max_steps=16);

.. image:: _static/step_matrix/step_matrix_su_0.svg


We can see, since we have only one user in this example, `step_matrix` contains only 0's and 1's.
At step 1 user had event `main` (100% of users have event main as first event in the trajecotry),
then at step 2 user proceed to `catalog`, etc., etc., etc. By the step 13 user's trajectory
ended and there are no more events, therefore all subsequent events starting from step 13 are
special events `ENDED` indicating no other events present.

Let's now plot `step_matrix` for the full dataset containing all users:

.. code:: ipython3

    data.rete.step_matrix(max_steps=16);

.. image:: _static/step_matrix/step_matrix_0.svg

By looking at the first column we can immediately say that users in the analyzed cohort start
their sessions from events `catalog` (72%) and `main` (28%). At step 2 12% of users already
ended their sessions and have no other events (row `ENDED` at step 2 is 0.12). We can see, that
52% of users finish their sessions with 6 or less events (row `ENDED` at step 7 is 0.52). Some
conversions start happen after step 7 (row `payment_done` have 0.02 at step 7). And so on. Note,
that at each step all values in every column always sum up to 1 (meaning that all users have some
specific event or `ENDED` state). Below we will explore other options for `step_matrix` function
to make the output much more informative and tailored for the goals of particular analysis.


Thresholding
============

When we plot `step_matrix` using full dataset sometimes we want first focus on bigger picture and
avoid rows with event where insignificant fraction of users was present. Such thresholding can be
done using `thresh` parameter (float, default: 0). If the row has all values less than specified
`thresh`, such row will not be shown.

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh=0.05);

.. image:: _static/step_matrix/step_matrix_1.svg

All events cutted away by thresholding are grouped together in `THRESHOLDED_X` row, where X - is
the total number of dropped events.

Targets analysis
================

Very often there are specific events of particular importance for product analyst (for example
such as `cart`, or `order_confirmed`, or `subscribe`, etc.). Often such events have much lower
occurrence rate comparing other events (like `main page` or `catalog`) and often ended up
thresholded from `step_matrix` or shown with non-informative coloring. In this case we can
isolate those events of particular importance (`targets`) to individual rows, each of which
will have their individual color scale. This can be done with parameter `targets`:

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh=0.05,
                          targets=['payment_done']);

.. image:: _static/step_matrix/step_matrix_2.svg

Specified target events are always shown in the bottom of step matrix regardless
of selected threshold. Multiple targets can be included as a list:

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh=0.05,
                          targets=['product1','cart','payment_done']);

.. image:: _static/step_matrix/step_matrix_3.svg

If we want to compare some targets and plot them using same color scaling, we can combine
them in sub-list inside the `targets` list:

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh=0.05,
                          targets=['product1',['cart','payment_done']]);

.. image:: _static/step_matrix/step_matrix_4.svg

Now we can visually compare by color how mamy users reach `cart` vs `payment_done` at particular
step in their trajectory.

Targets can be presented as accumulated values (or both):

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh=0.05,
                          targets=['product1',['cart','payment_done']],
                          accumulated='only');

.. image:: _static/step_matrix/step_matrix_5.svg

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh=0.05,
                          targets=['product1',['cart','payment_done']],
                          accumulated='both');

.. image:: _static/step_matrix/step_matrix_6.svg

Centered step matrix
====================

Sometimes we are interested in flow of users through specific event: how do users reach
specific event and what do they do after? This information can be visualized with step_marix
using parameter centered:

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh = 0.2,
                          centered={'event':'cart',
                                    'left_gap':5,
                                    'occurrence':1});

.. image:: _static/step_matrix/step_matrix_7.svg

Note, that when plot step_matrix with parameter centered we only keep users who have reached
specified event (the column 0 has value 1 at specified event). Parameter centered is a dictionary
wich requires three keys:
    * 'event' - name of the event we are interested. This event will be taken as 0. Negative step numbers will corresponds to events before selected event and positive step numbers will correspond to steps after selected event.
    * 'left_gap' - integer number which indicates how much steps before centered event we want to show on step matrix
    * 'occurrence' - which occurrence number of target event we are interested in. For example, if in the example above, all trajectories will be aligned to have first 'cart' occurrence as step 0.

Importantly, when centered step matrix is used, only users who have selected event in
their trajectories present (or it's n`th occurrence) will be shown. Therefore, the column
with step index 0 will always have 1 at selected event and zero at all other events. Fraction
of users kept for centered step matrix shown in the title. In the example above, 51.3% of users
have reach event 'cart' at least once.

We can use all targets functionality with centered step_matrix, for example:

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh = 0.2,
                          centered={'event':'cart',
                                    'left_gap':5,
                                    'occurrence':1},
                          targets=['payment_done']);

.. image:: _static/step_matrix/step_matrix_8.svg

Custom events sorting
=====================

Sometimes it is needed to obtain step_matrix with events listed in the specific order
(for example, to compare two step_matrixes). This can be done with parameter sorting which accepts
list of event names in the required order to show up in the step matrix. For convenience, to obtain
list of event names from the most recent step_matrix output you can always refer to
retentioneering.config['step_matrix']['sorting'] after each step_matrix run.
Let's consider an example:

.. code:: ipython3

    data.rete.step_matrix(max_steps=16,
                          thresh=0.07);

.. image:: _static/step_matrix/step_matrix_sorting_0.svg

Let's say we would like to change the order of the events in the resulted step_matrix. First, we
can obtain list of event names from the last step_matrix output using retentioneering.config:

.. code:: ipython3

    print(retentioneering.config['step_matrix']['sorting'])

output:

.. parsed-literal::

    ['catalog', 'main', 'lost', 'cart', 'product2', 'product1', 'ENDED', 'THRESHOLDED_7']

Now we can conveniently copy the list of events, reorganize it in the required order and pass
to step_matrix function as sorting parameter:

.. code:: ipython3

    custom_order = ['main',
                    'catalog',
                    'product1',
                    'product2',
                    'cart',
                    'lost',
                    'ENDED',
                    'THRESHOLDED_7']

    data.rete.step_matrix(max_steps=16,
                          thresh=0.07,
                          sorting=custom_order);

.. image:: _static/step_matrix/step_matrix_sorting_1.svg


Note, that ordering only affects non-target events. Target events will always be in
the same order as they are specified in the parameter targets.


Differential step matrix
========================

Sometimes we need to compare behavior of several groups of users. For example, when we would like
to compare behavior of users who had conversion to target vs. who had not, or compare behavior of
test and control groups in A/B test, or compare behavior between specific segments of users.

In this case it is informative to plot a step_matrix as difference between step_matrix for
group_A and step_matrix for group_B. This can be done using parameter groups, which requires a
tuple of two elements (g1 and g2): where g_1 and g_2 are collections
of user_id`s (list, tuple or set). Two separate step_matrixes M1 and M2
will be calculated for users from g_1 and g_2, respectively. Resulting
matrix will be the matrix M = M1-M2. Note, that values in each column
in differential step matrix will always sum up to 0 (since columns in both M1
and M2 always sum up to 1).

.. code:: ipython3

    g1 = set(data[data['event']=='payment_done']['user_id'])
    g2 = set(data['user_id']) - g1

    data.rete.step_matrix(max_steps=16,
                          thresh = 0.05,
                          centered={'event':'cart',
                                    'left_gap':5,
                                    'occurrence':1},
                          groups=(g1, g2));



.. image:: _static/step_matrix/step_matrix_9.svg

Let's consider another example of differential step matrix use, where we will compare behavior
of two user clusters. First, let's obtain behavioural segments and visualize the results of
segmentation using conversion to 'payment_done' and event 'cart' (to learn more about
user behavior clustering read
`here <https://retentioneering.github.io/retentioneering-tools/_build/html/clustering.html>`__):

.. code:: ipython3

    data.rete.get_clusters(plot_type='cluster_bar',
                           targets=['payment_done', 'cart'],
                           refit_cluster=True);

.. image:: _static/step_matrix/cluster_bar_0.svg

We can see 8 clusters with the corresponding conversion rates to specified events (% of users in
the given cluster who had at least one specified event). Let's say we would like to compare
behavior segments 0 and 3. Both have relatively high conversion rate and cart visit rate. Let's
find out how they are differ using differential step_matrix. All we need is to get user_id's
collections from cluster_mapping attribute and pass it to groups parameter of step_matrix:

.. code:: ipython3

    g1 = data.rete.cluster_mapping[0]
    g2 = data.rete.cluster_mapping[3]

    data.rete.step_matrix(max_steps=16,
                          thresh = 0.05,
                          centered={'event':'cart',
                                    'left_gap':5,
                                    'occurrence':1},
                          groups=(g1, g2));

.. image:: _static/step_matrix/step_matrix_10.svg

We can clearly see that these two behavioural segments are quite similar to each other with
the only strong difference at the second step after 'cart' event: users of segment 3 prefer to
select 'delivery_courier' (large positive value), and users of segment 0 prefer to select
'delivery_pickup' (large negative value).