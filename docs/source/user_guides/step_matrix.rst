Step Matrix
==========

The following user guide is also available as `Google Colab <https://colab.research.google.com/drive/12l603hupPLIWp9H1ljkr5RUQLuunbLY3?usp=share_link>`_

Step matrix intro
-----------------

Step matrix is a very powerful tool in the retentioneering arsenal. It
allows getting a quick high-level understanding of user behavior. Step
matrix has powerful customization options to tailor the output depending
on the goal of the analysis.

To better understand how step_matrix works let’s first consider an
intuitive example. Let’s say we are analyzing web-store logs and have a
dataset with event logs from four user sessions with the following
events in the following order:

.. image:: /_static/user_guides/step_matrix/step_matrix_demo.svg

We can visualize this dataset as a heatmap indicating what fraction of
users were at which step in their trajectories:

.. code:: ipython3

    from retentioneering.eventstream import Eventstream
    import pandas as pd
    from retentioneering import datasets

.. code:: ipython3

    simple_example = pd.DataFrame({
        'user_id': ['user1', 'user2', 'user3', 'user4',
                    'user1', 'user3', 'user4',
                    'user1', 'user3', 'user4',
                    'user1', 'user3', 'user4',
                    'user1', 'user3',
                    'user3'
                   ],
        'event': ['main', 'main', 'main', 'catalog',
                  'catalog', 'catalog', 'product',
                  'product', 'catalog', 'main',
                  'cart', 'product', 'catalog',
                  'catalog', 'cart',
                  'order'
                 ],
        'timestamp': [0, 0, 0, 0,
                      1, 1, 1,
                      2, 2, 2,
                      3, 3, 3,
                      5, 5,
                      6
                     ]
    })

    Eventstream(simple_example).step_matrix(max_steps=7)






.. image:: /_static/user_guides/step_matrix/output_7_1.png

This is the simplest step matrix. It has individual unique events as
rows, and columns correspond to the positional number of events in the
user log, and the value in the matrix shows what percentage of users
have given event at a given step. We can also see a special synthetic
event called “ENDED”, which shows the percentage of users who have ended
their path to the given step.

Below we will explore how to plot and customize the step matrix.

Basic example
-------------

In order to start, we need to: - import the required libraries - load
sample dataset - create ``StepMatrix`` object

.. code:: ipython3

    from retentioneering.eventstream import Eventstream
    import pandas as pd
    from retentioneering import datasets

.. code:: ipython3

    source = datasets.load_simple_shop()

Creating an instance of the StepMatrix class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At the moment when an instance of a class is created, it is still
“naive”. To pass it the parameters specified in brackets, you need to
use the “.fit()” method.

.. code:: ipython3

    from retentioneering.tooling.step_matrix import StepMatrix
    step_matrix = StepMatrix(
        eventstream=source,
        max_steps=12
        )
    step_matrix.fit()

Methods and attributes
~~~~~~~~~~~~~~~~~~~~~~

To visualize data as a heatmap, we can call ``.plot()`` method.

.. code:: ipython3

    step_matrix.plot();


.. image:: /_static/user_guides/step_matrix/output_18_0.png



To see the matrix data, we can call the ``.values`` attribute. This
attribute returns two datasets: the step matrix itself and the target
events separately. At the moment we are not using the target parameter,
so the attribute call looks like this: .values[0].

.. code:: ipython3

    step_matrix.values[0]




.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>1</th>
          <th>2</th>
          <th>3</th>
          <th>4</th>
          <th>5</th>
          <th>6</th>
          <th>7</th>
          <th>8</th>
          <th>9</th>
          <th>10</th>
          <th>11</th>
          <th>12</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>catalog</th>
          <td>0.716076</td>
          <td>0.445215</td>
          <td>0.384164</td>
          <td>0.310051</td>
          <td>0.251400</td>
          <td>0.211677</td>
          <td>0.169022</td>
          <td>0.147427</td>
          <td>0.134897</td>
          <td>0.117835</td>
          <td>0.101840</td>
          <td>0.094908</td>
        </tr>
        <tr>
          <th>main</th>
          <td>0.283924</td>
          <td>0.162357</td>
          <td>0.121834</td>
          <td>0.094108</td>
          <td>0.085311</td>
          <td>0.079712</td>
          <td>0.070914</td>
          <td>0.064250</td>
          <td>0.053586</td>
          <td>0.050120</td>
          <td>0.049853</td>
          <td>0.037057</td>
        </tr>
        <tr>
          <th>lost</th>
          <td>0.000000</td>
          <td>0.118102</td>
          <td>0.101306</td>
          <td>0.093842</td>
          <td>0.075180</td>
          <td>0.066649</td>
          <td>0.060784</td>
          <td>0.054385</td>
          <td>0.040523</td>
          <td>0.035724</td>
          <td>0.023460</td>
          <td>0.022661</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>0.000000</td>
          <td>0.089843</td>
          <td>0.109571</td>
          <td>0.080778</td>
          <td>0.064783</td>
          <td>0.047454</td>
          <td>0.046388</td>
          <td>0.031725</td>
          <td>0.027459</td>
          <td>0.024527</td>
          <td>0.021061</td>
          <td>0.022394</td>
        </tr>
        <tr>
          <th>payment_choice</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.033591</td>
          <td>0.043455</td>
          <td>0.031991</td>
          <td>0.023994</td>
          <td>0.022661</td>
          <td>0.017329</td>
          <td>0.010131</td>
          <td>0.011464</td>
        </tr>
        <tr>
          <th>delivery_choice</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.054119</td>
          <td>0.061584</td>
          <td>0.049054</td>
          <td>0.034391</td>
          <td>0.031725</td>
          <td>0.026926</td>
          <td>0.018395</td>
          <td>0.018395</td>
          <td>0.014396</td>
          <td>0.012263</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.003999</td>
          <td>0.024793</td>
          <td>0.024793</td>
          <td>0.018395</td>
          <td>0.014929</td>
          <td>0.013063</td>
          <td>0.010131</td>
        </tr>
        <tr>
          <th>product2</th>
          <td>0.000000</td>
          <td>0.114370</td>
          <td>0.065849</td>
          <td>0.057851</td>
          <td>0.045854</td>
          <td>0.035724</td>
          <td>0.030392</td>
          <td>0.023727</td>
          <td>0.020794</td>
          <td>0.020261</td>
          <td>0.017595</td>
          <td>0.016262</td>
        </tr>
        <tr>
          <th>product1</th>
          <td>0.000000</td>
          <td>0.070115</td>
          <td>0.045055</td>
          <td>0.042655</td>
          <td>0.031991</td>
          <td>0.025860</td>
          <td>0.020794</td>
          <td>0.017595</td>
          <td>0.017062</td>
          <td>0.011197</td>
          <td>0.012263</td>
          <td>0.010397</td>
        </tr>
        <tr>
          <th>payment_card</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.017595</td>
          <td>0.020261</td>
          <td>0.017062</td>
          <td>0.012797</td>
          <td>0.010664</td>
          <td>0.010131</td>
          <td>0.005065</td>
        </tr>
        <tr>
          <th>delivery_courier</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.025327</td>
          <td>0.032791</td>
          <td>0.024793</td>
          <td>0.015729</td>
          <td>0.017595</td>
          <td>0.011997</td>
          <td>0.007465</td>
          <td>0.007731</td>
          <td>0.006398</td>
        </tr>
        <tr>
          <th>delivery_pickup</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.014396</td>
          <td>0.016796</td>
          <td>0.015463</td>
          <td>0.012530</td>
          <td>0.009597</td>
          <td>0.010131</td>
          <td>0.005332</td>
          <td>0.007198</td>
          <td>0.003999</td>
        </tr>
        <tr>
          <th>payment_cash</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.004799</td>
          <td>0.006931</td>
          <td>0.004799</td>
          <td>0.004266</td>
          <td>0.004532</td>
          <td>0.002133</td>
          <td>0.001866</td>
        </tr>
        <tr>
          <th>ENDED</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.118102</td>
          <td>0.219408</td>
          <td>0.313250</td>
          <td>0.388430</td>
          <td>0.457745</td>
          <td>0.536124</td>
          <td>0.607038</td>
          <td>0.661690</td>
          <td>0.709144</td>
          <td>0.745135</td>
        </tr>
      </tbody>
    </table>
    </div>



Single user dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

So, after getting instructed with the basic syntax of the step matrix
tool, let’s try it on a small dataset.

To intuitively understand what step_matrix is, let us begin with
plotting step_matrix for an extremely simple dataset containing only one
user’s events. To do this without going back to the dataframe format,
let’s use the ``.filter()``. It is an eventstream method, it takes two
arguments as input: a callable function that defines the filtering
criteria, and a data scheme, in this case, it is the default data scheme
for the eventstream.

.. code:: ipython3

    single_user = source.filter(lambda df, schema: df[schema.user_id] == 613604495);

.. code:: ipython3

    single_user.to_dataframe()




.. raw:: html

    <div><table class="dataframe">
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
          <th>158</th>
          <td>19cb553f-be93-4e45-a764-cf837a296c9f</td>
          <td>raw</td>
          <td>158</td>
          <td>main</td>
          <td>2019-11-02 23:25:03.672939</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>159</th>
          <td>1cda37ea-8389-4194-9c99-b2440140c1ea</td>
          <td>raw</td>
          <td>159</td>
          <td>catalog</td>
          <td>2019-11-02 23:25:07.390498</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>160</th>
          <td>d6678868-94ef-46c9-9b1a-2ece2716abe4</td>
          <td>raw</td>
          <td>160</td>
          <td>catalog</td>
          <td>2019-11-02 23:25:48.043605</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>161</th>
          <td>49cac82a-658f-43b2-9616-e97333c7dda5</td>
          <td>raw</td>
          <td>161</td>
          <td>product2</td>
          <td>2019-11-02 23:26:08.845033</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>162</th>
          <td>0f788fb3-675a-4903-af09-ec14c43d859d</td>
          <td>raw</td>
          <td>162</td>
          <td>cart</td>
          <td>2019-11-02 23:26:37.007346</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>163</th>
          <td>fa7280aa-67ee-43c4-8030-bdeb3458e7d7</td>
          <td>raw</td>
          <td>163</td>
          <td>catalog</td>
          <td>2019-11-02 23:26:38.406224</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>164</th>
          <td>7fde4606-e1ce-45fa-9360-1e80e96a270a</td>
          <td>raw</td>
          <td>164</td>
          <td>cart</td>
          <td>2019-11-02 23:27:09.279245</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>165</th>
          <td>34d5c1c0-7a19-48bf-a86a-7f40e663b686</td>
          <td>raw</td>
          <td>165</td>
          <td>catalog</td>
          <td>2019-11-02 23:27:11.432713</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>166</th>
          <td>886bb82a-6234-41c1-a0f2-0f7757f04dd0</td>
          <td>raw</td>
          <td>166</td>
          <td>product2</td>
          <td>2019-11-02 23:27:43.193619</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>167</th>
          <td>059630e6-44d1-484a-a3ae-474b74205f8b</td>
          <td>raw</td>
          <td>167</td>
          <td>cart</td>
          <td>2019-11-02 23:27:48.110186</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>168</th>
          <td>a74c1d6b-d0f6-438b-adce-1dcb35c39443</td>
          <td>raw</td>
          <td>168</td>
          <td>delivery_choice</td>
          <td>2019-11-02 23:27:48.292051</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>169</th>
          <td>b5577147-48e4-412e-aac2-9808631c8a75</td>
          <td>raw</td>
          <td>169</td>
          <td>delivery_pickup</td>
          <td>2019-11-02 23:27:59.789239</td>
          <td>613604495</td>
        </tr>
        <tr>
          <th>170</th>
          <td>de93092b-f586-40dc-ae96-081740bf3673</td>
          <td>raw</td>
          <td>170</td>
          <td>lost</td>
          <td>2019-11-02 23:28:00.789239</td>
          <td>613604495</td>
        </tr>
      </tbody>
    </table>
    </div>



To learn more about ``.filter()`` method and how to work with data
processors, you can follow the link:

#@TODO Добавить ссылĸу на туториал с датапроцессорами. j.ostanina

Let’s plot a simple intuitive step_matrix for our single user dataset:

.. code:: ipython3

    sm_single_user = StepMatrix(
        eventstream=single_user,
        max_steps=12
        )
    sm_single_user.fit()
    sm_single_user.plot();



.. figure:: docs/source/_static/user_guides/step_matrix/output_28_0.png


As we can see, since we have only one user in this example, step_matrix
contains only 0’s and 1’s. At step 1 user had event “main” (100% of
users have event main as the first event in the trajectory), then at
step 2 user proceeded to catalog, etc. By step 13 user’s trajectory has
ended, and there were no more events, so the rest of the table is filled
with zeros.

Full dataset
~~~~~~~~~~~~~~

Let’s now plot step_matrix for the full dataset containing all users:

.. code:: ipython3

    sm = StepMatrix(eventstream=source, max_steps=16)
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_31_0.png



Now it is clearly visible that in each cell we have the number of users
divided by the total number of users. Looking at the first column we can
immediately say that users in the analyzed cohort start their sessions
from events catalog (72%) and main (28%). Some conversions start
happening after step 7 (row payment_done have 0.02 at step 7). And so
on.

Path end
~~~~~~~~~~

We can add some synthetic events, for example, path_start and path_end.
For the step matrix, the path_end event is very important, step matrix
recognizes it and processes it accordingly. To understand how, let’s
create an eventstream with start and end events.

.. code:: ipython3

    source_start_end = source.add_start_end();

.. code:: ipython3

    sm = StepMatrix(eventstream=source_start_end, max_steps=16)
    sm.fit()
    sm.plot();



.. image:: /_static/user_guides/step_matrix/output_36_0.png



Note that the “path_end” event is always placed at the end of the step
matrix. This line calculates the cumulative share of users who left the
clickstream at each step.

Thresholding
------------

When we plot step_matrix using the full dataset sometimes we want first
to focus on the bigger picture and avoid rows with events where an
insignificant fraction of users were present. Such thresholding can be
done using thresh parameter (float, default: 0). If the row has all
values less than the specified thresh, such row will not be shown.

.. code:: ipython3

    sm = StepMatrix(eventstream=source, max_steps=16, thresh=0.05)
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_40_0.png



All events cutted away by thresholding are grouped together in
THRESHOLDED_X row, where X - is the total number of dropped events.

Targets analysis
----------------

Very often there are specific events of particular importance for
product analyst (for example such as cart, or order_confirmed, or
subscribe, etc.). Often such events have much lower occurrence rate
comparing other events (like main page or catalog) and often ended up
thresholded from step_matrix or shown with non-informative coloring. In
this case we can isolate those events of particular importance (targets)
to individual rows, each of which will have their individual color
scale. This can be done with parameter targets:

.. code:: ipython3

    sm = StepMatrix(eventstream=source, max_steps=16,
                          thresh=0.05,
                          targets=['payment_done'])
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_44_0.png



Specified target events are always shown in the bottom of step matrix
regardless of selected threshold. Multiple targets can be included as a
list:

.. code:: ipython3

    sm = StepMatrix(eventstream=source,
                    max_steps=16,
                    thresh=0.05,
                    targets=['product1','cart','payment_done'])
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_46_0.png


If we want to compare some targets and plot them using same color
scaling, we can combine them in sub-list inside the targets list:

.. code:: ipython3

    sm = StepMatrix(eventstream=source,
                    max_steps=16,
                    thresh=0.05,
                    targets=['product1',['cart','payment_done']])
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_48_0.png



Now we can visually compare by color how many users reach cart vs
payment_done at particular step in their trajectory.

Targets can be presented as accumulated values (or both):

.. code:: ipython3

    sm = StepMatrix(eventstream=source,
                    max_steps=16,
                    thresh=0.05,
                    targets=['product1',['cart','payment_done']],
                    accumulated='only')
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_50_0.png


.. code:: ipython3

    sm = StepMatrix(eventstream=source,
                    max_steps=16,
                    thresh=0.05,
                    targets=['product1',['cart','payment_done']],
                    accumulated='both')
    sm.fit()
    sm.plot();

.. image:: /_static/user_guides/step_matrix/output_51_0.png



To get the target events in DataFrame format, we can use the ``.values``
attribute. If we apply indexing, ``.values[0]`` returns step_matrix,
.\ ``values[1]`` returns targets

.. code:: ipython3

    sm = StepMatrix(eventstream=source,
                    max_steps=12,
                    thresh=0.05,
                    targets=['product1',['cart','payment_done']],
                    accumulated='both')
    sm.fit()
    sm.values[0]


.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>1</th>
          <th>2</th>
          <th>3</th>
          <th>4</th>
          <th>5</th>
          <th>6</th>
          <th>7</th>
          <th>8</th>
          <th>9</th>
          <th>10</th>
          <th>11</th>
          <th>12</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>catalog</th>
          <td>0.716076</td>
          <td>0.445215</td>
          <td>0.384164</td>
          <td>0.310051</td>
          <td>0.251400</td>
          <td>0.211677</td>
          <td>0.169022</td>
          <td>0.147427</td>
          <td>0.134897</td>
          <td>0.117835</td>
          <td>0.101840</td>
          <td>0.094908</td>
        </tr>
        <tr>
          <th>main</th>
          <td>0.283924</td>
          <td>0.162357</td>
          <td>0.121834</td>
          <td>0.094108</td>
          <td>0.085311</td>
          <td>0.079712</td>
          <td>0.070914</td>
          <td>0.064250</td>
          <td>0.053586</td>
          <td>0.050120</td>
          <td>0.049853</td>
          <td>0.037057</td>
        </tr>
        <tr>
          <th>lost</th>
          <td>0.000000</td>
          <td>0.118102</td>
          <td>0.101306</td>
          <td>0.093842</td>
          <td>0.075180</td>
          <td>0.066649</td>
          <td>0.060784</td>
          <td>0.054385</td>
          <td>0.040523</td>
          <td>0.035724</td>
          <td>0.023460</td>
          <td>0.022661</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>0.000000</td>
          <td>0.089843</td>
          <td>0.109571</td>
          <td>0.080778</td>
          <td>0.064783</td>
          <td>0.047454</td>
          <td>0.046388</td>
          <td>0.031725</td>
          <td>0.027459</td>
          <td>0.024527</td>
          <td>0.021061</td>
          <td>0.022394</td>
        </tr>
        <tr>
          <th>delivery_choice</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.054119</td>
          <td>0.061584</td>
          <td>0.049054</td>
          <td>0.034391</td>
          <td>0.031725</td>
          <td>0.026926</td>
          <td>0.018395</td>
          <td>0.018395</td>
          <td>0.014396</td>
          <td>0.012263</td>
        </tr>
        <tr>
          <th>product2</th>
          <td>0.000000</td>
          <td>0.114370</td>
          <td>0.065849</td>
          <td>0.057851</td>
          <td>0.045854</td>
          <td>0.035724</td>
          <td>0.030392</td>
          <td>0.023727</td>
          <td>0.020794</td>
          <td>0.020261</td>
          <td>0.017595</td>
          <td>0.016262</td>
        </tr>
        <tr>
          <th>product1</th>
          <td>0.000000</td>
          <td>0.070115</td>
          <td>0.045055</td>
          <td>0.042655</td>
          <td>0.031991</td>
          <td>0.025860</td>
          <td>0.020794</td>
          <td>0.017595</td>
          <td>0.017062</td>
          <td>0.011197</td>
          <td>0.012263</td>
          <td>0.010397</td>
        </tr>
        <tr>
          <th>ENDED</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.118102</td>
          <td>0.219408</td>
          <td>0.313250</td>
          <td>0.388430</td>
          <td>0.457745</td>
          <td>0.536124</td>
          <td>0.607038</td>
          <td>0.661690</td>
          <td>0.709144</td>
          <td>0.745135</td>
        </tr>
        <tr>
          <th>THRESHOLDED_6</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.039723</td>
          <td>0.083178</td>
          <td>0.110104</td>
          <td>0.112237</td>
          <td>0.097841</td>
          <td>0.080245</td>
          <td>0.060251</td>
          <td>0.050387</td>
          <td>0.038923</td>
        </tr>
      </tbody>
    </table>
    </div>



.. code:: ipython3

    sm = StepMatrix(eventstream=source,
                    max_steps=12,
                    thresh=0.05,
                    targets=['product1',['cart','payment_done']],
                    accumulated='both')
    sm.fit()
    sm.values[1]




.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>1</th>
          <th>2</th>
          <th>3</th>
          <th>4</th>
          <th>5</th>
          <th>6</th>
          <th>7</th>
          <th>8</th>
          <th>9</th>
          <th>10</th>
          <th>11</th>
          <th>12</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>product1</th>
          <td>0.0</td>
          <td>0.070115</td>
          <td>0.045055</td>
          <td>0.042655</td>
          <td>0.031991</td>
          <td>0.025860</td>
          <td>0.020794</td>
          <td>0.017595</td>
          <td>0.017062</td>
          <td>0.011197</td>
          <td>0.012263</td>
          <td>0.010397</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>0.0</td>
          <td>0.089843</td>
          <td>0.109571</td>
          <td>0.080778</td>
          <td>0.064783</td>
          <td>0.047454</td>
          <td>0.046388</td>
          <td>0.031725</td>
          <td>0.027459</td>
          <td>0.024527</td>
          <td>0.021061</td>
          <td>0.022394</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>0.0</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.003999</td>
          <td>0.024793</td>
          <td>0.024793</td>
          <td>0.018395</td>
          <td>0.014929</td>
          <td>0.013063</td>
          <td>0.010131</td>
        </tr>
        <tr>
          <th>ACC_product1</th>
          <td>0.0</td>
          <td>0.070115</td>
          <td>0.115169</td>
          <td>0.157825</td>
          <td>0.189816</td>
          <td>0.215676</td>
          <td>0.236470</td>
          <td>0.254066</td>
          <td>0.271128</td>
          <td>0.282325</td>
          <td>0.294588</td>
          <td>0.304985</td>
        </tr>
        <tr>
          <th>ACC_cart</th>
          <td>0.0</td>
          <td>0.089843</td>
          <td>0.199413</td>
          <td>0.280192</td>
          <td>0.344975</td>
          <td>0.392429</td>
          <td>0.438816</td>
          <td>0.470541</td>
          <td>0.498001</td>
          <td>0.522527</td>
          <td>0.543588</td>
          <td>0.565982</td>
        </tr>
        <tr>
          <th>ACC_payment_done</th>
          <td>0.0</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.003999</td>
          <td>0.028792</td>
          <td>0.053586</td>
          <td>0.071981</td>
          <td>0.086910</td>
          <td>0.099973</td>
          <td>0.110104</td>
        </tr>
      </tbody>
    </table>
    </div>



Centered step matrix
--------------------

Sometimes we are interested in flow of users through specific event: how
do users reach specific event and what do they do after? This
information can be visualized with step_marix using parameter
``centered``:

.. code:: ipython3

    sm = StepMatrix(eventstream=source,
                    max_steps=16,
                    thresh = 0.2,
                    centered={'event':'cart',
                              'left_gap':5,
                              'occurrence':1})
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_57_0.png



Note, that when plotting step_matrix with parameter centered we only
keep users who have reached the specified event (column 0 has value 1 at
the specified event). Parameter centered is a dictionary which requires
three keys:

-  ‘event’ - the name of the event we are interested in. This event will
   be taken as 0. Negative step numbers will correspond to events before
   the selected event and positive step numbers will correspond to steps
   after the selected event

-  ‘left_gap’ - integer number which indicates how many steps before the
   centered event we want to show on the step matrix

-  ‘occurrence’ - which occurrence number of target event we are
   interested in. For example, in the illustration above, all
   trajectories will be aligned to have the first ‘cart’ occurrence as
   step 0

Importantly, when a centered step matrix is used, only users who have
selected events in their trajectories present (or it’s n`th occurrence)
will be shown. Therefore, the column with step index 0 will always have
1 at the selected event and zero at all other events. The fraction of
users kept for the centered step matrix is shown in the title. In the
example above, 51.3% of users have reached the event ‘cart’ at least
once.

We can use all targets functionality with centered step_matrix, for
example:

.. code:: ipython3

    sm = StepMatrix(eventstream=source,
                    max_steps=16,
                    thresh = 0.2,
                    centered={'event':'cart',
                              'left_gap':5,
                              'occurrence':1},
                    targets=['payment_done'])
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_59_0.png


Custom events sorting
---------------------

Sometimes it is needed to obtain step_matrix with events listed in the
specific order (for example, to compare two step_matrixes). This can be
done with parameter sorting which accepts a list of event names in the
required order to show up in the step matrix. Let’s consider an example:

.. code:: ipython3

    sm = StepMatrix(eventstream=source,
                    max_steps=16,
                    thresh=0.07)
    sm.fit()
    sm.plot();

.. image:: /_static/user_guides/step_matrix/output_62_0.png




Let’s say we would like to change the order of the events in the
resulted step_matrix. First, we can obtain a list of event names from
the step_matrix output using ``.values[0]``:

.. code:: ipython3

    sm.values[0].index




.. parsed-literal::

    Index(['catalog', 'main', 'lost', 'cart', 'product2', 'product1', 'ENDED',
           'THRESHOLDED_7'],
          dtype='object')



Now we can conveniently copy the list of events, reorganize it in the
required order and pass it to the step_matrix function as a sorting
parameter:

.. code:: ipython3

    custom_order = ['main',
                    'catalog',
                    'product1',
                    'product2',
                    'cart',
                    'lost',
                    'ENDED',
                    'THRESHOLDED_7']
    sm = StepMatrix(eventstream=source,
                    max_steps=16,
                    thresh=0.07,
                    sorting=custom_order)
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_66_0.png


Note, that ordering only affects non-target events. Target events will
always be in the same order as they are specified in the parameter
targets.

Differential step_matrix
------------------------

Sometimes we need to compare the behavior of several groups of users.
For example, when we would like to compare the behavior of users who had
a conversion to target vs those who had not, compare the behavior of
test and control groups in the A/B test, or compare behavior between
specific segments of users.

In this case, it is informative to plot a step_matrix as the difference
between step_matrix for group_A and step_matrix for group_B. This can be
done using parameter groups, which require a tuple of two elements (g1
and g2): where g_1 and g_2 are collections of user_id`s (list, tuple, or
set). Two separate step_matrices M1 and M2 will be calculated for users
from g_1 and g_2, respectively. The resulting matrix will be the matrix
M = M1-M2. Note, that values in each column in the differential step
matrix will always sum up to 0 (since columns in both M1 and M2 always
sum up to 1).

.. code:: ipython3

    raw_data = source.to_dataframe()
    g1 = set(raw_data[raw_data['event']=='payment_done']['user_id'])
    g2 = set(raw_data['user_id']) - g1

    sm = StepMatrix(eventstream=source,
                    max_steps=16,
                    thresh = 0.05,
                    centered={'event':'cart',
                              'left_gap':5,
                              'occurrence':1},
                    groups=(g1, g2))
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_70_0.png


Clusters
--------

Let’s consider another example of differential step matrix use, where we
will compare behavior of two user clusters. First, let’s obtain
behavioural segments and visualize the results of segmentation using
conversion to ‘payment_done’ and event ‘cart’.

To learn more about user behavior clustering read here:
#@TODO:ссылка на юзергайд с кластерами

.. code:: ipython3

    from retentioneering.tooling.clusters import Clusters

    clusters = Clusters(eventstream=source)
    clusters.fit(method='kmeans', n_clusters=8, feature_type='count', ngram_range=(1, 1))
    clusters.plot(targets=['payment_done', 'cart']);


.. image:: /_static/user_guides/step_matrix/output_74_0.png



We can see 8 clusters with the corresponding conversion rates to
specified events (% of users in the given cluster who had at least one
specified event). Let’s say we would like to compare behavior between
segments 0 and 3. Both have relatively high conversion rate and cart
visit rate. Let’s find out how they are differ using differential
step_matrix. All we need is to get user_id’s collections from
cluster_mapping attribute and pass it to groups parameter of
step_matrix:

.. code:: ipython3

    g1 = clusters.cluster_mapping[0]
    g2 = clusters.cluster_mapping[3]

    sm = StepMatrix(eventstream=source,
                    max_steps=16,
                    thresh = 0.05,
                    centered={'event':'cart',
                                    'left_gap':5,
                                    'occurrence':1},
                    groups=(g1, g2));
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_76_0.png



Weight_col
----------

All this time, we have been calculating matrices by the percentage of
users remaining in the clickstream by a certain step. But for 100% we
can take not only users. For example, we can take sessions.

To do this, we need to divide the event stream into sessions. The split
sessions method will help us with this. But first, first we need to
decide on the duration of the session.

To find the average session duration, you can use a histogram that shows
the distribution of the delta between 2 consecutive events, it’s an
eventstream method ``.timedelta_hist()``. The cutoff should be chosen
from the segment between the “bells”, as shown in the figure.

.. code:: ipython3

    source.timedelta_hist(timedelta_unit='m', bins=100, log_scale=True)


.. image:: /_static/user_guides/step_matrix/output_81_0.png



Looks like the distance between “bells” is about 100 minutes, this is
the approximate length of the session.

Then we set the parameters for dividing into sessions: the length of the
session will be 100 minutes. The resulting object will be a new
eventstream.

.. code:: ipython3

    result = source.split_sessions((100.0,'m'), session_col='session_id')


# @TODO cсылка на .timedelta_hist()

To learn more about working with data processors, you can follow the
link:


# @TODO Добавить ссылĸу на туториал с датапроцессорами. j.ostanina

Now we feed the result as input to the step_matrix tool and specify the
``weight_col=['session_id']`` parameter.

.. code:: ipython3

    sm = StepMatrix(eventstream=result,
                    max_steps=16,
                    weight_col=['session_id'])
    sm.fit()
    sm.plot();


.. image:: /_static/user_guides/step_matrix/output_89_0.png


Now we see in the cells the share of all sessions for which the
specified event happened at the specified step. For example, for 54
percents of sessions, the third step was a catalog.

ShortCut for StepMatrix (as an eventstream method)
----------------------------------------------------

We can also use StepMatrix as an eventstream method. By default, the
``.plot()`` method is called. ``values`` attribute is also avaliable,
but it can be done in one line:

.. code:: ipython3

    source.step_matrix(max_steps=16);


.. image:: /_static/user_guides/step_matrix/output_93_0.png


.. code:: ipython3

    source.step_matrix(max_steps=12, show_plot=False).values[0]




.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>1</th>
          <th>2</th>
          <th>3</th>
          <th>4</th>
          <th>5</th>
          <th>6</th>
          <th>7</th>
          <th>8</th>
          <th>9</th>
          <th>10</th>
          <th>11</th>
          <th>12</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>catalog</th>
          <td>0.716076</td>
          <td>0.445215</td>
          <td>0.384164</td>
          <td>0.310051</td>
          <td>0.251400</td>
          <td>0.211677</td>
          <td>0.169022</td>
          <td>0.147427</td>
          <td>0.134897</td>
          <td>0.117835</td>
          <td>0.101840</td>
          <td>0.094908</td>
        </tr>
        <tr>
          <th>main</th>
          <td>0.283924</td>
          <td>0.162357</td>
          <td>0.121834</td>
          <td>0.094108</td>
          <td>0.085311</td>
          <td>0.079712</td>
          <td>0.070914</td>
          <td>0.064250</td>
          <td>0.053586</td>
          <td>0.050120</td>
          <td>0.049853</td>
          <td>0.037057</td>
        </tr>
        <tr>
          <th>lost</th>
          <td>0.000000</td>
          <td>0.118102</td>
          <td>0.101306</td>
          <td>0.093842</td>
          <td>0.075180</td>
          <td>0.066649</td>
          <td>0.060784</td>
          <td>0.054385</td>
          <td>0.040523</td>
          <td>0.035724</td>
          <td>0.023460</td>
          <td>0.022661</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>0.000000</td>
          <td>0.089843</td>
          <td>0.109571</td>
          <td>0.080778</td>
          <td>0.064783</td>
          <td>0.047454</td>
          <td>0.046388</td>
          <td>0.031725</td>
          <td>0.027459</td>
          <td>0.024527</td>
          <td>0.021061</td>
          <td>0.022394</td>
        </tr>
        <tr>
          <th>payment_choice</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.033591</td>
          <td>0.043455</td>
          <td>0.031991</td>
          <td>0.023994</td>
          <td>0.022661</td>
          <td>0.017329</td>
          <td>0.010131</td>
          <td>0.011464</td>
        </tr>
        <tr>
          <th>delivery_choice</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.054119</td>
          <td>0.061584</td>
          <td>0.049054</td>
          <td>0.034391</td>
          <td>0.031725</td>
          <td>0.026926</td>
          <td>0.018395</td>
          <td>0.018395</td>
          <td>0.014396</td>
          <td>0.012263</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.003999</td>
          <td>0.024793</td>
          <td>0.024793</td>
          <td>0.018395</td>
          <td>0.014929</td>
          <td>0.013063</td>
          <td>0.010131</td>
        </tr>
        <tr>
          <th>product2</th>
          <td>0.000000</td>
          <td>0.114370</td>
          <td>0.065849</td>
          <td>0.057851</td>
          <td>0.045854</td>
          <td>0.035724</td>
          <td>0.030392</td>
          <td>0.023727</td>
          <td>0.020794</td>
          <td>0.020261</td>
          <td>0.017595</td>
          <td>0.016262</td>
        </tr>
        <tr>
          <th>product1</th>
          <td>0.000000</td>
          <td>0.070115</td>
          <td>0.045055</td>
          <td>0.042655</td>
          <td>0.031991</td>
          <td>0.025860</td>
          <td>0.020794</td>
          <td>0.017595</td>
          <td>0.017062</td>
          <td>0.011197</td>
          <td>0.012263</td>
          <td>0.010397</td>
        </tr>
        <tr>
          <th>payment_card</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.017595</td>
          <td>0.020261</td>
          <td>0.017062</td>
          <td>0.012797</td>
          <td>0.010664</td>
          <td>0.010131</td>
          <td>0.005065</td>
        </tr>
        <tr>
          <th>delivery_courier</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.025327</td>
          <td>0.032791</td>
          <td>0.024793</td>
          <td>0.015729</td>
          <td>0.017595</td>
          <td>0.011997</td>
          <td>0.007465</td>
          <td>0.007731</td>
          <td>0.006398</td>
        </tr>
        <tr>
          <th>delivery_pickup</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.014396</td>
          <td>0.016796</td>
          <td>0.015463</td>
          <td>0.012530</td>
          <td>0.009597</td>
          <td>0.010131</td>
          <td>0.005332</td>
          <td>0.007198</td>
          <td>0.003999</td>
        </tr>
        <tr>
          <th>payment_cash</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.004799</td>
          <td>0.006931</td>
          <td>0.004799</td>
          <td>0.004266</td>
          <td>0.004532</td>
          <td>0.002133</td>
          <td>0.001866</td>
        </tr>
        <tr>
          <th>ENDED</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.118102</td>
          <td>0.219408</td>
          <td>0.313250</td>
          <td>0.388430</td>
          <td>0.457745</td>
          <td>0.536124</td>
          <td>0.607038</td>
          <td>0.661690</td>
          <td>0.709144</td>
          <td>0.745135</td>
        </tr>
      </tbody>
    </table>
    </div>



.. code:: ipython3

    sm = source.step_matrix(max_steps=12, show_plot=False)
    sm.values[0]




.. raw:: html

    <div><table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>1</th>
          <th>2</th>
          <th>3</th>
          <th>4</th>
          <th>5</th>
          <th>6</th>
          <th>7</th>
          <th>8</th>
          <th>9</th>
          <th>10</th>
          <th>11</th>
          <th>12</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>catalog</th>
          <td>0.716076</td>
          <td>0.445215</td>
          <td>0.384164</td>
          <td>0.310051</td>
          <td>0.251400</td>
          <td>0.211677</td>
          <td>0.169022</td>
          <td>0.147427</td>
          <td>0.134897</td>
          <td>0.117835</td>
          <td>0.101840</td>
          <td>0.094908</td>
        </tr>
        <tr>
          <th>main</th>
          <td>0.283924</td>
          <td>0.162357</td>
          <td>0.121834</td>
          <td>0.094108</td>
          <td>0.085311</td>
          <td>0.079712</td>
          <td>0.070914</td>
          <td>0.064250</td>
          <td>0.053586</td>
          <td>0.050120</td>
          <td>0.049853</td>
          <td>0.037057</td>
        </tr>
        <tr>
          <th>lost</th>
          <td>0.000000</td>
          <td>0.118102</td>
          <td>0.101306</td>
          <td>0.093842</td>
          <td>0.075180</td>
          <td>0.066649</td>
          <td>0.060784</td>
          <td>0.054385</td>
          <td>0.040523</td>
          <td>0.035724</td>
          <td>0.023460</td>
          <td>0.022661</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>0.000000</td>
          <td>0.089843</td>
          <td>0.109571</td>
          <td>0.080778</td>
          <td>0.064783</td>
          <td>0.047454</td>
          <td>0.046388</td>
          <td>0.031725</td>
          <td>0.027459</td>
          <td>0.024527</td>
          <td>0.021061</td>
          <td>0.022394</td>
        </tr>
        <tr>
          <th>payment_choice</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.033591</td>
          <td>0.043455</td>
          <td>0.031991</td>
          <td>0.023994</td>
          <td>0.022661</td>
          <td>0.017329</td>
          <td>0.010131</td>
          <td>0.011464</td>
        </tr>
        <tr>
          <th>delivery_choice</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.054119</td>
          <td>0.061584</td>
          <td>0.049054</td>
          <td>0.034391</td>
          <td>0.031725</td>
          <td>0.026926</td>
          <td>0.018395</td>
          <td>0.018395</td>
          <td>0.014396</td>
          <td>0.012263</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.003999</td>
          <td>0.024793</td>
          <td>0.024793</td>
          <td>0.018395</td>
          <td>0.014929</td>
          <td>0.013063</td>
          <td>0.010131</td>
        </tr>
        <tr>
          <th>product2</th>
          <td>0.000000</td>
          <td>0.114370</td>
          <td>0.065849</td>
          <td>0.057851</td>
          <td>0.045854</td>
          <td>0.035724</td>
          <td>0.030392</td>
          <td>0.023727</td>
          <td>0.020794</td>
          <td>0.020261</td>
          <td>0.017595</td>
          <td>0.016262</td>
        </tr>
        <tr>
          <th>product1</th>
          <td>0.000000</td>
          <td>0.070115</td>
          <td>0.045055</td>
          <td>0.042655</td>
          <td>0.031991</td>
          <td>0.025860</td>
          <td>0.020794</td>
          <td>0.017595</td>
          <td>0.017062</td>
          <td>0.011197</td>
          <td>0.012263</td>
          <td>0.010397</td>
        </tr>
        <tr>
          <th>payment_card</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.017595</td>
          <td>0.020261</td>
          <td>0.017062</td>
          <td>0.012797</td>
          <td>0.010664</td>
          <td>0.010131</td>
          <td>0.005065</td>
        </tr>
        <tr>
          <th>delivery_courier</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.025327</td>
          <td>0.032791</td>
          <td>0.024793</td>
          <td>0.015729</td>
          <td>0.017595</td>
          <td>0.011997</td>
          <td>0.007465</td>
          <td>0.007731</td>
          <td>0.006398</td>
        </tr>
        <tr>
          <th>delivery_pickup</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.014396</td>
          <td>0.016796</td>
          <td>0.015463</td>
          <td>0.012530</td>
          <td>0.009597</td>
          <td>0.010131</td>
          <td>0.005332</td>
          <td>0.007198</td>
          <td>0.003999</td>
        </tr>
        <tr>
          <th>payment_cash</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.004799</td>
          <td>0.006931</td>
          <td>0.004799</td>
          <td>0.004266</td>
          <td>0.004532</td>
          <td>0.002133</td>
          <td>0.001866</td>
        </tr>
        <tr>
          <th>ENDED</th>
          <td>0.000000</td>
          <td>0.000000</td>
          <td>0.118102</td>
          <td>0.219408</td>
          <td>0.313250</td>
          <td>0.388430</td>
          <td>0.457745</td>
          <td>0.536124</td>
          <td>0.607038</td>
          <td>0.661690</td>
          <td>0.709144</td>
          <td>0.745135</td>
        </tr>
      </tbody>
    </table>
    </div>
