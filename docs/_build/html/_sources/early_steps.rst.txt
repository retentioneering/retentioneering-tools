
Introduction
~~~~~~~~~~~~

This notebook can be found `here <https://github.com/retentioneering/retentioneering-tools/blob/master/examples/early_steps.ipynb>`__.

First steps
===========

.. code:: ipython3

    from retentioneering import init_config
    import pandas as pd


Firstly, we need to initialize our config file

.. code:: ipython3

    init_config(
        experiments_folder='experiments', # folder for saving experiment results: graph visualization, heatmaps and etc.
        index_col='user_pseudo_id', # column by which we split users / sessions / whatever
        event_col='event_name', # column that describes event
        event_time_col='event_timestamp', # column that describes timestamp of event
        positive_target_event='lost', # name of positive target event
        negative_target_event='passed', # name of negative target event
        pos_target_definition={ # how to define positive event, e.g. empty means that add passed for whom was not 'lost'
            'time_limit': 600
        },
        neg_target_definition={ # how to define negative event, e.g. users who were inactive for 600 seconds.
    
        },
    #     neg_target_definition={ # you also can define target event as list of other events
    #         'event_list': ['lost']
    #     }
    )

We need to create instance of pandas DataFrame with our data.

.. code:: ipython3

    data = pd.read_csv('data/train.csv')
    data = data.sort_values('event_timestamp')

.. code:: ipython3

    data = data.retention.prepare()

.. code:: ipython3

    data.head()




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
          <th>event_name</th>
          <th>event_timestamp</th>
          <th>user_pseudo_id</th>
        </tr>
      </thead>
      <tbody>
        <tr style="font-size: 10px;">
          <th>13779</th>
          <td>onboarding_welcome_screen</td>
          <td>1538341204985002000</td>
          <td>3f711081df2e582efe4be33349b811ae</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>13780</th>
          <td>onboarding__chooseLoginType</td>
          <td>1538341208435003000</td>
          <td>3f711081df2e582efe4be33349b811ae</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>13781</th>
          <td>onboarding_privacy_policyShown</td>
          <td>1538341208468004000</td>
          <td>3f711081df2e582efe4be33349b811ae</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>13782</th>
          <td>onboarding_login_Type1</td>
          <td>1538341208478005000</td>
          <td>3f711081df2e582efe4be33349b811ae</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>13783</th>
          <td>onboarding_privacy_policyAccepted</td>
          <td>1538341211123008000</td>
          <td>3f711081df2e582efe4be33349b811ae</td>
        </tr>
      </tbody>
    </table>
    </div>



.. code:: ipython3

    edgelist = data.retention.get_edgelist()
    edgelist.head()




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
          <th>event_name</th>
          <th>next_event</th>
          <th>event_count</th>
        </tr>
      </thead>
      <tbody>
        <tr style="font-size: 10px;">
          <th>0</th>
          <td>onboarding__chooseLoginType</td>
          <td>lost</td>
          <td>0.000242</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>1</th>
          <td>onboarding__chooseLoginType</td>
          <td>onboarding_login_Type1</td>
          <td>0.100024</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>2</th>
          <td>onboarding__chooseLoginType</td>
          <td>onboarding_login_Type2</td>
          <td>0.038415</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>3</th>
          <td>onboarding__chooseLoginType</td>
          <td>onboarding_privacy_policyShown</td>
          <td>0.515342</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>4</th>
          <td>onboarding__loginFailure</td>
          <td>lost</td>
          <td>0.000242</td>
        </tr>
      </tbody>
    </table>
    </div>



You can use any columns as edge source and target using ``cols`` param,
by default it is equal to list of ``event_col`` and automatically
created ``next_event`` (shift of it) is used.

Also you can use any column and any aggregation e.g. one can calculate
number of unique users, who passed through edge via next chunk

.. code:: ipython3

    data.head()




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
          <th>event_name</th>
          <th>event_timestamp</th>
          <th>user_pseudo_id</th>
          <th>next_event</th>
          <th>next_timestamp</th>
        </tr>
      </thead>
      <tbody>
        <tr style="font-size: 10px;">
          <th>30255</th>
          <td>onboarding_welcome_screen</td>
          <td>1538341610616002000</td>
          <td>000bf8e1812a0335c7e65d52b3f6e816</td>
          <td>onboarding_otherLogin__show</td>
          <td>1.538342e+18</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>30256</th>
          <td>onboarding_otherLogin__show</td>
          <td>1538341616091017000</td>
          <td>000bf8e1812a0335c7e65d52b3f6e816</td>
          <td>onboarding_welcome_screen</td>
          <td>1.538342e+18</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>30257</th>
          <td>onboarding_welcome_screen</td>
          <td>1538341620614024000</td>
          <td>000bf8e1812a0335c7e65d52b3f6e816</td>
          <td>onboarding_welcome_screen</td>
          <td>1.538342e+18</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>30258</th>
          <td>onboarding_welcome_screen</td>
          <td>1538341626961003000</td>
          <td>000bf8e1812a0335c7e65d52b3f6e816</td>
          <td>onboarding_welcome_screen</td>
          <td>1.538342e+18</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>30259</th>
          <td>onboarding_welcome_screen</td>
          <td>1538341628689012000</td>
          <td>000bf8e1812a0335c7e65d52b3f6e816</td>
          <td>onboarding_welcome_screen</td>
          <td>1.538342e+18</td>
        </tr>
      </tbody>
    </table>
    </div>



.. code:: ipython3

    edgelist = data.retention.get_edgelist(edge_col='user_pseudo_id', edge_attributes='users_nunique', norm=False)
    edgelist.sort_values('users_nunique', ascending=False).head()




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
          <th>event_name</th>
          <th>next_event</th>
          <th>users_nunique</th>
        </tr>
      </thead>
      <tbody>
        <tr style="font-size: 10px;">
          <th>84</th>
          <td>onboarding_welcome_screen</td>
          <td>onboarding_welcome_screen</td>
          <td>2586</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>85</th>
          <td>onboarding_welcome_screen</td>
          <td>passed</td>
          <td>2330</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>3</th>
          <td>onboarding__chooseLoginType</td>
          <td>onboarding_privacy_policyShown</td>
          <td>2112</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>79</th>
          <td>onboarding_welcome_screen</td>
          <td>onboarding__chooseLoginType</td>
          <td>1898</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>67</th>
          <td>onboarding_privacy_policyShown</td>
          <td>onboarding_login_Type1</td>
          <td>1667</td>
        </tr>
      </tbody>
    </table>
    </div>



or adjacency matrix

.. code:: ipython3

    data.retention.get_adjacency()




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
          <th>onboarding__chooseLoginType</th>
          <th>lost</th>
          <th>onboarding_login_Type1</th>
          <th>onboarding_login_Type2</th>
        </tr>
      </thead>
      <tbody>
        <tr style="font-size: 10px;">
          <th>onboarding__chooseLoginType</th>
          <td>0.00</td>
          <td>0.00</td>
          <td>0.1</td>
          <td>0.04</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>lost</th>
          <td>0.00</td>
          <td>0.00</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_login_Type1</th>
          <td>0.00</td>
          <td>0.01</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_login_Type2</th>
          <td>0.01</td>
          <td>0.01</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_privacy_policyShown</th>
          <td>0.00</td>
          <td>0.00</td>
          <td>0.4</td>
          <td>0.11</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding__loginFailure</th>
          <td>0.00</td>
          <td>0.00</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_login_Type1_cancel</th>
          <td>0.05</td>
          <td>0.02</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_welcome_screen</th>
          <td>0.47</td>
          <td>0.25</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_otherLogin__show</th>
          <td>0.00</td>
          <td>0.02</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_privacy_policyAccepted</th>
          <td>0.03</td>
          <td>0.04</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_privacy_policyDecline</th>
          <td>0.01</td>
          <td>0.00</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_privacy_policyTapToPolicy</th>
          <td>0.00</td>
          <td>0.00</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_otherLogin__chooseLoginType</th>
          <td>0.00</td>
          <td>0.00</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>passed</th>
          <td>0.00</td>
          <td>0.00</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_otherLogin_privacy_policyDecline</th>
          <td>0.00</td>
          <td>0.00</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_otherLogin_privacy_policyShown</th>
          <td>0.00</td>
          <td>0.01</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_profile_edit_close</th>
          <td>0.01</td>
          <td>0.00</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_otherLogin__close</th>
          <td>0.06</td>
          <td>0.02</td>
          <td>0.0</td>
          <td>0.00</td>
        </tr>
      </tbody>
    </table>
    </div>



All similar parameters work for adjacency matrix calculation.

.. code:: ipython3

    data.retention.get_adjacency(edge_col='user_pseudo_id', edge_attributes='users_nunique', norm=False)




.. raw:: html

    <div>
    <style scoped>
        .dataframe tbody tr th:only-of-type {
            vertical-align: middle;
            font-size: 12px;
        }

        .dataframe tbody tr th {
            vertical-align: top;
            font-size: 12px;
        }

        .dataframe thead th {
            text-align: right;
            font-size: 12px;
        }
    </style>
    <table border="1" class="dataframe">
      <thead>
        <tr style="font-size: 10px;text-align: right;">
          <th></th>
          <th>onboarding__chooseLoginType</th>
          <th>lost</th>
          <th>onboarding_login_Type1</th>
          <th>onboarding_login_Type2</th>
        </tr>
      </thead>
      <tbody>
        <tr style="font-size: 10px;">
          <th>onboarding__chooseLoginType</th>
          <td>0.0</td>
          <td>1.0</td>
          <td>356.0</td>
          <td>142.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>lost</th>
          <td>0.0</td>
          <td>0.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_login_Type1</th>
          <td>12.0</td>
          <td>40.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_login_Type2</th>
          <td>34.0</td>
          <td>32.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_privacy_policyShown</th>
          <td>0.0</td>
          <td>0.0</td>
          <td>1667.0</td>
          <td>455.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding__loginFailure</th>
          <td>0.0</td>
          <td>1.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_login_Type1_cancel</th>
          <td>200.0</td>
          <td>91.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_welcome_screen</th>
          <td>1898.0</td>
          <td>1043.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_otherLogin__show</th>
          <td>0.0</td>
          <td>82.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_privacy_policyAccepted</th>
          <td>127.0</td>
          <td>161.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_privacy_policyDecline</th>
          <td>46.0</td>
          <td>17.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_privacy_policyTapToPolicy</th>
          <td>1.0</td>
          <td>7.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_otherLogin__chooseLoginType</th>
          <td>3.0</td>
          <td>12.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>passed</th>
          <td>0.0</td>
          <td>0.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_otherLogin_privacy_policyDecline</th>
          <td>11.0</td>
          <td>4.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_otherLogin_privacy_policyShown</th>
          <td>9.0</td>
          <td>33.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_profile_edit_close</th>
          <td>34.0</td>
          <td>19.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
        <tr style="font-size: 10px;">
          <th>onboarding_otherLogin__close</th>
          <td>249.0</td>
          <td>99.0</td>
          <td>0.0</td>
          <td>0.0</td>
        </tr>
      </tbody>
    </table>
    </div>



or we can simply visualize graph.

By default weight in visualization is equal to rate of unique users, who
pass the edge, you can change it to rate of all event occasions by
turning ``user_based`` equal ``False``.

.. code:: ipython3

    data.retention.plot_graph(thresh=0.05, width=800, height=800)


.. raw:: html


            <iframe
                width="900"
                height="900"
                src="_static/index.html"
                frameborder="0"
                allowfullscreen
            ></iframe>



If you change node positions and want to save resulting layout, you can
click on donwload button and load it to graph visualizer as follows.

.. code:: ipython3

    data.retention.plot_graph(layout_dump='node_params.json', width=800, height=800)



Also you can use other data columns and aggregation functions from
``retention.get_edgelist()`` method (be sure that in this case
``user_based=False``).

For example, we can visualize mean time between events.

Firtly, we should add column with time difference between event
timestamps.

.. code:: ipython3

    data['seconds_between_events'] = (data.next_timestamp - data.event_timestamp).dt.total_seconds()
    # use show_percent=False to visualize absolute value
    data.retention.plot_graph(user_based=False, edge_col='seconds_between_events', edge_attributes='time_mean', thresh=0.01, width=800, height=800, show_percent=False)


Temporal funnel
===============

Let’s plot the temporal funnel. Rows correspond to different events and
columns correspond to step number in the user trajectory, value
corresponds to fraction of all users who had corresponding event at
corresponding step. For example, you can see that all users in the
analysis start from “welcome_screen” (step 1) and end ended up passed
(~0.6) or lost (~0.4) after 24 steps

.. code:: ipython3

    desc_table = data.retention.get_step_matrix(max_steps=30)



.. image:: _static/output_24_0.png


And we can calculate temporal funnel difference between two groups

.. code:: ipython3

    # create group filter based on target events
    diff_filter = data.retention.create_filter()

    # calculate difference table between two groups
    diff_table = data.retention.get_step_matrix_difference(diff_filter, max_steps=30)



.. image:: _static/output_26_0.png


Clustering
==========

We can use clustering with different visualizations

Clutermap allows to see how important different events are for
clustering. For example we can see that ``onboarding_welcome_screen`` is
always equal, so it does not affect clustering, but
``onboarding_chooseLoginType`` varies accross users and creates some
clusters.

.. code:: ipython3

    data.retention.get_clusters(plot_type='cluster_heatmap');



.. image:: _static/output_30_0.png


Then it will be useful to visualize projection of user trajectories to
understand how many clusters we have.

.. code:: ipython3

    data.retention.learn_tsne(plot_type='targets');



.. image:: _static/output_32_0.png


We can see that projection is poor, so it will be good to tune it. To
update TSNE weights we need to set ``refit`` parameter to ``True``.

Any parameter from ``sklearn.manifold.TSNE`` can be used, e.g.
``perplexity`` can help to obtain better visualization.

.. code:: ipython3

    data.retention.learn_tsne(perplexity=10, plot_type='targets', refit=True);



.. image:: _static/output_34_0.png


Now we can see two dense cirle clusters.

Any parameters from ``sklearn.cluster.KMeans`` can be used.

.. code:: ipython3

    data.retention.get_clusters(n_clusters=8, plot_type='cluster_tsne', refit_cluster=True);



.. image:: _static/output_36_0.png


We do not use target events in clustering, so we can compare different
groups in terms of what target event is likely to occur in them.

.. code:: ipython3

    data.retention.get_clusters(plot_type='cluster_pie');



.. image:: _static/output_38_0.png


We can see that clusters ``0`` and ``1`` are pretty interesting, so we
can visualize graph for them.

.. code:: ipython3

    (data
     .retention
     .filter_cluster(0)
     .retention
     .plot_graph(width=800, height=800))


.. raw:: html


            <iframe
                width="900"
                height="900"
                src="_static/cluster1.html"
                frameborder="0"
                allowfullscreen
            ></iframe>



.. code:: ipython3

    (data
     .retention
     .filter_cluster(1)
     .retention
     .plot_graph(width=800, height=800))



.. raw:: html


            <iframe
                width="900"
                height="900"
                src="_static/cluster2.html"
                frameborder="0"
                allowfullscreen
            ></iframe>



Supervised classifier
=====================

Supervised learning is usually better then clustering.

.. code:: ipython3

    model = data.retention.create_model()

To understand what features are meaningful, we can visualize graph of
weights.

Larger the node or edge, larger its effect on probability of target
event. Green nodes mean positive effect, red nodes – negative.

.. code:: ipython3

    features = data.retention.extract_features(ngram_range=(1,2))
    target = features.index.isin(data.retention.get_positive_users())

.. code:: ipython3

    model.permutation_importance(features, target, thresh=0.)


.. parsed-literal::


                ROC-AUC: 0.9124591164734438
                PR-AUC: 0.8975341600045529
                Accuracy: 0.8849514563106796


.. raw:: html


            <iframe
                width="600"
                height="600"
                src="_static/importance.html"
                frameborder="0"
                allowfullscreen
            ></iframe>



You can use any different model with sklearn-api (ont only sklearn
package has it e.g. ``lightgm`` can be used too).

And pass params to it.

.. code:: ipython3

    from sklearn.ensemble import RandomForestClassifier
    model = data.retention.create_model(RandomForestClassifier, n_estimators=25)

.. code:: ipython3

    features = data.retention.extract_features(ngram_range=(1,2))
    target = features.index.isin(data.retention.get_positive_users())

.. code:: ipython3

    model.permutation_importance(features, target, thresh=0.)


Output:

            ROC-AUC: 0.9592964985907655

            PR-AUC: 0.9456069017198432

            Accuracy: 0.9269417475728156

