<div align="left">

[![Rete logo](https://github.com/retentioneering/pics/blob/master/pics/logo_long_black.png)](https://github.com/retentioneering/retentioneering-tools)

[![Pipi version](https://img.shields.io/pypi/v/retentioneering)](https://pypi.org/project/retentioneering/)
[![Telegram](https://img.shields.io/badge/channel-on%20telegram-blue)](https://t.me/retentioneering_meetups)
[![Python version](https://img.shields.io/pypi/pyversions/retentioneering)](https://pypi.org/project/retentioneering/)
[![Downloads](https://pepy.tech/badge/retentioneering)](https://pepy.tech/project/retentioneering)
[![Travis Build Status](https://travis-ci.com/retentioneering/retentioneering-tools.svg)](https://travis-ci.com/github/retentioneering/retentioneering-tools)


## What is Retentioneering?


Retentioneering is a Python framework and library to assist product analysts and 
marketing analysts as it makes it easier to process and analyze clickstreams, 
event streams, trajectories, and event logs. You can segment users, clients 
(agents), build ML pipelines to predict agent category or probability of
 target event based on historical data.

In a common scenario you can use raw data from Google Analytics BigQuery stream 
or any other silimal streams in form of events and their timestamps for users, 
and Retentioneering is all you need to explore the user behavior from that data, 
it can reveal much more isights than funnel analytics, as it will automatically 
build the behavioral segments and their patterns, highlighting what events and 
pattern impact your conversion rates, retention and revenue.

Retentioneering extends Pandas, NetworkX, Scikit-learn for in-depth processing 
of event sequences data, specifically Retentioneering provides a powerful environment 
to perform an in-depth analysis of customer journey maps, bringing behavior-driven 
segmentation of users and machine learning pipelines to product analytics.

Most recent is Retentioneering 2.0.0, this version has major updates from 1.0.x 
and it is not reverse compatible with previous releases due to major syntax changes.
With significant improvements we now provided architecture and the solid ground for 
farther updates and rapid development of analytical tools. Please update, leave your
feedback and stay tuned.

[![intro 0](https://github.com/retentioneering/pics/blob/master/pics/rete20/intro_0.png)](https://github.com/retentioneering/retentioneering-tools)

## Changelog

This is new major release Retentioneering 2.0. Change log is available [here](https://retentioneering.github.io/retentioneering-tools/_build/html/release_notes.html).

Complete documentation is available [here](https://retentioneering.github.io/retentioneering-tools/).


## Installation

Option 1. Run directly from google.colab. Open google.colab and click File-> “new notebook”. 
In the code cell run following to install Retentioneering (same command will install directly 
from Jupyter notebook):

```bash
!pip3 install retentioneering
```

Option 2. Install Retentioneering from PyPI:

```bash
pip3 install retentioneering
```

Option 3. Install Retentioneering directly from the source:

```bash
git clone https://github.com/retentioneering/retentioneering-tools
cd retentioneering-tools
python3 setup.py install
```

## Quick start

[Start using Retentioneering for clickstream analysis](https://retentioneering.github.io/retentioneering-tools/_build/html/getting_started.html)

Or directly open this notebook in [Google Colab](https://colab.research.google.com/github/retentioneering/retentioneering-tools/blob/master/docs/source/_static/examples/graph_tutorial.ipynb) to run with sample data.

Suggested first steps:

```python
import retentioneering

# load sample user behavior data as a pandas dataframe: 
data = retentioneering.datasets.load_simple_shop()

# update config to pass columns names:
retentioneering.config.update({
    'user_col': 'user_id',
    'event_col':'event',
    'event_time_col':'timestamp',
})
```

Above we imported sample dataset, which is regular pandas dataframe containing raw user
behavior data from hypothetical web-site or app in form of sequence of records
{'user_id', 'event', 'timestamp'}, and pass those column names to retentioneering.config.
Now, let's plot the graph to visualize user behaviour from the dataset 
(read more about graphs [here](https://retentioneering.github.io/retentioneering-tools/_build/html/plot_graph.html)):

<div align="left">

 ```python
data.rete.plot_graph(norm_type='node',
                      weight_col='user_id',
                      thresh=0.2,
                      targets = {'payment_done':'green',
                                 'lost':'red'})
```

[![intro 1](https://github.com/retentioneering/pics/blob/master/pics/rete20/graph_0.png)](https://github.com/retentioneering/retentioneering-tools)

Here we obtain the high-level graph of user activity where 
edge A --> B weight shows percent of users transitioning to event B from 
all users reached event A (note, edges with small weighs are 
thresholded to avoid visual clutter, read more in the documentation)

To automatically find distinct behavioral patterns we can cluster users from the
dataset based on their behavior (read more about behavioral clustering [here](https://retentioneering.github.io/retentioneering-tools/_build/html/clustering.html)):

<div align="left">

```pyhton
data.rete.get_clusters(method='kmeans',
                       n_clusters=8,
                       ngram_range=(1,2),
                       plot_type='cluster_bar',
                       targets=['payment_done','cart']);
```

[![intro 1](https://github.com/retentioneering/pics/blob/master/pics/rete20/clustering_2.svg)](https://github.com/retentioneering/retentioneering-tools)

<div align="left">

Users with similar behavior grouped in the same cluster. Clusters with low conversion rate
can represent systematic problem in the product: specific behavior pattern which does not 
lead to product goals. Obtained user segments can be explored deeper to understand 
problematic behavior pattern. In the example above for instance, cluster 4 has low 
conversion rate to purchase but high conversion rate to cart visit.

```python
clus_4 = data.rete.filter_cluster(4)
clus_4.rete.plot_graph(thresh=0.1,
                        weight_col='user_id',
                        targets = {'lost':'red',
                                   'payment_done':'green'})
```
<div align="left">

[![intro 1](https://github.com/retentioneering/pics/blob/master/pics/rete20/graph_1.png)](https://github.com/retentioneering/retentioneering-tools)


To explore more features please see the [documentation](https://retentioneering.github.io/retentioneering-tools/)

## Step-by-step guides

- [Visualize users behavior](https://retentioneering.github.io/retentioneering-tools/_build/html/plot_graph.html) 
- [Users flow and step matrix](https://retentioneering.github.io/retentioneering-tools/_build/html/step_matrix.html)
- [Users behavioral segmentation](https://retentioneering.github.io/retentioneering-tools/_build/html/clustering.html) 
- [Compare segments and AB tests](https://retentioneering.github.io/retentioneering-tools/_build/html/compare.html)
- [Funnel analysis](https://retentioneering.github.io/retentioneering-tools/_build/html/funnel.html)


## Contributing

This is community-driven open source project in active development. Any contributions, 
new ideas, bug reports, bug fixes, documentation improvements are very welcome.

Retentioneering now provides several opensource solutions for data-driven product 
analytics and web analytics. Please checkout this repository for JS library to track 
the mutations of the website elements: https://github.com/retentioneering/retentioneering-dom-observer

Apps are better with math!:)
Retentioneering is a research laboratory, analytics methodology and opensource 
tools founded by [Maxim Godzi](https://www.linkedin.com/in/godsie/) and 
[Anatoly Zaytsev](https://www.linkedin.com/in/anatoly-zaytsev/) in 2015. 
Please feel free to contact us at retentioneering@gmail.com if you have any 
questions regarding this repo.
