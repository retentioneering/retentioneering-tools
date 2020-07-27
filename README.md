<div align="left">

[![Rete logo](https://github.com/retentioneering/pics/blob/master/pics/logo_long_black.png)](https://github.com/retentioneering/retentioneering-tools)

[![Pipi version](https://img.shields.io/pypi/v/retentioneering)](https://pypi.org/project/retentioneering/)
[![Telegram](https://img.shields.io/badge/channel-on%20telegram-blue)](https://t.me/retentioneering_meetups)
[![Reddit](https://img.shields.io/reddit/subreddit-subscribers/retentioneering?style=social)](https://www.reddit.com/r/retentioneering/)
[![Python version](https://img.shields.io/pypi/pyversions/retentioneering)](https://pypi.org/project/retentioneering/)
[![License](https://img.shields.io/pypi/l/retentioneering)](https://www.mozilla.org/en-US/MPL/)
[![Travis Build Status](https://travis-ci.com/retentioneering/retentioneering-tools.svg?branch=unit_tests)](https://travis-ci.com/github/retentioneering/retentioneering-tools)


## What is it?


Retentioneering Tools is a Python framework to process and analyze clickstreams, event streams, trajectories, and event logs. You can segment users, clients (agents), build ML pipelines to predict agent category or probability of target event based on historical data. With a simulator tool you can resample the data based on fitted Markov model per each behavioral segment to explore scenarios and calculate the elasticity and sensitivity of your target KPIs to changes agent behavior at the event level.

Retentioneering extends Pandas, NetworkX, Scikit-learn for in-depth processing of event sequences data, specifically Retentioneering Tools provides a powerful environment to perform an in-depth analysis of customer journey maps, bringing behavior-driven segmentation of users and machine learning pipelines to product analytics. Retentioneering is also developing customer journey map simulation engines that allow the data scientists to explore the business impact of CJM mutations and optimize product and online marketing.

Product analysts can apply Retentioneering Tools as a Python framework to explore, grow, and optimize the product based on deep analysis of user trajectories. Using Retentioneering you can vectorize clickstream logs and cluster user trajectories to automatically identify common successful or churn patterns. You can explore those patterns using our tools such as graph visualizer, step matrix, multiple clustering, and segmentation engines, and many others.

This repository contains both python library with easy to use utils, but also we provide several demos Python Notebooks and datasets to illustrate how to automate product analytics routines.

## How it works?

All you need to get started with Retentioneering is clickstream log of events from your web-site or app: {user_ID, event_ID, timestamp} (or use provided sample [datasets](https://github.com/retentioneering/retentioneering-tools/tree/master/examples/data) in .csv format). You can vectorize individual user trajectories in dataset and plot all your users logs on 2D map using TSNE or UMAP projection:

```python
data.retention.learn_tsne(plot_type='targets');
```
<div align="center">


[![intro 1](https://github.com/retentioneering/pics/blob/master/pics/intro_1.png)](https://github.com/retentioneering/retentioneering-tools)


<div align="left">

Users with similar patterns will appear as close dots at such map. Group of users who do not reach specified target event represent some systematic problem: usage pattern which systematically does not lead to product goals. Next you can segment users based on their behavior in the product.

<div align="center">

[![intro 2](https://github.com/retentioneering/pics/blob/master/pics/intro_2.png)](https://github.com/retentioneering/retentioneering-tools)


<div align="left">


Obtained user segments can be explored with graph visualizer or step matrixes or clustered again:

```python
(data.retention.filter_cluster(4)
 .retention.plot_graph(thresh = 0.05))
```
<div align="left">

<img src="https://github.com/retentioneering/pics/blob/master/pics/graph_0.png" width="600px">


Plot reverse step matrix where rows correspond to events and columns show event position in the trajectory. Numbers show fraction of users having corresponding event at corresponding step:

```python
(data.retention.filter_cluster(4)
.retention.get_step_matrix(reverse='neg'))
```
<div align="left">

<img src="https://github.com/retentioneering/pics/blob/master/pics/matrix_0.png" width="500px">


To explore more features please see the [documentation](https://retentioneering.github.io/retentioneering-tools/)

## Installation

### Python and Jupyter

Firstly, you need to install python and Jupyter.
We support only python 3.6 or higher versions.
For quick start better to install [Anaconda](https://www.anaconda.com/).

### Python package

- You can install our package using pip:

```bash
pip3 install retentioneering
```

- Or directly from the source:

```bash
git clone https://github.com/retentioneering/retentioneering-tools
cd retentioneering-tools
pip3 install .
```

## Documentation

#### Explore [example notebooks](https://github.com/retentioneering/retentioneering-tools/tree/master/examples) to get started or go through documentation pages:

- [First steps](https://retentioneering.github.io/retentioneering-tools/_build/html/early_steps.html#first-steps) Configuration, preparing your data graph basics.
- [Step Matrix](https://retentioneering.github.io/retentioneering-tools/_build/html/early_steps.html#temporal-funnel), [Clustering](https://retentioneering.github.io/retentioneering-tools/_build/html/early_steps.html#clustering) Clustermap, TSNE projections, target events in clustering, cluster graphs.
- [Supervised classifier](https://retentioneering.github.io/retentioneering-tools/_build/html/early_steps.html#supervised-classifier) Supervised learning, sklearn-api.
- [Analysis](https://retentioneering.github.io/retentioneering-tools/_build/html/mobile-app-case.html#analysis) Step matrix and clustering.
- [Predict application remove](https://retentioneering.github.io/retentioneering-tools/_build/html/mobile-app-case.html#predict-app-remove)
- [Packages and Subpackages](https://retentioneering.github.io/retentioneering-tools/_build/html/retentioneering.html)
- [Utils and functions documentation](https://retentioneering.github.io/retentioneering-tools/)

## Contributing

This is community-driven open source project in active development. Any contributions, new ideas, bug reports, bug fixes, documentation improvements are very welcome.

Feel free to reach out to us: retentioneering[at]gmail.com

Retentioneering now provides several opensource solutions for data-driven product analytics and web analytics. Please checkout this repository for JS library to track the mutations of the website elements: https://github.com/retentioneering/retentioneering-dom-observer

Apps better with math!:)
Retentioneering is a research laboratory, analytics methodology and opensource tools founded by Maxim Godzi and Anatoly Zaytsev in 2015. Please feel free to contact us at retentioneeringATgmail.com if you have any questions regarding this rep, or to obtain more tools that we are not able to provide though the public rep.
