[![Rete logo](https://raw.githubusercontent.com/retentioneering/pics/master/pics/logo_long_black.png)](https://github.com/retentioneering/retentioneering-tools)
[![Discord](https://img.shields.io/badge/server-on%20discord-blue)](https://discord.com/invite/hBnuQABEV2)
[![Telegram](https://img.shields.io/badge/chat-on%20telegram-blue)](https://t.me/retentioneering_support)
[![Python version](https://img.shields.io/pypi/pyversions/retentioneering)](https://pypi.org/project/retentioneering/)
[![Pipi version](https://img.shields.io/pypi/v/retentioneering)](https://pypi.org/project/retentioneering/)
[![Downloads](https://pepy.tech/badge/retentioneering)](https://pepy.tech/project/retentioneering)
[![Downloads](https://static.pepy.tech/badge/retentioneering/month)](https://pepy.tech/project/retentioneering)

## What is Retentioneering?

Retentioneering is a Python library that makes analyzing clickstreams, user paths (trajectories), and event logs much easier, and yields much broader and deeper insights than funnel analysis.

You can use Retentioneering to explore user behavior, segment users, and form hypotheses about what drives users to desirable actions or to churning away from a product.

Retentioneering uses clickstream data to build behavioral segments, highlighting the events and patterns in user behavior that impact your conversion rates, retention, and revenue. The Retentioneering library is created for data analysts, marketing analysts, product owners, managers, and anyone else whose job is to improve a product’s quality.

[![A simplified scenario of user behavior exploration with Retentioneering.](https://raw.githubusercontent.com/retentioneering/pics/master/pics/rete20/intro_0.png)](https://github.com/retentioneering/retentioneering-tools)


As a natural part of the [Jupyter](https://jupyter.org/) environment, Retentioneering extends the abilities of [pandas](https://pandas.pydata.org), [NetworkX](https://networkx.org/), [scikit-learn](https://scikit-learn.org) libraries to process sequential events data more efficiently. Retentioneering tools are interactive and tailored for analytical research, so you do not have to be a Python expert to use it. With just a few lines of code, you can wrangle data, explore customer journey maps, and make visualizations.

### Retentioneering structure

Retentioneering consists of two major parts: [the preprocessing module](https://doc.retentioneering.com/stable/doc/getting_started/quick_start.html#quick-start-preprocessing) and [the path analysis tools](https://doc.retentioneering.com/stable/doc/getting_started/quick_start.html#quick-start-rete-tools).

The **preprocessing module** provides a wide range of hands-on methods specifically designed for processing clickstream data, which can be called either using code, or via the preprocessing GUI. With separate methods for grouping or filtering events, splitting a clickstream into sessions, and much more, the Retentioneering preprocessing module enables you to dramatically reduce the amount of code, and therefore potential errors. Plus, if you’re dealing with a branchy analysis, which often happens, the preprocessing methods will help you make the calculations structured and reproducible, and organize them as a calculation graph. This is especially helpful for working with a team.

The **path analysis tools** bring behavior-driven segmentation of users to product analysis by providing a powerful set of techniques for performing in-depth analysis of customer journey maps. The tools feature informative and interactive visualizations that make it possible to quickly understand in very high resolution the complex structure of a clickstream.

## Documentation

Complete documentation is available [here](https://doc.retentioneering.com/stable/doc/index.html).

## Installation

Retentioneering can be installed via pip using [PyPI](https://pypi.org/project/retentioneering/).

```bash
pip install retentioneering
```

Or directly from Jupyter notebook or [google.colab](https://colab.research.google.com/).

```bash
!pip install retentioneering
```

## Quick start

We recommend starting your Retentioneering journey with the [Quick Start document](https://doc.retentioneering.com/stable/doc/getting_started/quick_start.html).


## Step-by-step guides

- [Eventstream](https://doc.retentioneering.com/stable/doc/user_guides/eventstream.html)

### Preprocessing

- [Data processors](https://doc.retentioneering.com/stable/doc/user_guides/dataprocessors.html)
- [Preprocessing graph](https://doc.retentioneering.com/stable/doc/user_guides/preprocessing.html)
- [Preprocessing tutorial](https://colab.research.google.com/drive/1WwVI5oQF81xp9DJ6rP5HyM_UjuNPjUk0?usp=sharing)

### Path analysis tools

- [Transition graph](https://doc.retentioneering.com/stable/doc/user_guides/transition_graph.html)
- [Step matrix](https://doc.retentioneering.com/stable/doc/user_guides/step_matrix.html)
- [Step Sankey](https://doc.retentioneering.com/stable/doc/user_guides/step_sankey.html)
- [Clusters](https://doc.retentioneering.com/stable/doc/user_guides/clusters.html)
- [Funnel](https://doc.retentioneering.com/stable/doc/user_guides/funnel.html)
- [Cohorts](https://doc.retentioneering.com/stable/doc/user_guides/cohorts.html)
- [Stattests](https://doc.retentioneering.com/stable/doc/user_guides/stattests.html)

## Raw data type
Raw data can be downloaded from Google Analytics BigQuery stream, or any other such streams. Just convert that data to the list of triples - user_id, event, and timestamp - and pass it to Retentioneering tools. The package also includes some datasets for a quick start.

## Changelog

- [Version 3.3.0](https://doc.retentioneering.com/stable/doc/whatsnew/v3.3.0.html)
- [Version 3.2.1](https://doc.retentioneering.com/stable/doc/whatsnew/v3.2.1.html)
- [Version 3.2](https://doc.retentioneering.com/stable/doc/whatsnew/v3.2.0.html)
- [Version 3.1](https://doc.retentioneering.com/stable/doc/whatsnew/v3.1.0.html)
- [Version 3.0](https://doc.retentioneering.com/3.0/doc/whatsnew/v3.0.0.html)
- [Version 2.0 (archive)](https://github.com/retentioneering/retentioneering-tools-2-archive)

## Contributing

This is community-driven open source project in active development. Any contributions,
new ideas, bug reports, bug fixes, documentation improvements are very welcome.

Retentioneering now provides several opensource solutions for data-driven product
analytics and web analytics. Please checkout [this repository](https://github.com/retentioneering/retentioneering-dom-observer) for JS library to track the mutations of the website elements.

Apps are better with math! :)
Retentioneering is a research laboratory, analytics methodology and opensource
tools founded by [Maxim Godzi](https://www.linkedin.com/in/godsie/) in 2015.
Please feel free to contact us at retentioneering@gmail.com if you have any
questions regarding this repo.
