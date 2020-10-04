Release notes
=============

Version 2.0
-----------

- Completely reworked retentioneering workflow: functions init_config() and retention.prepare() are removed. In 2.0 it is not required to initialize "positive" and "negative" events before the analysis. You can start exploring your user behavior data first and define targets when needed as optional parameters. To access all retentioneering tools attribute ".retention" was renamed to ".rete". To get started with updated workflow refer to `this guide <https://retentioneering.github.io/retentioneering-tools/_build/html/getting_started.html>`__.

- plot_step_matrix() function was significantly reworked and renamed to step_matrix(). To read more about new step_matrix() functionality refer to `this description <https://retentioneering.github.io/retentioneering-tools/_build/html/step_matrix.html>`__

- get_edgelist(), get_adjacency() and plot_graph() functions now have customizable weighting options (total nuber of events, normalized by full dataset, normalized by nodes, etc.). To learn more please fere to `this description <https://retentioneering.github.io/retentioneering-tools/_build/html/plot_graph.html>`__

- new function compare() was added to compare two segments of users or test/control groups in AB test based on defined metrics. Read more about compare function in `this description <https://retentioneering.github.io/retentioneering-tools/_build/html/compare.html>`__

- Users behavior segmentation was reworked and now works significantly faster and updated with new functionality. For more information refer to `this description <https://retentioneering.github.io/retentioneering-tools/_build/html/clustering.html>`__

- Functionality to plot user conversion funnels was reworked and improved. To learn more about new features read `this description <https://retentioneering.github.io/retentioneering-tools/_build/html/funnel.html>`__
