# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


import pandas as pd
import numpy as np
from IPython.display import IFrame, display
import json

__TEMPLATE__ = """
<!DOCTYPE html>
<html>
<head>
  <title>Expand All. Collapse All</title>
  <meta charset="UTF-8">
  
  <!-- first import Vue -->
  <script src="https://unpkg.com/vue/dist/vue.js"></script>
  <!-- import JavaScript -->
  <script src="https://cdn.jsdelivr.net/npm/liquor-tree/dist/liquor-tree.umd.js"></script>
</head>
<body>
  <div id="app">
      <tree
        ref="tree"      
        :data="treeData"
        :options="treeOptions"
        @node:expanded="onNodeExpand">
        <div slot-scope="{{ node }}" class="node-container">
            <div class="node-controls">
              <input type="checkbox" @click.stop="aggregateTo(node)">{{{{node.text}}}}</input>
            </div>
        </div>
      </tree>
    <div id="download">
      <a href="#" @click.stop="saveFile()">Download Filter</a>
    </div>
  </div>

  <script>
    let aggList = [];

    new Vue({{
      el: '#app',
      data: function() {{
        return {{
          treeData: getTreeData(),
          treeOptions: {{
          	checkbox: true
          }}
        }}
      }},

      methods: {{
        aggregateTo: function(node) {{
            if (!aggList.includes(node.text)) {{
                aggList.push(node.text)
            }} else {{
                aggList = aggList.filter(function(value, index, arr) {{
                  return (value !== node.text)
                }})
            }}
            console.log(aggList)
        }},
      	onNodeExpand : function(node) {{
         const expandedNodes = this.$refs.tree.findAll({{ state: {{ expanded: true }} }});

         expandedNodes.forEach(expandedNode => {{
           if (expandedNode.id !== node.id && node.depth === expandedNode.depth) {{
             expandedNode.collapse()
           }}
         }});
        }},
        saveFile : function() {{
      	  let checkedNodes = this.$refs.tree.findAll({{ state: {{ checked: true }} }});
      	  let filter_names = [];
      	  checkedNodes.forEach(function (x) {{
      	    filter_names.push(x.data.text);
          }});
      	  downloadLayout({{
            "filter_names": filter_names,
            "agg_list": aggList
          }});
        }}
      }}
    }});


function getTreeData() {{
  return {tree_data}
}}

function downloadLayout(x) {{
    var a = document.createElement("a");
    var file = new Blob([JSON.stringify(x)], {{type: "text/json;charset=utf-8"}});
    a.href = URL.createObjectURL(file);
    a.download = "filter_list.json";
    a.click();
}}
  </script>
</body>
</html>
"""


def _create_node(idx, splitted_names, res, pre):
    if idx == splitted_names.shape[1]:
        return res
    cols = splitted_names[idx].unique()
    for i in cols:
        if i is None:
            continue
        res.append({
            'text': pre + i,
            'state': {
                'expanded': False,
                'checked': True,
            },
            'children': _create_node(idx + 1, splitted_names[splitted_names[idx] == i], [], pre + i + '_')
        })
    return res


def show_tree_filter(event_col, width=500, height=500, **kwargs):
    """
    Shows tree selector, based on your event names.
    It uses `_` for splitting event names for group aggregation

    :param event_col: column with events
    :param width: width of IFrame
    :param height: height of IFrame
    :param kwargs: do nothing
    :return: nothing
    """

    splitted_names = pd.Series(event_col.unique()).str.split('_', expand=True)
    res = _create_node(0, splitted_names, [], '')
    res = __TEMPLATE__.format(tree_data=json.dumps(res))
    with open('./filter.html', 'w') as f:
        f.write(res)
    display(IFrame('./filter.html', width=width, height=height))


def use_tree_filter(data, path, **kwargs):
    """
    Filters data based on config from tree filter

    :param data: clickstream of events
    :param path: path to aggregation config
    :param kwargs: do nothing
    :return: filtered clickstream
    """
    with open(path) as f:
        tree_filter = json.load(f)

    f = data[data.retention.retention_config['event_col']].isin(tree_filter['filter_names'])
    data = data[f].reset_index(drop=True)
    data['dump_event_col'] = data[data.retention.retention_config['event_col']]
    for agg in tree_filter['agg_list']:
        f = data[data.retention.retention_config['event_col']].str.startswith(agg)
        data[data.retention.retention_config['event_col']] = np.where(f, agg,
                                                                      data[data.retention.retention_config['event_col']]
                                                                      )
    return data
