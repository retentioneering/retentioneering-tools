# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


__RENDER_INNER_IFRAME__ = """
<iframe id="{id}" src="about:blank" width="{width}" height="{height}" onload="window.reteLoadedIframes =  window.reteLoadedIframes || []; window.reteLoadedIframes.push(`{id}`)">
</iframe>
<script>
   (function() {{
      console.log('init graph iframe')
      const iframe = document.getElementById(`{id}`)
      const iframeLoaded = window.reteLoadedIframes && window.reteLoadedIframes.includes(`{id}`)

      const init = () => {{
        console.log('graph iframe loaded')
        const iframeDocument = document.getElementById(`{id}`).contentDocument
        iframeDocument.body.innerHTML = `{graph_body}`
        const styles = iframeDocument.createElement("style")
        styles.innerHTML = `{graph_styles}`


        const graphScript = iframeDocument.createElement("script")
        graphScript.src = `{graph_script_src}`

        graphScript.addEventListener("load", () => {{
          console.log('graph lib loaded')
          const initGraph = iframeDocument.createElement("script")
          initGraph.innerHTML = `{init_graph_js}`

          iframeDocument.body.appendChild(initGraph)
        }})

        iframeDocument.head.appendChild(styles)
        iframeDocument.head.appendChild(graphScript)

        iframeDocument.body.dataset.templateId = '{id}_template'
        console.log('init graph iframe end')
      }}

      console.log(`iframe loaded: ` + iframeLoaded)

      if (iframeLoaded) {{
        init()
        return
      }}
      iframe.onload = () => {{
        init()
        window.reteLoadedIframes.push(`{id}`)
      }}
   }})()
</script>
<template id="{id}_template">
  {template}
</template>
"""

__GRAPH_STYLES__ = """
    .svg-watermark {{
      width: 100%;
      font-size: 80px;
      fill: #c2c2c2;
      opacity: 0.3;
      font-family: Arial;
    }}

    .link {{
      fill: none;
      stroke: #666;
      stroke-opacity: 0.7;
    }}

    text {{
      font: 12px sans-serif;
      pointer-events: none;
    }}

    circle {{
      fill: #ccc;
      stroke: #333;
      stroke-width: 1.5px;
    }}

    .selected-node {{
      stroke: blue;
      stroke-width: 3px;
    }}

    .circle.source_node {{
      fill: #f3f310;
    }}

    .circle.nice_node {{
      fill: green;
    }}

    .circle.bad_node {{
      fill: red;
    }}

    .link.source {{
      stroke: #f3f310;
    }}
    .link.nice {{
      stroke: green;
    }}
    .link.bad {{
      stroke: red;
    }}

"""

__GRAPH_BODY__ = """
  <div id="root"></div>
"""

__INIT_GRAPH__ = """
    initialize({{
      serverId: {server_id},
      env: {env},
      configNodes: {nodes},
      configLinks: {links},
      nodesColsNames: {node_cols_names},
      linksWeightsNames: {links_weights_names},
      nodesThreshold: {nodes_threshold},
      linksThreshold: {links_threshold},
      showWeights: {show_weights},
      showPercents: {show_percents},
      showNodesNames: {show_nodes_names},
      showAllEdgesForTargets: {show_all_edges_for_targets},
      showNodesWithoutLinks: {show_nodes_without_links},
      useLayoutDump: Boolean({layout_dump}),
      weightTemplate: {weight_template},
    }})
"""


__FULL_HTML__ = """
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Rete graph</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
  </head>
  <body>
    {content}
  </body>
  </html>
"""
