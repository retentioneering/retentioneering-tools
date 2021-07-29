# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


__VEGA_TEMPLATE__ = """
<html>
<head>
  <meta charset="UTF-8"
  <script src="https://api.retentioneering.com/files/d3.v4.min.js?a={func_name}"></script>
  <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/vega@5"></script>
  <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/vega-lite@4"></script>
  <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/vega-embed@3.4"></script>

</head>
<body>
  <div id="visual-object-view"></div>

  <script type="text/javascript">
    var visualObject = {visual_object};

    vegaEmbed('#visual-object-view', visualObject);
  </script>
</body>
</html>
"""

__TEMPLATE__ = """
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Graph Editor</title>
  <script src="https://code.jquery.com/jquery-3.4.1.js"></script>
  <script src="https://api.retentioneering.com/files/d3.v4.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.7/umd/popper.min.js" integrity="sha384-UO2eT0CpHqdSJQ6hJty5KVphtPhzWj9WO1clHTMGa3JDZwrnQq4sF86dIHNDz0W1" crossorigin="anonymous"></script>
  <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js" integrity="sha384-JjSmVgyd0p3pXB1rRibZUAYoIIy6OrQ6VrjIEaFf/nJGzIxFDsf4x0xIM+B07jRM" crossorigin="anonymous"></script>
  <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">
  <script src="https://static.server.retentioneering.com/viztools/draw-graph.min.js" type="text/javascript"></script>
  <style type="text/css">
      .download {{
        display: flex;
        align-items: center;
      }}

      .download__btn {{
        margin-right: 16px;
      }}

      .download__link {{
        color: inherit !important;
      }}


      .watermark {{
        width: 100%;
      }}
      .watermark h3 {{
        width: 100%;
        text-align: center;
      }}

      .svg-watermark {{
        width: 100%;
        font-size: 80px;
        fill: #c2c2c2;
        opacity: 0.3;
        font-family: Arial;
      }}

      html {{
        font-size: 10px;
      }}

      circle {{
        fill: #ccc;
        stroke: #333;
        stroke-width: 1.5px;
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

      .link {{
        fill: none;
        stroke: #666;
        stroke-opacity: 0.7;
      }}

      #nice_target {{
        fill: green;
      }}

      .link.nice_target {{
        stroke: green;
      }}

      #source {{
        fill: yellow;
      }}

      .link.source {{
        stroke: #f3f310;
      }}

      .link.positive {{
        stroke: green;
      }}

      .link.negative {{
        stroke: red;
      }}

      #source {{
        fill: orange;
      }}

      .link.source1 {{
        stroke: orange;
      }}

      #bad_target {{
        fill: red;
      }}

      .link.bad_target {{
        stroke: red;
      }}
      text {{
        font: 12px sans-serif;
        pointer-events: none;
      }}

      main li {{
        display: inline;
      }}
      .graphlist {{
        list-style-type: none;

      }}
      .graphloader {{
        margin-top: 5%;
        margin-bottom: 5%;
      }}
      .graphloader input {{
        margin: auto;
      }}

      h1 {{
        text-align: center;
      }}

      .bottom-checkbox {{
        margin-right: 5%;
        display: inline;
      }}

      .checkbox-class {{
        margin-right: 3px;
      }}

      .node-edit {{
        position: relative;
        font-size: 12px;
        border: none;
        background-color: rgba(1,1,1,0);
      }}

      .node-edit:focus {{
        background-color: #ddd;
      }}

      #option {{
        margin-left: 5px;
      }}

      #freakingGraph {{
        border: solid 2px black;
        /*position: relative;*/
      }}

      .container {{
        margin: 0!important;
        padding-right: 0!important;
        max-width: 1200px!important;
      }}
      .col-8 {{
        padding: 0px 4px 0px 2px!important;
      }}
      .col-4 {{
        padding-right: 0px!important;
      }}
      @media (max-width: 576px) {{
        form label {{
          font-size: 10px;
        }}
      }}


      @media (max-width: 768px) {{
        form label {{
          font-size: 0.8rem;
        }}
      }}


      @media (max-width: 992px) {{
        form label {{
          font-size: 1rem;
        }}
      }}

      @media (max-width: 1200px) {{
        form label {{
          font-size: 1rem;
        }}
      }}

      @media (min-width: 1201px) {{
        form label {{
          font-size: 1.4rem;
        }}
      }}

  </style>
</head>
<body>



  <main>

    <div class="container">

        <div class="row">
          <div class="watermark" style="z-index: 1010; background-color: #FFF; width: 100%">
            <h3>Retentioneering</h3>
          </div>
          <div class="col-8">

            <div id="freakingGraph" style="z-index: 1000">
              <!-- graph will be appended here -->
            </div>
          </div>
          <div class="col-4" style="z-index: 1010; background-color: #FFF">
            <form>
              <div id="check-boxes">

              </div>
              <br>
              <input name="submit" value="Update nodes" style="width: 80%;" type="button" onclick="changeNodes()">

            </form>

            <br>
            <br>
            <div style="z-index: 1010; background-color: #FFF">
              <h6>Nodes Threshold</h6>
              <input id="threshold-node-range" name="threshold-node" type="range" min="0" max="1" step="0.01" value="0.05"
              oninput="updateNodeThresholdText(this.value)" onchange="updateNodeThresholdText(this.value)">
              <label id="threshold-node-text">0.05</label>
            </div>
            <br>
            <div>
              <h6>Links Threshold</h6>
              <input id="threshold-link-range" name="threshold" type="range" min="0" max="1" step="0.01" value={thresh}
              oninput="updateLinkThresholdText(this.value*{scale})" onchange="updateLinkThresholdText(this.value*{scale})">
              <label id="threshold-link-text">{thresh}</label>
            </div>
            <div>
              <input type="button" value="Set thresholds" onclick="setThresholds()">
            </div>
          </div>




          <div class="col-12" style="z-index: 1010; background-color: #FFF">

            <div class="weight-checkbox bottom-checkbox">
              <input type="checkbox" class="checkbox checkbox-class" checked value="weighted" id="show-weights"><label> Show weights </label>
            </div>

            <div class="percent-checkbox bottom-checkbox">
              <input type="checkbox" class="checkbox checkbox-class" checked id="show-percents"><label> Percents </label>
            </div>

            <div class="bottom-checkbox">
              <input type="checkbox" class="checkbox checkbox-class" checked id="show-names" onchange="changeNamesVisibility(this.checked)"><label> Show nodes names</label>
            </div>

            <div class="bottom-checkbox">
              <input type="checkbox" class="checkbox checkbox-class" id="block-targets" onchange="setLinkThreshold ()"><label> Show all edges for targets </label>
            </div>
            <div class="download">
              <div id="option" class="download__btn">
                <input name="downloadButton"
                type="button"
                value="download"
                onclick="downloadLayout()" />
              </div>
              <div class="download__btn">
                <button type="button" onclick="downloadSVG('svg', 'graph')">
                  download SVG
                </button>
              </div>
              <div class="download__btn">
                <button type="button" onclick="downloadPNG('svg', 'graph', 4)">
                  download PNG
                </button>
              </div>
            </div>
          </div>
      </div>




  </main>
  <script type="text/javascript">
    updateLinkThresholdText({thresh}*{scale});
    initialize({nodes}, {node_params}, {links}, {layout_dump});

    if (!{show_percent}) {{
      $('.percent-checkbox').hide();
    }}
  </script>
</body>
</html>
"""

__OLD_TEMPLATE__ = """
<!DOCTYPE html>
<meta charset="utf-8">
<style>
                circle {{
                  fill: #ccc;
                  stroke: #333;
                  stroke-width: 1.5px;
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
                .link {{
                  fill: none;
                  stroke: #666;
                  stroke-opacity: 0.7;
                }}
                #nice_target {{
                  fill: green;
                }}
                .link.nice_target {{
                  stroke: green;
                }}
                #source {{
                  fill: yellow;
                }}
                .link.source {{
                  stroke: #f3f310;
                }}

                .link.positive {{
                  stroke: green;
                }}

                .link.negative {{
                  stroke: red;
                }}
                #source {{
                  fill: orange;
                }}
                .link.source1 {{
                  stroke: orange;
                }}
                #bad_target {{
                  fill: red;
                }}
                .link.bad_target {{
                  stroke: red;
                }}
                text {{
                  font: 12px sans-serif;
                  pointer-events: none;
                }}
</style>
<body>
<script src="https://api.retentioneering.com/files/d3.v4.min.js"></script>
<div>
  <input type="checkbox" class="checkbox" value="weighted"><label> Show weights </label>
</div>
<div id="option">
    <input name="downloadButton"
           type="button"
           value="download"
           onclick="downloadLayout()" />
</div>
<script>
var links = {links};
var node_params = {node_params};
var nodes = {nodes};
var width = {width},
    height = {height};
var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height);
let defs = svg.append("g").selectAll("marker")
    .data(links)
  .enter().append("marker")
    .attr("id", function(d) {{ return d.source.index + '-' + d.target.index; }})
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", function(d) {{
        if (d.target.name !== d.source.name) {{
            return 7 + d.target.degree;
        }} else {{
            return 0;
        }}
    }})
    .attr("refY", calcMarkers)
    .attr("markerWidth", 10)
    .attr("markerHeight", 10)
    .attr("markerUnits", "userSpaceOnUse")
    .attr("orient", "auto");
defs.append("path")
    .attr("d", "M0,-5L10,0L0,5");
function calcMarkers(d) {{
    let dist = Math.sqrt((nodes[d.target.index].x - nodes[d.source.index].x) ** 2 + (nodes[d.target.index].y - nodes[d.source.index].y) ** 2);
    if (dist > 0 && dist <= 200){{
        return - Math.sqrt((0.5 - (d.target.degree ) / 2 / dist)) * (d.target.degree) / 2;
    }} else {{
        return 0;
    }}
}}
var path = svg.append("g").selectAll("path")
    .data(links)
  .enter().append("path")
    .attr("class", function(d) {{ return "link " + d.type; }})
    .attr("stroke-width", function(d) {{ return Math.max(d.weight * 20, 1); }})
    .attr("marker-end", function(d) {{ return "url(#" + d.source.index + '-' + d.target.index + ")"; }})
    .attr("id", function(d,i) {{ return "link_"+i; }})
    .attr("d", linkArc)
    ;
var edgetext = svg.append("g").selectAll("text")
    .data(links)
   .enter().append("text")
   .append("textPath")
    .attr("xlink:href",function(d,i){{return "#link_"+i;}})
    .style("text-anchor","middle")
    .attr("startOffset", "50%")
    ;

function update() {{
    d3.selectAll(".checkbox").each(function(d) {{
        cb = d3.select(this);
        if (cb.property("checked")) {{
            edgetext = edgetext.text(function(d) {{
                if ({show_percent}) {{
                    return Math.round(d.weight_text * 100) / 100;
                }} else {{
                    return Math.round(d.weight_text * 100) + "%";
                }}
            }})
        }} else {{
            edgetext = edgetext.text(function(d) {{ return ; }})
        }}
    }})
}};
d3.selectAll(".checkbox").on("change",update);
function dragstarted(d) {{
  d3.select(this).raise().classed("active", true);
}}
function dragged(d) {{
  d3.select(this).attr("cx", d.x = d3.event.x).attr("cy", d.y = d3.event.y);
}}
function dragended(d) {{
  d3.select(this).classed("active", false);
  path = path.attr("d", linkArc);
  text = text
        .attr('x', function(d) {{ return d.x; }})
        .attr('y', function(d) {{ return d.y; }})
        ;
  defs = defs.attr("refY", calcMarkers);
  defs.append("path")
    .attr("d", "M0,-5L10,0L0,5");
}};
var circle = svg.append("g").selectAll("circle")
    .data(nodes)
  .enter().append("circle")
    .attr("class", function(d) {{ return "circle " + d.type; }})
    .attr("r", function(d) {{ return d.degree; }})
    .attr('cx', function(d) {{ return d.x; }})
    .attr('cy', function(d) {{ return d.y; }})
    .call(d3.drag()
        .on("start", dragstarted)
        .on("drag", dragged)
        .on("end", dragended));
var text = svg.append("g").selectAll("text")
    .data(nodes)
  .enter().append("text")
    .attr('x', function(d) {{ return d.x; }})
    .attr('y', function(d) {{ return d.y; }})
    .text(function(d) {{ return d.name; }});
function linkArc(d) {{
  var dx = nodes[d.target.index].x - nodes[d.source.index].x,
      dy = nodes[d.target.index].y - nodes[d.source.index].y,
      dr = dx * dx + dy * dy;
      dr = Math.sqrt(dr);
      if (dr > 200) {{
        dr *= 5
      }} else {{
        dr /= 2
      }};
      if (dr > 0) {{return "M" + nodes[d.source.index].x + "," + nodes[d.source.index].y + "A" + (dr * 1.1) + "," + (dr * 1.1) + " 0 0,1 " + nodes[d.target.index].x + "," + nodes[d.target.index].y;}}
      else {{return "M" + nodes[d.source.index].x + "," + nodes[d.source.index].y + "A" + 20 + "," + 20 + " 0 1,0 " + (nodes[d.target.index].x + 0.1) + "," + (nodes[d.target.index].y + 0.1);}}
}}
function downloadLayout() {{
    var a = document.createElement("a");
    var file = new Blob([JSON.stringify(nodes)], {{type: "text/json;charset=utf-8"}});
    a.href = URL.createObjectURL(file);
    a.download = "node_params.json";
    a.click();
}}
</script>

"""

__SANDBOX_TEMPLATE__ = """
<html>
<head>
  <title>Fuck me</title>
  <script src="https://d3js.org/d3.v5.min.js"></script>
</head>
<body>
  <div id="graph" style="width: 80%;"></div>
  <script type="text/javascript">
      let height = 600;
      let width = 600;
      data = func()
      const svg = d3.select("#graph").append("svg")
          .attr("viewBox", [0, 0, width, height]);

      const circle = svg.selectAll("circle")
        .data(data)
        .join("circle")
          .attr("transform", d => `translate(${d})`)
          .attr("r", 1.5);

      svg.call(d3.zoom()
          .extent([[0, 0], [width, height]])
          .scaleExtent([1, 8])
          .on("zoom", zoomed));

      function zoomed() {
        const {transform} = d3.event;
        //console.log(transform);
        //console.log(d3.event);
        circle.attr("transform", d => `translate(${transform.apply(d)})`);
      }





    function func(){
      const randomX = d3.randomNormal(width / 2, 80);
      const randomY = d3.randomNormal(height / 2, 80);
      return Array.from({length: 2000}, () => [randomX(), randomY()]);
    }

  </script>
</body>
</html>

"""
