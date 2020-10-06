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
  <script src="https://d3js.org/d3.v5.min.js"></script>
  <script type="text/javascript">

    var node_params;
    var mynodes = [];
    var mylinks = [];


    let maxDegree = 0;
    let maxWeigth = 0;
    let width = 800;
    let height = 800;


    function initialize(initNodes, initNodeParams, initLinks) {{
      mynodes = initNodes;
      node_params = initNodeParams;
      mylinks = initLinks;

      if (!{layout_dump}) {{
        
        let rawNodes = [];

        let delta = 0.1;
        for (i = 0; i < mynodes.length; i++) {{
          let newRawNode = null;

          //fixing correct positions for target nodes
          if (mynodes[i].type == 'bad_node') {{
            newRawNode = {{
              'index': mynodes[i].index,
              'name': mynodes[i].name,
              'type': mynodes[i].type,
              'degree': mynodes[i].degree,
              'fx': width * (1 - delta),
              'fy': height / 2
            }};
          }} else if (mynodes[i].type == 'nice_node') {{
            newRawNode = {{
              'index': mynodes[i].index,
              'name': mynodes[i].name,
              'type': mynodes[i].type,
              'degree': mynodes[i].degree,
              'fx': width * delta,
              'fy': height / 2
            }};
          }} else {{
            //for non-target nodes there x-s and y-s will be added after forceSimulation
            newRawNode = {{
              'index': mynodes[i].index,
              'name': mynodes[i].name,
              'type': mynodes[i].type,
              'degree': mynodes[i].degree
            }};
          }}
          //needed for normalization later
          if (mynodes[i].degree > maxDegree) {{
            maxDegree = mynodes[i].degree;
          }}

          rawNodes.push(newRawNode);
        }}
        
        var layout = d3
          .forceSimulation(rawNodes)
          .tick(5)
          ;


        let maxX = 0,
            maxY = 0,
            minX = 0,
            minY = 0;

        for (let i = 0; i < rawNodes.length; i++) {{
          if (rawNodes[i].type == 'suit_node') {{
            if (rawNodes[i].x > maxX) {{
              maxX = rawNodes[i].x;
            }}
            if (rawNodes[i].x < minX) {{
              minX = rawNodes[i].x;
            }}
            if (rawNodes[i].y > maxY) {{
              maxY = rawNodes[i].y;
            }}
            if (rawNodes[i].y < minY) {{
              minY = rawNodes[i].y;
            }}
          }}
        }}

        let offsetMaxX = -minX + maxX;
        let offsetMaxY = -minY + maxY;

        //Coordinates now have some unpredicted values. I set them so they fill my viewbox with some padding from borders.
        //At first I normalize them, then multiply by width and height.

        for (let i = 0; i < rawNodes.length; i++) {{

          if (rawNodes[i].type == 'suit_node') {{
            //x, y >= 0
            rawNodes[i].x += -minX;
            rawNodes[i].y += -minY;

            //x, y from [0, 1 - 2 * delta]
            rawNodes[i].x = rawNodes[i].x / offsetMaxX * (1 - 2 * delta);
            rawNodes[i].y = rawNodes[i].y / offsetMaxY * (1 - 2 * delta);

            //x, y from [delta, 1 - delta]
            rawNodes[i].x += delta;
            rawNodes[i].y += delta;

            //x, y from [(width | height) * delta, (width | height) * (1 - delta)]

            rawNodes[i].x *= width;
            rawNodes[i].y *= height;

          }}

        }}

        for (let i = 0; i < mynodes.length; i++) {{
          mynodes[i].x = rawNodes[i].x;
          mynodes[i].y = rawNodes[i].y;
        }}

        for (let i = 0; i < mylinks.length;  i++) {{
          mylinks[i].source.x = rawNodes[mylinks[i].source.index].x;
          mylinks[i].source.y = rawNodes[mylinks[i].source.index].y;
          mylinks[i].target.x = rawNodes[mylinks[i].target.index].x;
          mylinks[i].target.y = rawNodes[mylinks[i].target.index].y;
          if (mylinks[i].weight > maxWeigth) {{
            maxWeigth = mylinks[i].weight;
          }}
        }}
      }} else {{
        // if layout_dump was used:
        for (i = 0; i < mynodes.length; i++) {{
          if (mynodes[i].degree > maxDegree) {{
            maxDegree = mynodes[i].degree;
          }}
        }}
        for (let i = 0; i < mylinks.length;  i++) {{
          if (mylinks[i].weight > maxWeigth) {{
            maxWeigth = mylinks[i].weight;
          }}
        }} 
      }}

      
      
      
      makeCheckboxes();
      setLinkThreshold();
      displayingWeights();
    }}

    function drawGraph(nodes, links) {{
      zoom = d3.zoom()
          .scaleExtent([0.5, 8])
          .translateExtent([[0, 0], [width, height]])
          .extent([[0, 0], [width, height]])
          .on("zoom", zoomed)
          ;

      var svg = d3.select("#freakingGraph").append("svg")
        .attr("viewBox", [0, 0, width, height])
        .call(zoom)
        ;

      //I append all elemets to maingroup so zoom works properly
      var maingroup = svg.append('g');

      function zoomed() {{
        maingroup.attr("transform", d3.event.transform);
      }}


      function calcMarkers(d) {{

          let dist = Math.sqrt((nodes[whereEquals(d.target.index)].x - nodes[whereEquals(d.source.index)].x) ** 2 + (nodes[whereEquals(d.target.index)].y - nodes[whereEquals(d.source.index)].y) ** 2);
          if (dist > 0 && dist <= 200){{
              return - Math.sqrt((0.5 - (d.target.degree ) / 2 / dist)) * (d.target.degree) / 2;

          }} else {{
              return 0;
          }}
      }}

      var path = maingroup.append("g").selectAll("path")
          .data(links)
          .enter()
          .append("path")
          .attr("class", function(d) {{ return "link " + d.type; }})
          .attr("stroke-width", function(d) {{ return Math.max(d.weight * 20, 1); }})
          .attr("id", function(d,i) {{ return "link_"+i; }})
          .attr("d", linkArc)
          ;

      let textMarkersSelection = maingroup.append("g").selectAll("text")
          .data(links)
          .enter();

      textMarkersSelection.append("text")
          .style("font-size", "13px")
          .attr("dy", "4.2px")
          .append("textPath")
          .attr("xlink:href", function(d,i) {{ return "#link_"+i; }})
          .attr("startOffset", "35%")
          .text("➤")
          ;

      textMarkersSelection.append("text")
          .style("font-size", "13px")
          .attr("dy", "4.2px")
          .append("textPath")
          .attr("xlink:href", function(d,i) {{ return "#link_"+i; }})
          .attr("startOffset", "65%")
          .text("➤")
          ;

      var edgetext = maingroup.append("g").selectAll("text")
          .data(links)
          .enter().append("text")
          .append("textPath")
          .attr("xlink:href",function(d,i){{return "#link_"+i;}})
          .style("text-anchor","middle")
          .attr("startOffset", "50%")
          .attr("id", function(d,i) {{ return "node_text"+i; }})
          ;

      function whereEquals(index) {{
        for (var i = 0; i < nodes.length; i++) {{
          if (index == nodes[i].index) {{
            return i;
          }}
        }}
      }}


        function roundToSignificantFigures(num, n) {{
            if(num == 0) {{
                return 0;
            }}

            d = Math.ceil(Math.log10(num < 0 ? -num: num));
            power = n - d;

            magnitude = Math.pow(10, power);
            shifted = Math.round(num*magnitude);
            return shifted/magnitude;
        }};



      function displayingWeights() {{
        d3.selectAll("#show-weights").each(function(d) {{
          cb = d3.select(this);
          if (cb.property("checked")) {{
            edgetext = edgetext.text(function(d) {{
                if ($('#show-percents')[0].checked) {{
                    if (d['weight_text'] > 1) {{
                      return d['weight_text']
                    }} else {{
                      return roundToSignificantFigures(d['weight_text'] * 100, 2) + "%";
                    }};
                }} else {{
                    if (d['weight_text'] > 1) {{
                      return d['weight_text']
                    }} else {{
                      return roundToSignificantFigures(d['weight_text'], 2);
                    }};
                }}
            }})
          }} else {{
            edgetext = edgetext.text(function(d) {{ return ; }})
          }}


        }})
      }};



        d3.selectAll("#show-weights").on("change", displayingWeights);
        d3.selectAll("#show-percents").on("change", displayingWeights);

        function dragstarted(d) {{
          d3.select(this).raise().classed("active", true);
        }}

        function dragged(d) {{
          d3.select(this)
          .attr("cx", d.x = d3.event.x)
          .attr("cy", d.y = d3.event.y);
        }}

        function dragended(d) {{

          d3.select(this).classed("active", false);
          path = path.attr("d", linkArc);

          text = text
            .attr('x', function(d) {{ return d.x; }})
            .attr('y', function(d) {{ return d.y; }})
            ;
          defs.attr("refY", function(d) {{ return calcMarkers(d); }});
          defs.append("path")
            .attr("d", "M0,-5L10,0L0,5");
        }};

        var circle = maingroup.append("g").selectAll("circle")
            .data(nodes)
            .enter().append("circle")
            .attr("class", function(d) {{ return "circle " + d.type; }})
            .attr("r", function(d) {{ return d.degree; }})
            .attr('cx', function(d) {{ return d.x; }})
            .attr('cy', function(d) {{ return d.y; }})
            .style("cursor", "default")
            .call(d3.drag()
                .on("start", dragstarted)
                .on("drag", dragged)
                .on("end", dragended));


        var text = maingroup.append("g").selectAll("text")
          .data(nodes)
          .enter().append("text")
          .attr('x', function(d) {{ return d.x; }})
          .attr('y', function(d) {{ return d.y; }})
          .attr('id', function(d) {{ return "node-name" + d.index }})
          .attr('class', 'node-name')
          .text(function(d) {{ return d.name; }})
          ;

        function linkArc(d) {{
          var dx = nodes[whereEquals(d.target.index)].x - nodes[whereEquals(d.source.index)].x,
              dy = nodes[whereEquals(d.target.index)].y - nodes[whereEquals(d.source.index)].y,
              dr = dx * dx + dy * dy;
              dr = Math.sqrt(dr);
            if (dr > 200) {{
              dr *= 5
            }} else {{
              dr /= 2
            }};
            if (dr > 0) {{
              return "M" + nodes[whereEquals(d.source.index)].x + "," + nodes[whereEquals(d.source.index)].y + "A" + (dr * 1.1) + "," + (dr * 1.1) + " 0 0,1 " + nodes[whereEquals(d.target.index)].x + "," + nodes[whereEquals(d.target.index)].y;
            }}
            else {{
              minRadius = 24;
              radius = Math.max(minRadius, nodes[whereEquals(d.source.index)].degree);
              return "M" + nodes[whereEquals(d.source.index)].x + "," + nodes[whereEquals(d.source.index)].y + "A" + radius + "," + radius + " 0 1,0 " + (nodes[whereEquals(d.target.index)].x + 0.1) + "," + (nodes[whereEquals(d.target.index)].y + 0.1);
            }}
        }}

        //synch with 'show names' and 'show weights' checkboxes
        displayingWeights();
        changeNamesVisibility(document.getElementById("show-names").checked)
    }}



    function changeLabel(curinput) {{
      document.getElementById('label' + curinput.id.substring(4)).innerHTML = curinput.value;
      mynodes[whereEquals1(curinput.id.substring(4))].name = curinput.value;
      $(curinput).attr('size', curinput.value.length + 2)
    }};

    function whereEquals1(index) {{
      for (var i = 0; i < mynodes.length; i++) {{
        if (index == mynodes[i].index) {{
          return i;
        }}
      }}
    }}

    function makeCheckboxes() {{

      for (var i = 0; i < mynodes.length; i++) {{

        var newDiv = document.createElement('div');
        newDiv.id = '#checkdiv' + mynodes[i].index;
        $( '#check-boxes' ).append(newDiv);

        var newCheckbox = document.createElement('input');
        newCheckbox.type = 'checkbox';
        newCheckbox.id = 'checkbox' + mynodes[i].index;
        newCheckbox.checked = true;
        newCheckbox.className = 'checkbox-class node-checkbox';
        $( newDiv ).append(newCheckbox);

        var newNameInput = document.createElement('input');
        newNameInput.id = 'name-input' + mynodes[i].index;
        newNameInput.type = 'text';
        newNameInput.value = mynodes[i].name;
        $( newNameInput ).attr('size', newNameInput.value.length + 3);

        $( newDiv ).append(newNameInput);
        $( newNameInput ).on('keypress', updateName);

      }}
    }}

    function updateName() {{

      document.getElementById('node-name' + this.id.substring(10)).innerHTML = this.value;
      mynodes[this.id.substring(10)].name = this.value;
      this.size = this.value.length + 3;

    }}

    function getCorrectLinks(newIdx) {{
      var newLinks = [];
      for (var i = 0; i < mylinks.length; i++) {{
        if (newIdx.includes(mylinks[i].source.index) && newIdx.includes(mylinks[i].target.index)) {{
          newLinks.push(mylinks[i]);
        }}
      }}
      return newLinks;
    }}
    function clearSVG() {{
      $( 'svg' ).remove();
      $( '.node-edit' ).each(function() {{
        this.remove();
      }});
    }}
    function changeNodes() {{
      var newNodes = [];
      var newIdx = [];

      $( '.node-checkbox' ).each(function(i, obj) {{

        if (this.checked) {{
          newNodes.push(mynodes[i]);
          newIdx.push(mynodes[i].index);
        }}

      }});

      var newLinks = getCorrectLinks(newIdx)


      clearSVG();
      drawGraph(newNodes, newLinks);
    }}

    function setLinkThreshold () {{
      let idxInLinks = new Array(mynodes.length).fill(false);
      let newLinks = [];

      let thresholdValue = $('#threshold-link-range').val();
      let blockDeleteTargets = $('#block-targets')[0].checked;

      for (let i = 0; i < mylinks.length; i++) {{
        if (mylinks[i].target.type == 'nice_node' || mylinks[i].source.type == 'nice_node' || mylinks[i].target.type == 'bad_node' || mylinks[i].source.type == 'bad_node') {{
          if (blockDeleteTargets) {{

            newLinks.push(mylinks[i]);
            idxInLinks[mylinks[i].target.index] = true;
            idxInLinks[mylinks[i].source.index] = true;
          }} else if (mylinks[i].weight * maxWeigth >= thresholdValue) {{
            newLinks.push(mylinks[i]);
            idxInLinks[mylinks[i].target.index] = true;
            idxInLinks[mylinks[i].source.index] = true;
          }}
        }} else if (mylinks[i].weight * maxWeigth >= thresholdValue) {{
          newLinks.push(mylinks[i]);
          idxInLinks[mylinks[i].target.index] = true;
          idxInLinks[mylinks[i].source.index] = true;
        }}
      }}

      let newNodes = [];
      for (let i = 0; i < mynodes.length; i++) {{
        if (idxInLinks[i]) {{
          newNodes.push(mynodes[i]);
          $('#checkbox' + mynodes[i].index).prop('checked', true);
        }} else {{
          $('#checkbox' + mynodes[i].index).prop('checked', false);
        }}
      }}
      clearSVG();
      drawGraph(newNodes, newLinks);
    }}

    function updateLinkThresholdText(val) {{
      document.getElementById('threshold-link-text').innerHTML = val;
    }}

    function setNodeThreshold() {{
      var newNodes = [];
      var newIdx = [];

      let thresholdValue = $('#threshold-node-range').val();
      let blockDeleteTargets = $('#block-targets')[0].checked;

      for (let i = 0; i < mynodes.length; i++) {{
        if (mynodes[i].type == 'bad_node' || mynodes[i].type == 'nice_node') {{
          if (blockDeleteTargets) {{
            newNodes.push(mynodes[i]);
            newIdx.push(mynodes[i].index);
            $('#checkbox' + mynodes[i].index).prop('checked', true);
          }} else if (mynodes[i].degree >= $('#threshold-node-range').val() * maxDegree) {{
            newNodes.push(mynodes[i]);
            newIdx.push(mynodes[i].index);
            $('#checkbox' + mynodes[i].index).prop('checked', true);
          }} else {{
            $('#checkbox' + mynodes[i].index).prop('checked', false);
          }}
        }} else if (mynodes[i].degree >= $('#threshold-node-range').val() * maxDegree) {{
          newNodes.push(mynodes[i]);
          newIdx.push(mynodes[i].index);
          $('#checkbox' + mynodes[i].index).prop('checked', true);
        }} else {{
          $('#checkbox' + mynodes[i].index).prop('checked', false);
        }}
      }}
      var newLinks = getCorrectLinks(newIdx);

      clearSVG();
      drawGraph(newNodes, newLinks);
    }}

    function updateNodeThresholdText(val) {{
      document.getElementById('threshold-node-text').innerHTML = val;
    }}


    function changeNamesVisibility(isHidden) {{

      if (isHidden) {{
        $('.node-name').each(
          function() {{
            $(this).show();
          }});
      }} else {{
        $('.node-name').each(
          function() {{
            $(this).hide();
          }});
      }}
    }}

    function downloadLayout() {{
        var a = document.createElement("a");
        var file = new Blob([JSON.stringify(mynodes)], {{type: "text/json;charset=utf-8"}});
        a.href = URL.createObjectURL(file);
        a.download = "node_params.json";
        console.log(1);
        a.click();

    }}

  </script>

  <style type="text/css">
      watermark {{
        width: 100%;
      }}
      watermark h3 {{
        width: 100%;
        text-align: center;
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
  <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">
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
              <input type="button" value="Set threshold" onclick="setNodeThreshold()">
            </div>
            <br>
            <div>
              <h6>Links Threshold</h6>
              <input id="threshold-link-range" name="threshold" type="range" min="0" max="1" step="0.01" value={thresh}
              oninput="updateLinkThresholdText(this.value*{scale})" onchange="updateLinkThresholdText(this.value*{scale})">
              <label id="threshold-link-text">{thresh}</label>
              <input type="button" value="Set threshold" onclick="setLinkThreshold()">
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
            <div id="option">
              <input name="downloadButton"
              type="button"
              value="download"
              onclick="downloadLayout()" />
            </div>
          </div>
      </div>




  </main>

  <script src="https://api.retentioneering.com/files/d3.v4.min.js"></script>

  <script type="text/javascript">

    updateLinkThresholdText({thresh}*{scale});
    initialize({nodes}, {node_params}, {links});

    if (!{show_percent}) {{
      $('.percent-checkbox').hide();
    }}

  </script>

  <script src="https://code.jquery.com/jquery-3.3.1.slim.min.js" integrity="sha384-q8i/X+965DzO0rT7abK41JStQIAqVgRVzpbzo5smXKp4YfRvH+8abtTE1Pi6jizo" crossorigin="anonymous"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.7/umd/popper.min.js" integrity="sha384-UO2eT0CpHqdSJQ6hJty5KVphtPhzWj9WO1clHTMGa3JDZwrnQq4sF86dIHNDz0W1" crossorigin="anonymous"></script>
  <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js" integrity="sha384-JjSmVgyd0p3pXB1rRibZUAYoIIy6OrQ6VrjIEaFf/nJGzIxFDsf4x0xIM+B07jRM" crossorigin="anonymous"></script>
  <script src="https://code.jquery.com/jquery-3.4.1.js"></script>
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
