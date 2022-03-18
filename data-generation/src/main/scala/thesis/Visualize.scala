package thesis

import org.apache.spark.graphx.{Edge, Graph, VertexId}

import java.io.{BufferedWriter, File, FileWriter}

object Visualize {
  // I think a way to visualize the graphs would be convenient now that we don't have Neo4j browser

  /** Converts a Graph to .gexf format and writes it to file
   *
   * @param g        Graph to be visualized
   * @param filename Filename to be written to
   * @tparam VD Vertex type
   * @tparam ED Edge type
   */
  def writeGraphToGephiFormat[VD, ED](g: Graph[VD, ED], filename: String = "myGraph.gexf"): Unit = {
    val edgesToXml = (e: Edge[ED]) => "<edge source=\"" + e.srcId + "\" target=\"" + e.dstId + "\" label=\"" + e.attr + "\" />\n"
    val vertexToXml = (v: (VertexId, VD)) => "<node id=\"" + v._1 + "\" label=\"" + v._2 + "\" />\n"

    val str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(vertexToXml).collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(edgesToXml).collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"
    val pw = new java.io.PrintWriter(filename)
    pw.write(str)
    pw.close()
  }

  /** Function for generating a d3.js representation of the graph. Does not work very well
   *
   * @param g A Graph
   * @tparam VD Vertex type
   * @tparam ED Edge type
   */
  def drawGraph[VD, ED](g: Graph[VD, ED]): Unit = {

    val u = java.util.UUID.randomUUID
    val v = g.vertices.collect.map(_._1)
    val file = new File("graph.html")
    val bw = new BufferedWriter(new FileWriter(file))
    val x =
      """
<html>
<div id='a""" + u +
        """' style='width:960px; height:500px'></div>
<style>
  .node circle { fill: gray; }
  .node text { font: 10px sans-serif;
      text-anchor: middle;
      fill: white; }
  line.link { stroke: gray;
      stroke-width: 1.5px; }
</style>
<script src="https://d3js.org/d3.v3.min.js"></script>
<script>
  var width = 960;
  height = 500;
  var svg = d3.select("#a""" + u +
        """").append("svg").attr("width", width).attr("height", height);
  var nodes = [""" + v.map("{id:" + _ + "}").mkString(",") +
        """];
  var links = [""" + g.edges.collect.map(
        e => "{source:nodes[" + v.indexWhere(_ == e.srcId) +
          "],target:nodes[" +
          v.indexWhere(_ == e.dstId) + "]}").mkString(",") +
        """];
  var link = svg.selectAll(".link").data(links);
  link.enter().insert("line", ".node").attr("class", "link");
  var node = svg.selectAll(".node").data(nodes);
  var nodeEnter = node.enter().append("g").attr("class", "node")
  nodeEnter.append("circle").attr("r", 8);
  nodeEnter.append("text").attr("dy", "0.35em").text(function(d) { return d.id; });
  d3.layout.force().linkDistance(50).charge(-200).chargeDistance(300).friction(0.95).linkStrength(0.5).size([width, height]).on("tick", function() {
    link.attr("x1", function(d) { return d.source.x; })
      .attr("y1", function(d) { return d.source.y; })
      .attr("x2", function(d) { return d.target.x; })
      .attr("y2", function(d) { return d.target.y; });
  node.attr("transform", function(d) {
    return "translate(" + d.x + "," + d.y + ")";
   });
  }).nodes(nodes).links(links).start();
</script>
</html>
 """
    bw.write(x)
    bw.close()
  }

}
