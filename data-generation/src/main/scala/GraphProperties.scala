import breeze.linalg.linspace
import breeze.plot.{Figure, plot}
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths

import scala.reflect.ClassTag

object GraphProperties {


  //def getPowerLawExponent

  /** Visualize the power law for node outdegreenees
   * Saves line diagram to file and opens it in a viewer
   *
   * @param graph Graph
   * @tparam VD VertexType
   * @tparam ED EdgeType
   */
  def visPowerLaw[VD, ED](graph: Graph[VD, ED], graphName:String = "Graph", filename: String = "lines.png"): Unit = {
    val g = graph.ops
    val f = Figure()
    val p = f.subplot(0)

    val num_vertice = g.numVertices.toDouble
    val n = linspace(0, num_vertice, num_vertice.toInt)
    val out: Array[Double] = g.outDegrees.values.collect().sorted.map(_.toDouble)
    p += plot(n, out)
    p.xlabel = s"x axis - $graphName"
    p.ylabel = "y axis"
    f.saveas(filename)
  }

  /** Get diameter of graph (sortof)
   * GOTCHAS:
   * 1. This method respects edge direction. In the general sense of diameter this should be ignored.
   * 2. This scales exponentially or something like that. Get shortest path between all vertices and then return max path length
   *
   * @param graph Graph
   * @tparam VD VertexType
   * @tparam ED EdgeType
   * @return diameter of graph
   */
  def shortestPath[VD, ED: ClassTag](graph: Graph[VD, ED]): Int = {
    val vertex_ids = graph.vertices.map(_._1).collect()
    ShortestPaths.run(graph, vertex_ids).vertices.mapValues(_.values.max).values.max()
  }

}
