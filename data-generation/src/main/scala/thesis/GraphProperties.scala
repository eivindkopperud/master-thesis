package thesis

import breeze.linalg.linspace
import breeze.plot.{Figure, plot}
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths

import scala.reflect.ClassTag

object GraphProperties {



  //TODO def getPowerLawExponent

  /** Visualize the power law for node outdegreenees
   * Saves line diagram to file and opens it in a viewer
   *
   * @param graph Graph
   * @tparam VD VertexType
   * @tparam ED EdgeType
   */
  def visPowerLaw[VD, ED](graph: Graph[VD, ED], graphName: String = "Graph", filename: String = "lines.png"): Unit = {
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
   * ClassTag is there so that the type ED is not gone at runtime. So called Type Erasure, that happens with the JVM
   *
   * @param graph Graph
   * @tparam VD VertexType
   * @tparam ED EdgeType
   * @return diameter of graph
   */
  def getDirectedDiameterOfGraph[VD, ED: ClassTag](graph: Graph[VD, ED]): Int = {
    val vertex_ids = graph.vertices.map(_._1).collect()
    ShortestPaths.run(graph, vertex_ids).vertices.mapValues(_.values.max).values.max()
  }

  case class GraphStats(order: Long, size: Long, average_outdegree: Double)

  /** ToString for GraphStats
   *
   * But I haven't learned idiomatic Scala yet. Think this should be done differently
   *
   * @param g GraphStats object
   * @return Table format as a string
   */
  def graphStatsToString(g: GraphStats): String = {
    s"""description\tvalue
       |order\t${g.order}
       |size\t${g.size}
       |Average outdegree\t${g.average_outdegree}
    """.stripMargin
  }

  /** Print general stats about graph
   *
   * TODO find a better datastructure, not very extensible right now
   * TODO Add more stats (PowerLaw exponent etc)
   *
   * @param graph Graph
   * @tparam VD VertexType
   * @tparam ED EdgeType
   */
  def printStats[VD, ED](graph: Graph[VD, ED]): Unit = {
    val graphStats = GraphStats(
      graph.ops.numVertices,
      graph.ops.numEdges,
      graph.ops.outDegrees.values.sum() / graph.ops.numEdges)
    println(graphStatsToString(graphStats))
  }


}
