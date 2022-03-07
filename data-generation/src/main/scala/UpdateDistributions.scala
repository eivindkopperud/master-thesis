import DistributionType.DistributionType
import UpdateDistributionMode.UpdateDistributionMode
import breeze.plot.{Figure, hist}
import breeze.stats.distributions.{Gaussian, LogNormal}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}

import scala.reflect.ClassTag

object UpdateDistributionMode extends Enumeration {
  type UpdateDistributionMode = Value
  val Uniform, PositiveCorrelation, NegativeCorrelation = Value
}

object DistributionType extends Enumeration {
  type DistributionType = Value
  val LogNormal, Gaussian = Value
}

object UpdateDistributions {

  /** Add an update count to all nodes
   *
   * @param sc           A SparkContext
   * @param graph        A graph
   * @param mode         Mode for distributing the weights based on vertex degree
   * @param distribution Some probability distribution
   * @param mu           Expected value
   * @param sigma        Standard deviation
   * @return */
  def addVertexUpdateDistribution[VD, ED: ClassTag](sc: SparkContext, graph: Graph[VD, ED], mode: UpdateDistributionMode, distribution: DistributionType, mu: Int, sigma: Double): Graph[Int, ED] = {
    val graphWithDegree = graph
      .vertices
      .zip(graph.ops.degrees)
      .map(vertex => (vertex._1._1, vertex._2._2))
      .collect()
      .sortBy(vertexTuple => vertexTuple)(getVertexSorting(mode))

    val updates = graph.mapVertices((_, _) => getDistributionDraw(distribution, mu, sigma))
      .vertices.collect()
      .sortBy(_._2)
      .map(_._2)

    val rdd = sc.parallelize(
      graphWithDegree
        .zip(updates)
        .map(vertex => (vertex._1._1, vertex._2))
    )

    Graph[Int, ED](rdd, graph.edges)
  }

  /** Get a vertex ordering rule based on a distribution mode
   *
   * @param mode Indicates the mode of which the nodes are ordered by
   * @return A Ordering function
   * */
  private def getVertexSorting(mode: UpdateDistributionMode): Ordering[(VertexId, Int)] = {
    mode match {
      case UpdateDistributionMode.Uniform => (x: (VertexId, Int), y: (VertexId, Int)) => x._1 compareTo y._1
      case UpdateDistributionMode.PositiveCorrelation => (x: (VertexId, Int), y: (VertexId, Int)) => x._2 compareTo y._2
      case UpdateDistributionMode.NegativeCorrelation => (x: (VertexId, Int), y: (VertexId, Int)) => y._2 compareTo x._2
    }
  }

  /** Add an update count to all edges
   *
   * @param sc           A SparkContext
   * @param graph        A graph where vertices have a update count as a Int
   * @param mode         Mode for distributing the weights based on vertex degree
   * @param distribution Some probability distribution
   * @param mu           Expected value
   * @param sigma        Standard deviation
   * @return */
  def addEdgeUpdateDistribution[VD, ED: ClassTag](sc: SparkContext, graph: Graph[Int, ED], mode: UpdateDistributionMode, distribution: DistributionType, mu: Int, sigma: Double): Graph[Int, Int] = {
    val vertexUpdateHashMap = graph
      .vertices
      .collect()
      .toMap

    val graphWithDegree = graph
      .mapEdges(edge => getVertexUpdateSum[ED](edge, vertexUpdateHashMap))
      .edges
      .collect()
      .sortBy(edge => edge)(getEdgeSorting(mode))

    val updates = graph
      .mapEdges(_ => getDistributionDraw(distribution, mu, sigma))
      .edges
      .collect()
      .sortBy(edge => edge.attr)
      .map(edge => edge.attr)

    val rdd = sc.parallelize(
      graphWithDegree
        .zip(updates)
        .map(edgeTuple => Edge(edgeTuple._1.srcId, edgeTuple._1.dstId, edgeTuple._2))
    )
    Graph[Int, Int](graph.vertices, rdd)
  }

  /**
   *
   * @param edge
   * @param hashMap Hash map with vertices as keys and update counts as values
   * @return The sum of updates of the edge's connected vertices
   */
  private def getVertexUpdateSum[ED](edge: Edge[ED], hashMap: Map[VertexId, Int]): Int = {
    hashMap.getOrElse(edge.srcId, 0) + hashMap.getOrElse(edge.dstId, 0)
  }

  private def getEdgeSorting(mode: UpdateDistributionMode): Ordering[Edge[Int]] = {
    mode match {
      case UpdateDistributionMode.Uniform => (x: Edge[_], y: Edge[_]) => x.hashCode() compareTo y.hashCode()
      case UpdateDistributionMode.PositiveCorrelation => (x: Edge[Int], y: Edge[Int]) => x.attr compareTo y.attr
      case UpdateDistributionMode.NegativeCorrelation => (x: Edge[Int], y: Edge[Int]) => y.attr compareTo x.attr
    }
  }

  /** Draw a number from a probability distribution
   *
   * @param distribution The type of probability distribution
   * @param mu           Expected value
   * @param sigma        Standard deviation
   * @return A value in the distribution
   *         TODO: Make the input parameter agnostic - not only mu and sigma
   * */
  private def getDistributionDraw(distribution: DistributionType, mu: Int, sigma: Double): Int = {
    distribution match {
      case DistributionType.LogNormal => LogNormal(mu, sigma).draw().toInt
      case DistributionType.Gaussian => Gaussian(mu, sigma).draw().toInt
    }
  }


  /** Add log normal distributed weights as the property for vertices and edges
   *
   * Idk why this isn't just one function.
   * TODO make a general function where the distribution can be inserted
   *
   * @param sc    A SparkContext
   * @param graph A graph
   * @param mu    Expected value
   * @param sigma Standard deviation
   */
  def addLogNormalGraphUpdateDistribution[VD, ED: ClassTag](sc: SparkContext, graph: Graph[VD, ED], mu: Int = 100, sigma: Double = 2): Graph[Int, Int] = {
    val g1 = addVertexUpdateDistribution(sc, graph, UpdateDistributionMode.PositiveCorrelation, DistributionType.LogNormal, mu, sigma)
    addEdgeUpdateDistribution(sc, g1, UpdateDistributionMode.PositiveCorrelation, DistributionType.LogNormal, mu, sigma)
  }

  /** Plot the distribution of the Int associated with vertices
   *
   * @param graph    A graph
   * @param bins     Number of bins for the histogram
   * @param title    Title for the generated diagram
   * @param filename Filename for the diagram
   * @tparam ED Edges can have any type */
  def plotUpdateDistributionVertices[ED](graph: Graph[Int, ED], bins: Int = 100, title: String = "Here your dist", filename: String = "plot.png"): Unit = {
    val figure = Figure()
    val plot = figure.subplot(0)
    plot += hist(graph.vertices.values.collect(), bins)
    plot.title = title
    figure.saveas(filename)
  }

  /** Plot the distribution of the Int associated with edges
   *
   * @param graph    A graph
   * @param bins     Number of bins for the histogram
   * @param title    Title for the generated diagram
   * @param filename Filename for the diagram
   * @tparam VD Vertices can have any type */
  def plotUpdateDistributionEdges[VD](graph: Graph[VD, Int], bins: Int = 100, title: String = "Here your dist", filename: String = "plot.png"): Unit = {
    val figure = Figure()
    val plot = figure.subplot(0)
    val values = graph.edges.collect().map(x => x.attr.toDouble)
    plot += hist(values, bins)
    plot.title = title
    figure.saveas(filename)
  }
}


