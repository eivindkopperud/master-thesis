package thesis

import breeze.plot.{Figure, hist}
import breeze.stats.distributions.{Gaussian, LogNormal, Uniform}
import factories.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}
import thesis.Action.{CREATE, DELETE}
import thesis.DistributionType.{LogNormalType, UniformType}
import thesis.Entity.EDGE

import java.nio.file.{Files, Paths}
import scala.reflect.ClassTag

sealed abstract class CorrelationMode

object CorrelationMode {
  final case object Uniform extends CorrelationMode

  final case object PositiveCorrelation extends CorrelationMode

  final case object NegativeCorrelation extends CorrelationMode
}

sealed abstract class DistributionType

object DistributionType {
  final case class LogNormalType(mu: Int, sigma: Double) extends DistributionType

  final case class GaussianType(mu: Int, sigma: Double) extends DistributionType

  final case class UniformType(low: Double, high: Double) extends DistributionType
}

final case class IntervalAndUpdateCount(interval: TimeInterval, count: Int)

final case class IntervalAndDegrees(interval: TimeInterval, degree: Int)

object UpdateDistributions {

  // This function should only have VertexRDD[VD] as input. The edges are are not relevant

  /** Add an update count to all nodes
   *
   * @param sc           A SparkContext
   * @param graph        A graph
   * @param mode         Mode for distributing the weights based on vertex degree
   * @param distribution Some probability distribution
   * @return Processed graph
   */
  def addVertexUpdateDistribution[VD: ClassTag, ED: ClassTag](sc: SparkContext, graph: Graph[VD, ED], mode: CorrelationMode, distribution: DistributionType): Graph[Int, ED] = {
    getLogger.warn(s"Adding updates to vertices")
    val verticesWithDegree = sortVerticesByMode(graph.ops.degrees, mode)

    val updates = graph.mapVertices((_, _) => getDistributionDraw(distribution))
      .vertices
      .sortBy(_._2)
      .map(_._2)

    val vertices = verticesWithDegree
      .zip(updates)
      .map(vertex => (vertex._1._1, vertex._2))

    Graph[Int, ED](vertices, graph.edges)
  }

  /** Sorts vertex weights based on the mode
   *
   * @param mode Indicates the mode of which the nodes are ordered by
   * @return Sorted RDD
   * */
  def sortVerticesByMode(vertices: VertexRDD[Int], mode: CorrelationMode): RDD[(VertexId, Int)] = {
    mode match {
      case CorrelationMode.Uniform => vertices.sortBy(_.hashCode())
      case CorrelationMode.PositiveCorrelation => vertices.sortBy(_._2, ascending = true)
      case CorrelationMode.NegativeCorrelation => vertices.sortBy(_._2, ascending = false)
    }
  }

  private def sortEdgesByMode(edges: RDD[Edge[IntervalAndDegrees]], mode: CorrelationMode): RDD[Edge[IntervalAndDegrees]] = {
    mode match {
      case CorrelationMode.Uniform => edges.sortBy(_.hashCode())
      case CorrelationMode.PositiveCorrelation => edges.sortBy(_.attr.degree, ascending = true)
      case CorrelationMode.NegativeCorrelation => edges.sortBy(_.attr.degree, ascending = false)
    }
  }

  /** Add an update count to all edges
   *
   * @param sc           A SparkContext
   * @param graph        A graph where vertices have a update count as a Int
   * @param mode         Mode for distributing the weights based on vertex degree
   * @param distribution Some probability distribution
   * @return */
  def addEdgeUpdateDistribution[VD](sc: SparkContext,
                                    graph: Graph[Int, TimeInterval],
                                    mode: CorrelationMode,
                                    distribution: DistributionType): Graph[Int, IntervalAndUpdateCount] = {
    getLogger.warn(s"Adding updates to edges")
    // TODO remove this collect
    val vertexUpdateHashMap = graph
      .vertices
      .collect()
      .toMap

    val edgesWithDegree = {
      val edgesWithDegreeUnsorted = graph
        .mapEdges(edge =>
          IntervalAndDegrees(edge.attr, getVertexUpdateSum(edge, vertexUpdateHashMap)))
        .edges
      sortEdgesByMode(edgesWithDegreeUnsorted, mode)
    }

    val numberOfUpdates = graph
      .mapEdges(_ => getDistributionDraw(distribution))
      .edges
      .sortBy(edge => edge.attr)
      .map(edge => edge.attr)

    val edgesWithUpdateCount = edgesWithDegree
      .zip(numberOfUpdates)
      .map(edgeAndCount =>
        Edge(edgeAndCount._1.srcId, edgeAndCount._1.dstId, IntervalAndUpdateCount(edgeAndCount._1.attr.interval, edgeAndCount._2)))
    Graph[Int, IntervalAndUpdateCount](graph.vertices, edgesWithUpdateCount)
  }


  /** Get the sum of the edges connected vertices
   *
   * @param edge    relevant edge
   * @param hashMap Hash map with vertices as keys and update counts as values
   * @return The sum of updates of the edge's connected vertices
   */
  private def getVertexUpdateSum[ED](edge: Edge[ED], hashMap: Map[VertexId, Int]): Int = {
    hashMap.getOrElse(edge.srcId, 0) + hashMap.getOrElse(edge.dstId, 0)
  }

  /** Draw a number from a probability distribution
   *
   * @param distribution The type of probability distribution
   * @return A value in the distribution
   * */
  private def getDistributionDraw(distribution: DistributionType): Int = {
    distribution match {
      case DistributionType.LogNormalType(mu, sigma) => LogNormal(mu, sigma).draw().toInt
      case DistributionType.GaussianType(mu, sigma) => Gaussian(mu, sigma).draw().toInt
      case DistributionType.UniformType(low, high) => Uniform(low, high).draw().toInt
    }
  }

  private def getSortedUniformDistribution(low: Double, high: Double, numSamples: Int): List[Long] =
    Uniform(low, high).sample(numSamples).map(_.toLong).toList.sorted

  //For example, the number of random version-4 UUIDs which need to be generated
  // in order to have a 50% probability of at least one collision is 2.71 quintillion
  // This number is equivalent to generating 1 billion UUIDs per second for about 85 years.
  // from https://en.wikipedia.org/wiki/Universally_unique_identifier#Collisions
  // Using getLeastSignificantBits we half that 128bit space to 64. So in my head
  // you can find a collision after 85/2 years.
  // After our discussion it will not be 85/2 years but something else
  def uuid: Long = java.util.UUID.randomUUID.getLeastSignificantBits & Long.MaxValue

  def generateLogs(graph: Graph[Int, IntervalAndUpdateCount]): RDD[LogTSV] = {
    val (vertices, edges) = (graph.vertices, graph.edges)
    val vertexIdWithTimestamp = vertices.map(vertex => {
      val (id, numberOfUpdates) = vertex
      (id, getSortedUniformDistribution(0, 1000, numberOfUpdates)) // TODO find end timestamp

    })
    val vertexLogs = vertexIdWithTimestamp.map(vertex => {
      val (id, timestamps) = vertex
      val logs = {
        if (timestamps.isEmpty) {
          List()
        } else {
          val create = List(LogFactory().generateVertexTSV(id, timestamps.head).copy(action = CREATE))
          create ++ timestamps.tail.map(LogFactory().generateVertexTSV(id, _))
        }
      }
      (id, logs)
    }).flatMap(_._2)

    val edgeIdWithTimestamp = edges.map(edge => {
      val distribution = getSortedUniformDistribution(edge.attr.interval.start.getEpochSecond, edge.attr.interval.stop.getEpochSecond, edge.attr.count)
      (EDGE(uuid, edge.srcId, edge.dstId), (edge.attr.interval, distribution)) // Add Surrogate key
    })
    val edgeLogs = edgeIdWithTimestamp.map(edge => {
      val (ids, (interval, timestamps)) = edge
      val startTSV = LogFactory().getCreateDeleteEdgeTSV(ids, interval.start, CREATE)
      val endTSV = LogFactory().getCreateDeleteEdgeTSV(ids, interval.stop, DELETE)
      (ids, List(startTSV) ++ timestamps.map(LogFactory().generateEdgeTSV(ids, _)) ++ List(endTSV))
    }).flatMap(_._2)

    (vertexLogs ++ edgeLogs).sortBy(_.timestamp) // Sorting is expensive, but I think it is needed
  }


  /** Add log normal distributed weights as the property for vertices and edges
   *
   * Idk why this isn't just one function.
   * TODO make a general function where the distribution can be inserted
   *
   * @param sc    A SparkContext
   * @param mu    Expected value
   * @param sigma Standard deviation
   */
  def addLogNormalGraphUpdateDistribution[VD: ClassTag](sc: SparkContext, graph: Graph[VD, TimeInterval], mu: Int = 100, sigma: Double = 2): Graph[Int, IntervalAndUpdateCount] = {
    val g1 = addVertexUpdateDistribution(sc, graph, CorrelationMode.PositiveCorrelation, LogNormalType(mu, sigma))
    addEdgeUpdateDistribution(sc, g1, CorrelationMode.PositiveCorrelation, LogNormalType(mu, sigma))
  }

  def addGraphUpdateDistribution[VD: ClassTag](graph: Graph[VD, TimeInterval], mode: DistributionType = UniformType(0, 10))(implicit sc: SparkContext): Graph[Int, IntervalAndUpdateCount] = {
    getLogger.warn(s"Adding updates with distribution type:$mode")
    val g1 = addVertexUpdateDistribution(sc, graph, CorrelationMode.PositiveCorrelation, mode)
    addEdgeUpdateDistribution(sc, g1, CorrelationMode.PositiveCorrelation, mode)
  }

  def getLogs[VD: ClassTag](sc: SparkContext, graph: Graph[VD, TimeInterval]): RDD[LogTSV] = {
    getLogger.warn("Generating updates")
    val g = addGraphUpdateDistribution(graph)(implicitly, sc)
    generateLogs(g)
  }

  def getLogger: Logger = LoggerFactory.getLogger("UpdateDistributionSpec")

  def saveLogs(logs: RDD[LogTSV], path: String = "stored_logs"): RDD[LogTSV] = {
    logs.saveAsObjectFile(path)
    getLogger.warn(s"Saving logs to directory: $path")
    logs
  }

  /** Load logs or Generate new
   *
   * If previous graph exists on disk, load from disk.
   * Else generate the graph and persist it on disk for later retrieval.
   * Saves a lot of time and RAM on bigger loads
   *
   * @param sc    Spark Context
   * @param graph Graph generated from dataset
   * @param path  Path to persisted graph
   * @tparam VD Type of input graph
   * @return RDD[LogTSV] ready for further processing
   */
  def loadOrGenerateLogs[VD: ClassTag](sc: SparkContext, graph: Graph[VD, TimeInterval], path: String = "stored_logs"): RDD[LogTSV] = {
    if (Files.exists(Paths.get(path))) {
      getLogger.warn("Fetching from file")
      sc.objectFile[LogTSV](path)
    } else {
      saveLogs(getLogs[VD](sc, graph))
    }
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
