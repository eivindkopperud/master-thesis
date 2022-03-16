import Action.UPDATE
import DistributionType.LogNormalType
import Entity.{EDGE, VERTEX}
import LTSV.Attributes
import breeze.plot.{Figure, hist}
import breeze.stats.distributions.{Gaussian, LogNormal, Uniform}
import com.github.javafaker.Faker
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import java.time.Instant
import scala.collection.immutable.HashMap
import scala.reflect.ClassTag
import scala.util.Random

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
  final case class UniformType(low:Double, high: Double) extends DistributionType
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
  def addVertexUpdateDistribution[VD: ClassTag, ED: ClassTag](sc: SparkContext, graph: Graph[VD, ED], mode: CorrelationMode, distribution: DistributionType): Graph[Int, ED] = {
    val verticesWithDegree: RDD[(VertexId, Int)] = sortVertexByMode(graph.ops.degrees, mode)

    val updates: RDD[Int] = graph.mapVertices((_, _) => getDistributionDraw(distribution))
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
  private def sortVertexByMode(vertices: VertexRDD[Int], mode: CorrelationMode): RDD[(VertexId, Int)] = {
    mode match {
      case CorrelationMode.Uniform => vertices.sortBy(_.hashCode())
      case CorrelationMode.PositiveCorrelation => vertices.sortBy(_._2, ascending = true)
      case CorrelationMode.NegativeCorrelation => vertices.sortBy(_._2, ascending = false)
    }
  }

  /** Add an update count to all edges
   *
   * @param sc           A SparkContext
   * @param graph        A graph where vertices have a update count as a Int
   * @param mode         Mode for distributing the weights based on vertex degree
   * @param distribution Some probability distribution
   * @return */
  def addEdgeUpdateDistribution[VD, ED: ClassTag](sc: SparkContext, graph: Graph[Int, ED], mode: CorrelationMode, distribution: DistributionType): Graph[Int, Int] = {
    //The HashMap is never updated?
    val vertexUpdateHashMap = graph
      .vertices
      .collect()
      .toMap

    val EdgesWithDegree = graph
      .mapEdges(edge => getVertexUpdateSum[ED](edge, vertexUpdateHashMap))
      .edges
      .collect()
      .sortBy(edge => edge)(getEdgeSorting(mode)) // this can be rewritten the same way as addVertexUpdateDistribution

    val numberOfUpdates = graph
      .mapEdges(_ => getDistributionDraw(distribution))
      .edges
      .collect()
      .sortBy(edge => edge.attr)
      .map(edge => edge.attr)

    val edgesWithUpdateCount = sc.parallelize(
      EdgesWithDegree
        .zip(numberOfUpdates)
        .map(edgeAndCount => Edge(edgeAndCount._1.srcId, edgeAndCount._1.dstId, edgeAndCount._2))
    )
    Graph[Int, Int](graph.vertices, edgesWithUpdateCount)
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

  private def getEdgeSorting(mode: CorrelationMode): Ordering[Edge[Int]] = {
    mode match {
      case CorrelationMode.Uniform => (x: Edge[_], y: Edge[_]) => x.hashCode() compareTo y.hashCode() // We want no correlation so they are ordered randomly
      case CorrelationMode.PositiveCorrelation => (x: Edge[Int], y: Edge[Int]) => x.attr compareTo y.attr
      case CorrelationMode.NegativeCorrelation => (x: Edge[Int], y: Edge[Int]) => y.attr compareTo x.attr
    }
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

  private def getSortedUniformDistribution(low:Double, high:Double, numSamples:Int):List[Int] =
    Uniform(low,high).sample(numSamples).map(_.toInt).toList.sorted

  def generateTSVs(graph: Graph[Int, Int]): RDD[LogTSV] = {
    val (vertices, edges) = (graph.vertices, graph.edges)
    val vertexIdWithTimestamp = vertices.map(vertex => {
      val (id, numberOfUpdates) = vertex
      (id, getSortedUniformDistribution(0, 1000, numberOfUpdates)) // TODO find end timestamp

    })
    val vertexLogs = vertexIdWithTimestamp.map(vertex => {
      val (id, timestamps) = vertex
      (id, timestamps.map(generateVertexTSV(id, _)))
    }).flatMap(_._2)

    // Has to be rewritten as Edge[Int] should be Edge[(Int, Timestamp)] from existing data
    val edgeIdWithTimestamp = edges.map(edge => {
      ((edge.srcId, edge.dstId), getSortedUniformDistribution(0,1000, edge.attr))
    })
    val edgeLogs = edgeIdWithTimestamp.map(edge => {
      val (ids, timestamps) = edge
      (ids, timestamps.map(generateEdgeTSV(ids, _)))
    }).flatMap(_._2)

    (vertexLogs ++ edgeLogs).sortBy(_.timestamp)
  }
  def generateEdgeTSV(srcIdAndDstId: (Long, Long), timestamp: Int) = {
    LogTSV(
      timestamp = Instant.ofEpochSecond(timestamp),
      action = UPDATE,
      objectType = EDGE.tupled(srcIdAndDstId),
      attributes = getRandomAttributes()
    )
  }


  def generateVertexTSV(id: VertexId, timestamp: Int): LogTSV = {
    LogTSV(
      timestamp = Instant.ofEpochSecond(timestamp),
      action = UPDATE,
      objectType = VERTEX(id),
      attributes = getRandomAttributes()
    )

  }

  private def getRandomItem(items: List[String]) ={
    val random = new Random()
    items(random.nextInt(items.length))
  }
  def getRandomAttributes():Attributes = {
    val faker = new Faker()
    HashMap[String,String](
      ("color", faker.color().name()),
      ("animal", faker.animal().name()),
      ("size", new Random().nextInt(10000).toString)
    )
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
  def addLogNormalGraphUpdateDistribution[VD, ED: ClassTag](sc: SparkContext, graph: Graph[VD, ED], mu: Int = 100, sigma: Double = 2): Graph[Int, Int] = {
    val g1 = addVertexUpdateDistribution(sc, graph, CorrelationMode.PositiveCorrelation, LogNormalType(mu, sigma))
    addEdgeUpdateDistribution(sc, g1, CorrelationMode.PositiveCorrelation, LogNormalType(mu, sigma))
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
  def plotUpdateDistributionEdges[VD](graph: Graph[VD, Int], bins: Int = 100, title: String = "Here your dist", filename: String = "plot.png") = {
    val figure = Figure()
    val plot = figure.subplot(0)
    val values = graph.edges.collect().map(x => x.attr.toDouble)
    plot += hist(values, bins)
    plot.title = title
    figure.saveas(filename)
  }

}


