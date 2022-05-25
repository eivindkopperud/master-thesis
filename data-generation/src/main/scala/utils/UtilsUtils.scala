package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import thesis.DataTypes.{AttributeGraph, Attributes}
import thesis.DistributionType.{GaussianType, LogNormalType, UniformType, ZipfType}
import thesis.SparkConfiguration.getSparkSession
import thesis.TopologyGraphGenerator.generateGraph
import thesis.UpdateDistributions.loadOrGenerateLogs
import thesis.{CorrelationMode, DataSource, DistributionType, Interval, SnapshotEdgePayload}

object UtilsUtils {
  def uuid: Long = java.util.UUID.randomUUID.getLeastSignificantBits & Long.MaxValue

  def zipGraphs(g1: AttributeGraph, g2: AttributeGraph):
  (Seq[((VertexId, Attributes), (VertexId, Attributes))], Seq[(Edge[SnapshotEdgePayload], Edge[SnapshotEdgePayload])]) =
    (g1.vertices.zip(g2.vertices).collect(), g1.edges.zip(g2.edges).collect())

  def getConfig(key: String): String = sys.env(key)

  def getConfigSafe(key: String): Option[String] = sys.env.get(key)

  def loadThreshold(): Int = UtilsUtils.getConfigSafe("THRESHOLD") match {
    case Some(value) => value.toInt
    case None => 40
  }

  def numberOfNeighboursPrNode()(implicit sparkContext: SparkSession) = {
    val edges = generateGraph(loadThreshold(), loadDataSource()).mapEdges(edge => {
      val Interval(start, stop) = edge.attr
      if (stop.isBefore(start)) Interval(stop, start) else Interval(start, stop)
    }).edges
    val outEdges = edges.map(e => (e.srcId, e)).groupByKey().map(idAndEdges => (idAndEdges._1, idAndEdges._2.size))
    val inEdges = edges.map(e => (e.dstId, e)).groupByKey().map(idAndEdges => (idAndEdges._1, idAndEdges._2.size))
    val bothEdges = outEdges.join(inEdges)
      .map(vertexAndTup => (vertexAndTup._1, vertexAndTup._2._1 + vertexAndTup._2._2)).sortBy(_._2)

    val vertexIdAndDegrees = bothEdges.collect()
    println(vertexIdAndDegrees.mkString("Array(", ", ", ")"))
    val length = vertexIdAndDegrees.length
    val isEven = length % 2 == 0
    if (isEven) {
      Left((vertexIdAndDegrees(((length / 2) - 1)), vertexIdAndDegrees(length / 2)))
    } else {
      Right(vertexIdAndDegrees((length / 2).ceil.toInt))
    }
  }

  implicit class CollectTuple[A, B](tuplesRdds: (RDD[A], RDD[B])) {
    def collect(): (Array[A], Array[B]) = {
      val (a, b) = tuplesRdds
      (a.collect(), b.collect())
    }
  }

  implicit class PersistGraph[V, E](graph: Graph[V, E]) {
    def saveAsObjectFiles(path: String): Unit = {
      graph.vertices.saveAsObjectFile("storage/V_" + path)
      graph.edges.saveAsObjectFile("storage/E_" + path)
    }
  }

  /**
   * Try to load data source. Return ContactsHyperText as default.
   */
  def loadDataSource(): DataSource = UtilsUtils.getConfigSafe("DATA_SOURCE") match {
    case Some(value) =>
      if (value.toInt == 1) return DataSource.Reptilian
      if (value.toInt == 2) return DataSource.ContactsHyperText
      if (value.toInt == 3) return DataSource.FbMessages
      DataSource.Reptilian
    case None => DataSource.ContactsHyperText
  }

  def loadTimestamp(): Long = UtilsUtils.getConfigSafe("TIMESTAMP") match {
    case Some(value) => value.toLong
    case None => 0
  }

  def loadIntervalDelta(): Int = UtilsUtils.getConfigSafe("INTERVAL_DELTA") match {
    case Some(value) => value.toInt
    case None => 0
  }

  def loadVertexId(): Long = UtilsUtils.getConfigSafe("VERTEX_ID") match {
    case Some(value) => value.toLong
    case None => 0
  }

  def loadDistributionType(): DistributionType = {
    UtilsUtils.getConfigSafe("DISTRIBUTION_TYPE") match {
      case Some(value) =>
        if (value.toInt == 1) return UniformType(0, 0)
        if (value.toInt == 2) return LogNormalType(0, 0)
        if (value.toInt == 3) return GaussianType(0, 0)
        if (value.toInt == 4) return ZipfType(0, 0)
        UniformType(0, 0)
      case None => UniformType(0, 0)
    }
  }

  def persistSomeDistributions(): Unit = {
    getConfig("ENV_VARIABLES_ARE_SET") // Use this line if you want to make sure that env variabels are set
    implicit val spark: SparkSession = getSparkSession
    implicit val sc: SparkContext = spark.sparkContext

    val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

    val distribution = (iteration: Int, distributionType: DistributionType) => distributionType match {
      case LogNormalType(param1, param2) => LogNormalType(param1, iteration * param2)
      case GaussianType(param1, param2) => GaussianType(iteration * param1, param2)
      case UniformType(param1, param2) => UniformType(param1, iteration * param2)
      case ZipfType(param1, param2) => ZipfType(param1, iteration * param2)
    }
    val graph = {
      generateGraph(loadThreshold(), loadDataSource()).mapEdges(edge => {
        val Interval(start, stop) = edge.attr
        if (stop.isBefore(start)) Interval(stop, start) else Interval(start, stop)
      })
    }
    //val distributions = Seq(LogNormalType(1, 0.4), UniformType(1, 8), GaussianType(4, 2))
    val correlationModes = Seq(CorrelationMode.NegativeCorrelation, CorrelationMode.Uniform)
    for (corrMode <- correlationModes) {
      logger.warn(s" Correlation mode $corrMode ")
      for (iteration <- 1 to 5) {
        logger.warn(s" Iteration $iteration ")
        loadOrGenerateLogs(graph, distribution(iteration, LogNormalType(1, 0.4)), loadDataSource(), correlationMode = corrMode)
      }
    }
  }
}
