package benchmarks

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import thesis.CorrelationMode.PositiveCorrelation
import thesis.DistributionType.{GaussianType, LogNormalType, UniformType, ZipfType}
import thesis.SnapshotIntervalType.Count
import thesis.TopologyGraphGenerator.generateGraph
import thesis.{Interval, Landy, SnapshotDelta}
import thesis.UpdateDistributions.loadOrGenerateLogs
import utils.UtilsUtils
import utils.UtilsUtils.{loadDataSource, loadDistributionType, loadThreshold}

import java.time.Instant

class Q1DegreeBenchmark extends QueryBenchmark(iterationCount = 20, customColumn = "Vertex degree", benchmarkSuffixes = Seq("landy", "snapshots")) {
  val param1 = UtilsUtils.getConfig("DISTRIBUTION_PARAM1").toDouble
  val param2 = UtilsUtils.getConfig("DISTRIBUTION_PARAM2").toDouble
  val distributionType = UtilsUtils.getConfig("DISTRIBUTION_TYPE") match {
    case "1" => UniformType(param1, param2)
    case "2" => LogNormalType(param1.toInt, param2)
    case "3" => GaussianType(param1.toInt, param2)
    case "4" => ZipfType(param1.toInt, param2)
    case _ => UniformType(0, 0)
  }
  lazy val graph: Graph[Long, Interval] = {
    generateGraph(loadThreshold(), loadDataSource()).mapEdges(edge => {
      val Interval(start, stop) = edge.attr
      if (stop.isBefore(start)) Interval(stop, start) else Interval(start, stop)
    })
  }
  val logs = loadOrGenerateLogs(graph, distributionType, loadDataSource(), correlationMode = PositiveCorrelation)
  val landyGraph = Landy(logs)
  val snapshotDeltaGraph = SnapshotDelta(logs, Count((logs.count() / 10).toInt))
  val interval = Interval(Instant.MIN, Instant.MAX)
  val vertexIds = UtilsUtils.getNumberOfNeighboursPrNode(graph)

  override def execute(iteration: Int): Unit = {
    // Warm up to ensure the first doesn't require more work.
    val vertex = vertexIds(((iterationCount - iteration.toDouble) / iterationCount.toDouble * (vertexIds.size-1)).toInt)
    logger.warn(s"i $iteration: Running warmup")
    landyGraph.directNeighbours(0, interval).collect()
    snapshotDeltaGraph.directNeighbours(0, interval).collect()

    logger.warn(s"i $iteration: Unpersisting, then running landy")
    unpersist()
    benchmarks(0).benchmarkAvg(landyGraph.directNeighbours(vertex._1, interval).collect(), customColumnValue = vertex._2.toString)

    logger.warn(s"i $iteration: Unpersisting, then running snapshotsdelta")
    unpersist()
    benchmarks(1).benchmarkAvg(snapshotDeltaGraph.directNeighbours(vertex._1, interval).collect(), customColumnValue = vertex._2.toString)
  }

}
