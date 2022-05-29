package benchmarks

import org.apache.spark.graphx.Graph
import thesis.DistributionType.LogNormalType
import thesis.SnapshotIntervalType.Count
import thesis.SparkConfiguration.getSparkSession
import thesis.TopologyGraphGenerator.generateGraph
import thesis.UpdateDistributions.loadOrGenerateLogs
import thesis.{DataSource, Interval, SnapshotDelta, Validity}
import utils.UtilsUtils.PersistGraph


object StorageComparison {

  implicit val spark = getSparkSession
  implicit val sc = spark.sparkContext

  val datasource = DataSource.ContactsHyperText
  lazy val graph: Graph[Long, Interval] = {
    generateGraph(40, datasource).mapEdges(edge => {
      val Interval(start, stop) = edge.attr
      if (stop.isBefore(start)) Interval(stop, start) else Interval(start, stop)
    })
  }


  def run(): Unit = {
    val logs = loadOrGenerateLogs(graph, LogNormalType(1, 0.4), datasource)
    val numberOfLogs = logs.count()
    val landyGraph = Validity(logs)
    val snapshotDeltaGraph10 = SnapshotDelta(logs, Count((numberOfLogs / 10).toInt))
    val snapshotDeltaGraph100 = SnapshotDelta(logs, Count((numberOfLogs / 100).toInt))
    landyGraph.underlyingGraph.saveAsObjectFiles("landy.data")
    saveSnapshot(snapshotDeltaGraph10, "10")
    saveSnapshot(snapshotDeltaGraph100, "100")

  }

  def saveSnapshot(s: SnapshotDelta, prefix: String = ""): Unit = {
    val gs = s.graphs
    val logs = s.logs
    gs.zipWithIndex.foreach(snapshotAndIndex => {
      val (snapshot, index) = snapshotAndIndex
      snapshot.graph.saveAsObjectFiles(s"$prefix-snapshot-$index.data")
    })
    logs.saveAsObjectFile(s"storage/$prefix-logs.data")
  }

}
