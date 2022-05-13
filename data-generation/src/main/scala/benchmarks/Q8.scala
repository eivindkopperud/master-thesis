package benchmarks

import org.apache.spark.graphx.Graph
import thesis.DistributionType.LogNormalType
import thesis.SnapshotIntervalType.Count
import thesis.TopologyGraphGenerator.generateGraph
import thesis.UpdateDistributions.loadOrGenerateLogs
import thesis.{DataSource, Interval, Landy, SnapshotDelta}
import utils.UtilsUtils.CollectTuple

class Q8(
          iterationCount: Int = 21,
          customColumn: String = "percentage of entire time interval queried",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends QueryBenchmark(iterationCount, customColumn, benchmarkSuffixes) {

  lazy val graph: Graph[Long, Interval] = {
    generateGraph(40, DataSource.ContactsHyperText).mapEdges(edge => {
      val Interval(start, stop) = edge.attr
      if (stop.isBefore(start)) Interval(stop, start) else Interval(start, stop)
    })
  }
  val dataSource = DataSource.ContactsHyperText
  val logs = loadOrGenerateLogs(graph, LogNormalType(1, 0.8), dataSource)

  val landyGraph = Landy(logs)
  val snapshotDeltaGraph = SnapshotDelta(logs, Count(1000))

  val logTimestamps = logs.map(_.timestamp).collect()
  val numTimestamps = logs.count()
  val firstTimestamp = logTimestamps.head


  override def execute(iteration: Int): Unit = {
    val timestampIndex: Int = (numTimestamps.toDouble / iterationCount.toDouble).floor.toInt * iteration
    val percentage: Double = iteration.toDouble / iterationCount.toDouble * 100
    val interval = Interval(firstTimestamp, logTimestamps(timestampIndex))

    // Warm up to ensure the first doesn't require more work.
    landyGraph.activatedEntities(Interval(firstTimestamp, logTimestamps.last)).collect()
    snapshotDeltaGraph.activatedEntities(Interval(firstTimestamp, logTimestamps.last)).collect()


    unpersist()
    benchmarks(0).benchmarkAvg(landyGraph.activatedEntities(interval).collect(), customColumnValue = percentage.toString)
    unpersist()
    benchmarks(1).benchmarkAvg(snapshotDeltaGraph.activatedEntities(interval).collect(), customColumnValue = percentage.toString)
  }
}
