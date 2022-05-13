package benchmarks

import org.apache.spark.graphx.Graph
import thesis.DataSource.ContactsHyperText
import thesis.DistributionType.{LogNormalType, UniformType}
import thesis.SnapshotIntervalType.Count
import thesis.TopologyGraphGenerator.generateGraph
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{DataSource, Interval, Landy, SnapshotDelta}
import utils.TimeUtils.secondsToInstant

import java.time.Instant

class Q7(
          iterationCount: Int = 21,
          customColumn: String = "percentage of entire time interval queried",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends QueryBenchmark(iterationCount, customColumn, benchmarkSuffixes) {
  val g = addGraphUpdateDistribution(graph, LogNormalType(1, 0.8))
  val logs = generateLogs(g)

  val landyGraph = Landy(logs)
  val snapshotDeltaGraph = SnapshotDelta(logs, Count(1000))

  val logTimestamps = logs.map(_.timestamp).collect()
  val numTimestamps = logs.count()
  val firstTimestamp = logTimestamps.head

  lazy val graph: Graph[Long, Interval] = {
    generateGraph(40, DataSource.ContactsHyperText).mapEdges(edge => {
      val Interval(start, stop) = edge.attr
      if (stop.isBefore(start)) Interval(stop, start) else Interval(start, stop)
    })
  }

  override def execute(iteration: Int): Unit = {
    val timestampIndex: Int = (numTimestamps.toDouble / iterationCount.toDouble).floor.toInt * iteration
    val percentage: Double = iteration.toDouble / iterationCount.toDouble * 100
    val interval = Interval(firstTimestamp, logTimestamps(timestampIndex))

    // Warm up to ensure the first doesn't require more work.
    landyGraph.directNeighbours(1, Interval(firstTimestamp, logTimestamps.last)).collect()
    snapshotDeltaGraph.directNeighbours(1, Interval(firstTimestamp, logTimestamps.last)).collect()


    unpersist()
    benchmarks(0).benchmarkAvg(landyGraph.directNeighbours(1, interval).collect(), customColumnValue = percentage.toString)
    unpersist()
    benchmarks(1).benchmarkAvg(snapshotDeltaGraph.directNeighbours(1, interval).collect(), customColumnValue = percentage.toString)
  }
}
