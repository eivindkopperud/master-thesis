package benchmarks

import org.apache.spark.graphx.Graph
import thesis.DataSource.{ContactsHyperText, Reptilian}
import thesis.DistributionType.UniformType
import thesis.SnapshotIntervalType.Count
import thesis.TopologyGraphGenerator.generateGraph
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{Interval, Landy, SnapshotDelta}
import utils.TimeUtils.secondsToInstant

import java.time.Instant

class Q4(
          iterationCount: Int = 5,
          customColumn: String = "average number of logs for each entity",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends QueryBenchmark(iterationCount, customColumn, benchmarkSuffixes) {
  val threshold = 40
  val dataSource = ContactsHyperText
  val distribution = (iteration: Int) => UniformType(1, 1 + 2 * iteration)
  val timestamp = 1082148639L
  val intervalDelta = 1000
  val vertexId = 37

  lazy val graph: Graph[Long, Interval] = {
    generateGraph(threshold, dataSource).mapEdges(edge => {
      val Interval(start, stop) = edge.attr
      if (stop.isBefore(start)) Interval(stop, start) else Interval(start, stop)
    })
  }

  override def execute(iteration: Int): Unit = {
    val g = addGraphUpdateDistribution(graph, distribution(iteration))
    val logs = generateLogs(g)

    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Count(intervalDelta))

    val expectedLogPrEntity = (iteration + 1).toString
    val interval = Interval(0, Instant.MAX)

    // Warm up to ensure the first doesn't require more work.
    landyGraph.activatedEntities(Interval(0, 0))
    snapshotDeltaGraph.activatedEntities(Interval(0, 0))

    unpersist()
    benchmarks(0).benchmarkAvg(landyGraph.activatedEntities(interval), numberOfRuns = 5, customColumnValue = expectedLogPrEntity)
    unpersist()
    benchmarks(1).benchmarkAvg(snapshotDeltaGraph.activatedEntities(interval), numberOfRuns = 5, customColumnValue = expectedLogPrEntity)
  }
}
