package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{Interval, Landy, SnapshotDelta}
import utils.TimeUtils.secondsToInstant

import java.time.Instant

class Q1(
          iterationCount: Int = 5,
          customColumn: String = "average number of logs for each entity",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends ComparisonBenchmark(iterationCount, customColumn, benchmarkSuffixes) {

  override def execute(iteration: Int): Unit = {
    val g = addGraphUpdateDistribution(graph, distribution(iteration))
    val logs = generateLogs(g)

    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Count(intervalDelta))

    val interval = Interval(0, Instant.MAX)

    // Warm up to ensure the first doesn't require more work.
    landyGraph.directNeighbours(vertexId, Interval(0, 0))
    snapshotDeltaGraph.directNeighbours(vertexId, Interval(0, 0))

    unpersist()
    benchmarks(0).benchmarkAvg(landyGraph.directNeighbours(vertexId, interval).collect(), customColumnValue = getMean(iteration).toString)
    unpersist()
    benchmarks(1).benchmarkAvg(snapshotDeltaGraph.directNeighbours(vertexId, interval).collect(), customColumnValue = getMean(iteration).toString)
  }
}
