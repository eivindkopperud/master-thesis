package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{Interval, Landy, SnapshotDelta}
import utils.TimeUtils.secondsToInstant

import java.time.Instant

class Q4(
          iterationCount: Int = 5,
          customColumn: String = "average number of logs for each entity",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends ComparisonBenchmark(iterationCount, customColumn, benchmarkSuffixes) {

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
    benchmarks(0).benchmarkAvg({
      val (vertexIds, edgeIds) = landyGraph.activatedEntities(interval)
      vertexIds.collect()
      edgeIds.collect()
    }, customColumnValue = getMean(iteration).toString)
    unpersist()
    benchmarks(1).benchmarkAvg({
      val (vertexIds, edgeIds) = snapshotDeltaGraph.activatedEntities(interval)
      vertexIds.collect()
      edgeIds.collect()
    }, customColumnValue = getMean(iteration).toString)
  }
}
