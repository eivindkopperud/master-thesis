package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{Landy, SnapshotDelta, VERTEX}
import utils.TimeUtils.secondsToInstant

class Q3(
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

    // Warm up to ensure the first doesn't require more work.
    landyGraph.getEntity(VERTEX(vertexId), 0L)
    snapshotDeltaGraph.getEntity(VERTEX(vertexId), 0L)

    unpersist()
    benchmarks(0).benchmarkAvg(landyGraph.getEntity(VERTEX(vertexId), timestamp), customColumnValue = getMean(iteration).toString)
    unpersist()
    benchmarks(1).benchmarkAvg(snapshotDeltaGraph.getEntity(VERTEX(vertexId), timestamp), customColumnValue = getMean(iteration).toString)
  }
}
