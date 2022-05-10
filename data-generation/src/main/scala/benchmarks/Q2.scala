package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{Landy, SnapshotDelta}
import utils.TimeUtils.secondsToInstant

/** Benchmark landy snapshot with a variation of log numbers. */
class Q2(
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
    landyGraph.snapshotAtTime(0)
    landyGraph.snapshotAtTime(0)

    unpersist()
    benchmarks(0).benchmarkAvg({
      val g = landyGraph.snapshotAtTime(timestamp)
      g.graph.edges.collect()
      g.graph.vertices.collect()
    }, customColumnValue = getMean(iteration).toString)
    unpersist()
    benchmarks(1).benchmarkAvg({
      val g = snapshotDeltaGraph.snapshotAtTime(timestamp)
      g.graph.edges.collect()
      g.graph.vertices.collect()
    }, customColumnValue = getMean(iteration).toString)

  }
}
