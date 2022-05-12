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
    logger.warn(s"i $iteration: Generating distribution")
    val g = addGraphUpdateDistribution(graph, distribution(iteration))
    logger.warn(s"i $iteration: Generating logs")
    val logs = generateLogs(g)

    logger.warn(s"i $iteration: Number of logs ${logs.count()}")

    logger.warn(s"i $iteration: Generating graphs")
    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Count(intervalDelta))

    val expectedLogPrEntity = (iteration + 1).toString

    // Warm up to ensure the first doesn't require more work.
    logger.warn(s"i $iteration: Running warmup")
    landyGraph.snapshotAtTime(0)
    landyGraph.snapshotAtTime(0)

    logger.warn(s"i $iteration: Unpersisting, then running landy")
    unpersist()
    benchmarks(0).benchmarkAvg({
      val g = landyGraph.snapshotAtTime(timestamp)
      g.graph.edges.collect()
      g.graph.vertices.collect()
    }, customColumnValue = getMean(iteration).toString)

    logger.warn(s"i $iteration: Unpersisting, then running snapshotsdelta")
    unpersist()
    benchmarks(1).benchmarkAvg({
      val g = snapshotDeltaGraph.snapshotAtTime(timestamp)
      g.graph.edges.collect()
      g.graph.vertices.collect()
    }, customColumnValue = getMean(iteration).toString)

  }
}
