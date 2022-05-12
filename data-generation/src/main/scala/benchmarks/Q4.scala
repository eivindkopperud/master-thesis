package benchmarks

import thesis.SnapshotIntervalType.Count
import thesis.UpdateDistributions.{addGraphUpdateDistribution, generateLogs}
import thesis.{Interval, Landy, SnapshotDelta}
import utils.TimeUtils.secondsToInstant

import java.time.Instant

class Q4(
          iterationCount: Int = 5,
          customColumn: String = "Number of logs",
          benchmarkSuffixes: Seq[String] = Seq("landy", "snapshot")
        ) extends ComparisonBenchmark(iterationCount, customColumn, benchmarkSuffixes) {

  override def execute(iteration: Int): Unit = {
    logger.warn(s"i $iteration: Generating distribution")
    val g = addGraphUpdateDistribution(graph, distribution(iteration))
    logger.warn(s"i $iteration: Generating logs")
    val logs = generateLogs(g)

    val numberOfLogs = logs.count().toString
    logger.warn(s"i $iteration: Number of logs $numberOfLogs")

    logger.warn(s"i $iteration: Generating graphs")
    val landyGraph = Landy(logs)
    val snapshotDeltaGraph = SnapshotDelta(logs, Count(intervalDelta))

    val expectedLogPrEntity = (iteration + 1).toString
    val interval = Interval(0, Instant.MAX)

    // Warm up to ensure the first doesn't require more work.
    logger.warn(s"i $iteration: Running warmup")
    landyGraph.activatedEntities(Interval(0, 0))
    snapshotDeltaGraph.activatedEntities(Interval(0, 0))

    logger.warn(s"i $iteration: Unpersisting, then running landy")
    unpersist()
    benchmarks(0).benchmarkAvg({
      val (vertexIds, edgeIds) = landyGraph.activatedEntities(interval)
      vertexIds.collect()
      edgeIds.collect()
    }, customColumnValue = numberOfLogs)

    logger.warn(s"i $iteration: Unpersisting, then running snapshotsdelta")
    unpersist()
    benchmarks(1).benchmarkAvg({
      val (vertexIds, edgeIds) = snapshotDeltaGraph.activatedEntities(interval)
      vertexIds.collect()
      edgeIds.collect()
    }, customColumnValue = numberOfLogs)
  }
}
